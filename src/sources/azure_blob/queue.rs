use std::{
    future::ready,
    io::{BufRead, BufReader, Cursor},
    panic,
    sync::Arc,
};

use async_stream::stream;
use azure_storage_blobs;
use azure_storage_queues;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use futures::{stream::StreamExt, FutureExt, Stream};
use serde::Deserialize;
use serde_with::serde_as;
use smallvec::SmallVec;
use snafu::Snafu;
use tokio::{pin, select};
use tokio_util::codec::FramedRead;
use vector_lib::{
    config::{log_schema, LegacyKey, LogNamespace},
    event::MaybeAsLogMut,
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol, Registered,
    },
    lookup::path,
};

use crate::{
    azure,
    codecs::Decoder,
    config::{SourceAcknowledgementsConfig, SourceContext},
    event::{BatchNotifier, EstimatedJsonEncodedSizeOf, Event, LogEvent},
    internal_events::EventsReceived,
    shutdown::ShutdownSignal,
    sinks::prelude::configurable_component,
    sources::azure_blob::{AzureBlobConfig, BlobStream},
    SourceSender,
};

/// Azure Queue configuration options.
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    /// The name of the storage queue to poll for events.
    pub(super) queue_name: String,
}

pub fn make_azure_row_stream(cfg: &AzureBlobConfig) -> crate::Result<BlobStream> {
    let queue_client = make_queue_client(cfg)?;
    let container_client = make_container_client(cfg)?;

    Ok(Box::pin(stream! {
        // TODO: add a way to stop this loop, possibly with shutdown
        loop {
            let messages_result = queue_client.get_messages().await;
            let messages = messages_result.expect("Failed reading messages");

            for message in messages.messages {
                let decoded_bytes = BASE64_STANDARD
                    .decode(&message.message_text)
                    .expect("Failed decoding message");
                let decoded_string = String::from_utf8(decoded_bytes).expect("Failed decoding UTF");
                let body: AzureStorageEvent =
                    serde_json::from_str(decoded_string.as_str()).expect("Wrong JSON");
                // TODO get the event type const from library?
                if body.event_type != "Microsoft.Storage.BlobCreated" {
                    info!(
                        "Ignoring event because of wrong event type: {}",
                        body.event_type
                    );
                    continue;
                }
                // TODO some smarter parsing should be done here
                let parts = body.subject.split("/").collect::<Vec<_>>();
                let container = parts[4];
                // TODO here we'd like to check if container matches the container
                // from config.
                let blob = parts[6];
                info!(
                    "New blob created in container '{}': '{}'",
                    &container, &blob
                );

                let blob_client = container_client.blob_client(blob);
                let mut result: Vec<u8> = vec![];
                let mut stream = blob_client.get().into_stream();
                while let Some(value) = stream.next().await {
                    let mut body = value.unwrap().data;
                    while let Some(value) = body.next().await {
                        let value = value.expect("Failed to read body chunk");
                        result.extend(&value);
                    }
                }

                let reader = Cursor::new(result);
                let buffered = BufReader::new(reader);
                queue_client
                    .pop_receipt_client(message)
                    .delete()
                    .await
                    .expect("Failed removing messages from queue");
                for line in buffered.lines() {
                    let line = line.map(|line| line.as_bytes().to_vec());
                    yield line.expect("ASDF");
                }
            }
        }
    }))
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum ProcessingError {
    // TODO improve errors with some details etc.
    InvalidMessageText,
    MessageTextNotUTF,
    InvalidMessageTextJSON,
    InvalidMessageSubject,
    ErrorReadingChunk,
    FailedGettingProperties,
    FailedGettingMetadata,
}

pub struct State {
    queue_client: Arc<azure_storage_queues::QueueClient>,
    container_client: Arc<azure_storage_blobs::prelude::ContainerClient>,

    decoder: Decoder,
}

pub(super) struct Ingestor {
    state: Arc<State>,
}

impl Ingestor {
    pub(super) async fn new(
        container_client: Arc<azure_storage_blobs::prelude::ContainerClient>,
        queue_client: Arc<azure_storage_queues::QueueClient>,
        _config: Config,
        decoder: Decoder,
    ) -> Result<Ingestor, ()> {
        let state = Arc::new(State {
            container_client,
            queue_client,
            decoder,
        });

        Ok(Ingestor { state })
    }

    pub(super) async fn run(
        self,
        cx: SourceContext,
        acknowledgements: SourceAcknowledgementsConfig,
        log_namespace: LogNamespace,
    ) -> Result<(), ()> {
        let acknowledgements = cx.do_acknowledgements(acknowledgements);
        let process = IngestorProcess::new(
            Arc::clone(&self.state),
            cx.out.clone(),
            cx.shutdown.clone(),
            log_namespace,
            acknowledgements,
        );
        process.run().await;
        Ok(())
    }
}

pub struct IngestorProcess {
    state: Arc<State>,
    out: SourceSender,
    shutdown: ShutdownSignal,
    acknowledgements: bool,
    log_namespace: LogNamespace,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
}

impl IngestorProcess {
    pub fn new(
        state: Arc<State>,
        out: SourceSender,
        shutdown: ShutdownSignal,
        log_namespace: LogNamespace,
        acknowledgements: bool,
    ) -> Self {
        Self {
            state,
            out,
            shutdown,
            acknowledgements,
            log_namespace,
            bytes_received: register!(BytesReceived::from(Protocol::HTTP)),
            events_received: register!(EventsReceived),
        }
    }

    async fn run(mut self) {
        let shutdown = self.shutdown.clone().fuse();
        pin!(shutdown);

        loop {
            select! {
                _ = &mut shutdown => break,
                _ = self.run_once() => {},
            }
        }
    }

    async fn run_once(&mut self) {
        // TODO this is a PoC. Need better error handling
        let messages_result = self.state.queue_client.get_messages().await;
        let messages = messages_result.expect("Failed reading messages");

        for message in messages.messages {
            match self.handle_storage_message(message).await {
                Ok(()) => {
                    // TODO do telemetry here and below instead of logs.
                    info!("Handled event!");
                }
                Err(err) => {
                    info!("Failed handling event: {:#?}", err);
                }
            }
        }
    }

    async fn handle_storage_message(
        &mut self,
        message: azure_storage_queues::operations::Message,
    ) -> Result<(), ProcessingError> {
        let decoded_bytes = match BASE64_STANDARD.decode(&message.message_text) {
            Ok(bytes) => bytes,
            Err(err) => return Err(ProcessingError::InvalidMessageText),
        };
        let decoded_string = match String::from_utf8(decoded_bytes) {
            Ok(dstring) => dstring,
            Err(err) => return Err(ProcessingError::MessageTextNotUTF),
        };
        let azure_storage_event: AzureStorageEvent =
            match serde_json::from_str(decoded_string.as_str()) {
                Ok(azure_storage_event) => azure_storage_event,
                Err(err) => return Err(ProcessingError::InvalidMessageTextJSON),
            };
        if azure_storage_event.event_type != "Microsoft.Storage.BlobCreated" {
            debug!(
                "Ignoring event because of wrong event type: {}",
                azure_storage_event.event_type
            );
            return Ok(());
        }
        let parts = azure_storage_event.subject.split("/").collect::<Vec<_>>();
        if parts.len() < 7 {
            return Err(ProcessingError::InvalidMessageSubject);
        }
        let container = parts[4];
        if self.state.container_client.container_name() != container {
            debug!(
                "Ignoring event because of wrong container name: {}",
                container
            );
            return Ok(());
        }
        let blob = parts[6];
        debug!(
            "New blob created in container '{}': '{}'",
            &container, &blob
        );

        let blob_client = self.state.container_client.blob_client(blob);
        let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(self.acknowledgements);

        // TODO do this
        // let metadata: HashMap<String, String> = match blob_client.get_metadata().await {
        // Ok(metadata) => metadata
        // .metadata
        // .iter()
        // .map(|(k, v)| (k.clone(), Bytes::copy_from_slice(v.as_bytes())))
        // .collect(),
        // Err(err) => return Err(ProcessingError::FailedGettingMetadata),
        // };

        // TODO do this
        // let properties_response = match blob_client.get_properties().await {
        // Ok(properties_response) => properties_response,
        // Err(err) => return Err(ProcessingError::FailedGettingProperties),
        // };
        // let properties = properties_response.blob.properties;

        // let timestamp: Option<DateTime<Utc>> = properties.last_modified.map(|ts| {
        // Utc.timestamp_opt(ts.unix_timestamp(), ts.nanosecond())
        // .unwrap()
        // });
        let blob_reader = super::azure_blob_decoder(blob_client.get().into_stream()).await;

        let mut read_error = None;
        let bytes_received = self.bytes_received.clone();
        let events_received = self.events_received.clone();
        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = Box::new(
            FramedRead::new(blob_reader, self.state.decoder.framer.clone())
                .map(|res| {
                    res.map(|bytes| {
                        bytes_received.emit(ByteSize(bytes.len()));
                        bytes
                    })
                    .map_err(|err| {
                        read_error = Some(err);
                    })
                    .ok()
                })
                .take_while(|res| ready(res.is_some()))
                .map(|r| r.expect("validated by take_while")),
        );

        let stream = lines.flat_map(|line| {
            let events = match self.state.decoder.deserializer_parse(line) {
                Ok((events, _events_size)) => events,
                Err(_error) => {
                    // Error is handled by `codecs::Decoder`, no further handling
                    // is needed here.
                    SmallVec::new()
                }
            };

            let events = events
                .into_iter()
                .map(|mut event: Event| {
                    event = event.with_batch_notifier_option(&batch);
                    if let Some(log_event) = event.maybe_as_log_mut() {
                        handle_single_log(log_event, self.log_namespace, &azure_storage_event);
                    }
                    events_received.emit(CountByteSize(1, event.estimated_json_encoded_size_of()));
                    event
                })
                .collect::<Vec<Event>>();
            futures::stream::iter(events)
        });
        Ok(())
    }
}

fn handle_single_log(
    log: &mut LogEvent,
    log_namespace: LogNamespace,
    azure_storage_event: &AzureStorageEvent,
) {
    log_namespace.insert_source_metadata(
        AzureBlobConfig::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("subject"))),
        path!("subject"),
        Bytes::from(azure_storage_event.subject.as_bytes().to_vec()),
    );

    log_namespace.insert_source_metadata(
        AzureBlobConfig::NAME,
        log,
        Some(LegacyKey::Overwrite("ingest_timestamp")),
        path!("ingest_timestamp"),
        chrono::Utc::now().to_rfc3339(),
    );

    // if let Some(metadata) = metadata {
    // for (key, value) in metadata {
    // log_namespace.insert_source_metadata(
    // AzureBlobConfig::NAME,
    // log,
    // Some(LegacyKey::Overwrite(path!(key))),
    // path!("metadata", key.as_str()),
    // value.clone(),
    // );
    // }
    // }

    log_namespace.insert_vector_metadata(
        log,
        log_schema().source_type_key(),
        path!("source_type"),
        Bytes::from_static(AzureBlobConfig::NAME.as_bytes()),
    );
}

fn make_queue_client(
    cfg: &AzureBlobConfig,
) -> crate::Result<Arc<azure_storage_queues::QueueClient>> {
    let q = cfg.queue.clone().ok_or("Missing queue.")?;
    azure::build_queue_client(
        cfg.connection_string
            .as_ref()
            .map(|v| v.inner().to_string()),
        cfg.storage_account.as_ref().map(|v| v.to_string()),
        q.queue_name.clone(),
        cfg.endpoint.clone(),
    )
}

fn make_container_client(
    cfg: &AzureBlobConfig,
) -> crate::Result<Arc<azure_storage_blobs::prelude::ContainerClient>> {
    azure::build_container_client(
        cfg.connection_string
            .as_ref()
            .map(|v| v.inner().to_string()),
        cfg.storage_account.as_ref().map(|v| v.to_string()),
        cfg.container_name.clone(),
        cfg.endpoint.clone(),
    )
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AzureStorageEvent {
    pub subject: String,
    pub event_type: String,
}

#[test]
fn test_azure_storage_event() {
    let event_value: AzureStorageEvent = serde_json::from_str(
        r#"{
          "topic": "/subscriptions/fa5f2180-1451-4461-9b1f-aae7d4b33cf8/resourceGroups/events_poc/providers/Microsoft.Storage/storageAccounts/eventspocaccount",
          "subject": "/blobServices/default/containers/content/blobs/foo",
          "eventType": "Microsoft.Storage.BlobCreated",
          "id": "be3f21f7-201e-000b-7605-a29195062628",
          "data": {
            "api": "PutBlob",
            "clientRequestId": "1fa42c94-6dd3-4172-95c4-fd9cf56b5009",
            "requestId": "be3f21f7-201e-000b-7605-a29195000000",
            "eTag": "0x8DC701C5D3FFDF6",
            "contentType": "application/octet-stream",
            "contentLength": 0,
            "blobType": "BlockBlob",
            "url": "https://eventspocaccount.blob.core.windows.net/content/foo",
            "sequencer": "0000000000000000000000000005C5360000000000276a63",
            "storageDiagnostics": {
              "batchId": "fec5b12c-2006-0034-0005-a25936000000"
            }
          },
          "dataVersion": "",
          "metadataVersion": "1",
          "eventTime": "2024-05-09T11:37:10.5637878Z"
        }"#,
    ).unwrap();

    assert_eq!(
        event_value.subject,
        "/blobServices/default/containers/content/blobs/foo".to_string()
    );
    assert_eq!(
        event_value.event_type,
        "Microsoft.Storage.BlobCreated".to_string()
    );
}
