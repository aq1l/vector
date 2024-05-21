use crate::config::{SourceAcknowledgementsConfig, SourceContext};
// use crate::internal_events::EventsReceived;
use crate::shutdown::ShutdownSignal;
use crate::sinks::prelude::configurable_component;
use crate::sources::azure_blob::BlobStream;
use crate::SourceSender;
use async_stream::stream;
use azure_storage_blobs;
use azure_storage_queues;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use futures::FutureExt;
use serde::Deserialize;
use serde_with::serde_as;
use std::{num::NonZeroUsize, panic, sync::Arc};
use tokio::{pin, select};
use tracing::Instrument;
use vector_lib::config::LogNamespace;
// use vector_lib::internal_event::{BytesReceived, Protocol, Registered};

/// Azure Queue configuration options.
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    /// The name of the storage queue to poll for events.
    pub(super) queue_name: String,

    /// Number of concurrent tasks to create for polling the queue for messages.
    ///
    /// Defaults to the number of available CPUs on the system.
    ///
    /// Should not typically need to be changed, but it can sometimes be beneficial to raise this
    /// value when there is a high rate of messages being pushed into the queue and the objects
    /// being fetched are small. In these cases, system resources may not be fully utilized without
    /// fetching more messages per second, as the SQS message consumption rate affects the S3 object
    /// retrieval rate.
    #[configurable(metadata(docs::type_unit = "tasks"))]
    #[configurable(metadata(docs::examples = 5))]
    pub(super) client_concurrency: Option<NonZeroUsize>,
}

pub struct State {
    container_client: Arc<azure_storage_blobs::prelude::ContainerClient>,
    queue_client: Arc<azure_storage_queues::QueueClient>,

    client_concurrency: usize,
}

pub(super) struct Ingestor {
    state: Arc<State>,
}

impl Ingestor {
    pub(super) async fn new(
        container_client: Arc<azure_storage_blobs::prelude::ContainerClient>,
        queue_client: Arc<azure_storage_queues::QueueClient>,
        config: Config,
    ) -> Result<Ingestor, ()> {
        let state = Arc::new(State {
            container_client,
            queue_client,
            client_concurrency: config
                .client_concurrency
                .map(|n| n.get())
                .unwrap_or_else(crate::num_threads),
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
        let mut handles = Vec::new();
        for _ in 0..self.state.client_concurrency {
            let process = IngestorProcess::new(
                Arc::clone(&self.state),
                cx.out.clone(),
                cx.shutdown.clone(),
                log_namespace,
                acknowledgements,
            );
            let fut = process.run();
            let handle = tokio::spawn(fut.in_current_span());
            handles.push(handle);
        }

        // Wait for all of the processes to finish.  If any one of them panics, we resume
        // that panic here to properly shutdown Vector.
        for handle in handles.drain(..) {
            if let Err(e) = handle.await {
                if e.is_panic() {
                    panic::resume_unwind(e.into_panic());
                }
            }
        }

        Ok(())
    }
}

pub struct IngestorProcess {
    state: Arc<State>,
    // out: SourceSender,
    shutdown: ShutdownSignal,
    // acknowledgements: bool,
    // log_namespace: LogNamespace,
    // bytes_received: Registered<BytesReceived>,
    // events_received: Registered<EventsReceived>,
}

impl IngestorProcess {
    pub fn new(
        state: Arc<State>,
        _: SourceSender,
        shutdown: ShutdownSignal,
        _: LogNamespace,
        _: bool,
    ) -> Self {
        Self {
            state,
            // out,
            shutdown,
            // acknowledgements,
            // log_namespace,
            // bytes_received: register!(BytesReceived::from(Protocol::HTTP)),
            // events_received: register!(EventsReceived),
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

            let blob_client = self.state.container_client.blob_client(blob);
            let blob_content = blob_client
                .get_content()
                .await
                .expect("Failed getting blob content");
            info!("\tBlob content: {:#?}", blob_content);
            self.state
                .queue_client
                .pop_receipt_client(message)
                .delete()
                .await
                .expect("Failed removing messages from queue");
        }
    }
}

pub fn make_kwapik_stream(
    container_client: Arc<azure_storage_blobs::prelude::ContainerClient>,
    queue_client: Arc<azure_storage_queues::QueueClient>,
) -> BlobStream {
    Box::pin(stream! {
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
                let blob_content = blob_client
                    .get_content()
                    .await
                    .expect("Failed getting blob content");
                info!("\tBlob content: {:#?}", blob_content);
                queue_client
                    .pop_receipt_client(message)
                    .delete()
                    .await
                    .expect("Failed removing messages from queue");
                yield blob_content
            }
        }
    })
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
