use crate::azure;
use crate::codecs::DecodingConfig;
use crate::config::{
    LogNamespace, SourceAcknowledgementsConfig, SourceConfig, SourceContext, SourceOutput,
};
use crate::event::Event;
use crate::serde::{bool_or_struct, default_decoding};
use crate::shutdown::ShutdownSignal;
use crate::sinks::prelude::configurable_component;
use crate::SourceSender;
use bytes::Bytes;
use futures::Stream;
use futures_util::StreamExt;
use snafu::Snafu;
use std::pin::Pin;
use std::sync::Arc;
use vector_lib::codecs::decoding::{
    DeserializerConfig, FramingConfig, NewlineDelimitedDecoderOptions,
};
use vector_lib::codecs::NewlineDelimitedDecoderConfig;
use vector_lib::config::LegacyKey;
use vector_lib::sensitive_string::SensitiveString;
use vrl::path;
use crate::sources::azure_blob::queue::make_kwapik_stream;

pub mod queue;

/// Strategies for consuming objects from Azure Storage.
#[configurable_component]
#[derive(Clone, Copy, Debug, Derivative)]
#[serde(rename_all = "lowercase")]
#[derivative(Default)]
enum Strategy {
    /// Consumes objects by processing events sent to an [Azure Storage Queue][azure_storage_queue].
    ///
    /// [azure_storage_queue]: https://learn.microsoft.com/en-us/azure/storage/queues/storage-queues-introduction
    StorageQueue,

    /// This is a test strategy used only of development and PoC. Should be removed
    /// once development is done.
    #[derivative(Default)]
    Test,
}

/// WIP
/// A dummy implementation is used as a starter.
/// The source will send dummy messages at a fixed interval, incrementing a counter every
/// exec_interval_secs seconds.
#[configurable_component(source("azure_blob", "Collect logs from Azure Container."))]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(default, deny_unknown_fields)]
pub struct AzureBlobConfig {
    // TODO : everything
    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    log_namespace: Option<bool>,

    /// The interval, in seconds, between subsequent dummy messages
    #[serde(default = "default_exec_interval_secs")]
    exec_interval_secs: u64,

    /// The strategy to use to consume objects from Azure Storage.
    #[configurable(metadata(docs::hidden))]
    strategy: Strategy,

    /// Configuration options for Storage Queue.
    queue: Option<queue::Config>,

    /// The Azure Blob Storage Account connection string.
    ///
    /// Authentication with access key is the only supported authentication method.
    ///
    /// Either `storage_account`, or this field, must be specified.
    #[configurable(metadata(
        docs::examples = "DefaultEndpointsProtocol=https;AccountName=mylogstorage;AccountKey=storageaccountkeybase64encoded;EndpointSuffix=core.windows.net"
    ))]
    pub connection_string: Option<SensitiveString>,

    /// The Azure Blob Storage Account name.
    ///
    /// Attempts to load credentials for the account in the following ways, in order:
    ///
    /// - read from environment variables ([more information][env_cred_docs])
    /// - looks for a [Managed Identity][managed_ident_docs]
    /// - uses the `az` CLI tool to get an access token ([more information][az_cli_docs])
    ///
    /// Either `connection_string`, or this field, must be specified.
    ///
    /// [env_cred_docs]: https://docs.rs/azure_identity/latest/azure_identity/struct.EnvironmentCredential.html
    /// [managed_ident_docs]: https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview
    /// [az_cli_docs]: https://docs.microsoft.com/en-us/cli/azure/account?view=azure-cli-latest#az-account-get-access-token
    #[configurable(metadata(docs::examples = "mylogstorage"))]
    pub storage_account: Option<String>,

    /// The Azure Blob Storage Endpoint URL.
    ///
    /// This is used to override the default blob storage endpoint URL in cases where you are using
    /// credentials read from the environment/managed identities or access tokens without using an
    /// explicit connection_string (which already explicitly supports overriding the blob endpoint
    /// URL).
    ///
    /// This may only be used with `storage_account` and is ignored when used with
    /// `connection_string`.
    #[configurable(metadata(docs::examples = "https://test.blob.core.usgovcloudapi.net/"))]
    #[configurable(metadata(docs::examples = "https://test.blob.core.windows.net/"))]
    pub endpoint: Option<String>,

    /// The Azure Blob Storage Account container name.
    #[configurable(metadata(docs::examples = "my-logs"))]
    pub(super) container_name: String,

    #[configurable(derived)]
    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: SourceAcknowledgementsConfig,

    #[configurable(derived)]
    #[serde(default = "default_decoding")]
    #[derivative(Default(value = "default_decoding()"))]
    pub decoding: DeserializerConfig,
}

impl_generate_config_from_default!(AzureBlobConfig);

impl AzureBlobConfig {
    /// Self validation
    pub fn validate(&self) -> crate::Result<()> {
        if self.exec_interval_secs == 0 {
            return Err("exec_interval_secs must be greater than 0".into());
        }
        Ok(())
    }

    async fn create_queue_ingestor(&self) -> crate::Result<queue::Ingestor> {
        let container_client = self.make_container_client();
        match self.queue {
            Some(ref q) => {
                let queue_client = azure::build_queue_client(
                    self.connection_string
                        .as_ref()
                        .map(|v| v.inner().to_string()),
                    self.storage_account.as_ref().map(|v| v.to_string()),
                    q.queue_name.clone(),
                    self.endpoint.clone(),
                )
                .expect("Failed builing queue client");
                let ingestor = queue::Ingestor::new(
                    Arc::clone(&container_client),
                    Arc::clone(&queue_client),
                    q.clone(),
                )
                .await
                .expect("Failed creating ingestor");

                Ok(ingestor)
            }
            None => Err(CreateQueueIngestorError::ConfigMissing {}.into()),
        }
    }

    fn make_container_client(&self) -> Arc<azure_storage_blobs::prelude::ContainerClient> {
        azure::build_container_client(
            self.connection_string
                .as_ref()
                .map(|v| v.inner().to_string()),
            self.storage_account.as_ref().map(|v| v.to_string()),
            self.container_name.clone(),
            self.endpoint.clone(),
        )
        .expect("Failed builing container client")
    }

    fn make_queue_client(&self) -> Arc<azure_storage_queues::QueueClient> {
        let q = self.queue.clone().unwrap();
        azure::build_queue_client(
            self.connection_string
                .as_ref()
                .map(|v| v.inner().to_string()),
            self.storage_account.as_ref().map(|v| v.to_string()),
            q.queue_name.clone(),
            self.endpoint.clone(),
        )
        .expect("Failed builing queue client")
    }
}

type BlobStream = Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>;

async fn run_scheduled(
    config: AzureBlobConfig,
    _: ShutdownSignal,
    mut out: SourceSender,
    log_namespace: LogNamespace,
    kwapik_stream: BlobStream,
) -> Result<(), ()> {
    debug!("Starting test kwapik stream run.");

    let framing = FramingConfig::NewlineDelimited(NewlineDelimitedDecoderConfig {
        newline_delimited: NewlineDelimitedDecoderOptions { max_length: None },
    });
    // TODO: tidy decoder ownership
    let decoder = DecodingConfig::new(framing, config.decoding.clone(), log_namespace)
        .build()
        .unwrap();

    let mut output_stream = kwapik_stream.flat_map(|row: Vec<u8>| {
        // convert row to bytes::Bytes

        let (events, _) = decoder.deserializer_parse(Bytes::from(row)).unwrap();
        let events = events.into_iter().map(|mut event| match event {
            Event::Log(ref mut log_event) => {
                log_namespace.insert_source_metadata(
                    AzureBlobConfig::NAME,
                    log_event,
                    Some(LegacyKey::Overwrite("ingest_timestamp")),
                    path!("ingest_timestamp"),
                    chrono::Utc::now().to_rfc3339(),
                );
                event
            }
            _ => panic!("TODO: this should never happen, handle this gracefully"),
        });
        futures::stream::iter(events)
    });
    out.send_event_stream(&mut output_stream).await.unwrap();

    Ok(())
}

#[derive(Debug, Snafu)]
enum CreateQueueIngestorError {
    #[snafu(display("Configuration for `queue` required when strategy=queue"))]
    ConfigMissing,
}

#[async_trait::async_trait]
#[typetag::serde(name = "azure_blob")]
impl SourceConfig for AzureBlobConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        self.validate().unwrap();
        let log_namespace = cx.log_namespace(self.log_namespace);
        let self_clone = self.clone();
        match self.strategy {
            Strategy::Test => {
                let queue_client = self.make_queue_client();
                let container_client = self.make_container_client();
                let kwapik_stream = make_kwapik_stream(container_client,queue_client);
                Ok(Box::pin(run_scheduled(
                    self_clone,
                    cx.shutdown,
                    cx.out.clone(),
                    log_namespace,
                    kwapik_stream,
                )))
            },
            Strategy::StorageQueue => Ok(Box::pin(self.create_queue_ingestor().await?.run(
                cx,
                self.acknowledgements,
                log_namespace,
            ))),
        }
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let schema_definition = self
            .decoding
            .schema_definition(log_namespace)
            .with_standard_vector_source_metadata();

        vec![SourceOutput::new_logs(
            self.decoding.output_type(),
            schema_definition,
        )]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

fn default_exec_interval_secs() -> u64 {
    1
}
