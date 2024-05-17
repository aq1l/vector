use crate::azure;
use crate::config::{
    DataType, LogNamespace, SourceAcknowledgementsConfig, SourceConfig, SourceContext, SourceOutput,
};
use crate::event::Event;
use crate::serde::bool_or_struct;
use crate::shutdown::ShutdownSignal;
use crate::sinks::prelude::configurable_component;
use crate::SourceSender;
use async_stream::stream;
use futures_util::StreamExt;
use snafu::Snafu;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tokio_stream::wrappers::IntervalStream;
use vector_lib::codecs::SyslogDeserializerConfig;
use vector_lib::config::LegacyKey;
use vector_lib::event::{EventMetadata, LogEvent};
use vector_lib::sensitive_string::SensitiveString;
use vrl::prelude::Kind;
use vrl::{owned_value_path, path};

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
        let container_client = azure::build_container_client(
            self.connection_string
                .as_ref()
                .map(|v| v.inner().to_string()),
            self.storage_account.as_ref().map(|v| v.to_string()),
            self.container_name.clone(),
            self.endpoint.clone(),
        )
        .expect("Failed builing container client");
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

        match self.strategy {
            Strategy::Test => Ok(Box::pin(run_scheduled(
                self.clone(),
                self.exec_interval_secs,
                cx.shutdown,
                cx.out.clone(),
                log_namespace,
            ))),
            Strategy::StorageQueue => Ok(Box::pin(self.create_queue_ingestor().await?.run(
                cx,
                self.acknowledgements,
                log_namespace,
            ))),
        }
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let schema_definition = SyslogDeserializerConfig::from_source(Self::NAME)
            .schema_definition(log_namespace)
            .with_standard_vector_source_metadata()
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!(
                    "timestamp"
                ))),
                &owned_value_path!("timestamp"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!(
                    "blob_name"
                ))),
                &owned_value_path!("blob_name"),
                Kind::bytes(),
                None,
            );

        vec![SourceOutput::new_logs(DataType::Log, schema_definition)]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

fn default_exec_interval_secs() -> u64 {
    1
}


struct BlobRow {
    payload: String,
    timestamp: String, // TODO: use some proper timestampt type
    blob_name: String, // instead of repeating the filename for each row, we could have
    // sth like Stream<Item = (Stream<Item = BlobRow>, filename, any_file_related_data_common_across_all_streamed_rows)>
}

async fn run_scheduled(
    _: AzureBlobConfig,
    exec_interval_secs: u64,
    shutdown: ShutdownSignal,
    mut out: SourceSender,
    log_namespace: LogNamespace,
) -> Result<(), ()> {
    debug!("Starting scheduled exec runs.");

    // we could also use a stream of stream, see BlobRow comment
    let mut kwapik_stream= Box::pin(stream! {
        let schedule = Duration::from_secs(exec_interval_secs);
        let mut counter = 0;
        let mut interval = IntervalStream::new(time::interval(schedule)).take_until(shutdown.clone());
        while interval.next().await.is_some() {
            counter += 1;
            yield BlobRow {
                payload: counter.to_string(),
                timestamp: chrono::Utc::now().to_rfc3339(),
                blob_name: "some_blob".to_string(),
            };
        }
    });

    let output_stream = Box::pin(stream! {
        while let Some(row) = kwapik_stream.next().await {
            let mut log_event = LogEvent::from_map(
                btreemap! {
                    "message" => row.payload,
                },
                EventMetadata::default().into(),
            );
            log_namespace.insert_source_metadata(
                AzureBlobConfig::NAME,
                &mut log_event,
                Some(LegacyKey::Overwrite("timestamp")),
                path!("timestamp"),
                row.timestamp.to_owned(),
            );
            log_namespace.insert_source_metadata(
                AzureBlobConfig::NAME,
                &mut log_event,
                Some(LegacyKey::Overwrite("blob_name")),
                path!("blob_name"),
                row.blob_name.to_owned(),
            );
            yield Event::Log(log_event);
        }
    });
    out.send_event_stream(output_stream).await.unwrap();

    Ok(())
}
