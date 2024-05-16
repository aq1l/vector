use crate::azure;
use crate::config::{DataType, LogNamespace, SourceConfig, SourceContext, SourceOutput};
use crate::event::Event;
use crate::shutdown::ShutdownSignal;
use crate::sinks::prelude::configurable_component;
use crate::SourceSender;
use azure_storage_blobs::prelude::*;
use futures_util::StreamExt;
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

// pub mod queue;

/// Strategies for consuming objects from Azure Storage.
#[configurable_component]
#[derive(Clone, Copy, Debug, Derivative)]
#[serde(rename_all = "lowercase")]
#[derivative(Default)]
enum Strategy {
    /// Consumes objects by processing events sent to an [Azure Storage Queue][azure_storage_queue].
    ///
    /// [azure_storage_queue]: https://learn.microsoft.com/en-us/azure/storage/queues/storage-queues-introduction
    #[derivative(Default)]
    StorageQueue,
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

    // /// Configuration options for Storage Queue.
    // queue: Option<queue::Config>,
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
}

#[async_trait::async_trait]
#[typetag::serde(name = "azure_blob")]
impl SourceConfig for AzureBlobConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        self.validate().unwrap();
        let client = azure::build_container_client(
            self.connection_string
                .as_ref()
                .map(|v| v.inner().to_string()),
            self.storage_account.as_ref().map(|v| v.to_string()),
            self.container_name.clone(),
            self.endpoint.clone(),
        )?;
        let log_namespace = cx.log_namespace(self.log_namespace);

        Ok(Box::pin(run_scheduled(
            self.clone(),
            self.exec_interval_secs,
            Arc::clone(&client),
            cx.shutdown,
            cx.out,
            log_namespace,
        )))
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let schema_definition = SyslogDeserializerConfig::from_source(Self::NAME)
            .schema_definition(log_namespace)
            .with_standard_vector_source_metadata()
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!(
                    "some_additional_metadata"
                ))),
                &owned_value_path!("some_additional_metadata"),
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

async fn run_scheduled(
    _: AzureBlobConfig,
    exec_interval_secs: u64,
    client: Arc<ContainerClient>,
    shutdown: ShutdownSignal,
    out: SourceSender,
    log_namespace: LogNamespace,
) -> Result<(), ()> {
    debug!("Starting scheduled exec runs.");
    let schedule = Duration::from_secs(exec_interval_secs);

    let blob_client = client.blob_client("foo");
    let blob_content_result = blob_client.get_content().await;
    match blob_content_result {
        Ok(content) => info!("\tBlob content: {:#?}", content),
        Err(error) => info!("Failed {:#?}", error),
    };
    let mut counter = 0;
    let mut interval = IntervalStream::new(time::interval(schedule)).take_until(shutdown.clone());
    while interval.next().await.is_some() {
        counter += 1;

        let mut log_event = LogEvent::from_map(
            btreemap! {
                "message" => counter.to_string(),
            },
            EventMetadata::default().into(),
        );
        log_namespace.insert_source_metadata(
            AzureBlobConfig::NAME,
            &mut log_event,
            Some(LegacyKey::Overwrite("some_additional_metadata")),
            path!("some_additional_metadata"),
            "some content".to_owned(),
        );
        let event = Event::Log(log_event);

        out.clone().send_event(event).await.unwrap();
    }
    Ok(())
}
