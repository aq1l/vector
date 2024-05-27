use std::pin::Pin;
use std::time::Duration;

use async_stream::stream;
use bytes::Bytes;
use futures::stream::StreamExt;
use futures::Stream;
use tokio::time;
use tokio_stream::wrappers::IntervalStream;
use vrl::path;

use vector_lib::codecs::decoding::{
    DeserializerConfig, FramingConfig, NewlineDelimitedDecoderOptions,
};
use vector_lib::codecs::NewlineDelimitedDecoderConfig;
use vector_lib::config::LegacyKey;
use vector_lib::sensitive_string::SensitiveString;

use crate::codecs::DecodingConfig;
use crate::config::{
    LogNamespace, SourceAcknowledgementsConfig, SourceConfig, SourceContext, SourceOutput,
};
use crate::event::Event;
use crate::serde::{bool_or_struct, default_decoding};
use crate::shutdown::ShutdownSignal;
use crate::sinks::prelude::configurable_component;
use crate::sources::azure_blob::queue::make_azure_row_stream;
use crate::SourceSender;

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
        match self.strategy {
            Strategy::StorageQueue => {
                if self.queue.is_none() || self.queue.as_ref().unwrap().queue_name.is_empty() {
                    return Err("Azure event grid queue must be set.".into());
                }
                if self.storage_account.clone().unwrap_or_default().is_empty() {
                    return Err("Azure Storage Account must be set.".into());
                }
                if self.container_name.is_empty() {
                    return Err("Azure Container must be set.".into());
                }
            }
            Strategy::Test => {
                if self.exec_interval_secs == 0 {
                    return Err("exec_interval_secs must be greater than 0".into());
                }
            }
        }

        Ok(())
    }
}

type BlobStream = Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>;

async fn run_streaming(
    config: AzureBlobConfig,
    _: ShutdownSignal,
    mut out: SourceSender,
    log_namespace: LogNamespace,
    mut row_stream: BlobStream,
) -> Result<(), ()> {
    debug!("Starting Azure streaming.");

    let framing = FramingConfig::NewlineDelimited(NewlineDelimitedDecoderConfig {
        newline_delimited: NewlineDelimitedDecoderOptions { max_length: None },
    });
    let decoder = DecodingConfig::new(framing, config.decoding.clone(), log_namespace)
        .build()
        .map_err(|e| {
            error!("Failed to build decoder: {}", e);
            ()
        })?;

    // TODO: use filter_map to handle errors
    let mut output_stream = stream! {
        while let Some(row) = row_stream.next().await {
            let deser_result = decoder.deserializer_parse(Bytes::from(row));
            if deser_result.is_err(){
                continue;
            }
            let (events, _) = deser_result.unwrap();
            for mut event in events.into_iter(){
                match event {
                    Event::Log(ref mut log_event) => {
                        log_namespace.insert_source_metadata(
                            AzureBlobConfig::NAME,
                            log_event,
                            Some(LegacyKey::Overwrite("ingest_timestamp")),
                            path!("ingest_timestamp"),
                            chrono::Utc::now().to_rfc3339(),
                        );
                        yield event
                    }
                    _ => {
                        error!("Expected Azure rows as Log Events, but got {:?}.", event);
                    }
                }
            }
        }
    }.boxed();
    out.send_event_stream(&mut output_stream)
        .await
        .map_err(|e| {
            error!("Failed to build decoder: {}", e);
            ()
        })?;

    Ok(())
}

#[async_trait::async_trait]
#[typetag::serde(name = "azure_blob")]
impl SourceConfig for AzureBlobConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        self.validate()?;
        let log_namespace = cx.log_namespace(self.log_namespace);
        let self_clone = self.clone();

        let row_stream = match self.strategy {
            Strategy::Test => {
                // streaming incremented numbers periodically
                let exec_interval_secs = self.exec_interval_secs;
                let shutdown = cx.shutdown.clone();
                stream! {
                    let schedule = Duration::from_secs(exec_interval_secs);
                    let mut counter = 0;
                    let mut interval = IntervalStream::new(time::interval(schedule)).take_until(shutdown);
                    while interval.next().await.is_some() {
                        counter += 1;
                        // yield counter string as Vec<u8>
                        yield counter.to_string().into_bytes();
                    }
                }.boxed()
            }
            Strategy::StorageQueue => make_azure_row_stream(self)?,
        };
        Ok(Box::pin(run_streaming(
            self_clone,
            cx.shutdown,
            cx.out.clone(),
            log_namespace,
            row_stream,
        )))
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
