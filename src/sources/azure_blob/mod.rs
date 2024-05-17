use std::time::Duration;
use futures_util::StreamExt;
use tokio::time;
use tokio_stream::wrappers::IntervalStream;
use vrl::{owned_value_path, path};
use vrl::prelude::Kind;
use vector_lib::codecs::SyslogDeserializerConfig;
use vector_lib::config::LegacyKey;
use vector_lib::event::{EventMetadata, LogEvent};
use crate::config::{DataType, LogNamespace, SourceConfig, SourceContext, SourceOutput};
use crate::event::Event;
use crate::shutdown::ShutdownSignal;
use crate::sinks::prelude::configurable_component;
use crate::SourceSender;


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
        let log_namespace = cx.log_namespace(self.log_namespace);

        Ok(Box::pin(run_scheduled(
            self.clone(),
            self.exec_interval_secs,
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
                Some(LegacyKey::Overwrite(owned_value_path!("some_additional_metadata"))),
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

fn default_exec_interval_secs() -> u64 { 1 }

async fn run_scheduled(
    _: AzureBlobConfig,
    exec_interval_secs: u64,
    shutdown: ShutdownSignal,
    out: SourceSender,
    log_namespace: LogNamespace,
) -> Result<(), ()> {
    debug!("Starting scheduled exec runs.");
    let schedule = Duration::from_secs(exec_interval_secs);

    let mut counter = 0;
    let mut interval = IntervalStream::new(time::interval(schedule)).take_until(shutdown.clone());
    while interval.next().await.is_some() {
        counter += 1;

        let mut log_event = LogEvent::from_map(btreemap! {
            "message" => counter.to_string(),
        }, EventMetadata::default().into());
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

