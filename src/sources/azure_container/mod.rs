use std::time::Duration;
use futures_util::StreamExt;
use tokio::time;
use tokio_stream::wrappers::IntervalStream;
use vector_lib::event::{EventMetadata, LogEvent};
use crate::config::{LogNamespace, SourceConfig, SourceContext, SourceOutput};
use crate::event::Event;
use crate::shutdown::ShutdownSignal;
use crate::sinks::prelude::configurable_component;
use crate::SourceSender;


/// WIP
/// A dummy implementation is used as a starter.
/// The source will send dummy messages at a fixed interval, incrementing a counter every
/// exec_interval_secs seconds.
#[configurable_component(source("azure_container", "Collect logs from Azure Container."))]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(default, deny_unknown_fields)]
pub struct AzureContainerConfig {
    // TODO : everything

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    log_namespace: Option<bool>,

    /// The interval, in seconds, between subsequent dummy messages
    #[serde(default = "default_exec_interval_secs")]
    exec_interval_secs: u64,
}

impl_generate_config_from_default!(AzureContainerConfig);

impl AzureContainerConfig {
    /// Self validation
    pub fn validate(&self) -> crate::Result<()> {
        if self.exec_interval_secs == 0 {
            return Err("exec_interval_secs must be greater than 0".into());
        }
        Ok(())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "azure_container")]
impl SourceConfig for AzureContainerConfig {
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

    fn outputs(&self, _: LogNamespace) -> Vec<SourceOutput> {
        unimplemented!()
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

fn default_exec_interval_secs() -> u64 { 1 }

async fn run_scheduled(
    _: AzureContainerConfig,
    exec_interval_secs: u64,
    shutdown: ShutdownSignal,
    out: SourceSender,
    _: LogNamespace,
) -> Result<(), ()> {
    debug!("Starting scheduled exec runs.");
    let schedule = Duration::from_secs(exec_interval_secs);

    let mut counter = 0;
    let mut interval = IntervalStream::new(time::interval(schedule)).take_until(shutdown.clone());
    while interval.next().await.is_some() {
        counter += 1;

        let log_event = LogEvent::from_map(btreemap! {
            "message" => counter.to_string(),
        }, EventMetadata::default().into());
        let event = Event::Log(log_event);
        out.clone().send_event(event).await.unwrap();
    }
    Ok(())
}

