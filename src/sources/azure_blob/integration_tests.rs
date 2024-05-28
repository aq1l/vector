use azure_core::error::HttpError;
use azure_storage_blobs::prelude::PublicAccess;
use http::StatusCode;

use super::{
    queue::{make_container_client, make_queue_client, Config},
    time::Duration,
    AzureBlobConfig, Strategy,
};
use crate::{
    event::Event,
    serde::default_decoding,
    test_util::components::{run_and_assert_source_compliance, SOURCE_TAGS},
};

impl AzureBlobConfig {
    pub async fn new_emulator() -> AzureBlobConfig {
        let address = std::env::var("AZURE_ADDRESS").unwrap_or_else(|_| "localhost".into());
        let config = AzureBlobConfig {
                connection_string: Some(format!("UseDevelopmentStorage=true;DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{}:10000/devstoreaccount1;QueueEndpoint=http://{}:10001/devstoreaccount1;TableEndpoint=http://{}:10002/devstoreaccount1;", address, address, address).into()),
                storage_account: None,
                container_name: "logs".to_string(),
                strategy: Strategy::StorageQueue,
                queue: Some(Config {
                    queue_name: "myqueue".to_string(),
                }),
                // TODO shouldn't we have blob_endpoint and queue_endpoint?
                endpoint: None,
                acknowledgements: Default::default(),
                // TODO this should be option
                exec_interval_secs: 0,
                log_namespace: None,
                decoding: default_decoding(),
            };

        config.ensure_container().await;
        config.ensure_queue().await;

        config
    }

    async fn run_assert(&self) -> Vec<Event> {
        run_and_assert_source_compliance(self.clone(), Duration::from_secs(2), &SOURCE_TAGS).await
    }

    async fn ensure_container(&self) {
        let client = make_container_client(self).expect("Failed to create container client");
        let request = client
            .create()
            .public_access(PublicAccess::None)
            .into_future();

        let response = match request.await {
            Ok(_) => Ok(()),
            Err(reason) => match reason.downcast_ref::<HttpError>() {
                Some(err) => match StatusCode::from_u16(err.status().into()) {
                    Ok(StatusCode::CONFLICT) => Ok(()),
                    _ => Err(format!("Unexpected status code {}", err.status())),
                },
                _ => Err(format!("Unexpected error {}", reason)),
            },
        };

        response.expect("Failed to create container")
    }

    async fn ensure_queue(&self) {
        let client = make_queue_client(self).expect("Failed to create queue client");
        let request = client.create().into_future();

        let response = match request.await {
            Ok(_) => Ok(()),
            Err(reason) => match reason.downcast_ref::<HttpError>() {
                Some(err) => match StatusCode::from_u16(err.status().into()) {
                    Ok(StatusCode::CONFLICT) => Ok(()),
                    _ => Err(format!("Unexpected status code {}", err.status())),
                },
                _ => Err(format!("Unexpected error {}", reason)),
            },
        };

        response.expect("Failed to create queue")
    }
}

#[tokio::test]
async fn azure_blob_read_lines_from_blob() {
    let config = AzureBlobConfig::new_emulator().await;

    let _events = config.run_assert().await;
    assert_eq!(1, 2);
}
