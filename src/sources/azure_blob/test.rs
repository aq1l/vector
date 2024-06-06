use super::*;
use crate::{
    config::LogNamespace, event::EventStatus, serde::default_decoding, shutdown::ShutdownSignal,
    test_util::collect_n, SourceSender,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[tokio::test]
async fn test_messages_delivered() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        default_decoding(),
    );
    let mut streamer = streamer.expect("Failed to create streamer");
    let success_called = Arc::new(AtomicBool::new(false));
    let success_called_clone = success_called.clone();
    let blob_pack = BlobPack {
        row_stream: Box::pin(stream! {
            let lines = vec!["foo", "bar"];
            for line in lines {
                yield line.as_bytes().to_vec();
            }
        }),
        success_handler: Box::new(move || {
            let success_called_clone = success_called_clone.clone();
            Box::pin(async move {
                success_called_clone.store(true, Ordering::SeqCst);
            })
        }),
    };
    streamer
        .process_blob_pack(blob_pack)
        .await
        .expect("Failed processing blob pack");

    let events = collect_n(rx, 2).await;
    assert_eq!(events[0].as_log().value().to_string(), "\"foo\"");
    assert_eq!(events[1].as_log().value().to_string(), "\"bar\"");
    assert!(
        success_called.load(Ordering::SeqCst),
        "Success handler was not called"
    );
}

#[tokio::test]
async fn test_messages_rejected() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Rejected);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        default_decoding(),
    );
    let mut streamer = streamer.expect("Failed to create streamer");
    let success_called = Arc::new(AtomicBool::new(false));
    let success_called_clone = success_called.clone();
    let blob_pack = BlobPack {
        row_stream: Box::pin(stream! {
            let lines = vec!["foo", "bar"];
            for line in lines {
                yield line.as_bytes().to_vec();
            }
        }),
        success_handler: Box::new(move || {
            let success_called_clone = success_called_clone.clone();
            Box::pin(async move {
                success_called_clone.store(true, Ordering::SeqCst);
            })
        }),
    };
    streamer
        .process_blob_pack(blob_pack)
        .await
        .expect("Failed processing blob pack");

    let events = collect_n(rx, 2).await;
    assert_eq!(events[0].as_log().value().to_string(), "\"foo\"");
    assert_eq!(events[1].as_log().value().to_string(), "\"bar\"");
    assert!(
        !success_called.load(Ordering::SeqCst),
        "Success handler was called"
    );
}
