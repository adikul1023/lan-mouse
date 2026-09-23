use std::pin::Pin;
use futures::{Stream, StreamExt};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use ashpd::desktop::Session;

use super::protocol::ClipboardMessage;
use super::{CLIPBOARD_INCOMING, CLIPBOARD_OUTGOING};

use crate::clipboard::task::{ClipboardPortal, ClipboardTask};

pub struct AshpdClipboardPortal {
    clipboard: std::sync::Arc<ashpd::desktop::clipboard::Clipboard>,
    session: std::sync::Arc<Session<ashpd::desktop::input_capture::InputCapture>>,
}

impl AshpdClipboardPortal {
    pub async fn new(session: std::sync::Arc<Session<ashpd::desktop::input_capture::InputCapture>>) -> Result<Self, String> {
        let clipboard = ashpd::desktop::clipboard::Clipboard::new()
            .await
            .map_err(|e| e.to_string())?;
        Ok(Self { clipboard: std::sync::Arc::new(clipboard), session })
    }
}

impl ClipboardPortal for AshpdClipboardPortal {
    fn receive_selection_owner_changed(&self) -> Pin<Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>> {
        let clipboard = self.clipboard.clone();
        Box::pin(async move {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tokio::task::spawn(async move {
                if let Ok(stream) = clipboard.receive_selection_owner_changed::<ashpd::desktop::input_capture::InputCapture>().await {
                    tokio::pin!(stream);
                    while let Some(_) = stream.next().await {
                        if tx.send(()).await.is_err() {
                            break;
                        }
                    }
                }
            });
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)) as Pin<Box<dyn Stream<Item = ()> + Send>>
        })
    }

    fn receive_selection_transfer(&self) -> Pin<Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>> {
        let clipboard = self.clipboard.clone();
        Box::pin(async move {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tokio::task::spawn(async move {
                if let Ok(stream) = clipboard.receive_selection_transfer::<ashpd::desktop::input_capture::InputCapture>().await {
                    tokio::pin!(stream);
                    while let Some(_) = stream.next().await {
                        if tx.send(()).await.is_err() {
                            break;
                        }
                    }
                }
            });
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)) as Pin<Box<dyn Stream<Item = ()> + Send>>
        })
    }

    fn selection_write<'a>(&'a self, _mime: &'a str, _data: Vec<u8>) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move { Ok(()) })
    }

    fn selection_read<'a>(&'a self, _mime: &'a str) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send + 'a>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn set_selection<'a>(&'a self, mime: &'a str) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move { Ok(()) })
    }
}

pub fn init_clipboard_task() {
    tokio::task::spawn_local(async {
        let session_rx = input_capture::libei::get_clipboard_session_rx();
        ClipboardTask::run_with_factory(session_rx, |session| async move {
            match AshpdClipboardPortal::new(session).await {
                Ok(portal) => Some(Box::new(portal) as Box<dyn ClipboardPortal>),
                Err(e) => {
                    log::warn!("Failed to create ashpd Clipboard proxy: {}", e);
                    None
                }
            }
        }).await
    });
}
