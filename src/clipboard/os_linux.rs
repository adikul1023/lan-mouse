use std::pin::Pin;
use futures_util::{Stream, StreamExt};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use ashpd::desktop::Session;

use super::protocol::ClipboardMessage;
use super::{CLIPBOARD_INCOMING, CLIPBOARD_OUTGOING};

use crate::clipboard::task::{ClipboardPortal, ClipboardTask};

pub struct AshpdClipboardPortal {
    clipboard: ashpd::desktop::clipboard::Clipboard,
    session: Session<ashpd::desktop::input_capture::InputCapture>,
}

impl AshpdClipboardPortal {
    pub async fn new(session: Session<ashpd::desktop::input_capture::InputCapture>) -> Result<Self, String> {
        let clipboard = ashpd::desktop::clipboard::Clipboard::new()
            .await
            .map_err(|e| e.to_string())?;
        Ok(Self { clipboard, session })
    }
}

impl ClipboardPortal for AshpdClipboardPortal {
    fn receive_selection_owner_changed(&self) -> Pin<Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>> {
        Box::pin(async move {
            match self.clipboard.receive_selection_owner_changed(&self.session).await {
                Ok(stream) => Box::pin(stream.map(|_| ())) as Pin<Box<dyn Stream<Item = ()> + Send>>,
                Err(_) => Box::pin(futures::stream::empty()) as Pin<Box<dyn Stream<Item = ()> + Send>>,
            }
        })
    }

    fn receive_selection_transfer(&self) -> Pin<Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>> {
        Box::pin(async move {
            match self.clipboard.receive_selection_transfer(&self.session).await {
                Ok(stream) => Box::pin(stream.map(|_| ())) as Pin<Box<dyn Stream<Item = ()> + Send>>,
                Err(_) => Box::pin(futures::stream::empty()) as Pin<Box<dyn Stream<Item = ()> + Send>>,
            }
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
        let session_rx = crate::input_capture::libei::get_clipboard_session_rx();
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
