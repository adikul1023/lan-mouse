use std::pin::Pin;
use std::sync::{Arc, Mutex};

use clipboard_win::formats;
use futures::Stream;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

use super::task::ClipboardPortal;

#[link(name = "user32")]
extern "system" {
    fn GetClipboardSequenceNumber() -> u32;
}

struct EchoSuppressor {
    expected_sequence_number: Option<u32>,
    is_writing: bool,
}

impl EchoSuppressor {
    fn new() -> Self {
        Self {
            expected_sequence_number: None,
            is_writing: false,
        }
    }

    fn prepare_write(&mut self) {
        self.is_writing = true;
    }

    fn record_write(&mut self) {
        self.expected_sequence_number = Some(unsafe { GetClipboardSequenceNumber() });
        self.is_writing = false;
    }

    fn check_and_clear_echo(&mut self) -> bool {
        if self.is_writing {
            return true;
        }
        if let Some(expected) = self.expected_sequence_number {
            let current = unsafe { GetClipboardSequenceNumber() };
            if expected == current {
                self.expected_sequence_number = None;
                return true;
            }
            // If the current sequence number is different but we expected one,
            // we should still clear it so we don't accidentally suppress a future update
            // that happens to wrap around or hit that number.
            self.expected_sequence_number = None;
        }
        false
    }
}

pub struct WindowsClipboardPortal {
    owner_changed_rx: Arc<tokio::sync::Mutex<Option<mpsc::Receiver<()>>>>,
    suppressor: Arc<Mutex<EchoSuppressor>>,
}

impl WindowsClipboardPortal {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(4);
        let suppressor = Arc::new(Mutex::new(EchoSuppressor::new()));
        let suppressor_clone = suppressor.clone();

        // Spawn a dedicated thread for the clipboard monitor since it uses a hidden window
        // and its `recv()` blocks the thread.
        std::thread::spawn(move || {
            let mut monitor = match clipboard_win::Monitor::new() {
                Ok(m) => m,
                Err(e) => {
                    log::error!("Failed to create Windows clipboard monitor: {e}");
                    return;
                }
            };

            log::info!("Windows clipboard monitor started.");

            // Wait for clipboard events
            while let Ok(true) = monitor.recv() {
                // When we receive an event, check if it's an echo.
                let mut is_echo = false;

                log::info!("[DEBUG WINDOWS] WM_CLIPBOARDUPDATE received");

                if let Ok(mut supp) = suppressor_clone.lock() {
                    is_echo = supp.check_and_clear_echo();
                }

                if is_echo {
                    log::info!(
                        "[DEBUG WINDOWS] WM_CLIPBOARDUPDATE suppressed as our own write (echo)"
                    );
                    continue;
                } else {
                    log::info!("[DEBUG WINDOWS] WM_CLIPBOARDUPDATE accepted as external change");
                }

                // If not an echo, notify the task
                if tx.blocking_send(()).is_err() {
                    log::info!(
                        "[DEBUG WINDOWS] Clipboard portal owner_changed channel closed, terminating monitor."
                    );
                    break;
                }
            }
        });

        Self {
            owner_changed_rx: Arc::new(tokio::sync::Mutex::new(Some(rx))),
            suppressor,
        }
    }
}

impl ClipboardPortal for WindowsClipboardPortal {
    fn eager_fetch(&self) -> bool {
        // Windows V1 uses eager fetching since we don't support true delayed rendering yet.
        true
    }

    fn receive_selection_owner_changed(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>,
    > {
        Box::pin(async move {
            let rx = self.owner_changed_rx.lock().await.take().unwrap();
            Box::pin(ReceiverStream::new(rx)) as Pin<Box<dyn Stream<Item = ()> + Send>>
        })
    }

    fn receive_selection_transfer(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>,
    > {
        Box::pin(async move {
            // Because we use eager fetching, we never emit events from this stream.
            // The ClipboardTask will fetch data immediately on Offer.
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Box::pin(ReceiverStream::new(rx)) as Pin<Box<dyn Stream<Item = ()> + Send>>
        })
    }

    fn selection_write<'a>(
        &'a self,
        mime: &'a str,
        data: Vec<u8>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            log::info!(
                "[DEBUG WINDOWS] selection_write() invoked with length {}",
                data.len()
            );
            if mime != "text/plain" {
                return Err(format!("Unsupported MIME type: {}", mime));
            }

            let text = String::from_utf8(data).map_err(|e| e.to_string())?;

            if let Ok(mut supp) = self.suppressor.lock() {
                supp.prepare_write();
            }

            tokio::task::spawn_blocking(move || {
                clipboard_win::set_clipboard(formats::Unicode, text)
            })
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;

            if let Ok(mut supp) = self.suppressor.lock() {
                supp.record_write();
                log::info!(
                    "[DEBUG WINDOWS] clipboard sequence number recorded after write: {:?}",
                    supp.expected_sequence_number
                );
            }

            Ok(())
        })
    }

    fn selection_read<'a>(
        &'a self,
        mime: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send + 'a>> {
        Box::pin(async move {
            log::info!("[DEBUG WINDOWS] selection_read() invoked for mime {}", mime);
            if mime != "text/plain" {
                return Err(format!("Unsupported MIME type: {}", mime));
            }

            let text: String =
                tokio::task::spawn_blocking(|| clipboard_win::get_clipboard(formats::Unicode))
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| e.to_string())?;

            // Apply limit (account for 13 bytes framing overhead)
            let max_payload = (crate::clipboard::transport::MAX_CLIPBOARD_FRAME_SIZE - 13) as usize;
            if text.len() > max_payload {
                return Err("Clipboard text exceeds 10MB limit".to_string());
            }

            let data = text.into_bytes();
            log::info!(
                "[DEBUG WINDOWS] text length successfully read: {}",
                data.len()
            );
            Ok(data)
        })
    }

    fn set_selection<'a>(
        &'a self,
        _mime: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            // No-op for Windows since we don't own the selection without data (no delayed rendering).
            Ok(())
        })
    }
}

pub fn init_clipboard_task() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (tx, rx) = watch::channel(Some(()));
        // We hold the tx so it doesn't close, even though it never updates.
        let _tx = tx;
        super::task::ClipboardTask::run_with_factory(rx, |_| async {
            Some(Box::new(WindowsClipboardPortal::new()) as Box<dyn ClipboardPortal>)
        })
        .await;
    })
}
