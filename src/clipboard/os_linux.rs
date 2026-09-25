use ashpd::desktop::Session;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::clipboard::task::{ClipboardPortal, ClipboardTask};

pub struct AshpdClipboardPortal {
    clipboard: std::sync::Arc<ashpd::desktop::clipboard::Clipboard>,
    _session: std::sync::Arc<Session<ashpd::desktop::input_capture::InputCapture>>,
}

impl AshpdClipboardPortal {
    pub async fn new(
        session: std::sync::Arc<Session<ashpd::desktop::input_capture::InputCapture>>,
    ) -> Result<Self, String> {
        let clipboard = ashpd::desktop::clipboard::Clipboard::new()
            .await
            .map_err(|e| e.to_string())?;
        Ok(Self {
            clipboard: std::sync::Arc::new(clipboard),
            _session: session,
        })
    }
}

impl ClipboardPortal for AshpdClipboardPortal {
    fn receive_selection_owner_changed(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>,
    > {
        let clipboard = self.clipboard.clone();
        Box::pin(async move {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tokio::task::spawn(async move {
                if let Ok(stream) = clipboard
                    .receive_selection_owner_changed::<ashpd::desktop::input_capture::InputCapture>(
                    )
                    .await
                {
                    tokio::pin!(stream);
                    while let Some(_) = stream.next().await {
                        if tx.send(()).await.is_err() {
                            break;
                        }
                    }
                }
            });
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
                as Pin<Box<dyn Stream<Item = ()> + Send>>
        })
    }

    fn receive_selection_transfer(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>,
    > {
        let clipboard = self.clipboard.clone();
        Box::pin(async move {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tokio::task::spawn(async move {
                if let Ok(stream) = clipboard
                    .receive_selection_transfer::<ashpd::desktop::input_capture::InputCapture>()
                    .await
                {
                    tokio::pin!(stream);
                    while let Some(_) = stream.next().await {
                        if tx.send(()).await.is_err() {
                            break;
                        }
                    }
                }
            });
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
                as Pin<Box<dyn Stream<Item = ()> + Send>>
        })
    }

    fn selection_write<'a>(
        &'a self,
        _items: Vec<super::task::ClipboardItem>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move { Ok(()) })
    }

    fn selection_read<'a>(
        &'a self,
        _mime: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send + 'a>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn set_selection<'a>(
        &'a self,
        _mime: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move { Ok(()) })
    }

    fn get_available_mime_types(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<String>, String>> + Send + '_>> {
        Box::pin(async move { Ok(vec!["text/plain".to_string()]) })
    }

    fn eager_fetch(&self) -> bool {
        true
    }
}

pub struct WlClipboardPortal {
    current_copy_child: std::sync::Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<Result<(), String>>>>>,
}

impl WlClipboardPortal {
    pub fn new() -> Self {
        Self {
            current_copy_child: std::sync::Arc::new(std::sync::Mutex::new(None)),
        }
    }
}

impl ClipboardPortal for WlClipboardPortal {
    fn receive_selection_owner_changed(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>,
    > {
        let current_child = self.current_copy_child.clone();
        Box::pin(async move {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tokio::task::spawn(async move {
                if let Ok(mut child) = tokio::process::Command::new("wl-paste")
                    .arg("--watch")
                    .arg("echo")
                    .arg("changed")
                    .stdout(std::process::Stdio::piped())
                    .spawn()
                {
                    if let Some(mut stdout) = child.stdout.take() {
                        let mut buf = [0u8; 1024];
                        while let Ok(n) = stdout.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }

                            let mut is_echo = false;
                            if let Ok(mut lock) = current_child.try_lock() {
                                if let Some(handle) = lock.as_ref() {
                                    if !handle.is_finished() {
                                        is_echo = true;
                                    } else {
                                        *lock = None;
                                    }
                                }
                            }

                            if is_echo {
                                log::info!(
                                    "[DEBUG LINUX] owner_changed ignored as echo (wl-copy is still running)"
                                );
                                continue;
                            }
                            log::info!("[DEBUG LINUX] owner_changed accepted as external change");

                            if tx.send(()).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
                as Pin<Box<dyn Stream<Item = ()> + Send>>
        })
    }

    fn receive_selection_transfer(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>,
    > {
        Box::pin(async move {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
                as Pin<Box<dyn Stream<Item = ()> + Send>>
        })
    }

    fn selection_write<'a>(
        &'a self,
        items: Vec<super::task::ClipboardItem>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        let child_arc = self.current_copy_child.clone();
        Box::pin(async move {
            let handle = tokio::task::spawn_blocking(move || {
                let mut opts = wl_clipboard_rs::copy::Options::new();
                opts.foreground(true);
                let mut sources = Vec::new();
                for item in items {
                    if item.mime_type.starts_with("image/") {
                        if let Err(e) = super::validate_image_dimensions(&item.data) {
                            log::warn!("Invalid image data: {e}");
                            continue;
                        }
                    }
                    let mime = if item.mime_type == "text/plain" {
                        wl_clipboard_rs::copy::MimeType::Text
                    } else {
                        wl_clipboard_rs::copy::MimeType::Specific(item.mime_type)
                    };
                    sources.push((mime, wl_clipboard_rs::copy::Source::Bytes(item.data.into())));
                }

                if sources.is_empty() {
                    return Ok(());
                }

                opts.copy_multi(sources).map_err(|e| format!("wl-clipboard-rs copy error: {:?}", e))
            });
            
            if let Ok(mut lock) = child_arc.lock() {
                *lock = Some(handle);
            }
            Ok(())
        })
    }

    fn selection_read<'a>(
        &'a self,
        mime: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send + 'a>> {
        let mime_str = mime.to_string();
        Box::pin(async move {
            let mut child = tokio::process::Command::new("wl-paste")
                .arg("--type")
                .arg(&mime_str)
                .arg("--no-newline")
                .stdout(std::process::Stdio::piped())
                .spawn()
                .map_err(|e| e.to_string())?;

            let mut stdout = child.stdout.take().ok_or("Failed to open stdout")?;
            let mut data = Vec::new();
            let mut buf = [0u8; 8192];
            // MAX_CLIPBOARD_FRAME_SIZE includes 13 bytes protocol overhead for Data message
            let max_payload = crate::clipboard::transport::MAX_CLIPBOARD_FRAME_SIZE as usize - 13;

            loop {
                let n = stdout.read(&mut buf).await.map_err(|e| e.to_string())?;
                if n == 0 {
                    break;
                }
                if data.len() + n > max_payload {
                    let _ = child.kill().await;
                    return Err(format!(
                        "Clipboard data exceeds {}MB limit",
                        crate::clipboard::transport::MAX_CLIPBOARD_FRAME_SIZE / (1024 * 1024)
                    ));
                }
                data.extend_from_slice(&buf[..n]);
            }

            let _ = child.wait().await;

            if mime_str.starts_with("image/") {
                super::validate_image_dimensions(&data)?;
            }
            Ok(data)
        })
    }

    fn set_selection<'a>(
        &'a self,
        _mime: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move { Ok(()) })
    }

    fn get_available_mime_types(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<String>, String>> + Send + '_>> {
        Box::pin(async move {
            let output = tokio::process::Command::new("wl-paste")
                .arg("--list-types")
                .output()
                .await
                .map_err(|e| e.to_string())?;

            let mut types = Vec::new();
            if let Ok(s) = String::from_utf8(output.stdout) {
                for line in s.lines() {
                    let t = line.trim();
                    if !t.is_empty() {
                        types.push(t.to_string());
                    }
                }
            }
            if types.is_empty() {
                types.push("text/plain".to_string());
            }
            Ok(types)
        })
    }

    fn eager_fetch(&self) -> bool {
        true
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
                    log::info!("Falling back to wl-clipboard for Linux Wayland clipboard support");
                    Some(Box::new(WlClipboardPortal::new()) as Box<dyn ClipboardPortal>)
                }
            }
        })
        .await
    });
}
