use std::pin::Pin;
use std::sync::{Arc, Mutex};

use clipboard_win::formats;
use futures::Stream;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

fn encode_cf_html(fragment: &[u8]) -> Vec<u8> {
    let html_prefix = "<html>\r\n<body>\r\n<!--StartFragment-->";
    let html_suffix = "<!--EndFragment-->\r\n</body>\r\n</html>";

    let start_html = 105;
    let start_fragment = start_html + html_prefix.len();
    let end_fragment = start_fragment + fragment.len();
    let end_html = end_fragment + html_suffix.len();

    let header = format!(
        "Version:0.9\r\nStartHTML:{:010}\r\nEndHTML:{:010}\r\nStartFragment:{:010}\r\nEndFragment:{:010}\r\n",
        start_html, end_html, start_fragment, end_fragment
    );

    let mut result = Vec::new();
    result.extend_from_slice(header.as_bytes());
    result.extend_from_slice(html_prefix.as_bytes());
    result.extend_from_slice(fragment);
    result.extend_from_slice(html_suffix.as_bytes());
    result.push(0);
    result
}

fn decode_cf_html(data: &[u8]) -> Result<Vec<u8>, String> {
    let data_str = String::from_utf8_lossy(data);

    let mut start_fragment = 0;
    let mut end_fragment = data.len();
    let mut found_start = false;
    let mut found_end = false;

    for line in data_str.lines() {
        if line.starts_with("StartFragment:") {
            if let Ok(offset) = line["StartFragment:".len()..].trim().parse::<usize>() {
                start_fragment = offset;
                found_start = true;
            }
        } else if line.starts_with("EndFragment:") {
            if let Ok(offset) = line["EndFragment:".len()..].trim().parse::<usize>() {
                end_fragment = offset;
                found_end = true;
            }
        }
    }

    if found_start && found_end && start_fragment <= end_fragment && end_fragment <= data.len() {
        Ok(data[start_fragment..end_fragment].to_vec())
    } else {
        Err("Malformed CF_HTML or invalid fragment offsets".to_string())
    }
}

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
            if mime == "text/plain" {
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
            } else if mime == "text/html" {
                if let Ok(mut supp) = self.suppressor.lock() {
                    supp.prepare_write();
                }

                tokio::task::spawn_blocking(move || {
                    let html_fmt = clipboard_win::register_format("HTML Format")
                        .ok_or_else(|| "Failed to register HTML Format".to_string())?;
                    let encoded = encode_cf_html(&data);
                    clipboard_win::set_clipboard(
                        clipboard_win::formats::RawData(html_fmt.get()),
                        encoded,
                    )
                    .map_err(|e| e.to_string())
                })
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
            } else if mime.starts_with("image/") {
                if let Ok(mut supp) = self.suppressor.lock() {
                    supp.prepare_write();
                }
                let data = data.clone();
                let mime_str = mime.to_string();
                
                tokio::task::spawn_blocking(move || {
                    let png_fmt = clipboard_win::register_format("PNG");
                    
                    // Always try to write the modern PNG format if we received a PNG/JPEG
                    if let Some(fmt) = png_fmt {
                        // If it's already a PNG, just write it
                        if mime_str == "image/png" {
                            let _ = clipboard_win::set_clipboard(
                                clipboard_win::formats::RawData(fmt.get()),
                                &data,
                            );
                        }
                    }

                    // For legacy app compatibility, decode the image and write it as CF_DIB (Bitmap).
                    // The clipboard-win `formats::Bitmap` expects a standard .bmp file layout 
                    // and handles the CF_DIB conversion internally.
                    if let Ok(img) = image::load_from_memory(&data) {
                        let mut bmp_data = std::io::Cursor::new(Vec::new());
                        if img.write_to(&mut bmp_data, image::ImageFormat::Bmp).is_ok() {
                            let _ = clipboard_win::set_clipboard(
                                clipboard_win::formats::Bitmap,
                                bmp_data.into_inner(),
                            );
                        }
                    }
                    Ok::<(), String>(())
                })
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
            } else {
                return Err(format!("Unsupported MIME type: {}", mime));
            }

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
            let data = if mime == "text/plain" {
                let text: String =
                    tokio::task::spawn_blocking(|| clipboard_win::get_clipboard(formats::Unicode))
                        .await
                        .map_err(|e| e.to_string())?
                        .map_err(|e| e.to_string())?;
                text.into_bytes()
            } else if mime == "text/html" {
                tokio::task::spawn_blocking(|| {
                    let html_fmt = clipboard_win::register_format("HTML Format")
                        .ok_or_else(|| "Failed to register HTML Format".to_string())?;
                    let data: Vec<u8> = clipboard_win::get_clipboard(
                        clipboard_win::formats::RawData(html_fmt.get()),
                    )
                    .map_err(|e| e.to_string())?;
                    decode_cf_html(&data)
                })
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?
            } else if mime == "image/png" || mime == "image/jpeg" {
                tokio::task::spawn_blocking(move || {
                    let png_fmt = clipboard_win::register_format("PNG");
                    
                    // Prefer reading native PNG directly if available
                    if let Some(fmt) = png_fmt {
                        if clipboard_win::is_format_avail(fmt.get()) {
                            if let Ok(data) = clipboard_win::get_clipboard::<Vec<u8>, _>(
                                clipboard_win::formats::RawData(fmt.get()),
                            ) {
                                return Ok(data);
                            }
                        }
                    }
                    
                    // Fallback: Read as Bitmap (CF_DIB) and encode to PNG
                    let bmp_data: Vec<u8> = clipboard_win::get_clipboard(clipboard_win::formats::Bitmap)
                        .map_err(|e| format!("Failed to read CF_DIB: {}", e))?;
                        
                    let img = image::load_from_memory_with_format(&bmp_data, image::ImageFormat::Bmp)
                        .map_err(|e| format!("Failed to parse CF_DIB as BMP: {}", e))?;
                        
                    let mut png_data = std::io::Cursor::new(Vec::new());
                    img.write_to(&mut png_data, image::ImageFormat::Png)
                        .map_err(|e| format!("Failed to encode image to PNG: {}", e))?;
                        
                    Ok::<Vec<u8>, String>(png_data.into_inner())
                })
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?
            } else {
                return Err(format!("Unsupported MIME type: {}", mime));
            };

            // Apply limit (account for 13 bytes framing overhead)
            let max_payload = (crate::clipboard::transport::MAX_CLIPBOARD_FRAME_SIZE - 13) as usize;
            if data.len() > max_payload {
                return Err("Clipboard data exceeds 10MB limit".to_string());
            }
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

    fn get_available_mime_types(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<String>, String>> + Send + '_>> {
        Box::pin(async move {
            let mut available = tokio::task::spawn_blocking(move || {
                let mut avail = Vec::new();
                let _clip =
                    clipboard_win::Clipboard::new_attempts(10).map_err(|e| e.to_string())?;
                let html_fmt = clipboard_win::register_format("HTML Format");
                let png_fmt = clipboard_win::register_format("PNG");

                for format in clipboard_win::EnumFormats::new() {
                    if Some(format) == html_fmt.map(|f| f.get()) {
                        if !avail.contains(&"text/html".to_string()) {
                            avail.push("text/html".to_string());
                        }
                    } else if format == 13 || format == 1 { // CF_UNICODETEXT or CF_TEXT
                        if !avail.contains(&"text/plain".to_string()) {
                            avail.push("text/plain".to_string());
                        }
                    } else if Some(format) == png_fmt.map(|f| f.get()) || format == 17 || format == 8 { // PNG, CF_DIBV5, CF_DIB
                        if !avail.contains(&"image/png".to_string()) {
                            avail.push("image/png".to_string());
                        }
                    }
                }
                
                Ok::<Vec<String>, String>(avail)
            })
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;

            if available.is_empty() {
                available.push("text/plain".to_string()); // fallback
            }
            Ok(available)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cf_html_roundtrip_ascii() {
        let input = b"<b>hello</b>";
        let encoded = encode_cf_html(input);
        let decoded = decode_cf_html(&encoded).unwrap();
        assert_eq!(decoded, input);
    }

    #[test]
    fn test_cf_html_roundtrip_unicode() {
        let input = "<b>こんにちは (Hello in Japanese) / नमस्ते (Hindi) / 🌍 (Emoji)</b>".as_bytes();
        let encoded = encode_cf_html(input);
        let decoded = decode_cf_html(&encoded).unwrap();
        assert_eq!(decoded, input);
    }

    #[test]
    fn test_cf_html_roundtrip_multiline() {
        let input = b"<ul>\r\n  <li>Line 1</li>\n  <li>Line 2</li>\r\n</ul>";
        let encoded = encode_cf_html(input);
        let decoded = decode_cf_html(&encoded).unwrap();
        assert_eq!(decoded, input);
    }

    #[test]
    fn test_cf_html_roundtrip_empty() {
        let input = b"";
        let encoded = encode_cf_html(input);
        let decoded = decode_cf_html(&encoded).unwrap();
        assert_eq!(decoded, input);
    }

    #[test]
    fn test_cf_html_roundtrip_large() {
        let input = vec![b'A'; 100_000];
        let encoded = encode_cf_html(&input);
        let decoded = decode_cf_html(&encoded).unwrap();
        assert_eq!(decoded, input);
    }

    #[test]
    fn test_cf_html_decode_malformed() {
        let malformed = b"Version:0.9\r\nStartHTML:0000000000\r\nEndHTML:0000000000\r\n";
        let result = decode_cf_html(malformed);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Malformed CF_HTML or invalid fragment offsets");
        
        let invalid_offsets = b"Version:0.9\r\nStartFragment:0000000100\r\nEndFragment:0000000050\r\n";
        let result = decode_cf_html(invalid_offsets);
        assert!(result.is_err());
    }
}
