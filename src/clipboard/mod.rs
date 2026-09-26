pub mod auth;
pub mod protocol;
pub mod transport;

pub mod file_task;
pub mod task;
pub mod tests;

#[cfg(all(unix, not(target_os = "macos")))]
pub mod os_linux;

#[cfg(target_os = "windows")]
pub mod os_windows;

use lan_mouse_ipc::{ClientHandle, FrontendEvent};
use protocol::ClipboardMessage;
use std::sync::LazyLock;
use tokio::sync::{broadcast, watch};

pub static ACTIVE_CLIPBOARD_PEER: LazyLock<watch::Sender<Option<ClientHandle>>> =
    LazyLock::new(|| {
        let (tx, _) = watch::channel(None);
        tx
    });

pub static CLIPBOARD_OUTGOING: LazyLock<broadcast::Sender<ClipboardMessage>> =
    LazyLock::new(|| {
        let (tx, _) = broadcast::channel(16);
        tx
    });

pub static CLIPBOARD_INCOMING: LazyLock<broadcast::Sender<ClipboardMessage>> =
    LazyLock::new(|| {
        let (tx, _) = broadcast::channel(16);
        tx
    });

pub static CLIPBOARD_TRANSPORT_CONNECTED: LazyLock<broadcast::Sender<()>> = LazyLock::new(|| {
    let (tx, _) = broadcast::channel(2);
    tx
});

pub static LOCAL_CLIPBOARD_CHANGED: LazyLock<broadcast::Sender<()>> = LazyLock::new(|| {
    let (tx, _) = broadcast::channel(4);
    tx
});

pub static TRANSFER_EVENTS: LazyLock<broadcast::Sender<FrontendEvent>> = LazyLock::new(|| {
    let (tx, _) = broadcast::channel(32);
    tx
});

// A small utility function to initialize the OS clipboard loop.
pub fn init_clipboard_task() {
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        os_linux::init_clipboard_task();
    }

    #[cfg(target_os = "windows")]
    {
        os_windows::init_clipboard_task();
    }

    file_task::init_file_task();
}

pub fn validate_image_dimensions(data: &[u8]) -> Result<(), String> {
    if data.len() < 24 {
        return Ok(()); // let actual image parser fail
    }
    if &data[0..8] == b"\x89PNG\r\n\x1a\n" {
        let width = u32::from_be_bytes([data[16], data[17], data[18], data[19]]) as usize;
        let height = u32::from_be_bytes([data[20], data[21], data[22], data[23]]) as usize;
        if width > 8192 || height > 8192 {
            return Err(format!(
                "Image dimensions exceed 8192x8192 ({}x{})",
                width, height
            ));
        }
        if width
            .checked_mul(height)
            .and_then(|a| a.checked_mul(4))
            .unwrap_or(usize::MAX)
            > 256 * 1024 * 1024
        {
            return Err("Decoded image size exceeds 256 MiB limit".to_string());
        }
    }
    Ok(())
}

/// Detects if an HTML snippet is merely a structural wrapper for an image (like what Firefox
/// produces when you "Copy Image"). We consider it "image only" if it contains an <img> tag
/// and contains no meaningful text outside of HTML tags.
pub fn is_image_only_html(html: &str) -> bool {
    let lower = html.to_lowercase();
    if !lower.contains("<img ") {
        return false;
    }

    let mut in_tag = false;
    let mut text_content = String::new();
    for c in html.chars() {
        if c == '<' {
            in_tag = true;
        } else if c == '>' {
            in_tag = false;
        } else if !in_tag {
            if !c.is_whitespace() {
                text_content.push(c);
            }
        }
    }

    let cleaned = text_content.replace("&nbsp;", "").replace("&#160;", "");
    cleaned.is_empty()
}
