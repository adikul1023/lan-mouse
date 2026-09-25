pub mod auth;
pub mod protocol;
pub mod transport;

pub mod task;
pub mod tests;

#[cfg(all(unix, not(target_os = "macos")))]
pub mod os_linux;

#[cfg(target_os = "windows")]
pub mod os_windows;

use lan_mouse_ipc::ClientHandle;
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
}
