use futures::{Stream, StreamExt};
use std::collections::HashSet;
use std::pin::Pin;
use tokio::sync::watch;

use super::CLIPBOARD_OUTGOING;
use super::protocol::ClipboardMessage;

pub struct ClipboardItem {
    pub mime_type: String,
    pub data: Vec<u8>,
}

pub trait ClipboardPortal: Send + Sync {
    fn receive_selection_owner_changed(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>,
    >;
    fn receive_selection_transfer(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>,
    >;
    fn selection_write<'a>(
        &'a self,
        items: Vec<ClipboardItem>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>;

    fn selection_read<'a>(
        &'a self,
        mime: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send + 'a>>;

    fn set_selection<'a>(
        &'a self,
        mime: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>;

    fn get_available_mime_types(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<String>, String>> + Send + '_>>;

    fn get_supported_mime_types(&self) -> Vec<String> {
        vec![
            "text/plain".to_string(),
            "text/html".to_string(),
            "image/png".to_string(),
            "image/jpeg".to_string(),
        ]
    }

    fn eager_fetch(&self) -> bool {
        false
    }
}

const MAX_CLIPBOARD_BUNDLE_SIZE: usize = 250 * 1024 * 1024; // 250MB
const MAX_CLIPBOARD_ITEM_SIZE: usize = 100 * 1024 * 1024; // 100MB

struct ClipboardTransfer {
    id: u64,
    expected_mimes: HashSet<String>,
    received_items: Vec<ClipboardItem>,
    total_bytes: usize,
}

pub struct ClipboardTask {
    portal: Option<Box<dyn ClipboardPortal>>,
    current_offer_id: u64,
    next_offer_id: u64,
    active_transfer: Option<ClipboardTransfer>,
}

impl ClipboardTask {
    pub async fn run_with_factory<S, F, Fut>(
        mut session_rx: watch::Receiver<Option<S>>,
        portal_factory: F,
    ) where
        S: Clone + Send + Sync + 'static,
        F: Fn(S) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Option<Box<dyn ClipboardPortal>>> + Send,
    {
        let unique_start_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut task = Self {
            portal: None,
            current_offer_id: 0,
            next_offer_id: unique_start_id,
            active_transfer: None,
        };

        let mut incoming_rx = super::CLIPBOARD_INCOMING.subscribe();
        let mut transport_rx = super::CLIPBOARD_TRANSPORT_CONNECTED.subscribe();
        let mut owner_changed_stream: Option<Pin<Box<dyn Stream<Item = ()> + Send>>> = None;
        let mut transfer_stream: Option<Pin<Box<dyn Stream<Item = ()> + Send>>> = None;

        // Process initial session
        let initial_session = { session_rx.borrow().clone() };
        if let Some(session) = initial_session {
            log::info!("ClipboardTask: Received initial InputCapture session");
            if let Some(portal) = portal_factory(session).await {
                owner_changed_stream = Some(portal.receive_selection_owner_changed().await);
                transfer_stream = Some(portal.receive_selection_transfer().await);
                task.portal = Some(portal);
            }
        }

        loop {
            tokio::select! {
                Ok(_) = session_rx.changed() => {
                    let session_opt = { session_rx.borrow().clone() };

                    owner_changed_stream = None;
                    transfer_stream = None;
                    task.portal = None;
                    task.active_transfer = None;

                    if let Some(session) = session_opt {
                        log::info!("ClipboardTask: Received new InputCapture session");
                        if let Some(portal) = portal_factory(session).await {
                            owner_changed_stream = Some(portal.receive_selection_owner_changed().await);
                            transfer_stream = Some(portal.receive_selection_transfer().await);
                            task.portal = Some(portal);
                        }
                    } else {
                        log::info!("ClipboardTask: Session cleared");
                    }
                }

                Ok(msg) = incoming_rx.recv() => {
                    task.handle_incoming_message(msg).await;
                }

                Ok(_) = transport_rx.recv() => {
                    task.handle_peer_connected().await;
                }

                Some(_) = async {
                    if let Some(s) = &mut owner_changed_stream {
                        s.next().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    task.handle_owner_changed().await;
                }

                Some(_) = async {
                    if let Some(s) = &mut transfer_stream {
                        s.next().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    task.handle_transfer().await;
                }
            }
        }
    }

    async fn handle_incoming_message(&mut self, msg: ClipboardMessage) {
        let Some(portal) = &self.portal else { return };

        match msg {
            ClipboardMessage::Offer { id, mime_types } => {
                log::info!(
                    "[DEBUG TASK] Received clipboard offer (id={id}, mime_types={:?}) over TCP",
                    mime_types
                );
                self.current_offer_id = id;

                let supported = portal.get_supported_mime_types();
                let mut requested_mimes = Vec::new();
                let mut expected_set = HashSet::new();

                for mime in &mime_types {
                    if supported.contains(mime) && expected_set.insert(mime.clone()) {
                        requested_mimes.push(mime.clone());
                    }
                }

                if requested_mimes.is_empty() {
                    log::warn!(
                        "[DEBUG TASK] No supported MIME types offered: {:?}",
                        mime_types
                    );
                    self.active_transfer = None;
                    return;
                }

                self.active_transfer = Some(ClipboardTransfer {
                    id,
                    expected_mimes: expected_set,
                    received_items: Vec::new(),
                    total_bytes: 0,
                });

                if portal.eager_fetch() {
                    log::info!(
                        "[DEBUG TASK] Eager fetch enabled, generating Request for offer {id}"
                    );
                    let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Request {
                        id,
                        mime_types: requested_mimes.clone(),
                    });
                }

                for mime in requested_mimes {
                    if let Err(e) = portal.set_selection(&mime).await {
                        log::warn!("[DEBUG TASK] Failed to set selection for {mime}: {e}");
                    }
                }
            }
            ClipboardMessage::Request { id, mime_types } => {
                log::info!(
                    "[DEBUG TASK] Request received over TCP for offer {id} ({:?})",
                    mime_types
                );
                if id != self.current_offer_id {
                    log::warn!("[DEBUG TASK] Requested superseded offer {id}");
                    let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Error { id, code: 404 });
                    return;
                }

                for mime_type in mime_types {
                    match portal.selection_read(&mime_type).await {
                        Ok(data) => {
                            log::info!(
                                "[DEBUG TASK] Generating Data message for offer {id} ({mime_type})"
                            );
                            let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Data {
                                id,
                                mime_type,
                                data,
                            });
                        }
                        Err(e) => {
                            log::warn!("Failed to read selection {mime_type} from portal: {e}");
                            let _ =
                                CLIPBOARD_OUTGOING.send(ClipboardMessage::Error { id, code: 500 });
                        }
                    }
                }
            }
            ClipboardMessage::Data {
                id,
                mime_type,
                data,
            } => {
                log::info!(
                    "[DEBUG TASK] Data received over TCP for offer {id} ({mime_type}) with length {}",
                    data.len()
                );

                let Some(transfer) = &mut self.active_transfer else {
                    log::warn!("[DEBUG TASK] Received data but no active transfer");
                    return;
                };

                if transfer.id != id {
                    log::warn!("[DEBUG TASK] Received data for mismatched offer {id}");
                    return;
                }

                if !transfer.expected_mimes.contains(&mime_type) {
                    log::warn!("[DEBUG TASK] Received unrequested MIME type {mime_type}");
                    return;
                }

                if transfer
                    .received_items
                    .iter()
                    .any(|i| i.mime_type == mime_type)
                {
                    log::warn!("[DEBUG TASK] Received duplicate Data for {mime_type}");
                    return;
                }

                if data.len() > MAX_CLIPBOARD_ITEM_SIZE {
                    log::warn!("[DEBUG TASK] Item size exceeded limit");
                    self.active_transfer = None;
                    return;
                }

                if transfer.total_bytes.saturating_add(data.len()) > MAX_CLIPBOARD_BUNDLE_SIZE {
                    log::warn!("[DEBUG TASK] Aggregate clipboard size exceeded limit");
                    self.active_transfer = None;
                    return;
                }

                transfer.total_bytes += data.len();
                transfer
                    .received_items
                    .push(ClipboardItem { mime_type, data });

                if transfer.received_items.len() == transfer.expected_mimes.len() {
                    log::info!(
                        "[DEBUG TASK] All requested representations received. Committing clipboard."
                    );
                    let items = std::mem::take(&mut transfer.received_items);
                    self.active_transfer = None;

                    if let Err(e) = portal.selection_write(items).await {
                        log::warn!("[DEBUG TASK] Failed to write selection to portal: {e}");
                    }
                }
            }
            ClipboardMessage::Error { id, code } => {
                log::warn!("Clipboard error from remote: id={id}, code={code}");
                if let Some(transfer) = &self.active_transfer {
                    if transfer.id == id {
                        self.active_transfer = None;
                    }
                }
            }
            _ => {}
        }
    }

    async fn handle_owner_changed(&mut self) {
        log::info!("[DEBUG TASK] Clipboard owner-change event received");
        let id = self.next_offer_id;
        self.next_offer_id += 1;
        self.current_offer_id = id;

        let mime_types = self.generate_offer_mime_types().await;

        if mime_types.is_empty() {
            log::info!("[DEBUG TASK] No generic MIME types available, skipping Offer generation (likely file-only clipboard)");
            return;
        }

        log::info!(
            "[DEBUG TASK] Generating Offer (id={id}) with types: {:?}",
            mime_types
        );
        let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Offer { id, mime_types });
    }

    async fn handle_peer_connected(&mut self) {
        if self.current_offer_id > 0 {
            log::info!(
                "[DEBUG TASK] Peer connected. Re-sending current Offer (id={})",
                self.current_offer_id
            );
            let mime_types = self.generate_offer_mime_types().await;
            let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Offer {
                id: self.current_offer_id,
                mime_types,
            });
        }
    }

    async fn handle_transfer(&mut self) {
        log::info!("Local app requested clipboard transfer");
        if let Some(transfer) = &self.active_transfer {
            let mimes: Vec<String> = transfer.expected_mimes.iter().cloned().collect();
            let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Request {
                id: transfer.id,
                mime_types: mimes,
            });
        } else {
            let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Request {
                id: self.current_offer_id,
                mime_types: vec!["text/plain".to_string()],
            });
        }
    }

    async fn generate_offer_mime_types(&self) -> Vec<String> {
        if let Some(portal) = &self.portal {
            if let Ok(available) = portal.get_available_mime_types().await {
                return available;
            }
        }
        Vec::new()
    }
}
