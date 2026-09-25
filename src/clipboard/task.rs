use futures::{Stream, StreamExt};
use std::pin::Pin;
use tokio::sync::watch;

use super::CLIPBOARD_OUTGOING;
use super::protocol::ClipboardMessage;

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
        mime: &'a str,
        data: Vec<u8>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>;
    fn selection_read<'a>(
        &'a self,
        mime: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send + 'a>>;
    fn set_selection<'a>(
        &'a self,
        mime: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>;

    /// Return true if the OS integration expects the network to eagerly fetch data immediately upon receiving an Offer.
    fn eager_fetch(&self) -> bool {
        false
    }
}

pub struct ClipboardTask {
    portal: Option<Box<dyn ClipboardPortal>>,
    current_offer_id: u64,
    next_offer_id: u64,
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
        let mut task = Self {
            portal: None,
            current_offer_id: 0,
            next_offer_id: 1,
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
            ClipboardMessage::Offer { id, mime_type } => {
                log::info!(
                    "[DEBUG TASK] Received clipboard offer (id={id}, mime={mime_type}) over TCP"
                );
                self.current_offer_id = id;
                if mime_type == "text/plain" {
                    if let Err(e) = portal.set_selection(&mime_type).await {
                        log::warn!("[DEBUG TASK] Failed to set selection on portal: {e}");
                    }
                    if portal.eager_fetch() {
                        log::info!(
                            "[DEBUG TASK] Eager fetch enabled, generating Request for offer {id}"
                        );
                        let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Request { id });
                    }
                }
            }
            ClipboardMessage::Request { id } => {
                log::info!("[DEBUG TASK] Request received over TCP for offer {id}");
                if id != self.current_offer_id {
                    log::warn!("[DEBUG TASK] Requested superseded offer {id}");
                    let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Error { id, code: 404 });
                    return;
                }
                match portal.selection_read("text/plain").await {
                    Ok(data) => {
                        log::info!("[DEBUG TASK] Generating Data message for offer {id}");
                        let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Data { id, data });
                    }
                    Err(e) => {
                        log::warn!("Failed to read selection from portal: {e}");
                        let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Error { id, code: 500 });
                    }
                }
            }
            ClipboardMessage::Data { id, data } => {
                log::info!(
                    "[DEBUG TASK] Data received over TCP for offer {id} with length {}",
                    data.len()
                );
                if id != self.current_offer_id {
                    log::warn!("[DEBUG TASK] Received data for superseded offer {id}");
                    return;
                }
                log::info!("[DEBUG TASK] Invoking selection_write()");
                if let Err(e) = portal.selection_write("text/plain", data).await {
                    log::warn!("[DEBUG TASK] Failed to write selection to portal: {e}");
                }
            }
            ClipboardMessage::Error { id, code } => {
                log::warn!("Clipboard error from remote: id={id}, code={code}");
            }
            _ => {}
        }
    }

    async fn handle_owner_changed(&mut self) {
        log::info!("[DEBUG TASK] Clipboard owner-change event received");
        let id = self.next_offer_id;
        self.next_offer_id += 1;
        self.current_offer_id = id;

        log::info!("[DEBUG TASK] Generating Offer (id={id})");
        let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Offer {
            id,
            mime_type: "text/plain".to_string(),
        });
    }

    async fn handle_peer_connected(&mut self) {
        if self.current_offer_id > 0 {
            log::info!(
                "[DEBUG TASK] Peer connected. Re-sending current Offer (id={})",
                self.current_offer_id
            );
            let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Offer {
                id: self.current_offer_id,
                mime_type: "text/plain".to_string(),
            });
        }
    }

    async fn handle_transfer(&mut self) {
        log::info!("Local app requested clipboard transfer");
        let _ = CLIPBOARD_OUTGOING.send(ClipboardMessage::Request {
            id: self.current_offer_id,
        });
    }
}
