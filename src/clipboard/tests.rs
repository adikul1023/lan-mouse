#![cfg(test)]

use super::protocol::ClipboardMessage;
use super::task::{ClipboardPortal, ClipboardTask};
use super::{CLIPBOARD_INCOMING, CLIPBOARD_OUTGOING};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

#[derive(Clone)]
struct MockClipboardPortal {
    owner_changed_rx: Arc<tokio::sync::Mutex<Option<mpsc::Receiver<()>>>>,
    transfer_rx: Arc<tokio::sync::Mutex<Option<mpsc::Receiver<()>>>>,
    events: Arc<Mutex<Vec<String>>>,
    read_data: Vec<u8>,
    eager_fetch: bool,
}

struct ReceiverStream {
    rx: mpsc::Receiver<()>,
}

impl Stream for ReceiverStream {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl ClipboardPortal for MockClipboardPortal {
    fn eager_fetch(&self) -> bool {
        self.eager_fetch
    }

    fn receive_selection_owner_changed(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>,
    > {
        Box::pin(async move {
            let rx = self.owner_changed_rx.lock().await.take().unwrap();
            Box::pin(ReceiverStream { rx }) as Pin<Box<dyn Stream<Item = ()> + Send>>
        })
    }

    fn receive_selection_transfer(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + '_>,
    > {
        Box::pin(async move {
            let rx = self.transfer_rx.lock().await.take().unwrap();
            Box::pin(ReceiverStream { rx }) as Pin<Box<dyn Stream<Item = ()> + Send>>
        })
    }

    fn selection_write<'a>(
        &'a self,
        mime: &'a str,
        data: Vec<u8>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            self.events.lock().unwrap().push(format!(
                "selection_write({}, {} bytes)",
                mime,
                data.len()
            ));
            Ok(())
        })
    }

    fn selection_read<'a>(
        &'a self,
        mime: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send + 'a>> {
        Box::pin(async move {
            self.events
                .lock()
                .unwrap()
                .push(format!("selection_read({})", mime));
            if self.read_data.is_empty() {
                return Err("Simulated OS integration failure".to_string());
            }
            Ok(self.read_data.clone())
        })
    }

    fn set_selection<'a>(
        &'a self,
        mime: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            self.events
                .lock()
                .unwrap()
                .push(format!("set_selection({})", mime));
            Ok(())
        })
    }
}

#[tokio::test]
async fn test_clipboard_state_machine() {
    let (owner_tx, owner_rx) = mpsc::channel(1);
    let (transfer_tx, transfer_rx) = mpsc::channel(1);
    let events = Arc::new(Mutex::new(Vec::new()));

    let portal = MockClipboardPortal {
        owner_changed_rx: Arc::new(tokio::sync::Mutex::new(Some(owner_rx))),
        transfer_rx: Arc::new(tokio::sync::Mutex::new(Some(transfer_rx))),
        events: events.clone(),
        read_data: b"hello_local".to_vec(),
        eager_fetch: false,
    };

    let mut outgoing_rx = CLIPBOARD_OUTGOING.subscribe();
    let portal_opt = Arc::new(tokio::sync::Mutex::new(Some(portal)));

    // We mock the session channel
    // 7. Active Peer routing and conditional ClientLeft
    // Set active peer to 100
    let _ = super::ACTIVE_CLIPBOARD_PEER.send_replace(Some(100));

    // Simulate ClientLeft(50) (an old/stale client)
    super::ACTIVE_CLIPBOARD_PEER.send_if_modified(|current| {
        if *current == Some(50) {
            *current = None;
            true
        } else {
            false
        }
    });

    // The active peer should STILL be 100, because 50 is stale.
    assert_eq!(
        *super::ACTIVE_CLIPBOARD_PEER.subscribe().borrow(),
        Some(100)
    );

    // Simulate ClientLeft(100) (the actual active client)
    super::ACTIVE_CLIPBOARD_PEER.send_if_modified(|current| {
        if *current == Some(100) {
            *current = None;
            true
        } else {
            false
        }
    });

    // The active peer should now be None.
    assert_eq!(*super::ACTIVE_CLIPBOARD_PEER.subscribe().borrow(), None);

    let (session_tx, session_rx) = watch::channel(Some(()));

    let task_handle = tokio::spawn(async move {
        ClipboardTask::run_with_factory(session_rx, move |_: ()| {
            let p_opt = portal_opt.clone();
            async move {
                let p = p_opt.lock().await.take();
                p.map(|x| Box::new(x) as Box<dyn ClipboardPortal>)
            }
        })
        .await;
    });

    // Yield to allow task to start and subscribe
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Test 1: Local clipboard changes -> Offer generated
    owner_tx.send(()).await.unwrap();

    let msg = outgoing_rx.recv().await.unwrap();
    let _offer_id = match msg {
        ClipboardMessage::Offer { id, mime_type } => {
            assert_eq!(mime_type, "text/plain");
            id
        }
        _ => panic!("Expected Offer"),
    };

    // Test 2: Remote Offer arrives -> portal selection is registered
    let remote_offer_id = 999;
    CLIPBOARD_INCOMING
        .send(ClipboardMessage::Offer {
            id: remote_offer_id,
            mime_type: "text/plain".to_string(),
        })
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    {
        let evs = events.lock().unwrap();
        assert!(evs.contains(&"set_selection(text/plain)".to_string()));
    }

    // Test 3: Local App requests remote clipboard -> Request sent
    transfer_tx.send(()).await.unwrap();
    let msg = outgoing_rx.recv().await.unwrap();
    match msg {
        ClipboardMessage::Request { id } => {
            assert_eq!(id, remote_offer_id);
        }
        _ => panic!("Expected Request"),
    }

    // Test 4: Data arrives -> selection_write()
    CLIPBOARD_INCOMING
        .send(ClipboardMessage::Data {
            id: remote_offer_id,
            data: b"remote_data".to_vec(),
        })
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    {
        let evs = events.lock().unwrap();
        assert!(evs.contains(&"selection_write(text/plain, 11 bytes)".to_string()));
    }

    // Test 5: Old Request ID -> Error(NotFound)
    CLIPBOARD_INCOMING
        .send(ClipboardMessage::Request {
            id: remote_offer_id - 1, // Stale ID
        })
        .unwrap();

    let msg = outgoing_rx.recv().await.unwrap();
    match msg {
        ClipboardMessage::Error { id, code } => {
            assert_eq!(id, remote_offer_id - 1);
            assert_eq!(code, 404);
        }
        _ => panic!("Expected Error(NotFound)"),
    }

    // Test 6: Session recreation -> old streams terminated
    // If we replace the session with None, then new owner_tx sends should be ignored.
    session_tx.send(None).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Sending should fail because the receiver was dropped!
    assert!(owner_tx.send(()).await.is_err());

    task_handle.abort();
}

#[tokio::test]
async fn test_eager_fetch() {
    let (owner_tx, owner_rx) = mpsc::channel(1);
    let (transfer_tx, transfer_rx) = mpsc::channel(1);
    let events = Arc::new(Mutex::new(Vec::new()));

    let portal = MockClipboardPortal {
        owner_changed_rx: Arc::new(tokio::sync::Mutex::new(Some(owner_rx))),
        transfer_rx: Arc::new(tokio::sync::Mutex::new(Some(transfer_rx))),
        events: events.clone(),
        read_data: b"hello_local".to_vec(),
        eager_fetch: true,
    };

    let mut outgoing_rx = CLIPBOARD_OUTGOING.subscribe();

    let (_session_tx, session_rx) = watch::channel(Some(()));

    let _ = super::ACTIVE_CLIPBOARD_PEER.send_replace(Some(100));

    let task_handle = tokio::task::spawn(async move {
        let portal_clone = portal.clone();
        ClipboardTask::run_with_factory(session_rx, move |_| {
            let p = portal_clone.clone();
            async move { Some(Box::new(p) as Box<dyn ClipboardPortal>) }
        })
        .await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Simulate remote Offer
    CLIPBOARD_INCOMING
        .send(ClipboardMessage::Offer {
            id: 1,
            mime_type: "text/plain".to_string(),
        })
        .unwrap();

    // Because eager_fetch is true, the task should immediately send a Request
    let msg = outgoing_rx.recv().await.unwrap();
    match msg {
        ClipboardMessage::Request { id } => {
            assert_eq!(id, 1);
        }
        _ => panic!("Expected Request"),
    }

    task_handle.abort();
}

#[tokio::test]
async fn test_failure_isolation_and_stress() {
    let (owner_tx, owner_rx) = mpsc::channel(1);
    let (_transfer_tx, transfer_rx) = mpsc::channel(1);
    let events = Arc::new(Mutex::new(Vec::new()));

    let portal = MockClipboardPortal {
        owner_changed_rx: Arc::new(tokio::sync::Mutex::new(Some(owner_rx))),
        transfer_rx: Arc::new(tokio::sync::Mutex::new(Some(transfer_rx))),
        events: events.clone(),
        read_data: Vec::new(), // Empty indicates simulated error
        eager_fetch: false,
    };

    let mut outgoing_rx = CLIPBOARD_OUTGOING.subscribe();
    let portal_opt = Arc::new(tokio::sync::Mutex::new(Some(portal)));
    let (session_tx, session_rx) = watch::channel(Some(()));

    let _ = super::ACTIVE_CLIPBOARD_PEER.send_replace(Some(100));

    let task_handle = tokio::spawn(async move {
        ClipboardTask::run_with_factory(session_rx, move |_: ()| {
            let p_opt = portal_opt.clone();
            async move {
                let p = p_opt.lock().await.take();
                p.map(|x| Box::new(x) as Box<dyn ClipboardPortal>)
            }
        })
        .await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // We must trigger an owner change to get a valid current_offer_id
    owner_tx.send(()).await.unwrap();
    let msg = outgoing_rx.recv().await.unwrap();
    let current_offer_id = match msg {
        ClipboardMessage::Offer { id, .. } => id,
        _ => panic!("Expected Offer"),
    };

    // Test: OS integration failure (wl-paste fails)
    CLIPBOARD_INCOMING
        .send(ClipboardMessage::Request {
            id: current_offer_id,
        })
        .unwrap();
    // Wait for the task to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    // It should send a 500 error since we simulate an OS failure
    let msg = outgoing_rx.recv().await.unwrap();
    match msg {
        ClipboardMessage::Error { id, code } => {
            assert_eq!(id, current_offer_id);
            assert_eq!(code, 500);
        }
        _ => panic!("Expected Error 500 for OS failure"),
    }

    // Phase 4C: Stress / Soak Testing - payload sizes
    // We send incoming Data of various sizes to see if the portal processes them without blocking
    let sizes = [
        1024,             // 1 KiB
        100 * 1024,       // 100 KiB
        1024 * 1024,      // 1 MiB
        5 * 1024 * 1024,  // 5 MiB
        10 * 1024 * 1024, // 10 MiB
    ];

    for size in sizes {
        // Must send an Offer first so it expects the Data
        let offer_id = size as u64;
        CLIPBOARD_INCOMING
            .send(ClipboardMessage::Offer {
                id: offer_id,
                mime_type: "text/plain".to_string(),
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        CLIPBOARD_INCOMING
            .send(ClipboardMessage::Data {
                id: offer_id,
                data: vec![0u8; size],
            })
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Verify the portal got the valid writes
    {
        let evs = events.lock().unwrap();
        assert!(evs.contains(&"selection_write(text/plain, 1024 bytes)".to_string()));
        assert!(evs.contains(&"selection_write(text/plain, 10485760 bytes)".to_string()));
    }

    // Rapid clipboard changes
    for i in 200..205 {
        CLIPBOARD_INCOMING
            .send(ClipboardMessage::Offer {
                id: i,
                mime_type: "text/plain".to_string(),
            })
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send data for an old offer (203 instead of 204) -> should be discarded
    CLIPBOARD_INCOMING
        .send(ClipboardMessage::Data {
            id: 203,
            data: vec![0],
        })
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    {
        let evs = events.lock().unwrap();
        // It shouldn't have done selection_write for the stale data
        assert!(!evs.contains(&"selection_write(text/plain, 1 bytes)".to_string()));
    }

    // Verify Reconnect Behavior (handle_peer_connected)
    // 1. Task has a current offer ID
    // We trigger local owner_changed to generate an offer
    let _ = owner_tx.send(()).await;

    // Read the Offer that goes out
    let msg = outgoing_rx.recv().await.unwrap();
    let current_offer_id = match msg {
        ClipboardMessage::Offer { id, .. } => id,
        _ => panic!("Expected Offer"),
    };

    // Simulate TCP reconnect
    let _ = super::CLIPBOARD_TRANSPORT_CONNECTED.send(());

    // We should see the EXACT SAME Offer resent
    let msg = outgoing_rx.recv().await.unwrap();
    match msg {
        ClipboardMessage::Offer { id, .. } => {
            assert_eq!(id, current_offer_id);
        }
        _ => panic!("Expected Offer on reconnect"),
    }

    task_handle.abort();
}
