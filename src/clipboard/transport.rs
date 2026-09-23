use tokio::io::{AsyncReadExt, AsyncWriteExt};
use super::protocol::{ClipboardMessage, ProtocolError};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use lan_mouse_ipc::ClientHandle;
use crate::client::ClientManager;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use webrtc_dtls::crypto::Certificate;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use rustls::ServerConfig;
use std::collections::HashMap;
use std::sync::RwLock;

pub const MAX_CLIPBOARD_FRAME_SIZE: u32 = 10 * 1024 * 1024; // 10 MB

pub async fn read_message<R: AsyncReadExt + Unpin>(
    stream: &mut R,
) -> Result<ClipboardMessage, ProtocolError> {
    let length = stream.read_u32().await?;
    if length > MAX_CLIPBOARD_FRAME_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Frame too large",
        )
        .into());
    }

    let mut buf = vec![0u8; length as usize];
    stream.read_exact(&mut buf).await?;

    let mut cursor = Cursor::new(buf);
    ClipboardMessage::decode(&mut cursor)
}

pub async fn write_message<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    msg: &ClipboardMessage,
) -> Result<(), ProtocolError> {
    let mut buf = Vec::new();
    msg.encode(&mut buf)?;

    let length = buf.len() as u32;
    if length > MAX_CLIPBOARD_FRAME_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Frame too large for write",
        )
        .into());
    }

    stream.write_u32(length).await?;
    stream.write_all(&buf).await?;
    stream.flush().await?;

    Ok(())
}

pub async fn connect_clipboard(
    client_manager: ClientManager,
    handle: ClientHandle,
    addr: SocketAddr,
    cert: Certificate,
    authorized_keys: Arc<RwLock<HashMap<String, String>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Initiating clipboard TCP connection to {addr}...");

    // 1. Establish TCP
    let tcp_stream = TcpStream::connect(addr).await?;

    // 2. Setup TLS Config using existing certificate identity
    let pem = cert.serialize_pem();
    let mut cursor = std::io::Cursor::new(pem.as_bytes());
    let cert_chain = rustls_pemfile::certs(&mut cursor)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .collect::<Vec<_>>();

    let mut cursor = std::io::Cursor::new(pem.as_bytes());
    let private_key = rustls_pemfile::private_key(&mut cursor)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "No private key in PEM"))?;

    let expected_fingerprint = {
        let hostname = client_manager.get_hostname(handle).unwrap_or_default();
        let auth_keys = authorized_keys.read().expect("lock");
        auth_keys.iter().find(|(_, h)| *h == &hostname).map(|(f, _)| f.clone())
    };

    let Some(expected_fingerprint) = expected_fingerprint else {
        return Err("No authorized fingerprint found for peer".into());
    };

    let verifier = Arc::new(super::auth::LanMouseServerVerifier::new(expected_fingerprint));

    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(cert_chain, private_key)?;

    let connector = TlsConnector::from(Arc::new(config));

    // Using a dummy server name since we verify purely by fingerprint
    let server_name = ServerName::try_from("lan-mouse-peer").unwrap().to_owned();

    // 3. Perform TLS Handshake
    let mut tls_stream = connector.connect(server_name, tcp_stream).await?;

    log::info!("Clipboard TLS connection established with {addr}");

    // 4. Send Protocol Hello
    let hello = ClipboardMessage::Hello { protocol_version: 1 };
    write_message(&mut tls_stream, &hello).await?;

    let mut outgoing_rx = super::CLIPBOARD_OUTGOING.subscribe();

    // 5. Active receive loop
    loop {
        let active = *super::ACTIVE_CLIPBOARD_PEER.subscribe().borrow() == Some(handle);
        if !active {
            log::info!("Clipboard connection to {addr} closed: peer is no longer the active clipboard peer");
            break;
        }

        tokio::select! {
            result = tokio::time::timeout(std::time::Duration::from_secs(2), read_message(&mut tls_stream)) => {
                match result {
                    Ok(Ok(msg)) => {
                        log::info!("Clipboard received from {addr}: {:?}", msg);
                        if let ClipboardMessage::Hello { protocol_version } = msg {
                            if protocol_version != 1 {
                                log::warn!("Unsupported protocol version {protocol_version} from {addr}");
                                let _ = write_message(&mut tls_stream, &ClipboardMessage::Error { id: 0, code: 400 }).await;
                                break;
                            }
                            let ack = ClipboardMessage::HelloAck { protocol_version: 1 };
                            let _ = write_message(&mut tls_stream, &ack).await;
                        } else {
                            let _ = super::CLIPBOARD_INCOMING.send(msg);
                        }
                    }
                    Ok(Err(e)) => {
                        log::warn!("Clipboard connection to {addr} closed: {e}");
                        break;
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
            Ok(msg) = outgoing_rx.recv() => {
                if *super::ACTIVE_CLIPBOARD_PEER.subscribe().borrow() == Some(handle) {
                    if let Err(e) = write_message(&mut tls_stream, &msg).await {
                        log::warn!("Failed to write clipboard message to {addr}: {e}");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

pub async fn listen_clipboard(
    listener: TcpListener,
    cert: Certificate,
    authorized_keys: Arc<RwLock<HashMap<String, String>>>,
    client_manager: ClientManager,
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup TLS Server Config
    let pem = cert.serialize_pem();
    let mut cursor = std::io::Cursor::new(pem.as_bytes());
    let cert_chain = rustls_pemfile::certs(&mut cursor)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .collect::<Vec<_>>();

    let mut cursor = std::io::Cursor::new(pem.as_bytes());
    let private_key = rustls_pemfile::private_key(&mut cursor)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "No private key in PEM"))?;

    let verifier = Arc::new(super::auth::LanMouseClientVerifier::new(authorized_keys.clone()));

    let config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(cert_chain, private_key)?;

    let acceptor = TlsAcceptor::from(Arc::new(config));

    // 2. Accept Loop
    loop {
        let (tcp_stream, peer_addr) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let authorized_keys = authorized_keys.clone();
        let client_manager = client_manager.clone();

        tokio::task::spawn_local(async move {
            match acceptor.accept(tcp_stream).await {
                Ok(mut tls_stream) => {
                    let peer_certs = tls_stream.get_ref().1.peer_certificates();
                    let Some(peer_certs) = peer_certs else {
                        log::warn!("Clipboard TLS accept failed: Missing peer certificates");
                        return;
                    };

                    if peer_certs.is_empty() {
                        log::warn!("Clipboard TLS accept failed: Empty peer certificates");
                        return;
                    }

                    let fingerprint = crate::crypto::generate_fingerprint(&peer_certs[0]);

                    let hostname = authorized_keys.read().expect("lock").get(&fingerprint).cloned();
                    let Some(hostname) = hostname else {
                        log::warn!("Clipboard TLS accept failed: Peer hostname not found");
                        return;
                    };

                    let mut found_handle = None;
                    for (h, c, s) in client_manager.get_client_states() {
                        if c.hostname == Some(hostname.clone()) && s.active && Some(peer_addr.ip()) == s.active_addr.map(|a| a.ip()) {
                            found_handle = Some(h);
                            break;
                        }
                    }

                    let Some(handle) = found_handle else {
                        log::warn!("Clipboard TLS accept failed: Peer {hostname} is not actively connected via DTLS");
                        return;
                    };

                    log::info!("Clipboard TLS connection accepted from {peer_addr} (handle: {handle})");

                    // Wait for Hello from Client
                    match tokio::time::timeout(std::time::Duration::from_secs(5), read_message(&mut tls_stream)).await {
                        Ok(Ok(ClipboardMessage::Hello { protocol_version })) => {
                            log::info!("Clipboard Hello from {peer_addr} (v{protocol_version})");
                            if protocol_version != 1 {
                                log::warn!("Unsupported protocol version {protocol_version} from {peer_addr}");
                                let _ = write_message(&mut tls_stream, &ClipboardMessage::Error { id: 0, code: 400 }).await;
                                return;
                            }

                            let ack = ClipboardMessage::HelloAck { protocol_version: 1 };
                            if let Err(e) = write_message(&mut tls_stream, &ack).await {
                                log::warn!("Failed to send clipboard HelloAck to {peer_addr}: {e}");
                            } else {
                                let mut outgoing_rx = super::CLIPBOARD_OUTGOING.subscribe();
                                // Active receive loop
                                loop {
                                    let active = *super::ACTIVE_CLIPBOARD_PEER.subscribe().borrow() == Some(handle);
                                    if !active {
                                        log::info!("Clipboard connection to {peer_addr} closed: peer is no longer the active clipboard peer");
                                        break;
                                    }

                                    tokio::select! {
                                        result = tokio::time::timeout(std::time::Duration::from_secs(2), read_message(&mut tls_stream)) => {
                                            match result {
                                                Ok(Ok(msg)) => {
                                                    log::info!("Clipboard received from {peer_addr}: {:?}", msg);
                                                    let _ = super::CLIPBOARD_INCOMING.send(msg);
                                                }
                                                Ok(Err(e)) => {
                                                    log::warn!("Clipboard connection to {peer_addr} closed: {e}");
                                                    break;
                                                }
                                                Err(_) => continue,
                                            }
                                        }
                                        Ok(msg) = outgoing_rx.recv() => {
                                            if *super::ACTIVE_CLIPBOARD_PEER.subscribe().borrow() == Some(handle) {
                                                if let Err(e) = write_message(&mut tls_stream, &msg).await {
                                                    log::warn!("Failed to write clipboard message to {peer_addr}: {e}");
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Ok(msg)) => log::warn!("Expected Hello, got {:?}", msg),
                        Ok(Err(e)) => log::warn!("Failed to read clipboard Hello from {peer_addr}: {e}"),
                        Err(_) => log::warn!("Timed out waiting for clipboard Hello from {peer_addr}"),
                    }
                }
                Err(e) => log::warn!("Clipboard TLS accept failed for {peer_addr}: {e}"),
            }
        });
    }
}
