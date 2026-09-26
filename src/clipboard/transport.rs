use super::protocol::{ClipboardMessage, ProtocolError};
use rustls::ClientConfig;
use rustls::ServerConfig;
use rustls::pki_types::ServerName;
use std::collections::HashMap;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;
use webrtc_dtls::crypto::Certificate;

pub const MAX_CLIPBOARD_FRAME_SIZE: u32 = 100 * 1024 * 1024; // 100 MB

pub async fn read_message<R: AsyncReadExt + Unpin>(
    stream: &mut R,
) -> Result<ClipboardMessage, ProtocolError> {
    let length = stream.read_u32().await?;
    if length > MAX_CLIPBOARD_FRAME_SIZE {
        // Read and discard the oversized payload to keep the stream synchronized
        let mut remaining = length as usize;
        let mut discard_buf = [0u8; 8192];
        while remaining > 0 {
            let to_read = std::cmp::min(remaining, discard_buf.len());
            match tokio::time::timeout(
                std::time::Duration::from_secs(2),
                stream.read(&mut discard_buf[..to_read]),
            )
            .await
            {
                Ok(Ok(0)) => {
                    return Err(ProtocolError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "EOF during discard",
                    )));
                }
                Ok(Ok(n)) => remaining -= n,
                Ok(Err(e)) => return Err(ProtocolError::Io(e)),
                Err(_) => {
                    return Err(ProtocolError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Transfer stalled during discard",
                    )));
                }
            }
        }
        return Err(ProtocolError::FrameTooLarge(length));
    }

    let mut buf = vec![0u8; length as usize];
    let mut read_so_far = 0;
    while read_so_far < length as usize {
        match tokio::time::timeout(
            std::time::Duration::from_secs(2),
            stream.read(&mut buf[read_so_far..]),
        )
        .await
        {
            Ok(Ok(0)) => {
                return Err(ProtocolError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "EOF during transfer",
                )));
            }
            Ok(Ok(n)) => read_so_far += n,
            Ok(Err(e)) => return Err(ProtocolError::Io(e)),
            Err(_) => {
                return Err(ProtocolError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Transfer stalled",
                )));
            }
        }
    }

    let mut cursor = Cursor::new(buf);
    ClipboardMessage::decode(&mut cursor)
}

pub async fn write_message<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    msg: &ClipboardMessage,
    version: u16,
) -> Result<(), ProtocolError> {
    let mut buf = Vec::new();
    msg.encode(&mut buf, version)?;

    let length = buf.len() as u32;
    if length == 0 {
        return Ok(());
    }
    if length > MAX_CLIPBOARD_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge(length));
    }

    stream.write_u32(length).await?;
    stream.write_all(&buf).await?;
    stream.flush().await?;

    Ok(())
}

pub async fn connect_clipboard(
    expected_fingerprint: String,
    addr: SocketAddr,
    cert: Certificate,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Initiating clipboard TCP connection to {addr}...");

    // 1. Establish TCP
    let tcp_stream = tokio::net::TcpStream::connect(addr).await?;
    let _ = tcp_stream.set_nodelay(true);

    // 2. Setup TLS Config using existing certificate identity
    let cert_chain = cert.certificate.clone();
    let private_key =
        rustls::pki_types::PrivateKeyDer::Pkcs8(cert.private_key.serialized_der.clone().into());

    let verifier = Arc::new(super::auth::LanMouseServerVerifier::new(
        expected_fingerprint,
    ));

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
    let hello = ClipboardMessage::Hello {
        protocol_version: 4,
    };
    write_message(&mut tls_stream, &hello, 4).await?;

    // Wait for HelloAck from Server
    let session_version = match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        read_message(&mut tls_stream),
    )
    .await
    {
        Ok(Ok(ClipboardMessage::HelloAck { protocol_version })) => {
            log::info!("Clipboard HelloAck from {addr} (v{protocol_version})");
            protocol_version
        }
        Ok(Ok(msg)) => {
            log::warn!("Expected HelloAck, got {:?}", msg);
            return Err("Expected HelloAck".into());
        }
        Ok(Err(e)) => {
            log::warn!("Failed to read clipboard HelloAck from {addr}: {e}");
            return Err(e.into());
        }
        Err(_) => {
            log::warn!("Timed out waiting for clipboard HelloAck from {addr}");
            return Err("Timed out waiting for HelloAck".into());
        }
    };

    let mut outgoing_rx = super::CLIPBOARD_OUTGOING.subscribe();
    let _ = super::CLIPBOARD_TRANSPORT_CONNECTED.send(());

    let (mut rx, mut tx) = tokio::io::split(tls_stream);

    let received_offers = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::<
        u64,
    >::new()));
    let received_requests = std::sync::Arc::new(std::sync::Mutex::new(
        std::collections::HashSet::<u64>::new(),
    ));

    // 5. Active receive and write loops
    let write_task = tokio::task::spawn_local({
        let received_offers = received_offers.clone();
        let received_requests = received_requests.clone();
        async move {
            loop {
                match outgoing_rx.recv().await {
                    Ok(msg) => {
                        match &msg {
                            ClipboardMessage::Request { id, .. } => {
                                if !received_offers.lock().unwrap().contains(id) {
                                    continue;
                                }
                            }
                            ClipboardMessage::Data { id, .. }
                            | ClipboardMessage::Error { id, .. }
                                if !received_requests.lock().unwrap().contains(id) =>
                            {
                                continue;
                            }
                            _ => {}
                        }

                        if let Err(e) = write_message(&mut tx, &msg, session_version).await {
                            log::warn!("Failed to write clipboard message to {addr}: {e}");
                            match e {
                                ProtocolError::FrameTooLarge(_) => continue,
                                _ => break,
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("Clipboard transport lagged behind by {} messages", n);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        }
    });

    loop {
        match read_message(&mut rx).await {
            Ok(msg) => {
                if let ClipboardMessage::Offer { id, .. } = &msg {
                    received_offers.lock().unwrap().insert(*id);
                } else if let ClipboardMessage::Request { id, .. } = &msg {
                    received_requests.lock().unwrap().insert(*id);
                }
                if let ClipboardMessage::Data { id, data, .. } = &msg {
                    log::info!(
                        "Clipboard received Data for id {id} (len: {}) from {addr}",
                        data.len()
                    );
                } else if let ClipboardMessage::FileChunk {
                    id,
                    file_index,
                    offset,
                    data,
                } = &msg
                {
                    log::info!(
                        "Clipboard received FileChunk for id {id} file {file_index} offset {offset} (len: {}) from {addr}",
                        data.len()
                    );
                } else {
                    log::info!("Clipboard received from {addr}: {:?}", msg);
                }
                if let ClipboardMessage::Hello { protocol_version } = msg {
                    if protocol_version != 1 {
                        log::warn!("Unsupported protocol version {protocol_version} from {addr}");
                        break;
                    }
                } else {
                    let _ = super::CLIPBOARD_INCOMING.send(msg);
                }
            }
            Err(ProtocolError::FrameTooLarge(len)) => {
                log::error!(
                    "Clipboard frame too large ({} bytes). Sending 413 Error to {addr}.",
                    len
                );
                let _ =
                    super::CLIPBOARD_OUTGOING.send(ClipboardMessage::Error { id: 0, code: 413 });
                continue;
            }
            Err(e) => {
                log::warn!("Clipboard connection to {addr} closed: {e}");
                write_task.abort();
                break;
            }
        }
    }

    Ok(())
}

pub async fn listen_clipboard(
    listener: TcpListener,
    cert: Certificate,
    authorized_keys: Arc<RwLock<HashMap<String, String>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup TLS Server Config
    let cert_chain = cert.certificate.clone();
    let private_key =
        rustls::pki_types::PrivateKeyDer::Pkcs8(cert.private_key.serialized_der.clone().into());

    let verifier = Arc::new(super::auth::LanMouseClientVerifier::new(
        authorized_keys.clone(),
    ));

    let config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(cert_chain, private_key)?;

    let acceptor = TlsAcceptor::from(Arc::new(config));

    // 2. Accept Loop
    loop {
        let (tcp_stream, peer_addr) = listener.accept().await?;
        let _ = tcp_stream.set_nodelay(true);
        let acceptor = acceptor.clone();

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
                    log::info!(
                        "Clipboard TLS connection accepted from {peer_addr} (fingerprint: {fingerprint})"
                    );

                    // Wait for Hello from Client
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        read_message(&mut tls_stream),
                    )
                    .await
                    {
                        Ok(Ok(ClipboardMessage::Hello { protocol_version })) => {
                            log::info!("Clipboard Hello from {peer_addr} (v{protocol_version})");

                            let session_version = std::cmp::min(protocol_version, 4);

                            let ack = ClipboardMessage::HelloAck {
                                protocol_version: session_version,
                            };
                            if let Err(e) =
                                write_message(&mut tls_stream, &ack, session_version).await
                            {
                                log::warn!("Failed to send clipboard HelloAck to {peer_addr}: {e}");
                            } else {
                                let mut outgoing_rx = super::CLIPBOARD_OUTGOING.subscribe();
                                let _ = super::CLIPBOARD_TRANSPORT_CONNECTED.send(());
                                let (mut rx, mut tx) = tokio::io::split(tls_stream);

                                let received_offers = std::sync::Arc::new(std::sync::Mutex::new(
                                    std::collections::HashSet::<u64>::new(),
                                ));
                                let received_requests = std::sync::Arc::new(std::sync::Mutex::new(
                                    std::collections::HashSet::<u64>::new(),
                                ));

                                // Active receive and write loops
                                let write_task = tokio::task::spawn_local({
                                    let received_offers = received_offers.clone();
                                    let received_requests = received_requests.clone();
                                    async move {
                                        loop {
                                            match outgoing_rx.recv().await {
                                                Ok(msg) => {
                                                    match &msg {
                                                        ClipboardMessage::Request { id, .. } => {
                                                            if !received_offers.lock().unwrap().contains(id)
                                                            {
                                                                continue;
                                                            }
                                                        }
                                                        ClipboardMessage::Data { id, .. }
                                                        | ClipboardMessage::Error { id, .. }
                                                            if !received_requests
                                                                .lock()
                                                                .unwrap()
                                                                .contains(id) =>
                                                        {
                                                            continue;
                                                        }
                                                        _ => {}
                                                    }

                                                    if let Err(e) =
                                                        write_message(&mut tx, &msg, session_version).await
                                                    {
                                                        log::warn!(
                                                            "Failed to write clipboard message to {peer_addr}: {e}"
                                                        );
                                                        match e {
                                                            ProtocolError::FrameTooLarge(_) => continue,
                                                            _ => break,
                                                        }
                                                    }
                                                }
                                                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                                    log::warn!("Clipboard transport lagged behind by {} messages", n);
                                                }
                                                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                });

                                loop {
                                    match read_message(&mut rx).await {
                                        Ok(msg) => {
                                            if let ClipboardMessage::Offer { id, .. } = &msg {
                                                received_offers.lock().unwrap().insert(*id);
                                            } else if let ClipboardMessage::Request { id, .. } =
                                                &msg
                                            {
                                                received_requests.lock().unwrap().insert(*id);
                                            }
                                            if let ClipboardMessage::Data { id, data, .. } = &msg {
                                                log::info!(
                                                    "Clipboard received Data for id {id} (len: {}) from {peer_addr}",
                                                    data.len()
                                                );
                                            } else if let ClipboardMessage::FileChunk {
                                                id,
                                                file_index,
                                                offset,
                                                data,
                                            } = &msg
                                            {
                                                log::info!(
                                                    "Clipboard received FileChunk for id {id} file {file_index} offset {offset} (len: {}) from {peer_addr}",
                                                    data.len()
                                                );
                                            } else {
                                                log::info!(
                                                    "Clipboard received from {peer_addr}: {:?}",
                                                    msg
                                                );
                                            }
                                            let _ = super::CLIPBOARD_INCOMING.send(msg);
                                        }
                                        Err(ProtocolError::FrameTooLarge(len)) => {
                                            log::error!(
                                                "Clipboard frame too large ({} bytes). Sending 413 Error to {peer_addr}.",
                                                len
                                            );
                                            let _ = super::CLIPBOARD_OUTGOING
                                                .send(ClipboardMessage::Error { id: 0, code: 413 });
                                            continue;
                                        }
                                        Err(e) => {
                                            log::warn!(
                                                "Clipboard connection to {peer_addr} closed: {e}"
                                            );
                                            write_task.abort();
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Ok(msg)) => log::warn!("Expected Hello, got {:?}", msg),
                        Ok(Err(e)) => {
                            log::warn!("Failed to read clipboard Hello from {peer_addr}: {e}")
                        }
                        Err(_) => {
                            log::warn!("Timed out waiting for clipboard Hello from {peer_addr}")
                        }
                    }
                }
                Err(e) => log::warn!("Clipboard TLS accept failed for {peer_addr}: {e}"),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_clipboard_frame_size_limits() {
        let mut stream = Vec::new();

        // 1. Exactly MAX_CLIPBOARD_FRAME_SIZE frame -> accepted
        let exactly_max = ClipboardMessage::Data {
            id: 1,
            mime_type: "text/plain".to_string(),
            // 13 bytes overhead: 1 msg_type + 8 id + 4 len (for V1 text/plain)
            data: vec![0u8; (MAX_CLIPBOARD_FRAME_SIZE - 13) as usize],
        };
        assert!(write_message(&mut stream, &exactly_max, 1).await.is_ok());

        let mut cursor = Cursor::new(&stream);
        let decoded = read_message(&mut cursor).await;
        assert!(decoded.is_ok());

        // 2. 10 MiB + 1 byte -> rejected
        let mut stream = Vec::new();
        let over_max = ClipboardMessage::Data {
            id: 1,
            mime_type: "text/plain".to_string(),
            data: vec![0u8; (MAX_CLIPBOARD_FRAME_SIZE - 12) as usize],
        };
        let res = write_message(&mut stream, &over_max, 1).await;
        assert!(matches!(res, Err(ProtocolError::FrameTooLarge(_))));

        // 3. 15 MiB -> rejected cleanly on read before allocation
        let mut stream = Vec::new();
        let len = 15 * 1024 * 1024_u32;
        use tokio::io::AsyncWriteExt;
        let mut cursor = std::io::Cursor::new(&mut stream);
        cursor.write_u32(len).await.unwrap();
        // The read_message should reject it just by reading the length
        let mut cursor = Cursor::new(&stream);
        let res = read_message(&mut cursor).await;
        // Since we don't write the payload, the discard loop will hit EOF
        match res {
            Err(ProtocolError::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof),
            _ => panic!("Expected UnexpectedEof due to missing payload"),
        }

        // 4. Malformed/huge frame length -> rejected before allocation
        let mut stream = Vec::new();
        let len = 0xFFFFFFFF_u32;
        let mut cursor = std::io::Cursor::new(&mut stream);
        cursor.write_u32(len).await.unwrap();
        let mut cursor = Cursor::new(&stream);
        let res = read_message(&mut cursor).await;
        match res {
            Err(ProtocolError::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof),
            _ => panic!("Expected UnexpectedEof due to missing payload"),
        }

        // 5. Invalid protocol message type
        let mut stream = Vec::new();
        let mut cursor = std::io::Cursor::new(&mut stream);
        cursor.write_u32(1).await.unwrap();
        cursor.write_u8(99).await.unwrap(); // Unknown type 99
        let mut cursor = Cursor::new(&stream);
        let res = read_message(&mut cursor).await;
        assert!(matches!(res, Err(ProtocolError::InvalidMessageType(99))));

        // 6. TCP disconnect during active transfer (UnexpectedEof)
        let mut stream = Vec::new();
        let mut cursor = std::io::Cursor::new(&mut stream);
        cursor.write_u32(100).await.unwrap(); // Expecting 100 bytes
        cursor.write_all(&[1, 2, 3]).await.unwrap(); // Only 3 bytes sent before EOF
        let mut cursor = Cursor::new(&stream);
        let res = read_message(&mut cursor).await;
        match res {
            Err(ProtocolError::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof),
            _ => panic!("Expected UnexpectedEof"),
        }
    }

    #[tokio::test]
    async fn test_v1_v2_compatibility() {
        let msg_offer = ClipboardMessage::Offer {
            id: 42,
            mime_types: vec!["text/html".to_string(), "text/plain".to_string()],
        };

        let msg_data = ClipboardMessage::Data {
            id: 42,
            mime_type: "text/html".to_string(),
            data: b"hello".to_vec(),
        };

        // 1. V1 Serialisation strips to text/plain and legacy format
        let mut v1_stream = Vec::new();
        write_message(&mut v1_stream, &msg_offer, 1).await.unwrap();

        let mut cursor = Cursor::new(&v1_stream);
        let decoded_offer = read_message(&mut cursor).await.unwrap();

        // When reading from V1 stream (which doesn't encode version in stream for Offer),
        // we parse it as V1 and read_message natively yields text/plain.
        if let ClipboardMessage::Offer { id, mime_types } = decoded_offer {
            assert_eq!(id, 42);
            assert_eq!(mime_types, vec!["text/plain".to_string()]);
        } else {
            panic!("Expected Offer");
        }

        let msg_data_v1 = ClipboardMessage::Data {
            id: 42,
            mime_type: "text/plain".to_string(),
            data: b"hello".to_vec(),
        };

        // V1 Data serialisation
        let mut v1_stream = Vec::new();
        write_message(&mut v1_stream, &msg_data_v1, 1)
            .await
            .unwrap();
        let mut cursor = Cursor::new(&v1_stream);
        let decoded_data = read_message(&mut cursor).await.unwrap();

        if let ClipboardMessage::Data {
            id,
            mime_type,
            data,
        } = decoded_data
        {
            assert_eq!(id, 42);
            assert_eq!(mime_type, "text/plain");
            assert_eq!(data, b"hello");
        } else {
            panic!("Expected Data");
        }

        // 2. V2 Serialisation preserves HTML and fields
        let mut v2_stream = Vec::new();
        write_message(&mut v2_stream, &msg_offer, 2).await.unwrap();
        let mut cursor = Cursor::new(&v2_stream);
        let decoded_offer = read_message(&mut cursor).await.unwrap();

        if let ClipboardMessage::Offer { id, mime_types } = decoded_offer {
            assert_eq!(id, 42);
            assert_eq!(
                mime_types,
                vec!["text/html".to_string(), "text/plain".to_string()]
            );
        } else {
            panic!("Expected Offer");
        }

        let mut v2_stream = Vec::new();
        write_message(&mut v2_stream, &msg_data, 2).await.unwrap();
        let mut cursor = Cursor::new(&v2_stream);
        let decoded_data = read_message(&mut cursor).await.unwrap();

        if let ClipboardMessage::Data {
            id,
            mime_type,
            data,
        } = decoded_data
        {
            assert_eq!(id, 42);
            assert_eq!(mime_type, "text/html");
            assert_eq!(data, b"hello");
        } else {
            panic!("Expected Data");
        }
    }
}
