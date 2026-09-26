use crate::clipboard::protocol::{ClipboardMessage, FileMetadata};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const CHUNK_SIZE: usize = 256 * 1024; // 256 KiB
const MAX_FILES: usize = 10_000;
const MAX_FILE_SIZE: u64 = 10 * 1024 * 1024 * 1024; // 10 GiB
const MAX_TOTAL_SIZE: u64 = 20 * 1024 * 1024 * 1024; // 20 GiB
const MAX_FILENAME_LEN: usize = 255;

struct ActiveOutgoingTransfer {
    id: u64,
    files: Vec<PathBuf>,
    file_metadata: Vec<FileMetadata>,
    ack_tx: Option<tokio::sync::mpsc::Sender<(u64, u32, u64)>>,
}

struct ActiveIncomingTransfer {
    id: u64,
    metadata: Vec<FileMetadata>,
    temp_dir: PathBuf,
    current_file_index: Option<u32>,
    current_file_hasher: Option<Sha256>,
    current_file_written: u64,
    current_file_handle: Option<tokio::fs::File>,
    start_time: std::time::Instant,
    total_write_time: std::time::Duration,
    total_hash_time: std::time::Duration,
    total_bytes_received: u64,
}

pub static EXPECTING_FILE_ECHO: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn init_file_task() {
    tokio::task::spawn_local(async {
        log::info!("Starting File Clipboard Task...");
        let mut local_change_rx = super::LOCAL_CLIPBOARD_CHANGED.subscribe();
        let mut incoming_rx = super::CLIPBOARD_INCOMING.subscribe();

        let mut transport_connected_rx = super::CLIPBOARD_TRANSPORT_CONNECTED.subscribe();

        let mut active_outgoing: Option<ActiveOutgoingTransfer> = None;
        let mut active_incoming: Option<ActiveIncomingTransfer> = None;

        // At startup, clear old cached transfers.
        let cache_base = std::env::temp_dir().join("lan-mouse/clipboard");
        if let Err(e) = tokio::fs::remove_dir_all(&cache_base).await {
            log::warn!(
                "Could not clear old file clipboard cache (might not exist): {}",
                e
            );
        }

        loop {
            tokio::select! {
                Ok(_) = local_change_rx.recv() => {
                    handle_local_clipboard_change(&mut active_outgoing).await;
                }
                Ok(msg) = incoming_rx.recv() => {
                    handle_incoming_message(msg, &mut active_outgoing, &mut active_incoming).await;
                }
                Ok(_) = transport_connected_rx.recv() => {
                    if let Some(outgoing) = &active_outgoing {
                        log::info!("Transport connected, resending FileOffer {}", outgoing.id);
                        let _ = super::CLIPBOARD_OUTGOING.send(ClipboardMessage::FileOffer {
                            id: outgoing.id,
                            files: outgoing.file_metadata.clone(),
                        });
                    }
                }
            }
        }
    });
}

async fn handle_local_clipboard_change(active_outgoing: &mut Option<ActiveOutgoingTransfer>) {
    let files = read_os_clipboard_files().await;
    if files.is_empty() {
        return; // Not a file copy event
    }

    let mut total_size = 0u64;
    let mut valid_files = Vec::new();
    let mut file_metadatas = Vec::new();

    for path in files {
        if valid_files.len() >= MAX_FILES {
            log::warn!("File limit reached, dropping remaining files.");
            break;
        }

        if let Ok(meta) = tokio::fs::metadata(&path).await {
            if meta.is_file() {
                let size = meta.len();
                if size > MAX_FILE_SIZE {
                    log::warn!("File {} exceeds max size, skipping.", path.display());
                    continue;
                }
                if total_size.saturating_add(size) > MAX_TOTAL_SIZE {
                    log::warn!("Total file size exceeds limit, stopping.");
                    break;
                }

                let file_name = match path.file_name().and_then(|n| n.to_str()) {
                    Some(name) => name.to_string(),
                    None => continue,
                };

                if file_name.len() > MAX_FILENAME_LEN {
                    log::warn!("Filename {} too long, skipping.", file_name);
                    continue;
                }

                total_size += size;
                valid_files.push(path);
                file_metadatas.push(FileMetadata {
                    name: file_name,
                    size,
                });
            }
        }
    }

    if valid_files.is_empty() {
        return;
    }

    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64; // Using nanos to avoid collisions with main clipboard task

    log::info!(
        "Detected local file copy, generating FileOffer {id} for {} files",
        valid_files.len()
    );

    *active_outgoing = Some(ActiveOutgoingTransfer {
        id,
        files: valid_files,
        file_metadata: file_metadatas.clone(),
        ack_tx: None,
    });

    let _ = super::CLIPBOARD_OUTGOING.send(ClipboardMessage::FileOffer {
        id,
        files: file_metadatas,
    });
}

async fn handle_incoming_message(
    msg: ClipboardMessage,
    active_outgoing: &mut Option<ActiveOutgoingTransfer>,
    active_incoming: &mut Option<ActiveIncomingTransfer>,
) {
    match msg {
        ClipboardMessage::FileOffer { id, files } => {
            log::info!("Received FileOffer {id} with {} files", files.len());
            // Receiver: create temp dir and send FileRequest
            let temp_dir = std::env::temp_dir()
                .join("lan-mouse/clipboard/")
                .join(id.to_string());
            if let Err(e) = tokio::fs::create_dir_all(&temp_dir).await {
                log::error!("Failed to create temp dir for files: {}", e);
                return;
            }

            *active_incoming = Some(ActiveIncomingTransfer {
                id,
                metadata: files.clone(),
                temp_dir,
                current_file_index: None,
                current_file_hasher: None,
                current_file_written: 0,
                current_file_handle: None,
                start_time: std::time::Instant::now(),
                total_write_time: std::time::Duration::ZERO,
                total_hash_time: std::time::Duration::ZERO,
                total_bytes_received: 0,
            });

            let indices = (0..files.len() as u32).collect();
            let _ = super::CLIPBOARD_OUTGOING.send(ClipboardMessage::FileRequest {
                id,
                file_indices: indices,
            });
        }
        ClipboardMessage::FileRequest { id, file_indices } => {
            log::info!("Received FileRequest for {id}");
            // Sender: begin streaming files
            let transfer = if let Some(t) = active_outgoing {
                if t.id == id {
                    t
                } else {
                    return;
                }
            } else {
                return;
            };

            let id = transfer.id;
            let files_to_send: Vec<(u32, PathBuf)> = file_indices
                .into_iter()
                .filter_map(|i| transfer.files.get(i as usize).map(|p| (i, p.clone())))
                .collect();

            let (ack_tx, mut ack_rx) = tokio::sync::mpsc::channel(2);
            transfer.ack_tx = Some(ack_tx);

            tokio::task::spawn_local(async move {
                let start_time = std::time::Instant::now();
                let mut total_bytes_sent = 0u64;
                let mut chunk_count = 0u64;
                let mut total_read_time = std::time::Duration::ZERO;
                let mut total_hash_time = std::time::Duration::ZERO;
                let mut total_ack_wait_time = std::time::Duration::ZERO;
                let mut min_ack_latency = std::time::Duration::MAX;
                let mut max_ack_latency = std::time::Duration::ZERO;

                for (index, path) in files_to_send {
                    log::info!("Attempting to open file for transfer: {}", path.display());
                    match tokio::fs::File::open(&path).await {
                        Ok(mut file) => {
                            let mut buf = vec![0u8; CHUNK_SIZE];
                            let mut offset = 0u64;
                            let mut hasher = Sha256::new();

                            loop {
                                let read_start = std::time::Instant::now();
                                match file.read(&mut buf).await {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        total_read_time += read_start.elapsed();

                                        let hash_start = std::time::Instant::now();
                                        hasher.update(&buf[..n]);
                                        total_hash_time += hash_start.elapsed();

                                        let chunk = ClipboardMessage::FileChunk {
                                            id,
                                            file_index: index,
                                            offset,
                                            data: buf[..n].to_vec(),
                                        };
                                        let _ = super::CLIPBOARD_OUTGOING.send(chunk);

                                        let ack_start = std::time::Instant::now();
                                        match tokio::time::timeout(
                                            std::time::Duration::from_secs(10),
                                            ack_rx.recv(),
                                        )
                                        .await
                                        {
                                            Ok(Some((ack_id, ack_idx, ack_off))) => {
                                                let ack_latency = ack_start.elapsed();
                                                total_ack_wait_time += ack_latency;
                                                min_ack_latency = min_ack_latency.min(ack_latency);
                                                max_ack_latency = max_ack_latency.max(ack_latency);
                                                chunk_count += 1;
                                                total_bytes_sent += n as u64;

                                                if ack_id != id || ack_idx != index || ack_off != offset {
                                                    log::warn!("Mismatched FileChunkAck: expected {id}:{index}:{offset}, got {ack_id}:{ack_idx}:{ack_off}");
                                                }
                                            }
                                            Ok(None) => return, // Cancelled
                                            Err(_) => {
                                                log::error!("Timeout waiting for FileChunkAck");
                                                let _ = super::CLIPBOARD_OUTGOING
                                                    .send(ClipboardMessage::Error { id, code: 504 });
                                                return;
                                            }
                                        }

                                        offset += n as u64;
                                    }
                                    Err(e) => {
                                        log::error!("Error reading file {}: {}", path.display(), e);
                                        let _ = super::CLIPBOARD_OUTGOING
                                            .send(ClipboardMessage::Error { id, code: 500 });
                                        return;
                                    }
                                }
                            }

                            let hash = hasher.finalize();
                            let mut hash_arr = [0u8; 32];
                            hash_arr.copy_from_slice(&hash);

                            log::info!("Finished reading file {}, sending FileComplete", path.display());
                            let _ = super::CLIPBOARD_OUTGOING.send(ClipboardMessage::FileComplete {
                                id,
                                file_index: index,
                                size: offset,
                                sha256: hash_arr,
                            });
                        }
                        Err(e) => {
                            log::error!("Failed to open file for transfer {}: {}", path.display(), e);
                            let _ = super::CLIPBOARD_OUTGOING
                                .send(ClipboardMessage::Error { id, code: 500 });
                        }
                    }
                }
                
                let total_time = start_time.elapsed();
                log::info!("=== SENDER TRANSFER STATS ===");
                log::info!("Total time: {:?}", total_time);
                log::info!("Total bytes: {}", total_bytes_sent);
                if total_time.as_secs_f64() > 0.0 {
                    log::info!("Throughput: {:.2} MB/s", (total_bytes_sent as f64 / 1_000_000.0) / total_time.as_secs_f64());
                }
                log::info!("Chunks: {}", chunk_count);
                log::info!("Chunk size: {}", CHUNK_SIZE);
                if chunk_count > 0 {
                    log::info!("Avg ACK latency: {:?}", total_ack_wait_time / chunk_count as u32);
                    log::info!("Min ACK latency: {:?}", min_ack_latency);
                    log::info!("Max ACK latency: {:?}", max_ack_latency);
                }
                log::info!("Total time waiting for ACKs: {:?}", total_ack_wait_time);
                log::info!("Total read time: {:?}", total_read_time);
                log::info!("Total hash time: {:?}", total_hash_time);
            });
        }
        ClipboardMessage::FileChunk {
            id,
            file_index,
            offset,
            data,
        } => {
            // Receiver: write chunk to disk
            if let Some(transfer) = active_incoming {
                if transfer.id == id {
                    if transfer.current_file_index != Some(file_index) {
                        // Switch file
                        if let Some(f) = transfer.metadata.get(file_index as usize) {
                            let safe_name = Path::new(&f.name).file_name().unwrap_or_default();
                            let part_path =
                                transfer.temp_dir.join(safe_name).with_extension("part");
                            if let Ok(file) = tokio::fs::OpenOptions::new()
                                .create(true)
                                .write(true)
                                .truncate(true)
                                .open(&part_path)
                                .await
                            {
                                transfer.current_file_handle = Some(file);
                                transfer.current_file_index = Some(file_index);
                                transfer.current_file_hasher = Some(Sha256::new());
                                transfer.current_file_written = 0;
                            }
                        }
                    }

                    if let Some(file) = &mut transfer.current_file_handle {
                        if transfer.current_file_written == offset {
                            let write_start = std::time::Instant::now();
                            if let Ok(_) = file.write_all(&data).await {
                                transfer.total_write_time += write_start.elapsed();
                                transfer.current_file_written += data.len() as u64;
                                transfer.total_bytes_received += data.len() as u64;

                                let hash_start = std::time::Instant::now();
                                if let Some(hasher) = &mut transfer.current_file_hasher {
                                    hasher.update(&data);
                                }
                                transfer.total_hash_time += hash_start.elapsed();

                                let _ = super::CLIPBOARD_OUTGOING.send(ClipboardMessage::FileChunkAck {
                                    id,
                                    file_index,
                                    offset,
                                });
                            }
                        } else {
                            log::warn!(
                                "Out of order chunk received, ignoring (expected offset {}, got {})",
                                transfer.current_file_written,
                                offset
                            );
                        }
                    }
                }
            }
        }
        ClipboardMessage::FileComplete {
            id,
            file_index,
            size,
            sha256,
        } => {
            // Receiver: verify hash and rename
            if let Some(transfer) = active_incoming {
                if transfer.id == id && transfer.current_file_index == Some(file_index) {
                    if transfer.current_file_written == size {
                        if let Some(hasher) = transfer.current_file_hasher.take() {
                            let hash = hasher.finalize();
                            if hash.as_slice() == sha256 {
                                if let Some(f) = transfer.metadata.get(file_index as usize) {
                                    let safe_name =
                                        Path::new(&f.name).file_name().unwrap_or_default();
                                    let part_path =
                                        transfer.temp_dir.join(safe_name).with_extension("part");
                                    let final_path = transfer.temp_dir.join(safe_name);

                                    // Flush file
                                    if let Some(mut file) = transfer.current_file_handle.take() {
                                        let _ = file.flush().await;
                                    }

                                    if let Err(e) = tokio::fs::rename(&part_path, &final_path).await
                                    {
                                        log::error!(
                                            "Failed to rename part file to {}: {}",
                                            final_path.display(),
                                            e
                                        );
                                    } else {
                                        log::info!(
                                            "File transfer complete: {}",
                                            final_path.display()
                                        );
                                    }
                                }
                            } else {
                                log::error!("Hash mismatch for file {}", file_index);
                                if let Some(file) = transfer.current_file_handle.take() {
                                    drop(file);
                                }
                                if let Some(f) = transfer.metadata.get(file_index as usize) {
                                    let safe_name =
                                        Path::new(&f.name).file_name().unwrap_or_default();
                                    let part_path =
                                        transfer.temp_dir.join(safe_name).with_extension("part");
                                    let _ = tokio::fs::remove_file(part_path).await;
                                }
                            }
                        }
                    }

                    // Check if all files are complete
                    let is_last = file_index as usize == transfer.metadata.len() - 1;
                    if is_last {
                        let total_time = transfer.start_time.elapsed();
                        log::info!("=== RECEIVER TRANSFER STATS ===");
                        log::info!("Total time: {:?}", total_time);
                        log::info!("Total bytes: {}", transfer.total_bytes_received);
                        if total_time.as_secs_f64() > 0.0 {
                            log::info!("Throughput: {:.2} MB/s", (transfer.total_bytes_received as f64 / 1_000_000.0) / total_time.as_secs_f64());
                        }
                        log::info!("Total write time: {:?}", transfer.total_write_time);
                        log::info!("Total hash time: {:?}", transfer.total_hash_time);

                        log::info!("All files complete, writing to OS clipboard.");
                        let mut final_paths = Vec::new();
                        for m in &transfer.metadata {
                            let safe_name = Path::new(&m.name).file_name().unwrap_or_default();
                            final_paths.push(transfer.temp_dir.join(safe_name));
                        }

                        write_os_clipboard_files(final_paths).await;
                        *active_incoming = None;
                    }
                }
            }
        }
        ClipboardMessage::Error { id, .. } => {
            if let Some(transfer) = active_incoming {
                if transfer.id == id {
                    log::warn!("Transfer failed, cleaning up temp dir");
                    let _ = tokio::fs::remove_dir_all(&transfer.temp_dir).await;
                    *active_incoming = None;
                }
            }
            if let Some(transfer) = active_outgoing {
                if transfer.id == id {
                    *active_outgoing = None;
                }
            }
        }
        ClipboardMessage::FileChunkAck { id, file_index, offset } => {
            if let Some(transfer) = active_outgoing {
                if transfer.id == id {
                    if let Some(tx) = &transfer.ack_tx {
                        let _ = tx.try_send((id, file_index, offset));
                    }
                }
            }
        }
        _ => {}
    }
}

#[cfg(target_os = "windows")]
async fn read_os_clipboard_files() -> Vec<PathBuf> {
    tokio::task::spawn_blocking(|| {
        let mut paths = Vec::new();
        if let Ok(_clip) = clipboard_win::Clipboard::new_attempts(5) {
            if clipboard_win::is_format_avail(15) {
                // CF_HDROP
                if let Ok(p) =
                    clipboard_win::get_clipboard::<Vec<String>, _>(clipboard_win::formats::FileList)
                {
                    paths = p.into_iter().map(PathBuf::from).collect();
                }
            }
        }
        paths
    })
    .await
    .unwrap_or_default()
}

#[cfg(all(unix, not(target_os = "macos")))]
async fn read_os_clipboard_files() -> Vec<PathBuf> {
    let output = match tokio::process::Command::new("wl-paste")
        .arg("--type")
        .arg("text/uri-list")
        .output()
        .await
    {
        Ok(o) => o,
        Err(_) => return Vec::new(),
    };

    if !output.status.success() {
        return Vec::new();
    }

    let mut paths = Vec::new();
    if let Ok(s) = String::from_utf8(output.stdout) {
        for line in s.lines() {
            let t = line.trim();
            if t.starts_with("file://") {
                let uri = &t[7..]; // Strip file://
                // Very basic url decode (e.g. %20 -> space)
                // We should decode correctly, but for basic usage replace %20
                let decoded = uri.replace("%20", " ");
                paths.push(PathBuf::from(decoded));
            }
        }
    }
    paths
}

#[cfg(target_os = "windows")]
async fn write_os_clipboard_files(paths: Vec<PathBuf>) {
    EXPECTING_FILE_ECHO.store(true, std::sync::atomic::Ordering::SeqCst);
    tokio::task::spawn_blocking(move || {
        use clipboard_win::Setter;
        if let Ok(_clip) = clipboard_win::Clipboard::new_attempts(5) {
            let string_paths: Vec<String> = paths
                .into_iter()
                .filter_map(|p| p.to_str().map(|s| s.to_string()))
                .collect();
            let _ = clipboard_win::formats::FileList.write_clipboard(&string_paths);
        }
    })
    .await
    .unwrap_or(());
}

#[cfg(all(unix, not(target_os = "macos")))]
async fn write_os_clipboard_files(paths: Vec<PathBuf>) {
    let mut uri_list = String::new();
    
    for path in paths.iter() {
        if let Some(s) = path.to_str() {
            // Basic url-encoding for spaces
            let encoded = s.replace(" ", "%20");
            let uri = format!("file://{}", encoded);
            
            uri_list.push_str(&uri);
            // Dolphin Native Copy always terminates every URI line (including the last one) with CRLF.
            uri_list.push_str("\r\n");
        }
    }

    // Detach into an OS thread rather than a tokio blocking task.
    // This ensures wl_clipboard_rs's internal Wayland event loop is not inadvertently
    // interrupted by the Tokio runtime's lifecycle management.
    EXPECTING_FILE_ECHO.store(true, std::sync::atomic::Ordering::SeqCst);
    std::thread::spawn(move || {
        let mut opts = wl_clipboard_rs::copy::Options::new();
        opts.foreground(true); // Keep ownership of the clipboard

        // Provide the exact MIME types Dolphin advertised in the control experiment
        let sources = vec![
            wl_clipboard_rs::copy::MimeSource {
                mime_type: wl_clipboard_rs::copy::MimeType::Specific("text/uri-list".to_string()),
                source: wl_clipboard_rs::copy::Source::Bytes(uri_list.clone().into_bytes().into()),
            },
            wl_clipboard_rs::copy::MimeSource {
                mime_type: wl_clipboard_rs::copy::MimeType::Specific(
                    "application/x-kde4-urilist".to_string(),
                ),
                source: wl_clipboard_rs::copy::Source::Bytes(uri_list.into_bytes().into()),
            },
            wl_clipboard_rs::copy::MimeSource {
                mime_type: wl_clipboard_rs::copy::MimeType::Specific(
                    "application/vnd.portal.filetransfer".to_string(),
                ),
                // XDG portal filetransfer relies on a session token, but providing an empty payload 
                // or just the URI list often satisfies listeners looking for the MIME signature.
                source: wl_clipboard_rs::copy::Source::Bytes(Vec::new().into()),
            }
        ];

        if let Err(e) = opts.copy_multi(sources) {
            log::error!("Failed to write files to Wayland clipboard: {}", e);
        }
    });
}
