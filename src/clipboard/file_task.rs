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
}

pub fn init_file_task() {
    tokio::task::spawn_local(async {
        log::info!("Starting File Clipboard Task...");
        let mut local_change_rx = super::LOCAL_CLIPBOARD_CHANGED.subscribe();
        let mut incoming_rx = super::CLIPBOARD_INCOMING.subscribe();

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
                for (index, path) in files_to_send {
                    log::info!("Attempting to open file for transfer: {}", path.display());
                    match tokio::fs::File::open(&path).await {
                        Ok(mut file) => {
                            let mut buf = vec![0u8; CHUNK_SIZE];
                            let mut offset = 0u64;
                            let mut hasher = Sha256::new();

                            loop {
                                match file.read(&mut buf).await {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        hasher.update(&buf[..n]);
                                        let chunk = ClipboardMessage::FileChunk {
                                            id,
                                            file_index: index,
                                            offset,
                                            data: buf[..n].to_vec(),
                                        };
                                        let _ = super::CLIPBOARD_OUTGOING.send(chunk);

                                        match tokio::time::timeout(
                                            std::time::Duration::from_secs(10),
                                            ack_rx.recv(),
                                        )
                                        .await
                                        {
                                            Ok(Some((ack_id, ack_idx, ack_off))) => {
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
                            if let Ok(_) = file.write_all(&data).await {
                                transfer.current_file_written += data.len() as u64;
                                if let Some(hasher) = &mut transfer.current_file_hasher {
                                    hasher.update(&data);
                                }
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
    
    for (i, path) in paths.iter().enumerate() {
        if let Some(s) = path.to_str() {
            // A very basic encoding for spaces. In a robust implementation,
            // we'd use a full url-encoding scheme.
            let encoded = s.replace(" ", "%20");
            let uri = format!("file://{}", encoded);
            
            uri_list.push_str(&uri);
            
            // Separate multiple files with CRLF
            if i < paths.len() - 1 {
                uri_list.push_str("\r\n");
            }
        }
    }

    // Shell out to wl-copy, matching our wl-paste strategy.
    // wl-copy automatically forks into the background and serves the clipboard,
    // avoiding the Wayland event loop drop issues of wl_clipboard_rs in a blocking thread.
    let mut child = match tokio::process::Command::new("wl-copy")
        .arg("--type")
        .arg("text/uri-list")
        .stdin(std::process::Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            log::error!("Failed to spawn wl-copy: {}", e);
            return;
        }
    };

    if let Some(mut stdin) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        if let Err(e) = stdin.write_all(uri_list.as_bytes()).await {
            log::error!("Failed to write to wl-copy stdin: {}", e);
        }
    }

    let _ = child.wait().await;
}
