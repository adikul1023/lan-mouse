use std::io::{self, Read, Write};
use std::string::FromUtf8Error;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid UTF-8 string: {0}")]
    Utf8(#[from] FromUtf8Error),
    #[error("Invalid message type ID: {0}")]
    InvalidMessageType(u8),
    #[error("Payload too short")]
    PayloadTooShort,
    #[error("Frame too large: {0}")]
    FrameTooLarge(u32),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ClipboardMessage {
    Hello {
        protocol_version: u16,
    },
    HelloAck {
        protocol_version: u16,
    },
    Offer {
        id: u64,
        mime_types: Vec<String>,
    },
    Request {
        id: u64,
        mime_types: Vec<String>,
    },
    Data {
        id: u64,
        mime_type: String,
        data: Vec<u8>,
    },
    Error {
        id: u64,
        code: u16,
    },
    FileOffer {
        id: u64,
        files: Vec<FileMetadata>,
    },
    FileRequest {
        id: u64,
        file_indices: Vec<u32>,
    },
    FileChunk {
        id: u64,
        file_index: u32,
        offset: u64,
        data: Vec<u8>,
    },
    FileComplete {
        id: u64,
        file_index: u32,
        size: u64,
        sha256: [u8; 32],
    },
    FileChunkAck {
        id: u64,
        file_index: u32,
        offset: u64,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FileMetadata {
    pub name: String,
    pub size: u64,
}

impl ClipboardMessage {
    pub const MSG_HELLO: u8 = 1;
    pub const MSG_HELLO_ACK: u8 = 2;
    pub const MSG_OFFER: u8 = 3;
    pub const MSG_REQUEST: u8 = 4;
    pub const MSG_DATA: u8 = 5;
    pub const MSG_ERROR: u8 = 6;
    pub const MSG_OFFER_V2: u8 = 7;
    pub const MSG_REQUEST_V2: u8 = 8;
    pub const MSG_DATA_V2: u8 = 9;
    pub const MSG_REQUEST_V3: u8 = 10;
    pub const MSG_FILE_OFFER: u8 = 11;
    pub const MSG_FILE_REQUEST: u8 = 12;
    pub const MSG_FILE_CHUNK: u8 = 13;
    pub const MSG_FILE_COMPLETE: u8 = 14;
    pub const MSG_FILE_CHUNK_ACK: u8 = 15;

    pub fn encode(&self, out: &mut impl Write, version: u16) -> io::Result<()> {
        match self {
            ClipboardMessage::Hello { protocol_version } => {
                out.write_all(&[Self::MSG_HELLO])?;
                out.write_all(&protocol_version.to_be_bytes())?;
            }
            ClipboardMessage::HelloAck { protocol_version } => {
                out.write_all(&[Self::MSG_HELLO_ACK])?;
                out.write_all(&protocol_version.to_be_bytes())?;
            }
            ClipboardMessage::Offer { id, mime_types } => {
                if version >= 2 {
                    out.write_all(&[Self::MSG_OFFER_V2])?;
                    out.write_all(&id.to_be_bytes())?;
                    let count = std::cmp::min(mime_types.len(), 255) as u8;
                    out.write_all(&[count])?;
                    for mime in mime_types.iter().take(count as usize) {
                        let bytes = mime.as_bytes();
                        let len = std::cmp::min(bytes.len(), 255) as u8;
                        out.write_all(&[len])?;
                        out.write_all(&bytes[..len as usize])?;
                    }
                } else if mime_types.contains(&"text/plain".to_string()) {
                    out.write_all(&[Self::MSG_OFFER])?;
                    out.write_all(&id.to_be_bytes())?;
                    let bytes = b"text/plain";
                    out.write_all(&(bytes.len() as u16).to_be_bytes())?;
                    out.write_all(bytes)?;
                }
            }
            ClipboardMessage::Request { id, mime_types } => {
                if version >= 3 {
                    out.write_all(&[Self::MSG_REQUEST_V3])?;
                    out.write_all(&id.to_be_bytes())?;
                    let count = std::cmp::min(mime_types.len(), 255) as u8;
                    out.write_all(&[count])?;
                    for mime in mime_types.iter().take(count as usize) {
                        let bytes = mime.as_bytes();
                        let len = std::cmp::min(bytes.len(), 255) as u8;
                        out.write_all(&[len])?;
                        out.write_all(&bytes[..len as usize])?;
                    }
                } else if version >= 2 {
                    if let Some(mime) = mime_types.first() {
                        out.write_all(&[Self::MSG_REQUEST_V2])?;
                        out.write_all(&id.to_be_bytes())?;
                        let bytes = mime.as_bytes();
                        let len = std::cmp::min(bytes.len(), 255) as u8;
                        out.write_all(&[len])?;
                        out.write_all(&bytes[..len as usize])?;
                    }
                } else if mime_types.contains(&"text/plain".to_string()) {
                    out.write_all(&[Self::MSG_REQUEST])?;
                    out.write_all(&id.to_be_bytes())?;
                }
            }
            ClipboardMessage::Data {
                id,
                mime_type,
                data,
            } => {
                if version >= 2 {
                    out.write_all(&[Self::MSG_DATA_V2])?;
                    out.write_all(&id.to_be_bytes())?;
                    let bytes = mime_type.as_bytes();
                    let len = std::cmp::min(bytes.len(), 255) as u8;
                    out.write_all(&[len])?;
                    out.write_all(&bytes[..len as usize])?;
                    out.write_all(&(data.len() as u32).to_be_bytes())?;
                    out.write_all(data)?;
                } else if mime_type == "text/plain" {
                    out.write_all(&[Self::MSG_DATA])?;
                    out.write_all(&id.to_be_bytes())?;
                    out.write_all(&(data.len() as u32).to_be_bytes())?;
                    out.write_all(data)?;
                }
            }
            ClipboardMessage::Error { id, code } => {
                out.write_all(&[Self::MSG_ERROR])?;
                out.write_all(&id.to_be_bytes())?;
                out.write_all(&code.to_be_bytes())?;
            }
            ClipboardMessage::FileOffer { id, files } => {
                if version >= 4 {
                    out.write_all(&[Self::MSG_FILE_OFFER])?;
                    out.write_all(&id.to_be_bytes())?;
                    let count = std::cmp::min(files.len(), 10_000) as u32;
                    out.write_all(&count.to_be_bytes())?;
                    for f in files.iter().take(count as usize) {
                        let bytes = f.name.as_bytes();
                        let len = std::cmp::min(bytes.len(), 255) as u8;
                        out.write_all(&[len])?;
                        out.write_all(&bytes[..len as usize])?;
                        out.write_all(&f.size.to_be_bytes())?;
                    }
                }
            }
            ClipboardMessage::FileRequest { id, file_indices } => {
                if version >= 4 {
                    out.write_all(&[Self::MSG_FILE_REQUEST])?;
                    out.write_all(&id.to_be_bytes())?;
                    let count = std::cmp::min(file_indices.len(), 10_000) as u32;
                    out.write_all(&count.to_be_bytes())?;
                    for index in file_indices.iter().take(count as usize) {
                        out.write_all(&index.to_be_bytes())?;
                    }
                }
            }
            ClipboardMessage::FileChunk {
                id,
                file_index,
                offset,
                data,
            } => {
                if version >= 4 {
                    out.write_all(&[Self::MSG_FILE_CHUNK])?;
                    out.write_all(&id.to_be_bytes())?;
                    out.write_all(&file_index.to_be_bytes())?;
                    out.write_all(&offset.to_be_bytes())?;
                    out.write_all(&(data.len() as u32).to_be_bytes())?;
                    out.write_all(data)?;
                }
            }
            ClipboardMessage::FileComplete {
                id,
                file_index,
                size,
                sha256,
            } => {
                if version >= 4 {
                    out.write_all(&[Self::MSG_FILE_COMPLETE])?;
                    out.write_all(&id.to_be_bytes())?;
                    out.write_all(&file_index.to_be_bytes())?;
                    out.write_all(&size.to_be_bytes())?;
                    out.write_all(sha256)?;
                }
            }
            ClipboardMessage::FileChunkAck {
                id,
                file_index,
                offset,
            } => {
                if version >= 4 {
                    out.write_all(&[Self::MSG_FILE_CHUNK_ACK])?;
                    out.write_all(&id.to_be_bytes())?;
                    out.write_all(&file_index.to_be_bytes())?;
                    out.write_all(&offset.to_be_bytes())?;
                }
            }
        }
        Ok(())
    }

    pub fn decode(src: &mut impl Read) -> Result<Self, ProtocolError> {
        let mut msg_type = [0u8; 1];
        src.read_exact(&mut msg_type)?;
        match msg_type[0] {
            Self::MSG_HELLO => {
                let mut v = [0u8; 2];
                src.read_exact(&mut v)?;
                Ok(ClipboardMessage::Hello {
                    protocol_version: u16::from_be_bytes(v),
                })
            }
            Self::MSG_HELLO_ACK => {
                let mut v = [0u8; 2];
                src.read_exact(&mut v)?;
                Ok(ClipboardMessage::HelloAck {
                    protocol_version: u16::from_be_bytes(v),
                })
            }
            Self::MSG_OFFER => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                let mut len_b = [0u8; 2];
                src.read_exact(&mut len_b)?;
                let len = u16::from_be_bytes(len_b) as usize;
                if len > 255 {
                    return Err(ProtocolError::InvalidMessageType(Self::MSG_OFFER));
                }
                let mut mime_b = vec![0u8; len];
                src.read_exact(&mut mime_b)?;
                Ok(ClipboardMessage::Offer {
                    id: u64::from_be_bytes(id_b),
                    mime_types: vec![String::from_utf8(mime_b)?],
                })
            }
            Self::MSG_REQUEST => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                Ok(ClipboardMessage::Request {
                    id: u64::from_be_bytes(id_b),
                    mime_types: vec!["text/plain".to_string()],
                })
            }
            Self::MSG_DATA => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                let mut len_b = [0u8; 4];
                src.read_exact(&mut len_b)?;
                let len = u32::from_be_bytes(len_b) as usize;
                let mut data = vec![0u8; len];
                src.read_exact(&mut data)?;
                Ok(ClipboardMessage::Data {
                    id: u64::from_be_bytes(id_b),
                    mime_type: "text/plain".to_string(),
                    data,
                })
            }
            Self::MSG_OFFER_V2 => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                let mut count_b = [0u8; 1];
                src.read_exact(&mut count_b)?;
                let count = count_b[0] as usize;
                let mut mime_types = Vec::with_capacity(count);
                for _ in 0..count {
                    let mut len_b = [0u8; 1];
                    src.read_exact(&mut len_b)?;
                    let len = len_b[0] as usize;
                    let mut mime_b = vec![0u8; len];
                    src.read_exact(&mut mime_b)?;
                    mime_types.push(String::from_utf8(mime_b)?);
                }
                Ok(ClipboardMessage::Offer {
                    id: u64::from_be_bytes(id_b),
                    mime_types,
                })
            }
            Self::MSG_REQUEST_V2 => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                let mut len_b = [0u8; 1];
                src.read_exact(&mut len_b)?;
                let len = len_b[0] as usize;
                let mut mime_b = vec![0u8; len];
                src.read_exact(&mut mime_b)?;
                Ok(ClipboardMessage::Request {
                    id: u64::from_be_bytes(id_b),
                    mime_types: vec![String::from_utf8(mime_b)?],
                })
            }
            Self::MSG_REQUEST_V3 => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                let mut count_b = [0u8; 1];
                src.read_exact(&mut count_b)?;
                let count = count_b[0] as usize;
                let mut mime_types = Vec::with_capacity(count);
                for _ in 0..count {
                    let mut len_b = [0u8; 1];
                    src.read_exact(&mut len_b)?;
                    let len = len_b[0] as usize;
                    let mut mime_b = vec![0u8; len];
                    src.read_exact(&mut mime_b)?;
                    mime_types.push(String::from_utf8(mime_b)?);
                }
                Ok(ClipboardMessage::Request {
                    id: u64::from_be_bytes(id_b),
                    mime_types,
                })
            }
            Self::MSG_DATA_V2 => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                let mut m_len_b = [0u8; 1];
                src.read_exact(&mut m_len_b)?;
                let m_len = m_len_b[0] as usize;
                let mut mime_b = vec![0u8; m_len];
                src.read_exact(&mut mime_b)?;
                let mime_type = String::from_utf8(mime_b)?;

                let mut len_b = [0u8; 4];
                src.read_exact(&mut len_b)?;
                let len = u32::from_be_bytes(len_b) as usize;
                let mut data = vec![0u8; len];
                src.read_exact(&mut data)?;
                Ok(ClipboardMessage::Data {
                    id: u64::from_be_bytes(id_b),
                    mime_type,
                    data,
                })
            }
            Self::MSG_ERROR => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                let mut code_b = [0u8; 2];
                src.read_exact(&mut code_b)?;
                Ok(ClipboardMessage::Error {
                    id: u64::from_be_bytes(id_b),
                    code: u16::from_be_bytes(code_b),
                })
            }
            Self::MSG_FILE_OFFER => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                let mut count_b = [0u8; 4];
                src.read_exact(&mut count_b)?;
                let count = u32::from_be_bytes(count_b);
                if count > 10_000 {
                    return Err(ProtocolError::InvalidMessageType(Self::MSG_FILE_OFFER));
                }
                let mut files = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let mut len_b = [0u8; 1];
                    src.read_exact(&mut len_b)?;
                    let len = len_b[0] as usize;
                    let mut name_b = vec![0u8; len];
                    src.read_exact(&mut name_b)?;
                    let name = String::from_utf8(name_b)?;
                    let mut size_b = [0u8; 8];
                    src.read_exact(&mut size_b)?;
                    files.push(FileMetadata {
                        name,
                        size: u64::from_be_bytes(size_b),
                    });
                }
                Ok(ClipboardMessage::FileOffer {
                    id: u64::from_be_bytes(id_b),
                    files,
                })
            }
            Self::MSG_FILE_REQUEST => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                let mut count_b = [0u8; 4];
                src.read_exact(&mut count_b)?;
                let count = u32::from_be_bytes(count_b);
                if count > 10_000 {
                    return Err(ProtocolError::InvalidMessageType(Self::MSG_FILE_REQUEST));
                }
                let mut file_indices = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let mut idx_b = [0u8; 4];
                    src.read_exact(&mut idx_b)?;
                    file_indices.push(u32::from_be_bytes(idx_b));
                }
                Ok(ClipboardMessage::FileRequest {
                    id: u64::from_be_bytes(id_b),
                    file_indices,
                })
            }
            Self::MSG_FILE_CHUNK => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                let mut idx_b = [0u8; 4];
                src.read_exact(&mut idx_b)?;
                let mut off_b = [0u8; 8];
                src.read_exact(&mut off_b)?;
                let mut len_b = [0u8; 4];
                src.read_exact(&mut len_b)?;
                let len = u32::from_be_bytes(len_b) as usize;

                // Safety check: chunk shouldn't be insanely large. Let's bound it to frame size (100MB).
                // In practice we use 256KB chunks.
                if len as u32 > crate::clipboard::transport::MAX_CLIPBOARD_FRAME_SIZE {
                    return Err(ProtocolError::FrameTooLarge(len as u32));
                }

                let mut data = vec![0u8; len];
                src.read_exact(&mut data)?;
                Ok(ClipboardMessage::FileChunk {
                    id: u64::from_be_bytes(id_b),
                    file_index: u32::from_be_bytes(idx_b),
                    offset: u64::from_be_bytes(off_b),
                    data,
                })
            }
            Self::MSG_FILE_COMPLETE => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                let mut idx_b = [0u8; 4];
                src.read_exact(&mut idx_b)?;
                let mut size_b = [0u8; 8];
                src.read_exact(&mut size_b)?;
                let mut sha256 = [0u8; 32];
                src.read_exact(&mut sha256)?;
                Ok(ClipboardMessage::FileComplete {
                    id: u64::from_be_bytes(id_b),
                    file_index: u32::from_be_bytes(idx_b),
                    size: u64::from_be_bytes(size_b),
                    sha256,
                })
            }
            Self::MSG_FILE_CHUNK_ACK => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                let mut idx_b = [0u8; 4];
                src.read_exact(&mut idx_b)?;
                let mut off_b = [0u8; 8];
                src.read_exact(&mut off_b)?;
                Ok(ClipboardMessage::FileChunkAck {
                    id: u64::from_be_bytes(id_b),
                    file_index: u32::from_be_bytes(idx_b),
                    offset: u64::from_be_bytes(off_b),
                })
            }
            t => Err(ProtocolError::InvalidMessageType(t)),
        }
    }
}
