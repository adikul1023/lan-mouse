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
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ClipboardMessage {
    Hello { protocol_version: u16 },
    HelloAck { protocol_version: u16 },
    Offer { id: u64, mime_type: String },
    Request { id: u64 },
    Data { id: u64, data: Vec<u8> },
    Error { id: u64, code: u16 },
}

impl ClipboardMessage {
    pub const MSG_HELLO: u8 = 1;
    pub const MSG_HELLO_ACK: u8 = 2;
    pub const MSG_OFFER: u8 = 3;
    pub const MSG_REQUEST: u8 = 4;
    pub const MSG_DATA: u8 = 5;
    pub const MSG_ERROR: u8 = 6;

    pub fn encode(&self, out: &mut impl Write) -> io::Result<()> {
        match self {
            ClipboardMessage::Hello { protocol_version } => {
                out.write_all(&[Self::MSG_HELLO])?;
                out.write_all(&protocol_version.to_be_bytes())?;
            }
            ClipboardMessage::HelloAck { protocol_version } => {
                out.write_all(&[Self::MSG_HELLO_ACK])?;
                out.write_all(&protocol_version.to_be_bytes())?;
            }
            ClipboardMessage::Offer { id, mime_type } => {
                out.write_all(&[Self::MSG_OFFER])?;
                out.write_all(&id.to_be_bytes())?;
                let bytes = mime_type.as_bytes();
                out.write_all(&(bytes.len() as u16).to_be_bytes())?;
                out.write_all(bytes)?;
            }
            ClipboardMessage::Request { id } => {
                out.write_all(&[Self::MSG_REQUEST])?;
                out.write_all(&id.to_be_bytes())?;
            }
            ClipboardMessage::Data { id, data } => {
                out.write_all(&[Self::MSG_DATA])?;
                out.write_all(&id.to_be_bytes())?;
                out.write_all(&(data.len() as u32).to_be_bytes())?;
                out.write_all(data)?;
            }
            ClipboardMessage::Error { id, code } => {
                out.write_all(&[Self::MSG_ERROR])?;
                out.write_all(&id.to_be_bytes())?;
                out.write_all(&code.to_be_bytes())?;
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
                let mut mime_b = vec![0u8; len];
                src.read_exact(&mut mime_b)?;
                Ok(ClipboardMessage::Offer {
                    id: u64::from_be_bytes(id_b),
                    mime_type: String::from_utf8(mime_b)?,
                })
            }
            Self::MSG_REQUEST => {
                let mut id_b = [0u8; 8];
                src.read_exact(&mut id_b)?;
                Ok(ClipboardMessage::Request {
                    id: u64::from_be_bytes(id_b),
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
            t => Err(ProtocolError::InvalidMessageType(t)),
        }
    }
}
