use crate::coding::CodingError;
use coding::UnexpectedEnd;
use quinn::{ReadExactError, WriteError};
use quinn_proto::coding;
use thiserror::Error;

pub mod connect;
pub mod frame;
pub mod huffman;
pub mod qpack;
pub mod settings;
pub mod unistream;

pub use settings::*;

pub const ALPN_H3: &[u8] = b"h3";

#[derive(Debug, Error)]
pub enum H3Error {
    #[error("Settings Error")]
    SettingsError,

    #[error("Codec Error {0}")]
    CodingError(#[from] CodingError),

    #[error("Connection Error")]
    ConnectionError(#[from] quinn::ConnectionError),

    #[error("Protocol Error {0}")]
    ProtocolError(&'static str),

    #[error("Read Error")]
    ReadExactError(#[from] ReadExactError),

    #[error("Unexpected End")]
    UnexpectedEnd(#[from] UnexpectedEnd),

    #[error("Write Error {0}")]
    WriteErrir(#[from] WriteError),

    #[error("Qpack Decode Error {0}")]
    QpackDeocdeError(#[from] qpack::DecodeError),
}
