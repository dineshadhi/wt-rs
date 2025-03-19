use crate::coding::CodingError;
use coding::UnexpectedEnd;
use quinn::{ReadExactError, WriteError};
use quinn_proto::coding;
use thiserror::Error;

pub mod connect;
pub mod frame;
/// Huffman Encoding for H3 Headers Parsing
pub mod huffman;
/// QPack spec specified by H3 for Heeaders
pub mod qpack;
/// Generic H3 Settings Module, handles all WebTransport related settings.
pub mod settings;
/// WebTransport Streams are stacked on top of H3 QUIC Streams. This module is designed to facilitate accept / open of both Uni & Bi Streams of WebTransport
pub mod streams;

pub use settings::*;
pub use streams::*;

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

    #[error("ConectError")]
    ConnectError,
}
