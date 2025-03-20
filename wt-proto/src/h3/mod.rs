use thiserror::Error;

pub mod connect;
pub mod frame;
pub mod huffman;
pub mod qpack;
pub mod settings;
pub mod streams;

pub use settings::*;
pub use streams::*;

pub const ALPN_H3: &[u8] = b"h3";

#[derive(Debug, Error)]
pub enum H3Error {
    #[error("Settings Error")]
    SettingsError,

    #[error("Codec Error {0}")]
    CodingError(#[from] crate::coding::CodingError),

    #[error("Connection Error")]
    ConnectionError(#[from] quinn::ConnectionError),

    #[error("Protocol Error {0}")]
    ProtocolError(&'static str),

    #[error("Read Error")]
    ReadExactError(#[from] quinn::ReadExactError),

    #[error("Write Error {0}")]
    WriteErrir(#[from] quinn::WriteError),

    #[error("Qpack Decode Error {0}")]
    QpackDeocdeError(#[from] qpack::DecodeError),

    #[error("ConectError")]
    ConnectError,
}
