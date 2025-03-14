pub mod conn;
pub mod connect;
pub mod settings;
pub use conn::*;

use quinn::{SendDatagramError, WriteError};
use thiserror::Error;
use wt_proto::{coding::CodingError, h3};

#[derive(Error, Debug)]
pub enum WTError {
    #[error("H3 Error {0}")]
    H3Error(#[from] h3::H3Error),

    #[error("Codec Error {0}")]
    CodingError(#[from] CodingError),

    #[error("Web Transport Not Supported")]
    WebTransportNotSupported,

    #[error("Protocol Error - {0}")]
    ProtocolError(&'static str),

    #[error("WT Connection Eror {0}")]
    ConnectionError(#[from] quinn::ConnectionError),

    #[error("Write Error {0}")]
    WriteErr(#[from] WriteError),

    #[error("Send Datagram Error {0}")]
    SendDatagramError(#[from] SendDatagramError),
}
