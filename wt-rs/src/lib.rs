pub mod conn;
pub mod connect;
pub mod endpoint;
pub mod settings;
pub mod streams;

pub use conn::*;
pub use endpoint::*;

use quinn::{ReadError, SendDatagramError, WriteError};
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

    #[error("ConnectError {0}")]
    ConnectError(#[from] quinn::ConnectError),

    #[error("Send Datagram Error {0}")]
    SendDatagramError(#[from] SendDatagramError),

    #[error("Read Error {0}")]
    ReadError(#[from] ReadError),

    #[error("Write Error {0}")]
    WriteError(#[from] WriteError),

    #[error("Accpet Error")]
    AcceptError(&'static str),
}
