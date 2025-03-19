use std::{marker::PhantomData, net::SocketAddr};

use quinn::crypto::rustls::HandshakeData;
use wt_proto::h3;

use crate::{Connection, WTError};

pub struct Init;
pub struct Server;
pub struct Client;

pub struct Endpoint<S = Init> {
    inner: quinn::Endpoint,
    state: PhantomData<S>,
}

impl Endpoint<Init> {
    pub fn server(config: quinn::ServerConfig, addr: SocketAddr) -> Endpoint<Server> {
        let e = quinn::Endpoint::server(config, addr).unwrap();
        tracing::info!("New WebTransport Server : {:?}", addr);
        Endpoint {
            inner: e,
            state: PhantomData,
        }
    }

    pub fn client(config: quinn::ClientConfig, bindaddr: SocketAddr) -> Endpoint<Client> {
        let mut e = quinn::Endpoint::client(bindaddr).unwrap();
        e.set_default_client_config(config);
        Endpoint {
            inner: e,
            state: PhantomData::<Client>,
        }
    }
}

impl Endpoint<Server> {
    pub async fn accept(&mut self) -> Result<Connection, WTError> {
        let incoming = self.inner.accept().await.unwrap();
        let conn = incoming.accept()?.await?;

        let hdata = match conn.handshake_data() {
            Some(data) => data,
            None => return Err(WTError::ProtocolError("Handshake Error : Cannot get Handshake Details")),
        };

        let alpn = match hdata.downcast_ref::<HandshakeData>() {
            Some(hdata) => hdata.protocol.to_owned().unwrap(),
            None => return Err(WTError::ProtocolError("Handshake Error : ALPN Unknown")),
        };

        match alpn.as_slice() {
            h3::ALPN_H3 => Connection::upgrade(conn).await,
            _ => Err(WTError::ProtocolError("Invalid ALPN : {alpn}")),
        }
    }
}

impl Endpoint<Client> {
    pub async fn connect(&mut self, addr: SocketAddr, server_name: &str) -> Result<Connection, WTError> {
        let conn = self.inner.connect(addr, server_name).unwrap().await?;
        let conn = Connection::open(conn).await?;
        Ok(conn)
    }
}
