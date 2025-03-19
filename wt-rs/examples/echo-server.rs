#![allow(unused)]
use core::str;
use std::{
    borrow::{BorrowMut, Cow},
    error::Error,
    fmt::Debug,
    fs,
    io::{BufReader, Read},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::from_utf8,
    sync::Arc,
    time::{self, Duration},
};

use quinn::{
    crypto::rustls::{HandshakeData, QuicServerConfig},
    TokioRuntime,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::{
    io::AsyncReadExt,
    time::{sleep, Sleep},
};
use tracing_subscriber::{filter::LevelParseError, fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use wt_proto::{
    coding::VarIntExt,
    h3::{self, frame::Frame},
};

trait LossyString {
    fn lossy(&self) -> &str;
}

impl LossyString for Vec<u8> {
    fn lossy(&self) -> &str {
        std::str::from_utf8(self).unwrap()
    }
}

fn load_certs(key: PathBuf, cert: PathBuf) -> Result<quinn::ServerConfig, Box<dyn Error>> {
    let provider = rustls::crypto::aws_lc_rs::default_provider();

    let chain: Vec<CertificateDer> = match cert.extension().unwrap().to_str() {
        Some("der") => {
            let certbytes = fs::read(cert.clone())?;
            vec![CertificateDer::from(certbytes)]
        }
        Some("pem") | Some("cert") => {
            let chain = fs::File::open(cert)?;
            let mut chain = BufReader::new(chain);
            rustls_pemfile::certs(&mut chain).collect::<Result<_, _>>()?
        }
        _ => {
            panic!("Certfificate Extension Error")
        }
    };

    let key = match key.extension().unwrap().to_str() {
        Some("der") => {
            let keybytes = fs::read(key.clone())?;
            PrivateKeyDer::try_from(keybytes).unwrap()
        }
        Some("key") | Some("pem") => {
            let key = fs::File::open(key)?;
            let mut key = BufReader::new(key);
            rustls_pemfile::private_key(&mut key)?.unwrap()
        }
        _ => {
            panic!("Key Extension Error")
        }
    };

    let mut crypto_config = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(chain, key)?;

    crypto_config.alpn_protocols = vec![h3::ALPN_H3.to_vec()];

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(crypto_config)?));
    Ok(server_config)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let certpath = PathBuf::from("cert/");

    // let server_config = load_certs(certpath.join("key.der"), certpath.join("cert.der"))?;
    let server_config = load_certs(certpath.join("key.pem"), certpath.join("cert.pem"))?;
    let listenaddr = "[::]:4433".parse().unwrap();

    let mut endpoint = wt::Endpoint::server(server_config, listenaddr);

    loop {
        let wtconn = endpoint.accept().await?;

        let mut wt1 = wtconn.clone();
        let mut wt2 = wtconn.clone();
        let mut wt3 = wtconn.clone();

        tokio::spawn(async move {
            loop {
                let data = wt1.read_datagram().await.unwrap();
                tracing::debug!("Received Datagram - {}", String::from_utf8_lossy(&data[..]));
                wt1.send_datagram(data).await.unwrap();
                tracing::debug!("Sent Datagram !!!");
            }
        });

        tokio::spawn(async move {
            loop {
                let mut rs = wt2.accept_uni().await.unwrap();
                let d = rs.read_chunk().await.unwrap().unwrap().bytes;
                tracing::debug!("Received Uni - {}", String::from_utf8_lossy(&d[..]));

                let mut send = wt2.open_uni().await.unwrap();
                send.write_all(&d[..]).await.unwrap();
                tracing::debug!("Sent Uni Stream back !!!");
            }
        });

        tokio::spawn(async move {
            loop {
                let (_, mut rs) = wt3.accept_bi().await.unwrap();
                let d = rs.read_chunk().await.unwrap().unwrap().bytes;
                tracing::debug!("Received Bi - {}", String::from_utf8_lossy(&d[..]));

                let (mut wt, _) = wt3.open_bi().await.unwrap();
                wt.write_all(&d[..]).await.unwrap();
                tracing::debug!("Received Bi - {}", String::from_utf8_lossy(&d[..]));
            }
        });
    }

    Ok(())
}
