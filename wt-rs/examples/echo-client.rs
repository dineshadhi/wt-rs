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

use bytes::Bytes;
use quinn::{
    crypto::rustls::{HandshakeData, QuicClientConfig, QuicServerConfig},
    TokioRuntime,
};
use rustls::{
    client::danger::ServerCertVerifier,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use tokio::{
    io::AsyncReadExt,
    time::{sleep, Sleep},
};
use tracing_subscriber::{filter::LevelParseError, fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use wt_proto::{
    coding::VarIntExt,
    h3::{self, frame::Frame},
};
use wt_rs as wt;

trait LossyString {
    fn lossy(&self) -> &str;
}

impl LossyString for Vec<u8> {
    fn lossy(&self) -> &str {
        std::str::from_utf8(self).unwrap()
    }
}

fn load_certs(key: PathBuf, cert: PathBuf) -> Result<quinn::ClientConfig, Box<dyn Error>> {
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

    let mut root_certs = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().expect("could not load platform certs") {
        root_certs.add(cert).unwrap();
    }

    let mut crypto_config = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(root_certs)
        .with_no_client_auth();

    crypto_config.alpn_protocols = vec![h3::ALPN_H3.to_vec()];

    let client_config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto_config)?));

    Ok(client_config)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let certpath = PathBuf::from("cert/");

    // let server_config = load_certs(certpath.join("key.der"), certpath.join("cert.der"))?;
    let client_config = load_certs(certpath.join("key.pem"), certpath.join("cert.pem"))?;
    let bindaddr = "[::]:5000".parse().unwrap();
    let serveraddr = "127.0.0.1:4433".parse().unwrap();

    let mut endpoint = wt::Endpoint::client(client_config, bindaddr);
    let wt = endpoint.connect(serveraddr, "wt-server").await?;

    let mut wt1 = wt.clone();
    let mut wt2 = wt.clone();
    let mut wt3 = wt.clone();

    tokio::spawn(async move {
        loop {
            wt1.send_datagram(Bytes::from("Datagram".as_bytes())).await.unwrap();
            let data = wt1.read_datagram().await.unwrap();
            tracing::debug!("Received -  {}", String::from_utf8_lossy(&data[..]));
            sleep(Duration::from_secs(1)).await;
        }
    });

    tokio::spawn(async move {
        loop {
            let mut send = wt2.open_uni().await.unwrap();
            send.write_all("UniStream".as_bytes()).await.unwrap();

            let mut rs = wt2.accept_uni().await.unwrap();
            let d = rs.read_chunk().await.unwrap().unwrap().bytes;
            tracing::debug!("Received - {}", String::from_utf8_lossy(&d[..]));
            sleep(Duration::from_secs(1)).await;
        }
    });

    let handle = tokio::spawn(async move {
        loop {
            let (mut wt, _) = wt3.open_bi().await.unwrap();
            wt.write_all("BiStream".as_bytes()).await.unwrap();

            let (_, mut rs) = wt3.accept_bi().await.unwrap();
            let d = rs.read_chunk().await.unwrap().unwrap().bytes;
            tracing::debug!("Received - {}", String::from_utf8_lossy(&d[..]));
            sleep(Duration::from_secs(1)).await;
        }
    });

    handle.await.unwrap();

    Ok(())
}
