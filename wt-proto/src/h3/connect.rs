use crate::h3::frame::Frame;
use bytes::BytesMut;

use super::{qpack, H3Error};

#[derive(Debug)]
pub struct Request {
    pub writer: quinn::SendStream,
    #[allow(dead_code)]
    reader: quinn::RecvStream,
    pub headers: qpack::Headers,
}

impl Request {
    pub async fn accept(conn: &mut quinn::Connection) -> Result<Self, H3Error> {
        let (writer, mut reader) = conn.accept_bi().await?;
        let (ftype, _len, mut fdata) = Frame::accept(&mut reader).await?;

        if ftype != Frame::HEADERS {
            tracing::error!("Connect Error - Expected Headers - Got {:x?}", ftype.0.into_inner());
            return Err(H3Error::ProtocolError("Incorrect Ftype"));
        }

        let headers = qpack::Headers::decode(&mut fdata)?;

        if headers.get(":method").is_none_or(|val| val != "CONNECT") {
            return Err(H3Error::ProtocolError(":method invalid in H3 Header"));
        }

        tracing::debug!("[H3 Headers][{:?}]", headers);

        Ok(Self { writer, reader, headers })
    }

    pub async fn open(conn: &mut quinn::Connection, headers: qpack::Headers) -> Result<Self, H3Error> {
        let mut hdata = BytesMut::new();
        let mut buf = BytesMut::new();
        headers.encode(&mut hdata);
        Frame::HEADERS.encode(&mut buf, hdata.freeze());

        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(&buf[..]).await?;

        let (ftype, _, mut fdata) = Frame::accept(&mut recv).await?;

        if ftype != Frame::HEADERS {
            return Err(H3Error::ProtocolError("Received Unknown Header on Connect Open"));
        }

        let resp = qpack::Headers::decode(&mut fdata)?;

        if resp.get(":status").is_none_or(|code| code != "200") {
            tracing::error!("Received Error Reponse : {:?}", resp);
            return Err(H3Error::ConnectError);
        }

        if resp.get("sec-webtransport-http3-draft").is_none_or(|draft| draft != "draft02") {
            tracing::error!("HTTP3 Draft Not Supported : {:?}", resp);
            return Err(H3Error::ConnectError);
        }

        Ok(Self {
            writer: send,
            reader: recv,
            headers,
        })
    }

    pub async fn ok(&mut self) -> Result<(), H3Error> {
        let mut fdata = BytesMut::new();

        let mut headers = qpack::Headers::default();
        headers.set(":status", "200");
        headers.set("sec-webtransport-http3-draft", "draft02");
        headers.encode(&mut fdata);

        let fdata = fdata.freeze();
        let mut buffer = BytesMut::new();
        Frame::HEADERS.encode(&mut buffer, fdata);

        tracing::debug!("[H3 Reponse][{:?}]", headers);

        self.writer.write_all(&buffer).await?;
        Ok(())
    }
}
