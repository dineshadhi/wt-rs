use bytes::BytesMut;

use crate::h3::frame::Frame;

use super::{qpack, H3Error};

#[derive(Debug)]
pub struct Request {
    writer: quinn::SendStream,
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
            return Err(H3Error::ProtocolError("Incorrect Ftype".into()));
        }

        let headers = qpack::Headers::decode(&mut fdata)?;

        if !headers.get(":method").is_some_and(|val| val == "CONNECT") {
            return Err(H3Error::ProtocolError(":method invalid in H3 Header".into()));
        }

        tracing::debug!("[H3 Headers][{:?}]", headers);

        Ok(Self { writer, reader, headers })
    }

    pub async fn open(_conn: &mut quinn::Connection) -> Result<Self, H3Error> {
        todo!()
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
