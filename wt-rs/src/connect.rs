use crate::WTError;
use wt_proto::{
    coding::VarInt,
    h3::{self, qpack},
};

pub struct Connect {
    inner: h3::connect::Request,
}

impl Connect {
    pub async fn accept(conn: &mut quinn::Connection) -> Result<Connect, WTError> {
        let mut request = h3::connect::Request::accept(conn).await?;

        if request.headers.get(":protocol").is_none_or(|val| val != "webtransport") {
            return Err(WTError::ProtocolError(":protocol invalid in H3 Header"));
        }

        request.ok().await?;

        Ok(Connect { inner: request })
    }

    pub async fn open(conn: &mut quinn::Connection) -> Result<Connect, WTError> {
        let mut headers = qpack::Headers::default();
        headers.set("origin", "https://googlechrome.github.io");
        headers.set(":protocol", "webtransport");
        headers.set(":method", "CONNECT");
        headers.set(":scheme", "https");
        headers.set("sec-webtransport-http3-draft02", "1");

        let request = h3::connect::Request::open(conn, headers).await?;
        Ok(Connect { inner: request })
    }

    pub fn session_id(&self) -> VarInt {
        let val = quinn::VarInt::from(self.inner.writer.id());
        VarInt::from_u64(val.into_inner()).unwrap()
    }

    pub fn headers(&self) -> &qpack::Headers {
        &self.inner.headers
    }
}
