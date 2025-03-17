use quinn::VarInt;
use wt_proto::h3::{self, qpack};

use crate::WTError;

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

    pub fn session_id(&self) -> VarInt {
        VarInt::from(self.inner.writer.id())
    }

    pub fn headers(&self) -> &qpack::Headers {
        &self.inner.headers
    }
}
