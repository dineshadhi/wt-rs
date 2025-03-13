use crate::{coding::VarIntExt, h3::H3Error};
use bytes::{BufMut, BytesMut};
use quinn::VarInt;
use quinn_proto::coding::BufMutExt;

#[derive(PartialEq, Eq, Debug)]
pub struct UniStream(pub VarInt);

macro_rules! unistream {
    {$($stype:ident = $value:expr)*} => {
        impl UniStream {
            $(pub const $stype : UniStream = UniStream(VarInt::from_u32($value));)*
        }
    };
}

unistream! {
    CONTROL = 0x00
    WEBTRANSPORT = 0x54
}

impl UniStream {
    // Poll simply reads the type of already accepted quinn::RecvStream
    pub async fn poll(mut s: quinn::RecvStream) -> Result<(UniStream, quinn::RecvStream), H3Error> {
        Ok((UniStream(s.read_varint().await?), s))
    }

    pub async fn accept(conn: &mut quinn::Connection) -> Result<(UniStream, quinn::RecvStream), H3Error> {
        let mut stream = conn.accept_uni().await?;
        let stype = UniStream(stream.read_varint().await?);
        Ok((stype, stream))
    }

    pub async fn open(&self, conn: &mut quinn::Connection) -> Result<(BytesMut, quinn::SendStream), H3Error> {
        let mut buffer = BytesMut::new();
        self.encode(&mut buffer);

        let stream = conn.open_uni().await?;

        Ok((buffer, stream))
    }

    fn encode<B: BufMut>(&self, buffer: &mut B) {
        buffer.write_var(self.0.into_inner());
    }
}
