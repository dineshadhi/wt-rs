use crate::{coding::VarIntExt, h3::H3Error};
use bytes::{BufMut, BytesMut};
use quinn::VarInt;
use quinn_proto::coding::BufMutExt;

#[derive(PartialEq, Eq, Debug, Clone)]
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
    PUSH = 0x01
    QPACK_ENCODER = 0x02
    QPACK_DECODER = 0x03
    WEBTRANSPORT= 0x54
}

impl UniStream {
    pub async fn accept(conn: &mut quinn::Connection) -> Result<(UniStream, quinn::RecvStream), H3Error> {
        let mut stream = conn.accept_uni().await?;
        let stype = UniStream(stream.read_varint().await?);
        Ok((stype, stream))
    }

    pub async fn open(&self, conn: &mut quinn::Connection) -> Result<quinn::SendStream, H3Error> {
        let mut buffer = BytesMut::new();
        self.encode(&mut buffer);

        let mut stream = conn.open_uni().await?;
        stream.write_all(&buffer[..]).await?;

        Ok(stream)
    }

    pub fn encode<B: BufMut>(&self, buffer: &mut B) {
        buffer.write_var(self.0.into_inner());
    }
}

#[derive(PartialEq)]
pub struct BiStream(pub VarInt);

macro_rules! bistream {
    {$($stype:ident = $value:expr)*} => {
        impl BiStream {
            $(pub const $stype : BiStream = BiStream(VarInt::from_u32($value));)*
        }
    };
}

bistream! {
    WEBTRANSPORT = 0x41
}

impl BiStream {
    pub async fn accept(conn: &mut quinn::Connection) -> Result<(quinn::SendStream, quinn::RecvStream), H3Error> {
        let (send, mut recv) = conn.accept_bi().await?;
        let header = BiStream(recv.read_varint().await?);

        if header != BiStream::WEBTRANSPORT {
            return Err(H3Error::ProtocolError("Recevied Unknown Header on BiStream"));
        }

        Ok((send, recv))
    }

    pub async fn open(&self, conn: &mut quinn::Connection) -> Result<(quinn::SendStream, quinn::RecvStream), H3Error> {
        let mut buffer = BytesMut::new();
        self.encode(&mut buffer);

        let (mut send, recv) = conn.open_bi().await?;
        send.write_all(&buffer[..]).await?;

        Ok((send, recv))
    }

    pub fn encode<B: BufMut>(&self, buffer: &mut B) {
        buffer.write_var(self.0.into_inner());
    }
}
