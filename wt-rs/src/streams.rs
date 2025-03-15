use bytes::BytesMut;
use quinn::{Chunk, RecvStream, SendStream, VarInt};
use wt_proto::{
    coding::{VarIntExt, VarIntMutExt},
    h3::unistream::UniStream,
};

use crate::WTError;

pub struct WriteStream {
    pub inner: SendStream,
}

impl WriteStream {
    pub async fn open(mut stream: SendStream, id: VarInt) -> Result<Self, WTError> {
        let mut buffer = BytesMut::new();
        buffer.write_varint(id.into_inner());
        stream.write_all(&buffer[..]).await?;

        Ok(WriteStream { inner: stream })
    }

    pub async fn write_all(&mut self, data: &[u8]) -> Result<(), WTError> {
        Ok(self.inner.write_all(data).await?)
    }
}

pub struct ReadStream {
    pub inner: RecvStream,
    pub stype: Option<UniStream>,
    pub id: Option<VarInt>,
}

impl ReadStream {
    pub async fn new(stream: RecvStream) -> Result<Self, WTError> {
        Ok(Self {
            inner: stream,
            stype: None,
            id: None,
        })
    }

    // Accept from incoming WebTransport Stream
    pub async fn accept(mut stream: RecvStream) -> Result<Self, WTError> {
        let stype = UniStream(stream.read_varint().await?);
        let sid = stream.read_varint().await?;

        Ok(Self {
            inner: stream,
            stype: Some(stype),
            id: Some(sid),
        })
    }

    pub async fn read_chunk(&mut self) -> Result<Option<Chunk>, WTError> {
        Ok(self.inner.read_chunk(usize::MAX, true).await?)
    }
}

impl Into<ReadStream> for RecvStream {
    fn into(self) -> ReadStream {
        ReadStream {
            inner: self,
            stype: None,
            id: None,
        }
    }
}

impl Into<WriteStream> for SendStream {
    fn into(self) -> WriteStream {
        WriteStream { inner: self }
    }
}
