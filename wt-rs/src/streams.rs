use bytes::BytesMut;
use quinn::{Chunk, RecvStream, SendStream, VarInt};
use wt_proto::coding::VarIntMutExt;

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
}

impl ReadStream {
    pub async fn new(stream: RecvStream) -> Result<Self, WTError> {
        Ok(Self { inner: stream })
    }

    /// Reads a chunk of data from the quinn's API.
    pub async fn read_chunk(&mut self) -> Result<Option<Chunk>, WTError> {
        Ok(self.inner.read_chunk(usize::MAX, true).await?)
    }
}

impl From<RecvStream> for ReadStream {
    fn from(val: RecvStream) -> Self {
        ReadStream { inner: val }
    }
}

impl From<SendStream> for WriteStream {
    fn from(val: SendStream) -> Self {
        WriteStream { inner: val }
    }
}
