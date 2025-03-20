use bytes::{Bytes, BytesMut};
use quinn::{Chunk, ReadExactError, RecvStream, SendStream};
use wt_proto::coding::{CodingError, VarInt, VarIntAsyncExt, VarIntMutExt};

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

    pub async fn read_varint(&mut self) -> Result<VarInt, CodingError> {
        self.inner.read_varint().await
    }

    pub async fn read_exact_len(&mut self, len: usize) -> Result<Bytes, ReadExactError> {
        let mut buffer = vec![0; len];
        self.inner.read_exact(&mut buffer[..]).await?;
        Ok(Bytes::from(buffer))
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
