use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use coding::UnexpectedEnd;
use quinn::{ReadExactError, VarInt, VarIntBoundsExceeded};
use quinn_proto::coding::{self, BufExt, BufMutExt};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CodingError {
    #[error("Read Exact Error {0}")]
    ReadExactError(#[from] ReadExactError),

    #[error("VarInt Malformed. Tag - {0:#X}")]
    VarIntMalformed(u8),

    #[error("Unexpected End")]
    UnexpectedEnd(#[from] UnexpectedEnd),

    #[error("VarInt Bounds Exceeded")]
    VarIntBoundsExceeded(#[from] VarIntBoundsExceeded),
}

// Convenient trait to handle both quinn::RecvStream and BytesMut to read varint and data
#[async_trait]
pub trait VarIntExt {
    async fn read_varint(&mut self) -> Result<VarInt, CodingError>;
    async fn read_len(&mut self, len: usize) -> Result<Bytes, CodingError>;
}

pub trait VarIntMutExt {
    fn write_varint(&mut self, val: u64);
}

#[async_trait]
impl VarIntExt for quinn::RecvStream {
    async fn read_len(&mut self, len: usize) -> Result<Bytes, CodingError> {
        let mut buffer = vec![0; len];
        self.read_exact(&mut buffer).await?;
        Ok(Bytes::from(buffer))
    }

    async fn read_varint(&mut self) -> Result<VarInt, CodingError> {
        // Read just one byte
        let mut buf = [0; 8];
        self.read_exact(&mut buf[..1]).await?;

        // Tag is first 2 bits of the u8. Tag gives us the number of bytes occupied by the VarInt.
        let tag = buf[0] >> 6;

        // Tag(0b00) -> 1
        // Tag(0b01) -> 2
        // Tag(0b10) -> 4
        // Tag(0b11) -> 8

        // Remove the tag bits from the original buf
        buf[0] &= 0b0011_1111;

        // Read the remaining bytes based on the tag and compute U64
        let val = match tag {
            0b00 => u64::from(buf[0]),
            0b01 => {
                self.read_exact(&mut buf[1..2]).await?;
                u16::from_be_bytes(buf[..2].try_into().unwrap()) as u64
            }
            0b10 => {
                self.read_exact(&mut buf[1..4]).await?;
                u32::from_be_bytes(buf[..3].try_into().unwrap()) as u64
            }
            0b11 => {
                self.read_exact(&mut buf[1..8]).await?;
                u64::from_be_bytes(buf)
            }
            _ => {
                return Err(CodingError::VarIntMalformed(tag));
            }
        };

        // Unwrapping here because we compute the u64
        Ok(VarInt::from_u64(val).unwrap())
    }
}

#[async_trait]
impl VarIntExt for BytesMut {
    async fn read_varint(&mut self) -> Result<VarInt, CodingError> {
        let varint = VarInt::from_u64(self.get_var()?)?;
        Ok(varint)
    }

    async fn read_len(&mut self, len: usize) -> Result<Bytes, CodingError> {
        Ok(self.split_to(len).freeze())
    }
}

#[async_trait]
impl VarIntExt for Bytes {
    async fn read_varint(&mut self) -> Result<VarInt, CodingError> {
        let varint = VarInt::from_u64(self.get_var()?)?;
        Ok(varint)
    }

    async fn read_len(&mut self, len: usize) -> Result<Bytes, CodingError> {
        Ok(self.split_to(len))
    }
}

impl VarIntMutExt for BytesMut {
    fn write_varint(&mut self, v: u64) {
        self.write_var(v);
    }
}
