use async_trait::async_trait;
use bytes::{Buf, BufMut};
use quinn::{ReadExactError, VarInt, VarIntBoundsExceeded};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CodingError {
    #[error("Read Exact Error {0}")]
    ReadExactError(#[from] ReadExactError),

    #[error("VarInt Malformed. Tag - {0:#X}")]
    VarIntMalformed(u8),

    #[error("VarInt Bounds Exceeded")]
    VarIntBoundsExceeded(#[from] VarIntBoundsExceeded),

    #[error("UnexpectedEnd : Not Enough Data to Decode VarInt")]
    UnexpectedEnd,
}

/// VarInt is an encoding technique to store integers according to their size. Smaller values takes less bytes, larger value takes more.
/// They form the basic blocks of any QUIC based protocols.
/// This convinient extension helps decoding the VarInt directly from a quinn::RecvStream and a Buf.
#[async_trait]
pub trait VarIntAsyncExt {
    /// Reads VarInt
    async fn read_varint(&mut self) -> Result<VarInt, CodingError>;
}

pub trait VarIntExt {
    /// Reads VarInt
    fn read_varint(&mut self) -> Result<VarInt, CodingError>;
}

pub trait VarIntMutExt {
    fn write_varint(&mut self, val: u64);
}

#[async_trait]
impl VarIntAsyncExt for quinn::RecvStream {
    async fn read_varint(&mut self) -> Result<VarInt, CodingError> {
        // Read just one byte
        let mut buf = [0; 8];
        self.read_exact(&mut buf[..1]).await?;

        // Tag is first 2 bits of the u8. Tag gives us the number of bytes occupied by the VarInt.
        let tag = buf[0] >> 6;

        // Tag(0b00) -> 1 bytes
        // Tag(0b01) -> 2 bytes
        // Tag(0b10) -> 4 bytes
        // Tag(0b11) -> 8 bytes

        // Remove the tag bits from the original buf
        buf[0] &= 0b0011_1111;

        // Read the remaining bytes based on the tag and compute U64
        let val: u64 = match tag {
            0b00 => u64::from(buf[0]),
            0b01 => {
                self.read_exact(&mut buf[1..2]).await?;
                u16::from_be_bytes(buf[..2].try_into().unwrap()) as u64
            }
            0b10 => {
                self.read_exact(&mut buf[1..4]).await?;
                u32::from_be_bytes(buf[..4].try_into().unwrap()) as u64
            }
            0b11 => {
                self.read_exact(&mut buf[1..8]).await?;
                u64::from_be_bytes(buf)
            }
            _ => {
                return Err(CodingError::VarIntMalformed(tag));
            }
        };

        Ok(VarInt::from_u64(val)?)
    }
}

impl<B: Buf> VarIntExt for B {
    fn read_varint(&mut self) -> Result<VarInt, CodingError> {
        let mut buf = [0; 8];
        buf[0] = self.get_u8();

        let tag = buf[0] >> 6;

        // Tag(0b00) -> 1 bytes
        // Tag(0b01) -> 2 bytes
        // Tag(0b10) -> 4 bytes
        // Tag(0b11) -> 8 bytes

        // Remove the tag bits from the original buf
        buf[0] &= 0b0011_1111;

        // Read the remaining bytes based on the tag and compute U64
        let val: u64 = match tag {
            0b00 => u64::from(buf[0]),
            0b01 => {
                if self.remaining() < 1 {
                    return Err(CodingError::UnexpectedEnd);
                }

                self.copy_to_slice(&mut buf[1..2]);
                u16::from_be_bytes(buf[..2].try_into().unwrap()) as u64
            }
            0b10 => {
                if self.remaining() < 3 {
                    return Err(CodingError::UnexpectedEnd);
                }
                self.copy_to_slice(&mut buf[1..4]);
                u32::from_be_bytes(buf[..4].try_into().unwrap()) as u64
            }
            0b11 => {
                if self.remaining() < 7 {
                    return Err(CodingError::UnexpectedEnd);
                }

                self.copy_to_slice(&mut buf[1..8]);
                u64::from_be_bytes(buf)
            }
            _ => {
                return Err(CodingError::VarIntMalformed(tag));
            }
        };

        Ok(VarInt::from_u64(val)?)
    }
}

impl<T: BufMut> VarIntMutExt for T {
    fn write_varint(&mut self, x: u64) {
        if x < 2u64.pow(6) {
            self.put_u8(x as u8);
        } else if x < 2u64.pow(14) {
            self.put_u16((0b01 << 14) | x as u16);
        } else if x < 2u64.pow(30) {
            self.put_u32((0b10 << 30) | x as u32);
        } else if x < 2u64.pow(62) {
            self.put_u64((0b11 << 62) | x);
        } else {
            unreachable!("malformed VarInt")
        }
    }
}
