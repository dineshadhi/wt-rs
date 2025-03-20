use crate::coding::{VarInt, VarIntAsyncExt, VarIntMutExt};
use bytes::{BufMut, Bytes};
use std::fmt::Debug;

use super::H3Error;

#[derive(PartialEq, Eq)]
pub struct Frame(pub VarInt);

impl Debug for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Frame::DATA => write!(f, "Frame::DATA"),
            Frame::HEADERS => write!(f, "Frame::HEADERS"),
            Frame::CANCEL_PUSH => write!(f, "Frame::CANCEL_PUSH"),
            Frame::SETTINGS => write!(f, "Frame::SETTINGS"),
            Frame::PUSH_PROMISE => write!(f, "Frame::PUSH_PROMISE"),
            Frame::GOAWAY => write!(f, "Frame::GOAWAY"),
            Frame::MAX_PUSH_ID => write!(f, "Frame::MAX_PUSH_ID"),
            _ => write!(f, "Frame::UNKNOWN"),
        }
    }
}

macro_rules! frame {
    {$($ftype:ident = $val:expr,)*} => {
        impl Frame {
             $(pub const $ftype: Frame = Frame(VarInt::from_u32($val));)*
        }
    };
}

// Reference : https://datatracker.ietf.org/doc/html/rfc9114#section-7
frame! {
    DATA = 0x00,
    HEADERS = 0x01,
    CANCEL_PUSH = 0x03,
    SETTINGS = 0x04,
    PUSH_PROMISE = 0x05,
    GOAWAY = 0x07,
    MAX_PUSH_ID = 0x0d,
}

impl Frame {
    // HTTP3 greases frames. If the frame is greased, we should ignore it according to the spec.
    // Reference : https://datatracker.ietf.org/doc/html/rfc9114#section-7.2.8
    fn is_grease(&self) -> bool {
        let val = self.0.into_inner();
        if val < 0x21 {
            return false;
        }

        (val - 0x21) % 0x1f == 0
    }

    pub fn encode<B: BufMut>(&self, b: &mut B, fdata: Bytes) {
        b.write_varint(self.0.into_inner());
        b.write_varint(fdata.len() as u64);
        b.put(fdata);
    }

    pub async fn accept(v: &mut quinn::RecvStream) -> Result<(Frame, usize, Bytes), H3Error> {
        loop {
            let ftype = Frame(v.read_varint().await?);
            let len = v.read_varint().await?.into_inner() as usize;

            let mut data = vec![0; len];
            v.read_exact(data.as_mut_slice()).await?;

            if ftype.is_grease() {
                continue;
            }

            return Ok((ftype, len, Bytes::from(data)));
        }
    }
}
