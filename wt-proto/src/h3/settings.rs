use super::H3Error;
use crate::{
    coding::{VarIntExt, VarIntMutExt},
    h3::{frame::Frame, streams::UniStream},
};
use bytes::{Buf, Bytes, BytesMut};
use quinn::VarInt;
use std::{collections::HashMap, fmt::Debug};

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct Setting(pub VarInt);

impl Setting {
    // https://datatracker.ietf.org/doc/html/rfc9114#section-7.2.4.1
    // Setting is greased in HTTP3 spec. If its greased, we must ignore it.
    fn is_grease(&self) -> bool {
        let val = self.0.into_inner();
        if val < 0x21 {
            return false;
        }

        (val - 0x21) % 0x1f == 0
    }
}

impl Debug for Setting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            &Setting::ENABLE_CONNECT_PROTOCOL => write!(f, "ENABLE_CONNECT"),
            &Setting::WEBTRANSPORT_MAX_SESSIONS => write!(f, "MAX_WEBTRANSPORT_SESSIONS"),
            &Setting::QPACK_MAX_TABLE_CAPACITY => write!(f, "QPACK_MAX_TABLE_CAPACITY"),
            &Setting::MAX_FIELD_SECTION_SIZE => write!(f, "MAX_FIELD_SECTION_SIZE"),
            &Setting::QPACK_BLOCKED_STREAMS => write!(f, "QPACK_BLOCKED_STREAMS"),
            &Setting::ENABLE_DATAGRAM => write!(f, "ENABLE_DATAGRAM"),
            &Setting::ENABLE_DATAGRAM_DEPRECATED => write!(f, "ENABLE_DATAGRAM_DEPRECATED"),
            &Setting::WEBTRANSPORT_ENABLE_DEPRECATED => write!(f, "WEBTRANSPORT_ENABLE_DEPRECATED"),
            &Setting::WEBTRANSPORT_MAX_SESSIONS_DEPRECATED => {
                write!(f, "WEBTRANSPORT_MAX_SESSIONS_DEPRECATED")
            }
            s => {
                if s.is_grease() {
                    write!(f, "GREASE [{:#x}]", self.0.into_inner())
                } else {
                    write!(f, "{:#x}", self.0.into_inner())
                }
            }
        }
    }
}

macro_rules! settings {
    {$($setting:ident = $val:expr,)*} => {
        impl Setting {
            $(pub const $setting : Setting = Setting(VarInt::from_u32($val));)*
        }
    };
}

settings! {
    ENABLE_CONNECT_PROTOCOL = 0x8,
    WEBTRANSPORT_MAX_SESSIONS = 0xc671706a,
    QPACK_MAX_TABLE_CAPACITY = 0x1,
    MAX_FIELD_SECTION_SIZE = 0x6,
    QPACK_BLOCKED_STREAMS = 0x7,
    ENABLE_DATAGRAM = 0x33,
    ENABLE_DATAGRAM_DEPRECATED = 0xFFD277,
    WEBTRANSPORT_ENABLE_DEPRECATED = 0x2b603742,
    WEBTRANSPORT_MAX_SESSIONS_DEPRECATED = 0x2b603743,
}

#[derive(Default, Clone)]
pub struct Settings {
    inner: HashMap<Setting, VarInt>,
}

impl Debug for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut str = "[".to_string();
        for (setting, value) in self.inner.clone() {
            str = format!("{}  {:?} - {:?}", str, setting, value)
        }

        write!(f, "{}]", str)
    }
}

impl Settings {
    pub fn new() -> Self {
        let mut inner = HashMap::default();
        let grease = VarInt::from_u32(0x1f + 0x21);
        let randomvalue = VarInt::from_u32(0x6429);
        // Adding grease. Implementations must contain atlease one grease
        inner.insert(Setting(grease), randomvalue);
        Self { inner }
    }

    pub async fn decode(data: &mut Bytes, len: usize) -> Result<Self, H3Error> {
        let mut settings = Settings::default();
        let mut data = data.split_to(len);

        while data.has_remaining() {
            dbg!(data.len());
            let setting = Setting(data.read_varint()?);
            let value = data.read_varint()?.into_inner() as u32;

            settings.insert(setting, value);
        }

        Ok(settings)
    }

    pub fn encode(&self) -> (usize, Bytes) {
        let mut data = BytesMut::new();
        for (setting, value) in self.inner.clone() {
            data.write_varint(setting.0.into_inner());
            data.write_varint(value.into_inner());
        }

        (data.len(), data.freeze())
    }

    fn insert(&mut self, setting: Setting, value: u32) {
        self.inner.insert(setting, VarInt::from_u32(value));
    }

    pub fn enable_webtransport(&mut self, max_sessions: u32) {
        self.insert(Setting::ENABLE_CONNECT_PROTOCOL, 1);
        self.insert(Setting::ENABLE_DATAGRAM, 1);
        self.insert(Setting::ENABLE_DATAGRAM_DEPRECATED, 1);
        self.insert(Setting::WEBTRANSPORT_MAX_SESSIONS, max_sessions);
        self.insert(Setting::WEBTRANSPORT_MAX_SESSIONS_DEPRECATED, max_sessions);
        self.insert(Setting::WEBTRANSPORT_ENABLE_DEPRECATED, 1);
    }

    // TODO : Needs a lot of RFC reading to get this right across all the browsers
    pub fn supports_webtransport(&self) -> bool {
        if let Some(n) = self.inner.get(&Setting::WEBTRANSPORT_ENABLE_DEPRECATED) {
            return n.into_inner() >= 1;
        }

        if let Some(n) = self.inner.get(&Setting::WEBTRANSPORT_MAX_SESSIONS) {
            return n.into_inner() >= 1;
        }

        if let Some(n) = self.inner.get(&Setting::WEBTRANSPORT_MAX_SESSIONS_DEPRECATED) {
            return n.into_inner() >= 1;
        }

        false
    }

    pub async fn open(mut conn: quinn::Connection, settings: Settings) -> Result<quinn::SendStream, H3Error> {
        let mut cs = UniStream::CONTROL.open(&mut conn).await?;
        let (_, fdata) = settings.encode();

        let mut buffer = BytesMut::new();
        Frame::SETTINGS.encode(&mut buffer, fdata);

        tracing::debug!("[Sending Settings][{:?}]", settings);

        cs.write_all(&buffer).await?;
        Ok(cs)
    }

    // Accepts a Unistream from the QUIC Connection and reads the Settings Frame
    pub async fn accept(mut conn: quinn::Connection) -> Result<(Settings, quinn::RecvStream), H3Error> {
        let (stype, mut cs) = UniStream::accept(&mut conn).await?;

        if stype != UniStream::CONTROL {
            tracing::error!("[Settings Error - Expected Control Stream - Received {:x}]", stype.0.into_inner());
            return Err(H3Error::SettingsError);
        }

        let (ftype, len, mut data) = Frame::accept(&mut cs).await?;

        if ftype != Frame::SETTINGS {
            tracing::error!("[Settings Error - Expected Control Stream - Received {:x}]", stype.0.into_inner());
            return Err(H3Error::SettingsError);
        }

        let settings = Settings::decode(&mut data, len).await?;

        tracing::debug!("[Received Settings][{:?}]", settings);

        Ok((settings, cs))
    }
}
