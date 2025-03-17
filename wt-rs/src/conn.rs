use std::{future::Future, pin::Pin, sync::Arc};

use crate::{
    connect::Connect,
    settings::Settings,
    streams::{ReadStream, WriteStream},
    WTError,
};
use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use quinn::{RecvStream, SendStream, VarInt};
use tokio::sync::Mutex;
use wt_proto::h3::unistream::UniStream;

#[derive(Clone)]
pub struct Connection {
    #[allow(dead_code)]
    id: VarInt,
    #[allow(dead_code)]
    inner: quinn::Connection,
    #[allow(dead_code)]
    settings: Option<Arc<Settings>>,
    #[allow(dead_code)]
    connect: Option<Arc<Connect>>,
    uniaccept: Option<Arc<Mutex<UniAccept>>>,
    biaccept: Option<Arc<Mutex<BiAccept>>>,
}

impl Connection {
    pub async fn upgrade(mut conn: quinn::Connection) -> Result<Connection, WTError> {
        let settings = Settings::accept(conn.clone()).await?;
        let connect = Connect::accept(&mut conn).await?;
        let id = connect.session_id();

        let uniaccept = UniAccept::new(id, conn.clone());
        let biaccept = BiAccept::new(id, conn.clone());

        let conn = Connection {
            id,
            inner: conn,
            settings: Some(Arc::new(settings)),
            connect: Some(Arc::new(connect)),
            uniaccept: Some(Arc::new(Mutex::new(uniaccept))),
            biaccept: Some(Arc::new(Mutex::new(biaccept))),
        };

        Ok(conn)
    }

    pub async fn accept_uni(&mut self) -> Result<ReadStream, WTError> {
        match self.uniaccept.to_owned() {
            Some(a) => a.lock().await.accept_uni().await,
            None => Ok(self.inner.accept_uni().await?.into()),
        }
    }
    pub async fn accept_bi(&mut self) -> Result<(WriteStream, ReadStream), WTError> {
        match self.biaccept.to_owned() {
            Some(a) => a.lock().await.accept_bi().await,
            None => {
                let (send, recv) = self.inner.accept_bi().await?;
                Ok((send.into(), recv.into()))
            }
        }
    }

    pub async fn open_uni(&mut self) -> Result<WriteStream, WTError> {
        match self.connect {
            Some(_) => {
                let stream = UniStream::UNIWEBTRANSPORT.open(&mut self.inner).await?;
                let ws = WriteStream::open(stream, self.id).await?;
                Ok(ws)
            }
            None => Ok(self.inner.open_uni().await?.into()),
        }
    }

    pub async fn read_datagram(&mut self) -> Result<Bytes, WTError> {
        let data = self.inner.read_datagram().await?;
        Ok(data)
    }

    pub async fn send_datagram(&mut self, data: Bytes) -> Result<(), WTError> {
        self.inner.send_datagram(data)?;
        Ok(())
    }
}

type AcceptUni = dyn Future<Output = Result<ReadStream, WTError>> + Send;
type AcceptBi = dyn Future<Output = Result<(WriteStream, ReadStream), WTError>> + Send;

/// Accepting a Stream in WebTransport is serious undertaking. H3 Streams may come in the way and annoy us, because they block on read().
/// Iterating each stream to find the WebTransport will be thwarted by this annoying block_on_read by the H3 Streams.
/// WTAccept is a least complicated way as far as I know to accept a WebTransport streams.
/// If you find a better way, send patch
pub struct UniAccept {
    id: VarInt,
    conn: quinn::Connection,

    // Unoredered queue : We add async functions in to it, then it gets resolved whenever we poll them.
    uni: FuturesUnordered<Pin<Box<AcceptUni>>>,

    // Placeholders for Streams so that it doesn't get dropped
    qpack_encoder: Option<ReadStream>,
    qpack_decoder: Option<ReadStream>,
    push: Option<ReadStream>,
}

pub struct BiAccept {
    id: VarInt,
    conn: quinn::Connection,
    bi: FuturesUnordered<Pin<Box<AcceptBi>>>,
}

impl BiAccept {
    pub fn new(id: VarInt, conn: quinn::Connection) -> Self {
        Self {
            id,
            conn,
            bi: FuturesUnordered::new(),
        }
    }

    async fn decode_bi((send, recv): (SendStream, RecvStream)) -> Result<(WriteStream, ReadStream), WTError> {
        let read = ReadStream::accept(recv).await?;
        Ok((send.into(), read))
    }

    fn process_bi_stream(&mut self, (write, read): (WriteStream, ReadStream)) -> Result<Option<(WriteStream, ReadStream)>, WTError> {
        if read.id.unwrap() != self.id {
            return Err(WTError::ProtocolError("Recevied Stream with Invalid Stream ID"));
        }

        match read.stype.clone().unwrap() {
            UniStream::BIWEBTRANSPORT => Ok(Some((write, read))),
            _ => {
                tracing::warn!("[Received Bistream with Unknown Stream Header]");
                Ok(None)
            }
        }
    }

    pub async fn accept_bi(&mut self) -> Result<(WriteStream, ReadStream), WTError> {
        loop {
            tokio::select! {
                qstream = self.conn.accept_bi() => {
                    tracing::debug!("Bi Accepted");
                    self.bi.push(Box::pin(Self::decode_bi(qstream?)));
                },
                next = self.bi.next() => if let Some(rs) = next { if let Some(bistream) = self.process_bi_stream(rs?)? {
                    return Ok(bistream)
                }}
            }
        }
    }
}

impl UniAccept {
    pub fn new(id: VarInt, conn: quinn::Connection) -> Self {
        Self {
            id,
            conn,
            uni: FuturesUnordered::new(),
            qpack_encoder: None,
            qpack_decoder: None,
            push: None,
        }
    }

    fn process_uni_stream(&mut self, rs: ReadStream) -> Result<Option<ReadStream>, WTError> {
        if rs.id.unwrap() != self.id {
            return Err(WTError::ProtocolError("Received Stream with Invalid Stream ID"));
        }
        match rs.stype.clone().unwrap() {
            UniStream::UNIWEBTRANSPORT => Ok(Some(rs)),
            UniStream::QPACK_ENCODER => {
                tracing::debug!("Received QPack ecoder Unistream");
                self.qpack_encoder = Some(rs);
                Ok(None)
            }
            UniStream::QPACK_DECODER => {
                tracing::debug!("Received QPack Decoder Unistream");
                self.qpack_decoder = Some(rs);
                Ok(None)
            }
            UniStream::PUSH => {
                tracing::debug!("Received H3 Push Unistream");
                self.push = Some(rs);
                Ok(None)
            }
            _ => {
                tracing::warn!("Received Uni Stream with Unknown Header");
                Ok(None)
            }
        }
    }

    /// Blocks until a new uni stream is available on the connection
    pub async fn accept_uni(&mut self) -> Result<ReadStream, WTError> {
        loop {
            tokio::select! {
                qstream = self.conn.accept_uni() => {
                    self.uni.push(Box::pin(ReadStream::accept(qstream?))); // Push it to the UniStream Futures Queue. Hopefully, it will get resolved and the output shows in the next branch of this select!{} on the next iteration
                },
                next = self.uni.next() => if let Some(rs) = next { if let Some(rstream) = self.process_uni_stream(rs?)? {
                    return Ok(rstream)
                }}
            }
        }
    }
}
