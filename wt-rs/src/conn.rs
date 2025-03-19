use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use crate::{
    connect::Connect,
    settings::Settings,
    streams::{ReadStream, WriteStream},
    WTError,
};
use bytes::Bytes;
use futures::{future::poll_fn, ready, stream::FuturesUnordered, Stream, StreamExt};
use quinn::VarInt;
use wt_proto::{
    coding::VarIntExt,
    h3::streams::{BiStream, UniStream},
};

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
    accept: Arc<Mutex<WTAccept>>,
}

impl Connection {
    /// Upgrades a simple QUIC connection to a WebTransport Connection. What WebSocket::Upgrade is for `tcp`, this is WebTransport::Upgrade for `quic`.
    pub async fn upgrade(mut conn: quinn::Connection) -> Result<Connection, WTError> {
        let settings = Settings::accept(conn.clone()).await?;
        let connect = Connect::accept(&mut conn).await?;
        let id = connect.session_id();

        let accept = WTAccept::new(id, conn.clone());

        let conn = Connection {
            id,
            inner: conn,
            settings: Some(Arc::new(settings)),
            connect: Some(Arc::new(connect)),
            accept: Arc::new(Mutex::new(accept)),
        };

        Ok(conn)
    }

    pub async fn open(mut conn: quinn::Connection) -> Result<Connection, WTError> {
        let settings = Settings::accept(conn.clone()).await?;
        let connect = Connect::open(&mut conn).await?;
        let id = connect.session_id();

        let accept = WTAccept::new(id, conn.clone());

        let conn = Connection {
            id,
            inner: conn,
            settings: Some(Arc::new(settings)),
            connect: Some(Arc::new(connect)),
            accept: Arc::new(Mutex::new(accept)),
        };

        Ok(conn)
    }

    /// Accepts a Unidirectional Stream.
    pub async fn accept_uni(&mut self) -> Result<ReadStream, WTError> {
        if self.connect.is_some() {
            poll_fn(|ctx| self.accept.lock().unwrap().poll_accept_uni(ctx)).await
        } else {
            let stream = self.inner.accept_uni().await?;
            Ok(stream.into())
        }
    }

    /// Accepts a Bidirectional Stream.
    pub async fn accept_bi(&mut self) -> Result<(WriteStream, ReadStream), WTError> {
        if self.connect.is_some() {
            poll_fn(|ctx| self.accept.lock().unwrap().poll_accept_bi(ctx)).await
        } else {
            let (send, recv) = self.inner.accept_bi().await?;
            Ok((send.into(), recv.into()))
        }
    }

    pub async fn open_uni(&mut self) -> Result<WriteStream, WTError> {
        match self.connect {
            Some(_) => {
                let stream = UniStream::WEBTRANSPORT.open(&mut self.inner).await?;
                let ws = WriteStream::open(stream, self.id).await?;
                Ok(ws)
            }
            None => Ok(self.inner.open_uni().await?.into()),
        }
    }

    pub async fn open_bi(&mut self) -> Result<(WriteStream, ReadStream), WTError> {
        match self.connect {
            Some(_) => {
                let (ws, rs) = BiStream::WEBTRANSPORT.open(&mut self.inner).await?;
                let ws = WriteStream::open(ws, self.id).await?;
                Ok((ws, rs.into()))
            }
            None => {
                let (ws, rs) = self.inner.open_bi().await?;
                Ok((ws.into(), rs.into()))
            }
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

type AcceptUni = dyn Stream<Item = Result<quinn::RecvStream, quinn::ConnectionError>> + Send;
type AcceptBi = dyn Stream<Item = Result<(quinn::SendStream, quinn::RecvStream), quinn::ConnectionError>> + Send;
type PendingUni = dyn Future<Output = Result<(UniStream, ReadStream), WTError>> + Send + 'static;
type PendingBi = dyn Future<Output = Result<(BiStream, (WriteStream, ReadStream)), WTError>> + Send + 'static;

/// Accepting a Stream in WebTransport is serious undertaking. H3 Streams may come in the way and annoy us, because they block on read().
/// Iterating each stream to find the WebTransport will be thwarted by this annoying block_on_read by the H3 Streams.
/// WTAccept is a least complicated way as far as I know to accept a WebTransport streams.
/// If you find a better way, send patch
pub struct WTAccept {
    id: VarInt,
    #[allow(unused)]
    conn: quinn::Connection,

    // Unoredered queue : We add async functions in to it, then it gets resolved whenever we poll them.
    pending_uni: FuturesUnordered<Pin<Box<PendingUni>>>,
    pending_bi: FuturesUnordered<Pin<Box<PendingBi>>>,

    // Streams that takes conn and accepts a stream every time we poll.
    uni: Pin<Box<AcceptUni>>,
    bi: Pin<Box<AcceptBi>>,

    // Placeholders for Streams so that it doesn't get dropped
    qpack_encoder: Option<ReadStream>,
    qpack_decoder: Option<ReadStream>,
    push: Option<ReadStream>,
}

impl WTAccept {
    pub fn new(id: VarInt, conn: quinn::Connection) -> Self {
        let uni = futures::stream::unfold(conn.clone(), |c| async move { Some((c.accept_uni().await, c)) });
        let bi = futures::stream::unfold(conn.clone(), |c| async move { Some((c.accept_bi().await, c)) });

        Self {
            id,
            conn,
            pending_uni: FuturesUnordered::new(),
            pending_bi: FuturesUnordered::new(),
            uni: Box::pin(uni),
            bi: Box::pin(bi),
            qpack_encoder: None,
            qpack_decoder: None,
            push: None,
        }
    }

    async fn decode_uni(id: VarInt, mut stream: quinn::RecvStream) -> Result<(UniStream, ReadStream), WTError> {
        let stype = UniStream(stream.read_varint().await?);
        let sid = stream.read_varint().await?;

        if sid != id {
            return Err(WTError::ProtocolError("UniStream Accept : Session ID mismatch"));
        }

        tracing::debug!("New QUIC Uni Stream !!!");

        Ok((stype, stream.into()))
    }

    pub fn poll_accept_uni(&mut self, ctx: &mut Context<'_>) -> Poll<Result<ReadStream, WTError>> {
        loop {
            if let Poll::Ready(Some(stream)) = self.uni.poll_next_unpin(ctx) {
                let pending = Self::decode_uni(self.id, stream?);
                self.pending_uni.push(Box::pin(pending));
                continue; // Loop through all streams from accept_unis() on quic connection.
            }

            // Then search for a stream that is readable and ready
            let (stype, stream) = match ready!(self.pending_uni.poll_next_unpin(ctx)) {
                Some(s) => s?,
                None => return Poll::Pending, // If not, return Pending
            };

            match stype {
                // Return if its WebTransports
                UniStream::WEBTRANSPORT => return Poll::Ready(Ok(stream)),
                // Otherwise, simply store it to prevent dropping
                UniStream::QPACK_ENCODER => self.qpack_encoder = Some(stream),
                UniStream::QPACK_DECODER => self.qpack_decoder = Some(stream),
                UniStream::PUSH => self.push = Some(stream),
                _ => {
                    tracing::warn!("Received Unknown UniStream {:x?}", stype.0.into_inner())
                }
            }
        }
    }

    async fn decode_bi(id: VarInt, streams: (quinn::SendStream, quinn::RecvStream)) -> Result<(BiStream, (WriteStream, ReadStream)), WTError> {
        let send = streams.0;
        let mut recv = streams.1;

        let stype = BiStream(recv.read_varint().await?);
        let sid = recv.read_varint().await?;

        if sid != id {
            return Err(WTError::ProtocolError("BiStream Accept : Session ID mismatch"));
        }

        Ok((stype, (send.into(), recv.into())))
    }

    pub fn poll_accept_bi(&mut self, ctx: &mut Context<'_>) -> Poll<Result<(WriteStream, ReadStream), WTError>> {
        loop {
            if let Poll::Ready(Some(stream)) = self.bi.poll_next_unpin(ctx) {
                let pending = Self::decode_bi(self.id, stream?);
                self.pending_bi.push(Box::pin(pending));
                continue; // loop through all bistreams from accpet_bi() on the quic connection
            }

            // Then search for a stream that is readable and ready
            let (stype, (ws, rs)) = match ready!(self.pending_bi.poll_next_unpin(ctx)) {
                Some(s) => s?,
                None => return Poll::Pending, // Return Pending if not
            };

            match stype {
                // Return if its WebTransports
                BiStream::WEBTRANSPORT => return Poll::Ready(Ok((ws, rs))),
                _ => {
                    tracing::debug!("Received BiStream with Unknown Header : {:x?}", stype.0.into_inner());
                }
            }
        }
    }
}
