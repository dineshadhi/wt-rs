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
use wt_proto::{
    coding::{VarInt, VarIntAsyncExt},
    h3::streams::{BiStream, UniStream},
};

#[derive(Clone)]
pub struct Connection {
    #[allow(dead_code)]
    id: Option<VarInt>,
    #[allow(dead_code)]
    inner: quinn::Connection,
    #[allow(dead_code)]
    settings: Option<Arc<Settings>>,
    #[allow(dead_code)]
    connect: Option<Arc<Connect>>,
    accept: Option<Arc<Mutex<WTAccept>>>,
}

impl From<quinn::Connection> for Connection {
    fn from(value: quinn::Connection) -> Self {
        Self {
            id: None,
            inner: value,
            settings: None,
            connect: None,
            accept: None,
        }
    }
}

impl Connection {
    /// Upgrades a simple QUIC connection to a WebTransport Connection. What WebSocket::Upgrade is for `tcp`, this is WebTransport::Upgrade for `quic`.
    pub async fn upgrade(mut conn: quinn::Connection) -> Result<Connection, WTError> {
        let settings = Settings::accept(conn.clone()).await?;
        let connect = Connect::accept(&mut conn).await?;
        let id = connect.session_id();

        let accept = WTAccept::new(id, conn.clone());

        let conn = Connection {
            id: Some(id),
            inner: conn,
            settings: Some(Arc::new(settings)),
            connect: Some(Arc::new(connect)),
            accept: Some(Arc::new(Mutex::new(accept))),
        };

        Ok(conn)
    }

    pub async fn open(mut conn: quinn::Connection) -> Result<Connection, WTError> {
        let settings = Settings::accept(conn.clone()).await?;
        let connect = Connect::open(&mut conn).await?;
        let id = connect.session_id();

        let accept = WTAccept::new(id, conn.clone());

        let conn = Connection {
            id: Some(id),
            inner: conn,
            settings: Some(Arc::new(settings)),
            connect: Some(Arc::new(connect)),
            accept: Some(Arc::new(Mutex::new(accept))),
        };

        Ok(conn)
    }

    /// Accepts a Unidirectional Stream.
    pub async fn accept_uni(&mut self) -> Result<ReadStream, WTError> {
        match &self.accept {
            Some(accept) => poll_fn(|ctx| accept.lock().unwrap().poll_accept_uni(ctx)).await,
            None => {
                let stream = self.inner.accept_uni().await?;
                Ok(stream.into())
            }
        }
    }

    /// Accepts a Bidirectional Stream.
    pub async fn accept_bi(&mut self) -> Result<(WriteStream, ReadStream), WTError> {
        match &self.accept {
            Some(accept) => poll_fn(|ctx| accept.lock().unwrap().poll_accept_bi(ctx)).await,
            None => {
                let (send, recv) = self.inner.accept_bi().await?;
                Ok((send.into(), recv.into()))
            }
        }
    }

    pub async fn open_uni(&mut self) -> Result<WriteStream, WTError> {
        match self.connect {
            Some(_) => {
                let stream = UniStream::WEBTRANSPORT.open(&mut self.inner).await?;
                let ws = WriteStream::open(stream, self.id.unwrap()).await?;
                Ok(ws)
            }
            None => Ok(self.inner.open_uni().await?.into()),
        }
    }

    pub async fn open_bi(&mut self) -> Result<(WriteStream, ReadStream), WTError> {
        match self.connect {
            Some(_) => {
                let (ws, rs) = BiStream::WEBTRANSPORT.open(&mut self.inner).await?;
                let ws = WriteStream::open(ws, self.id.unwrap()).await?;
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

type SelectedBi = Result<(BiStream, (WriteStream, ReadStream)), WTError>;
type SelectedUni = Result<(UniStream, ReadStream), WTError>;

/// Accepting a Stream in WebTransport on Async land is a serious undertaking. H3 Streams may come in the way and annoy us, because they block on read() (I think its bad design).
/// Iterating each stream to find the WebTransport will be thwarted by this annoying block_on_read by the H3 Streams.
/// WTAccept is a least complicated way as far as I know to accept a WebTransport streams.
/// We can do stack tokio::spawns and call it a day, but this is a nifty way to handle things ergonimically.
/// If you find a better way, send patch
pub struct WTAccept {
    id: VarInt,
    #[allow(unused)]
    conn: quinn::Connection,

    // Unoredered queue : We add async functions in to it, then it gets resolved whenever we poll them.
    drained_uni: FuturesUnordered<Pin<Box<PendingUni>>>,
    drained_bi: FuturesUnordered<Pin<Box<PendingBi>>>,

    // Streams that takes conn and accepts a stream every time we poll.
    uni: Pin<Box<AcceptUni>>,
    bi: Pin<Box<AcceptBi>>,

    // Placeholders for H3 Streams so that it doesn't get dropped and close the connection
    qpack_encoder: Option<ReadStream>,
    qpack_decoder: Option<ReadStream>,
    push: Option<ReadStream>,
}

// The strategy is as follows,
// 1. Drain all Streams from the underlying Quic Connection to a FuturesUnordered Queue.
// 2. Poll this FuturesQueue to get the respective stream.
// 3. Analyse if its a WebTransport Stream or else do this again.

impl WTAccept {
    pub fn new(id: VarInt, conn: quinn::Connection) -> Self {
        // Create Futures::Stream of Accepted Streams from the Quic Connection
        let uni = futures::stream::unfold(conn.clone(), |c| async move { Some((c.accept_uni().await, c)) });
        let bi = futures::stream::unfold(conn.clone(), |c| async move { Some((c.accept_bi().await, c)) });

        Self {
            id,
            conn,
            drained_uni: FuturesUnordered::new(),
            drained_bi: FuturesUnordered::new(),
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

        Ok((stype, stream.into()))
    }

    // Drains all Incoming Unistreams in to a Unordered FutureQueue to be polled later
    fn drain_uni(&mut self, ctx: &mut Context<'_>) -> Poll<Result<(), WTError>> {
        while let Poll::Ready(s) = self.uni.poll_next_unpin(ctx) {
            match s {
                Some(stream) => {
                    let pending = Self::decode_uni(self.id, stream?);
                    self.drained_uni.push(Box::pin(pending));
                }
                // Happens when session is terminated
                None => return Poll::Ready(Err(WTError::AcceptError("UniStream drain failed : session terminated probably"))),
            }
        }

        // All streams are drained and added to the Futures, now we are ready for processing the streams
        Poll::Ready(Ok(()))
    }

    // Polls the next available drained stream in the FutureQueue and returns it
    fn select_uni(&mut self, ctx: &mut Context<'_>) -> Poll<SelectedUni> {
        if let Poll::Ready(s) = self.drained_uni.poll_next_unpin(ctx) {
            return match s {
                Some(streams) => Poll::Ready(Ok(streams?)),
                None => Poll::Pending,
            };
        }

        Poll::Pending
    }

    // A convinient poll wrapper to loop through all the blocking H3 streams and find the WebTransport Stream
    pub fn poll_accept_uni(&mut self, ctx: &mut Context<'_>) -> Poll<Result<ReadStream, WTError>> {
        loop {
            // Wait for all Unistreams to be drained in to Futures Queue. No worries, if there is no streams to be accepted, it simply returns
            ready!(self.drain_uni(ctx))?;

            // Then search for a stream that is readable and ready
            let (stype, stream) = ready!(self.select_uni(ctx))?;

            match stype {
                // Return if its WebTransports.
                UniStream::WEBTRANSPORT => return Poll::Ready(Ok(stream)),
                // Otherwise, its a H3 stream probably. So, simply store it to prevent dropping.
                UniStream::QPACK_ENCODER => self.qpack_encoder = Some(stream),
                UniStream::QPACK_DECODER => self.qpack_decoder = Some(stream),
                UniStream::PUSH => self.push = Some(stream),
                _ => {
                    // Drop a warning, if we get a weird header.
                    tracing::warn!("Received Unknown UniStream {:x?}", stype.0.into_inner())
                }
            };
        }
    }

    async fn decode_bi(id: VarInt, streams: (quinn::SendStream, quinn::RecvStream)) -> SelectedBi {
        let send = streams.0;
        let mut recv = streams.1;

        let stype = BiStream(recv.read_varint().await?);
        let sid = recv.read_varint().await?;

        if sid != id {
            return Err(WTError::ProtocolError("BiStream Accept : Session ID mismatch"));
        }

        Ok((stype, (send.into(), recv.into())))
    }

    // Drains all Incoming Bistreams in to a Unordered FutureQueue to be polled later
    fn drain_bi(&mut self, ctx: &mut Context<'_>) -> Poll<Result<(), WTError>> {
        while let Poll::Ready(s) = self.bi.poll_next_unpin(ctx) {
            match s {
                Some(streams) => {
                    let pending = Self::decode_bi(self.id, streams?);
                    self.drained_bi.push(Box::pin(pending));
                }
                // Happens when session is terminated
                None => return Poll::Ready(Err(WTError::AcceptError("BiStream drain failed : session terminated probably"))),
            }
        }

        // All streams are drained and added to the Futures, now we are ready for processing the streams
        Poll::Ready(Ok(()))
    }

    // Polls the next available drained stream in the FutureQueue and returns it
    fn select_bi(&mut self, ctx: &mut Context<'_>) -> Poll<SelectedBi> {
        if let Poll::Ready(s) = self.drained_bi.poll_next_unpin(ctx) {
            return match s {
                Some(streams) => Poll::Ready(Ok(streams?)),
                None => Poll::Pending,
            };
        }

        Poll::Pending
    }

    // A convinient poll wrapper to loop through all the blocking H3 streams and find the WebTransport Stream
    pub fn poll_accept_bi(&mut self, ctx: &mut Context<'_>) -> Poll<Result<(WriteStream, ReadStream), WTError>> {
        loop {
            // Wait for all Bistreams to be drained in to Futures Queue. No worries, if there is no streams to be accepted, it simply returns
            ready!(self.drain_bi(ctx))?;

            // Then search for a stream that is readable and ready
            let (stype, (ws, rs)) = ready!(self.select_bi(ctx))?;

            match stype {
                BiStream::WEBTRANSPORT => return Poll::Ready(Ok((ws, rs))),
                _ => {
                    // Drop a warning, if we get a weird header
                    tracing::warn!("Received BiStream with Unknown Header : {:x?}", stype.0.into_inner());
                    // Signal to be polled again, because we haven't found the WebTransport Stream yet
                }
            }
        }
    }
}
