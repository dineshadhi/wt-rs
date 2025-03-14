use std::{future::Future, pin::Pin, sync::Arc};

use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use quinn::VarInt;
use tokio::sync::Mutex;
use wt_proto::{
    coding::VarIntExt,
    coding::VarIntMutExt,
    h3::{unistream::UniStream, H3Error},
};

use crate::{connect::Connect, settings::Settings, WTError};

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
    accept: Option<Arc<Mutex<WTAccept>>>,
}

impl Connection {
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
            accept: Some(Arc::new(Mutex::new(accept))),
        };

        Ok(conn)
    }

    pub async fn accept_uni(&mut self) -> Result<quinn::RecvStream, WTError> {
        match self.accept.to_owned() {
            Some(a) => a.lock().await.accept_uni().await,
            None => Ok(self.inner.accept_uni().await?),
        }
    }

    pub async fn open_uni(&mut self) -> Result<quinn::SendStream, WTError> {
        match self.connect {
            Some(_) => {
                let (mut buffer, mut stream) = UniStream::WEBTRANSPORT.open(&mut self.inner).await?;
                buffer.write_varint(self.id.into_inner());
                stream.write_all(&buffer[..]).await?;
                Ok(stream)
            }
            None => Ok(self.inner.open_uni().await?),
        }
    }

    pub async fn read_datagram(&mut self) -> Result<Bytes, WTError> {
        let data = self.inner.read_datagram().await?;
        Ok(Bytes::from(data))
    }

    pub async fn send_datagram(&mut self, data: Bytes) -> Result<(), WTError> {
        self.inner.send_datagram(data)?;
        Ok(())
    }
}

type AcceptUni = dyn Future<Output = Result<(UniStream, quinn::RecvStream), H3Error>> + Send;

/// Accepting a Stream in WebTransport is serious undertaking. H3 Streams may come in the way and annoy us, because they block on read().
/// Iterating each stream to find the WebTransport will be thwarted by this annoying block_on_read by the H3 Streams.
/// WTAccept is a least complicated way as far as I know to accept a WebTransport streams.
/// If you find a better way, send patch
pub struct WTAccept {
    id: VarInt,
    conn: quinn::Connection,
    uni: FuturesUnordered<Pin<Box<AcceptUni>>>,
}

impl WTAccept {
    pub fn new(id: VarInt, conn: quinn::Connection) -> Self {
        Self {
            id,
            conn,
            uni: FuturesUnordered::new(),
        }
    }

    async fn process_uni_stream(&mut self, s: (UniStream, quinn::RecvStream)) -> Result<Option<quinn::RecvStream>, WTError> {
        let (stype, mut stream) = (s.0, s.1);

        match stype {
            UniStream::WEBTRANSPORT => {
                let sid = stream.read_varint().await?;
                if self.id != sid {
                    return Err(WTError::ProtocolError("Recevied Stream with Invalid Stream ID"));
                }
                Ok(Some(stream))
            }
            _ => Ok(None),
        }
    }

    pub async fn accept_uni(&mut self) -> Result<quinn::RecvStream, WTError> {
        loop {
            tokio::select! {
                qstream = self.conn.accept_uni() => {
                    // Unistream::Poll Simply reads the type of the stream asynchronously.
                    let poll = UniStream::poll(qstream?);
                    // Push it to the UniStream Futures Queue. Hopefully, it will get resolved and the output shows in the next branch of this select!{} on the next iteration
                    self.uni.push(Box::pin(poll));
                },
               s = self.uni.next() => match s.transpose() {
                   Ok(s) => if let Some(wtstream) = s {
                       if let Some(wts) = self.process_uni_stream(wtstream).await? {
                           return Ok(wts)
                       }
                   },
                   Err(e) => {
                       // We don't return here, because we are inside loop { select!{} }, there might be other streams waiting in queue.
                       tracing::error!("[Error Accepting Uni Stream][{e}]");
                   }
               }
            }
        }
    }
}
