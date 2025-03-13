use std::{future::Future, pin::Pin, sync::Arc};

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::sync::Mutex;
use wt_proto::h3::{unistream::UniStream, H3Error};

use crate::{connect::Connect, settings::Settings, WTError};

pub struct Connection {
    #[allow(dead_code)]
    inner: quinn::Connection,
    #[allow(dead_code)]
    settings: Arc<Settings>,
    #[allow(dead_code)]
    connect: Arc<Connect>,
    accept: Arc<Mutex<WTAccept>>,
}

impl Connection {
    pub async fn upgrade(mut conn: quinn::Connection) -> Result<Connection, WTError> {
        let settings = Settings::accept(conn.clone()).await?;
        let connect = Connect::accept(&mut conn).await?;

        let accept = WTAccept::new(conn.clone());

        let conn = Connection {
            inner: conn,
            settings: Arc::new(settings),
            connect: Arc::new(connect),
            accept: Arc::new(Mutex::new(accept)),
        };

        Ok(conn)
    }

    pub async fn accept_uni(&mut self) -> Result<quinn::RecvStream, WTError> {
        self.accept.lock().await.accept_uni().await
    }
}

type AcceptUni = dyn Future<Output = Result<(UniStream, quinn::RecvStream), H3Error>> + Send;

/// Accepting a Stream in WebTransport is serious undertaking. H3 Streams may come in the way and annoy us, because they block on read().
/// Iterating each stream to find the WebTransport will be thwarted by this annoying block_on_read by the H3 Streams.
/// WTAccept is a least complicated way as far as I know to accept a WebTransport streams.
/// If you find a better way, send patch
pub struct WTAccept {
    conn: quinn::Connection,
    uni: FuturesUnordered<Pin<Box<AcceptUni>>>,
}

impl WTAccept {
    pub fn new(conn: quinn::Connection) -> Self {
        Self {
            conn,
            uni: FuturesUnordered::new(),
        }
    }

    fn process_uni_stream(&mut self, s: (UniStream, quinn::RecvStream)) -> Option<quinn::RecvStream> {
        Some(s.1)
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
                       if let Some(wts) = self.process_uni_stream(wtstream) {
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
