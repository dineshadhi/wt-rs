use crate::{connect::Connect, settings::Settings, WTError};
use std::sync::Arc;

pub struct Connection {
    #[allow(dead_code)]
    settings: Arc<Settings>,
    #[allow(dead_code)]
    connect: Arc<Connect>,
}

impl Connection {
    pub async fn upgrade(mut conn: quinn::Connection) -> Result<Connection, WTError> {
        let settings = Settings::accept(conn.clone()).await?;
        let connect = Connect::accept(&mut conn).await?;

        let conn = Connection {
            settings: Arc::new(settings),
            connect: Arc::new(connect),
        };

        Ok(conn)
    }
}
