use wt_proto::h3;

use crate::WTError;

#[derive(Debug)]
pub struct Settings {
    #[allow(dead_code)]
    reader: quinn::RecvStream,
    #[allow(dead_code)]
    writer: quinn::SendStream,
}

impl Settings {
    pub async fn accept(conn: quinn::Connection) -> Result<Settings, WTError> {
        let mut settings = h3::Settings::default();
        settings.enable_webtransport(1);

        let accept = h3::Settings::accept(conn.clone());
        let open = h3::Settings::open(conn.clone(), settings);

        // Accept & Open concurrently
        let ((settings, reader), writer) = tokio::try_join!(accept, open)?;

        if !settings.supports_webtransport() {
            return Err(WTError::WebTransportNotSupported);
        }

        Ok(Settings { reader, writer })
    }
}
