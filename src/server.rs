use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast::Receiver,
};
use tokio_tungstenite::tungstenite::{self, Message};
use tracing::info;

pub async fn listen(addr: String, rx: Receiver<Vec<Message>>, timeout: Duration) {
    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.expect("Failed to bind");
    info!("Listening on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        let rx = rx.resubscribe();
        tokio::spawn(async move {
            if let Err(err) = accept_connection(stream, rx, timeout).await {
                info!("Client failed to connect: {}", err);
            }
        });
    }
}

async fn accept_connection(
    stream: TcpStream,
    mut rx: Receiver<Vec<Message>>,
    timeout: Duration,
) -> anyhow::Result<()> {
    let addr = stream
        .peer_addr()
        .context("Connected streams should have a peer address")?;

    let ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .context("Error during the websocket handshake occurred")?;

    info!("Connected: {}", addr);

    let (mut write, mut read) = ws_stream.split();

    let read_handle = async {
        loop {
            let timeout = tokio::time::timeout(timeout, read.next());
            let message = timeout.await.context("Client timeout")?;
            let message = message.ok_or(tungstenite::Error::ConnectionClosed)??;
            match message {
                Message::Close(_) => Err(tungstenite::Error::ConnectionClosed),
                _ => Ok(()),
            }?;
        }

        #[expect(unreachable_code)]
        Ok(())
    };

    let write_handle = async {
        loop {
            let message_batch = rx.recv().await?;
            for message in message_batch {
                write.feed(message).await?;
            }

            write.flush().await?;
        }

        #[expect(unreachable_code)]
        Ok(())
    };

    let res: anyhow::Result<()> = tokio::select! {
        res = read_handle => res,
        res = write_handle => res,
    };

    if let Err(err) = res {
        info!("Disconnected: {}", err);
    }

    Ok(())
}
