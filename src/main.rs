use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{
    Router,
    extract::{
        self,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use clap::Parser;
use russh_extra::{Client, Session, russh::ChannelMsg};
use tokio::sync::{Mutex, Semaphore};
use tracing::{error, info, warn};

#[derive(Parser)]
struct Config {
    #[arg(long)]
    host: String,
    #[arg(short, long, default_value_t = 22)]
    port: u16,
    /// Server public key fingerprint with SHA256:base64 encoding.
    /// You may want this for safety of your secrets.
    #[arg(long)]
    fingerprint: Option<String>,

    #[arg(short, long)]
    user: String,
    #[arg(long)]
    password: Option<String>,

    /// The command followed by `sudo npurun`.
    /// Replace the prompt with `%s`; the prompt is shell-escaped before insertion.
    #[arg(long)]
    cmd: String,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("ssh communication error: {0}")]
    Ssh(#[from] russh_extra::Error),
    #[error("ssh channel error: {0}")]
    Channel(#[from] russh_extra::russh::Error),
    #[error("websocket communication error: {0}")]
    WebSocket(#[from] axum::Error),
    #[error("proxy request lock closed")]
    LockClosed,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()).into_response()
    }
}

fn ssh_client(config: &Config) -> Result<Client, Error> {
    let mut builder = Client::builder()
        .endpoint((&*config.host, config.port))
        .username(&*config.user);
    if let Some(password) = config.password.as_deref() {
        builder = builder.password(password);
    }
    if let Some(fingerprint) = config.fingerprint.as_deref() {
        builder = builder.try_pinned_host_key_sha256(fingerprint)?;
    } else {
        warn!("accepting any host key");
        builder = builder.accept_any_host_key();
    }
    Ok(builder.build())
}

struct ContextInner {
    config: Config,
    session: Session,
    lock: Semaphore,
    pending: Mutex<HashMap<u64, String>>,
    next_request_id: AtomicU64,
}

type Context = Arc<ContextInner>;

async fn submit_request(extract::State(cx): extract::State<Context>, prompt: String) -> Response {
    let id = cx.next_request_id.fetch_add(1, Ordering::Relaxed);
    cx.pending.lock().await.insert(id, prompt);

    (
        StatusCode::ACCEPTED,
        [(header::LOCATION, format!("/ws/{id}"))],
        id.to_string(),
    )
        .into_response()
}

async fn proxy(
    extract::Path(id): extract::Path<u64>,
    extract::State(cx): extract::State<Context>,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(prompt) = cx.pending.lock().await.remove(&id) else {
        return (StatusCode::NOT_FOUND, "request not found").into_response();
    };

    ws.on_upgrade(move |socket| async move {
        if let Err(err) = stream_command(socket, cx, &prompt).await {
            error!(error = %err, "proxy websocket failed");
        }
    })
}

async fn stream_command(mut socket: WebSocket, cx: Context, prompt: &str) -> Result<(), Error> {
    let _permit = loop {
        tokio::select! {
            permit = cx.lock.acquire() => break permit.map_err(|_| Error::LockClosed)?,
            message = socket.recv() => {
                match message {
                    None | Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Err(err)) => return Err(err.into()),
                    Some(Ok(_)) => {}
                }
            }
        }
    };
    let command = cx.config.cmd.replace("%s", prompt).to_string();

    let handle = cx.session.russh_handle().await?;
    let mut channel = handle.channel_open_session().await?;
    drop(handle);
    if let Err(err) = channel.exec(true, command).await {
        let _ = channel.close().await;
        return Err(err.into());
    }

    let mut exit_status = None;
    loop {
        tokio::select! {
            message = socket.recv() => {
                match message {
                    None | Some(Ok(Message::Close(_))) => {
                        let _ = channel.close().await;
                        return Ok(());
                    }
                    Some(Err(err)) => {
                        let _ = channel.close().await;
                        return Err(err.into());
                    }
                    Some(Ok(_)) => {}
                }
            }
            message = channel.wait() => {
                match message {
                    Some(ChannelMsg::Data { data }) => {
                        if let Err(err) = socket.send(Message::Binary(data)).await {
                            let _ = channel.close().await;
                            return Err(err.into());
                        }
                    }
                    Some(ChannelMsg::ExtendedData { ext: 1, data }) => {
                        warn!(
                            stderr = %String::from_utf8_lossy(&data),
                            "remote command wrote to stderr"
                        );
                    }
                    Some(ChannelMsg::ExitStatus { exit_status: status }) => {
                        exit_status = Some(status);
                    }
                    Some(ChannelMsg::ExitSignal { signal_name, .. }) => {
                        warn!(?signal_name, "remote command terminated by signal");
                    }
                    Some(ChannelMsg::Eof) => {}
                    Some(ChannelMsg::Close) | None => break,
                    Some(_) => {}
                }
            }
        }
    }
    let close = match exit_status {
        Some(0) => Message::Close(None),
        Some(status) => {
            warn!(status, "remote command exited unsuccessfully");
            Message::Close(Some(CloseFrame {
                code: 1011,
                reason: format!("remote command exited with status {status}").into(),
            }))
        }
        None => Message::Close(Some(CloseFrame {
            code: 1011,
            reason: "remote command closed without an exit status".into(),
        })),
    };
    socket.send(close).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = Config::parse();
    let client = ssh_client(&config)?;
    let session = client.connect().await?;
    info!("ssh connection established");
    let cx = Arc::new(ContextInner {
        config,
        session,
        lock: Semaphore::new(1),
        pending: Mutex::new(HashMap::new()),
        next_request_id: AtomicU64::new(1),
    });

    let app: Router = Router::new()
        .route("/", axum::routing::post(submit_request))
        .route("/ws/{id}", axum::routing::get(proxy))
        .with_state(cx);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}
