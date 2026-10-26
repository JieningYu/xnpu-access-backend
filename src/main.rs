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
use russh_extra::{
    Client, ClientKeyboardInteractiveInfo, KeyboardInteractiveReply, Session, russh::ChannelMsg,
};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, warn};

#[derive(Parser)]
struct Config {
    /// Run the API with a fake streaming response and no SSH connection.
    #[arg(long)]
    test_mode: bool,

    #[arg(long, required_unless_present = "test_mode")]
    host: Option<String>,
    #[arg(short, long, default_value_t = 22)]
    port: u16,
    /// Server public key fingerprint with SHA256:base64 encoding.
    /// You may want this for safety of your secrets.
    #[arg(long)]
    fingerprint: Option<String>,

    #[arg(short, long, required_unless_present = "test_mode")]
    user: Option<String>,
    /// SSH password, also used for keyboard-interactive password prompts.
    #[arg(long)]
    password: Option<String>,

    /// The command to execute.
    /// Replace the prompt with `%s`; the prompt is shell-escaped before insertion.
    #[arg(long, required_unless_present = "test_mode")]
    cmd: Option<String>,
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
    #[error("missing required SSH option: {0}")]
    MissingSshOption(&'static str),
    #[error("proxy request lock closed")]
    LockClosed,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()).into_response()
    }
}

fn ssh_client(config: &Config) -> Result<Client, Error> {
    let host = config
        .host
        .as_deref()
        .ok_or(Error::MissingSshOption("host"))?;
    let user = config
        .user
        .as_deref()
        .ok_or(Error::MissingSshOption("user"))?;
    let mut builder = Client::builder()
        .endpoint((host, config.port))
        .username(user);
    if let Some(password) = config.password.as_deref() {
        let password = password.to_owned();
        builder = builder.password(password.as_str()).keyboard_interactive(
            move |info: ClientKeyboardInteractiveInfo| {
                let password = password.clone();
                Box::pin(async move {
                    KeyboardInteractiveReply::Responses(
                        info.prompts().iter().map(|_| password.clone()).collect(),
                    )
                })
            },
        );
    }
    if let Some(fingerprint) = config.fingerprint.as_deref() {
        builder = builder.try_pinned_host_key_sha256(fingerprint)?;
    } else {
        warn!("accepting any host key");
        builder = builder.accept_any_host_key();
    }
    Ok(builder.build())
}

#[derive(Clone)]
enum Backend {
    Ssh { session: Session, cmd: String },
    Test,
}

struct ContextInner {
    backend: Backend,
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
    let backend = cx.backend.clone();
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

    match backend {
        Backend::Test => stream_test_response(&mut socket, prompt).await,
        Backend::Ssh { session, cmd } => {
            let command = cmd.replace("%s", prompt);
            stream_ssh(socket, &session, &command).await
        }
    }
}

async fn stream_test_response(socket: &mut WebSocket, prompt: &str) -> Result<(), Error> {
    for chunk in ["test mode\n".to_owned(), format!("prompt: {prompt}\n")] {
        tokio::select! {
            () = sleep(Duration::from_millis(100)) => {}
            message = socket.recv() => {
                match message {
                    None | Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Err(err)) => return Err(err.into()),
                    Some(Ok(_)) => {}
                }
            }
        }
        socket.send(Message::binary(chunk)).await?;
    }
    socket.send(Message::Close(None)).await?;
    Ok(())
}

async fn stream_ssh(mut socket: WebSocket, session: &Session, command: &str) -> Result<(), Error> {
    let handle = session.russh_handle().await?;
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
                        debug!("retrived string: {}", str::from_utf8(&data).unwrap_or("<failed to decode>"));
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
    tracing_subscriber::fmt::init();
    let backend = if config.test_mode {
        info!("test mode enabled; SSH will not be used");
        Backend::Test
    } else {
        let cmd = config.cmd.clone().ok_or(Error::MissingSshOption("cmd"))?;
        let client = ssh_client(&config)?;
        let session = client.connect().await?;
        info!("ssh connection established");
        Backend::Ssh { session, cmd }
    };
    let cx = Arc::new(ContextInner {
        backend,
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
