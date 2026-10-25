use std::sync::Arc;

use axum::{Router, extract, http::StatusCode, response::IntoResponse};
use clap::Parser;
use russh_extra::{Client, RemoteCommand, Session};
use serde::Deserialize;
use tokio::sync::Semaphore;
use tracing::{info, warn};

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
    /// Replace the prompt with `%s`.
    #[arg(long)]
    cmd: String,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("ssh communication error: {0}")]
    Ssh(#[from] russh_extra::Error),
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
}

fn ssh_client(config: &Config) -> Result<Client, Error> {
    let mut builder = Client::builder()
        .endpoint((&*config.host, config.port))
        .username(&*config.user);
    if let Some(passwd) = config.password.as_deref() {
        builder = builder.password(passwd);
    }
    if let Some(fp) = config.fingerprint.as_deref() {
        builder = builder.try_pinned_host_key_sha256(fp)?;
    } else {
        warn!("accepting any host key");
        builder = builder.accept_any_host_key()
    }
    Ok(builder.build())
}

struct ContextInner {
    config: Config,
    session: Session,
    lock: Semaphore,
}

type Context = Arc<ContextInner>;

#[derive(Deserialize)]
struct Request {
    prompt: String,
}

async fn proxy(
    extract::State(cx): extract::State<Context>,
    extract::Form(req): extract::Form<Request>,
) -> Result<String, Error> {
    let _permit = cx.lock.acquire().await.unwrap();
    let cmd = RemoteCommand::new(format!(
        "sudo -n npurun {}",
        cx.config.cmd.replace("%s", &req.prompt)
    ));
    // TODO: make this ws based
    let out = cx.session.command(cmd).await?;
    Ok(String::from_utf8_lossy_owned(out.stdout.to_vec()))
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
    });

    let app: Router = Router::new()
        .route("/", axum::routing::get(proxy))
        .with_state(cx);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await?;
    Ok(())
}
