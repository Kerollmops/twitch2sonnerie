use std::cmp::Ordering;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::{DateTime, Utc};
use clap::Parser;
use rustmann::protos::riemann::Event;
use rustmann::{EventBuilder, RiemannClient, RiemannClientOptionsBuilder};
use sonnerie::{CreateTx, WriteFailure};
use tokio::sync::mpsc;
use twitch_irc::login::StaticLoginCredentials;
use twitch_irc::message::ServerMessage;
use twitch_irc::{ClientConfig, SecureTCPTransport, TwitchIRCClient};

/// A tool to save the chats of Twitch channels into a [sonnerie] database.
#[derive(Debug, Parser)]
#[clap(version, author = "Kerollmops <renault.cle@gmail.com>")]
struct Opts {
    /// The sonneries database path.
    #[clap(long)]
    db_path: PathBuf,

    /// The amount of time to wait between each commit into the sonnerie database.
    #[clap(long, default_value = "30")]
    commit_timeout_secs: u64,

    #[clap(long, default_value = "twitch2sonnerie")]
    riemann_service_name: String,
    #[clap(long, default_value = "localhost")]
    riemann_host: String,
    #[clap(long, default_value = "5555")]
    riemann_port: u16,

    /// The list of channels to stream.
    channels: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Opts {
        channels,
        db_path,
        commit_timeout_secs,
        riemann_service_name,
        riemann_host,
        riemann_port,
    } = Opts::parse();

    // create the configuration for the riemann monitoring
    let (event_sender, mut event_receiver) = mpsc::unbounded_channel();
    let riemann_client = RiemannClient::new(
        &RiemannClientOptionsBuilder::default().host(riemann_host).port(riemann_port).build(),
    );

    // default configuration is to join chat as anonymous.
    let (mut incoming_messages, client) =
        TwitchIRCClient::<SecureTCPTransport, StaticLoginCredentials>::new(ClientConfig::default());
    let (messages_sender, messages_receiver) = mpsc::channel(2000);

    // first thing you should do: start consuming incoming messages,
    // otherwise they will back up.
    let fetch_handle = tokio::spawn(async move {
        while let Some(message) = incoming_messages.recv().await {
            if let Ok(message) = TimedUserMessage::from_private_message(message) {
                if messages_sender.send(message).await.is_err() {
                    break;
                }
            }
        }
    });

    // A tokio tasks that send the events to riemann.
    let monitoring_handle = tokio::spawn(async move {
        while let Some(event) = event_receiver.recv().await {
            if let Err(err) = riemann_client.send_events(vec![event]).await {
                eprintln!("While sending the event to Riemann: {}", err);
            }
        }
    });

    // join a channels
    for channel in channels {
        client.join(channel)?;
    }

    let write_handle = tokio::task::spawn_blocking(move || {
        if let Err(e) = write_incoming_messages(
            messages_receiver,
            commit_timeout_secs,
            db_path,
            &riemann_service_name,
            event_sender.clone(),
        ) {
            let event = EventBuilder::new()
                .service(&riemann_service_name)
                .host(gethostname::gethostname().to_string_lossy())
                .state("error")
                .description(WriteFailureDisplay(e).to_string())
                .build();

            if let Err(err) = event_sender.send(event) {
                eprintln!("Error while sending an event in the monitoring channel: {}", err);
            }
        }
    });

    // keep the tokio executor alive.
    // If you return instead of waiting the background task will exit.
    let (fetch_result, write_result, monitoring_result) =
        futures::join!(fetch_handle, write_handle, monitoring_handle);

    fetch_result?;
    write_result?;
    monitoring_result?;

    Ok(())
}

fn write_incoming_messages(
    mut messages_receiver: mpsc::Receiver<TimedUserMessage>,
    commit_timeout_secs: u64,
    db_path: PathBuf,
    riemann_service_name: &str,
    event_sender: mpsc::UnboundedSender<Event>,
) -> Result<(), WriteFailure> {
    let mut previous_commit_time = Instant::now();
    let mut messages = Vec::new();

    while let Some(message) = messages_receiver.blocking_recv() {
        let now = Instant::now();
        if now.duration_since(previous_commit_time).as_secs() > commit_timeout_secs {
            previous_commit_time = now;
            let number_of_messages = messages.len();
            messages = prepare_and_write_messages(&db_path, messages)?;

            let event = EventBuilder::new()
                .service(riemann_service_name)
                .host(gethostname::gethostname().to_string_lossy())
                .state("ok")
                .description(format!(
                    "{} messages were appended into the sonnerie database",
                    number_of_messages
                ))
                .metric_sint64(number_of_messages as i64)
                .ttl(commit_timeout_secs as f32 * 2.0)
                .build();

            if let Err(err) = event_sender.send(event) {
                eprintln!("Error while sending an event in the monitoring channel: {}", err);
            }
        }
        messages.push(message);
    }

    prepare_and_write_messages(&db_path, messages).map(drop)
}

fn prepare_and_write_messages(
    path: &Path,
    mut messages: Vec<TimedUserMessage>,
) -> Result<Vec<TimedUserMessage>, WriteFailure> {
    let mut txn = CreateTx::new(path)?;

    messages.sort_unstable();
    messages.dedup_by_key(|msg| msg.server_timestamp);

    for message in messages.drain(..) {
        txn.add_record(
            &message.channel_login,
            message.server_timestamp.naive_utc(),
            sonnerie::record(message.sender_login.as_str()).add(message.message_text.as_str()),
        )?;
    }

    txn.commit().map_err(Into::into).map(|_| messages)
}

#[derive(Debug, PartialEq, Eq)]
struct TimedUserMessage {
    server_timestamp: DateTime<Utc>,
    channel_login: String,
    sender_login: String,
    message_text: String,
}

impl TimedUserMessage {
    fn from_private_message(msg: ServerMessage) -> Result<TimedUserMessage, ServerMessage> {
        match msg {
            ServerMessage::Privmsg(msg) => Ok(TimedUserMessage {
                server_timestamp: msg.server_timestamp,
                channel_login: msg.channel_login,
                sender_login: msg.sender.login,
                message_text: msg.message_text,
            }),
            otherwise => Err(otherwise),
        }
    }
}

impl PartialOrd for TimedUserMessage {
    fn partial_cmp(&self, other: &TimedUserMessage) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimedUserMessage {
    fn cmp(&self, other: &TimedUserMessage) -> Ordering {
        self.channel_login
            .cmp(&other.channel_login)
            .then_with(|| self.server_timestamp.cmp(&other.server_timestamp))
    }
}

struct WriteFailureDisplay(WriteFailure);

impl fmt::Display for WriteFailureDisplay {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.0 {
            WriteFailure::KeyOrderingViolation { first, second } => {
                write!(
                    f,
                    "The key `{:?}` does not come lexicographically after `{:?}`, \
                    but they were added in that order",
                    second, first
                )
            }
            WriteFailure::TimeOrderingViolation { first, second, key } => {
                write!(
                    f,
                    "The timestamp `{}` does not come chronologically after `{}`, \
                    but they were added in that order, in the same key (`{:?}`)",
                    second, first, key
                )
            }
            WriteFailure::IncorrectLength(_len) => f.write_str("The size of data was not expected"),
            WriteFailure::IOError(e) => write!(f, "{}", e),
        }
    }
}
