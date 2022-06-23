use std::cmp::Ordering;
use std::mem;
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::{DateTime, Utc};
use clap::Parser;
use futures::future;
use sonnerie::row_format::{parse_row_format, RowFormat};
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

    /// The list of channels to stream.
    channels: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Opts { channels, db_path, commit_timeout_secs } = Opts::parse();

    // default configuration is to join chat as anonymous.
    let config = ClientConfig::default();
    let (mut incoming_messages, client) =
        TwitchIRCClient::<SecureTCPTransport, StaticLoginCredentials>::new(config);
    let (messages_sender, mut messages_receiver) = mpsc::channel(2000);

    // first thing you should do: start consuming incoming messages,
    // otherwise they will back up.
    let fetch_handle = tokio::spawn(async move {
        while let Some(message) = incoming_messages.recv().await {
            if let Ok(message) = TimedUserMessage::from_private_nessage(message) {
                if messages_sender.send(message).await.is_err() {
                    break;
                }
            }
        }
    });

    // join a channels
    for channel in channels {
        client.join(channel)?;
    }

    let write_handle = tokio::task::spawn_blocking(move || {
        let format = parse_row_format("ss");
        let mut previous_commit_time = Instant::now();
        let mut messages = Vec::new();
        let mut buffer = Vec::new();

        while let Some(message) = messages_receiver.blocking_recv() {
            let now = Instant::now();
            if now.duration_since(previous_commit_time).as_secs() > commit_timeout_secs {
                previous_commit_time = now;
                messages = prepare_and_write_messages(&db_path, messages, &format, &mut buffer)?;
            }
            messages.push(message);
        }

        prepare_and_write_messages(&db_path, messages, &format, &mut buffer).map(drop)
    });

    // keep the tokio executor alive.
    // If you return instead of waiting the background task will exit.
    let (fetch_result, write_result) = future::join(fetch_handle, write_handle).await;

    fetch_result?;
    write_result?.unwrap();

    Ok(())
}

fn prepare_and_write_messages(
    path: &Path,
    mut messages: Vec<TimedUserMessage>,
    row_format: &Box<dyn RowFormat>,
    protocol_buffer: &mut Vec<u8>,
) -> Result<Vec<TimedUserMessage>, WriteFailure> {
    let mut txn = CreateTx::new(path)?;

    messages.sort_unstable();
    messages.dedup_by_key(|msg| msg.server_timestamp);

    for mut message in messages.drain(..) {
        let mut text = mem::take(&mut message.sender_login);
        text.push(' ');
        text.push_str(&mut message.message_text.replace(' ', "\\ "));

        protocol_buffer.clear();
        let ts = message.server_timestamp.timestamp_nanos() as u64;
        row_format.to_stored_format(ts, &text, protocol_buffer).unwrap();
        txn.add_record(&message.channel_login, "ss", protocol_buffer)?;
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
    fn from_private_nessage(msg: ServerMessage) -> Result<TimedUserMessage, ServerMessage> {
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
