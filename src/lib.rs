use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};

use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_stream::Stream;
use tokio_stream::{wrappers::LinesStream, StreamExt};

pub mod init;

pub type NodeIdentifier = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload> {
    pub src: NodeIdentifier,
    #[serde(rename = "dest")]
    pub dst: NodeIdentifier,
    pub body: MessageBody<Payload>,
}

pub type MessageIdentifier = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageBody<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<MessageIdentifier>,
    pub in_reply_to: Option<MessageIdentifier>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[async_trait::async_trait]
pub trait Node {
    type Payload;

    async fn run(
        self,
        rx: impl Stream<Item = Message<Self::Payload>> + Unpin + Send,
        tx: UnboundedSender<Message<Self::Payload>>,
    ) -> anyhow::Result<()>;

    async fn from_init(init: init::Init) -> anyhow::Result<Self>
    where
        Self: Sized;
}

fn stream_from_stdin<Payload>() -> impl Stream<Item = Message<Payload>>
where
    Payload: std::fmt::Debug + DeserializeOwned,
{
    LinesStream::new(BufReader::new(tokio::io::stdin()).lines()).map(|line| {
        let line = line.context("reading from STDIN").unwrap();
        let m = serde_json::from_str::<Message<Payload>>(&line)
            .context("message parsing")
            .unwrap();
        tracing::event!(tracing::Level::INFO, message = ?m, "Read message from STDIN");
        m
    })
}

fn serialize_to_stdout<Payload>(m: Message<Payload>)
where
    Payload: Serialize + std::fmt::Debug,
{
    println!(
        "{}",
        serde_json::to_string(&m).expect("serializing message failed")
    );
    tracing::event!(tracing::Level::INFO, message = ?m, "Wrote message to STDOUT");
}

pub async fn run_node<N, Payload>() -> anyhow::Result<()>
where
    N: Node<Payload = Payload>,
    Payload: Send + Serialize + std::fmt::Debug + DeserializeOwned + 'static,
{
    let node = init::init_node::<N, Payload>().await?;
    let (tx, mut rx) = mpsc::unbounded_channel::<Message<Payload>>();
    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            serialize_to_stdout(message)
        }
    });

    let messages = stream_from_stdin::<Payload>();
    node.run(messages, tx).await?;
    Ok(())
}
