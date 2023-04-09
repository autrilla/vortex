use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::{Stream, StreamExt};
use vortex::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}
struct EchoNode {
    id: NodeIdentifier,
    message_id: MessageIdentifier,
}

#[async_trait::async_trait]
impl Node<Message<EchoPayload>> for EchoNode {
    async fn run(
        mut self,
        mut rx: impl Stream<Item = Message<EchoPayload>> + Unpin + Send,
        tx: UnboundedSender<Message<EchoPayload>>,
    ) -> anyhow::Result<()> {
        while let Some(message) = rx.next().await {
            if let EchoPayload::Echo { echo } = message.body.payload {
                tx.send(Message {
                    src: self.id.clone(),
                    dst: message.src,
                    body: MessageBody {
                        id: Some(self.message_id),
                        in_reply_to: message.body.id,
                        payload: EchoPayload::EchoOk { echo },
                    },
                })?;
                self.message_id += 1;
            }
        }
        Ok(())
    }

    async fn from_init(init: init::Init) -> anyhow::Result<Self> {
        Ok(Self {
            message_id: 0,
            id: init.node_id,
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run_node::<EchoNode, _>().await
}
