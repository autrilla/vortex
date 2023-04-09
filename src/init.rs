use anyhow::bail;

use crate::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    pub node_id: NodeIdentifier,
    pub node_ids: Vec<NodeIdentifier>,
}

pub(crate) async fn init_node<N, Payload>() -> anyhow::Result<N>
where
    N: Node<Message<Payload>>,
    Payload: Send + Serialize + DeserializeOwned + 'static,
{
    let init_message = {
        stream_from_stdin::<InitPayload>()
            .next()
            .await
            .expect("missing init message")
    };
    let init = match init_message.body.payload {
        InitPayload::Init(i) => i,
        InitPayload::InitOk => bail!("Expected init message as first message"),
    };
    serialize_to_stdout(Message {
        src: init_message.dst,
        dst: init_message.src,
        body: MessageBody {
            id: Some(0),
            in_reply_to: init_message.body.id,
            payload: InitPayload::InitOk,
        },
    });
    N::from_init(init).await
}
