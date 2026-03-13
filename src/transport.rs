use std::collections::HashSet;
use std::pin::Pin;

use async_stream::try_stream;
use async_trait::async_trait;
use futures_core::Stream;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use url::Url;
use uuid::Uuid;

pub type OpenClawEventStream =
    Pin<Box<dyn Stream<Item = Result<OpenClawEvent, OpenClawTransportError>> + Send>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenClawStreamRequest {
    pub session_id: String,
    pub message: String,
    pub agent_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OpenClawEvent {
    TextDelta { text: String },
    Done { content: String },
    Error { message: String },
    Raw { payload: Value },
}

#[derive(Debug, thiserror::Error)]
pub enum OpenClawTransportError {
    #[error("openclaw url '{0}' is invalid")]
    InvalidUrl(String),
    #[error("openclaw websocket failed")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("openclaw url parse failed")]
    Url(#[from] url::ParseError),
    #[error("failed to serialize openclaw message")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid OpenClaw payload: {0}")]
    InvalidPayload(String),
}

#[async_trait]
pub trait OpenClawTransport: Send + Sync {
    async fn submit_stream(
        &self,
        request: OpenClawStreamRequest,
    ) -> Result<OpenClawEventStream, OpenClawTransportError>;
    async fn healthcheck(&self) -> Result<(), OpenClawTransportError>;
}

#[derive(Clone, Debug)]
pub struct OpenClawWsTransport {
    gateway_url: Url,
    auth_token: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RequestEnvelope<T> {
    r#type: &'static str,
    id: String,
    method: String,
    params: T,
}

impl<T> RequestEnvelope<T> {
    fn new(method: &str, params: T) -> Self {
        Self {
            r#type: "req",
            id: Uuid::new_v4().to_string(),
            method: method.to_string(),
            params,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConnectParams {
    min_protocol: u32,
    max_protocol: u32,
    role: &'static str,
    scopes: Vec<&'static str>,
    client: ClientInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth: Option<AuthInfo>,
}

#[derive(Debug, Clone, Serialize)]
struct ClientInfo {
    id: &'static str,
    version: &'static str,
    platform: &'static str,
    mode: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct AuthInfo {
    token: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentParams {
    message: String,
    idempotency_key: String,
    agent_id: String,
}

impl OpenClawWsTransport {
    pub fn new(gateway_url: impl AsRef<str>) -> Result<Self, OpenClawTransportError> {
        Ok(Self {
            gateway_url: normalize_openclaw_url(gateway_url.as_ref())?,
            auth_token: None,
        })
    }

    pub fn with_auth_token(mut self, auth_token: impl Into<String>) -> Self {
        self.auth_token = Some(auth_token.into());
        self
    }

    fn connect_request(&self) -> RequestEnvelope<ConnectParams> {
        RequestEnvelope::new(
            "connect",
            ConnectParams {
                min_protocol: 3,
                max_protocol: 3,
                role: "operator",
                scopes: vec!["operator.read", "operator.write"],
                client: ClientInfo {
                    id: "robrix-openclaw-channel",
                    version: env!("CARGO_PKG_VERSION"),
                    platform: "desktop",
                    mode: "robrix",
                },
                auth: self.auth_token.as_ref().map(|token| AuthInfo {
                    token: token.clone(),
                }),
            },
        )
    }

    fn agent_request(&self, request: &OpenClawStreamRequest) -> RequestEnvelope<AgentParams> {
        RequestEnvelope::new(
            "agent",
            AgentParams {
                message: request.message.clone(),
                idempotency_key: request.session_id.clone(),
                agent_id: request.agent_id.clone(),
            },
        )
    }
}

#[async_trait]
impl OpenClawTransport for OpenClawWsTransport {
    async fn submit_stream(
        &self,
        request: OpenClawStreamRequest,
    ) -> Result<OpenClawEventStream, OpenClawTransportError> {
        let gateway_url = self.gateway_url.clone();
        let connect_request = self.connect_request();
        let agent_request = self.agent_request(&request);

        let stream = try_stream! {
            let (socket, _) = connect_async(gateway_url.as_str()).await?;
            let (mut write, mut read) = socket.split();
            let mut connected = false;
            let mut seen_seqs = HashSet::new();
            let mut accumulated = String::new();

            write
                .send(WsMessage::Text(serde_json::to_string(&connect_request)?.into()))
                .await?;

            while let Some(message) = read.next().await {
                let message = message?;
                let text = match message {
                    WsMessage::Text(text) => text,
                    WsMessage::Close(_) => {
                        if !accumulated.is_empty() {
                            yield OpenClawEvent::Done {
                                content: accumulated.clone(),
                            };
                        }
                        break;
                    }
                    _ => continue,
                };

                let payload: Value = serde_json::from_str(&text)
                    .map_err(|_| OpenClawTransportError::InvalidPayload(text.to_string()))?;
                match payload.get("type").and_then(Value::as_str) {
                    Some("event") => {
                        if let Some(seq) = payload.get("seq").and_then(Value::as_u64)
                            && !seen_seqs.insert(seq)
                        {
                            continue;
                        }

                        match payload.get("event").and_then(Value::as_str) {
                            Some("connect.challenge") => {
                                write
                                    .send(WsMessage::Text(serde_json::to_string(&connect_request)?.into()))
                                    .await?;
                            }
                            Some("agent.text")
                            | Some("agent.content")
                            | Some("agent.text.delta")
                            | Some("agent.content.delta")
                            | Some("agent") => {
                                if let Some(text) = extract_openclaw_text_delta(&payload) {
                                    if let Some(delta) = merge_and_extract_delta(&mut accumulated, &text) {
                                        yield OpenClawEvent::TextDelta { text: delta };
                                    }
                                }
                            }
                            Some("agent.error") => {
                                let message = payload
                                    .get("payload")
                                    .and_then(|payload| {
                                        payload.get("errorMessage")
                                            .and_then(Value::as_str)
                                            .or_else(|| payload.get("message").and_then(Value::as_str))
                                            .or_else(|| payload.get("error").and_then(Value::as_str))
                                    })
                                    .unwrap_or("Unknown OpenClaw error")
                                    .to_string();
                                yield OpenClawEvent::Error { message };
                                break;
                            }
                            Some("chat") => {
                                let state = payload
                                    .get("payload")
                                    .and_then(|payload| payload.get("state"))
                                    .and_then(Value::as_str);
                                if matches!(state, Some("done") | Some("complete")) {
                                    yield OpenClawEvent::Done {
                                        content: accumulated.clone(),
                                    };
                                    break;
                                }
                            }
                            Some("agent.done") | Some("agent.complete") | Some("agent.end") => {}
                            _ => yield OpenClawEvent::Raw { payload },
                        }
                    }
                    Some("res") => {
                        let ok = payload.get("ok").and_then(Value::as_bool).unwrap_or(false);
                        if !ok {
                            let message = payload
                                .get("errorMessage")
                                .and_then(Value::as_str)
                                .or_else(|| payload.get("error").and_then(Value::as_str))
                                .unwrap_or("Unknown OpenClaw error")
                                .to_string();
                            yield OpenClawEvent::Error { message };
                            break;
                        }

                        let response_payload = payload.get("payload");
                        let is_hello_ok = response_payload
                            .and_then(|payload| payload.get("type"))
                            .and_then(Value::as_str)
                            == Some("hello-ok");
                        if is_hello_ok && !connected {
                            connected = true;
                            write
                                .send(WsMessage::Text(serde_json::to_string(&agent_request)?.into()))
                                .await?;
                            continue;
                        }

                        if let Some(summary) = response_payload
                            .and_then(|payload| payload.get("summary"))
                            .and_then(Value::as_str)
                            .or_else(|| {
                                response_payload
                                    .and_then(|payload| payload.get("result"))
                                    .and_then(|result| result.get("summary"))
                                    .and_then(Value::as_str)
                            })
                            .or_else(|| {
                                response_payload
                                    .and_then(|payload| payload.get("result"))
                                    .and_then(|result| result.get("text"))
                                    .and_then(Value::as_str)
                            })
                        {
                            if let Some(delta) = merge_and_extract_delta(&mut accumulated, summary) {
                                yield OpenClawEvent::TextDelta { text: delta };
                            }
                            yield OpenClawEvent::Done {
                                content: accumulated.clone(),
                            };
                            break;
                        }

                        let status = response_payload
                            .and_then(|payload| payload.get("status"))
                            .and_then(Value::as_str);
                        if matches!(status, Some("ok") | Some("completed")) && !accumulated.is_empty() {
                            yield OpenClawEvent::Done {
                                content: accumulated.clone(),
                            };
                            break;
                        }
                    }
                    _ => yield OpenClawEvent::Raw { payload },
                }
            }
        };

        Ok(Box::pin(stream))
    }

    async fn healthcheck(&self) -> Result<(), OpenClawTransportError> {
        let (mut socket, _) = connect_async(self.gateway_url.as_str()).await?;
        let _ = socket.close(None).await;
        Ok(())
    }
}

fn normalize_openclaw_url(raw: &str) -> Result<Url, OpenClawTransportError> {
    let url = Url::parse(raw)?;
    let normalized = match url.scheme() {
        "ws" | "wss" => url,
        "http" => {
            let replaced = raw.replacen("http://", "ws://", 1);
            Url::parse(&replaced)?
        }
        "https" => {
            let replaced = raw.replacen("https://", "wss://", 1);
            Url::parse(&replaced)?
        }
        _ => return Err(OpenClawTransportError::InvalidUrl(raw.to_string())),
    };
    Ok(normalized)
}

fn extract_openclaw_text_delta(payload: &Value) -> Option<String> {
    payload
        .get("payload")
        .and_then(|payload| {
            payload
                .get("delta")
                .and_then(Value::as_str)
                .or_else(|| payload.get("text").and_then(Value::as_str))
                .or_else(|| {
                    payload
                        .get("data")
                        .and_then(|data| data.get("delta"))
                        .and_then(Value::as_str)
                })
                .or_else(|| {
                    payload
                        .get("data")
                        .and_then(|data| data.get("text"))
                        .and_then(Value::as_str)
                })
        })
        .map(ToOwned::to_owned)
}

fn merge_and_extract_delta(accumulated: &mut String, incoming: &str) -> Option<String> {
    let before = accumulated.clone();
    merge_text_content(accumulated, incoming);

    if accumulated == &before {
        return None;
    }
    if accumulated.starts_with(&before) {
        return Some(accumulated[before.len()..].to_string());
    }

    Some(incoming.to_string())
}

fn merge_text_content(content: &mut String, incoming: &str) {
    if content.is_empty() {
        content.push_str(incoming);
        return;
    }

    if incoming.starts_with(content.as_str()) {
        *content = incoming.to_string();
        return;
    }

    if content.starts_with(incoming) {
        return;
    }

    let overlap = find_overlap_bytes(content, incoming);
    if overlap == 0 {
        content.push_str(incoming);
        return;
    }
    content.push_str(&incoming[overlap..]);
}

fn find_overlap_bytes(content: &str, incoming: &str) -> usize {
    let content_bounds = char_bounds(content);
    let incoming_bounds = char_bounds(incoming);
    let max_overlap = content_bounds
        .len()
        .saturating_sub(1)
        .min(incoming_bounds.len().saturating_sub(1));

    for overlap in (1..=max_overlap).rev() {
        let content_start = content_bounds[content_bounds.len() - 1 - overlap];
        let incoming_end = incoming_bounds[overlap];
        if content[content_start..] == incoming[..incoming_end] {
            return incoming_end;
        }
    }

    0
}

fn char_bounds(value: &str) -> Vec<usize> {
    let mut bounds: Vec<usize> = value.char_indices().map(|(idx, _)| idx).collect();
    bounds.push(value.len());
    bounds
}

#[cfg(test)]
mod tests {
    use super::{OpenClawWsTransport, merge_and_extract_delta};

    #[test]
    fn normalizes_http_urls_to_ws() {
        let transport = OpenClawWsTransport::new("https://127.0.0.1:24282/ws").unwrap();
        assert_eq!(transport.gateway_url.as_str(), "wss://127.0.0.1:24282/ws");
    }

    #[test]
    fn merge_preserves_non_duplicate_suffix() {
        let mut accumulated = "hello".to_string();
        let delta = merge_and_extract_delta(&mut accumulated, "hello world").unwrap();
        assert_eq!(delta, " world");
        assert_eq!(accumulated, "hello world");
    }
}
