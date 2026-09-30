//! OpenAI chat completions wire format (both directions).

use crate::error::TransformError;
use crate::sse::SseEvent;
use crate::transformer::{Credentials, InboundTransformer, OutboundRequest, OutboundTransformer};
use crate::{ContentPart, Delta, ErrorResponse, Message, Request, Response, Role, StreamChoice, StreamChunk, Usage};
use serde_json::{json, Value};

pub const FORMAT: &str = "openai/chat_completions";

// ---------- Inbound (client speaks OpenAI) ----------

#[derive(Debug, Default)]
pub struct OpenAiInbound;

impl OpenAiInbound {
    pub fn new() -> Self {
        Self
    }
}

impl InboundTransformer for OpenAiInbound {
    fn format(&self) -> &'static str {
        FORMAT
    }

    fn transform_request(&self, body: &[u8]) -> Result<Request, TransformError> {
        // Unified Request is an OpenAI superset; string/array content handled by
        // MessageContent's untagged enum, extras via `extra` flatten.
        serde_json::from_slice(body).map_err(TransformError::Json)
    }

    fn transform_response(&self, resp: &Response) -> Result<Vec<u8>, TransformError> {
        serde_json::to_vec(resp).map_err(TransformError::Json)
    }

    fn transform_stream_chunk(&self, chunk: &StreamChunk) -> Result<Vec<SseEvent>, TransformError> {
        let data = serde_json::to_string(chunk).map_err(TransformError::Json)?;
        Ok(vec![SseEvent::data(data)])
    }

    fn stream_end(&self) -> Vec<SseEvent> {
        vec![SseEvent::data("[DONE]")]
    }

    fn transform_error(&self, err: &ErrorResponse) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "error": {
                "message": err.message,
                "type": err.kind,
                "code": err.code,
            }
        }))
        .unwrap_or_default()
    }
}

// ---------- Outbound (upstream is OpenAI-compatible) ----------

#[derive(Debug, Default)]
pub struct OpenAiOutbound;

impl OpenAiOutbound {
    pub fn new() -> Self {
        Self
    }
}

fn parse_usage(v: &Value) -> Usage {
    let mut u = Usage {
        prompt_tokens: v.get("prompt_tokens").and_then(Value::as_u64).unwrap_or(0),
        completion_tokens: v.get("completion_tokens").and_then(Value::as_u64).unwrap_or(0),
        total_tokens: v.get("total_tokens").and_then(Value::as_u64).unwrap_or(0),
        ..Default::default()
    };
    if let Some(c) = v.pointer("/prompt_tokens_details/cached_tokens").and_then(Value::as_u64) {
        u.cached_tokens = Some(c);
    }
    if let Some(r) = v
        .pointer("/completion_tokens_details/reasoning_tokens")
        .and_then(Value::as_u64)
    {
        u.reasoning_tokens = Some(r);
    }
    u
}

impl OutboundTransformer for OpenAiOutbound {
    fn format(&self) -> &'static str {
        FORMAT
    }

    fn build_request(&self, req: &Request, creds: &Credentials) -> Result<OutboundRequest, TransformError> {
        let mut body = serde_json::to_value(req).map_err(TransformError::Json)?;
        if req.stream {
            // Ensure usage arrives on the final stream chunk.
            if let Some(obj) = body.as_object_mut() {
                obj.insert(
                    "stream_options".into(),
                    json!({"include_usage": true}),
                );
            }
        }
        Ok(OutboundRequest {
            path: "/chat/completions".into(),
            headers: vec![
                ("Authorization".into(), format!("Bearer {}", creds.api_key)),
                ("content-type".into(), "application/json".into()),
            ],
            body: serde_json::to_vec(&body).map_err(TransformError::Json)?,
        })
    }

    fn transform_response(&self, body: &[u8]) -> Result<Response, TransformError> {
        let v: Value = serde_json::from_slice(body).map_err(TransformError::Json)?;
        let mut resp = Response {
            id: v.get("id").and_then(Value::as_str).unwrap_or_default().to_string(),
            model: v.get("model").and_then(Value::as_str).unwrap_or_default().to_string(),
            choices: Vec::new(),
            usage: v.get("usage").map(parse_usage),
            extra: Default::default(),
        };
        for c in v.get("choices").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]) {
            let message: Message = serde_json::from_value(
                c.get("message").cloned().unwrap_or_else(|| json!({"role": "assistant"})),
            )
            .map_err(TransformError::Json)?;
            resp.choices.push(crate::Choice {
                index: c.get("index").and_then(Value::as_u64).unwrap_or(0) as u32,
                message,
                finish_reason: c
                    .get("finish_reason")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            });
        }
        if let Some(obj) = v.as_object() {
            for (k, val) in obj {
                if !matches!(k.as_str(), "id" | "model" | "choices" | "usage") {
                    resp.extra.insert(k.clone(), val.clone());
                }
            }
        }
        Ok(resp)
    }

    fn transform_stream_event(&self, event: &SseEvent) -> Result<Vec<StreamChunk>, TransformError> {
        if event.is_done() {
            return Ok(vec![]);
        }
        let v: Value = serde_json::from_str(event.data.trim()).map_err(TransformError::Json)?;
        let mut chunk = StreamChunk {
            id: v.get("id").and_then(Value::as_str).unwrap_or_default().to_string(),
            model: v.get("model").and_then(Value::as_str).unwrap_or_default().to_string(),
            choices: Vec::new(),
            usage: v.get("usage").filter(|u| !u.is_null()).map(parse_usage),
            extra: Default::default(),
        };
        for c in v.get("choices").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]) {
            let delta: Delta = serde_json::from_value(
                c.get("delta").cloned().unwrap_or_else(|| json!({})),
            )
            .map_err(TransformError::Json)?;
            chunk.choices.push(StreamChoice {
                index: c.get("index").and_then(Value::as_u64).unwrap_or(0) as u32,
                delta,
                finish_reason: c
                    .get("finish_reason")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            });
        }
        if let Some(obj) = v.as_object() {
            for (k, val) in obj {
                if !matches!(k.as_str(), "id" | "model" | "choices" | "usage") {
                    chunk.extra.insert(k.clone(), val.clone());
                }
            }
        }
        Ok(vec![chunk])
    }

    fn is_stream_end(&self, event: &SseEvent) -> bool {
        event.is_done()
    }

    fn extract_error(&self, status: u16, body: &[u8]) -> ErrorResponse {
        let mut err = ErrorResponse {
            message: String::new(),
            kind: None,
            code: None,
            status: Some(status),
        };
        if let Ok(v) = serde_json::from_slice::<Value>(body) {
            let e = v.get("error").cloned().unwrap_or(v);
            err.message = e.get("message").and_then(Value::as_str).unwrap_or_default().to_string();
            err.kind = e.get("type").and_then(Value::as_str).map(str::to_string);
            err.code = e
                .get("code")
                .map(|c| if c.is_string() { c.as_str().unwrap_or_default().to_string() } else { c.to_string() });
        }
        if err.message.is_empty() {
            err.message = format!("upstream error {status}");
        }
        err
    }
}

// ---------- shared helpers for tests/other modules ----------

/// Build an OpenAI image_url content part.
pub fn image_part(url: &str) -> ContentPart {
    ContentPart::ImageUrl {
        image_url: crate::ImageUrl { url: url.to_string(), detail: None },
    }
}

/// Empty assistant delta convenience (used by relay-side synth, tests).
pub fn assistant_delta() -> Delta {
    Delta { role: Some(Role::Assistant), content: None, tool_calls: None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transformers::{create_inbound, create_outbound};

    const REQ: &str = r#"{
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "hi"},
            {"role": "user", "content": [
                {"type": "text", "text": "what is this"},
                {"type": "image_url", "image_url": {"url": "https://x/y.png"}}
            ]}
        ],
        "stream": true,
        "max_tokens": 100,
        "temperature": 0.5,
        "reasoning_effort": "high"
    }"#;

    #[test]
    fn inbound_parses_string_and_array_content() {
        let t = OpenAiInbound::new();
        let req = t.transform_request(REQ.as_bytes()).unwrap();
        assert_eq!(req.model, "gpt-4o");
        assert_eq!(req.messages.len(), 3);
        assert_eq!(req.messages[0].content.as_ref().unwrap().text(), "be brief");
        match req.messages[2].content.as_ref().unwrap() {
            crate::MessageContent::Parts(parts) => {
                assert_eq!(parts[0].text_ref().unwrap(), "what is this");
            }
            other => panic!("expected parts, got {other:?}"),
        }
        assert_eq!(req.extra.get("reasoning_effort").unwrap(), "high");
    }

    trait TextRef {
        fn text_ref(&self) -> Option<String>;
    }
    impl TextRef for ContentPart {
        fn text_ref(&self) -> Option<String> {
            match self {
                ContentPart::Text { text } => Some(text.clone()),
                _ => None,
            }
        }
    }

    #[test]
    fn outbound_injects_stream_options() {
        let t = OpenAiOutbound::new();
        let req = OpenAiInbound::new().transform_request(REQ.as_bytes()).unwrap();
        let out = t.build_request(&req, &Credentials { api_key: "sk-x".into() }).unwrap();
        assert_eq!(out.path, "/chat/completions");
        assert!(out.headers.iter().any(|(k, v)| k == "Authorization" && v == "Bearer sk-x"));
        let body: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    const RESP: &str = r#"{
        "id": "chatcmpl-1", "model": "gpt-4o",
        "choices": [{"index":0,"message":{"role":"assistant","content":"hello"},
                     "finish_reason":"stop"}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
                  "prompt_tokens_details": {"cached_tokens": 4},
                  "completion_tokens_details": {"reasoning_tokens": 2}}
    }"#;

    #[test]
    fn outbound_maps_usage_details() {
        let t = OpenAiOutbound::new();
        let r = t.transform_response(RESP.as_bytes()).unwrap();
        let u = r.usage.unwrap();
        assert_eq!(u.prompt_tokens, 10);
        assert_eq!(u.cached_tokens, Some(4));
        assert_eq!(u.reasoning_tokens, Some(2));
        assert_eq!(r.choices[0].message.content.as_ref().unwrap().text(), "hello");
    }

    /// Regression: CC stream tool_call deltas after the first carry neither
    /// id nor type (only index + arguments fragment) — must not fail parsing.
    #[test]
    fn stream_tool_call_continuation_deltas_parse() {
        let t = OpenAiOutbound::new();
        let first = SseEvent::data(r#"{"id":"c1","model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}"#);
        let chunks = t.transform_stream_event(&first).unwrap();
        let tc = &chunks[0].choices[0].delta.tool_calls.as_ref().unwrap()[0];
        assert_eq!(tc.id, "call_a");
        assert_eq!(tc.index, Some(0));
        assert_eq!(tc.function.name, "get_weather");

        let cont = SseEvent::data(r#"{"id":"c1","model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"ci"}}]},"finish_reason":null}]}"#);
        let chunks = t.transform_stream_event(&cont).unwrap();
        let tc = &chunks[0].choices[0].delta.tool_calls.as_ref().unwrap()[0];
        assert_eq!(tc.id, "");
        assert_eq!(tc.index, Some(0));
        assert_eq!(tc.function.name, "");
        assert_eq!(tc.function.arguments, "{\"ci");

        // A second parallel call distinguished by index.
        let second = SseEvent::data(r#"{"id":"c1","model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"call_b","type":"function","function":{"name":"f2","arguments":""}}]},"finish_reason":null}]}"#);
        let chunks = t.transform_stream_event(&second).unwrap();
        let tc = &chunks[0].choices[0].delta.tool_calls.as_ref().unwrap()[0];
        assert_eq!(tc.id, "call_b");
        assert_eq!(tc.index, Some(1));
    }

    #[test]
    fn stream_done_sentinel_and_chunk() {
        let t = OpenAiOutbound::new();
        let done = SseEvent::data("[DONE]");
        assert!(t.is_stream_end(&done));
        assert!(t.transform_stream_event(&done).unwrap().is_empty());

        let chunk = SseEvent::data(r#"{"id":"c1","model":"m","choices":[{"index":0,"delta":{"content":"yo"},"finish_reason":null}],"usage":null}"#);
        assert!(!t.is_stream_end(&chunk));
        let chunks = t.transform_stream_event(&chunk).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].choices[0].delta.content.as_deref(), Some("yo"));
    }

    #[test]
    fn stream_final_chunk_with_usage() {
        let t = OpenAiOutbound::new();
        let ev = SseEvent::data(r#"{"id":"c1","model":"m","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10,"prompt_tokens_details":{"cached_tokens":1}}}"#);
        let chunks = t.transform_stream_event(&ev).unwrap();
        let u = chunks[0].usage.as_ref().unwrap();
        assert_eq!(u.total_tokens, 10);
        assert_eq!(u.cached_tokens, Some(1));
    }

    #[test]
    fn extract_error_shape() {
        let t = OpenAiOutbound::new();
        let e = t.extract_error(429, br#"{"error":{"message":"rate limited","type":"rate_limit_error","code":"429"}}"#);
        assert_eq!(e.message, "rate limited");
        assert_eq!(e.kind.as_deref(), Some("rate_limit_error"));
        assert_eq!(e.status, Some(429));
    }

    #[test]
    fn roundtrip_to_anthropic_body() {
        // openai body -> unified -> anthropic body must be valid & parseable
        let inbound = OpenAiInbound::new();
        let unified = inbound.transform_request(REQ.as_bytes()).unwrap();
        let ant = crate::transformers::anthropic::AnthropicOutbound::new();
        let out = ant
            .build_request(&unified, &Credentials { api_key: "k".into() })
            .unwrap();
        assert_eq!(out.path, "/v1/messages");
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["system"][0]["text"], "be brief");
        assert_eq!(v["max_tokens"], 100);
        assert_eq!(v["messages"][0]["role"], "user");
    }

    #[test]
    fn inbound_stream_chunk_serializes() {
        let t = OpenAiInbound::new();
        let chunk = StreamChunk {
            id: "c1".into(),
            model: "m".into(),
            choices: vec![StreamChoice {
                index: 0,
                delta: Delta { role: None, content: Some("a".into()), tool_calls: None },
                finish_reason: None,
            }],
            usage: None,
            extra: Default::default(),
        };
        let evs = t.transform_stream_chunk(&chunk).unwrap();
        assert_eq!(evs.len(), 1);
        let v: Value = serde_json::from_str(&evs[0].data).unwrap();
        assert_eq!(v["choices"][0]["delta"]["content"], "a");
        let end = t.stream_end();
        assert_eq!(end[0].data, "[DONE]");
    }

    #[test]
    fn factories_resolve() {
        assert!(create_inbound(FORMAT).is_some());
        assert!(create_outbound(FORMAT).is_some());
        assert!(create_inbound("nope").is_none());
    }
}
