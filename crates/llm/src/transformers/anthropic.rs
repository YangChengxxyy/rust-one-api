//! Anthropic Messages API wire format (`/v1/messages`).

use std::sync::Mutex;

use crate::error::TransformError;
use crate::sse::SseEvent;
use crate::transformer::{Credentials, InboundTransformer, OutboundRequest, OutboundTransformer};
use crate::{Choice, ContentPart, FunctionDef, Delta, ErrorResponse, FunctionCall, ImageUrl, Message, MessageContent, Request, Response, Role, StreamChoice, StreamChunk, Tool, ToolCall, Usage};
use serde_json::{json, Map, Value};

pub const FORMAT: &str = "claude/messages";
pub const ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_MAX_TOKENS: u32 = 4096;

// ---------------- helpers ----------------

fn stop_reason_to_unified(reason: Option<&str>) -> Option<String> {
    reason.map(|r| match r {
        "end_turn" | "stop_sequence" => "stop".to_string(),
        "tool_use" => "tool_calls".to_string(),
        "max_tokens" => "length".to_string(),
        other => other.to_string(),
    })
}

fn unified_to_stop_reason(reason: Option<&str>) -> String {
    match reason {
        Some("tool_calls") => "tool_use".to_string(),
        Some("length") => "max_tokens".to_string(),
        Some("stop_sequence") => "stop_sequence".to_string(),
        _ => "end_turn".to_string(),
    }
}

/// data:{media_type};base64,{data} -> (media_type, data)
fn split_data_url(url: &str) -> Option<(String, String)> {
    let rest = url.strip_prefix("data:")?;
    let (mt, data) = rest.split_once(";base64,")?;
    Some((mt.to_string(), data.to_string()))
}

fn usage_to_anthropic(u: &Usage) -> Value {
    let mut m = Map::new();
    m.insert("input_tokens".into(), json!(u.prompt_tokens));
    m.insert("output_tokens".into(), json!(u.completion_tokens));
    if let Some(c) = u.cached_tokens {
        m.insert("cache_read_input_tokens".into(), json!(c));
    }
    if let Some(c) = u.cache_write_tokens {
        m.insert("cache_creation_input_tokens".into(), json!(c));
    }
    Value::Object(m)
}

fn usage_from_anthropic(v: &Value) -> Usage {
    let g = |k: &str| v.get(k).and_then(Value::as_u64);
    let prompt = g("input_tokens").unwrap_or(0);
    let completion = g("output_tokens").unwrap_or(0);
    Usage {
        prompt_tokens: prompt,
        completion_tokens: completion,
        total_tokens: prompt + completion,
        cached_tokens: g("cache_read_input_tokens"),
        cache_write_tokens: g("cache_creation_input_tokens"),
        reasoning_tokens: None,
        extra: Default::default(),
    }
}

fn tool_choice_from_anthropic(v: &Value) -> Value {
    match v.get("type").and_then(Value::as_str) {
        Some("tool") => json!({
            "type": "function",
            "function": {"name": v.get("name").and_then(Value::as_str).unwrap_or_default()}
        }),
        _ => v.clone(),
    }
}

fn tool_choice_to_anthropic(v: &Value) -> Value {
    match v.get("type").and_then(Value::as_str) {
        Some("function") => {
            let name = v.pointer("/function/name").and_then(Value::as_str).unwrap_or_default();
            json!({"type": "tool", "name": name})
        }
        _ => v.clone(),
    }
}

/// unified Message -> Anthropic content blocks (best-effort).
fn message_to_blocks(msg: &Message) -> Vec<Value> {
    let mut blocks = Vec::new();
    match msg.content.as_ref() {
        Some(MessageContent::Text(t)) if !t.is_empty() => blocks.push(json!({"type": "text", "text": t})),
        Some(MessageContent::Parts(parts)) => {
            for p in parts {
                match p {
                    ContentPart::Text { text } => blocks.push(json!({"type": "text", "text": text})),
                    ContentPart::ImageUrl { image_url } => {
                        if let Some((mt, data)) = split_data_url(&image_url.url) {
                            blocks.push(json!({
                                "type": "image",
                                "source": {"type": "base64", "media_type": mt, "data": data}
                            }));
                        } else {
                            blocks.push(json!({
                                "type": "image",
                                "source": {"type": "url", "url": image_url.url}
                            }));
                        }
                    }
                    ContentPart::Unknown => {}
                }
            }
        }
        _ => {}
    }
    for tc in msg.tool_calls.as_deref().unwrap_or(&[]) {
        let input: Value = serde_json::from_str(&tc.function.arguments).unwrap_or(json!({}));
        blocks.push(json!({
            "type": "tool_use", "id": tc.id, "name": tc.function.name, "input": input
        }));
    }
    blocks
}

/// unified Request -> Anthropic /v1/messages JSON body.
fn unified_to_anthropic_body(req: &Request) -> Value {
    let mut system_blocks: Vec<Value> = Vec::new();
    let mut messages: Vec<Value> = Vec::new();
    for m in &req.messages {
        match m.role {
            Role::System | Role::Developer => {
                if let Some(c) = m.content.as_ref() {
                    let t = c.text();
                    if !t.is_empty() {
                        system_blocks.push(json!({"type": "text", "text": t}));
                    }
                }
            }
            Role::Tool => messages.push(json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": m.tool_call_id.clone().unwrap_or_default(),
                    "content": m.content.as_ref().map(|c| Value::String(c.text())).unwrap_or(Value::Null),
                }],
            })),
            role => {
                let r = if role == Role::Assistant { "assistant" } else { "user" };
                let blocks = message_to_blocks(m);
                if blocks.is_empty() {
                    continue;
                }
                messages.push(json!({"role": r, "content": blocks}));
            }
        }
    }
    let mut body = Map::new();
    if !system_blocks.is_empty() {
        body.insert("system".into(), Value::Array(system_blocks));
    }
    body.insert("messages".into(), Value::Array(messages));
    body.insert(
        "max_tokens".into(),
        json!(req.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS)),
    );
    if req.stream {
        body.insert("stream".into(), json!(true));
    }
    if let Some(t) = req.temperature {
        body.insert("temperature".into(), json!(t));
    }
    if let Some(t) = req.top_p {
        body.insert("top_p".into(), json!(t));
    }
    if let Some(s) = req.stop.as_ref() {
        body.insert("stop_sequences".into(), json!(s));
    }
    if let Some(u) = req.user.as_ref() {
        body.insert("metadata".into(), json!({"user_id": u}));
    }
    if let Some(tc) = req.tool_choice.as_ref() {
        body.insert("tool_choice".into(), tool_choice_to_anthropic(tc));
    }
    if let Some(tools) = req.tools.as_ref() {
        let decls: Vec<Value> = tools
            .iter()
            .map(|t| {
                let mut d = Map::new();
                d.insert("name".into(), json!(t.function.name));
                if let Some(desc) = t.function.description.as_deref() {
                    d.insert("description".into(), json!(desc));
                }
                if let Some(p) = &t.function.parameters {
                    d.insert("input_schema".into(), p.clone());
                }
                Value::Object(d)
            })
            .collect();
        body.insert("tools".into(), json!(decls));
    }
    Value::Object(body)
}

/// Anthropic /v1/messages body -> unified Request.
fn anthropic_to_unified(body: &[u8]) -> Result<Request, TransformError> {
    let v: Value = serde_json::from_slice(body).map_err(TransformError::Json)?;
    let mut messages: Vec<Message> = Vec::new();

    match v.get("system") {
        Some(Value::String(s)) => messages.push(Message {
            role: Role::System,
            content: Some(MessageContent::Text(s.clone())),
            name: None,
            tool_calls: None,
            tool_call_id: None,
        }),
        Some(Value::Array(blocks)) => {
            let text: Vec<String> = blocks
                .iter()
                .filter_map(|b| b.get("text").and_then(Value::as_str).map(str::to_string))
                .collect();
            if !text.is_empty() {
                messages.push(Message {
                    role: Role::System,
                    content: Some(MessageContent::Text(text.join("\n"))),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                });
            }
        }
        _ => {}
    }

    for m in v.get("messages").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]) {
        let role = match m.get("role").and_then(Value::as_str) {
            Some("assistant") => Role::Assistant,
            _ => Role::User,
        };
        let mut msg = Message { role, content: None, name: None, tool_calls: None, tool_call_id: None };
        let mut parts: Vec<ContentPart> = Vec::new();
        let mut texts: Vec<String> = Vec::new();
        let blocks = m.get("content").cloned().unwrap_or(Value::Null);
        let blocks: Vec<Value> = match blocks {
            Value::String(s) => vec![json!({"type": "text", "text": s})],
            Value::Array(a) => a,
            _ => vec![],
        };
        for b in blocks {
            match b.get("type").and_then(Value::as_str) {
                Some("text") => {
                    texts.push(b.get("text").and_then(Value::as_str).unwrap_or_default().to_string());
                    parts.push(ContentPart::Text {
                        text: b.get("text").and_then(Value::as_str).unwrap_or_default().to_string(),
                    });
                }
                Some("image") => {
                    if let Some(src) = b.get("source") {
                        if src.get("type").and_then(Value::as_str) == Some("base64") {
                            let url = format!(
                                "data:{};base64,{}",
                                src.get("media_type").and_then(Value::as_str).unwrap_or("image/png"),
                                src.get("data").and_then(Value::as_str).unwrap_or_default()
                            );
                            parts.push(ContentPart::ImageUrl { image_url: ImageUrl { url, detail: None } });
                        }
                    }
                }
                Some("tool_use") => {
                    let input = b.get("input").cloned().unwrap_or(json!({}));
                    msg.tool_calls.get_or_insert_with(Vec::new).push(ToolCall {
                        id: b.get("id").and_then(Value::as_str).unwrap_or_default().to_string(),
                        kind: "function".into(),
                        function: FunctionCall {
                            name: b.get("name").and_then(Value::as_str).unwrap_or_default().to_string(),
                            arguments: serde_json::to_string(&input).unwrap_or_else(|_| "{}".into()),
                        },
                    });
                }
                Some("tool_result") => {
                    // tool_result becomes its own role:tool message
                    let text = match b.get("content") {
                        Some(Value::String(s)) => s.clone(),
                        Some(Value::Array(a)) => a
                            .iter()
                            .filter_map(|x| x.get("text").and_then(Value::as_str))
                            .collect::<Vec<_>>()
                            .join(""),
                        _ => String::new(),
                    };
                    messages.push(Message {
                        role: Role::Tool,
                        content: Some(MessageContent::Text(text)),
                        name: None,
                        tool_calls: None,
                        tool_call_id: b.get("tool_use_id").and_then(Value::as_str).map(str::to_string),
                    });
                }
                _ => {}
            }
        }
        // content: single text -> Text; mixed/images -> Parts
        let only_text = parts.iter().all(|p| matches!(p, ContentPart::Text { .. }));
        if !parts.is_empty() {
            if only_text {
                msg.content = Some(MessageContent::Text(texts.join("")));
            } else {
                msg.content = Some(MessageContent::Parts(parts));
            }
        }
        if msg.content.is_some() || msg.tool_calls.is_some() {
            messages.push(msg);
        }
    }

    let mut req = Request {
        model: v.get("model").and_then(Value::as_str).unwrap_or_default().to_string(),
        messages,
        stream: v.get("stream").and_then(Value::as_bool).unwrap_or(false),
        max_tokens: v.get("max_tokens").and_then(Value::as_u64).map(|x| x as u32),
        temperature: v.get("temperature").and_then(Value::as_f64).map(|x| x as f32),
        top_p: v.get("top_p").and_then(Value::as_f64).map(|x| x as f32),
        stop: v.get("stop_sequences").and_then(Value::as_array).map(|a| {
            a.iter().filter_map(Value::as_str).map(str::to_string).collect()
        }),
        tools: v.get("tools").and_then(Value::as_array).map(|tools| {
            tools
                .iter()
                .filter_map(|t| {
                    Some(Tool {
                        kind: "function".into(),
                        function: FunctionDef {
                            name: t.get("name")?.as_str()?.to_string(),
                            description: t.get("description").and_then(Value::as_str).map(str::to_string),
                            parameters: t.get("input_schema").cloned(),
                        },
                    })
                })
                .collect()
        }),
        tool_choice: v.get("tool_choice").map(tool_choice_from_anthropic),
        response_format: None,
        user: v.pointer("/metadata/user_id").and_then(Value::as_str).map(str::to_string),
        extra: Default::default(),
    };
    if req.tool_choice.as_ref().map(|t| t.is_null()).unwrap_or(false) {
        req.tool_choice = None;
    }
    Ok(req)
}

// ---------------- Inbound (client speaks Anthropic) ----------------

#[derive(Default)]
pub struct AnthropicInbound {
    stream: Mutex<InboundStreamState>,
}

#[derive(Default)]
struct InboundStreamState {
    started: bool,
    /// Currently open content block.
    open: Option<OpenBlock>,
    next_index: u32,
    /// finish reason already emitted via message_delta
    finished: bool,
}

enum OpenBlock {
    Text { index: u32 },
    Tool { index: u32 },
}

impl AnthropicInbound {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AnthropicInbound {
    /// Close the open block if any; returns content_block_stop event.
    fn close_block(state: &mut InboundStreamState) -> Vec<SseEvent> {
        match state.open.take() {
            Some(block) => {
                if let OpenBlock::Tool { .. } = block {
                    state.next_index += 1;
                }
                vec![SseEvent::named("content_block_stop", format!(r#"{{"index":{}}}"#, index_of(&block)))]
            }
            None => vec![],
        }
    }
}

fn index_of(b: &OpenBlock) -> u32 {
    match b {
        OpenBlock::Text { index } | OpenBlock::Tool { index } => *index,
    }
}

impl InboundTransformer for AnthropicInbound {
    fn format(&self) -> &'static str {
        FORMAT
    }

    fn transform_request(&self, body: &[u8]) -> Result<Request, TransformError> {
        anthropic_to_unified(body)
    }

    fn transform_response(&self, resp: &Response) -> Result<Vec<u8>, TransformError> {
        // unified Response -> Anthropic response body
        let mut blocks: Vec<Value> = Vec::new();
        if let Some(choice) = resp.choices.first() {
            let msg = &choice.message;
            match msg.content.as_ref() {
                Some(MessageContent::Text(t)) if !t.is_empty() => {
                    blocks.push(json!({"type": "text", "text": t}))
                }
                Some(MessageContent::Parts(parts)) => {
                    for p in parts {
                        if let ContentPart::Text { text } = p {
                            blocks.push(json!({"type": "text", "text": text}));
                        }
                    }
                }
                _ => {}
            }
            for tc in msg.tool_calls.as_deref().unwrap_or(&[]) {
                let input: Value = serde_json::from_str(&tc.function.arguments).unwrap_or(json!({}));
                blocks.push(json!({
                    "type": "tool_use", "id": tc.id, "name": tc.function.name, "input": input
                }));
            }
        }
        let stop_reason = resp
            .choices
            .first()
            .and_then(|c| c.finish_reason.as_deref())
            .map(|f| unified_to_stop_reason(Some(f)));
        let mut body = Map::new();
        body.insert("id".into(), json!(resp.id));
        body.insert("type".into(), json!("message"));
        body.insert("role".into(), json!("assistant"));
        body.insert("model".into(), json!(resp.model));
        body.insert("content".into(), Value::Array(blocks));
        if let Some(sr) = stop_reason {
            body.insert("stop_reason".into(), json!(sr));
        }
        if let Some(u) = resp.usage.as_ref() {
            body.insert("usage".into(), usage_to_anthropic(u));
        }
        serde_json::to_vec(&Value::Object(body)).map_err(TransformError::Json)
    }

    fn transform_stream_chunk(&self, chunk: &StreamChunk) -> Result<Vec<SseEvent>, TransformError> {
        let mut state = self.stream.lock().unwrap_or_else(|e| e.into_inner());
        let mut events = Vec::new();
        let choice = chunk.choices.first();

        if !state.started {
            state.started = true;
            events.push(SseEvent::named(
                "message_start",
                json!({
                    "type": "message_start",
                    "message": {
                        "id": chunk.id, "type": "message", "role": "assistant",
                        "model": chunk.model, "content": [],
                        "usage": {"input_tokens": 0, "output_tokens": 0}
                    }
                })
                .to_string(),
            ));
        }

        // usage on final chunk -> message_delta usage (Anthropic reports usage there)
        let finish_usage = chunk.usage.as_ref().map(usage_to_anthropic);

        if let Some(choice) = choice {
            let delta = &choice.delta;
            if let Some(text) = delta.content.as_deref() {
                if !text.is_empty() {
                    match state.open.as_ref() {
                        Some(OpenBlock::Text { .. }) => {}
                        _ => {
                            events.extend(Self::close_block(&mut state));
                            let index = state.next_index;
                            state.next_index += 1;
                            state.open = Some(OpenBlock::Text { index });
                            events.push(SseEvent::named(
                                "content_block_start",
                                json!({"type":"content_block_start","index":index,
                                       "content_block":{"type":"text","text":""}})
                                .to_string(),
                            ));
                        }
                    }
                    let index = index_of(state.open.as_ref().unwrap());
                    events.push(SseEvent::named(
                        "content_block_delta",
                        json!({"type":"content_block_delta","index":index,
                               "delta":{"type":"text_delta","text":text}})
                        .to_string(),
                    ));
                }
            }
            for tc in delta.tool_calls.as_deref().unwrap_or(&[]) {
                match state.open.as_ref() {
                    Some(OpenBlock::Tool { index }) if *index == state.next_index - 1 => {
                        // continue same tool block (partial arguments)
                        let index = *index;
                        if !tc.function.arguments.is_empty() {
                            events.push(SseEvent::named(
                                "content_block_delta",
                                json!({"type":"content_block_delta","index":index,
                                       "delta":{"type":"input_json_delta","partial_json":tc.function.arguments}})
                                .to_string(),
                            ));
                        }
                    }
                    _ => {
                        events.extend(Self::close_block(&mut state));
                        let index = state.next_index;
                        state.next_index += 1;
                        state.open = Some(OpenBlock::Tool { index });
                        events.push(SseEvent::named(
                            "content_block_start",
                            json!({"type":"content_block_start","index":index,
                                   "content_block":{"type":"tool_use","id":tc.id,
                                                     "name":tc.function.name,"input":{}}})
                            .to_string(),
                        ));
                        if !tc.function.arguments.is_empty() {
                            events.push(SseEvent::named(
                                "content_block_delta",
                                json!({"type":"content_block_delta","index":index,
                                       "delta":{"type":"input_json_delta","partial_json":tc.function.arguments}})
                                .to_string(),
                            ));
                        }
                    }
                }
            }
        }

        if let Some(fr) = choice.and_then(|c| c.finish_reason.as_deref()) {
            events.extend(Self::close_block(&mut state));
            // Anthropic places usage at the top level of message_delta, not in delta.
            let mut event = json!({"type":"message_delta",
                "delta":{"stop_reason":unified_to_stop_reason(Some(fr))}});
            if let Some(u) = finish_usage {
                event["usage"] = u;
            }
            events.push(SseEvent::named("message_delta", event.to_string()));
            state.finished = true;
        } else if let Some(u) = finish_usage {
            // usage-only chunk with no finish_reason
            events.push(SseEvent::named(
                "message_delta",
                json!({"type":"message_delta","delta":{},"usage":u}).to_string(),
            ));
        }

        Ok(events)
    }

    fn stream_end(&self) -> Vec<SseEvent> {
        let mut state = self.stream.lock().unwrap_or_else(|e| e.into_inner());
        let mut events = Self::close_block(&mut state);
        if state.started && !state.finished {
            events.push(SseEvent::named(
                "message_delta",
                json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}).to_string(),
            ));
        }
        if state.started {
            events.push(SseEvent::named(
                "message_stop",
                json!({"type":"message_stop"}).to_string(),
            ));
        }
        events
    }

    fn transform_error(&self, err: &ErrorResponse) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "type": "error",
            "error": {"type": err.kind.clone().unwrap_or_else(|| "api_error".into()),
                      "message": err.message}
        }))
        .unwrap_or_default()
    }
}

// ---------------- Outbound (upstream is Anthropic) ----------------

#[derive(Default)]
pub struct AnthropicOutbound {
    /// stream state: id/model, open tool block index
    stream: Mutex<OutboundStreamState>,
}

#[derive(Default)]
struct OutboundStreamState {
    id: String,
    model: String,
    /// tool blocks currently keyed by content block index
    tools: std::collections::HashMap<u64, (String, String)>, // index -> (id, name)
}

impl AnthropicOutbound {
    pub fn new() -> Self {
        Self::default()
    }

    fn empty_chunk(&self, state: &OutboundStreamState) -> StreamChunk {
        StreamChunk {
            id: state.id.clone(),
            model: state.model.clone(),
            choices: Vec::new(),
            usage: None,
            extra: Default::default(),
        }
    }
}

impl OutboundTransformer for AnthropicOutbound {
    fn format(&self) -> &'static str {
        FORMAT
    }

    fn build_request(&self, req: &Request, creds: &Credentials) -> Result<OutboundRequest, TransformError> {
        let body = unified_to_anthropic_body(req);
        Ok(OutboundRequest {
            path: "/v1/messages".into(),
            headers: vec![
                ("x-api-key".into(), creds.api_key.clone()),
                ("anthropic-version".into(), ANTHROPIC_VERSION.into()),
                ("content-type".into(), "application/json".into()),
            ],
            body: serde_json::to_vec(&body).map_err(TransformError::Json)?,
        })
    }

    fn transform_response(&self, body: &[u8]) -> Result<Response, TransformError> {
        let v: Value = serde_json::from_slice(body).map_err(TransformError::Json)?;
        let mut message = Message { role: Role::Assistant, content: None, name: None, tool_calls: None, tool_call_id: None };
        let mut texts: Vec<String> = Vec::new();
        for b in v.get("content").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]) {
            match b.get("type").and_then(Value::as_str) {
                Some("text") => texts.push(b.get("text").and_then(Value::as_str).unwrap_or_default().to_string()),
                Some("tool_use") => message.tool_calls.get_or_insert_with(Vec::new).push(ToolCall {
                    id: b.get("id").and_then(Value::as_str).unwrap_or_default().to_string(),
                    kind: "function".into(),
                    function: FunctionCall {
                        name: b.get("name").and_then(Value::as_str).unwrap_or_default().to_string(),
                        arguments: serde_json::to_string(
                            &b.get("input").cloned().unwrap_or(json!({})),
                        )
                        .unwrap_or_else(|_| "{}".into()),
                    },
                }),
                _ => {}
            }
        }
        if !texts.is_empty() {
            message.content = Some(MessageContent::Text(texts.join("")));
        }
        Ok(Response {
            id: v.get("id").and_then(Value::as_str).unwrap_or_default().to_string(),
            model: v.get("model").and_then(Value::as_str).unwrap_or_default().to_string(),
            choices: vec![Choice {
                index: 0,
                message,
                finish_reason: stop_reason_to_unified(v.get("stop_reason").and_then(Value::as_str)),
            }],
            usage: v.get("usage").map(usage_from_anthropic),
            extra: Default::default(),
        })
    }

    fn transform_stream_event(&self, event: &SseEvent) -> Result<Vec<StreamChunk>, TransformError> {
        let v: Value = serde_json::from_str(event.data.trim()).map_err(TransformError::Json)?;
        let mut state = self.stream.lock().unwrap_or_else(|e| e.into_inner());
        let mut chunk = self.empty_chunk(&state);
        let ev_name = event.event.as_deref()
            .or_else(|| v.get("type").and_then(Value::as_str))
            .unwrap_or_default()
            .to_string();
        match ev_name.as_str() {
            "message_start" => {
                let msg = v.get("message").cloned().unwrap_or(json!({}));
                state.id = msg.get("id").and_then(Value::as_str).unwrap_or_default().to_string();
                state.model = msg.get("model").and_then(Value::as_str).unwrap_or_default().to_string();
                chunk.id = state.id.clone();
                chunk.model = state.model.clone();
                if let Some(u) = msg.get("usage") {
                    let mut usage = usage_from_anthropic(u);
                    usage.completion_tokens = 0;
                    usage.total_tokens = usage.prompt_tokens;
                    chunk.usage = Some(usage);
                }
                chunk.choices = vec![StreamChoice {
                    index: 0,
                    delta: Delta { role: Some(Role::Assistant), content: None, tool_calls: None },
                    finish_reason: None,
                }];
                Ok(vec![chunk])
            }
            "content_block_start" => {
                let index = v.get("index").and_then(Value::as_u64).unwrap_or(0);
                if v.pointer("/content_block/type").and_then(Value::as_str) == Some("tool_use") {
                    let id = v.pointer("/content_block/id").and_then(Value::as_str).unwrap_or_default().to_string();
                    let name = v.pointer("/content_block/name").and_then(Value::as_str).unwrap_or_default().to_string();
                    state.tools.insert(index, (id.clone(), name.clone()));
                    chunk.choices = vec![StreamChoice {
                        index: 0,
                        delta: Delta {
                            role: None,
                            content: None,
                            tool_calls: Some(vec![ToolCall {
                                id,
                                kind: "function".into(),
                                function: FunctionCall { name, arguments: String::new() },
                            }]),
                        },
                        finish_reason: None,
                    }];
                    Ok(vec![chunk])
                } else {
                    Ok(vec![])
                }
            }
            "content_block_delta" => {
                let index = v.get("index").and_then(Value::as_u64).unwrap_or(0);
                match v.pointer("/delta/type").and_then(Value::as_str) {
                    Some("text_delta") => {
                        chunk.choices = vec![StreamChoice {
                            index: 0,
                            delta: Delta {
                                role: None,
                                content: v.pointer("/delta/text").and_then(Value::as_str).map(str::to_string),
                                tool_calls: None,
                            },
                            finish_reason: None,
                        }];
                        Ok(vec![chunk])
                    }
                    Some("input_json_delta") => {
                        let (id, name) = state
                            .tools
                            .get(&index)
                            .cloned()
                            .unwrap_or_else(|| (String::new(), String::new()));
                        chunk.choices = vec![StreamChoice {
                            index: 0,
                            delta: Delta {
                                role: None,
                                content: None,
                                tool_calls: Some(vec![ToolCall {
                                    id,
                                    kind: "function".into(),
                                    function: FunctionCall {
                                        name,
                                        arguments: v
                                            .pointer("/delta/partial_json")
                                            .and_then(Value::as_str)
                                            .unwrap_or_default()
                                            .to_string(),
                                    },
                                }]),
                            },
                            finish_reason: None,
                        }];
                        Ok(vec![chunk])
                    }
                    _ => Ok(vec![]),
                }
            }
            "message_delta" => {
                let finish = stop_reason_to_unified(v.pointer("/delta/stop_reason").and_then(Value::as_str));
                let mut usage = v.get("usage").map(usage_from_anthropic);
                if let Some(u) = usage.as_mut() {
                    // preserve prompt tokens from message_start if omitted
                    if u.prompt_tokens == 0 {
                        u.prompt_tokens = 0; // message_delta usage lacks input_tokens by spec
                    }
                }
                chunk.choices = vec![StreamChoice {
                    index: 0,
                    delta: Delta::default(),
                    finish_reason: finish,
                }];
                chunk.usage = usage;
                Ok(vec![chunk])
            }
            _ => Ok(vec![]),
        }
    }

    fn is_stream_end(&self, event: &SseEvent) -> bool {
        event.event.as_deref() == Some("message_stop")
            || event.data.trim_start_matches('{').starts_with("\"type\":\"message_stop\"")
            || event.data.contains("\"message_stop\"")
    }

    fn extract_error(&self, status: u16, body: &[u8]) -> ErrorResponse {
        let mut err = ErrorResponse {
            message: String::new(),
            kind: None,
            code: None,
            status: Some(status),
        };
        if let Ok(v) = serde_json::from_slice::<Value>(body) {
            let e = v.pointer("/error").cloned().unwrap_or(v);
            err.message = e.get("message").and_then(Value::as_str).unwrap_or_default().to_string();
            err.kind = e.get("type").and_then(Value::as_str).map(str::to_string);
        }
        if err.message.is_empty() {
            err.message = format!("upstream error {status}");
        }
        err
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transformers::{create_inbound, create_outbound};

    const REQ: &str = r#"{
        "model": "claude-sonnet-4",
        "max_tokens": 1024,
        "system": "be brief",
        "stream": false,
        "metadata": {"user_id": "u1"},
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "look"},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "QUJD"}}
            ]},
            {"role": "assistant", "content": [
                {"type": "text", "text": "calling"},
                {"type": "tool_use", "id": "tu1", "name": "get_weather", "input": {"city": "SF"}}
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "tu1", "content": "sunny"}
            ]}
        ],
        "tools": [{"name": "get_weather", "description": "w", "input_schema": {"type":"object"}}],
        "tool_choice": {"type": "auto"}
    }"#;

    #[test]
    fn inbound_parses_blocks() {
        let t = AnthropicInbound::new();
        let req = t.transform_request(REQ.as_bytes()).unwrap();
        assert_eq!(req.model, "claude-sonnet-4");
        assert_eq!(req.max_tokens, Some(1024));
        assert_eq!(req.user.as_deref(), Some("u1"));
        assert_eq!(req.messages[0].role, Role::System);
        // user image message -> Parts with data url
        match req.messages[1].content.as_ref().unwrap() {
            MessageContent::Parts(parts) => {
                assert_eq!(parts.len(), 2);
                match &parts[1] {
                    ContentPart::ImageUrl { image_url } => {
                        assert_eq!(image_url.url, "data:image/png;base64,QUJD");
                    }
                    p => panic!("expected image, got {p:?}"),
                }
            }
            c => panic!("expected parts, got {c:?}"),
        }
        // assistant tool_use
        let asst = &req.messages[2];
        assert_eq!(asst.role, Role::Assistant);
        let tc = asst.tool_calls.as_ref().unwrap();
        assert_eq!(tc[0].id, "tu1");
        assert_eq!(tc[0].function.arguments, r#"{"city":"SF"}"#);
        // tool_result -> tool message
        let tool = &req.messages[3];
        assert_eq!(tool.role, Role::Tool);
        assert_eq!(tool.tool_call_id.as_deref(), Some("tu1"));
        assert_eq!(tool.content.as_ref().unwrap().text(), "sunny");
        assert_eq!(req.tools.as_ref().unwrap()[0].function.name, "get_weather");
    }

    #[test]
    fn outbound_builds_anthropic_body() {
        let t = AnthropicOutbound::new();
        let req = AnthropicInbound::new().transform_request(REQ.as_bytes()).unwrap();
        let out = t.build_request(&req, &Credentials { api_key: "ak".into() }).unwrap();
        assert_eq!(out.path, "/v1/messages");
        assert!(out.headers.iter().any(|(k, v)| k == "x-api-key" && v == "ak"));
        assert!(out.headers.iter().any(|(k, v)| k == "anthropic-version" && v == ANTHROPIC_VERSION));
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["system"][0]["text"], "be brief");
        assert_eq!(v["max_tokens"], 1024);
        let msgs = v["messages"].as_array().unwrap();
        // tool message becomes user tool_result
        assert_eq!(msgs[2]["content"][0]["type"], "tool_result");
        assert_eq!(msgs[2]["content"][0]["tool_use_id"], "tu1");
        assert_eq!(v["tools"][0]["input_schema"]["type"], "object");
    }

    #[test]
    fn outbound_defaults_max_tokens() {
        let t = AnthropicOutbound::new();
        let req = Request {
            model: "m".into(),
            messages: vec![Message { role: Role::User, content: Some(MessageContent::Text("hi".into())), name: None, tool_calls: None, tool_call_id: None }],
            stream: false,
            max_tokens: None,
            temperature: None, top_p: None, stop: None, tools: None,
            tool_choice: None, response_format: None, user: None, extra: Default::default(),
        };
        let out = t.build_request(&req, &Credentials { api_key: "k".into() }).unwrap();
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["max_tokens"], 4096);
    }

    const RESP: &str = r#"{
        "id": "msg_1", "model": "claude-sonnet-4", "role": "assistant",
        "content": [
            {"type": "text", "text": "hello"},
            {"type": "tool_use", "id": "tu2", "name": "f", "input": {"a": 1}}
        ],
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 11, "output_tokens": 7, "cache_read_input_tokens": 5, "cache_creation_input_tokens": 2}
    }"#;

    #[test]
    fn outbound_maps_response_and_usage() {
        let t = AnthropicOutbound::new();
        let r = t.transform_response(RESP.as_bytes()).unwrap();
        assert_eq!(r.choices[0].finish_reason.as_deref(), Some("tool_calls"));
        assert_eq!(r.choices[0].message.content.as_ref().unwrap().text(), "hello");
        let u = r.usage.unwrap();
        assert_eq!(u.prompt_tokens, 11);
        assert_eq!(u.completion_tokens, 7);
        assert_eq!(u.total_tokens, 18);
        assert_eq!(u.cached_tokens, Some(5));
        assert_eq!(u.cache_write_tokens, Some(2));
        let tc = r.choices[0].message.tool_calls.as_ref().unwrap();
        assert_eq!(tc[0].function.arguments, r#"{"a":1}"#);
    }

    fn text_chunk(id: &str, text: &str) -> StreamChunk {
        StreamChunk {
            id: id.into(),
            model: "claude".into(),
            choices: vec![StreamChoice {
                index: 0,
                delta: Delta { role: Some(Role::Assistant), content: Some(text.into()), tool_calls: None },
                finish_reason: None,
            }],
            usage: None,
            extra: Default::default(),
        }
    }

    #[test]
    fn inbound_stream_event_sequence() {
        let t = AnthropicInbound::new();
        let mut all = Vec::new();
        all.extend(t.transform_stream_chunk(&text_chunk("m1", "Hel")).unwrap());
        all.extend(t.transform_stream_chunk(&text_chunk("m1", "lo")).unwrap());
        let mut fin = text_chunk("m1", "");
        fin.choices[0].delta.content = None;
        fin.choices[0].finish_reason = Some("stop".into());
        fin.usage = Some(Usage { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5, ..Default::default() });
        all.extend(t.transform_stream_chunk(&fin).unwrap());
        all.extend(t.stream_end());

        let names: Vec<&str> = all.iter().map(|e| e.event.as_deref().unwrap()).collect();
        assert_eq!(
            names,
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop"
            ]
        );
        let ms: Value = serde_json::from_str(&all[0].data).unwrap();
        assert_eq!(ms["message"]["id"], "m1");
        let md: Value = serde_json::from_str(&all[5].data).unwrap();
        assert_eq!(md["delta"]["stop_reason"], "end_turn");
        // Anthropic places usage at the top level of message_delta.
        assert_eq!(md["usage"]["output_tokens"], 2);
        assert!(md["delta"].get("usage").is_none());
        let d1: Value = serde_json::from_str(&all[2].data).unwrap();
        assert_eq!(d1["delta"]["text"], "Hel");
    }

    #[test]
    fn inbound_stream_tool_call_sequence() {
        let t = AnthropicInbound::new();
        let mut chunk = text_chunk("m2", "");
        chunk.choices[0].delta.content = None;
        chunk.choices[0].delta.tool_calls = Some(vec![ToolCall {
            id: "t1".into(),
            kind: "function".into(),
            function: FunctionCall { name: "f".into(), arguments: "{\"x\":".into() },
        }]);
        let mut evs = t.transform_stream_chunk(&chunk).unwrap();
        chunk.choices[0].delta.tool_calls.as_mut().unwrap()[0].function.arguments = "1}".into();
        evs.extend(t.transform_stream_chunk(&chunk).unwrap());
        let mut fin = text_chunk("m2", "");
        fin.choices[0].delta.content = None;
        fin.choices[0].finish_reason = Some("tool_calls".into());
        evs.extend(t.transform_stream_chunk(&fin).unwrap());
        evs.extend(t.stream_end());
        let names: Vec<&str> = evs.iter().map(|e| e.event.as_deref().unwrap()).collect();
        assert_eq!(
            names,
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop"
            ]
        );
        let cbs: Value = serde_json::from_str(&evs[1].data).unwrap();
        assert_eq!(cbs["content_block"]["type"], "tool_use");
        let d2: Value = serde_json::from_str(&evs[3].data).unwrap();
        assert_eq!(d2["delta"]["partial_json"], "1}");
        let md: Value = serde_json::from_str(&evs[5].data).unwrap();
        assert_eq!(md["delta"]["stop_reason"], "tool_use");
    }

    #[test]
    fn outbound_stream_decodes_anthropic_sse() {
        let t = AnthropicOutbound::new();
        let feed = |t: &AnthropicOutbound, ev: SseEvent| t.transform_stream_event(&ev).unwrap();
        let start = feed(
            &t,
            SseEvent::named(
                "message_start",
                r#"{"type":"message_start","message":{"id":"msg_9","model":"claude","usage":{"input_tokens":12,"output_tokens":1}}}"#,
            ),
        );
        assert_eq!(start[0].id, "msg_9");
        assert_eq!(start[0].usage.as_ref().unwrap().prompt_tokens, 12);
        assert_eq!(start[0].choices[0].delta.role, Some(Role::Assistant));

        let cbs = feed(
            &t,
            SseEvent::named(
                "content_block_start",
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tu9","name":"g","input":{}}}"#,
            ),
        );
        assert_eq!(cbs[0].choices[0].delta.tool_calls.as_ref().unwrap()[0].id, "tu9");

        let d1 = feed(
            &t,
            SseEvent::named(
                "content_block_delta",
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"q\":"}}"#,
            ),
        );
        assert_eq!(
            d1[0].choices[0].delta.tool_calls.as_ref().unwrap()[0].function.arguments,
            "{\"q\":"
        );

        let d2 = feed(
            &t,
            SseEvent::named(
                "content_block_delta",
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}"#,
            ),
        );
        assert_eq!(d2[0].choices[0].delta.content.as_deref(), Some("hi"));

        let md = feed(
            &t,
            SseEvent::named(
                "message_delta",
                r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":4}}"#,
            ),
        );
        assert_eq!(md[0].choices[0].finish_reason.as_deref(), Some("stop"));
        assert_eq!(md[0].usage.as_ref().unwrap().completion_tokens, 4);

        let stop = SseEvent::named("message_stop", r#"{"type":"message_stop"}"#);
        assert!(t.is_stream_end(&stop));
        assert!(t.transform_stream_event(&stop).unwrap().is_empty());
    }

    #[test]
    fn extract_error_shape() {
        let t = AnthropicOutbound::new();
        let e = t.extract_error(
            400,
            br#"{"type":"error","error":{"type":"invalid_request_error","message":"max_tokens required"}}"#,
        );
        assert_eq!(e.kind.as_deref(), Some("invalid_request_error"));
        assert_eq!(e.message, "max_tokens required");
    }

    #[test]
    fn system_array_and_roundtrip_openai() {
        let body = r#"{"model":"c","max_tokens":10,"system":[{"type":"text","text":"a"},{"type":"text","text":"b"}],"messages":[{"role":"user","content":"hi"}]}"#;
        let req = AnthropicInbound::new().transform_request(body.as_bytes()).unwrap();
        assert_eq!(req.messages[0].content.as_ref().unwrap().text(), "a\nb");
        // unified -> openai outbound body parses and keeps messages
        let out = crate::transformers::openai::OpenAiOutbound::new()
            .build_request(&req, &Credentials { api_key: "k".into() })
            .unwrap();
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["messages"][0]["content"], "a\nb");
    }

    #[test]
    fn factories_resolve() {
        assert!(create_inbound(FORMAT).is_some());
        assert!(create_outbound(FORMAT).is_some());
    }
}
