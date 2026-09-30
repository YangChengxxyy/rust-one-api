//! OpenAI Responses API wire format (`/v1/responses`), both directions.
//!
//! Scope cuts (stateless gateway, aligned with the roadmap):
//! - `background: true` and `previous_response_id` are rejected with
//!   [`TransformError::Unsupported`] — both require server-side state.
//! - `reasoning` items, non-function tools, and `input_*` parts other than
//!   text/image are skipped; `store`/`include`/`truncation` are dropped.
//! - Only CC-safe extras pass through: `parallel_tool_calls`, `service_tier`,
//!   `prompt_cache_key`, `metadata`, `reasoning_effort`.
//!
//! Stream mapping note: the Responses stream is a named-event state machine
//! (`response.output_item.added` ... `response.completed`). Unified chunks
//! arrive CC-style, so closing events (`*.done`, `response.completed`) are
//! deferred to [`InboundTransformer::stream_end`] — usage lands on the final
//! CC chunk *after* `finish_reason`, and deferring lets the terminal
//! `response.completed` carry the full output + usage in one payload.

use std::sync::Mutex;

use crate::error::TransformError;
use crate::sse::SseEvent;
use crate::transformer::{Credentials, InboundTransformer, OutboundRequest, OutboundTransformer};
use crate::{
    Choice, ContentPart, Delta, ErrorResponse, FunctionCall, FunctionDef, ImageUrl, Message,
    MessageContent, Request, Response, Role, StreamChoice, StreamChunk, Tool, ToolCall, Usage,
};
use serde_json::{json, Map, Value};

pub const FORMAT: &str = "openai/responses";

// ---------- shared helpers ----------

fn usage_to_responses(u: &Usage) -> Value {
    json!({
        "input_tokens": u.prompt_tokens,
        "output_tokens": u.completion_tokens,
        "total_tokens": u.total_tokens,
        "input_tokens_details": { "cached_tokens": u.cached_tokens.unwrap_or(0) },
        "output_tokens_details": { "reasoning_tokens": u.reasoning_tokens.unwrap_or(0) },
    })
}

fn usage_from_responses(v: &Value) -> Usage {
    let mut u = Usage {
        prompt_tokens: v.get("input_tokens").and_then(Value::as_u64).unwrap_or(0),
        completion_tokens: v.get("output_tokens").and_then(Value::as_u64).unwrap_or(0),
        total_tokens: v.get("total_tokens").and_then(Value::as_u64).unwrap_or(0),
        ..Default::default()
    };
    if let Some(c) = v.pointer("/input_tokens_details/cached_tokens").and_then(Value::as_u64) {
        u.cached_tokens = Some(c);
    }
    if let Some(r) = v.pointer("/output_tokens_details/reasoning_tokens").and_then(Value::as_u64) {
        u.reasoning_tokens = Some(r);
    }
    u
}

/// `{"type":"function","name":...}` (Responses) -> `{"type":"function","function":{"name":...}}` (CC).
fn tool_choice_to_cc(v: &Value) -> Value {
    if v.get("type").and_then(Value::as_str) == Some("function") {
        if let Some(name) = v.get("name").and_then(Value::as_str) {
            return json!({"type": "function", "function": {"name": name}});
        }
    }
    v.clone()
}

/// CC-shaped tool_choice -> Responses shape. Strings pass through either way.
fn tool_choice_to_responses(v: &Value) -> Value {
    if v.get("type").and_then(Value::as_str) == Some("function") {
        if let Some(name) = v.pointer("/function/name").and_then(Value::as_str) {
            return json!({"type": "function", "name": name});
        }
    }
    v.clone()
}

fn text_msg(role: Role, text: &str) -> Message {
    Message {
        role,
        content: Some(MessageContent::Text(text.into())),
        name: None,
        tool_calls: None,
        tool_call_id: None,
    }
}

/// `function_call_output.output` may be a string or an array of text parts.
fn output_text_of(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|p| p.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(""),
        other => other.to_string(),
    }
}

/// Responses envelope: `{"error": {"message", "type", "code"}}` (same as CC).
fn openai_style_error(err: &ErrorResponse) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "error": {
            "message": err.message,
            "type": err.kind,
            "code": err.code,
        }
    }))
    .unwrap_or_default()
}

fn openai_style_extract_error(status: u16, body: &[u8]) -> ErrorResponse {
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
        err.code = e.get("code").map(|c| {
            if c.is_string() {
                c.as_str().unwrap_or_default().to_string()
            } else {
                c.to_string()
            }
        });
    }
    if err.message.is_empty() {
        err.message = format!("upstream error {status}");
    }
    err
}

/// SSE event name / data `type` discriminator for Responses events.
fn event_type(event: &SseEvent) -> String {
    if let Ok(v) = serde_json::from_str::<Value>(event.data.trim()) {
        if let Some(t) = v.get("type").and_then(Value::as_str) {
            return t.to_string();
        }
    }
    event.event.clone().unwrap_or_default()
}

// ---------- Inbound (client speaks Responses) ----------

#[derive(Debug, Clone, Default)]
struct FnState {
    item_id: String,
    call_id: String,
    name: String,
    args: String,
    index: u64,
    /// Source-side stream position (CC delta index / Responses output_index).
    src_index: Option<u32>,
}

#[derive(Debug, Default)]
struct InState {
    started: bool,
    closed: bool,
    id: String,
    model: String,
    created: Option<u64>,
    seq: u64,
    items_opened: u64,
    text_open: bool,
    text_item_id: String,
    text_index: u64,
    text: String,
    fns: Vec<FnState>,
    finish: Option<String>,
    usage: Option<Usage>,
}

impl InState {
    fn event(&mut self, ty: &str, mut payload: Value) -> SseEvent {
        let seq = self.seq;
        self.seq += 1;
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("type".into(), json!(ty));
            obj.insert("sequence_number".into(), json!(seq));
        }
        SseEvent::named(ty, payload.to_string())
    }

    fn response_skeleton(&self, status: &str) -> Value {
        json!({
            "id": self.id,
            "object": "response",
            "created_at": self.created.unwrap_or(0),
            "status": status,
            "model": self.model,
            "output": [],
        })
    }

    fn text_item(&self, status: &str) -> Value {
        json!({
            "id": self.text_item_id,
            "type": "message",
            "role": "assistant",
            "status": status,
            "content": [{"type": "output_text", "text": self.text, "annotations": []}],
        })
    }

    fn fn_item(f: &FnState, status: &str) -> Value {
        json!({
            "id": f.item_id,
            "type": "function_call",
            "call_id": f.call_id,
            "name": f.name,
            "arguments": f.args,
            "status": status,
        })
    }
}

#[derive(Debug, Default)]
pub struct ResponsesInbound {
    state: Mutex<InState>,
}

impl ResponsesInbound {
    pub fn new() -> Self {
        Self::default()
    }
}

fn parse_input(input: &Value, messages: &mut Vec<Message>) {
    match input {
        Value::String(s) => messages.push(text_msg(Role::User, s)),
        Value::Array(items) => {
            for item in items {
                let ty = item.get("type").and_then(Value::as_str);
                match ty {
                    Some("function_call") => {
                        let call_id = item
                            .get("call_id")
                            .and_then(Value::as_str)
                            .or_else(|| item.get("id").and_then(Value::as_str))
                            .unwrap_or_default()
                            .to_string();
                        messages.push(Message {
                            role: Role::Assistant,
                            content: None,
                            name: None,
                            tool_calls: Some(vec![ToolCall {
                                id: call_id,
                                kind: "function".into(),
                                index: None,
                                function: FunctionCall {
                                    name: item.get("name").and_then(Value::as_str).unwrap_or_default().into(),
                                    arguments: item
                                        .get("arguments")
                                        .and_then(Value::as_str)
                                        .unwrap_or("{}")
                                        .into(),
                                },
                            }]),
                            tool_call_id: None,
                        });
                    }
                    Some("function_call_output") => {
                        let output = output_text_of(item.get("output").unwrap_or(&Value::Null));
                        messages.push(Message {
                            role: Role::Tool,
                            content: Some(MessageContent::Text(output)),
                            name: None,
                            tool_calls: None,
                            tool_call_id: item.get("call_id").and_then(Value::as_str).map(str::to_string),
                        });
                    }
                    _ if item.get("role").is_some() && matches!(ty, None | Some("message")) => {
                        if let Some(msg) = parse_message_item(item) {
                            messages.push(msg);
                        }
                    }
                    // reasoning, item_reference, unknown items: skipped (scope cut).
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

fn parse_message_item(item: &Value) -> Option<Message> {
    let role = match item.get("role").and_then(Value::as_str)? {
        "system" => Role::System,
        "developer" => Role::Developer,
        "user" => Role::User,
        "assistant" => Role::Assistant,
        _ => return None,
    };
    let content = match item.get("content") {
        Some(Value::String(s)) => Some(MessageContent::Text(s.clone())),
        Some(Value::Array(parts)) => {
            let parts: Vec<ContentPart> = parts
                .iter()
                .filter_map(|p| match p.get("type").and_then(Value::as_str) {
                    Some("input_text") | Some("output_text") | Some("text") => {
                        Some(ContentPart::Text {
                            text: p.get("text").and_then(Value::as_str).unwrap_or_default().into(),
                        })
                    }
                    Some("input_image") => Some(ContentPart::ImageUrl {
                        image_url: ImageUrl {
                            url: p.get("image_url").and_then(Value::as_str).unwrap_or_default().into(),
                            detail: p.get("detail").and_then(Value::as_str).map(str::to_string),
                        },
                    }),
                    _ => None,
                })
                .collect();
            if parts.is_empty() {
                None
            } else {
                Some(MessageContent::Parts(parts))
            }
        }
        _ => None,
    };
    content.map(|content| Message {
        role,
        content: Some(content),
        name: None,
        tool_calls: None,
        tool_call_id: None,
    })
}

impl InboundTransformer for ResponsesInbound {
    fn format(&self) -> &'static str {
        FORMAT
    }

    fn transform_request(&self, body: &[u8]) -> Result<Request, TransformError> {
        let v: Value = serde_json::from_slice(body).map_err(TransformError::Json)?;
        if v.get("background").and_then(Value::as_bool) == Some(true) {
            return Err(TransformError::Unsupported(
                "background mode requires server-side state".into(),
            ));
        }
        if v.get("previous_response_id").and_then(Value::as_str).is_some() {
            return Err(TransformError::Unsupported(
                "previous_response_id requires server-side state".into(),
            ));
        }
        let mut messages = Vec::new();
        if let Some(instr) = v.get("instructions").and_then(Value::as_str) {
            if !instr.is_empty() {
                messages.push(text_msg(Role::System, instr));
            }
        }
        if let Some(input) = v.get("input") {
            parse_input(input, &mut messages);
        }
        let tools = v
            .get("tools")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter(|t| t.get("type").and_then(Value::as_str) == Some("function"))
                    .map(|t| Tool {
                        kind: "function".into(),
                        function: FunctionDef {
                            name: t.get("name").and_then(Value::as_str).unwrap_or_default().into(),
                            description: t.get("description").and_then(Value::as_str).map(str::to_string),
                            parameters: t.get("parameters").cloned(),
                        },
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|t: &Vec<Tool>| !t.is_empty());
        let mut extra = Map::new();
        for k in ["parallel_tool_calls", "service_tier", "prompt_cache_key", "metadata", "reasoning_effort"] {
            if let Some(val) = v.get(k) {
                if !val.is_null() {
                    extra.insert((*k).into(), val.clone());
                }
            }
        }
        Ok(Request {
            model: v.get("model").and_then(Value::as_str).unwrap_or_default().into(),
            messages,
            stream: v.get("stream").and_then(Value::as_bool).unwrap_or(false),
            max_tokens: v.get("max_output_tokens").and_then(Value::as_u64).map(|n| n as u32),
            temperature: v.get("temperature").and_then(Value::as_f64).map(|f| f as f32),
            top_p: v.get("top_p").and_then(Value::as_f64).map(|f| f as f32),
            stop: None,
            tools,
            tool_choice: v.get("tool_choice").map(tool_choice_to_cc),
            response_format: v
                .pointer("/text/format")
                .filter(|f| f.get("type").and_then(Value::as_str) != Some("text"))
                .cloned(),
            user: v.get("user").and_then(Value::as_str).map(str::to_string),
            extra,
        })
    }

    fn transform_response(&self, resp: &Response) -> Result<Vec<u8>, TransformError> {
        let choice = resp.choices.first();
        let finish = choice.and_then(|c| c.finish_reason.as_deref());
        let mut output = Vec::new();
        if let Some(m) = choice.map(|c| &c.message) {
            if let Some(content) = &m.content {
                let text = content.text();
                if !text.is_empty() {
                    output.push(json!({
                        "id": "msg_0",
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [{"type": "output_text", "text": text, "annotations": []}],
                    }));
                }
            }
            for (i, tc) in m.tool_calls.iter().flat_map(|v| v.iter()).enumerate() {
                output.push(json!({
                    "id": format!("fc_{i}"),
                    "type": "function_call",
                    "call_id": tc.id,
                    "name": tc.function.name,
                    "arguments": tc.function.arguments,
                    "status": "completed",
                }));
            }
        }
        let incomplete = finish == Some("length");
        let mut obj = json!({
            "id": resp.id,
            "object": "response",
            "model": resp.model,
            "status": if incomplete { "incomplete" } else { "completed" },
            "output": output,
        });
        if let Some(created) = resp.extra.get("created").and_then(Value::as_u64) {
            obj["created_at"] = json!(created);
        }
        if incomplete {
            obj["incomplete_details"] = json!({"reason": "max_output_tokens"});
        }
        if let Some(u) = &resp.usage {
            obj["usage"] = usage_to_responses(u);
        }
        serde_json::to_vec(&obj).map_err(TransformError::Json)
    }

    fn transform_stream_chunk(&self, chunk: &StreamChunk) -> Result<Vec<SseEvent>, TransformError> {
        let mut st = self
            .state
            .lock()
            .map_err(|_| TransformError::InvalidResponse("stream state poisoned".into()))?;
        let mut out = Vec::new();
        if !chunk.id.is_empty() {
            st.id = chunk.id.clone();
        }
        if !chunk.model.is_empty() {
            st.model = chunk.model.clone();
        }
        if st.created.is_none() {
            st.created = chunk.extra.get("created").and_then(Value::as_u64);
        }
        if !st.started {
            st.started = true;
            let skeleton = st.response_skeleton("in_progress");
            out.push(st.event("response.created", json!({"response": skeleton})));
            let skeleton = st.response_skeleton("in_progress");
            out.push(st.event("response.in_progress", json!({"response": skeleton})));
        }
        for choice in &chunk.choices {
            if let Some(text) = &choice.delta.content {
                if !st.text_open {
                    st.text_open = true;
                    st.text_item_id = format!("msg_{}", st.items_opened);
                    st.text_index = st.items_opened;
                    st.items_opened += 1;
                    let (item_id, idx) = (st.text_item_id.clone(), st.text_index);
                    out.push(st.event(
                        "response.output_item.added",
                        json!({"output_index": idx, "item": {
                            "id": item_id, "type": "message", "role": "assistant",
                            "status": "in_progress", "content": [],
                        }}),
                    ));
                    out.push(st.event(
                        "response.content_part.added",
                        json!({"item_id": item_id, "output_index": idx, "content_index": 0,
                            "part": {"type": "output_text", "text": "", "annotations": []}}),
                    ));
                }
                if !text.is_empty() {
                    st.text.push_str(text);
                    let (item_id, idx) = (st.text_item_id.clone(), st.text_index);
                    out.push(st.event(
                        "response.output_text.delta",
                        json!({"item_id": item_id, "output_index": idx,
                            "content_index": 0, "delta": text}),
                    ));
                }
            }
            for tc in choice.delta.tool_calls.as_deref().unwrap_or(&[]) {
                // Resolve the target call: source index (precise for parallel
                // calls) -> id -> most recently opened (CC continuation deltas
                // often carry neither).
                let pos = if let Some(i) = tc.index {
                    st.fns.iter().rposition(|f| f.src_index == Some(i))
                } else if !tc.id.is_empty() {
                    st.fns.iter().rposition(|f| f.call_id == tc.id)
                } else if st.fns.is_empty() {
                    None
                } else {
                    Some(st.fns.len() - 1)
                };
                let idx = match pos {
                    Some(i) => i,
                    None => {
                        let f = FnState {
                            item_id: format!("fc_{}", st.items_opened),
                            call_id: tc.id.clone(),
                            name: tc.function.name.clone(),
                            args: String::new(),
                            index: st.items_opened,
                            src_index: tc.index,
                        };
                        st.items_opened += 1;
                        out.push(st.event(
                            "response.output_item.added",
                            json!({"output_index": f.index, "item": InState::fn_item(&f, "in_progress")}),
                        ));
                        st.fns.push(f);
                        st.fns.len() - 1
                    }
                };
                if !tc.function.arguments.is_empty() {
                    st.fns[idx].args.push_str(&tc.function.arguments);
                    let f = st.fns[idx].clone();
                    out.push(st.event(
                        "response.function_call_arguments.delta",
                        json!({"item_id": f.item_id, "output_index": f.index, "delta": tc.function.arguments}),
                    ));
                }
            }
            if let Some(f) = &choice.finish_reason {
                st.finish = Some(f.clone());
            }
        }
        if let Some(u) = &chunk.usage {
            st.usage = Some(u.clone());
        }
        Ok(out)
    }

    fn stream_end(&self) -> Vec<SseEvent> {
        let mut st = match self.state.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        if !st.started || st.closed {
            return vec![];
        }
        st.closed = true;
        let mut out = Vec::new();
        let mut output_items: Vec<(u64, Value)> = Vec::new();
        if st.text_open {
            let (item_id, idx, text) = (st.text_item_id.clone(), st.text_index, st.text.clone());
            out.push(st.event(
                "response.output_text.done",
                json!({"item_id": item_id, "output_index": idx, "content_index": 0, "text": text}),
            ));
            out.push(st.event(
                "response.content_part.done",
                json!({"item_id": item_id, "output_index": idx, "content_index": 0,
                    "part": {"type": "output_text", "text": text, "annotations": []}}),
            ));
            let item = st.text_item("completed");
            out.push(st.event(
                "response.output_item.done",
                json!({"output_index": idx, "item": item}),
            ));
            output_items.push((idx, st.text_item("completed")));
        }
        let fns = st.fns.clone();
        for f in &fns {
            out.push(st.event(
                "response.function_call_arguments.done",
                json!({"item_id": f.item_id, "output_index": f.index, "arguments": f.args}),
            ));
            out.push(st.event(
                "response.output_item.done",
                json!({"output_index": f.index, "item": InState::fn_item(f, "completed")}),
            ));
            output_items.push((f.index, InState::fn_item(f, "completed")));
        }
        output_items.sort_by_key(|(idx, _)| *idx);
        let incomplete = st.finish.as_deref() == Some("length");
        let status = if incomplete { "incomplete" } else { "completed" };
        let mut response = json!({
            "id": st.id,
            "object": "response",
            "created_at": st.created.unwrap_or(0),
            "status": status,
            "model": st.model,
            "output": output_items.into_iter().map(|(_, item)| item).collect::<Vec<_>>(),
        });
        if let Some(u) = &st.usage {
            response["usage"] = usage_to_responses(u);
        }
        if incomplete {
            response["incomplete_details"] = json!({"reason": "max_output_tokens"});
        }
        let ty = format!("response.{status}");
        out.push(st.event(&ty, json!({"response": response})));
        out
    }

    fn transform_error(&self, err: &ErrorResponse) -> Vec<u8> {
        openai_style_error(err)
    }
}

// ---------- Outbound (upstream is a Responses API) ----------

#[derive(Debug, Default)]
struct OutState {
    id: String,
    model: String,
    created: Option<u64>,
    /// item_id -> (call_id, name) for open function_call items.
    items: std::collections::HashMap<String, (String, String)>,
    saw_fn: bool,
}

impl OutState {
    fn capture_response_meta(&mut self, r: &Value) {
        if let Some(id) = r.get("id").and_then(Value::as_str) {
            self.id = id.to_string();
        }
        if let Some(m) = r.get("model").and_then(Value::as_str) {
            self.model = m.to_string();
        }
        if self.created.is_none() {
            self.created = r.get("created_at").and_then(Value::as_u64);
        }
    }

    fn chunk(&self, delta: Delta, finish: Option<String>, usage: Option<Usage>) -> StreamChunk {
        let mut extra = Map::new();
        if let Some(c) = self.created {
            extra.insert("created".into(), json!(c));
        }
        StreamChunk {
            id: self.id.clone(),
            model: self.model.clone(),
            choices: vec![StreamChoice { index: 0, delta, finish_reason: finish }],
            usage,
            extra,
        }
    }
}

#[derive(Debug, Default)]
pub struct ResponsesOutbound {
    state: Mutex<OutState>,
}

impl ResponsesOutbound {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OutboundTransformer for ResponsesOutbound {
    fn format(&self) -> &'static str {
        FORMAT
    }

    fn build_request(&self, req: &Request, creds: &Credentials) -> Result<OutboundRequest, TransformError> {
        let mut input: Vec<Value> = Vec::new();
        let mut instructions: Vec<String> = Vec::new();
        for m in &req.messages {
            match m.role {
                Role::System | Role::Developer => {
                    if let Some(c) = &m.content {
                        let t = c.text();
                        if !t.is_empty() {
                            instructions.push(t);
                        }
                    }
                }
                Role::User => {
                    let content = match &m.content {
                        Some(MessageContent::Text(t)) => {
                            json!([{"type": "input_text", "text": t.clone()}])
                        }
                        Some(MessageContent::Parts(parts)) => Value::Array(
                            parts
                                .iter()
                                .filter_map(|p| match p {
                                    ContentPart::Text { text } => {
                                        Some(json!({"type": "input_text", "text": text.clone()}))
                                    }
                                    ContentPart::ImageUrl { image_url } => {
                                        Some(json!({"type": "input_image", "image_url": image_url.url.clone()}))
                                    }
                                    ContentPart::Unknown => None,
                                })
                                .collect(),
                        ),
                        None => continue,
                    };
                    input.push(json!({"role": "user", "content": content}));
                }
                Role::Assistant => {
                    if let Some(c) = &m.content {
                        let t = c.text();
                        if !t.is_empty() {
                            input.push(json!({"role": "assistant",
                                "content": [{"type": "output_text", "text": t}]}));
                        }
                    }
                    for tc in m.tool_calls.as_deref().unwrap_or(&[]) {
                        input.push(json!({
                            "type": "function_call",
                            "call_id": tc.id,
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        }));
                    }
                }
                Role::Tool => {
                    input.push(json!({
                        "type": "function_call_output",
                        "call_id": m.tool_call_id.clone().unwrap_or_default(),
                        "output": m.content.as_ref().map(|c| c.text()).unwrap_or_default(),
                    }));
                }
            }
        }
        let mut body = Map::new();
        body.insert("model".into(), json!(req.model));
        if req.stream {
            body.insert("stream".into(), json!(true));
        }
        if !instructions.is_empty() {
            body.insert("instructions".into(), json!(instructions.join("\n\n")));
        }
        body.insert("input".into(), Value::Array(input));
        if let Some(mt) = req.max_tokens {
            body.insert("max_output_tokens".into(), json!(mt));
        }
        if let Some(t) = req.temperature {
            body.insert("temperature".into(), json!(t));
        }
        if let Some(t) = req.top_p {
            body.insert("top_p".into(), json!(t));
        }
        if let Some(tools) = &req.tools {
            let flat: Vec<Value> = tools
                .iter()
                .map(|t| {
                    let mut o = json!({"type": t.kind, "name": t.function.name});
                    if let Some(d) = &t.function.description {
                        o["description"] = json!(d);
                    }
                    if let Some(p) = &t.function.parameters {
                        o["parameters"] = p.clone();
                    }
                    o
                })
                .collect();
            body.insert("tools".into(), Value::Array(flat));
        }
        if let Some(tc) = &req.tool_choice {
            body.insert("tool_choice".into(), tool_choice_to_responses(tc));
        }
        if let Some(rf) = &req.response_format {
            body.insert("text".into(), json!({"format": rf.clone()}));
        }
        if let Some(u) = &req.user {
            body.insert("user".into(), json!(u));
        }
        // `stop` has no Responses counterpart; dropped (documented above).
        for (k, v) in &req.extra {
            body.insert(k.clone(), v.clone());
        }
        Ok(OutboundRequest {
            path: "/responses".into(),
            headers: vec![
                ("Authorization".into(), format!("Bearer {}", creds.api_key)),
                ("content-type".into(), "application/json".into()),
            ],
            body: serde_json::to_vec(&Value::Object(body)).map_err(TransformError::Json)?,
        })
    }

    fn transform_response(&self, body: &[u8]) -> Result<Response, TransformError> {
        let v: Value = serde_json::from_slice(body).map_err(TransformError::Json)?;
        let mut message = Message {
            role: Role::Assistant,
            content: None,
            name: None,
            tool_calls: None,
            tool_call_id: None,
        };
        let mut texts: Vec<String> = Vec::new();
        for item in v.get("output").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]) {
            match item.get("type").and_then(Value::as_str) {
                Some("message") => {
                    for p in item.get("content").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]) {
                        if p.get("type").and_then(Value::as_str) == Some("output_text") {
                            texts.push(p.get("text").and_then(Value::as_str).unwrap_or_default().into());
                        }
                    }
                }
                Some("function_call") => {
                    let id = item
                        .get("call_id")
                        .and_then(Value::as_str)
                        .or_else(|| item.get("id").and_then(Value::as_str))
                        .unwrap_or_default()
                        .to_string();
                    message.tool_calls.get_or_insert_with(Vec::new).push(ToolCall {
                        id,
                        kind: "function".into(),
                        index: None,
                        function: FunctionCall {
                            name: item.get("name").and_then(Value::as_str).unwrap_or_default().into(),
                            arguments: item.get("arguments").and_then(Value::as_str).unwrap_or("{}").into(),
                        },
                    });
                }
                _ => {}
            }
        }
        if !texts.is_empty() {
            message.content = Some(MessageContent::Text(texts.join("")));
        }
        let has_calls = message.tool_calls.as_ref().is_some_and(|t| !t.is_empty());
        let status = v.get("status").and_then(Value::as_str).unwrap_or("completed");
        let finish = match status {
            "incomplete" => "length",
            _ if has_calls => "tool_calls",
            _ => "stop",
        };
        let mut extra = Map::new();
        if let Some(c) = v.get("created_at").and_then(Value::as_u64) {
            extra.insert("created".into(), json!(c));
        }
        Ok(Response {
            id: v.get("id").and_then(Value::as_str).unwrap_or_default().into(),
            model: v.get("model").and_then(Value::as_str).unwrap_or_default().into(),
            choices: vec![Choice {
                index: 0,
                message,
                finish_reason: Some(finish.into()),
            }],
            usage: v.get("usage").map(usage_from_responses),
            extra,
        })
    }

    fn transform_stream_event(&self, event: &SseEvent) -> Result<Vec<StreamChunk>, TransformError> {
        let ty = event_type(event);
        let v: Value = serde_json::from_str(event.data.trim()).map_err(TransformError::Json)?;
        let mut st = self
            .state
            .lock()
            .map_err(|_| TransformError::InvalidResponse("stream state poisoned".into()))?;
        match ty.as_str() {
            "response.created" => {
                if let Some(r) = v.get("response") {
                    st.capture_response_meta(r);
                }
                Ok(vec![st.chunk(
                    Delta { role: Some(Role::Assistant), content: None, tool_calls: None },
                    None,
                    None,
                )])
            }
            "response.in_progress" => {
                if let Some(r) = v.get("response") {
                    st.capture_response_meta(r);
                }
                Ok(vec![])
            }
            "response.output_text.delta" => {
                let delta = v.get("delta").and_then(Value::as_str).unwrap_or_default().to_string();
                if delta.is_empty() {
                    return Ok(vec![]);
                }
                Ok(vec![st.chunk(
                    Delta { role: None, content: Some(delta), tool_calls: None },
                    None,
                    None,
                )])
            }
            "response.output_item.added" => {
                let item = v.get("item").cloned().unwrap_or(Value::Null);
                if item.get("type").and_then(Value::as_str) != Some("function_call") {
                    return Ok(vec![]);
                }
                let item_id = item.get("id").and_then(Value::as_str).unwrap_or_default().to_string();
                let call_id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| item_id.clone());
                let name = item.get("name").and_then(Value::as_str).unwrap_or_default().to_string();
                st.items.insert(item_id, (call_id.clone(), name.clone()));
                st.saw_fn = true;
                let out_index = v.get("output_index").and_then(Value::as_u64).map(|n| n as u32);
                Ok(vec![st.chunk(
                    Delta {
                        role: None,
                        content: None,
                        tool_calls: Some(vec![ToolCall {
                            id: call_id,
                            kind: "function".into(),
                            index: out_index,
                            function: FunctionCall { name, arguments: String::new() },
                        }]),
                    },
                    None,
                    None,
                )])
            }
            "response.function_call_arguments.delta" => {
                let item_id = v.get("item_id").and_then(Value::as_str).unwrap_or_default().to_string();
                let call_id = st
                    .items
                    .get(&item_id)
                    .map(|(c, _)| c.clone())
                    .unwrap_or(item_id);
                let delta = v.get("delta").and_then(Value::as_str).unwrap_or_default().to_string();
                if delta.is_empty() {
                    return Ok(vec![]);
                }
                Ok(vec![st.chunk(
                    Delta {
                        role: None,
                        content: None,
                        tool_calls: Some(vec![ToolCall {
                            id: call_id,
                            kind: "function".into(),
                            index: v.get("output_index").and_then(Value::as_u64).map(|n| n as u32),
                            function: FunctionCall { name: String::new(), arguments: delta },
                        }]),
                    },
                    None,
                    None,
                )])
            }
            "response.completed" | "response.incomplete" => {
                let resp = v.get("response").cloned().unwrap_or(Value::Null);
                st.capture_response_meta(&resp);
                let usage = resp.get("usage").filter(|u| !u.is_null()).map(usage_from_responses);
                let finish = if ty == "response.incomplete" {
                    "length"
                } else if st.saw_fn {
                    "tool_calls"
                } else {
                    "stop"
                };
                Ok(vec![st.chunk(Delta::default(), Some(finish.into()), usage)])
            }
            _ => Ok(vec![]),
        }
    }

    fn is_stream_end(&self, event: &SseEvent) -> bool {
        matches!(
            event_type(event).as_str(),
            "response.completed" | "response.incomplete" | "response.failed" | "error"
        )
    }

    fn extract_error(&self, status: u16, body: &[u8]) -> ErrorResponse {
        openai_style_extract_error(status, body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- inbound request parsing ----------

    #[test]
    fn inbound_parses_full_request() {
        let t = ResponsesInbound::new();
        let body = json!({
            "model": "gpt-5",
            "instructions": "be terse",
            "input": [
                {"type": "message", "role": "user", "content": [
                    {"type": "input_text", "text": "hi"},
                    {"type": "input_image", "image_url": "https://x/img.png"},
                    {"type": "input_file", "file_url": "https://x/f.pdf"}
                ]},
                {"type": "reasoning", "summary": []},
                {"type": "function_call", "call_id": "call_1", "name": "get_weather", "arguments": "{\"city\":\"SF\"}"},
                {"type": "function_call_output", "call_id": "call_1", "output": "sunny"}
            ],
            "max_output_tokens": 512,
            "temperature": 0.5,
            "stream": true,
            "tools": [{"type": "function", "name": "get_weather", "description": "d",
                       "parameters": {"type": "object"}}, {"type": "web_search"}],
            "tool_choice": {"type": "function", "name": "get_weather"},
            "text": {"format": {"type": "json_object"}},
            "user": "u1",
            "store": false,
            "parallel_tool_calls": false
        });
        let req = t.transform_request(serde_json::to_string(&body).unwrap().as_bytes()).unwrap();
        assert_eq!(req.model, "gpt-5");
        assert!(req.stream);
        assert_eq!(req.max_tokens, Some(512));
        assert_eq!(req.temperature, Some(0.5));
        assert_eq!(req.user.as_deref(), Some("u1"));
        // instructions -> system message, first
        assert_eq!(req.messages[0].role, Role::System);
        assert_eq!(req.messages[0].content.as_ref().unwrap().text(), "be terse");
        // user parts: text + image kept, input_file dropped
        let user = &req.messages[1];
        assert_eq!(user.role, Role::User);
        match user.content.as_ref().unwrap() {
            MessageContent::Parts(parts) => assert_eq!(parts.len(), 2),
            _ => panic!("expected parts"),
        }
        // reasoning skipped; function_call -> assistant tool_call
        let asst = &req.messages[2];
        assert_eq!(asst.role, Role::Assistant);
        let tc = &asst.tool_calls.as_ref().unwrap()[0];
        assert_eq!(tc.id, "call_1");
        assert_eq!(tc.function.arguments, "{\"city\":\"SF\"}");
        // function_call_output -> tool message
        let tool = &req.messages[3];
        assert_eq!(tool.role, Role::Tool);
        assert_eq!(tool.tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(tool.content.as_ref().unwrap().text(), "sunny");
        assert_eq!(req.messages.len(), 4);
        // tools: flat -> nested, web_search dropped
        let tools = req.tools.as_ref().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].function.name, "get_weather");
        // tool_choice: responses shape -> CC shape
        assert_eq!(req.tool_choice.unwrap(), json!({"type": "function", "function": {"name": "get_weather"}}));
        // text.format -> response_format
        assert_eq!(req.response_format.unwrap(), json!({"type": "json_object"}));
        // extras whitelist
        assert_eq!(req.extra.get("parallel_tool_calls").unwrap(), &json!(false));
        assert!(req.extra.get("store").is_none());
    }

    #[test]
    fn inbound_string_input_and_defaults() {
        let t = ResponsesInbound::new();
        let req = t.transform_request(br#"{"model":"m","input":"hello"}"#).unwrap();
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0].role, Role::User);
        assert_eq!(req.messages[0].content.as_ref().unwrap().text(), "hello");
        assert!(!req.stream);
        assert!(req.tools.is_none());
    }

    #[test]
    fn inbound_rejects_stateful_features() {
        let t = ResponsesInbound::new();
        assert!(t
            .transform_request(br#"{"model":"m","input":"x","background":true}"#)
            .is_err());
        assert!(t
            .transform_request(br#"{"model":"m","input":"x","previous_response_id":"resp_1"}"#)
            .is_err());
    }

    // ---------- inbound response encoding ----------

    fn unified_response() -> Response {
        Response {
            id: "chatcmpl-1".into(),
            model: "gpt-5".into(),
            choices: vec![Choice {
                index: 0,
                message: Message {
                    role: Role::Assistant,
                    content: Some(MessageContent::Text("hello there".into())),
                    name: None,
                    tool_calls: Some(vec![ToolCall {
                        id: "call_9".into(),
                        kind: "function".into(),
                        index: None,
                        function: FunctionCall { name: "f".into(), arguments: "{\"a\":1}".into() },
                    }]),
                    tool_call_id: None,
                },
                finish_reason: Some("tool_calls".into()),
            }],
            usage: Some(Usage {
                prompt_tokens: 11,
                completion_tokens: 7,
                total_tokens: 18,
                cached_tokens: Some(3),
                cache_write_tokens: None,
                reasoning_tokens: Some(2),
                extra: Default::default(),
            }),
            extra: Map::from_iter([("created".into(), json!(123))]),
        }
    }

    #[test]
    fn inbound_response_encodes_responses_object() {
        let t = ResponsesInbound::new();
        let body = t.transform_response(&unified_response()).unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["object"], "response");
        assert_eq!(v["status"], "completed");
        assert_eq!(v["created_at"], 123);
        let output = v["output"].as_array().unwrap();
        assert_eq!(output[0]["type"], "message");
        assert_eq!(output[0]["content"][0]["type"], "output_text");
        assert_eq!(output[0]["content"][0]["text"], "hello there");
        assert_eq!(output[1]["type"], "function_call");
        assert_eq!(output[1]["call_id"], "call_9");
        assert_eq!(output[1]["arguments"], "{\"a\":1}");
        let u = &v["usage"];
        assert_eq!(u["input_tokens"], 11);
        assert_eq!(u["output_tokens"], 7);
        assert_eq!(u["input_tokens_details"]["cached_tokens"], 3);
        assert_eq!(u["output_tokens_details"]["reasoning_tokens"], 2);
    }

    #[test]
    fn inbound_response_length_maps_incomplete() {
        let t = ResponsesInbound::new();
        let mut r = unified_response();
        r.choices[0].finish_reason = Some("length".into());
        let v: Value = serde_json::from_slice(&t.transform_response(&r).unwrap()).unwrap();
        assert_eq!(v["status"], "incomplete");
        assert_eq!(v["incomplete_details"]["reason"], "max_output_tokens");
    }

    // ---------- inbound stream (unified chunks -> responses events) ----------

    fn chunk(content: Option<&str>, tool: Option<ToolCall>, finish: Option<&str>, usage: Option<Usage>) -> StreamChunk {
        StreamChunk {
            id: "chatcmpl-s".into(),
            model: "gpt-5".into(),
            choices: vec![StreamChoice {
                index: 0,
                delta: Delta {
                    role: None,
                    content: content.map(str::to_string),
                    tool_calls: tool.map(|t| vec![t]),
                },
                finish_reason: finish.map(str::to_string),
            }],
            usage,
            extra: Map::from_iter([("created".into(), json!(42))]),
        }
    }

    #[test]
    fn inbound_stream_event_sequence() {
        let t = ResponsesInbound::new();
        let mut evs = Vec::new();
        evs.extend(t.transform_stream_chunk(&chunk(Some("Hel"), None, None, None)).unwrap());
        evs.extend(t.transform_stream_chunk(&chunk(Some("lo"), None, None, None)).unwrap());
        evs.extend(
            t.transform_stream_chunk(&chunk(
                None,
                Some(ToolCall {
                    id: "call_1".into(),
                    kind: "function".into(),
                    index: None,
                    function: FunctionCall { name: "f".into(), arguments: String::new() },
                }),
                None,
                None,
            ))
            .unwrap(),
        );
        // arguments delta with empty id -> most recent open call
        evs.extend(
            t.transform_stream_chunk(&chunk(
                None,
                Some(ToolCall {
                    id: String::new(),
                    kind: "function".into(),
                    index: None,
                    function: FunctionCall { name: String::new(), arguments: "{\"x\":1}".into() },
                }),
                None,
                None,
            ))
            .unwrap(),
        );
        evs.extend(
            t.transform_stream_chunk(&chunk(
                None,
                None,
                Some("tool_calls"),
                Some(Usage { prompt_tokens: 5, completion_tokens: 9, total_tokens: 14, ..Default::default() }),
            ))
            .unwrap(),
        );
        evs.extend(t.stream_end());

        let names: Vec<&str> = evs.iter().map(|e| e.event.as_deref().unwrap()).collect();
        assert_eq!(
            names,
            vec![
                "response.created",
                "response.in_progress",
                "response.output_item.added",
                "response.content_part.added",
                "response.output_text.delta",
                "response.output_text.delta",
                "response.output_item.added",
                "response.function_call_arguments.delta",
                "response.output_text.done",
                "response.content_part.done",
                "response.output_item.done",
                "response.function_call_arguments.done",
                "response.output_item.done",
                "response.completed",
            ]
        );
        // sequence_number strictly increasing
        let seqs: Vec<u64> = evs
            .iter()
            .map(|e| serde_json::from_str::<Value>(&e.data).unwrap()["sequence_number"].as_u64().unwrap())
            .collect();
        assert!(seqs.windows(2).all(|w| w[0] < w[1]));
        // text accumulated; args done carries full arguments
        let text_done: Value = serde_json::from_str(&evs[8].data).unwrap();
        assert_eq!(text_done["text"], "Hello");
        let args_done: Value = serde_json::from_str(&evs[11].data).unwrap();
        assert_eq!(args_done["arguments"], "{\"x\":1}");
        // completed carries output + usage
        let completed: Value = serde_json::from_str(&evs[13].data).unwrap();
        let resp = &completed["response"];
        assert_eq!(resp["status"], "completed");
        assert_eq!(resp["output"].as_array().unwrap().len(), 2);
        assert_eq!(resp["usage"]["input_tokens"], 5);
        assert_eq!(resp["usage"]["output_tokens"], 9);
        // indices: text item 0, fn item 1
        assert_eq!(resp["output"][0]["type"], "message");
        assert_eq!(resp["output"][1]["type"], "function_call");
    }

    /// Regression: parallel tool calls interleave argument deltas keyed by
    /// CC delta index; each must land in its own function_call item.
    #[test]
    fn inbound_stream_parallel_tool_calls_route_by_index() {
        let t = ResponsesInbound::new();
        let tool_chunk = |tc: ToolCall| {
            let mut c = chunk(None, None, None, None);
            c.choices[0].delta.tool_calls = Some(vec![tc]);
            c
        };
        let tc = |id: &str, index: u32, name: &str, args: &str| ToolCall {
            id: id.into(),
            kind: "function".into(),
            index: Some(index),
            function: FunctionCall { name: name.into(), arguments: args.into() },
        };
        let mut evs = Vec::new();
        evs.extend(t.transform_stream_chunk(&tool_chunk(tc("call_a", 0, "fa", ""))).unwrap());
        evs.extend(t.transform_stream_chunk(&tool_chunk(tc("call_b", 1, "fb", ""))).unwrap());
        // interleaved continuation deltas, no id — routed purely by index
        evs.extend(t.transform_stream_chunk(&tool_chunk(tc("", 1, "", "{\"y\":"))).unwrap());
        evs.extend(t.transform_stream_chunk(&tool_chunk(tc("", 0, "", "{\"x\":"))).unwrap());
        evs.extend(t.transform_stream_chunk(&tool_chunk(tc("", 1, "", "2}"))).unwrap());
        evs.extend(t.transform_stream_chunk(&tool_chunk(tc("", 0, "", "1}"))).unwrap());
        evs.extend(t.stream_end());

        let arg_dones: Vec<Value> = evs
            .iter()
            .filter(|e| e.event.as_deref() == Some("response.function_call_arguments.done"))
            .map(|e| serde_json::from_str(&e.data).unwrap())
            .collect();
        assert_eq!(arg_dones.len(), 2);
        // fc items close in open order: fc_0 = call_a, fc_1 = call_b
        assert_eq!(arg_dones[0]["item_id"], "fc_0");
        assert_eq!(arg_dones[0]["arguments"], "{\"x\":1}");
        assert_eq!(arg_dones[1]["item_id"], "fc_1");
        assert_eq!(arg_dones[1]["arguments"], "{\"y\":2}");

        let completed: Value = serde_json::from_str(&evs.last().unwrap().data).unwrap();
        let output = completed["response"]["output"].as_array().unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(output[0]["call_id"], "call_a");
        assert_eq!(output[1]["call_id"], "call_b");
        assert_eq!(output[1]["arguments"], "{\"y\":2}");
    }

    #[test]
    fn inbound_stream_end_without_data_is_empty() {
        let t = ResponsesInbound::new();
        assert!(t.stream_end().is_empty());
    }

    // ---------- outbound request building ----------

    fn unified_request() -> Request {
        Request {
            model: "gpt-5".into(),
            messages: vec![
                text_msg(Role::System, "be terse"),
                text_msg(Role::User, "hi"),
                Message {
                    role: Role::Assistant,
                    content: Some(MessageContent::Text("sure".into())),
                    name: None,
                    tool_calls: Some(vec![ToolCall {
                        id: "call_1".into(),
                        kind: "function".into(),
                        index: None,
                        function: FunctionCall { name: "f".into(), arguments: "{\"a\":1}".into() },
                    }]),
                    tool_call_id: None,
                },
                Message {
                    role: Role::Tool,
                    content: Some(MessageContent::Text("done".into())),
                    name: None,
                    tool_calls: None,
                    tool_call_id: Some("call_1".into()),
                },
            ],
            stream: true,
            max_tokens: Some(64),
            temperature: Some(0.7),
            top_p: None,
            stop: Some(vec!["END".into()]),
            tools: Some(vec![Tool {
                kind: "function".into(),
                function: FunctionDef {
                    name: "f".into(),
                    description: Some("d".into()),
                    parameters: Some(json!({"type": "object"})),
                },
            }]),
            tool_choice: Some(json!({"type": "function", "function": {"name": "f"}})),
            response_format: Some(json!({"type": "json_object"})),
            user: Some("u1".into()),
            extra: Map::from_iter([("parallel_tool_calls".into(), json!(true))]),
        }
    }

    #[test]
    fn outbound_builds_responses_request() {
        let t = ResponsesOutbound::new();
        let out = t
            .build_request(&unified_request(), &Credentials { api_key: "k".into() })
            .unwrap();
        assert_eq!(out.path, "/responses");
        assert!(out.headers.iter().any(|(k, v)| k == "Authorization" && v == "Bearer k"));
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["model"], "gpt-5");
        assert_eq!(v["stream"], true);
        assert_eq!(v["instructions"], "be terse");
        assert_eq!(v["max_output_tokens"], 64);
        assert_eq!(v["parallel_tool_calls"], true);
        assert!(v.get("stop").is_none(), "stop has no Responses counterpart");
        let input = v["input"].as_array().unwrap();
        assert_eq!(input[0]["role"], "user");
        assert_eq!(input[0]["content"][0]["type"], "input_text");
        assert_eq!(input[1]["role"], "assistant");
        assert_eq!(input[1]["content"][0]["type"], "output_text");
        assert_eq!(input[2]["type"], "function_call");
        assert_eq!(input[2]["call_id"], "call_1");
        assert_eq!(input[3]["type"], "function_call_output");
        assert_eq!(input[3]["output"], "done");
        assert_eq!(input.len(), 4);
        // tools nested -> flat
        assert_eq!(v["tools"][0]["type"], "function");
        assert_eq!(v["tools"][0]["name"], "f");
        assert!(v["tools"][0].get("function").is_none());
        // tool_choice CC -> responses shape
        assert_eq!(v["tool_choice"], json!({"type": "function", "name": "f"}));
        // response_format -> text.format
        assert_eq!(v["text"]["format"], json!({"type": "json_object"}));
    }

    // ---------- outbound response parsing ----------

    #[test]
    fn outbound_parses_responses_object() {
        let t = ResponsesOutbound::new();
        let body = json!({
            "id": "resp_1", "object": "response", "created_at": 99, "status": "completed",
            "model": "gpt-5",
            "output": [
                {"id": "msg_0", "type": "message", "role": "assistant", "status": "completed",
                 "content": [{"type": "output_text", "text": "hi "}, {"type": "output_text", "text": "there"}]},
                {"id": "fc_0", "type": "function_call", "call_id": "call_7",
                 "name": "f", "arguments": "{\"b\":2}", "status": "completed"}
            ],
            "usage": {"input_tokens": 10, "output_tokens": 4, "total_tokens": 14,
                      "input_tokens_details": {"cached_tokens": 1},
                      "output_tokens_details": {"reasoning_tokens": 0}}
        });
        let resp = t.transform_response(serde_json::to_string(&body).unwrap().as_bytes()).unwrap();
        assert_eq!(resp.id, "resp_1");
        assert_eq!(resp.choices[0].finish_reason.as_deref(), Some("tool_calls"));
        assert_eq!(resp.choices[0].message.content.as_ref().unwrap().text(), "hi there");
        let tc = &resp.choices[0].message.tool_calls.as_ref().unwrap()[0];
        assert_eq!(tc.id, "call_7");
        assert_eq!(tc.function.arguments, "{\"b\":2}");
        let u = resp.usage.unwrap();
        assert_eq!(u.prompt_tokens, 10);
        assert_eq!(u.completion_tokens, 4);
        assert_eq!(u.cached_tokens, Some(1));
        assert_eq!(resp.extra.get("created").unwrap(), &json!(99));
    }

    // ---------- outbound stream (responses events -> unified chunks) ----------

    fn resp_event(ty: &str, payload: Value) -> SseEvent {
        let mut p = payload;
        p["type"] = json!(ty);
        SseEvent::named(ty, p.to_string())
    }

    #[test]
    fn outbound_stream_event_sequence() {
        let t = ResponsesOutbound::new();
        let mut chunks = Vec::new();
        let evs = vec![
            resp_event("response.created", json!({"response": {"id": "resp_s", "model": "gpt-5", "created_at": 7, "status": "in_progress"}})),
            resp_event("response.output_text.delta", json!({"item_id": "msg_0", "output_index": 0, "delta": "Hel"})),
            resp_event("response.output_item.added", json!({"output_index": 1, "item": {"id": "fc_0", "type": "function_call", "call_id": "call_3", "name": "f", "arguments": ""}})),
            resp_event("response.function_call_arguments.delta", json!({"item_id": "fc_0", "output_index": 1, "delta": "{\"q\":"})),
            resp_event("response.function_call_arguments.delta", json!({"item_id": "fc_0", "output_index": 1, "delta": "1}"})),
            resp_event("response.completed", json!({"response": {"id": "resp_s", "model": "gpt-5", "status": "completed", "usage": {"input_tokens": 3, "output_tokens": 6, "total_tokens": 9}}})),
        ];
        for e in &evs {
            chunks.extend(t.transform_stream_event(e).unwrap());
        }
        assert!(t.is_stream_end(evs.last().unwrap()));
        assert!(!t.is_stream_end(&evs[1]));

        assert_eq!(chunks.len(), 6);
        assert_eq!(chunks[0].choices[0].delta.role, Some(Role::Assistant));
        assert_eq!(chunks[0].id, "resp_s");
        assert_eq!(chunks[1].choices[0].delta.content.as_deref(), Some("Hel"));
        let open = chunks[2].choices[0].delta.tool_calls.as_ref().unwrap();
        assert_eq!(open[0].id, "call_3");
        assert_eq!(open[0].function.name, "f");
        let d1 = &chunks[3].choices[0].delta.tool_calls.as_ref().unwrap()[0];
        assert_eq!(d1.id, "call_3");
        assert_eq!(d1.function.arguments, "{\"q\":");
        let fin = &chunks[5];
        assert_eq!(fin.choices[0].finish_reason.as_deref(), Some("tool_calls"));
        let u = fin.usage.as_ref().unwrap();
        assert_eq!(u.prompt_tokens, 3);
        assert_eq!(u.completion_tokens, 6);
        assert_eq!(fin.extra.get("created").unwrap(), &json!(7));
    }

    #[test]
    fn outbound_stream_completed_text_finish_is_stop() {
        let t = ResponsesOutbound::new();
        t.transform_stream_event(&resp_event("response.created", json!({"response": {"id": "r", "model": "m"}})))
            .unwrap();
        let chunks = t
            .transform_stream_event(&resp_event("response.completed", json!({"response": {"id": "r", "model": "m", "usage": null}})))
            .unwrap();
        assert_eq!(chunks[0].choices[0].finish_reason.as_deref(), Some("stop"));
        assert!(chunks[0].usage.is_none());
    }

    // ---------- errors ----------

    #[test]
    fn error_envelope_matches_openai_shape() {
        let t = ResponsesInbound::new();
        let body = t.transform_error(&ErrorResponse {
            message: "boom".into(),
            kind: Some("server_error".into()),
            code: None,
            status: Some(500),
        });
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"]["message"], "boom");
        assert_eq!(v["error"]["type"], "server_error");

        let o = ResponsesOutbound::new();
        let err = o.extract_error(429, br#"{"error":{"message":"slow down","type":"rate_limit"}}"#);
        assert_eq!(err.message, "slow down");
        assert_eq!(err.status, Some(429));
    }
}
