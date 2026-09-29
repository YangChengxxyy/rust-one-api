//! Google Gemini generateContent wire format.

use crate::error::TransformError;
use crate::sse::SseEvent;
use crate::transformer::{Credentials, InboundTransformer, OutboundRequest, OutboundTransformer};
use crate::{Choice, ContentPart, Delta, ErrorResponse, FunctionCall, FunctionDef, ImageUrl, Message, MessageContent, Request, Response, Role, StreamChoice, StreamChunk, Tool, ToolCall, Usage};
use serde_json::{json, Map, Value};

pub const FORMAT: &str = "gemini/models";

// ---------------- helpers ----------------

fn finish_reason_from_gemini(r: Option<&str>) -> Option<String> {
    r.map(|r| match r {
        "STOP" => "stop".to_string(),
        "MAX_TOKENS" => "length".to_string(),
        "SAFETY" => "content_filter".to_string(),
        other => other.to_string(),
    })
}

fn finish_reason_to_gemini(r: Option<&str>) -> String {
    match r {
        Some("length") => "MAX_TOKENS".to_string(),
        Some("content_filter") => "SAFETY".to_string(),
        _ => "STOP".to_string(),
    }
}

fn usage_from_gemini(v: &Value) -> Usage {
    let g = |k: &str| v.get(k).and_then(Value::as_u64);
    Usage {
        prompt_tokens: g("promptTokenCount").unwrap_or(0),
        completion_tokens: g("candidatesTokenCount").unwrap_or(0),
        total_tokens: g("totalTokenCount").unwrap_or(0),
        cached_tokens: g("cachedContentTokenCount"),
        cache_write_tokens: None,
        reasoning_tokens: g("thoughtsTokenCount"),
        extra: Default::default(),
    }
}

fn usage_to_gemini(u: &Usage) -> Value {
    let mut m = Map::new();
    m.insert("promptTokenCount".into(), json!(u.prompt_tokens));
    m.insert("candidatesTokenCount".into(), json!(u.completion_tokens));
    m.insert("totalTokenCount".into(), json!(u.total_tokens));
    if let Some(c) = u.cached_tokens {
        m.insert("cachedContentTokenCount".into(), json!(c));
    }
    if let Some(r) = u.reasoning_tokens {
        m.insert("thoughtsTokenCount".into(), json!(r));
    }
    Value::Object(m)
}

/// unified Message -> Gemini content {role, parts}
fn message_to_content(msg: &Message) -> Value {
    let role = match msg.role {
        Role::Assistant => "model",
        Role::Tool => "user", // functionResponse is sent by "user"
        _ => "user",
    };
    let mut parts: Vec<Value> = Vec::new();
    match msg.content.as_ref() {
        Some(MessageContent::Text(t)) if !t.is_empty() => parts.push(json!({"text": t})),
        Some(MessageContent::Parts(ps)) => {
            for p in ps {
                match p {
                    ContentPart::Text { text } => parts.push(json!({"text": text})),
                    ContentPart::ImageUrl { image_url } => {
                        // inlineData only supports base64 payloads
                        if let Some(rest) = image_url.url.strip_prefix("data:") {
                            if let Some((mt, data)) = rest.split_once(";base64,") {
                                parts.push(json!({"inlineData": {"mimeType": mt, "data": data}}));
                            }
                        }
                    }
                    ContentPart::Unknown => {}
                }
            }
        }
        _ => {}
    }
    if let Some(tcs) = msg.tool_calls.as_ref() {
        for tc in tcs {
            let args: Value = serde_json::from_str(&tc.function.arguments).unwrap_or(json!({}));
            parts.push(json!({"functionCall": {"name": tc.function.name, "args": args}}));
        }
    }
    json!({"role": role, "parts": parts})
}

/// unified Request -> Gemini generateContent body.
fn unified_to_gemini_body(req: &Request) -> Value {
    let mut system_text: Vec<String> = Vec::new();
    let mut contents: Vec<Value> = Vec::new();
    for m in &req.messages {
        match m.role {
            Role::System | Role::Developer => {
                if let Some(c) = m.content.as_ref() {
                    let t = c.text();
                    if !t.is_empty() {
                        system_text.push(t);
                    }
                }
            }
            Role::Tool => {
                // role:tool message -> functionResponse part
                contents.push(json!({
                    "role": "user",
                    "parts": [{
                        "functionResponse": {
                            "name": m.name.clone().unwrap_or_default(),
                            "response": {
                                "result": m.content.as_ref().map(|c| Value::String(c.text())).unwrap_or(Value::Null)
                            }
                        }
                    }]
                }));
            }
            role => {
                let c = message_to_content(m);
                if role == Role::Assistant {
                    contents.push(c);
                } else {
                    contents.push(c);
                }
            }
        }
    }
    let mut body = Map::new();
    if !system_text.is_empty() {
        body.insert(
            "systemInstruction".into(),
            json!({"role": "user", "parts": [{"type": "text", "text": system_text.join("\n")}]}),
        );
    }
    body.insert("contents".into(), Value::Array(contents));
    let mut gc = Map::new();
    if let Some(m) = req.max_tokens {
        gc.insert("maxOutputTokens".into(), json!(m));
    }
    if let Some(t) = req.temperature {
        gc.insert("temperature".into(), json!(t));
    }
    if let Some(t) = req.top_p {
        gc.insert("topP".into(), json!(t));
    }
    if let Some(s) = req.stop.as_ref() {
        gc.insert("stopSequences".into(), json!(s));
    }
    if !gc.is_empty() {
        body.insert("generationConfig".into(), Value::Object(gc));
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
                    d.insert("parameters".into(), p.clone());
                }
                Value::Object(d)
            })
            .collect();
        body.insert("tools".into(), json!([{"functionDeclarations": decls}]));
    }
    Value::Object(body)
}

/// Gemini generateContent body -> unified Request. Model comes from URL path
/// upstream, so a missing `model` field is fine (unified model = "").
fn gemini_to_unified(body: &[u8]) -> Result<Request, TransformError> {
    let v: Value = serde_json::from_slice(body).map_err(TransformError::Json)?;
    let mut messages: Vec<Message> = Vec::new();

    if let Some(sys) = v.pointer("/systemInstruction/parts").and_then(Value::as_array) {
        let text: String = sys
            .iter()
            .filter_map(|p| p.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("");
        if !text.is_empty() {
            messages.push(Message {
                role: Role::System,
                content: Some(MessageContent::Text(text)),
                name: None,
                tool_calls: None,
                tool_call_id: None,
            });
        }
    }

    for c in v.get("contents").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]) {
        let role = match c.get("role").and_then(Value::as_str) {
            Some("model") => Role::Assistant,
            _ => Role::User,
        };
        let mut msg = Message { role, content: None, name: None, tool_calls: None, tool_call_id: None };
        let mut texts: Vec<String> = Vec::new();
        let mut parts: Vec<ContentPart> = Vec::new();
        for p in c.get("parts").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]) {
            if let Some(t) = p.get("text").and_then(Value::as_str) {
                texts.push(t.to_string());
                parts.push(ContentPart::Text { text: t.to_string() });
            } else if let Some(ind) = p.get("inlineData") {
                parts.push(ContentPart::ImageUrl {
                    image_url: ImageUrl {
                        url: format!(
                            "data:{};base64,{}",
                            ind.get("mimeType").and_then(Value::as_str).unwrap_or("image/png"),
                            ind.get("data").and_then(Value::as_str).unwrap_or_default()
                        ),
                        detail: None,
                    },
                });
            } else if let Some(fc) = p.get("functionCall") {
                let args = fc.get("args").cloned().unwrap_or(json!({}));
                let calls = msg.tool_calls.get_or_insert_with(Vec::new);
                let id = format!("call_{}", calls.len());
                calls.push(ToolCall {
                    id,
                    kind: "function".into(),
                    function: FunctionCall {
                        name: fc.get("name").and_then(Value::as_str).unwrap_or_default().to_string(),
                        arguments: serde_json::to_string(&args).unwrap_or_else(|_| "{}".into()),
                    },
                });
            } else if let Some(fr) = p.get("functionResponse") {
                messages.push(Message {
                    role: Role::Tool,
                    content: Some(MessageContent::Text(
                        serde_json::to_string(&fr.get("response").cloned().unwrap_or(Value::Null))
                            .unwrap_or_default(),
                    )),
                    name: fr.get("name").and_then(Value::as_str).map(str::to_string),
                    tool_calls: None,
                    tool_call_id: None,
                });
            }
        }
        if parts.iter().all(|p| matches!(p, ContentPart::Text { .. })) && !parts.is_empty() {
            msg.content = Some(MessageContent::Text(texts.join("")));
        } else if !parts.is_empty() {
            msg.content = Some(MessageContent::Parts(parts));
        }
        if msg.content.is_some() || msg.tool_calls.is_some() {
            messages.push(msg);
        }
    }

    let gc = v.get("generationConfig").cloned().unwrap_or(json!({}));
    Ok(Request {
        model: v.get("model").and_then(Value::as_str).unwrap_or("").to_string(),
        messages,
        stream: false, // gemini streams via a different endpoint
        max_tokens: gc.get("maxOutputTokens").and_then(Value::as_u64).map(|x| x as u32),
        temperature: gc.get("temperature").and_then(Value::as_f64).map(|x| x as f32),
        top_p: gc.get("topP").and_then(Value::as_f64).map(|x| x as f32),
        stop: gc.get("stopSequences").and_then(Value::as_array).map(|a| {
            a.iter().filter_map(Value::as_str).map(str::to_string).collect()
        }),
        tools: v.pointer("/tools/0/functionDeclarations").and_then(Value::as_array).map(|decls| {
            decls
                .iter()
                .filter_map(|d| {
                    Some(Tool {
                        kind: "function".into(),
                        function: FunctionDef {
                            name: d.get("name")?.as_str()?.to_string(),
                            description: d.get("description").and_then(Value::as_str).map(str::to_string),
                            parameters: d.get("parameters").cloned(),
                        },
                    })
                })
                .collect()
        }),
        tool_choice: None,
        response_format: gc.get("responseMimeType").cloned().map(|m| json!({"type": m})),
        user: None,
        extra: Default::default(),
    })
}

/// Gemini GenerateContentResponse JSON -> unified StreamChunk.
fn gemini_chunk_to_unified(v: &Value) -> StreamChunk {
    let mut delta = Delta::default();
    let mut finish = None;
    if let Some(cand) = v.pointer("/candidates/0") {
        let mut texts: Vec<String> = Vec::new();
        let mut tool_calls: Vec<ToolCall> = Vec::new();
        for p in cand.pointer("/content/parts").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]) {
            if let Some(t) = p.get("text").and_then(Value::as_str) {
                texts.push(t.to_string());
            } else if let Some(fc) = p.get("functionCall") {
                let id = format!("call_{}", tool_calls.len());
                tool_calls.push(ToolCall {
                    id,
                    kind: "function".into(),
                    function: FunctionCall {
                        name: fc.get("name").and_then(Value::as_str).unwrap_or_default().to_string(),
                        arguments: serde_json::to_string(&fc.get("args").cloned().unwrap_or(json!({})))
                            .unwrap_or_else(|_| "{}".into()),
                    },
                });
            }
        }
        if !texts.is_empty() {
            delta.content = Some(texts.join(""));
        }
        if !tool_calls.is_empty() {
            delta.tool_calls = Some(tool_calls);
        }
        delta.role = Some(Role::Assistant);
        finish = finish_reason_from_gemini(cand.get("finishReason").and_then(Value::as_str));
    }
    StreamChunk {
        id: v.get("responseId").and_then(Value::as_str).unwrap_or_default().to_string(),
        model: String::new(),
        choices: vec![StreamChoice { index: 0, delta, finish_reason: finish }],
        usage: v.get("usageMetadata").map(usage_from_gemini),
        extra: Default::default(),
    }
}

/// unified StreamChunk -> Gemini GenerateContentResponse chunk JSON.
fn unified_chunk_to_gemini(chunk: &StreamChunk) -> Value {
    let mut parts: Vec<Value> = Vec::new();
    let mut finish = None;
    if let Some(c) = chunk.choices.first() {
        if let Some(t) = c.delta.content.as_deref() {
            if !t.is_empty() {
                parts.push(json!({"text": t}));
            }
        }
        for tc in c.delta.tool_calls.as_deref().unwrap_or(&[]) {
            let args: Value = serde_json::from_str(&tc.function.arguments).unwrap_or(json!({}));
            parts.push(json!({"functionCall": {"name": tc.function.name, "args": args}}));
        }
        finish = c.finish_reason.as_deref().map(|f| finish_reason_to_gemini(Some(f)));
    }
    let mut cand = Map::new();
    cand.insert("content".into(), json!({"role": "model", "parts": parts}));
    if let Some(f) = finish {
        cand.insert("finishReason".into(), json!(f));
    }
    let mut body = Map::new();
    body.insert("candidates".into(), json!([Value::Object(cand)]));
    if let Some(u) = chunk.usage.as_ref() {
        body.insert("usageMetadata".into(), usage_to_gemini(u));
    }
    Value::Object(body)
}

// ---------------- Inbound (client speaks Gemini) ----------------

#[derive(Debug, Default)]
pub struct GeminiInbound;

impl GeminiInbound {
    pub fn new() -> Self {
        Self
    }
}

impl InboundTransformer for GeminiInbound {
    fn format(&self) -> &'static str {
        FORMAT
    }

    fn transform_request(&self, body: &[u8]) -> Result<Request, TransformError> {
        gemini_to_unified(body)
    }

    fn transform_response(&self, resp: &Response) -> Result<Vec<u8>, TransformError> {
        // unified -> Gemini GenerateContentResponse
        let msg = resp.choices.first().map(|c| &c.message);
        let mut parts: Vec<Value> = Vec::new();
        if let Some(m) = msg {
            match m.content.as_ref() {
                Some(MessageContent::Text(t)) if !t.is_empty() => parts.push(json!({"text": t})),
                Some(MessageContent::Parts(ps)) => {
                    for p in ps {
                        if let ContentPart::Text { text } = p {
                            parts.push(json!({"text": text}));
                        }
                    }
                }
                _ => {}
            }
            for tc in m.tool_calls.as_deref().unwrap_or(&[]) {
                let args: Value = serde_json::from_str(&tc.function.arguments).unwrap_or(json!({}));
                parts.push(json!({"functionCall": {"name": tc.function.name, "args": args}}));
            }
        }
        let mut cand = Map::new();
        cand.insert("content".into(), json!({"role": "model", "parts": parts}));
        if let Some(f) = resp.choices.first().and_then(|c| c.finish_reason.as_deref()) {
            cand.insert("finishReason".into(), json!(finish_reason_to_gemini(Some(f))));
        }
        let mut body = Map::new();
        if let Some(id) = resp.id.strip_prefix("resp-") {
            body.insert("responseId".into(), json!(id));
        }
        body.insert("candidates".into(), json!([Value::Object(cand)]));
        if let Some(u) = resp.usage.as_ref() {
            body.insert("usageMetadata".into(), usage_to_gemini(u));
        }
        serde_json::to_vec(&Value::Object(body)).map_err(TransformError::Json)
    }

    fn transform_stream_chunk(&self, chunk: &StreamChunk) -> Result<Vec<SseEvent>, TransformError> {
        let data = serde_json::to_string(&unified_chunk_to_gemini(chunk)).map_err(TransformError::Json)?;
        Ok(vec![SseEvent::data(data)])
    }

    fn stream_end(&self) -> Vec<SseEvent> {
        vec![] // no sentinel; stream ends with the HTTP body
    }

    fn transform_error(&self, err: &ErrorResponse) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "error": {"code": err.status.unwrap_or(500), "message": err.message, "status": err.kind}
        }))
        .unwrap_or_default()
    }
}

// ---------------- Outbound (upstream is Gemini) ----------------

#[derive(Debug, Default)]
pub struct GeminiOutbound;

impl GeminiOutbound {
    pub fn new() -> Self {
        Self
    }
}

impl OutboundTransformer for GeminiOutbound {
    fn format(&self) -> &'static str {
        FORMAT
    }

    fn build_request(&self, req: &Request, creds: &Credentials) -> Result<OutboundRequest, TransformError> {
        let body = unified_to_gemini_body(req);
        let model = url_encode(&req.model);
        let path = if req.stream {
            format!("/{model}:streamGenerateContent?alt=sse")
        } else {
            format!("/{model}:generateContent")
        };
        Ok(OutboundRequest {
            path,
            headers: vec![
                ("x-goog-api-key".into(), creds.api_key.clone()),
                ("content-type".into(), "application/json".into()),
            ],
            body: serde_json::to_vec(&body).map_err(TransformError::Json)?,
        })
    }

    fn transform_response(&self, body: &[u8]) -> Result<Response, TransformError> {
        let v: Value = serde_json::from_slice(body).map_err(TransformError::Json)?;
        let chunk = gemini_chunk_to_unified(&v);
        let sc = chunk.choices.into_iter().next();
        let message = sc
            .as_ref()
            .map(|c| Message {
                role: Role::Assistant,
                content: if c.delta.content.is_some() {
                    c.delta.content.clone().map(MessageContent::Text)
                } else {
                    None
                },
                name: None,
                tool_calls: c.delta.tool_calls.clone(),
                tool_call_id: None,
            })
            .unwrap_or(Message { role: Role::Assistant, content: None, name: None, tool_calls: None, tool_call_id: None });
        Ok(Response {
            id: chunk.id,
            model: String::new(),
            choices: vec![Choice {
                index: 0,
                message,
                finish_reason: sc.as_ref().and_then(|c| c.finish_reason.clone()),
            }],
            usage: chunk.usage,
            extra: Default::default(),
        })
    }

    fn transform_stream_event(&self, event: &SseEvent) -> Result<Vec<StreamChunk>, TransformError> {
        let v: Value = serde_json::from_str(event.data.trim()).map_err(TransformError::Json)?;
        Ok(vec![gemini_chunk_to_unified(&v)])
    }

    fn is_stream_end(&self, _event: &SseEvent) -> bool {
        false // no sentinel; stream ends with the HTTP body
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
            err.kind = e.get("status").and_then(Value::as_str).map(str::to_string);
            err.code = e.get("code").map(|c| if c.is_string() { c.as_str().unwrap_or_default().to_string() } else { c.to_string() });
        }
        if err.message.is_empty() {
            err.message = format!("upstream error {status}");
        }
        err
    }
}

fn url_encode(s: &str) -> String {
    // model ids in the path: escape characters unsafe in a URL path segment
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transformers::{create_inbound, create_outbound};

    const REQ: &str = r#"{
        "systemInstruction": {"parts": [{"text": "be nice"}]},
        "contents": [
            {"role": "user", "parts": [{"text": "hi"}, {"inlineData": {"mimeType": "image/png", "data": "QUJD"}}]},
            {"role": "model", "parts": [{"functionCall": {"name": "f", "args": {"x": 1}}}]},
            {"role": "user", "parts": [{"functionResponse": {"name": "f", "response": {"result": "ok"}}}]}
        ],
        "generationConfig": {"maxOutputTokens": 64, "temperature": 0.7, "topP": 0.9, "stopSequences": ["END"]},
        "tools": [{"functionDeclarations": [{"name": "f", "description": "d", "parameters": {"type": "object"}}]}]
    }"#;

    #[test]
    fn inbound_parses_gemini_body() {
        let t = GeminiInbound::new();
        let req = t.transform_request(REQ.as_bytes()).unwrap();
        assert_eq!(req.model, ""); // model arrives via URL path
        assert_eq!(req.messages[0].role, Role::System);
        assert_eq!(req.messages[0].content.as_ref().unwrap().text(), "be nice");
        match req.messages[1].content.as_ref().unwrap() {
            MessageContent::Parts(ps) => match &ps[1] {
                ContentPart::ImageUrl { image_url } => {
                    assert_eq!(image_url.url, "data:image/png;base64,QUJD");
                }
                p => panic!("expected image, got {p:?}"),
            },
            c => panic!("{c:?}"),
        }
        let asst = &req.messages[2];
        assert_eq!(asst.tool_calls.as_ref().unwrap()[0].function.arguments, r#"{"x":1}"#);
        let tool = &req.messages[3];
        assert_eq!(tool.role, Role::Tool);
        assert_eq!(tool.name.as_deref(), Some("f"));
        assert_eq!(req.max_tokens, Some(64));
        assert_eq!(req.temperature, Some(0.7));
        assert_eq!(req.stop.as_deref(), Some(&["END".to_string()][..]));
        assert_eq!(req.tools.as_ref().unwrap()[0].function.name, "f");
    }

    #[test]
    fn outbound_builds_generate_content() {
        let t = GeminiOutbound::new();
        let req = GeminiInbound::new().transform_request(REQ.as_bytes()).unwrap();
        let mut req = req;
        req.model = "gemini-2.0-flash".into();
        req.stream = false;
        let out = t.build_request(&req, &Credentials { api_key: "gk".into() }).unwrap();
        assert_eq!(out.path, "/gemini-2.0-flash:generateContent");
        assert!(out.headers.iter().any(|(k, v)| k == "x-goog-api-key" && v == "gk"));
        let v: Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(v["systemInstruction"]["parts"][0]["text"], "be nice");
        assert_eq!(v["generationConfig"]["maxOutputTokens"], 64);
        assert_eq!(v["contents"][1]["parts"][0]["functionCall"]["name"], "f");
        assert_eq!(v["contents"][2]["parts"][0]["functionResponse"]["name"], "f");

        req.stream = true;
        let out = t.build_request(&req, &Credentials { api_key: "gk".into() }).unwrap();
        assert_eq!(out.path, "/gemini-2.0-flash:streamGenerateContent?alt=sse");
    }

    const RESP: &str = r#"{
        "candidates": [{
            "content": {"role": "model", "parts": [
                {"text": "answer"},
                {"functionCall": {"name": "g", "args": {"y": true}}}
            ]},
            "finishReason": "STOP"
        }],
        "usageMetadata": {"promptTokenCount": 9, "candidatesTokenCount": 4, "totalTokenCount": 13,
                          "cachedContentTokenCount": 2, "thoughtsTokenCount": 3}
    }"#;

    #[test]
    fn outbound_maps_response_and_usage() {
        let t = GeminiOutbound::new();
        let r = t.transform_response(RESP.as_bytes()).unwrap();
        assert_eq!(r.choices[0].finish_reason.as_deref(), Some("stop"));
        assert_eq!(r.choices[0].message.content.as_ref().unwrap().text(), "answer");
        let tc = r.choices[0].message.tool_calls.as_ref().unwrap();
        assert_eq!(tc[0].function.arguments, r#"{"y":true}"#);
        let u = r.usage.unwrap();
        assert_eq!(u.prompt_tokens, 9);
        assert_eq!(u.completion_tokens, 4);
        assert_eq!(u.total_tokens, 13);
        assert_eq!(u.cached_tokens, Some(2));
        assert_eq!(u.reasoning_tokens, Some(3));
    }

    #[test]
    fn stream_chunk_roundtrip_to_openai_sse() {
        // gemini chunk -> unified -> openai SSE
        let out = GeminiOutbound::new();
        let ev = SseEvent::data(r#"{"candidates":[{"content":{"role":"model","parts":[{"text":"hey"}]}}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":1,"totalTokenCount":3}}"#);
        let chunks = out.transform_stream_event(&ev).unwrap();
        assert!(!out.is_stream_end(&ev));
        assert_eq!(chunks[0].choices[0].delta.content.as_deref(), Some("hey"));
        assert_eq!(chunks[0].usage.as_ref().unwrap().total_tokens, 3);

        let oa = crate::transformers::openai::OpenAiInbound::new();
        let sse = oa.transform_stream_chunk(&chunks[0]).unwrap();
        let v: Value = serde_json::from_str(&sse[0].data).unwrap();
        assert_eq!(v["choices"][0]["delta"]["content"], "hey");
        assert_eq!(v["usage"]["total_tokens"], 3);
        assert_eq!(oa.stream_end()[0].data, "[DONE]");
    }

    #[test]
    fn inbound_stream_chunk_serializes_gemini() {
        let t = GeminiInbound::new();
        let chunk = StreamChunk {
            id: "r1".into(),
            model: "m".into(),
            choices: vec![StreamChoice {
                index: 0,
                delta: Delta { role: Some(Role::Assistant), content: Some("z".into()), tool_calls: None },
                finish_reason: Some("stop".into()),
            }],
            usage: Some(Usage { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2, ..Default::default() }),
            extra: Default::default(),
        };
        let evs = t.transform_stream_chunk(&chunk).unwrap();
        let v: Value = serde_json::from_str(&evs[0].data).unwrap();
        assert_eq!(v["candidates"][0]["content"]["parts"][0]["text"], "z");
        assert_eq!(v["candidates"][0]["finishReason"], "STOP");
        assert_eq!(v["usageMetadata"]["totalTokenCount"], 2);
        assert!(t.stream_end().is_empty());
    }

    #[test]
    fn extract_error_shape() {
        let t = GeminiOutbound::new();
        let e = t.extract_error(429, br#"{"error":{"code":429,"message":"quota exceeded","status":"RESOURCE_EXHAUSTED"}}"#);
        assert_eq!(e.message, "quota exceeded");
        assert_eq!(e.kind.as_deref(), Some("RESOURCE_EXHAUSTED"));
        assert_eq!(e.code.as_deref(), Some("429"));
    }

    #[test]
    fn factories_resolve() {
        assert!(create_inbound(FORMAT).is_some());
        assert!(create_outbound(FORMAT).is_some());
    }
}
