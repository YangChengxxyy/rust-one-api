//! Bidirectional transformer contracts.
//!
//! - [`InboundTransformer`]: client wire format -> unified [`Request`], and
//!   unified [`Response`]/[`StreamChunk`] -> client wire format.
//! - [`OutboundTransformer`]: unified -> provider wire format and back.

use crate::{error::TransformError, sse::SseEvent, ErrorResponse, Request, Response, StreamChunk};

/// Upstream credentials (milestone: API key only; OAuth handled upstream later).
#[derive(Debug, Clone)]
pub struct Credentials {
    pub api_key: String,
}

/// A fully-built upstream HTTP request (relative to the channel base_url).
#[derive(Debug, Clone)]
pub struct OutboundRequest {
    /// Path (and query) appended to the channel base URL, e.g. `/v1/chat/completions`.
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub trait InboundTransformer: Send + Sync {
    /// Wire format id, e.g. `openai/chat_completions`, `claude/messages`, `gemini/models`.
    fn format(&self) -> &'static str;

    /// NOTE: instances are per-request (created via factory), so implementations
    /// MAY keep interior-mutable state (e.g. `Mutex`) to handle stateful stream
    /// protocols like Anthropic's content_block lifecycle.

    /// Client request body -> unified request.
    fn transform_request(&self, body: &[u8]) -> Result<Request, TransformError>;

    /// Unified non-stream response -> client wire body.
    fn transform_response(&self, resp: &Response) -> Result<Vec<u8>, TransformError>;

    /// Unified stream chunk -> zero or more client SSE events
    /// (Anthropic emits multiple named events per logical chunk).
    fn transform_stream_chunk(&self, chunk: &StreamChunk) -> Result<Vec<SseEvent>, TransformError>;

    /// Terminal SSE events to close the client stream (e.g. `[DONE]`,
    /// Anthropic `message_stop`). May be empty.
    fn stream_end(&self) -> Vec<SseEvent>;

    /// Unified error -> client wire error body.
    fn transform_error(&self, err: &ErrorResponse) -> Vec<u8>;
}

pub trait OutboundTransformer: Send + Sync {
    /// Provider wire format id (same vocabulary as inbound formats).
    fn format(&self) -> &'static str;

    /// Unified request -> provider HTTP request.
    fn build_request(&self, req: &Request, creds: &Credentials) -> Result<OutboundRequest, TransformError>;

    /// Provider non-stream body -> unified response.
    fn transform_response(&self, body: &[u8]) -> Result<Response, TransformError>;

    /// One provider SSE event -> zero or more unified chunks.
    fn transform_stream_event(&self, event: &SseEvent) -> Result<Vec<StreamChunk>, TransformError>;

    /// Whether this SSE event terminates the provider stream
    /// (OpenAI `[DONE]`, Anthropic `message_stop`, ...).
    fn is_stream_end(&self, event: &SseEvent) -> bool;

    /// Provider error body -> normalized error.
    fn extract_error(&self, status: u16, body: &[u8]) -> ErrorResponse;
}
