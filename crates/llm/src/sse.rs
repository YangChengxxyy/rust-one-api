//! Minimal SSE (text/event-stream) codec shared by transformers and the relay.

/// One parsed SSE event. `data` joins multi-line `data:` fields with `\n`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
}

impl SseEvent {
    pub fn data(data: impl Into<String>) -> Self {
        Self { event: None, data: data.into() }
    }

    pub fn named(event: impl Into<String>, data: impl Into<String>) -> Self {
        Self { event: Some(event.into()), data: data.into() }
    }

    /// OpenAI-style terminal sentinel.
    pub fn is_done(&self) -> bool {
        self.data.trim() == "[DONE]"
    }

    /// Serialize to on-the-wire form (ends with a blank line).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = String::new();
        if let Some(ev) = &self.event {
            out.push_str("event: ");
            out.push_str(ev);
            out.push('\n');
        }
        for line in self.data.split('\n') {
            out.push_str("data: ");
            out.push_str(line);
            out.push('\n');
        }
        out.push('\n');
        out.into_bytes()
    }
}

/// Incremental byte-stream parser: feed arbitrary chunks, get complete events.
/// Handles both `\n` and `\r\n` line endings; ignores comment lines (`:...`).
#[derive(Debug, Default)]
pub struct SseParser {
    buf: Vec<u8>,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feed(&mut self, chunk: &[u8]) -> Vec<SseEvent> {
        self.buf.extend_from_slice(chunk);
        let mut events = Vec::new();
        while let Some(pos) = find_event_boundary(&self.buf) {
            let block: Vec<u8> = self.buf.drain(..pos.1).collect();
            if let Some(ev) = parse_block(&block[..pos.0]) {
                events.push(ev);
            }
        }
        events
    }

    /// Flush any trailing unterminated block (call at end of stream).
    pub fn finish(&mut self) -> Vec<SseEvent> {
        let rest = std::mem::take(&mut self.buf);
        match parse_block(&rest) {
            Some(ev) => vec![ev],
            None => Vec::new(),
        }
    }
}

/// Returns (block_end, next_start) for the first blank-line boundary.
fn find_event_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while i < buf.len() {
        match buf[i] {
            b'\n' => {
                if i + 1 < buf.len() && buf[i + 1] == b'\n' {
                    return Some((i, i + 2));
                }
                if i + 2 < buf.len() && buf[i + 1] == b'\r' && buf[i + 2] == b'\n' {
                    return Some((i, i + 3));
                }
                i += 1;
            }
            b'\r' => {
                if i + 3 < buf.len()
                    && buf[i + 1] == b'\n'
                    && buf[i + 2] == b'\r'
                    && buf[i + 3] == b'\n'
                {
                    return Some((i, i + 4));
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    None
}

fn parse_block(block: &[u8]) -> Option<SseEvent> {
    let text = String::from_utf8_lossy(block);
    let mut event = None;
    let mut data_lines: Vec<&str> = Vec::new();
    for raw in text.split('\n') {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event = Some(rest.trim_start().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    if data_lines.is_empty() && event.is_none() {
        return None;
    }
    Some(SseEvent { event, data: data_lines.join("\n") })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_split_across_feeds() {
        let mut p = SseParser::new();
        assert!(p.feed(b"data: {\"a\":1").is_empty());
        let evs = p.feed(b"}\n\ndata: [DONE]\n\n");
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].data, "{\"a\":1}");
        assert!(evs[1].is_done());
    }

    #[test]
    fn handles_named_events_and_crlf() {
        let mut p = SseParser::new();
        let evs = p.feed(b"event: message_start\r\ndata: {}\r\n\r\n: ping\r\n\r\n");
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].event.as_deref(), Some("message_start"));
        assert_eq!(evs[0].data, "{}");
    }

    #[test]
    fn encodes_round_trip() {
        let ev = SseEvent::named("message_delta", "line1\nline2");
        let bytes = ev.encode();
        let mut p = SseParser::new();
        let out = p.feed(&bytes);
        assert_eq!(out, vec![ev]);
    }
}
