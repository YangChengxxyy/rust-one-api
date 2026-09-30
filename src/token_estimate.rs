//! Fallback token estimation used for billing when an upstream response or
//! stream carries no usage. This is a heuristic, NOT exact tokenization:
//! costs computed from estimated usage are approximate.

use llm::{MessageContent, Request, ToolCall, Usage};

/// Cap on accumulated completion text during streaming (bytes), bounding
/// memory for pathological streams. Accumulation beyond the cap is dropped,
/// undercounting the estimate (noted in the fallback warn log).
pub const COMPLETION_ACCUMULATOR_CAP: usize = 256 * 1024;

/// Heuristic token count: `ceil(ascii_chars / 4 + cjk_chars)`.
///
/// ASCII-dominant text is ~4 chars/token; CJK (roughly anything at or above
/// U+2E80, which also covers full-width forms and Hangul/Kana ranges) is
/// closer to one token per character, so those count fully.
pub fn estimate_tokens(text: &str) -> u64 {
    let mut ascii = 0u64;
    let mut cjk = 0u64;
    for ch in text.chars() {
        if ch >= '\u{2e80}' {
            cjk += 1;
        } else {
            ascii += 1;
        }
    }
    (ascii + 3) / 4 + cjk
}

/// Estimated prompt tokens for a request: message text content, tool-call
/// function name/arguments JSON, plus a 3-token per-message overhead
/// (chat-format wrapper, mirrors common tokenizers' per-message framing).
pub fn estimate_request_prompt(req: &Request) -> u64 {
    let mut total = 0u64;
    for msg in &req.messages {
        if let Some(content) = &msg.content {
            total += match content {
                MessageContent::Text(t) => estimate_tokens(t),
                MessageContent::Parts(parts) => parts
                    .iter()
                    .map(|p| match p {
                        llm::ContentPart::Text { text } => estimate_tokens(text),
                        _ => 0,
                    })
                    .sum(),
            };
        }
        if let Some(calls) = &msg.tool_calls {
            total += calls.iter().map(|c| estimate_tool_call(c)).sum::<u64>();
        }
        total += 3;
    }
    if let Some(tools) = &req.tools {
        for tool in tools {
            total += estimate_tokens(&tool.function.name);
            if let Some(d) = &tool.function.description {
                total += estimate_tokens(d);
            }
            if let Some(p) = &tool.function.parameters {
                if let Ok(s) = serde_json::to_string(p) {
                    total += estimate_tokens(&s);
                }
            }
            total += 3;
        }
    }
    total
}

fn estimate_tool_call(c: &ToolCall) -> u64 {
    estimate_tokens(&c.function.name) + estimate_tokens(&c.function.arguments)
}
/// Appends `s` to `buf`, capping total length at [`COMPLETION_ACCUMULATOR_CAP`]
/// bytes (truncation at a char boundary); overflow is silently dropped, so
/// later estimates undercount — acceptable for a fallback estimate.
pub fn push_capped(buf: &mut String, s: &str) {
    if buf.len() >= COMPLETION_ACCUMULATOR_CAP {
        return;
    }
    buf.push_str(s);
    if buf.len() > COMPLETION_ACCUMULATOR_CAP {
        let mut end = COMPLETION_ACCUMULATOR_CAP;
        while end > 0 && !buf.is_char_boundary(end) {
            end -= 1;
        }
        buf.truncate(end);
    }
}

/// Billing fallback: if the upstream reported usage (any non-zero field),
/// use it verbatim; otherwise synthesize from the estimated prompt and the
/// accumulated completion text (which is itself an estimate).
pub fn final_usage(seen: Option<&Usage>, req: &Request, completion_text: &str) -> Usage {
    if let Some(u) = seen {
        let non_zero =
            u.prompt_tokens > 0 || u.completion_tokens > 0 || u.total_tokens > 0;
        if non_zero {
            return u.clone();
        }
    }
    let prompt = estimate_request_prompt(req);
    let completion = estimate_tokens(completion_text);
    Usage {
        prompt_tokens: prompt,
        completion_tokens: completion,
        total_tokens: prompt + completion,
        ..Usage::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use llm::{FunctionCall, Message};

    #[test]
    fn ascii_roughly_four_chars_per_token() {
        assert_eq!(estimate_tokens(""), 0);
        assert_eq!(estimate_tokens("abcd"), 1);
        assert_eq!(estimate_tokens("abc"), 1); // ceil
        let s = "a".repeat(400);
        assert_eq!(estimate_tokens(&s), 100);
    }

    #[test]
    fn cjk_counts_per_character() {
        // Same char count, CJK weighs ~4x heavier.
        let ascii = "aaaaaaaa";
        let cjk = "你好语言模型";
        assert_eq!(estimate_tokens(ascii), 2);
        assert_eq!(estimate_tokens(cjk), 6);
        // Mixed: 4 ascii + 2 CJK = 1 + 2.
        assert_eq!(estimate_tokens("abcd你好"), 3);
    }

    #[test]
    fn request_prompt_includes_messages_and_overhead() {
        let req: Request = serde_json::from_value(serde_json::json!({
            "model": "m",
            "messages": [
                {"role": "user", "content": "12345678"}  // 2 tokens + 3 overhead
            ]
        }))
        .unwrap();
        assert_eq!(estimate_request_prompt(&req), 5);
    }

    #[test]
    fn request_prompt_includes_tool_calls_and_defs() {
        let msg = Message {
            role: llm::Role::Assistant,
            content: None,
            name: None,
            tool_calls: Some(vec![ToolCall {
                id: "t".into(),
                kind: "function".into(),
                index: None,
                function: FunctionCall {
                    name: "do".into(),          // 2 chars -> 1
                    arguments: "{\"a\":1}".into(), // 8 chars -> 2
                },
            }]),
            tool_call_id: None,
        };
        let req = Request {
            model: "m".into(),
            messages: vec![msg],
            stream: false,
            max_tokens: None,
            temperature: None,
            top_p: None,
            stop: None,
            tools: None,
            tool_choice: None,
            response_format: None,
            user: None,
            extra: Default::default(),
        };
        // message: 1 + 2 tokens + 3 overhead
        assert_eq!(estimate_request_prompt(&req), 6);
    }

    #[test]
    fn final_usage_prefers_reported_usage() {
        let req: Request = serde_json::from_value(serde_json::json!({
            "model": "m",
            "messages": [{"role": "user", "content": "12345678"}]
        }))
        .unwrap();
        let seen = Usage {
            prompt_tokens: 10,
            completion_tokens: 20,
            total_tokens: 30,
            ..Default::default()
        };
        let u = final_usage(Some(&seen), &req, "ignored");
        assert_eq!((u.prompt_tokens, u.completion_tokens, u.total_tokens), (10, 20, 30));
    }

    #[test]
    fn final_usage_synthesizes_when_missing() {
        let req: Request = serde_json::from_value(serde_json::json!({
            "model": "m",
            "messages": [{"role": "user", "content": "12345678"}] // 2 + 3
        }))
        .unwrap();
        let u = final_usage(None, &req, "abcdefgh"); // 2
        assert_eq!(u.prompt_tokens, 5);
        assert_eq!(u.completion_tokens, 2);
        assert_eq!(u.total_tokens, 7);
        // Zero-usage seen value also falls back.
        let u = final_usage(Some(&Usage::default()), &req, "");
        assert_eq!(u.prompt_tokens, 5);
        assert_eq!(u.completion_tokens, 0);
    }
}
