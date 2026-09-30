//! Concrete transformer implementations, one submodule per wire format.

pub mod anthropic;
pub mod gemini;
pub mod openai;
pub mod responses;

use crate::transformer::{InboundTransformer, OutboundTransformer};

/// Create an inbound transformer (client wire format -> unified) by format id.
pub fn create_inbound(format: &str) -> Option<Box<dyn InboundTransformer>> {
    match format {
        openai::FORMAT => Some(Box::new(openai::OpenAiInbound::new())),
        responses::FORMAT => Some(Box::new(responses::ResponsesInbound::new())),
        anthropic::FORMAT => Some(Box::new(anthropic::AnthropicInbound::new())),
        gemini::FORMAT => Some(Box::new(gemini::GeminiInbound::new())),
        _ => None,
    }
}

/// Create an outbound transformer (unified -> provider wire format) by format id.
pub fn create_outbound(format: &str) -> Option<Box<dyn OutboundTransformer>> {
    match format {
        openai::FORMAT => Some(Box::new(openai::OpenAiOutbound::new())),
        responses::FORMAT => Some(Box::new(responses::ResponsesOutbound::new())),
        anthropic::FORMAT => Some(Box::new(anthropic::AnthropicOutbound::new())),
        gemini::FORMAT => Some(Box::new(gemini::GeminiOutbound::new())),
        _ => None,
    }
}
