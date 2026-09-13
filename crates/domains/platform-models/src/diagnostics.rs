//! Public diagnostics are fixed platform text, never provider-supplied messages or schema values.
const DIAGNOSTICS: &[(&str, &str)] = &[
    ("openai_responses_provider_error", "模型服务返回失败或未完成的响应。请检查服务状态与输出预算后重试。"),
    ("anthropic_messages_provider_error", "模型服务返回失败或未完成的响应。请检查服务状态与输出预算后重试。"),
    ("model_structured_output_invalid_json", "模型回答不是有效的 JSON。请调整提示词，要求模型仅返回符合输出定义的 JSON 后重试。"),
    ("model_structured_output_schema_mismatch", "模型回答未满足智能体的输出约束（字段类型、必填项或长度）。请检查输出字段设置后重新发布智能体。"),
    ("model_structured_output_too_large", "模型回答超过结构化输出的容量限制。请缩短回答，或调整模型的输出限制后重试。"),
    ("model_connect_timeout", "连接模型服务超时。请检查模型服务地址和网络，稍后重试。"),
    ("model_stream_idle_timeout", "模型服务长时间没有返回新内容。请稍后重试，或检查模型服务状态。"),
    ("model_total_timeout", "模型调用超过总时间限制。请缩短请求或调整执行时间限制后重试。"),
    ("model_output_too_large", "模型回答超过输出容量限制。请缩短回答，或调整输出限制后重试。"),
];
const UNKNOWN: &str = "模型调用失败。请检查模型配置和服务状态后重试。";

pub fn model_failure_safe_message(code: &str) -> &'static str {
    DIAGNOSTICS
        .iter()
        .find(|(known, _)| *known == code)
        .map_or(UNKNOWN, |(_, message)| message)
}

/// Historical rows without an exact recognized message do not acquire an inferred diagnosis.
pub fn public_model_failure_summary(message: &str) -> Option<&'static str> {
    DIAGNOSTICS
        .iter()
        .map(|(_, message)| *message)
        .chain(std::iter::once(UNKNOWN))
        .find(|known| *known == message)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_exact_platform_messages_can_be_disclosed() {
        for (code, message) in DIAGNOSTICS {
            assert_eq!(model_failure_safe_message(code), *message);
            assert_eq!(public_model_failure_summary(message), Some(*message));
            assert!(
                message.len() <= insight_platform_contracts::MAX_PUBLIC_EVENT_SAFE_SUMMARY_BYTES
            );
        }
        assert_eq!(model_failure_safe_message("secret-canary"), UNKNOWN);
        for untrusted in [
            "secret-canary",
            "Model Provider request failed",
            "model_connect_timeout",
            "模型调用失败。请检查模型配置和服务状态后重试。 secret-canary",
        ] {
            assert_eq!(public_model_failure_summary(untrusted), None);
        }
    }
}
