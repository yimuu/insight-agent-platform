# Medical Report Agent Design

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

## Goal

Add a platform-owned medical report interpretation agent that accepts text and images, supports multi-turn context, and streams a three-step Chinese interpretation flow: abnormal indicators, comprehensive interpretation, and health advice.

## Scope

This feature extends the existing config-driven agent platform. It does not add an external agent creation API. The platform still owns all agents in the `agents/` directory.

## User-Facing Behavior

The new agent id is `medical_report_interpreter`.

The run input is an object with these fields:

- `report_text`: optional string containing OCR text or manually entered report text.
- `images`: optional array of image URLs or base64 data URLs.
- `messages`: optional array of prior conversation messages with `role` and `content`.
- `question`: optional string for the current follow-up question.

At least one of `report_text`, `images`, or `question` is expected for useful output. The JSON schema will require an object shape and validate the nested field types; deeper cross-field validation is left to prompts for now.

The agent supports multi-turn conversation by rendering `messages` and `question` into every LLM step prompt. The platform does not persist conversation state in this feature; callers send the conversation history each time.

## Medical Workflow

The agent has three LLM steps:

1. `abnormal_indicators`: identify abnormal or clinically relevant indicators from report text and images.
2. `comprehensive_interpretation`: synthesize the report, prior step output, conversation history, and current question.
3. `health_advice`: produce the final patient-facing advice.

Each step includes the same system prompt with safety boundaries:

- Answer in Chinese by default.
- Explain report contents, but do not provide a definitive diagnosis.
- Encourage clinical follow-up when risk, symptoms, or uncertainty are present.
- For urgent warning signs, recommend timely medical care.
- If the input is not a medical report, refuse briefly and do not invent a medical interpretation.

## Multimodal Model Messages

The current `ChatMessage` type only supports string content. This feature changes it to support OpenAI-compatible multimodal content:

- Text-only messages continue to serialize as `"content": "..."` to preserve compatibility.
- Multimodal user messages serialize as `"content": [{"type":"text","text":"..."},{"type":"image_url","image_url":{"url":"..."}}]`.

The runner will attach images only to LLM user messages when a step declares `image_input: input.images`. Existing agents are unchanged because they do not set `image_input`.

Image values are passed through as provided. They can be HTTPS URLs or `data:image/...;base64,...` URLs. This feature does not download, store, transform, or OCR images.

## Configuration Changes

`StepConfig` gains an optional `image_input` field. It is a dotted input path, initially supporting `input.images`.

When an LLM step has `image_input: input.images`, the runner reads `ctx.input.images` and appends each string as an `image_url` content part after the rendered text prompt. Non-string array items are ignored by the runner, while the agent schema prevents them in normal API use.

## Files

New files:

- `agents/medical_report_interpreter/agent.yaml`
- `agents/medical_report_interpreter/prompts/system.md`
- `agents/medical_report_interpreter/prompts/abnormal_indicators.md`
- `agents/medical_report_interpreter/prompts/comprehensive_interpretation.md`
- `agents/medical_report_interpreter/prompts/health_advice.md`

Modified files:

- `src/agent/config.rs`
- `src/engine/runner.rs`
- `src/model/types.rs`
- `src/model/openai.rs`
- `README.md`
- Tests under `tests/` and `src/model/openai.rs`.

## Testing

Add tests for:

- OpenAI request serialization for multimodal user messages.
- Runner constructing text plus image content for LLM steps that opt into `image_input`.
- Existing text-only agents still sending string content.
- Loader accepting the medical report agent with multiple prompt files.
- API input schema accepting `report_text`, `images`, `messages`, and `question` shapes for the new agent.

## Non-Goals

- No uploaded file storage.
- No OCR service.
- No server-side conversation memory.
- No conditional execution DSL.
- No hard medical classification gate outside prompts.
- No real model integration test with private API keys.
