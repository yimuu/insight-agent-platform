# Core Chat Dynamic Message Sources Design

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

> **Historical / superseded:** `core.chat` and its configured dynamic-source syntax were removed. Current true `Message[]` inputs, automatic in-place expansion, and instruction/data provenance are defined by [DSL Authoring Surface Redesign](./2026-07-17-dsl-authoring-surface-redesign.md).

## Goal

Allow `core.chat.messages` to interleave existing static messages with runtime message arrays read from agent input or a completed predecessor node. Dynamic messages remain structured JSON and are never generated or re-rendered through Handlebars.

The first version supports text and image input. The content-kind and model-capability design must admit future audio, file, and video inputs without changing dynamic-source expansion.

## DSL

Static messages retain their current syntax. A dynamic source is another entry in the ordered `messages` list:

```yaml
messages:
  - role: system
    content:
      template_ref: system

  - from:
      path: nodes.prepare_messages.output.messages
      optional: true
      max_messages: 50
      max_bytes: 262144
      allowed_content: [text, image_url]

  - role: user
    content: "{{ input.question }}"
```

The entries expand in configuration order. In the example, the final provider request contains the static system message, then every dynamic history message, then the static current question.

Dynamic values are already materialized data. Strings such as `{{ input.secret }}` inside a dynamic message are sent literally and are not rendered a second time.

## Configuration Model

The raw chat configuration changes from a list of static messages to a list of entries:

```rust
enum MessageEntryConfig {
    Static(MessageConfig),
    Dynamic { from: DynamicMessagesConfig },
}

struct DynamicMessagesConfig {
    path: String,
    optional: bool,
    max_messages: usize,
    max_bytes: usize,
    allowed_content: BTreeSet<DynamicContentKind>,
}

enum DynamicContentKind {
    Text,
    ImageUrl,
}
```

Defaults:

- `optional: false`
- `max_messages: 50`
- `max_bytes: 262144`
- `allowed_content: [text]`

Both limits must be positive. `allowed_content` must contain at least one known content kind. Unknown fields and unknown content kinds fail compilation.

Existing static message YAML remains valid and retains its current compile-time template, prompt-reference, image capability, and rendering behavior.

## Source Paths

The first version accepts canonical dotted object paths only:

```text
input.messages
input.conversation.history
nodes.prepare_messages.output
nodes.prepare_messages.output.messages
```

Rules:

- An input path starts with `input` and contains at least one field segment.
- A node path starts with `nodes.<node_id>.output` and may end there or continue through object fields.
- Every segment uses the DSL identifier grammar `[A-Za-z_][A-Za-z0-9_]*`.
- Array indices, bracket access, Handlebars, CEL, JSONPath, wildcards, and computed paths are not supported.

The compiled path stores either an input-field traversal or a node ID plus an output-field traversal. Runtime resolution reads `RunContext::input` or `RunContext::node_output` directly.

For `nodes.<node_id>.output...`, compilation adds `<node_id>` to the chat node's reference set. Existing graph validation therefore rejects missing, future, non-dominating, cross-branch, and otherwise invisible node references. No dynamic source can read node lifecycle state, node input, or errors outside the node's declared output.

## Dynamic Message Contract

The source value must be a JSON array. Each array item is a strict dynamic message with no unknown fields.

Text message:

```json
{
  "role": "user",
  "content": "What changed?"
}
```

Multipart text message:

```json
{
  "role": "assistant",
  "content": [
    {"type": "text", "text": "The report shows..."}
  ]
}
```

User image message:

```json
{
  "role": "user",
  "content": [
    {"type": "text", "text": "Review this image"},
    {
      "type": "image_url",
      "image_url": {"url": "http://example.test/report.png"}
    }
  ]
}
```

Contract:

- Dynamic roles are limited to `user` and `assistant`.
- Dynamic `system` messages are always rejected. System messages remain controlled by static agent configuration.
- Content is either a string or a non-empty content-parts array.
- The first version recognizes only `text` and `image_url` parts.
- A content string has kind `text` and requires `text` in `allowed_content`.
- Every part kind must appear in the source's `allowed_content` set.
- Image parts are allowed only in dynamic `user` messages.
- Image URLs must not be empty or whitespace-only. Core chat does not otherwise restrict the URL scheme or fetch the URL.
- Dynamic parts do not support `optional`; a producer omits a part it does not want to send.
- Unknown roles, fields, content kinds, and malformed content fail the entire chat node.
- Text strings and text-part values preserve the existing chat protocol behavior, including empty strings.

Dynamic parsing uses strict source-specific wire types and converts validated values into the existing `ChatMessage`, `ChatContent`, and `ChatContentPart` model types. This keeps runtime validation narrow without changing existing message serialization.

## Runtime Expansion

For each configured entry, runtime performs one of two operations:

- A static entry renders one existing compiled message.
- A dynamic entry resolves, checks, parses, validates, and expands zero or more messages.

Dynamic entry processing order is:

1. Resolve the configured path.
2. If it is missing, insert zero messages only when `optional: true`; otherwise fail.
3. Require an array. `null` and all non-array values fail even when the source is optional.
4. Serialize the source array to JSON and enforce `max_bytes` before parsing individual messages.
5. Enforce `max_messages` against the array length.
6. Strictly parse and validate each message in source order.
7. Append the converted messages at the dynamic entry's configured position.

An empty array is valid and expands to zero messages. After every entry is expanded, the final message list must contain at least one message. Otherwise the node fails before observability request logging and provider invocation.

No automatic truncation, reordering, merging, deduplication, role rewriting, or context-window management occurs.

## Content Capabilities

`allowed_content` describes every content kind a dynamic source may produce. It is also the compile-time declaration needed to validate the model before runtime data exists.

First-version mapping:

| Content kind | Required model capability |
|---|---|
| `text` | none |
| `image_url` | `ModelCapability::Vision` |

If a dynamic source allows `image_url`, its chat model must advertise Vision even when a particular run contains no images.

Future audio uses a modality name rather than a transport-specific name:

```text
DynamicContentKind::InputAudio
ModelCapability::AudioInput
```

The future `input_audio` part owns a source union such as URL, inline data plus format, or file ID. `audio_url` is not a top-level content kind. Adding audio requires new formal message-part and provider serialization support, but path resolution, source limits, expansion, ordering, and error handling remain unchanged.

Audio, files, and video are extension points only and are not implemented in the first version.

## Errors

Compile-time errors:

- `CHAT_DYNAMIC_MESSAGES_PATH_INVALID`: non-canonical or unsupported path.
- `CHAT_DYNAMIC_MESSAGES_CONFIG_INVALID`: zero limits, empty content set, unknown configuration, or invalid content declaration.
- `MODEL_CAPABILITY_REQUIRED`: an allowed dynamic content kind requires a capability the model lacks.
- Existing `INVALID_NODE_REFERENCE`: node source is absent or not completed on every incoming path.

Runtime errors:

- `CHAT_DYNAMIC_MESSAGES_SOURCE_MISSING`: required source path is missing.
- `CHAT_DYNAMIC_MESSAGES_INVALID`: source is not an array or a message violates the strict dynamic contract.
- `CHAT_DYNAMIC_MESSAGES_LIMIT_EXCEEDED`: array length exceeds `max_messages`.
- `CHAT_DYNAMIC_MESSAGES_TOO_LARGE`: serialized source array exceeds `max_bytes`.
- `CHAT_MESSAGES_EMPTY`: all static and dynamic entries expand to no messages.

Errors may identify the configuration entry, canonical source path, message index, part index, and violated rule. They must not include message text, image URLs, serialized source values, node output bodies, or model request bodies.

## Observability and Security

Existing chat request logs continue to count the final expanded message list and actual image parts. Logs remain body-free and never record source data or rendered messages.

Failures during source resolution, limits, parsing, or validation occur before `chat.request` logging and before the model is called.

Dynamic assistant messages are permitted because conversation history requires them, but they do not receive system authority. The platform does not reinterpret content, defend against semantic prompt injection inside user-controlled history, or persist conversation state. Agent authors decide which input or action output is trusted enough to include.

HTTP image URLs remain provider payload. The platform does not download, resolve, probe, or inspect them.

## Testing

Compilation tests cover:

- all existing static repository agents compile unchanged;
- static and dynamic entries parse in one ordered list;
- default and explicit dynamic-source settings;
- invalid paths, zero limits, empty/unknown content kinds, and unknown fields;
- `optional` on a dynamic source only, not on dynamic messages or parts;
- node source references participate in existing dominator and branch validation;
- `image_url` allowance requires a Vision model;
- text-only sources work with text-only models.

Runtime tests cover:

- ordered expansion before, between, and after static messages;
- `input.messages`, direct node output arrays, and nested node output arrays;
- dynamic `{{ ... }}` content remains literal;
- missing required and optional sources;
- empty arrays, `null`, non-arrays, and final empty requests;
- byte and message limits before provider invocation;
- strict roles, fields, strings, non-empty parts, and allowed content kinds;
- dynamic system rejection;
- user images, assistant image rejection, blank image rejection, and provider serialization;
- errors and logs contain no source values;
- chat observability counts final messages and images only.

Provider tests preserve the final message order and serialize text and image parts using the existing OpenAI-compatible contract.

## Expected Files

Likely production changes:

- `src/nodes/chat.rs`
- `src/dsl/references.rs` or a focused shared path parser module if extraction is reused

Likely test changes:

- `tests/core_chat_action.rs`
- `tests/dsl_compiler.rs`
- `tests/dsl_parallel.rs`
- `tests/observability.rs`
- `tests/repository_agents_v1.rs`

No medical-report agent configuration change is required to add the generic capability. Adopting dynamic history in that agent is a separate migration decision.

## Non-goals

- Dynamic system messages.
- Handlebars-rendered JSON message lists.
- Server-side conversation persistence.
- Automatic history truncation or summarization.
- Array-index, JSONPath, wildcard, or computed source paths.
- Optional dynamic message parts.
- Platform-side URL fetching or probing.
- Audio, file, or video provider support in the first version.
- Automatically migrating existing prompt-embedded history to dynamic messages.
