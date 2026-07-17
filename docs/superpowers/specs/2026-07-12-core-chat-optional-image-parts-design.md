# Core Chat Optional Image Parts Design

> **Historical syntax:** optional-image behavior was migrated to the vNext `ai.chat` operation; the `core.chat` examples below are not accepted. See [DSL vNext Region/SSA Design](./2026-07-16-dsl-vnext-region-ssa-design.md).

## Goal

Allow a `core.chat` image content part to be explicitly optional. Optional image parts are omitted when their URL input is absent or blank, allowing one chat node to handle both text-only and multimodal requests. Update `medical_report_interpreter` to use this capability and accept HTTP image URLs.

## DSL Contract

An image content part accepts an optional boolean field at the part level:

```yaml
- type: image_url
  optional: true
  image_url:
    url: "{{ input.image_url }}"
```

`optional` defaults to `false`. Existing image parts that do not declare it retain their current compile-time and runtime behavior. Text parts do not accept `optional`; unknown-field rejection remains enabled.

Declaring an optional image still requires the configured model to advertise the Vision capability. The node may receive an image at runtime, so capability validation remains a compile-time property of the node configuration rather than the current input value.

## Rendering Semantics

Required image parts continue to render through the existing strict Handlebars path.

For an image part with `optional: true`:

- A successfully rendered non-blank URL produces an `image_url` content part.
- A successfully rendered empty or whitespace-only URL omits the content part.
- A Handlebars `MissingVariable` error omits the content part.
- Every other template rendering error remains `TEMPLATE_RENDER_FAILED`.

Whitespace is used only to decide whether the rendered URL is blank. A non-blank rendered value is preserved as rendered.

If filtering optional images leaves a multipart message with no content parts, execution fails with `CHAT_CONTENT_PARTS_EMPTY`. The runtime does not omit the entire message and does not call the model with an empty multipart message.

## Medical Report Agent

The medical report input schema changes as follows:

- `image_url` is removed from the required field list.
- When present, `image_url` remains a string.
- Accepted values are the empty string or strings beginning with `http://`, `https://`, or `data:image/`.
- `null` remains invalid.

All four medical chat nodes mark their image part `optional: true`. Missing or blank image input therefore produces a text-only model request without duplicating nodes or changing the initial-turn versus follow-up routing.

Prompt copy must no longer claim that an image was always attached. The abnormal-indicator and follow-up prompts state that the model should use the image when one is provided. Other first-turn output structure and follow-up direct-answer behavior remain unchanged.

## HTTP Image Boundary

HTTP and HTTPS image URLs are passed through to the configured model provider as message content. The platform does not download, resolve, probe, validate reachability, or inspect the target address.

This change does not weaken the transport policy for the platform's own connection to the model provider. It only broadens the medical agent's input schema for a URL that the provider may fetch. No private-network allowlist or SSRF filter is added because the platform is not the HTTP client for the image URL.

## Error Handling

- Required image template failures retain `TEMPLATE_RENDER_FAILED`.
- Optional image missing-variable errors are the only rendering errors converted to omission.
- Optional image blank results are omitted without an error.
- A multipart message emptied by omission returns `CHAT_CONTENT_PARTS_EMPTY` before model invocation.
- Input values outside the medical schema, including `null`, non-strings, and unsupported URL schemes, retain `INPUT_INVALID` at the API boundary.

## Testing

Core chat tests cover:

- omitted `optional` preserves required image behavior;
- `optional: true` includes non-blank HTTP, HTTPS, and data-image URLs;
- missing, empty, and whitespace-only optional values omit the image part;
- required missing values still fail with `TEMPLATE_RENDER_FAILED`;
- non-`MissingVariable` errors on optional templates still fail with `TEMPLATE_RENDER_FAILED`;
- filtering every part returns `CHAT_CONTENT_PARTS_EMPTY` without invoking the model;
- optional images still require a Vision-capable model;
- `optional` on a text part is rejected as an unknown field.

Medical agent tests cover:

- repository compilation with optional image parts;
- input schema acceptance for a missing field, empty string, HTTP URL, HTTPS URL, and data-image URL;
- input schema rejection for `null`, non-string values, and unsupported schemes;
- initial-turn and follow-up execution without `image_url` sends text-only requests;
- an empty `image_url` sends text-only requests;
- an HTTP image URL is preserved in every model request on the selected path;
- existing initial three-step output and follow-up one-step output contracts remain intact.

## Files

Expected production changes:

- `src/nodes/chat.rs`
- `agents/medical_report_interpreter/agent.yaml`
- `agents/medical_report_interpreter/prompts/abnormal_indicators.md`
- `agents/medical_report_interpreter/prompts/follow_up.md`

Expected test changes:

- `tests/core_chat_action.rs`
- `tests/medical_report_follow_up.rs`

## Non-goals

- Optional text content parts.
- Conditional omission of entire messages.
- Downloading or validating image content.
- URL reachability checks or private-network classification.
- Changing provider transport security policy.
- Adding optional behavior to required image parts.
