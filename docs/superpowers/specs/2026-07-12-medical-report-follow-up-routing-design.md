# Medical Report Follow-up Routing Design

> **Historical syntax:** the product behavior is retained by the checked-in vNext Agent, but the condition/node graph below is not accepted. See [DSL vNext Region/SSA Design](./2026-07-16-dsl-vnext-region-ssa-design.md).

## Goal

Give `medical_report_interpreter` a dedicated multi-turn path. A follow-up request must answer the current question directly instead of reusing the first-turn prompts or executing the three-step initial interpretation workflow.

## Turn Classification

The request is classified only by `input.messages`:

- An empty `messages` array is an initial turn.
- A non-empty `messages` array is a follow-up turn.

`input.question`, `input.report_text`, and `input.image_url` do not affect this classification.

## Agent Graph

Change the agent entry to a `core.condition` node. The condition evaluates `size(input.messages) > 0`.

- A matching request goes to a new `follow_up` chat node and then to a dedicated `follow_up_result` output node.
- The default path goes to the existing `abnormal_indicators` node and continues through `comprehensive_interpretation`, `health_advice`, and `result` unchanged.

The resulting paths are:

```text
route -- messages non-empty --> follow_up --> follow_up_result
      `-- messages empty ----> abnormal_indicators
                                --> comprehensive_interpretation
                                --> health_advice
                                --> result
```

This is an agent configuration change. It does not require a new runtime node type, API field, or caller-selected agent.

## Follow-up Prompt Contract

Add a dedicated `prompts/follow_up.md` prompt. The `follow_up` node uses the existing medical system prompt and sends the follow-up prompt plus the report image to the configured vision model.

The prompt includes:

- `input.report_text`;
- every item in `input.messages`, preserving its role and content;
- `input.question`;
- the report image already attached to the user message.

The model must:

- answer the current question directly using the report and conversation history;
- avoid regenerating the complete first-turn interpretation;
- omit section titles, including `异常指标解读`, `综合解读`, and `健康建议`;
- avoid the first-turn three-step output format;
- retain the medical safety boundaries from the system prompt;
- state uncertainty when the report, units, reference ranges, or conversation context are insufficient;
- refuse briefly when the available context is not a medical report.

The follow-up output is the single model response as Markdown. It does not expose structured fields for the three initial interpretation results.

## Initial-turn Compatibility

When `input.messages` is empty, the existing three chat nodes, their prompts, emitted content, final Markdown composition, and structured output fields remain unchanged. This preserves the current first-turn response contract.

## Error Handling

Condition evaluation, prompt rendering, image handling, model failures, cancellation, and output persistence continue to use existing runtime behavior. No fallback from the follow-up path to the initial three-step path is introduced because that could repeat the report interpretation and multiply model calls unexpectedly.

## Testing

Add focused regression tests that compile and execute the repository-owned agent with a recording model:

- An empty `messages` array executes the three existing chat nodes and never executes `follow_up`.
- A non-empty `messages` array executes only `follow_up`, resulting in exactly one model request.
- The follow-up model request contains the dedicated prompt content, report text, history, current question, and image.
- The follow-up final output contains only the model response and has no initial-turn structured result fields.
- The repository agent compilation test expects the new condition, follow-up chat, and follow-up output nodes.

Prompt text will also be checked directly for the no-title and direct-answer requirements so the behavioral contract does not depend on a live model integration test.

## Non-goals

- Server-side conversation persistence.
- Automatic inference of turn type from `question` or report content.
- Changes to the input schema or HTTP API.
- Changes to the initial three-step prompt wording or output format.
- A second medical report agent dedicated to follow-ups.
