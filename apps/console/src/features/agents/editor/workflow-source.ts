import { stringify } from 'yaml'

/** Editable author source only. Rust owns validation, template parsing and lowering. */
export function initialWorkflowSource(): string {
  const output = {
    $schema: 'https://json-schema.org/draft/2020-12/schema',
    type: 'object',
    properties: {
      answer: { type: 'string', minLength: 1, maxLength: 4096, 'x-platform-max-bytes': 16384 },
    },
    required: ['answer'],
    additionalProperties: false,
  }
  return stringify(
    {
      version: 1,
      steps: [
        { id: 'analyze', prompt: '提取以下内容中的关键事实：\n{{ input.message }}', output },
        {
          id: 'summarize',
          prompt: '根据这些事实，用通俗中文给出摘要：\n{{ nodes.analyze.output.answer }}',
          output,
        },
      ],
      output: { answer: '{{ nodes.summarize.output.answer }}' },
    },
    { lineWidth: 0, aliasDuplicateObjects: false },
  )
}
