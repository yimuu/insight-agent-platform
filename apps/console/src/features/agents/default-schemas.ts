// New authoring drafts only: published interfaces and restored sources remain exact.
function textSchema(field: string, maxLength: number, maximumBytes: number): string {
  return JSON.stringify(
    {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      properties: {
        [field]: {
          type: 'string',
          minLength: 1,
          maxLength,
          'x-platform-max-bytes': maximumBytes,
        },
      },
      required: [field],
      additionalProperties: false,
    },
    null,
    2,
  )
}

export const DEFAULT_INPUT_SCHEMA = textSchema('message', 16_384, 16_384)
export const DEFAULT_OUTPUT_SCHEMA = textSchema('answer', 16_384, 65_536)
