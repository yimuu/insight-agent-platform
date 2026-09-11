import { useEffect, useState } from 'react'
import { createRoot } from 'react-dom/client'
import { TaskSchemaForm } from '../../src/features/tasks/TaskSchemaForm.tsx'
import type { TaskFormSchema } from '../../src/features/tasks/schema-form.ts'
import type { Json } from '../../src/shared/api/types.ts'
import '../../src/shared/styles/base.css'

const initialSchema = {
  $schema: 'https://json-schema.org/draft/2020-12/schema',
  type: 'object',
  additionalProperties: false,
  title: 'Task response',
  description: '<img src="https://invalid.example/never-fetch" onerror="alert(1)">',
  properties: {
    name: { title: 'Name', type: 'string', minLength: 2, maxLength: 5, 'x-platform-max-bytes': 8 },
    amount: { title: 'Amount', type: 'integer', minimum: 1, maximum: 3 },
    approved: { title: 'Approved', type: 'boolean' },
    colors: {
      title: 'Colors',
      type: 'array',
      minItems: 1,
      maxItems: 2,
      uniqueItems: true,
      items: {
        type: 'string',
        enum: ['red', 'blue'],
        minLength: 1,
        maxLength: 4,
        'x-platform-max-bytes': 4,
      },
    },
    details: {
      title: 'Details',
      type: 'object',
      additionalProperties: false,
      properties: {
        reply: {
          title: 'Reply',
          type: 'string',
          minLength: 1,
          maxLength: 10,
          'x-platform-max-bytes': 20,
        },
      },
      required: ['reply'],
    },
  },
  required: ['name', 'amount', 'approved', 'colors'],
} satisfies TaskFormSchema

type Configuration = {
  schema: TaskFormSchema
  digest: string
  disabled: boolean
  behavior: 'pending' | 'accept'
  subject: string
}
const fixture = {
  initialSchema,
  submitted: [] as Json[],
  configure: (_value: Partial<Configuration>) => {},
  settle: (_error?: string) => {},
}
Object.assign(window, { taskFormFixture: fixture })
export function Fixture() {
  const [configuration, configure] = useState<Configuration>({
    schema: initialSchema,
    digest: `sha256:${'a'.repeat(64)}`,
    disabled: false,
    behavior: 'pending',
    subject: 'task-a',
  })
  useEffect(() => {
    fixture.configure = (next) => configure((previous) => ({ ...previous, ...next }))
  }, [])
  return (
    <main style={{ maxWidth: 880, margin: '32px auto', padding: 24 }}>
      <h1>Task response fixture</h1>
      <TaskSchemaForm
        key={configuration.subject}
        schema={configuration.schema}
        responseSchemaDigest={configuration.digest}
        disabled={configuration.disabled}
        onSubmit={async (value) => {
          fixture.submitted.push(value)
          if (configuration.behavior === 'pending')
            await new Promise<void>((resolve, reject) => {
              fixture.settle = (error) => (error ? reject(new Error(error)) : resolve())
            })
        }}
      />
    </main>
  )
}
createRoot(document.getElementById('root')!).render(<Fixture />)
