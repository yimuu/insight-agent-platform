import registryText from '../../../../../contracts/platform-v1/schemas/nominal-types.json?raw'
import { digestJson } from '../compiler/compiler.ts'
import { object } from './tree.ts'
import type { JsonObject } from './tree.ts'
import type { NominalSchemas } from './tree.ts'

const documents = import.meta.glob('../../../../../contracts/platform-v1/schemas/nominal/*.json', {
  query: '?raw',
  import: 'default',
  eager: true,
}) as Record<string, string>
let loaded: Promise<NominalSchemas> | undefined
/** Only build-time bundled owning schemas are eligible. No URL is fetched. */
export function pinnedNominalSchemas(): Promise<NominalSchemas> {
  return (loaded ??= (async () => {
    const registry = JSON.parse(registryText) as {
      profile: string
      schemas: { name: string; path: string; canonical_digest: string; pinned_reference: string }[]
    }
    if (registry.profile !== 'insight.platform/v1')
      throw new Error('Unsupported local nominal registry.')
    const result = new Map<string, JsonObject>()
    await Promise.all(
      registry.schemas.map(async (entry) => {
        if (
          entry.pinned_reference !==
            `urn:insight:platform:v1:nominal:${entry.name}@${entry.canonical_digest}` ||
          !/^schemas\/nominal\/[a-z0-9-]+\.schema\.json$/.test(entry.path)
        )
          throw new Error('Invalid pinned nominal entry.')
        const source = documents[`../../../../../contracts/platform-v1/${entry.path}`]
        if (!source) throw new Error('Pinned nominal schema is unavailable in this Console build.')
        const schema: unknown = JSON.parse(source)
        if (!object(schema) || (await digestJson(schema)) !== entry.canonical_digest)
          throw new Error('Pinned nominal schema digest does not match the owning registry.')
        result.set(entry.pinned_reference, schema)
      }),
    )
    return result
  })())
}
