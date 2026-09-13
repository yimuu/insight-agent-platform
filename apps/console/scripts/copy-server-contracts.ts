import { copyFileSync } from 'node:fs'

copyFileSync(
  new URL('../../../crates/adapters/platform-postgres/schema-inventory.json', import.meta.url),
  new URL('../server-dist/schema-inventory.json', import.meta.url),
)
