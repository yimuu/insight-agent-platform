import { loadIdentityConfig } from './identity-config.ts'
import { startIdentityServer } from './identity-server.ts'
import { runUntilSignal } from './process.ts'

if (import.meta.main) {
  runUntilSignal(async () => {
    if (process.argv.length !== 4 || process.argv[2] !== '--config')
      throw new Error('Invalid arguments')
    return startIdentityServer(loadIdentityConfig(process.argv[3]))
  })
}
