import { loadTransportConfig } from './config.mjs'
import { startConsoleServer } from './gateway-server.mjs'
import { runUntilSignal } from './process.mjs'

if (import.meta.main) {
  runUntilSignal(async () => {
    if (process.argv.length !== 4 || process.argv[2] !== '--config') throw new Error('Invalid arguments')
    return startConsoleServer({ config: loadTransportConfig(process.argv[3]) })
  })
}
