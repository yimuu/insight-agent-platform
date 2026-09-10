import { loadTransportConfig } from './config.ts'
import { startConsoleServer } from './gateway-server.ts'
import { runUntilSignal } from './process.ts'

if (import.meta.main) {
  runUntilSignal(async () => {
    if (process.argv.length !== 4 || process.argv[2] !== '--config')
      throw new Error('Invalid arguments')
    return startConsoleServer({ config: loadTransportConfig(process.argv[3]) })
  })
}
