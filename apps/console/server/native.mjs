import { nativeTransportLimits } from './config.mjs'
import { defaultBundleRoot, startConsoleServer } from './gateway-server.mjs'
import { runUntilSignal } from './process.mjs'

// This adapter is also used by real Gateway qualification. The transport itself has one owner.
export async function startGatewayConsoleServer({ gatewayOrigin, managementGatewayOrigin, port = 0, bundleRoot } = {}) {
  return startConsoleServer({
    bundleRoot: bundleRoot ?? process.env.INSIGHT_CONSOLE_BUNDLE_ROOT ?? defaultBundleRoot,
    config: {
      schema_version: 1, topology: 'native', listen_host: '127.0.0.1', listen_port: port,
      runtime_origin: gatewayOrigin ?? process.env.INSIGHT_CONSOLE_GATEWAY_ORIGIN ?? '',
      management_origin: managementGatewayOrigin ?? process.env.INSIGHT_CONSOLE_MANAGEMENT_GATEWAY_ORIGIN ?? '',
      ...nativeTransportLimits,
    },
  })
}

if (import.meta.main) {
  runUntilSignal(() => startGatewayConsoleServer({ port: Number(process.env.INSIGHT_CONSOLE_PORT ?? 4173) }))
}
