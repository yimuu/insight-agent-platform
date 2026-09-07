import initialize, { compile_agent, compile_sources, inspect_agent_manifest, inspect_agent_sources, canonical_digest_json, build_expression_json } from './generated/insight_platform_agent_compiler_wasm.js'

const ready = initialize()
self.addEventListener('message', async (event: MessageEvent<{ operation: string; bytes: Uint8Array }>) => {
  try {
    await ready
    const operation = event.data.operation
    const input = event.data.bytes
    if (!(input instanceof Uint8Array)) throw new Error('invalid compiler transport')
    let bytes: Uint8Array
    if (operation === 'compile') bytes = compile_sources(input)
    else if (operation === 'compile_frozen') bytes = compile_agent(input)
    else if (operation === 'inspect_sources') bytes = inspect_agent_sources(input)
    else if (operation === 'inspect') bytes = inspect_agent_manifest(input)
    else if (operation === 'digest') bytes = canonical_digest_json(input)
    else if (operation === 'build_expression') bytes = build_expression_json(input)
    else throw new Error('unregistered compiler operation')
    self.postMessage({ bytes })
  } catch { self.postMessage({ failed: true }) }
})
