// Test transport only. The production adapter still runs in a dedicated browser Worker.
// Every request here executes the actual generated WASM, never a JS compiler substitute.
import { readFileSync } from 'node:fs'
import {
  initSync,
  compile_agent,
  compile_sources,
  inspect_agent_manifest,
  inspect_agent_sources,
  canonical_digest_json,
  build_expression_json,
} from '../src/shared/compiler/generated/insight_platform_agent_compiler_wasm.js'

initSync({
  module: readFileSync(
    new URL(
      '../src/shared/compiler/generated/insight_platform_agent_compiler_wasm_bg.wasm',
      import.meta.url,
    ),
  ),
})
Object.defineProperty(globalThis, 'Worker', {
  configurable: true,
  writable: true,
  value: class {
    listeners = new Map()
    stopped = false
    addEventListener(name, callback) {
      this.listeners.set(name, callback)
    }
    terminate() {
      this.stopped = true
    }
    postMessage({ operation, bytes }) {
      queueMicrotask(() => {
        if (this.stopped) return
        try {
          const fn = {
            inspect_sources: inspect_agent_sources,
            inspect: inspect_agent_manifest,
            compile: compile_sources,
            compile_frozen: compile_agent,
            digest: canonical_digest_json,
            build_expression: build_expression_json,
          }[operation]
          this.listeners.get('message')?.({ data: { bytes: fn(bytes) } })
        } catch {
          this.listeners.get('message')?.({ data: { failed: true } })
        }
      })
    }
  },
})
