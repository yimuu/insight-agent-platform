import { spawnSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { isAbsolute, join } from 'node:path'

const root = fileURLToPath(new URL('../../../', import.meta.url))
const output = fileURLToPath(new URL('../src/agent/generated/', import.meta.url))
function run(command, args) {
  const result = spawnSync(command, args, { cwd: root, stdio: 'inherit' })
  if (result.error) throw result.error
  if (result.status !== 0) process.exit(result.status ?? 1)
}
const bindgen = process.env.WASM_BINDGEN ?? 'wasm-bindgen'
const version = spawnSync(bindgen, ['--version'], { encoding: 'utf8' })
if (version.status !== 0 || version.stdout.trim() !== 'wasm-bindgen 0.2.126') {
  throw new Error('Install wasm-bindgen-cli 0.2.126, or set WASM_BINDGEN to that executable; no alternative compiler is used.')
}
run('cargo', ['build', '--locked', '-p', 'insight-platform-agent-compiler-wasm', '--target', 'wasm32-unknown-unknown', '--no-default-features', '--release'])
const metadata = spawnSync('cargo', ['metadata', '--locked', '--no-deps', '--format-version', '1'], { cwd: root, encoding: 'utf8' })
if (metadata.status !== 0) throw new Error('Cargo target directory could not be resolved.')
const targetDirectory = JSON.parse(metadata.stdout).target_directory
if (typeof targetDirectory !== 'string' || !isAbsolute(targetDirectory)) throw new Error('Cargo target directory is not an absolute path.')
run(bindgen, [join(targetDirectory, 'wasm32-unknown-unknown/release/insight_platform_agent_compiler_wasm.wasm'), '--target', 'web', '--out-dir', output])
