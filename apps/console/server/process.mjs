export function runUntilSignal(start) {
  start().then((server) => {
    process.stdout.write(`${JSON.stringify({ kind: 'insight.console.transport/v1', origin: server.origin })}\n`)
    for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => server.close().finally(() => process.exit(0)))
  }).catch(() => {
    // Startup errors may contain filesystem paths or configured targets. Do not echo them.
    process.stderr.write('Console transport startup failed\n')
    process.exitCode = 1
  })
}
