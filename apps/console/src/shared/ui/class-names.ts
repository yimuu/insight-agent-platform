/** Apply both shared primitives and feature overrides; a local selector never replaces the base class. */
export function classNames(...modules: Record<string, string>[]) {
  return (value: string | undefined | false) =>
    value
      ? value
          .split(/\s+/)
          .filter(Boolean)
          .flatMap((name) => {
            const matches = modules.flatMap((module) => (module[name] ? [module[name]] : []))
            return matches.length ? [...new Set(matches)] : [name]
          })
          .join(' ')
      : undefined
}
