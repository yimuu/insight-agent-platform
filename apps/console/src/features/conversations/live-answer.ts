// The text fallback for structured model output streams a JSON object. Preview its
// root answer string without exposing the transport wrapper or inventing tokens.
export function liveAnswer(text: string): string {
  const source = text.trimStart()
  if (!source.startsWith('{')) return text
  function stringAt(start: number) {
    let value = ''
    let at = start + 1
    while (at < source.length) {
      const character = source[at++]!
      if (character === '"') return { value, at, complete: true }
      if (character !== '\\') {
        value += character
        continue
      }
      const escape = source[at++]
      if (!escape) break
      if (escape === 'u') {
        const code = source.slice(at, at + 4)
        if (!/^[\da-fA-F]{4}$/.test(code)) break
        value += String.fromCharCode(parseInt(code, 16))
        at += 4
      } else {
        const decoded: Record<string, string> = {
          '"': '"',
          '\\': '\\',
          '/': '/',
          b: '\b',
          f: '\f',
          n: '\n',
          r: '\r',
          t: '\t',
        }
        if (!(escape in decoded)) break
        value += decoded[escape]
      }
    }
    // An escaped surrogate pair may straddle deltas.
    return { value: value.replace(/[\uD800-\uDBFF]$/, ''), at, complete: false }
  }
  let depth = 0
  for (let at = 0; at < source.length; at++) {
    const character = source[at]
    if (character === '{' || character === '[') depth++
    else if (character === '}' || character === ']') depth--
    else if (character === '"') {
      const token = stringAt(at)
      if (!token.complete) return ''
      let next = token.at
      while (/\s/.test(source[next] ?? '') && next < source.length) next++
      if (depth === 1 && token.value === 'answer' && source[next] === ':') {
        next++
        while (/\s/.test(source[next] ?? '') && next < source.length) next++
        return source[next] === '"' ? stringAt(next).value : ''
      }
      at = token.at - 1
    }
  }
  return depth === 0 ? text : ''
}
