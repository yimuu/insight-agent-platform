/** Translate presentation messages without changing schema validation or wire values. */
export function validationMessage(message: string): string {
  const exact: Record<string, string> = {
    'This field is required.': '此字段为必填项。',
    'Value must equal the schema constant.': '请使用 Schema 规定的固定值。',
    'Select a declared enum value.': '请选择规定的可选值。',
    'Select a schema variant.': '请选择一个数据分支。',
    'Select a value.': '请选择一个值。',
    'Select Yes or No.': '请选择是或否。',
    'Enter a JSON number.': '请输入合法的 JSON 数字。',
    'Use a finite interoperable JSON number.': '请输入有限且精度可表示的 JSON 数字。',
    'This property is not declared by the schema.': 'Schema 未声明此字段。',
    'Array items must be unique.': '数组元素不能重复。',
    'Each array item must be unique.': '数组元素不能重复。',
    'Text exceeds its UTF-8 limit or contains invalid Unicode.':
      '文本超过 UTF-8 字节限制或包含无效字符。',
    'Input exceeded this form’s field limit. Edit this field before submitting.':
      '内容超过字段限制，请修改后提交。',
  }
  if (exact[message]) return exact[message]
  const patterns: Array<[RegExp, (match: RegExpMatchArray) => string]> = [
    [/^(?:Add at least) (\d+) items?(?:\(s\))?\.$/, (m) => `请至少添加 ${m[1]} 项。`],
    [/^Use at most (\d+) items?(?:\(s\))?\.$/, (m) => `最多允许 ${m[1]} 项。`],
    [/^Use at least (\d+) characters\.$/, (m) => `请至少输入 ${m[1]} 个字符。`],
    [/^Use at most (\d+) characters\.$/, (m) => `最多允许 ${m[1]} 个字符。`],
    [/^Text must fit within (\d+) UTF-8 bytes\.$/, (m) => `文本最多允许 ${m[1]} 个 UTF-8 字节。`],
    [/^Text exceeds (\d+) UTF-8 bytes\.$/, (m) => `文本超过 ${m[1]} 个 UTF-8 字节。`],
    [/^Use a value of at least (.+)\.$/, (m) => `数值不能小于 ${m[1]}。`],
    [/^Use a value of at most (.+)\.$/, (m) => `数值不能大于 ${m[1]}。`],
    [/^Use a value greater than (.+)\.$/, (m) => `数值必须大于 ${m[1]}。`],
    [/^Use a value less than (.+)\.$/, (m) => `数值必须小于 ${m[1]}。`],
    [/^Response exceeds (\d+) UTF-8 bytes\.$/, (m) => `数据超过 ${m[1]} 个 UTF-8 字节。`],
  ]
  for (const [pattern, translate] of patterns) {
    const match = message.match(pattern)
    if (match) return translate(match)
  }
  return /[\u4e00-\u9fff]/.test(message)
    ? message
    : `数据不符合约定，请检查字段配置。（${message}）`
}
