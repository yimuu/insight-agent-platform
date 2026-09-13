/** DOM-only field actions shared by executable browser journeys. */
export function fieldElement(label: string): string {
  return `([...document.querySelectorAll('[data-code-editor]')].find(node => node.dataset.codeEditor === ${JSON.stringify(label)})?.querySelector('.cm-content') ?? [...document.querySelectorAll('label')].find(node => node.querySelector(':scope > span')?.textContent.trim() === ${JSON.stringify(label)})?.querySelector('input,textarea,select'))`
}
export function fieldValue(label: string): string {
  // CodeMirror virtualizes long documents. Read through its normal copy handler,
  // not just the visible lines, without touching private editor state or the OS clipboard.
  return `(() => {
    const node = ${fieldElement(label)};
    if (!node?.classList.contains('cm-content')) return node?.value;
    // Source editors now live in disclosure panels. Open their public controls
    // before focusing/copying, as a user must do to inspect the source.
    for (let parent = node.parentElement; parent; parent = parent.parentElement) {
      if (parent instanceof HTMLDetailsElement && !parent.open) parent.querySelector(':scope > summary')?.click();
    }
    const previous = document.activeElement;
    node.focus();
    node.dispatchEvent(new KeyboardEvent('keydown', {
      key: 'a', code: 'KeyA', keyCode: 65, bubbles: true, cancelable: true,
      metaKey: /Mac/.test(navigator.platform), ctrlKey: !/Mac/.test(navigator.platform),
    }));
    const clipboardData = new DataTransfer();
    node.dispatchEvent(new ClipboardEvent('copy', {clipboardData, bubbles: true, cancelable: true}));
    const text = clipboardData.getData('text/plain');
    if (previous instanceof HTMLElement && previous !== node) previous.focus({preventScroll:true});
    return text;
  })()`
}
export function setField(label: string, value: string): string {
  return `(async () => {
    const node = ${fieldElement(label)};
    if (!node || node.disabled) throw new Error('Field unavailable: ' + ${JSON.stringify(label)});
    for (let parent = node.parentElement; parent; parent = parent.parentElement) {
      if (parent instanceof HTMLDetailsElement && !parent.open) parent.querySelector(':scope > summary')?.click();
    }
    if (node.classList.contains('cm-content')) {
      const deadline = performance.now() + 8000;
      while (!node.isContentEditable) {
        if (!node.isConnected || performance.now() >= deadline) throw new Error('Editor is not editable: ' + ${JSON.stringify(label)});
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      node.focus();
      node.dispatchEvent(new KeyboardEvent('keydown', {
        key: 'a', code: 'KeyA', keyCode: 65, bubbles: true, cancelable: true,
        metaKey: /Mac/.test(navigator.platform), ctrlKey: !/Mac/.test(navigator.platform),
      }));
      const clipboardData = new DataTransfer();
      clipboardData.setData('text/plain', ${JSON.stringify(value)});
      node.dispatchEvent(new ClipboardEvent('paste', {clipboardData, bubbles: true, cancelable: true}));
      return;
    }
    const prototype = node instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : node instanceof HTMLSelectElement ? HTMLSelectElement.prototype : HTMLInputElement.prototype;
    Object.getOwnPropertyDescriptor(prototype, 'value').set.call(node, ${JSON.stringify(value)});
    node.dispatchEvent(new Event(node instanceof HTMLSelectElement ? 'change' : 'input', {bubbles:true}));
  })()`
}
