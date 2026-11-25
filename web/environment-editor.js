/** QQ environment tab. Host supplies revision-checked persona I/O and the current display name. */
export function environmentEditor({read, write, botName}) {
  function element(tag, className, text = '') {
    const node = document.createElement(tag);
    node.className = className;
    node.textContent = text;
    return node;
  }
  const root = element('section', 'qq-scene-card infinity-voice-card');
  const body = element('div', 'qq-scene-body');
  const heading = element('header', 'qq-scene-title');
  const name = element('span', 'qq-scene-bot');
  const title = element('h3', '', 'QQ 环境提示词');
  heading.append(title, name);
  const editor = element('textarea', 'qq-scene-field');
  editor.setAttribute('aria-label', 'QQ 环境提示词');
  editor.rows = 10;
  const footer = element('div', 'qq-scene-footer');
  const status = element('span', 'qq-scene-status');
  status.setAttribute('role', 'status');
  const actions = element('div', 'qq-scene-actions');
  const undo = element('button', 'btn ghost', '撤销修改');
  const retry = element('button', 'btn ghost', '重新读取');
  const save = element('button', 'btn qq-scene-save', '保存修改');
  for (const button of [undo, retry, save]) button.type = 'button';
  actions.append(undo, retry, save);
  footer.append(status, actions);
  body.append(heading, editor, footer);
  const details = element('details', 'qq-scene-declaration');
  const summary = element('summary', '', '查看 QQ 通道声明');
  const documentText = element('p', 'qq-scene-declaration-text', '正在读取通道声明…');
  const note = element('small', 'qq-scene-declaration-note');
  details.append(summary, documentText, note);
  root.append(body, details);

  let saved = '', revision = '', busy = false, needsRead = false, lastDeclaration = '';
  function render(message = '') {
    const dirty = editor.value !== saved;
    editor.disabled = busy || !revision;
    editor.hidden = !revision;
    undo.hidden = save.hidden = !dirty;
    undo.disabled = save.disabled = busy;
    retry.hidden = !needsRead;
    retry.disabled = busy;
    status.textContent = message || (dirty ? '有未保存的修改' : '');
    status.dataset.dirty = String(dirty && !message);
    footer.hidden = !dirty && !needsRead && !status.textContent;
    const label = botName();
    name.textContent = label || '';
    name.hidden = !label;
  }
  async function load() {
    if (busy) return;
    busy = true;
    render('正在读取提示词…');
    let message = '';
    try {
      const info = await read();
      if (typeof info?.text !== 'string' || !info.revision) throw new Error('提示词或版本信息缺失');
      revision = info.revision;
      editor.value = saved = info.text;
      needsRead = false;
    } catch (error) {
      needsRead = true;
      message = '读取失败：' + error.message;
    } finally {
      busy = false;
      render(message);
    }
  }
  async function persist() {
    if (busy || !revision || editor.value === saved) return;
    const text = editor.value;
    busy = true;
    render('正在保存…');
    let message = '';
    try {
      const info = await write(text, revision);
      if (!info?.revision) throw new Error('未收到保存回执，请重新读取核对');
      revision = info.revision;
      saved = text; needsRead = false;
      message = '已保存，下次 QQ 会话生效。';
    } catch (error) {
      message = error.message + '。草稿已保留。';
      if (error.status === 409) needsRead = true;
    } finally {
      busy = false;
      render(message);
    }
  }
  editor.addEventListener('input', () => render());
  editor.addEventListener('keydown', event => {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 's') {
      event.preventDefault(); void persist();
    }
  });
  undo.onclick = () => { editor.value = saved; render(); editor.focus(); };
  retry.onclick = () => {
    if (editor.value === saved || window.confirm('重新读取会放弃当前草稿，继续吗？')) void load();
  };
  save.onclick = () => void persist();
  render();
  return {
    root,
    async open() { if (!revision) await load(); },
    update(declaration) {
      const available = declaration?.status === 'available';
      lastDeclaration = available ? declaration.text : '';
      documentText.textContent = lastDeclaration || '当前没有可显示的 QQ 通道声明。';
      note.textContent = '';
      name.textContent = botName() || '';
      name.hidden = !name.textContent;
    },
    unavailable() {
      if (!lastDeclaration) documentText.textContent = '暂时无法读取 QQ 通道声明。';
      note.textContent = lastDeclaration ? '状态更新失败，显示上次读取的声明。' : '';
    }
  };
}
