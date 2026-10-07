(() => {
  'use strict';

  // ---- language
  const root = document.documentElement;
  const langButton = document.getElementById('lang');
  const setLang = (lang) => {
    root.lang = lang;
    langButton.textContent = lang === 'ja' ? 'English' : '日本語';
    try { localStorage.setItem('lang', lang); } catch (e) {}
  };
  let saved = null;
  try { saved = localStorage.getItem('lang'); } catch (e) {}
  setLang(saved || ((navigator.language || '').startsWith('ja') ? 'ja' : 'en'));
  langButton.addEventListener('click', () => setLang(root.lang === 'ja' ? 'en' : 'ja'));

  // ---- examples use this server's host
  const host = location.host;
  if (host && !/^(localhost|127\.)/.test(host)) {
    document.querySelectorAll('.host-name').forEach((e) => { e.textContent = host; });
  }
  const wsBase = (location.protocol === 'https:' ? 'wss://' : 'ws://') + (host || 'localhost:8080') + '/ws';

  // ---- copy buttons
  const copy = async (text, button) => {
    try {
      await navigator.clipboard.writeText(text);
      button.classList.add('done');
      const old = button.textContent;
      button.textContent = 'copied';
      setTimeout(() => { button.classList.remove('done'); button.textContent = old; }, 1200);
    } catch (e) {}
  };
  document.querySelectorAll('.code').forEach((block) => {
    const b = document.createElement('button');
    b.className = 'copy';
    b.type = 'button';
    b.textContent = 'copy';
    b.addEventListener('click', () => {
      const lines = block.querySelector('pre').innerText.split('\n').filter((l) => !/^\s*"/.test(l) && l.trim() !== '');
      copy(lines.join('\n'), b);
    });
    block.appendChild(b);
  });

  // ---- public sessions
  const list = document.getElementById('session-list');
  const live = document.getElementById('live');
  const el = (tag, cls, text) => {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined) e.textContent = text;
    return e;
  };
  const bilingual = (en, ja) => {
    const f = document.createDocumentFragment();
    const a = el('span', '', en); a.lang = 'en';
    const b = el('span', '', ja); b.lang = 'ja';
    f.append(a, b);
    return f;
  };
  const ago = (iso) => {
    const s = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
    if (s < 60) return ['just now', 'たった今'];
    if (s < 3600) return [`${Math.floor(s / 60)} min ago`, `${Math.floor(s / 60)} 分前`];
    return [`${Math.floor(s / 3600)} h ago`, `${Math.floor(s / 3600)} 時間前`];
  };
  const refresh = async () => {
    let sessions;
    try {
      const r = await fetch('/sessions', { cache: 'no-store' });
      if (!r.ok) throw new Error(r.status);
      sessions = await r.json();
    } catch (e) {
      return;
    }
    list.replaceChildren();
    if (sessions.length === 0) {
      const p = el('p', 'empty');
      p.append(bilingual('No public sessions right now. Start one with :YosegakiShare public.',
        '今は公開中のセッションはありません。:YosegakiShare public で始められます。'));
      list.append(p);
    }
    for (const s of sessions) {
      const row = el('div', 'session');
      const info = el('div');
      info.append(el('div', 'title', s.title || '[No Name]'));
      const meta = el('div', 'meta');
      const [en, ja] = ago(s.created);
      meta.append(bilingual(`hosted by ${s.host} · ${s.people} ${s.people === 1 ? 'person' : 'people'} · ${en}`,
        `ホスト ${s.host} · ${s.people} 人 · ${ja}`));
      info.append(meta);
      const b = el('button');
      b.type = 'button';
      b.textContent = ':YosegakiJoin';
      b.title = 'copy the join command';
      b.addEventListener('click', () => copy(`:YosegakiJoin ${wsBase}/${s.id}`, b));
      row.append(info, b);
      list.append(row);
    }
    live.hidden = false;
    live.replaceChildren(bilingual(
      `${sessions.length} public ${sessions.length === 1 ? 'session' : 'sessions'} on this server right now`,
      `このサーバで今 ${sessions.length} 件のセッションが公開中`));
  };
  refresh();
  setInterval(refresh, 15000);

  // ---- demo: two Vims editing the same buffer
  const panes = [...document.querySelectorAll('.pane')];
  if (!panes.length) return;
  const reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  const initial = [
    'package main',
    '',
    'import "fmt"',
    '',
    'func main() {',
    '    fmt.Println("hello")',
    '}',
  ];
  // Each actor moves to the end of a line and types; both run at once.
  const script = {
    mattn: [
      { wait: 6 },
      { moveEnd: '    fmt.Println("hello")' },
      { type: '\n    fmt.Println(greet("vim"))' },
      { wait: 10 },
      { moveEnd: 'package main' },
      { type: ' // 寄せ書き' },
    ],
    alice: [
      { wait: 14 },
      { moveEnd: '}' },
      { type: '\n\nfunc greet(name string) string {\n    return "hi, " + name\n}' },
    ],
  };
  const SYNTAX = /("(?:[^"\\]|\\.)*")|(\/\/.*$)|\b(package|import|func|return|string)\b/g;

  let lines, cursors, queues;
  const reset = () => {
    lines = initial.slice();
    cursors = { mattn: { l: 0, c: 0 }, alice: { l: 2, c: 0 } };
    queues = { mattn: script.mattn.map((s) => ({ ...s })), alice: script.alice.map((s) => ({ ...s })) };
  };

  const insert = (who, ch) => {
    const cur = cursors[who];
    const line = [...lines[cur.l]];
    for (const [name, o] of Object.entries(cursors)) {
      if (name === who) continue;
      if (ch === '\n') {
        if (o.l > cur.l) o.l++;
        else if (o.l === cur.l && o.c > cur.c) { o.l++; o.c -= cur.c; }
      } else if (o.l === cur.l && o.c > cur.c) {
        o.c++;
      }
    }
    if (ch === '\n') {
      lines.splice(cur.l, 1, line.slice(0, cur.c).join(''), line.slice(cur.c).join(''));
      cur.l++;
      cur.c = 0;
    } else {
      line.splice(cur.c, 0, ch);
      lines[cur.l] = line.join('');
      cur.c++;
    }
  };

  const step = (who) => {
    const q = queues[who];
    while (q.length) {
      const s = q[0];
      if (s.wait) { s.wait--; if (s.wait <= 0) q.shift(); return true; }
      if (s.moveEnd) {
        const l = lines.indexOf(s.moveEnd);
        cursors[who] = { l, c: [...lines[l]].length };
        q.shift();
        continue;
      }
      if (s.type) {
        const chars = [...s.type];
        insert(who, chars.shift());
        s.type = chars.join('');
        if (!s.type) q.shift();
        return true;
      }
    }
    return false;
  };

  const highlight = (text) => {
    const out = [];
    let last = 0;
    text.replace(SYNTAX, (m, str, cm, kw, off) => {
      out.push([text.slice(last, off), '']);
      out.push([m, str ? 'str' : cm ? 'cm' : 'kw']);
      last = off + m.length;
    });
    out.push([text.slice(last), '']);
    const classes = [];
    for (const [t, cls] of out) for (const ch of t) classes.push(cls);
    return classes;
  };

  const render = () => {
    for (const pane of panes) {
      const self = pane.dataset.self;
      const other = self === 'mattn' ? 'alice' : 'mattn';
      const screen = pane.querySelector('.screen');
      const frag = document.createDocumentFragment();
      const rows = 12;
      for (let i = 0; i < rows; i++) {
        if (i >= lines.length) {
          frag.append(el('span', 'tilde', '~'), '\n');
          continue;
        }
        const chars = [...lines[i]];
        const classes = highlight(lines[i]);
        const width = chars.length + 1;
        for (let c = 0; c < width; c++) {
          const ch = c < chars.length ? chars[c] : ' ';
          let cls = classes[c] || '';
          if (cursors[self].l === i && cursors[self].c === c) cls = 'cur-self';
          else if (cursors[other].l === i && cursors[other].c === c) cls = `cur-${other}`;
          if (c === chars.length && !cls) continue;
          frag.append(cls ? el('span', cls, ch) : ch);
        }
        if (cursors[other].l === i) {
          frag.append(el('span', `label ${other}`, `${other} #${other === 'mattn' ? 1 : 2}`));
        }
        frag.append('\n');
      }
      screen.replaceChildren(frag);
    }
  };

  reset();
  if (reduced) {
    while (step('mattn') | step('alice')) {}
    render();
    return;
  }
  let pause = 0;
  setInterval(() => {
    if (pause > 0) {
      if (--pause === 0) reset();
      render();
      return;
    }
    const a = step('mattn');
    const b = step('alice');
    if (!a && !b) pause = 40;
    render();
  }, 85);
})();
