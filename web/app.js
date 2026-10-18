import createRaftSim from './raftsim.mjs?v=__VERSION__';

const $ = (id) => document.getElementById(id);
const SVG = 'http://www.w3.org/2000/svg';
const N = 5;
const CENTER = { x: 260, y: 214 };
const RADIUS = 160;

// Version stamp keeps browsers from pairing a cached module with a newer page.
const Module = await createRaftSim({ locateFile: (file) => `${file}?v=__VERSION__` });
let sim = new Module.Simulation();
let state = JSON.parse(sim.state());
let speed = 1;
let paused = false;
let carry = 0;
let lastFrame = performance.now();
let lastEventCount = 0;
let lastEventStamp = '';

// ---------- layout ----------
const pos = Array.from({ length: N }, (_, i) => {
  const a = -Math.PI / 2 + (i * 2 * Math.PI) / N;
  return { x: CENTER.x + RADIUS * Math.cos(a), y: CENTER.y + RADIUS * Math.sin(a) };
});
const idx = (id) => Number(id.slice(1)) - 1;

const svg = $('cluster');
const layer = (name) => {
  const g = document.createElementNS(SVG, 'g');
  g.setAttribute('class', name);
  svg.append(g);
  return g;
};
const linksLayer = layer('links');
const msgLayer = layer('msgs');
const nodeLayer = layer('nodes');

const el = (tag, attrs, parent) => {
  const e = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  if (parent) parent.append(e);
  return e;
};

const links = [];
for (let i = 0; i < N; i++) {
  for (let j = i + 1; j < N; j++) {
    links.push({ i, j, line: el('line', { x1: pos[i].x, y1: pos[i].y, x2: pos[j].x, y2: pos[j].y, class: 'link' }, linksLayer) });
  }
}

const TIMER_R = 41;
const TIMER_LEN = 2 * Math.PI * TIMER_R;
const nodeEls = pos.map((p, i) => {
  const g = el('g', { class: 'node', transform: `translate(${p.x} ${p.y})`, tabindex: 0, role: 'button' }, nodeLayer);
  el('circle', { r: TIMER_R, class: 'timer-track' }, g);
  const timer = el('circle', { r: TIMER_R, class: 'timer', 'stroke-dasharray': `0 ${TIMER_LEN}`, transform: 'rotate(-90)' }, g);
  const body = el('circle', { r: 34, class: 'body' }, g);
  const cross = el('path', { d: 'M-12 -12 L12 12 M12 -12 L-12 12', class: 'cross' }, g);
  const name = el('text', { class: 'name', y: -4 }, g);
  name.textContent = `n${i + 1}`;
  const term = el('text', { class: 'term', y: 13 }, g);
  const role = el('text', { class: 'role', y: 58 }, g);
  g.addEventListener('click', () => toggleNode(i));
  g.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      toggleNode(i);
    }
  });
  return { g, timer, body, term, role, cross };
});

// ---------- rendering ----------
function render() {
  $('clock').textContent = `${(state.now / 1000).toFixed(1)} s`;
  const sides = state.nodes.map((n) => n.side);

  for (const l of links) {
    const cut = sides[l.i] !== sides[l.j];
    l.line.classList.toggle('cut', cut);
  }

  state.nodes.forEach((n, i) => {
    const e = nodeEls[i];
    const cls = !n.alive ? 'dead' : n.role;
    e.g.setAttribute('class', `node ${cls}${n.side ? ' isolated' : ''}`);
    e.g.setAttribute('aria-label', n.alive ? `${n.id}, ${n.role}, term ${n.term}. Activate to crash.` : `${n.id}, crashed. Activate to restart.`);
    e.term.textContent = n.alive ? `term ${n.term}` : '';
    e.role.textContent = !n.alive ? 'crashed' : n.role === 'leader' ? 'LEADER' : n.role;
    const t = n.alive && n.role !== 'leader' ? n.timer : 0;
    e.timer.setAttribute('stroke-dasharray', `${t * TIMER_LEN} ${TIMER_LEN}`);
  });

  // Messages: dots travelling along the links.
  const frag = document.createDocumentFragment();
  for (const m of state.msgs) {
    if (m.done && !m.dropped) continue;
    const a = pos[idx(m.from)];
    const b = pos[idx(m.to)];
    const p = m.done ? 1 : m.p;
    // Stop short of the circles so dots appear to leave and enter the nodes.
    const dx = b.x - a.x;
    const dy = b.y - a.y;
    const len = Math.hypot(dx, dy);
    const start = 36 / len;
    const t = start + (1 - 2 * start) * p;
    const x = a.x + dx * t;
    const y = a.y + dy * t;
    if (m.dropped) {
      const g = el('g', { class: 'msg dropped', transform: `translate(${x} ${y})` });
      el('path', { d: 'M-5 -5 L5 5 M5 -5 L-5 5' }, g);
      frag.append(g);
    } else {
      el('circle', { cx: x, cy: y, r: m.kind === 'heartbeat' || m.kind === 'append-ok' ? 4 : 6, class: `msg ${m.kind}` }, frag);
    }
  }
  msgLayer.replaceChildren(frag);

  renderWrites();
  renderEvents();
  renderNodes();
  renderTargets();
}

function renderWrites() {
  const label = { pending: 'waiting for majority', committed: 'committed', lost: 'never committed' };
  $('writes').replaceChildren(...[...state.writes].reverse().map((w) => {
    const li = document.createElement('li');
    li.className = w.status;
    li.innerHTML = `<code>${esc(w.key)}=${esc(w.value)}</code><span>via ${w.node} · index ${w.index}</span><b>${label[w.status]}</b>`;
    return li;
  }));
}

function renderEvents() {
  const ev = state.events;
  const stamp = ev.length ? `${ev[ev.length - 1].t}:${ev[ev.length - 1].text}` : '';
  if (ev.length === lastEventCount && stamp === lastEventStamp) return;
  lastEventCount = ev.length;
  lastEventStamp = stamp;
  $('events').replaceChildren(...[...ev].reverse().map((e) => {
    const li = document.createElement('li');
    li.innerHTML = `<time>${(e.t / 1000).toFixed(1)}s</time><span>${esc(e.text)}</span>`;
    return li;
  }));
}

function renderNodes() {
  const grid = $('node-grid');
  if (grid.children.length !== N) {
    grid.replaceChildren(...state.nodes.map(() => {
      const d = document.createElement('article');
      d.className = 'card';
      return d;
    }));
  }
  state.nodes.forEach((n, i) => {
    const card = grid.children[i];
    if (!n.alive) {
      card.className = 'card dead';
      card.innerHTML = `<header><b>${n.id}</b><span class="badge">crashed</span></header><p class="muted">Process down. Term, vote and log are safe on disk.</p>`;
      return;
    }
    card.className = `card ${n.role}`;
    const log = n.log.map((e) => {
      const committed = e.i <= n.commit;
      return `<li class="${committed ? 'committed' : 'uncommitted'} t${e.t % 6}" title="index ${e.i}, term ${e.t}${committed ? ', committed' : ', not committed yet'}"><small>${e.i}·t${e.t}</small>${esc(e.c)}</li>`;
    }).join('');
    const kv = Object.entries(n.kv).map(([k, v]) => `<li><code>${esc(k)}</code>${esc(v)}</li>`).join('') || '<li class="muted">empty</li>';
    card.innerHTML = `
      <header><b>${n.id}</b><span class="badge">${n.role}</span></header>
      <dl>
        <div><dt>term</dt><dd>${n.term}</dd></div>
        <div><dt>voted for</dt><dd>${n.votedFor || '–'}</dd></div>
        <div><dt>commit</dt><dd>${n.commit}</dd></div>
        <div><dt>applied</dt><dd>${n.applied}</dd></div>
      </dl>
      <h3>Log <span class="muted">(last 8)</span></h3>
      <ol class="entries">${log || '<li class="muted empty">empty</li>'}</ol>
      <h3>Key-value state</h3>
      <ul class="kv">${kv}</ul>`;
  });
}

function renderTargets() {
  const sel = $('target');
  if (sel.options.length === N + 1) return;
  for (let i = 0; i < N; i++) {
    const o = document.createElement('option');
    o.value = String(i);
    o.textContent = `n${i + 1} (even if it is not the leader)`;
    sel.append(o);
  }
}

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// ---------- loop ----------
// A timer rather than requestAnimationFrame, so the cluster keeps running
// (throttled) in background tabs instead of freezing.
function frame() {
  const nowMs = performance.now();
  const dt = Math.min(100, nowMs - lastFrame);
  lastFrame = nowMs;
  if (!paused) {
    carry += dt * speed;
    const steps = Math.floor(carry / 5) * 5;
    if (steps > 0) {
      carry -= steps;
      sim.step(steps);
    }
  }
  state = JSON.parse(sim.state());
  render();
}

function refresh() {
  state = JSON.parse(sim.state());
  render();
}

// ---------- controls ----------
function toggleNode(i) {
  if (state.nodes[i].alive) sim.crash(i);
  else sim.restartNode(i);
  refresh();
}

$('crash-leader').addEventListener('click', () => {
  const leader = sim.leader();
  if (leader >= 0) sim.crash(leader);
  refresh();
});
$('cut-leader').addEventListener('click', () => {
  sim.partitionLeader();
  refresh();
});
$('heal').addEventListener('click', () => {
  sim.heal();
  refresh();
});
$('restart-all').addEventListener('click', () => {
  state.nodes.forEach((n, i) => {
    if (!n.alive) sim.restartNode(i);
  });
  refresh();
});

$('put-form').addEventListener('submit', (e) => {
  e.preventDefault();
  const f = new FormData(e.target);
  sim.put(String(f.get('key')), String(f.get('value')), Number(f.get('target')));
  // Suggest a fresh value for the next write.
  const v = e.target.elements.value;
  const words = ['blue', 'green', 'amber', 'violet', 'teal', 'coral', 'indigo', 'olive'];
  v.value = words[(words.indexOf(v.value) + 1) % words.length];
  refresh();
});
$('get-btn').addEventListener('click', () => {
  const key = $('put-form').elements.key.value;
  sim.get(key);
  refresh();
});

for (const r of document.querySelectorAll('[name=speed]')) {
  r.addEventListener('change', () => {
    speed = Number(r.value);
  });
}
$('pause-btn').addEventListener('click', (e) => {
  paused = !paused;
  e.target.textContent = paused ? 'Resume' : 'Pause';
});
$('reset-btn').addEventListener('click', () => {
  sim.delete();
  sim = new Module.Simulation();
  lastEventCount = -1;
  refresh();
});

$('app').setAttribute('aria-busy', 'false');
lastFrame = performance.now();
setInterval(frame, 16);
