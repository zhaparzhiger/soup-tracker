// No framework on purpose. The run page is a pure function of an ordered log of points,
// which keeps "what the page shows" easy to reason about and easy to assert on in a test.

const KEY_STORE = 'tracker.apiKey';
const STALL_MS = 12000; // server heartbeats every 5 s; silence this long means a dead connection
const TABLE_LIMIT = 5000;

const $app = document.getElementById('app');
const $project = document.getElementById('project');
const $disconnect = document.getElementById('disconnect');

let apiKey = sessionStorage.getItem(KEY_STORE) || '';
let cleanup = () => {};

// ---------- tiny DOM helper: text only, never innerHTML ----------

function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === 'class') el.className = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else if (v === true) el.setAttribute(k, '');
    else if (v !== false && v != null) el.setAttribute(k, v);
  }
  for (const c of children.flat()) if (c != null) el.append(c);
  return el;
}

// ---------- API ----------

class ApiError extends Error {
  constructor(status, body) {
    super(body?.detail || body?.error || `HTTP ${status}`);
    this.status = status;
  }
}

async function api(path, { method = 'GET', body, key = apiKey } = {}) {
  const res = await fetch(path, {
    method,
    cache: 'no-store',
    headers: { ...(key ? { Authorization: `Bearer ${key}` } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => null);
  if (!res.ok) throw new ApiError(res.status, data);
  return data;
}

function setKey(key) {
  apiKey = key;
  if (key) sessionStorage.setItem(KEY_STORE, key);
  else sessionStorage.removeItem(KEY_STORE);
}

function signOut(message) {
  setKey('');
  route(message);
}

// ---------- live feed: one cursor, resumed after every disconnect ----------

class RunFeed {
  constructor(runId, onChange) {
    this.runId = runId;
    this.onChange = onChange;
    this.cursor = 0; // seq of the last point applied
    this.head = null; // last seq the server said we are caught up to
    this.series = new Map(); // key -> { steps: number[], values: number[] } sorted by step
    this.status = 'connecting';
    this.count = 0;
    this.reconnects = 0;
    this.skipped = 0; // points at or below the cursor (replayed), ignored
    this.gaps = 0; // a seq jumped ahead: we resync from the cursor
    this.stalls = 0; // silent connections cut by the watchdog
    this.lastEventAt = null;
    this.stopped = false;
    this.abort = null;
  }

  start() {
    void this.loop();
  }

  stop() {
    this.stopped = true;
    this.abort?.abort();
  }

  setStatus(status) {
    if (this.status !== status) {
      this.status = status;
      this.onChange();
    }
  }

  async loop() {
    let attempt = 0;
    while (!this.stopped) {
      const before = this.cursor;
      const outcome = await this.connectOnce();
      if (this.stopped) return;
      if (outcome === 'unauthorized' || outcome === 'not_found') return this.setStatus(outcome);
      attempt = this.cursor > before ? 0 : attempt + 1; // progress resets the backoff
      this.reconnects++;
      this.setStatus('reconnecting');
      const delay = Math.min(5000, 300 * 2 ** attempt) * (0.5 + Math.random() / 2);
      await new Promise((r) => setTimeout(r, delay));
    }
  }

  /** One connection. Resolves with why it ended; never throws. */
  async connectOnce() {
    const abort = (this.abort = new AbortController());
    let watchdog;
    const alive = () => {
      clearTimeout(watchdog);
      watchdog = setTimeout(() => {
        this.stalls++;
        abort.abort();
      }, STALL_MS);
    };
    alive(); // also covers a connect that never answers
    try {
      const res = await fetch(`/runs/${this.runId}/events`, {
        cache: 'no-store',
        signal: abort.signal,
        headers: { Authorization: `Bearer ${apiKey}`, Accept: 'text/event-stream', 'Last-Event-ID': String(this.cursor) },
      });
      if (res.status === 401) return 'unauthorized';
      if (res.status === 404) return 'not_found';
      if (!res.ok || !res.body) return 'retry';
      this.setStatus('catching up');
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      for (;;) {
        const { value, done } = await reader.read();
        if (done) return 'retry';
        alive();
        buffer += decoder.decode(value, { stream: true });
        let end;
        while ((end = buffer.indexOf('\n\n')) >= 0) {
          const outcome = this.handle(buffer.slice(0, end));
          buffer = buffer.slice(end + 2);
          if (outcome) {
            abort.abort();
            return outcome;
          }
        }
      }
    } catch {
      return 'retry';
    } finally {
      clearTimeout(watchdog);
    }
  }

  /** Applies one SSE message. Returns a reason to drop the connection, or undefined. */
  handle(message) {
    let event = 'message';
    let data = '';
    for (const line of message.split('\n')) {
      if (line.startsWith('event:')) event = line.slice(6).trim();
      else if (line.startsWith('data:')) data += line.slice(5).trim();
    }
    if (event === 'revoked') return 'unauthorized';
    if (event === 'sync') {
      this.head = JSON.parse(data).seq;
      this.setStatus(this.head === this.cursor ? 'live' : 'catching up');
      return;
    }
    if (event !== 'points') return; // heartbeat comment or unknown event
    for (const [seq, key, step, value] of JSON.parse(data)) {
      if (seq <= this.cursor) {
        this.skipped++;
        continue;
      }
      if (seq !== this.cursor + 1) {
        this.gaps++; // never apply past a hole: resume from the cursor instead
        return 'retry';
      }
      this.insert(key, step, value);
      this.cursor = seq;
    }
    this.lastEventAt = new Date();
    this.onChange(); // status stays as it is until the server's next `sync`
  }

  /** Display order is by step, so a late point is an insert in the middle. */
  insert(key, step, value) {
    let s = this.series.get(key);
    if (!s) this.series.set(key, (s = { steps: [], values: [] }));
    let lo = 0;
    let hi = s.steps.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (s.steps[mid] < step) lo = mid + 1;
      else hi = mid;
    }
    s.steps.splice(lo, 0, step);
    s.values.splice(lo, 0, value);
    this.count++;
  }
}

// ---------- views ----------

function viewConnect(message) {
  const error = h('p', { class: 'error', 'data-testid': 'connect-error' }, message || '');
  const nameInput = h('input', { name: 'name', placeholder: 'Project name', maxlength: '100', required: true, 'data-testid': 'project-name' });
  const keyInput = h('input', { name: 'key', placeholder: 'sk_…', autocomplete: 'off', spellcheck: 'false', required: true, 'data-testid': 'api-key-input' });

  const create = h('form', { class: 'card', onsubmit: async (e) => {
    e.preventDefault();
    try {
      const { api_key } = await api('/projects', { method: 'POST', body: { name: nameInput.value }, key: '' });
      setKey(api_key);
      $app.replaceChildren(viewNewKey(api_key));
    } catch (err) {
      error.textContent = err.message;
    }
  } }, h('h2', null, 'Create a project'), nameInput, h('button', { type: 'submit' }, 'Create project'));

  const connect = h('form', { class: 'card', onsubmit: async (e) => {
    e.preventDefault();
    const key = keyInput.value.trim();
    try {
      await api('/project', { key });
      setKey(key);
      route();
    } catch (err) {
      error.textContent = err.status === 401 ? 'That key is not valid (or was rotated).' : err.message;
    }
  } }, h('h2', null, 'Use an API key'), keyInput, h('button', { type: 'submit', 'data-testid': 'connect' }, 'Connect'));

  return h('div', { class: 'narrow' }, error, connect, create);
}

function viewNewKey(key) {
  return h('div', { class: 'narrow' }, h('div', { class: 'card' },
    h('h2', null, 'Your API key'),
    h('p', null, 'Save it now. It is shown only once; the server keeps just its hash.'),
    h('pre', { class: 'key', 'data-testid': 'new-key' }, key),
    h('button', { onclick: () => route() }, 'Continue'),
  ));
}

function viewRuns() {
  const tbody = h('tbody', { 'data-testid': 'run-list' });
  const note = h('p', { class: 'muted' });
  const nameInput = h('input', { placeholder: 'Run name', maxlength: '200', required: true, 'data-testid': 'run-name' });

  let shown = '';
  async function refresh() {
    try {
      const { runs } = await api('/runs');
      const fingerprint = JSON.stringify(runs);
      if (fingerprint !== shown) tbody.replaceChildren(...runs.map((r) => h('tr', { 'data-run-id': r.id },
        h('td', null, h('a', { href: `#/runs/${r.id}` }, r.name)),
        h('td', { class: 'num' }, String(r.last_seq)),
        h('td', { class: 'num' }, String(r.conflicts)),
        h('td', { class: 'muted' }, new Date(r.updated_at).toLocaleString()),
        h('td', { class: 'mono muted' }, r.id),
      )));
      shown = fingerprint;
      note.textContent = runs.length ? `Refreshed ${new Date().toLocaleTimeString()}` : 'No runs yet. Create one, then send metrics to it.';
    } catch (err) {
      if (err.status === 401) return signOut('Your API key is no longer valid.');
      note.textContent = `Could not load runs: ${err.message}`;
    }
  }

  const form = h('form', { class: 'row', onsubmit: async (e) => {
    e.preventDefault();
    try {
      const { run } = await api('/runs', { method: 'POST', body: { name: nameInput.value } });
      location.hash = `#/runs/${run.id}`;
    } catch (err) {
      note.textContent = err.message;
    }
  } }, nameInput, h('button', { type: 'submit', 'data-testid': 'create-run' }, 'New run'));

  void refresh();
  const timer = setInterval(refresh, 3000);
  cleanup = () => clearInterval(timer);

  return h('div', null,
    h('h1', null, 'Runs'), form,
    h('table', null, h('thead', null, h('tr', null,
      h('th', null, 'Name'), h('th', { class: 'num' }, 'Points'), h('th', { class: 'num' }, 'Conflicts'), h('th', null, 'Last update'), h('th', null, 'Run id'))), tbody),
    note,
  );
}

function viewRun(runId) {
  const title = h('h1', { 'data-testid': 'run-title' }, 'Run');
  const status = h('span', { class: 'badge', 'data-testid': 'status' }, 'connecting');
  const synced = h('b', { 'data-testid': 'synced-seq' }, '0');
  const count = h('b', { 'data-testid': 'point-count' }, '0');
  const diag = h('span', { class: 'muted', 'data-testid': 'diagnostics' });
  const summary = h('tbody', { 'data-testid': 'summary' });
  const charts = h('div', { class: 'charts' });
  const dataBody = h('tbody', { 'data-testid': 'data-rows' });
  const dataNote = h('p', { class: 'muted', 'data-testid': 'data-note' });
  const dataWrap = h('div', { hidden: true }, dataNote, h('table', { class: 'data' },
    h('thead', null, h('tr', null, h('th', null, 'Metric'), h('th', { class: 'num' }, 'Step'), h('th', { class: 'num' }, 'Value'))), dataBody));
  const toggle = h('button', { class: 'link', 'data-testid': 'toggle-data', onclick: () => {
    dataWrap.hidden = !dataWrap.hidden;
    toggle.textContent = dataWrap.hidden ? 'Show raw data' : 'Hide raw data';
    render();
  } }, 'Show raw data');

  const LABEL = {
    connecting: 'connecting', 'catching up': 'catching up', live: 'live', reconnecting: 'reconnecting — showing last synced data',
    unauthorized: 'API key revoked', not_found: 'run not found',
  };

  function render() {
    scheduled = false;
    status.textContent = LABEL[feed.status] || feed.status;
    status.dataset.state = feed.status;
    synced.textContent = String(feed.cursor);
    count.textContent = String(feed.count);
    diag.textContent = `reconnects ${feed.reconnects} · replayed points skipped ${feed.skipped} · gaps ${feed.gaps} · stalls ${feed.stalls}` +
      (feed.lastEventAt ? ` · last data ${feed.lastEventAt.toLocaleTimeString()}` : '');

    const keys = [...feed.series.keys()].sort();
    summary.replaceChildren(...keys.map((key) => {
      const s = feed.series.get(key);
      const n = s.steps.length;
      const [min, max] = range(s.values);
      return h('tr', { 'data-key': key },
        h('td', { class: 'mono' }, key), h('td', { class: 'num' }, String(n)),
        h('td', { class: 'num' }, String(s.steps[n - 1])), h('td', { class: 'num' }, short(s.values[n - 1])),
        h('td', { class: 'num' }, short(min)), h('td', { class: 'num' }, short(max)));
    }));
    charts.replaceChildren(...keys.map((key) => chart(key, feed.series.get(key))));

    if (!dataWrap.hidden) {
      const rows = [];
      for (const key of keys) {
        const s = feed.series.get(key);
        for (let i = 0; i < s.steps.length && rows.length < TABLE_LIMIT; i++) {
          rows.push(h('tr', null, h('td', { class: 'mono' }, key), h('td', { class: 'num' }, String(s.steps[i])), h('td', { class: 'num' }, String(s.values[i]))));
        }
      }
      dataBody.replaceChildren(...rows);
      dataNote.textContent = feed.count > TABLE_LIMIT ? `Showing the first ${TABLE_LIMIT} of ${feed.count} points.` : `All ${feed.count} points.`;
    }
    if (feed.status === 'unauthorized') signOut('Your API key was revoked. Enter the new key.');
  }

  let scheduled = false;
  const feed = new RunFeed(runId, () => {
    if (scheduled) return;
    scheduled = true;
    setTimeout(render, dataWrap.hidden ? 60 : 500); // coalesce bursts into one repaint
  });
  window.__feed = feed; // for debugging in the console

  api(`/runs/${runId}`).then(({ run }) => { title.textContent = run.name; }).catch((err) => {
    if (err.status === 401) signOut('Your API key is no longer valid.');
    else title.textContent = err.status === 404 ? 'Run not found' : 'Run';
  });
  feed.start();
  cleanup = () => feed.stop();

  return h('div', null,
    h('p', null, h('a', { href: '#/' }, '← Runs')),
    title,
    h('p', { class: 'statusbar' }, status, ' synced to seq ', synced, ' · points ', count, ' · ', diag),
    h('table', null, h('thead', null, h('tr', null, h('th', null, 'Metric'), h('th', { class: 'num' }, 'Points'),
      h('th', { class: 'num' }, 'Last step'), h('th', { class: 'num' }, 'Last value'), h('th', { class: 'num' }, 'Min'), h('th', { class: 'num' }, 'Max'))), summary),
    charts, toggle, dataWrap,
  );
}

/** Six significant digits for the summary. The raw data table shows the exact value. */
const short = (v) => String(Number(v.toPrecision(6)));

/** [min, max] with a loop: Math.min(...values) overflows the stack on long runs. */
function range(values) {
  let min = Infinity, max = -Infinity;
  for (const v of values) { if (v < min) min = v; if (v > max) max = v; }
  return [min, max];
}

const SVG = 'http://www.w3.org/2000/svg';
function chart(key, s) {
  const W = 640, H = 170, P = 8, MAX = 2000;
  const n = s.steps.length;
  const stride = Math.ceil(n / MAX); // the line is thinned for long runs; the table is not
  const x0 = s.steps[0], x1 = s.steps[n - 1];
  const [y0, y1] = range(s.values);
  const sx = (x) => P + (x1 === x0 ? 0.5 : (x - x0) / (x1 - x0)) * (W - 2 * P);
  const sy = (y) => H - P - (y1 === y0 ? 0.5 : (y - y0) / (y1 - y0)) * (H - 2 * P);
  let pts = '';
  for (let i = 0; i < n; i += stride) pts += `${sx(s.steps[i]).toFixed(1)},${sy(s.values[i]).toFixed(1)} `;
  const svg = document.createElementNS(SVG, 'svg');
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  const line = document.createElementNS(SVG, 'polyline');
  line.setAttribute('points', pts.trim());
  svg.append(line);
  return h('figure', { 'data-key': key },
    h('figcaption', null, h('span', { class: 'mono' }, key), h('span', { class: 'muted' }, ` step ${x0}–${x1} · ${n} points${stride > 1 ? ` · line shows every ${stride}th` : ''}`)),
    svg);
}

// ---------- router ----------

async function route(message) {
  cleanup();
  cleanup = () => {};
  $disconnect.hidden = !apiKey;
  $project.textContent = '';
  if (!apiKey) return $app.replaceChildren(viewConnect(typeof message === 'string' ? message : ''));
  api('/project').then(({ project }) => { $project.textContent = project.name; }).catch((err) => {
    if (err.status === 401) signOut('Your API key is no longer valid.');
  });
  const run = /^#\/runs\/([0-9a-f-]{36})$/.exec(location.hash);
  $app.replaceChildren(run ? viewRun(run[1]) : viewRuns());
}

$disconnect.addEventListener('click', () => signOut(''));
window.addEventListener('hashchange', () => route());
route();
