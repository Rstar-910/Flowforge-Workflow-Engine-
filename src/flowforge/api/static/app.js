/* FlowForge dashboard. Plain JS, no dependencies.
 *
 * Every value from the API is inserted with textContent / setAttribute (never
 * innerHTML), and the page is served with a CSP that forbids inline scripts.
 */
"use strict";

const REFRESH_MS = 2000;
const WORKERS_EVERY = 3; // refresh workers every N ticks
const MAX_EVENTS = 500;
const STATUSES = ["BLOCKED", "READY", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "FAILED",
  "UPSTREAM_FAILED", "CANCELLED"];
const TERMINAL_RUN = new Set(["SUCCEEDED", "FAILED", "CANCELLED"]);

const state = {
  key: safeGet("ff_key"),
  live: true,
  tick: 0,
  filter: "",
  runs: [],
  nextCursor: null,
  definitions: new Map(), // id -> {name, version}
  runId: null,
  run: null,
  tasks: [],
  taskId: null,
  attempts: [],
  events: [],
  eventsAfter: 0,
  isAdmin: null, // unknown until /v1/workers answers
  busy: false,
};

// ---------------------------------------------------------------- helpers

function safeGet(k) { try { return sessionStorage.getItem(k); } catch { return null; } }
function safeSet(k, v) { try { v == null ? sessionStorage.removeItem(k) : sessionStorage.setItem(k, v); } catch { /* storage unavailable */ } }

const $ = (id) => document.getElementById(id);

function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue;
    if (k === "class") el.className = v;
    else if (k === "text") el.textContent = v;
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v === true ? "" : v);
  }
  for (const c of children.flat()) {
    if (c == null || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}

const SVG_NS = "http://www.w3.org/2000/svg";
function s(tag, attrs = {}, ...children) {
  const el = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null) continue;
    if (k === "text") el.textContent = v;
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v);
  }
  for (const c of children.flat()) if (c) el.append(c);
  return el;
}

function badge(status) { return h("span", { class: `badge st-${status}`, text: status }); }

function ago(iso) {
  if (!iso) return "—";
  const sec = (Date.now() - new Date(iso).getTime()) / 1000;
  if (sec < 1) return "just now";
  if (sec < 60) return `${Math.floor(sec)}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return new Date(iso).toLocaleString();
}

function dur(fromIso, toIso) {
  if (!fromIso) return "—";
  const ms = (toIso ? new Date(toIso) : new Date()) - new Date(fromIso);
  return fmtMs(ms);
}

function fmtMs(ms) {
  if (ms == null) return "—";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)} s`;
  return `${Math.floor(ms / 60000)}m ${Math.round((ms % 60000) / 1000)}s`;
}

const shortId = (id) => (id ? id.slice(0, 8) : "");

function toast(msg, bad = false) {
  const t = $("toast");
  t.textContent = msg;
  t.className = bad ? "toast bad" : "toast";
  t.hidden = false;
  clearTimeout(toast._t);
  toast._t = setTimeout(() => { t.hidden = true; }, bad ? 6000 : 3000);
}

// ---------------------------------------------------------------- API

class ApiError extends Error {
  constructor(status, body) {
    const e = (body && body.error) || {};
    let msg = e.message || `HTTP ${status}`;
    if (Array.isArray(e.details) && e.details.length) {
      msg += ": " + e.details.map((d) => `${[].concat(d.loc || []).join(".")} ${d.msg}`).join("; ");
    }
    super(msg);
    this.status = status;
    this.code = e.code;
  }
}

async function api(path, { method = "GET", body, headers = {} } = {}) {
  const res = await fetch(path, {
    method,
    headers: {
      "X-API-Key": state.key || "",
      ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
      ...headers,
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
    cache: "no-store",
  });
  let data = null;
  const text = await res.text();
  if (text) { try { data = JSON.parse(text); } catch { data = null; } }
  if (!res.ok) {
    if (res.status === 401) signOut("Your API key was rejected.");
    throw new ApiError(res.status, data);
  }
  return data;
}

// ---------------------------------------------------------------- auth

function signOut(message) {
  state.key = null;
  safeSet("ff_key", null);
  $("app").hidden = true;
  $("controls").hidden = true;
  $("login").hidden = false;
  const err = $("login-error");
  err.hidden = !message;
  err.textContent = message || "";
  $("api-key").focus();
}

async function signIn(key) {
  state.key = key;
  try {
    await api("/v1/stats");
  } catch (e) {
    if (e.status !== 401) signOut(`Could not reach the API: ${e.message}`);
    return;
  }
  safeSet("ff_key", key);
  $("login").hidden = true;
  $("app").hidden = false;
  $("controls").hidden = false;
  state.isAdmin = null;
  await loadDefinitions();
  routeFromHash();
  await refresh(true);
}

// ---------------------------------------------------------------- data loading

async function loadDefinitions() {
  try {
    const data = await api("/v1/workflow-definitions?limit=200");
    state.definitions = new Map(data.items.map((d) => [d.id, d]));
  } catch (e) { toast(`Definitions: ${e.message}`, true); }
}

function defLabel(defId) {
  const d = state.definitions.get(defId);
  return d ? `${d.name} v${d.version}` : `definition ${shortId(defId)}`;
}

async function loadRuns(append = false) {
  const params = new URLSearchParams({ limit: "40" });
  if (state.filter) params.set("status", state.filter);
  if (append && state.nextCursor) params.set("cursor", state.nextCursor);
  const data = await api(`/v1/runs?${params}`);
  state.runs = append ? state.runs.concat(data.items) : data.items;
  state.nextCursor = data.next_cursor;
  if (state.runs.some((r) => !state.definitions.has(r.definition_id))) await loadDefinitions();
  renderRuns();
}

async function loadRun() {
  const id = state.runId;
  if (!id) return;
  const [run, tasks] = await Promise.all([api(`/v1/runs/${id}`), api(`/v1/runs/${id}/tasks`)]);
  if (id !== state.runId) return; // user navigated away meanwhile
  state.run = run;
  state.tasks = tasks.items;
  await loadEvents();
  if (state.taskId) await loadAttempts();
  renderRun();
}

async function loadEvents() {
  const id = state.runId;
  // Page through new events; cursor pagination on the event id.
  for (let i = 0; i < 5; i++) {
    const data = await api(`/v1/runs/${id}/events?after=${state.eventsAfter}&limit=500`);
    if (id !== state.runId) return;
    if (data.items.length) {
      state.events = state.events.concat(data.items).slice(-MAX_EVENTS);
      state.eventsAfter = data.items[data.items.length - 1].id;
    }
    if (!data.next_after) break;
  }
}

async function loadAttempts() {
  const tid = state.taskId;
  const data = await api(`/v1/tasks/${tid}/attempts`);
  if (tid === state.taskId) state.attempts = data.items;
}

async function loadStats() {
  const data = await api("/v1/stats");
  renderStats(data);
}

async function loadWorkers() {
  if (state.isAdmin === false) return;
  try {
    const data = await api("/v1/workers?limit=100");
    state.isAdmin = true;
    renderWorkers(data.items);
  } catch (e) {
    if (e.status === 403) { state.isAdmin = false; $("workers-panel").hidden = true; renderStats(null); }
    else throw e;
  }
}

async function refresh(force = false) {
  if (!state.key || state.busy) return;
  if (!force && (!state.live || document.hidden)) return;
  state.busy = true;
  try {
    const jobs = [loadStats(), loadRuns(), loadRun()];
    if (force || state.tick % WORKERS_EVERY === 0) jobs.push(loadWorkers());
    await Promise.all(jobs);
    state.tick++;
  } catch (e) {
    if (e.status !== 401) toast(`Refresh failed: ${e.message}`, true);
  } finally {
    state.busy = false;
  }
}

// ---------------------------------------------------------------- routing

function routeFromHash() {
  const m = location.hash.match(/^#\/runs\/([0-9a-f-]{36})(?:\/tasks\/([0-9a-f-]{36}))?$/);
  const runId = m ? m[1] : null;
  const taskId = m && m[2] ? m[2] : null;
  if (runId !== state.runId) {
    state.runId = runId;
    state.run = null;
    state.tasks = [];
    state.events = [];
    state.eventsAfter = 0;
    state.attempts = [];
    $("events").replaceChildren();
  }
  if (taskId !== state.taskId) { state.taskId = taskId; state.attempts = []; }
  renderSelection();
}

function openRun(id) { location.hash = `#/runs/${id}`; }
function openTask(id) { location.hash = `#/runs/${state.runId}/tasks/${id}`; }

window.addEventListener("hashchange", async () => {
  routeFromHash();
  if (!state.key) return; // logged out: signIn() loads the run after login
  try { await loadRun(); } catch (e) { if (e.status !== 401) toast(e.message, true); }
  renderRuns();
});

// ---------------------------------------------------------------- rendering

function renderSelection() {
  $("no-run").hidden = !!state.runId;
  $("run-view").hidden = !state.runId;
}

function renderStats(data) {
  if (data) renderStats.last = data;
  const d = renderStats.last;
  if (!d) return;
  const runs = d.runs || {};
  const tasks = d.active_tasks || {};
  const total = Object.values(runs).reduce((a, b) => a + b, 0);
  const tiles = [
    ["Runs", total],
    ["Running", (runs.RUNNING || 0) + (runs.PENDING || 0) + (runs.CANCELLING || 0)],
    ["Succeeded", runs.SUCCEEDED || 0],
    ["Failed", runs.FAILED || 0],
    ["Tasks ready", tasks.READY || 0],
    ["Tasks running", tasks.RUNNING || 0],
    ["Retry wait", tasks.RETRY_WAIT || 0],
  ];
  if (state.isAdmin && renderStats.workers != null) tiles.push(["Live workers", renderStats.workers]);
  $("stats").replaceChildren(...tiles.map(([label, value]) =>
    h("div", { class: "stat" }, h("div", { class: "label", text: label }), h("div", { class: "value", text: String(value) }))));
}

function renderRuns() {
  const list = $("runs");
  if (!state.runs.length) {
    list.replaceChildren(h("li", { class: "muted small", text: state.filter ? "No runs with this status." : "No runs yet." }));
  } else {
    list.replaceChildren(...state.runs.map((r) =>
      h("li", {},
        h("button", { type: "button", class: r.id === state.runId ? "selected" : "", onclick: () => openRun(r.id) },
          h("div", { class: "run-line" }, h("span", { class: "run-def", text: defLabel(r.definition_id) }), badge(r.status)),
          h("div", { class: "run-line muted small" },
            h("span", { class: "mono", text: shortId(r.id) }),
            h("span", { text: TERMINAL_RUN.has(r.status) ? `took ${dur(r.created_at, r.completed_at)}` : ago(r.created_at) }))))));
  }
  $("more-runs").hidden = !state.nextCursor;
}

function renderRun() {
  renderSelection();
  const r = state.run;
  if (!r) return;
  $("run-status").className = `badge st-${r.status}`;
  $("run-status").textContent = r.status;
  $("run-name").textContent = defLabel(r.definition_id);
  $("run-id").textContent = `run ${r.id}`;
  const facts = [
    ["Created", ago(r.created_at)],
    ["Duration", dur(r.started_at || r.created_at, r.completed_at)],
    ["Tasks", Object.entries(r.task_counts || {}).map(([k, v]) => `${v} ${k.toLowerCase()}`).join(", ") || "—"],
  ];
  if (r.idempotency_key) facts.push(["Idempotency key", r.idempotency_key]);
  if (r.deadline_at) {
    const left = (new Date(r.deadline_at) - Date.now()) / 1000;
    facts.push(["Deadline", r.deadline_exceeded_at ? "exceeded"
      : TERMINAL_RUN.has(r.status) ? new Date(r.deadline_at).toLocaleTimeString()
      : left > 0 ? `in ${fmtMs(left * 1000)}` : "due now"]);
  }
  $("run-facts").replaceChildren(...facts.map(([k, v]) => h("div", {}, h("dt", { text: k }), h("dd", { text: v }))));
  const fail = $("run-failure");
  fail.hidden = !r.failure_reason;
  fail.textContent = r.failure_reason || "";
  const cancel = $("cancel-run");
  cancel.hidden = TERMINAL_RUN.has(r.status);
  cancel.disabled = r.status === "CANCELLING";
  cancel.textContent = r.status === "CANCELLING" ? "Cancelling…" : "Cancel run";
  renderGraph();
  renderTasks();
  renderTaskDetail();
  renderEvents();
}

function renderLegend() {
  $("legend").replaceChildren(...STATUSES.map((st) => badge(st)));
}

function layers(tasks) {
  const byKey = new Map(tasks.map((t) => [t.task_key, t]));
  const level = new Map();
  const visit = (t, seen = new Set()) => {
    if (level.has(t.task_key)) return level.get(t.task_key);
    if (seen.has(t.task_key)) return 0; // defensive: cycles are rejected server-side
    seen.add(t.task_key);
    const deps = (t.depends_on || []).map((k) => byKey.get(k)).filter(Boolean);
    const l = deps.length ? 1 + Math.max(...deps.map((d) => visit(d, seen))) : 0;
    level.set(t.task_key, l);
    return l;
  };
  tasks.forEach((t) => visit(t));
  const cols = [];
  for (const t of tasks) (cols[level.get(t.task_key)] ||= []).push(t);
  cols.forEach((c) => c.sort((a, b) => a.task_key.localeCompare(b.task_key)));
  return cols;
}

function renderGraph() {
  const wrap = $("graph");
  const tasks = state.tasks;
  if (!tasks.length) { wrap.replaceChildren(); return; }
  const W = 156, H = 46, GX = 64, GY = 14, PAD = 16;
  const cols = layers(tasks);
  const pos = new Map();
  const tallest = Math.max(...cols.map((c) => c.length));
  const height = PAD * 2 + tallest * H + (tallest - 1) * GY;
  cols.forEach((col, ci) => {
    const colH = col.length * H + (col.length - 1) * GY;
    const y0 = (height - colH) / 2;
    col.forEach((t, ri) => pos.set(t.task_key, { x: PAD + ci * (W + GX), y: y0 + ri * (H + GY) }));
  });
  const width = PAD * 2 + cols.length * W + (cols.length - 1) * GX;
  const status = new Map(tasks.map((t) => [t.task_key, t.status]));

  const edges = [];
  for (const t of tasks) {
    for (const dep of t.depends_on || []) {
      const a = pos.get(dep), b = pos.get(t.task_key);
      if (!a || !b) continue;
      const x1 = a.x + W, y1 = a.y + H / 2, x2 = b.x, y2 = b.y + H / 2, mx = (x1 + x2) / 2;
      edges.push(s("path", {
        class: status.get(dep) === "SUCCEEDED" ? "edge done" : "edge",
        d: `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2 - 6},${y2}`,
        "marker-end": "url(#arrow)",
      }));
    }
  }
  const nodes = tasks.map((t) => {
    const p = pos.get(t.task_key);
    const label = t.task_key.length > 20 ? t.task_key.slice(0, 19) + "…" : t.task_key;
    const sub = `${t.status.toLowerCase().replace("_", " ")} · ${t.attempt_count}/${t.attempt_limit}`;
    return s("g", {
      class: `node n-${t.status}${t.id === state.taskId ? " selected" : ""}`,
      transform: `translate(${p.x},${p.y})`,
      tabindex: "0",
      role: "button",
      "aria-label": `${t.task_key}: ${t.status}`,
      onclick: () => openTask(t.id),
      onkeydown: (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openTask(t.id); } },
    },
    s("title", { text: `${t.task_key} (${t.handler})\n${t.status}${t.last_error_message ? "\n" + t.last_error_message : ""}` }),
    s("rect", { width: W, height: H, rx: 8 }),
    s("text", { x: 10, y: 19, text: label }),
    s("text", { x: 10, y: 35, class: "sub", text: sub }));
  });
  const svg = s("svg", { width, height, viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": "Task dependency graph" },
    s("defs", {}, s("marker", { id: "arrow", viewBox: "0 0 10 10", refX: "4", refY: "5", markerWidth: "7", markerHeight: "7", orient: "auto-start-reverse" },
      s("path", { d: "M0,0 L10,5 L0,10 z", class: "arrowhead" }))),
    ...edges, ...nodes);
  wrap.replaceChildren(svg);
}

function renderTasks() {
  const head = h("tr", {}, ...["Task", "Status", "Attempts", "Worker", "Duration", "Last error"].map((c) => h("th", { text: c })));
  const rows = state.tasks.map((t) => h("tr", {
    class: `clickable${t.id === state.taskId ? " selected" : ""}`,
    onclick: () => openTask(t.id),
  },
  h("td", {}, h("div", { text: t.task_key }), h("div", { class: "muted small", text: t.handler })),
  h("td", {}, badge(t.status)),
  h("td", { class: "num", text: `${t.attempt_count}/${t.attempt_limit}` }),
  h("td", { class: "mono small", text: t.lease_owner || "" }),
  h("td", { class: "num", text: t.started_at ? dur(t.started_at, t.completed_at) : "—" }),
  h("td", { class: "err-cell small", text: t.status === "SUCCEEDED" ? "" : (t.last_error_type ? `${t.last_error_type}: ${t.last_error_message || ""}` : "") })));
  $("tasks-table").replaceChildren(h("thead", {}, head), h("tbody", {}, rows));
}

function renderTaskDetail() {
  const t = state.tasks.find((x) => x.id === state.taskId);
  const body = $("task-detail-body");
  const retry = $("retry-task");
  if (!t) {
    $("task-detail-title").textContent = "Attempts";
    retry.hidden = true;
    body.replaceChildren(h("p", { class: "muted", text: "Select a task in the graph or table to see its attempt history." }));
    return;
  }
  $("task-detail-title").textContent = `Attempts · ${t.task_key}`;
  retry.hidden = !(t.status === "FAILED" && state.isAdmin && ["RUNNING", "FAILED"].includes(state.run && state.run.status));
  const info = h("dl", { class: "facts" },
    ...[["Handler", t.handler], ["Status", t.status], ["Timeout", `${t.timeout_seconds}s`],
      t.queue_timeout_seconds ? ["Queue timeout", `${t.queue_timeout_seconds}s`] : null,
      ["Max attempts", String(t.retry_policy.max_attempts)],
      t.next_attempt_at && t.status === "RETRY_WAIT" ? ["Next attempt", new Date(t.next_attempt_at).toLocaleTimeString()] : null,
      t.lease_expires_at ? ["Lease expires", new Date(t.lease_expires_at).toLocaleTimeString()] : null]
      .filter(Boolean).map(([k, v]) => h("div", {}, h("dt", { text: k }), h("dd", { text: v }))));
  const head = h("tr", {}, ...["#", "Worker", "Outcome", "Queue wait", "Duration", "Error"].map((c) => h("th", { text: c })));
  const rows = state.attempts.map((a) => h("tr", {},
    h("td", { class: "num", text: String(a.attempt_number) }),
    h("td", { class: "mono small", text: a.worker_id }),
    h("td", {}, badge(a.outcome)),
    h("td", { class: "num", text: fmtMs(a.queue_wait_ms) }),
    h("td", { class: "num", text: a.outcome === "RUNNING" ? dur(a.started_at) : fmtMs(a.duration_ms) }),
    h("td", { class: "err-cell small", text: a.error_type ? `${a.error_type}: ${a.error_message || ""}` : "" })));
  const output = t.output != null
    ? h("details", {}, h("summary", { class: "small", text: "Output" }), h("pre", { class: "mono small", text: JSON.stringify(t.output, null, 2) }))
    : null;
  body.replaceChildren(...[info,
    h("div", { class: "table-wrap" }, h("table", { class: "table" }, h("thead", {}, head), h("tbody", {}, rows))),
    state.attempts.length ? null : h("p", { class: "muted small", text: "No attempts yet." }),
    output].filter(Boolean));
}

function renderEvents() {
  const list = $("events");
  const items = state.events.slice().reverse();
  const pruned = state.run && state.run.events_pruned_at;
  $("events-count").textContent = pruned
    ? `older events removed by retention ${ago(pruned)}`
    : state.events.length ? `${state.events.length}${state.events.length >= MAX_EVENTS ? "+" : ""} events, newest first` : "";
  const keyOf = new Map(state.tasks.map((t) => [t.id, t.task_key]));
  list.replaceChildren(...items.map((e) => {
    const data = {};
    for (const [k, v] of Object.entries(e.data || {})) {
      if (v == null || k === "task_key") continue;
      data[k] = typeof v === "number" && !Number.isInteger(v) ? Number(v.toFixed(3)) : v;
    }
    const subject = e.task_id ? keyOf.get(e.task_id) || shortId(e.task_id) : "run";
    return h("li", {},
      h("span", { class: "muted small", text: new Date(e.created_at).toLocaleTimeString() }),
      h("span", {}, h("span", { class: "etype", text: e.event_type }), h("span", { class: "muted", text: ` · ${subject}` })),
      h("span", { class: "edata", text: Object.entries(data).map(([k, v]) => `${k}=${typeof v === "object" ? JSON.stringify(v) : v}`).join("  ") }));
  }));
}

function renderWorkers(workers) {
  const panel = $("workers-panel");
  panel.hidden = false;
  const live = workers.filter((w) => w.health === "live").length;
  renderStats.workers = live;
  renderStats(null);
  const head = h("tr", {}, ...["Worker", "Busy", "Health"].map((c) => h("th", { text: c })));
  const rows = workers.map((w) => {
    const stateText = w.health;
    const cls = { live: "st-SUCCEEDED", silent: "st-FAILED", stopped: "st-CANCELLED" }[w.health] || "st-BLOCKED";
    return h("tr", {},
      h("td", { class: "worker-cell" }, h("div", { class: "mono small", text: w.worker_id }), h("div", { class: "muted small", text: `pid ${w.pid}` })),
      h("td", { class: "num", text: `${w.running_tasks}/${w.concurrency}` }),
      h("td", {}, h("span", { class: `badge ${cls}`, text: stateText }),
        h("div", { class: "muted small", text: w.stopped_at ? `stopped ${ago(w.stopped_at)}` : `beat ${w.heartbeat_age_seconds.toFixed(0)}s ago` })));
  });
  $("workers-table").replaceChildren(h("thead", {}, head), h("tbody", {}, rows.length ? rows : [h("tr", {}, h("td", { colspan: "3", class: "muted", text: "No workers have registered." }))]));
}

function renderLive() {
  const live = $("live");
  live.className = state.live ? "live on" : "live";
  $("live-text").textContent = state.live ? `live · ${REFRESH_MS / 1000}s` : "paused";
  $("toggle-refresh").textContent = state.live ? "Pause" : "Resume";
}

// ---------------------------------------------------------------- actions

async function cancelRun() {
  if (!state.run || !confirm("Cancel this run? Queued tasks stop immediately; running handlers are asked to stop.")) return;
  try {
    const res = await api(`/v1/runs/${state.run.id}/cancel`, { method: "POST" });
    toast(`Run is ${res.status}`);
    await refresh(true);
  } catch (e) { toast(`Cancel failed: ${e.message}`, true); }
}

async function retryTask() {
  const t = state.tasks.find((x) => x.id === state.taskId);
  if (!t || !confirm(`Retry ${t.task_key} with a fresh attempt budget? Handlers with side effects may repeat them.`)) return;
  try {
    await api(`/v1/tasks/${t.id}/retry`, { method: "POST" });
    toast(`${t.task_key} re-queued`);
    await refresh(true);
  } catch (e) { toast(`Retry failed: ${e.message}`, true); }
}

function parseJson(text, what) {
  try { return JSON.parse(text); } catch (e) { throw new Error(`${what} is not valid JSON: ${e.message}`); }
}

async function openNewRun() {
  await loadDefinitions();
  const sel = $("def-select");
  const defs = [...state.definitions.values()];
  sel.replaceChildren(...(defs.length
    ? defs.map((d) => h("option", { value: d.id, text: `${d.name} v${d.version} (${d.definition.tasks.length} tasks)` }))
    : [h("option", { value: "", text: "No definitions yet — register one" })]));
  if (!defs.length) switchTab("register");
  $("new-run-error").hidden = true;
  $("new-run-dialog").showModal();
}

function switchTab(name) {
  document.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t.dataset.tab === name));
  document.querySelectorAll("[data-pane]").forEach((p) => { p.hidden = p.dataset.pane !== name; });
}

async function submitNewRun(ev) {
  ev.preventDefault();
  const err = $("new-run-error");
  err.hidden = true;
  const btn = $("new-run-submit");
  btn.disabled = true;
  try {
    const registering = !document.querySelector('[data-pane="register"]').hidden;
    let defId = $("def-select").value;
    if (registering) {
      const def = await api("/v1/workflow-definitions", { method: "POST", body: parseJson($("def-json").value, "Definition") });
      defId = def.id;
      state.definitions.set(def.id, def);
    }
    if (!defId) throw new Error("Choose or register a definition first.");
    const input = parseJson($("run-input").value || "{}", "Run input");
    const key = $("idem-key").value.trim();
    const run = await api(`/v1/workflow-definitions/${defId}/runs`, {
      method: "POST", body: { input }, headers: key ? { "Idempotency-Key": key } : {},
    });
    $("new-run-dialog").close();
    toast(run.idempotency_key && key ? `Run ${shortId(run.id)} (idempotent)` : `Run ${shortId(run.id)} started`);
    openRun(run.id);
    await refresh(true);
  } catch (e) {
    err.textContent = e.message;
    err.hidden = false;
  } finally {
    btn.disabled = false;
  }
}

// ---------------------------------------------------------------- wiring

function init() {
  renderLegend();
  renderLive();
  $("login-form").addEventListener("submit", (e) => { e.preventDefault(); signIn($("api-key").value.trim()); });
  $("sign-out").addEventListener("click", () => signOut());
  $("toggle-refresh").addEventListener("click", () => { state.live = !state.live; renderLive(); if (state.live) refresh(true); });
  $("status-filter").addEventListener("change", async (e) => {
    state.filter = e.target.value;
    state.nextCursor = null;
    try { await loadRuns(); } catch (err) { toast(err.message, true); }
  });
  $("more-runs").addEventListener("click", async () => { try { await loadRuns(true); } catch (e) { toast(e.message, true); } });
  $("cancel-run").addEventListener("click", cancelRun);
  $("retry-task").addEventListener("click", retryTask);
  $("open-new-run").addEventListener("click", openNewRun);
  $("new-run-cancel").addEventListener("click", () => $("new-run-dialog").close());
  $("new-run-form").addEventListener("submit", submitNewRun);
  document.querySelectorAll(".tab").forEach((t) => t.addEventListener("click", () => switchTab(t.dataset.tab)));
  document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(true); });
  setInterval(() => refresh(false), REFRESH_MS);
  if (state.key) signIn(state.key); else signOut();
}

init();
