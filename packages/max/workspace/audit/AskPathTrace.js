'use strict';

/**
 * TEMPORARY production instrumentation for WorkspaceEngine.ask().
 * Active only when the operator utterance is exactly "approved" (case-insensitive).
 * Remove after execution-path audit is complete.
 */

let _active = false;
// Opt-in programmatic collector (semantic evaluation harness). When set, every
// turn is traced into it instead of stdout. Never set in production; routing
// never reads trace state.
let _collector = null;

function isActive() {
  return _active;
}

function shouldTrace(question) {
  return String(question || '').trim().toLowerCase() === 'approved';
}

function beginTrace(question) {
  if (_collector) {
    _active = true;
    _collector.push({ kind: 'TRACE_BEGIN', payload: {} });
    return;
  }
  if (!shouldTrace(question)) return;
  _active = true;
  emit('TRACE_BEGIN', { utterance: String(question || '').trim() });
}

function endTrace() {
  if (!_active) return;
  emit('TRACE_END', {});
  _active = false;
}

function emit(kind, payload = {}) {
  if (!_active) return;
  if (_collector) {
    _collector.push({ kind, payload });
    return;
  }
  process.stdout.write(
    `[ASK_PATH_TRACE] ${kind} ${JSON.stringify(payload)}\n`
  );
}

function traceEnter(fn, extra = {}) {
  if (!_active) return;
  emit('ENTER', { fn, ...extra });
}

function traceEarlyReturn(fn, reason, extra = {}) {
  if (!_active) return;
  emit('EARLY_RETURN', { fn, reason, ...extra });
}

function traceRuntime(runtime, reason, extra = {}) {
  if (!_active) return;
  emit('RUNTIME_SELECTED', { runtime, reason, ...extra });
}

function traceOwner(owner, reason, extra = {}) {
  if (!_active) return;
  emit('RESPONSE_OWNER', { owner, reason, ...extra });
}

function traceBranch(label, extra = {}) {
  if (!_active) return;
  emit('BRANCH', { label, ...extra });
}

function traceFallback(fromOwner, toOwner, reason, extra = {}) {
  if (!_active) return;
  emit('OWNER_FALLBACK', { fromOwner, toOwner, reason, ...extra });
}

/**
 * Run `fn` with every ask() trace event captured into the returned array.
 * Evaluation/instrumentation only. Not re-entrant (asks run sequentially).
 */
async function collect(fn) {
  const events = [];
  const previous = _collector;
  _collector = events;
  try {
    const value = await fn();
    return { value, events };
  } finally {
    _collector = previous;
    _active = false;
  }
}

module.exports = {
  collect,
  isActive,
  shouldTrace,
  beginTrace,
  endTrace,
  traceEnter,
  traceEarlyReturn,
  traceRuntime,
  traceOwner,
  traceBranch,
  traceFallback,
  emit,
};
