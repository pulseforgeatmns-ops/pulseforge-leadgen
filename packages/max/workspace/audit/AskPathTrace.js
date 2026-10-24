'use strict';

/**
 * TEMPORARY production instrumentation for WorkspaceEngine.ask().
 * Active only when the operator utterance is exactly "approved" (case-insensitive).
 * Remove after execution-path audit is complete.
 */

let _active = false;

function isActive() {
  return _active;
}

function shouldTrace(question) {
  return String(question || '').trim().toLowerCase() === 'approved';
}

function beginTrace(question) {
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

module.exports = {
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
