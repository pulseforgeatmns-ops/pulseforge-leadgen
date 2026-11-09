'use strict';

const states = new Map();
const processStartedAt = new Date().toISOString();

function state(name) {
  if (!states.has(name)) states.set(name, {
    completed: 0, failed: 0, dropped: 0, pending: 0,
    lastSuccessAt: null, lastFailureAt: null, lastFailureReason: null,
  });
  return states.get(name);
}

function recordObserverHealth(name, event, details = {}) {
  const current = state(name);
  if (event === 'success') {
    current.completed += 1;
    current.lastSuccessAt = new Date().toISOString();
  } else if (event === 'failure') {
    current.failed += 1;
    current.lastFailureAt = new Date().toISOString();
    current.lastFailureReason = String(details.reason || 'unknown').slice(0, 80);
  } else if (event === 'dropped') current.dropped += 1;
  if (Number.isInteger(details.pending) && details.pending >= 0) current.pending = details.pending;
}

function observerHealthSnapshot() {
  return {
    scope: 'process',
    durableHistory: false,
    processStartedAt,
    observers: Object.fromEntries([...states].map(([name, value]) => [name, { ...value }])),
  };
}

function resetObserverHealth() { states.clear(); }

module.exports = { recordObserverHealth, observerHealthSnapshot, resetObserverHealth };
