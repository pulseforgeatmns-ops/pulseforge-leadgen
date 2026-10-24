'use strict';

const counters = Object.create(null);

function inc(name, dimensions = {}) {
  const dimKey = Object.keys(dimensions).sort().map(k => `${k}=${dimensions[k]}`).join(',');
  const key = dimKey ? `${name}|${dimKey}` : name;
  counters[key] = (counters[key] || 0) + 1;
}

function snapshot() {
  return { ...counters };
}

function resetForTests() {
  for (const k of Object.keys(counters)) delete counters[k];
}

module.exports = {
  inc,
  snapshot,
  resetForTests,
};
