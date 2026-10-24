'use strict';

/**
 * SPEC-117 — campaign pacing.
 * Prevent 100 identical emails. Interleave verticals.
 */

function paceVerticals(items = []) {
  const buckets = new Map();
  for (const item of items) {
    const key = String(item.vertical || 'unknown').toLowerCase();
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push(item);
  }
  const keys = [...buckets.keys()];
  const paced = [];
  let remaining = items.length;
  while (remaining > 0) {
    for (const key of keys) {
      const bucket = buckets.get(key);
      if (bucket && bucket.length) {
        paced.push(bucket.shift());
        remaining -= 1;
      }
    }
  }
  return paced;
}

function pacingWarning(items = []) {
  if (items.length < 4) return null;
  const counts = new Map();
  for (const item of items) {
    const key = String(item.vertical || 'unknown');
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  const dominant = [...counts.entries()].sort((a, b) => b[1] - a[1])[0];
  if (dominant && dominant[1] / items.length >= 0.8) {
    return `Queue is ${Math.round((dominant[1] / items.length) * 100)}% ${dominant[0]}. Interleave other verticals.`;
  }
  return null;
}

module.exports = {
  paceVerticals,
  pacingWarning,
};
