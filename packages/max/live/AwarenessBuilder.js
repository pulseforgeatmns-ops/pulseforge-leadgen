'use strict';

/**
 * Max awareness copy from live events (SPEC-011).
 * Deterministic presentation strings — never invents facts.
 */

/**
 * @param {object} input
 * @param {object[]} input.events - IntelligenceEvents since session / focus
 * @param {string} [input.openedAt]
 * @param {string} [input.entityLabel]
 * @param {string} [input.now]
 * @returns {{ lines: string[], headline: string|null }}
 */
function buildAwareness(input = {}) {
  const events = Array.isArray(input.events) ? input.events : [];
  const now = Date.parse(input.now || new Date().toISOString());
  /** @type {string[]} */
  const lines = [];

  if (events.length === 0) {
    return { lines: [], headline: null };
  }

  // Most recent material or latest event about focused entity
  const latest = events[events.length - 1];
  if (latest) {
    const ago = relativeMinutes(now, latest.timestamp);
    if (ago != null) {
      const label =
        (input.entityLabel && String(input.entityLabel)) ||
        (latest.entity && latest.entity.label) ||
        'This recommendation';
      if (
        /recommendation|confidence|leverage|blocked|promoted/i.test(
          String(latest.type)
        )
      ) {
        lines.push(
          `${label} changed ${ago === 0 ? 'just now' : `${ago} minute${ago === 1 ? '' : 's'} ago`}.`
        );
      }
    }
  }

  const evidenceEvents = events.filter((e) =>
    /evidence|hiring|contradict/i.test(String(e.type || ''))
  );
  if (evidenceEvents.length > 0) {
    const n = evidenceEvents.length;
    lines.push(
      `Since you opened this conversation, ${n} new evidence source${
        n === 1 ? '' : 's'
      } arrived.`
    );
  }

  const material = events.filter((e) => e.material);
  if (material.length > 0 && lines.length === 0) {
    lines.push(material[material.length - 1].summary);
  }

  if (lines.length === 0 && latest) {
    lines.push(latest.summary);
  }

  // Deduplicate
  const unique = [...new Set(lines)].slice(0, 3);
  return {
    lines: unique,
    headline: unique[0] || null,
  };
}

/**
 * @param {number} nowMs
 * @param {string} timestamp
 * @returns {number|null}
 */
function relativeMinutes(nowMs, timestamp) {
  const t = Date.parse(timestamp);
  if (!Number.isFinite(t) || !Number.isFinite(nowMs)) return null;
  return Math.max(0, Math.round((nowMs - t) / 60000));
}

module.exports = {
  buildAwareness,
};
