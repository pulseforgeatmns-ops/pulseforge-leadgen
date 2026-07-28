'use strict';

/**
 * Inbox deduplication (SPEC-037).
 * Multiple capabilities requesting the same operator action → one item.
 */

const { ACTIVE_STATUSES } = require('./types');

/**
 * Stable dedupe key: client + mission/campaign + kind + optional subject.
 * @param {object} item
 * @returns {string}
 */
function buildDedupeKey(item) {
  if (!item || typeof item !== 'object') return 'unknown';
  if (item.dedupeKey) return String(item.dedupeKey);
  const client =
    item.clientId != null ? String(item.clientId) : 'client:none';
  const scope =
    item.missionId != null
      ? `mission:${item.missionId}`
      : item.campaignId != null
        ? `campaign:${item.campaignId}`
        : 'scope:none';
  const kind = String(item.kind || 'unknown');
  const subject =
    item.subjectId != null ? `subject:${item.subjectId}` : 'subject:none';
  return [client, scope, kind, subject].join('|');
}

/**
 * Merge incoming candidates into existing active items by dedupe key.
 * Preserves existing id/status; unions sources; refreshes title/due if provided.
 *
 * @param {object[]} existing
 * @param {object[]} incoming
 * @returns {{ items: object[], merged: number, created: number }}
 */
function dedupeInboxItems(existing, incoming) {
  /** @type {Map<string, object>} */
  const byKey = new Map();
  let merged = 0;
  let created = 0;

  for (const item of Array.isArray(existing) ? existing : []) {
    const key = buildDedupeKey(item);
    const seededSources =
      Array.isArray(item.sources) && item.sources.length
        ? [...item.sources]
        : item.sourceCapability
          ? [
              {
                capability: item.sourceCapability,
                at: item.createdAt || new Date().toISOString(),
                title: item.title || null,
              },
            ]
          : [];
    byKey.set(key, {
      ...item,
      dedupeKey: key,
      sources: seededSources,
    });
  }

  for (const raw of Array.isArray(incoming) ? incoming : []) {
    const key = buildDedupeKey(raw);
    const sourceRef = {
      capability: raw.sourceCapability || null,
      at: raw.createdAt || new Date().toISOString(),
      title: raw.title || null,
    };
    const current = byKey.get(key);
    if (current && ACTIVE_STATUSES.has(current.status)) {
      const sources = [...(current.sources || [])];
      if (
        !sources.some(
          (s) =>
            s.capability === sourceRef.capability &&
            s.title === sourceRef.title
        )
      ) {
        sources.push(sourceRef);
      }
      byKey.set(key, {
        ...current,
        title: raw.title || current.title,
        dueDate: raw.dueDate || current.dueDate,
        notes: raw.notes || current.notes,
        deepLink: raw.deepLink || current.deepLink,
        sources,
        updatedAt: new Date().toISOString(),
      });
      merged += 1;
    } else if (current && !ACTIVE_STATUSES.has(current.status)) {
      // Re-open a fresh active item for the same key (new work cycle)
      byKey.set(key, {
        ...raw,
        id: raw.id || current.id,
        dedupeKey: key,
        status: raw.status || 'open',
        sources: [sourceRef],
        updatedAt: new Date().toISOString(),
      });
      created += 1;
    } else {
      byKey.set(key, {
        ...raw,
        dedupeKey: key,
        sources: Array.isArray(raw.sources) ? raw.sources : [sourceRef],
      });
      created += 1;
    }
  }

  return {
    items: [...byKey.values()],
    merged,
    created,
  };
}

module.exports = {
  buildDedupeKey,
  dedupeInboxItems,
};
