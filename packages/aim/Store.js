'use strict';

/**
 * SPEC-112 — in-memory AIM store.
 * AIM is not operating fact. Qualifications are derived scores.
 */

const { clone, asText, nowIso } = require('./types');
const { FEDIR_CLIENT_KEY, buildFedirAim } = require('./seeds/fedir');

function createMemoryAimStore(opts = {}) {
  const models = new Map();
  const qualifications = new Map();
  const knowledge = new Map();

  function putAim(aim) {
    const key = asText(aim.clientKey || aim.id);
    const copy = clone(aim);
    copy.updatedAt = nowIso();
    models.set(key, copy);
    if (Array.isArray(copy.knowledge)) {
      knowledge.set(key, clone(copy.knowledge));
    }
    return clone(copy);
  }

  function getAim(clientKey) {
    const key = asText(clientKey);
    const found = models.get(key);
    return found ? clone(found) : null;
  }

  function listAims() {
    return [...models.values()].map(clone);
  }

  function putQualification(row) {
    const clientKey = asText(row.clientKey);
    const prospectId = asText(row.prospectId) || `anon-${Date.now()}`;
    const list = qualifications.get(clientKey) || [];
    const next = list.filter((q) => q.prospectId !== prospectId);
    next.push(clone(row));
    qualifications.set(clientKey, next);
    return clone(row);
  }

  function listQualifications(clientKey) {
    return clone(qualifications.get(asText(clientKey)) || []);
  }

  function putKnowledge(clientKey, record) {
    const key = asText(clientKey);
    const list = knowledge.get(key) || [];
    const next = list.filter((r) => r.painId !== record.painId);
    next.push(clone(record));
    knowledge.set(key, next);
    const aim = models.get(key);
    if (aim) {
      aim.knowledge = clone(next);
      aim.knowledgeById = Object.fromEntries(next.map((r) => [r.painId, r]));
      models.set(key, aim);
    }
    return clone(record);
  }

  function listKnowledge(clientKey) {
    return clone(knowledge.get(asText(clientKey)) || []);
  }

  if (opts.seedFedir !== false) {
    putAim(buildFedirAim());
  }
  for (const extra of opts.seeds || []) putAim(extra);

  return {
    putAim,
    getAim,
    listAims,
    putQualification,
    listQualifications,
    putKnowledge,
    listKnowledge,
    FEDIR_CLIENT_KEY,
  };
}

module.exports = {
  createMemoryAimStore,
};
