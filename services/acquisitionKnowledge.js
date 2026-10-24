'use strict';

const ak = require('../packages/acquisition-knowledge');
const persistence = require('./acquisitionKnowledgePersistence');

function actorOptions(actor = {}) {
  return {
    actorId: actor.id || actor.email || actor.name || 'operator',
    actorRole: ak.normalizeRole(actor.role || actor.specialist || 'operator'),
  };
}

async function createKnowledge(input = {}, opts = {}) {
  return persistence.upsertKnowledgeObject(input, opts.pool, actorOptions(opts.actor));
}

async function promoteKnowledge(id, input = {}, opts = {}) {
  return persistence.promoteKnowledgeObject(id, input, opts.pool, {
    ...actorOptions(opts.actor),
    tenantId: input.tenantId || opts.tenantId,
  });
}

async function retrieveKnowledge(query = {}, opts = {}) {
  return persistence.queryKnowledgeObjects(query, opts.pool);
}

async function explainRecommendation(input = {}, opts = {}) {
  return persistence.recordRecommendationExplanation(input, opts.pool, {
    ...actorOptions(opts.actor),
    tenantId: input.tenantId || opts.tenantId,
  });
}

async function importKnowledge(input = {}, opts = {}) {
  const tenantId = ak.assertTenant(input.tenantId || opts.tenantId);
  const objects = Array.isArray(input.objects) ? input.objects : [];
  const dryRun = input.apply !== true;
  const actor = actorOptions(opts.actor);
  const normalized = objects.map((object, index) => ak.normalizeKnowledgeObject({
    ...object,
    tenantId,
    externalKey: object.externalKey || object.sourceKey || `import:${input.sourceName || 'unknown'}:${index + 1}`,
    provenance: {
      ...(object.provenance || {}),
      importSource: input.sourceName || input.source || null,
      importedAt: ak.nowIso(),
    },
  }, actor));
  if (dryRun) {
    return {
      spec: ak.SPEC,
      dryRun: true,
      tenantId,
      objectCount: normalized.length,
      objects: normalized,
      message: 'Import validated only; pass apply=true to write acquisition knowledge.',
    };
  }
  const saved = [];
  for (const object of normalized) {
    saved.push(await persistence.upsertKnowledgeObject(object, opts.pool, actor));
  }
  return {
    spec: ak.SPEC,
    dryRun: false,
    tenantId,
    objectCount: saved.length,
    objects: saved,
  };
}

function canonicalContextForSpecialist(rows = [], specialist = 'max') {
  return ak.canonicalContextForSpecialist(rows, specialist);
}

module.exports = {
  ...ak,
  ...persistence,
  createKnowledge,
  promoteKnowledge,
  retrieveKnowledge,
  explainRecommendation,
  importKnowledge,
  canonicalContextForSpecialist,
};
