'use strict';

const crypto = require('crypto');
const {
  SEMANTIC_TYPE,
  DEFAULT_TTL_MS,
  MAX_ACTIVE_CONTACTS,
  MAX_ACTIVE_ENTITIES,
  MAX_RECENT_THREADS,
} = require('./conversationMemoryTypes');
const { recordFingerprint } = require('./conversationMemoryRepository');
const { AMBIGUITY_KIND } = require('./types');
const { ENTITY_KIND } = require('./types');

function expiresAtFor(semanticType, now = new Date()) {
  const ms = DEFAULT_TTL_MS[semanticType] || DEFAULT_TTL_MS.RECENT_THREAD;
  return new Date(now.getTime() + ms).toISOString();
}

function trimSituationModelForStorage(situationModel) {
  if (!situationModel) return null;
  return {
    inputId: situationModel.inputId,
    conversationId: situationModel.conversationId,
    interpretedAt: situationModel.interpretedAt,
    threads: (situationModel.threads || []).map(t => ({
      threadId: t.threadId,
      accountName: t.accountName,
      entities: (t.entities || []).map(e => ({
        id: e.id,
        kind: e.kind,
        name: e.name,
        gender: e.gender,
        role: e.role,
        accountName: e.accountName,
        decisionMaker: e.decisionMaker,
        epistemic: e.epistemic,
      })),
      ambiguities: t.ambiguities || [],
      commitments: (t.commitments || []).slice(0, 4),
      corrections: (t.corrections || []).slice(0, 4),
    })),
    ambiguities: situationModel.ambiguities || [],
    corrections: (situationModel.corrections || []).slice(0, 6),
  };
}

function buildMemoryRecordsFromTurn({
  tenantId,
  conversationId,
  actorId,
  actorRole,
  situationModel,
  validation,
  sourceInputId,
  now = new Date(),
}) {
  const records = [];
  const occurredAt = situationModel?.occurredAt || situationModel?.interpretedAt || now.toISOString();
  const base = {
    tenantId,
    conversationId,
    actorId: actorId != null ? String(actorId) : null,
    actorRole: actorRole || null,
    sourceInputId,
    sourceSituationModelId: situationModel?.inputId || null,
    occurredAt,
    lastReferencedAt: now.toISOString(),
  };

  const threadSnapshot = trimSituationModelForStorage(situationModel);
  records.push({
    ...base,
    semanticType: SEMANTIC_TYPE.RECENT_THREAD,
    payload: { turn: threadSnapshot, bindingKey: `thread:${sourceInputId}` },
    recordFingerprint: recordFingerprint(['RECENT_THREAD', tenantId, conversationId, sourceInputId]),
    expiresAt: expiresAtFor(SEMANTIC_TYPE.RECENT_THREAD, now),
  });

  const contacts = [];
  for (const thread of situationModel?.threads || []) {
    for (const e of thread.entities || []) {
      if (e.kind === ENTITY_KIND.CONTACT || e.kind === 'contact') contacts.push({ ...e, accountName: e.accountName || thread.accountName });
    }
  }
  for (const contact of contacts.slice(0, MAX_ACTIVE_CONTACTS)) {
    const bindingKey = `contact:${(contact.name || '').toLowerCase()}:${(contact.accountName || '').toLowerCase()}`;
    records.push({
      ...base,
      semanticType: SEMANTIC_TYPE.ACTIVE_CONTACT,
      entityType: 'contact',
      entityId: contact.id || null,
      payload: { contact, bindingKey },
      recordFingerprint: recordFingerprint(['ACTIVE_CONTACT', tenantId, conversationId, bindingKey]),
      expiresAt: expiresAtFor(SEMANTIC_TYPE.ACTIVE_CONTACT, now),
      confidence: 0.9,
    });
  }

  const accounts = new Map();
  for (const thread of situationModel?.threads || []) {
    const name = thread.accountName || (thread.entities || []).find(e => e.kind === ENTITY_KIND.ACCOUNT)?.name;
    if (name) accounts.set(name.toLowerCase(), name);
  }
  let accountCount = 0;
  for (const name of accounts.values()) {
    if (accountCount >= MAX_ACTIVE_ENTITIES) break;
    const bindingKey = `account:${name.toLowerCase()}`;
    records.push({
      ...base,
      semanticType: SEMANTIC_TYPE.ACTIVE_ENTITY,
      entityType: 'account',
      payload: { name, bindingKey },
      recordFingerprint: recordFingerprint(['ACTIVE_ENTITY', tenantId, conversationId, bindingKey]),
      expiresAt: expiresAtFor(SEMANTIC_TYPE.ACTIVE_ENTITY, now),
    });
    accountCount += 1;
  }

  for (const corr of situationModel?.corrections || []) {
    const bindingKey = `correction:${corr.kind}:${corr.contactName || corr.newValue || corr.priorValue}`;
    records.push({
      ...base,
      semanticType: SEMANTIC_TYPE.CORRECTION_CONTEXT,
      payload: { correction: corr, bindingKey },
      recordFingerprint: recordFingerprint(['CORRECTION', tenantId, conversationId, sourceInputId, bindingKey]),
      expiresAt: expiresAtFor(SEMANTIC_TYPE.CORRECTION_CONTEXT, now),
    });
    if (corr.kind === 'decision_maker_role' && corr.contactName) {
      records.push({
        ...base,
        semanticType: SEMANTIC_TYPE.REFERENCE_BINDING,
        payload: {
          bindingKey: `dm:${corr.contactName.toLowerCase()}`,
          contactName: corr.contactName,
          role: corr.newValue,
          supersededPrior: corr.priorValue,
        },
        recordFingerprint: recordFingerprint(['REF_BIND', tenantId, conversationId, corr.contactName, corr.newValue]),
        expiresAt: expiresAtFor(SEMANTIC_TYPE.REFERENCE_BINDING, now),
      });
    }
  }

  for (const commit of (situationModel?.commitments || []).slice(0, 4)) {
    records.push({
      ...base,
      semanticType: SEMANTIC_TYPE.OPEN_COMMITMENT,
      payload: {
        commitment: commit,
        bindingKey: `commit:${commit.responsible}:${commit.windowPhrase}`,
      },
      recordFingerprint: recordFingerprint(['COMMIT', tenantId, conversationId, sourceInputId, commit.id || commit.responsible]),
      expiresAt: expiresAtFor(SEMANTIC_TYPE.OPEN_COMMITMENT, now),
    });
  }

  const material = validation?.materialAmbiguities || [];
  for (const amb of material) {
    if (amb.kind === AMBIGUITY_KIND.ACCOUNT || amb.kind === AMBIGUITY_KIND.PRONOUN || amb.kind === AMBIGUITY_KIND.CONTACT) {
      const deferred = inferDeferredIntent(situationModel);
      records.push({
        ...base,
        semanticType: SEMANTIC_TYPE.OPEN_QUESTION,
        payload: {
          bindingKey: `open:${amb.kind}:${amb.phrase || amb.pronoun || 'unknown'}`,
          ambiguity: amb,
          deferred,
          resolved: false,
        },
        recordFingerprint: recordFingerprint(['OPEN_Q', tenantId, conversationId, sourceInputId, amb.kind]),
        expiresAt: expiresAtFor(SEMANTIC_TYPE.OPEN_QUESTION, now),
      });
    }
  }

  return records.slice(0, MAX_RECENT_THREADS + MAX_ACTIVE_CONTACTS + MAX_ACTIVE_ENTITIES + 8);
}

function inferDeferredIntent(situationModel) {
  const raw = situationModel?.rawText || '';
  const lower = String(raw).toLowerCase();
  const deferred = {};
  if (/call|callback|thursday|friday|monday|tuesday|wednesday|saturday|sunday/i.test(lower)) {
    deferred.intent = 'callback';
    const day = lower.match(/\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b/i);
    if (day) deferred.temporalPhrase = day[1];
  }
  if (/exeter|granite|abc/i.test(lower)) {
    deferred.shorthandAccount = raw.match(/\b([A-Za-z][A-Za-z0-9&.'\-]{2,24})\s+said\b/i)?.[1] || null;
  }
  return deferred;
}

async function persistConversationMemoryTurn({
  repository,
  tenantId,
  conversationId,
  actorId,
  actorRole,
  situationModel,
  validation,
  sourceInputId,
  now = new Date(),
}) {
  if (!repository || !conversationId || tenantId == null) {
    return { written: 0, superseded: 0, records: [] };
  }

  const supersededBindingKeys = [];
  for (const corr of situationModel?.corrections || []) {
    if (corr.kind === 'decision_maker_role' && corr.contactName) {
      supersededBindingKeys.push(`dm:${String(corr.priorValue || 'decision_maker').toLowerCase()}`);
    }
  }

  let superseded = 0;
  if (supersededBindingKeys.length) {
    superseded += await repository.supersedeActive({
      tenantId,
      conversationId,
      actorId,
      semanticTypes: [SEMANTIC_TYPE.REFERENCE_BINDING],
      bindingKeys: supersededBindingKeys,
    });
  }

  const resolvedAccount = (situationModel?.threads || []).map(t => t.accountName).find(Boolean);
  if (resolvedAccount && validation?.validated) {
    superseded += await repository.supersedeActive({
      tenantId,
      conversationId,
      actorId,
      semanticTypes: [SEMANTIC_TYPE.OPEN_QUESTION],
    });
  }

  const records = buildMemoryRecordsFromTurn({
    tenantId,
    conversationId,
    actorId,
    actorRole,
    situationModel,
    validation,
    sourceInputId,
    now,
  });

  const upsert = await repository.upsertRecords(records);
  return { ...upsert, superseded, records };
}

module.exports = {
  buildMemoryRecordsFromTurn,
  persistConversationMemoryTurn,
  trimSituationModelForStorage,
  expiresAtFor,
};
