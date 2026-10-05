'use strict';

const { ATTENTION_STATUS, REVIEW_TRIGGER } = require('./types');

async function wakeAttentionForSubject({
  attentionStore,
  clientId,
  subjectType,
  subjectId,
  trigger = REVIEW_TRIGGER.EVIDENCE,
  evidence = [],
  now = new Date(),
}) {
  const items = await attentionStore.listUnresolved({ clientId, subjectType, subjectId });
  const woken = [];
  for (const item of items) {
    const updated = await attentionStore.updateAttention(item.id, {
      status: item.status === ATTENTION_STATUS.WAITING ? ATTENTION_STATUS.ACTIVE : item.status,
      next_review_at: now.toISOString(),
      review_trigger: trigger,
      supporting_evidence: [...(item.supporting_evidence || []), ...evidence].slice(-20),
      last_reviewed_at: now.toISOString(),
    });
    woken.push(updated);
  }
  return woken;
}

async function wakeAttentionForIngestion({
  attentionStore,
  clientId,
  ingestionResult,
  stateStore = null,
  now = new Date(),
}) {
  const evidence = [{
    kind: 'ingestion',
    ingestion_id: ingestionResult.ingestion_id,
    at: now.toISOString(),
  }];
  let prospectId = ingestionResult?.prospect?.id || null;
  if (!prospectId && stateStore?.expectations?.length) {
    const open = stateStore.expectations.find(e =>
      ['OPEN', 'WAITING', 'OVERDUE'].includes(e.status)
    );
    prospectId = open?.prospect_id || stateStore.expectations[0]?.prospect_id || null;
  }
  if (!prospectId && Array.isArray(stateStore?.prospects) && stateStore.prospects.length === 1) {
    prospectId = stateStore.prospects[0].id;
  }
  if (!prospectId) return [];
  const all = await attentionStore.listUnresolved({ clientId });
  const related = all.filter(item => {
    if (item.subject_type === 'prospect' && item.subject_id === prospectId) return true;
    if (item.subject_type === 'expectation') {
      const exp = (stateStore?.expectations || []).find(e => e.id === item.subject_id);
      return exp?.prospect_id === prospectId;
    }
    return false;
  });
  const woken = [];
  for (const item of related) {
    const updated = await attentionStore.updateAttention(item.id, {
      status: item.status === ATTENTION_STATUS.WAITING ? ATTENTION_STATUS.ACTIVE : item.status,
      next_review_at: now.toISOString(),
      review_trigger: REVIEW_TRIGGER.EVIDENCE,
      supporting_evidence: [...(item.supporting_evidence || []), ...evidence].slice(-20),
      last_reviewed_at: now.toISOString(),
    });
    woken.push(updated);
  }
  return woken;
}

module.exports = {
  wakeAttentionForSubject,
  wakeAttentionForIngestion,
};
