'use strict';

/**
 * SPEC-112 Phase 4 — Knowledge capture.
 * Empty proof stays empty. Do not invent case studies.
 */

const { asText, asList, nowIso, isPlainObject } = require('./types');

const KNOWLEDGE_FIELDS = Object.freeze([
  'definition',
  'observableEvidence',
  'commonObjections',
  'typicalLanguage',
  'recommendedMessaging',
  'discoveryQuestions',
  'caseStudies',
  'successStories',
]);

function asKnowledgeList(value) {
  if (Array.isArray(value)) return asList(value);
  const text = asText(value);
  return text ? [text] : [];
}

function buildPainKnowledge(partial = {}) {
  const src = isPlainObject(partial) ? partial : {};
  const record = {
    painId: asText(src.painId || src.id),
    label: asText(src.label),
    definition: asText(src.definition),
    observableEvidence: asKnowledgeList(src.observableEvidence || src.observable_evidence),
    commonObjections: asKnowledgeList(src.commonObjections || src.common_objections),
    typicalLanguage: asKnowledgeList(src.typicalLanguage || src.typical_language),
    recommendedMessaging: asKnowledgeList(
      src.recommendedMessaging || src.recommended_messaging
    ),
    discoveryQuestions: asKnowledgeList(src.discoveryQuestions || src.discovery_questions),
    caseStudies: asKnowledgeList(src.caseStudies || src.case_studies),
    successStories: asKnowledgeList(src.successStories || src.success_stories),
    updatedAt: src.updatedAt || nowIso(),
  };
  record.unknowns = [];
  if (!record.definition) record.unknowns.push('definition');
  if (!record.caseStudies.length) record.unknowns.push('caseStudies');
  if (!record.successStories.length) record.unknowns.push('successStories');
  return record;
}

function knowledgeMap(records = []) {
  const byId = {};
  for (const row of (Array.isArray(records) ? records : []).map(buildPainKnowledge)) {
    if (row.painId) byId[row.painId] = row;
  }
  return byId;
}

function getKnowledgeForPain(aim, painId) {
  if (!aim) return null;
  const map = aim.knowledgeById || knowledgeMap(aim.knowledge);
  return map[painId] || null;
}

module.exports = {
  KNOWLEDGE_FIELDS,
  buildPainKnowledge,
  knowledgeMap,
  getKnowledgeForPain,
};
