'use strict';

/**
 * SPEC-112 Phase 5 — Messaging intelligence for Paige.
 * Paige never starts from zero when AIM qualification exists.
 * Missing proof stays unknown — never invented.
 */

const { asText } = require('./types');
const { getKnowledgeForPain } = require('./KnowledgeCapture');

function proofFromKnowledge(knowledge) {
  if (!knowledge) {
    return {
      available: false,
      items: [],
      unknown: 'No captured proof for this pain yet.',
    };
  }
  const items = [...(knowledge.caseStudies || []), ...(knowledge.successStories || [])];
  if (!items.length) {
    return {
      available: false,
      items: [],
      unknown: 'Case studies and success stories are not captured yet — do not invent them.',
    };
  }
  return { available: true, items, unknown: null };
}

/**
 * @param {{ aim: object, qualification: object }} input
 * @returns {object} Paige briefing
 */
function briefPaige({ aim, qualification } = {}) {
  if (!qualification) {
    return {
      kind: 'aim_messaging_brief',
      spec: 'SPEC-112',
      available: false,
      reason: 'No AIM qualification — Paige has no market brief yet.',
    };
  }
  const top = qualification.topPain;
  const knowledge = top ? getKnowledgeForPain(aim, top.id) : null;
  const proof = proofFromKnowledge(knowledge);
  const likelyPain = top
    ? {
        id: top.id,
        label: top.label,
        percent: top.percent,
        definition: (knowledge && knowledge.definition) || top.definition || '',
      }
    : {
        id: null,
        label: null,
        percent: 0,
        definition: '',
      };

  const language = knowledge && knowledge.typicalLanguage.length
    ? knowledge.typicalLanguage
    : top
      ? [`They are likely feeling ${String(top.label).toLowerCase()}.`]
      : [];

  const messaging = knowledge && knowledge.recommendedMessaging.length
    ? knowledge.recommendedMessaging
    : [];

  const cta = messaging[0] ||
    (top
      ? `Open a conversation about ${String(top.label).toLowerCase()} — not a generic pitch.`
      : 'Do not pitch. Evidence of pain is still missing.');

  return {
    kind: 'aim_messaging_brief',
    spec: 'SPEC-112',
    available: true,
    prospectId: asText(qualification.prospectId),
    prospectName: asText(qualification.prospectName),
    likelyPain,
    language,
    proof,
    cta,
    avoid: [
      'Do not start from a blank prompt when AIM qualification exists.',
      'Do not invent case studies or success stories.',
      'Do not pitch transformation the prospect has not evidenced.',
    ],
    discoveryQuestions: (knowledge && knowledge.discoveryQuestions) || [],
    objections: (knowledge && knowledge.commonObjections) || [],
    recommendation: qualification.overallRecommendation,
  };
}

module.exports = {
  briefPaige,
  proofFromKnowledge,
};
