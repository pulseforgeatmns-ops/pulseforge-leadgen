'use strict';

/**
 * SPEC-112 — Acquisition Intelligence Model.
 * Understand a market before selling into it.
 */

const types = require('./types');
const market = require('./MarketUnderstanding');
const pain = require('./PainOntology');
const qualification = require('./Qualification');
const knowledge = require('./KnowledgeCapture');
const messaging = require('./MessagingIntelligence');
const pilot = require('./PilotStatus');
const { createMemoryAimStore } = require('./Store');
const fedir = require('./seeds/fedir');

function buildAim(partial = {}) {
  const marketUnderstanding = market.buildMarketUnderstanding(partial);
  const painOntology = pain.buildPainOntology(partial.painOntology || partial.categories || []);
  const knowledgeRecords = (partial.knowledge || []).map(knowledge.buildPainKnowledge);
  return {
    id: types.asText(partial.id) || `aim-${types.asText(partial.clientKey) || 'draft'}`,
    clientKey: types.asText(partial.clientKey),
    clientName: types.asText(partial.clientName),
    spec: 'SPEC-112',
    status: partial.status || types.AIM_STATUS.DRAFT,
    version: Number(partial.version) || 1,
    isOperatingFact: false,
    ...marketUnderstanding,
    painOntology,
    knowledge: knowledgeRecords,
    knowledgeById: knowledge.knowledgeMap(knowledgeRecords),
  };
}

function qualifyAndBrief(aim, prospect, opts = {}) {
  const result = qualification.qualifyProspect(aim, prospect, opts);
  const briefing = messaging.briefPaige({ aim, qualification: result });
  return { qualification: result, briefing };
}

module.exports = {
  ...types,
  ...market,
  ...pain,
  ...qualification,
  ...knowledge,
  ...messaging,
  ...pilot,
  createMemoryAimStore,
  ...fedir,
  buildAim,
  qualifyAndBrief,
};
