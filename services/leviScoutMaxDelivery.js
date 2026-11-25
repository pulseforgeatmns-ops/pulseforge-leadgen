'use strict';

const { TENANT_ID } = require('./leviScoutProductionContract');
const { AGENT } = require('./leviScoutEnablement');

function compactResult(result = {}) {
  return Object.freeze({
    tenantId: String(result.tenantId || TENANT_ID),
    cycleId: result.cycleId || null,
    status: result.status || 'completed',
    discovered: Number(result.discovered || 0),
    persisted: Number(result.persisted || 0),
    qualified: Number(result.qualified || 0),
    researchCandidates: Number(result.researchCandidates != null ? result.researchCandidates : result.discovered || 0),
    qualifiedProspects: Number(result.qualifiedProspects != null ? result.qualifiedProspects : result.qualified || 0),
    verifiableBusinessContacts: Number(result.verifiableBusinessContacts || 0),
    cities: Object.freeze([...(result.cities || [])]),
    outreachAttempted: false,
    paidProvidersUsed: false,
    inventedCriteria: false,
    classifications: Object.freeze({ ...(result.classifications || {}) }),
    missingBlueprintDecisions: Object.freeze([...(result.missingBlueprintDecisions || [])]),
    blueprintQuestions: Object.freeze([...(result.blueprintQuestions || [])]),
  });
}

function briefingText(snapshot) {
  const questions = snapshot.blueprintQuestions || [];
  const lines = [
    `Levi Scout (tenant ${snapshot.tenantId}) ${snapshot.cycleId || 'cycle'} ${snapshot.status}.`,
    `Discovered ${snapshot.discovered} in ${(snapshot.cities || []).join(', ') || 'no cities'}.`,
    `Research candidates: ${snapshot.researchCandidates}; qualified prospects: ${snapshot.qualifiedProspects}; verifiable public business contacts: ${snapshot.verifiableBusinessContacts}.`,
    'A permit is property activity, not borrower intent. Review-only persist. Places, paid providers and outbound stayed off.',
  ];
  if (questions.length) {
    lines.push(`Missing Blueprint decisions for Levi: ${questions.join(' ')}`);
  }
  return lines.join(' ');
}

async function deliverToMax(result, { db, sink } = {}) {
  const snapshot = compactResult(result);
  const card = {
    created_by: AGENT,
    action_type: 'levi_scout_research_briefing',
    title: 'Levi Scout research-only cycle',
    description: briefingText(snapshot),
    payload: snapshot,
    status: 'pending',
    client_id: Number(snapshot.tenantId),
  };
  if (typeof sink === 'function') await sink(card);
  if (db && typeof db.query === 'function') {
    await db.query(
      `INSERT INTO agent_actions (created_by, action_type, title, description, payload, status, client_id)
       VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)`,
      [card.created_by, card.action_type, card.title, card.description, JSON.stringify(card.payload), card.status, card.client_id]
    );
  }
  return snapshot;
}

module.exports = { compactResult, briefingText, deliverToMax };
