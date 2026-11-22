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
    cities: Object.freeze([...(result.cities || [])]),
    outreachAttempted: false,
    paidProvidersUsed: false,
    classifications: Object.freeze({ ...(result.classifications || {}) }),
  });
}

function briefingText(snapshot) {
  return [
    `Levi Scout (tenant ${snapshot.tenantId}) ${snapshot.cycleId || 'cycle'} ${snapshot.status}.`,
    `Discovered ${snapshot.discovered} in ${(snapshot.cities || []).join(', ') || 'no cities'}; ${snapshot.qualified} contract-qualified.`,
    'Review-only persist. Places, paid providers and outbound stayed off.',
  ].join(' ');
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
