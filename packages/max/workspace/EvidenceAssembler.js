'use strict';

const { normalizeEvidenceRef, normalizeEntityRef } = require('./WorkspaceTypes');

/**
 * Assemble evidence refs strictly from the MaxContext envelope / deck snapshot.
 * Never queries repositories. Never invents ids.
 *
 * @param {object} context - normalized MaxContext
 * @returns {object}
 */
function assembleEvidence(context) {
  const supporting = [];
  const contradicting = [];
  const confidenceContributors = [];
  const timelineReferences = [];
  const relatedEntities = [];
  const sourcesUsed = {
    briefing: false,
    reasoning: false,
    memory: false,
    policy: false,
    knowledge: false,
  };
  const unavailable = [];

  const cards = listRelevantCards(context);
  if (context.briefing) sourcesUsed.briefing = true;
  if (context.deck && context.deck.morningBrief) sourcesUsed.briefing = true;

  for (const card of cards) {
    collectFromCard(card, {
      supporting,
      contradicting,
      confidenceContributors,
      timelineReferences,
      relatedEntities,
      sourcesUsed,
    });
  }

  if (context.deck) {
    collectFromDeck(context.deck, {
      supporting,
      contradicting,
      confidenceContributors,
      timelineReferences,
      relatedEntities,
      sourcesUsed,
    });
  }

  if (supporting.length === 0) {
    unavailable.push('supporting_evidence');
  }
  if (contradicting.length === 0) {
    unavailable.push('contradicting_evidence');
  }

  // Sources on cards imply knowledge / reasoning provenance without querying
  for (const card of cards) {
    for (const src of card.sources || []) {
      const kind = String((src && src.kind) || '').toLowerCase();
      if (kind === 'briefing') sourcesUsed.briefing = true;
      if (kind === 'reasoning' || kind === 'recommendation') {
        sourcesUsed.reasoning = true;
      }
      if (kind === 'memory' || kind === 'watch' || kind === 'diff') {
        sourcesUsed.memory = true;
      }
      if (kind === 'policy') sourcesUsed.policy = true;
      if (kind === 'knowledge' || kind === 'evidence' || kind === 'claim') {
        sourcesUsed.knowledge = true;
      }
    }
    if (card.reasoningId) sourcesUsed.reasoning = true;
    if (card.policyId) sourcesUsed.policy = true;
    if (card.briefingId) sourcesUsed.briefing = true;
  }

  return {
    supportingEvidence: dedupeById(supporting),
    contradictingEvidence: dedupeById(contradicting),
    confidenceContributors: [...new Set(confidenceContributors)],
    timelineReferences: dedupeById(timelineReferences),
    relatedEntities: dedupeById(relatedEntities.map(normalizeEntityRef)),
    sourcesUsed,
    unavailable,
    confidence: firstConfidence(cards, context),
    asOf: context.asOf,
  };
}

function listRelevantCards(context) {
  const cards = [...(context.visibleCards || [])];
  if (context.deck && Array.isArray(context.deck.cards)) {
    for (const c of context.deck.cards) {
      if (c && !cards.some((x) => x.id === c.id)) cards.push(c);
    }
  }
  if (context.deck && context.deck.highestLeverageAction) {
    // HLA may only exist as model field; prefer matching visible card
  }
  if (!context.recommendationId && !context.companyId) {
    return cards;
  }
  const filtered = cards.filter((card) => cardMatchesFocus(card, context));
  return filtered.length ? filtered : cards;
}

function cardMatchesFocus(card, context) {
  const payload = card.payload || {};
  const rec = payload.recommendation || {};
  if (context.recommendationId) {
    if (
      String(card.reasoningId) === String(context.recommendationId) ||
      String(rec.id) === String(context.recommendationId) ||
      String(card.id).includes(String(context.recommendationId))
    ) {
      return true;
    }
  }
  if (context.companyId) {
    if (
      String(payload.companyId) === String(context.companyId) ||
      String(rec.companyId) === String(context.companyId) ||
      (context.selectedEntity &&
        String(context.selectedEntity.id) === String(payload.companyId))
    ) {
      return true;
    }
  }
  return false;
}

function collectFromCard(card, bag) {
  const payload = card.payload || {};

  pushSignals(payload.supportingSignals || payload.supportingEvidence, bag.supporting);
  pushSignals(
    payload.contradictingSignals ||
      payload.opposingSignals ||
      payload.contradictingEvidence,
    bag.contradicting
  );

  if (card.summary) {
    bag.confidenceContributors.push(String(card.summary));
  }
  if (card.confidence != null) {
    bag.confidenceContributors.push(`Card confidence ${card.confidence}`);
  }
  if (card.updatedAt) {
    bag.timelineReferences.push({
      id: `card-updated:${card.id}`,
      summary: `${card.title || card.type} updated`,
      at: card.updatedAt,
    });
  }

  const rec = payload.recommendation || {};
  if (rec.companyId || rec.companyName) {
    bag.relatedEntities.push({
      id: rec.companyId || rec.companyName,
      type: 'company',
      name: rec.companyName || rec.companyId,
    });
  }
  if (payload.companyId || payload.companyName) {
    bag.relatedEntities.push({
      id: payload.companyId || payload.companyName,
      type: 'company',
      name: payload.companyName || payload.companyId,
    });
  }

  if (payload.policy) {
    bag.sourcesUsed.policy = true;
    if (payload.policy.reason) {
      bag.confidenceContributors.push(`Policy: ${payload.policy.reason}`);
    }
  }
}

function collectFromDeck(deck, bag) {
  if (deck.morningBrief) {
    bag.sourcesUsed.briefing = true;
    if (deck.morningBrief.summary) {
      bag.supporting.push(
        normalizeEvidenceRef({
          id: 'briefing:summary',
          summary: deck.morningBrief.summary,
          sourceType: 'briefing',
          kind: 'briefing',
        })
      );
    }
  }

  const hla = deck.highestLeverageAction;
  if (hla) {
    bag.sourcesUsed.reasoning = true;
    pushSignals(hla.supportingSignals, bag.supporting);
    pushSignals(hla.contradictingSignals, bag.contradicting);
    if (hla.policy) bag.sourcesUsed.policy = true;
  }

  const watches = deck.watchAlerts || [];
  for (const alert of watches) {
    bag.sourcesUsed.memory = true;
    bag.supporting.push(
      normalizeEvidenceRef({
        id: alert.id || `watch:${alert.title}`,
        summary: alert.summary || alert.title || 'Watch alert',
        sourceType: 'memory',
        kind: 'watch',
      })
    );
  }

  const queue = (deck.priorityQueue && deck.priorityQueue.items) || [];
  for (const item of queue.slice(0, 5)) {
    bag.relatedEntities.push({
      id: item.companyId || item.id || item.title,
      type: 'company',
      name: item.companyName || item.title || item.id,
    });
    if (item.movement || item.trend) {
      bag.sourcesUsed.memory = true;
      bag.timelineReferences.push({
        id: `movement:${item.id || item.companyId}`,
        summary: `${item.companyName || item.title}: ${item.movement || item.trend}`,
        at: item.updatedAt || null,
      });
    }
  }
}

function pushSignals(list, target) {
  if (!Array.isArray(list)) return;
  for (const signal of list) {
    if (signal == null) continue;
    if (typeof signal === 'string') {
      target.push(
        normalizeEvidenceRef({
          id: `signal:${signal.slice(0, 40)}`,
          summary: signal,
          kind: 'signal',
        })
      );
      continue;
    }
    target.push(
      normalizeEvidenceRef({
        id: signal.id || signal.sourceId || `signal:${signal.summary || 'item'}`,
        summary: signal.summary || signal.statement || signal.title || '',
        sourceType: signal.sourceType || signal.kind || null,
        kind: signal.kind || 'signal',
        confidence: signal.confidence,
      })
    );
  }
}

function firstConfidence(cards, context) {
  for (const card of cards) {
    if (card.confidence != null && Number.isFinite(Number(card.confidence))) {
      return Number(card.confidence);
    }
    const payload = card.payload || {};
    if (payload.confidence != null && Number.isFinite(Number(payload.confidence))) {
      return Number(payload.confidence);
    }
  }
  const hla = context.deck && context.deck.highestLeverageAction;
  if (hla && hla.confidence != null && Number.isFinite(Number(hla.confidence))) {
    return Number(hla.confidence);
  }
  return null;
}

function dedupeById(items) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const id = String(item.id || JSON.stringify(item));
    if (seen.has(id)) continue;
    seen.add(id);
    out.push(item);
  }
  return out;
}

module.exports = {
  assembleEvidence,
};
