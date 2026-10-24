'use strict';

/**
 * SPEC-161 — Market Memory.
 * Persists synthesized market understanding across missions and measures evolution.
 * ADR-081 — Markets Are Living Systems.
 *
 * Invariant: every investigation begins by recalling existing market understanding
 * before collecting new evidence.
 */

const {
  buildMarketMemory,
  buildCompanyMemory,
  companyEntityKey,
  marketEntityKey,
  MEMORY_STATUS,
  asText,
  nowIso,
} = require('./types');

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function normalizeEntityName(name) {
  return asText(name).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function buildBusinessMemory(partial = {}) {
  return {
    entityId: asText(partial.entityId),
    name: asText(partial.name),
    industry: asText(partial.industry) || null,
    currentUnderstanding: partial.currentUnderstanding || {
      assertions: [],
      confidence: partial.confidence != null ? Number(partial.confidence) : 0,
      summary: partial.summary || null,
    },
    historicalUnderstandings: Array.isArray(partial.historicalUnderstandings)
      ? partial.historicalUnderstandings
      : [],
    confidenceHistory: Array.isArray(partial.confidenceHistory)
      ? partial.confidenceHistory
      : [],
    buyingSignalHistory: Array.isArray(partial.buyingSignalHistory)
      ? partial.buyingSignalHistory
      : [],
    relationshipHistory: Array.isArray(partial.relationshipHistory)
      ? partial.relationshipHistory
      : [],
    evidenceTimeline: Array.isArray(partial.evidenceTimeline) ? partial.evidenceTimeline : [],
    status: partial.status || 'active',
    lastUpdated: partial.lastUpdated || nowIso(),
    missionId: asText(partial.missionId) || null,
  };
}

function buildMarketMemoryRecord(partial = {}) {
  const base = buildMarketMemory(partial);
  return {
    ...base,
    marketId: partial.marketId || base.entityKey,
    industries: Array.isArray(partial.industries)
      ? partial.industries
      : base.knownIndustries || [],
    entities: Array.isArray(partial.entities) ? partial.entities : [],
    relationships: Array.isArray(partial.relationships) ? partial.relationships : [],
    marketUnderstanding: partial.marketUnderstanding || {
      summary: partial.summary || null,
      dominantTerminology: partial.dominantTerminology || [],
      trends: partial.trends || [],
      buyingSignals: partial.buyingSignals || [],
      outstandingUnknowns: partial.outstandingUnknowns || [],
    },
    historicalSnapshots: Array.isArray(partial.historicalSnapshots)
      ? partial.historicalSnapshots
      : [],
    confidenceHistory: Array.isArray(partial.confidenceHistory)
      ? partial.confidenceHistory
      : [],
    lastInvestigationAt: partial.lastInvestigationAt || null,
    investigationCount: partial.investigationCount != null ? Number(partial.investigationCount) : 0,
    spec: 'SPEC-161',
  };
}

function buildMarketSnapshot(input = {}) {
  return {
    missionId: asText(input.missionId) || null,
    at: input.at || nowIso(),
    confidence: input.confidence != null ? Number(input.confidence) : 0,
    entityCount: input.entityCount != null ? Number(input.entityCount) : 0,
    marketUnderstanding: input.marketUnderstanding || null,
    universeEstimate: input.universeEstimate || null,
    coverage: input.coverage || null,
    changesSincePrior: input.changesSincePrior || null,
    reason: input.reason || 'Investigation completed',
  };
}

function appendMarketSnapshot(marketMemory, snapshot) {
  const next = clone(marketMemory || buildMarketMemoryRecord());
  const record = buildMarketSnapshot(snapshot);
  next.historicalSnapshots = [...(next.historicalSnapshots || []), record];
  next.lastInvestigationAt = record.at;
  next.investigationCount = (next.investigationCount || 0) + 1;
  next.lastUpdated = record.at;
  if (record.confidence != null) {
    next.confidence = record.confidence;
    next.confidenceHistory = [
      ...(next.confidenceHistory || []),
      { at: record.at, confidence: record.confidence, missionId: record.missionId },
    ];
  }
  return next;
}

function entityKeyFromBusiness(business = {}) {
  return asText(business.entityId) || companyEntityKey(business);
}

function indexBusinesses(businesses = []) {
  const map = new Map();
  for (const row of businesses) {
    const key = entityKeyFromBusiness(row);
    if (key) map.set(key, row);
  }
  return map;
}

function businessFromUnderstanding(understanding = {}, context = {}) {
  const entity = asText(understanding.entity || understanding.name);
  if (!entity) return null;

  const buyingSignals = (understanding.buyingSignals || []).map((signal) => ({
    type: signal.type || signal.kind || 'signal',
    label: signal.label || signal.observation || String(signal),
    observedAt: signal.observedAt || context.at || nowIso(),
    source: signal.source || 'synthesis',
  }));

  return buildBusinessMemory({
    entityId: asText(understanding.entityId) || companyEntityKey({ name: entity }),
    name: entity,
    industry: understanding.industry || context.industry || null,
    currentUnderstanding: {
      assertions: understanding.assertions || [],
      confidence: understanding.confidence != null ? Number(understanding.confidence) : 0,
      summary: understanding.reasoning || (understanding.assertions || []).join('; '),
    },
    confidenceHistory: [
      {
        at: context.at || nowIso(),
        confidence: understanding.confidence != null ? Number(understanding.confidence) : 0,
        missionId: context.missionId || null,
      },
    ],
    buyingSignalHistory: buyingSignals,
    evidenceTimeline: (understanding.supportingEvidence || []).map((item) => ({
      at: item.timestamp || context.at || nowIso(),
      observation: item.observation || item.label || String(item),
      source: item.source || 'evidence',
      confidence: item.confidence != null ? Number(item.confidence) : null,
    })),
    missionId: context.missionId || null,
  });
}

function reviseBusinessUnderstanding(existing, incoming, context = {}) {
  if (!existing) return incoming;

  const priorUnderstanding = clone(existing.currentUnderstanding || {});
  const historical = [...(existing.historicalUnderstandings || [])];
  const priorConfidence = priorUnderstanding.confidence != null ? priorUnderstanding.confidence : 0;
  const nextConfidence =
    incoming.currentUnderstanding?.confidence != null
      ? Number(incoming.currentUnderstanding.confidence)
      : priorConfidence;

  const assertionsChanged =
    JSON.stringify(priorUnderstanding.assertions || []) !==
    JSON.stringify(incoming.currentUnderstanding?.assertions || []);

  if (assertionsChanged || Math.abs(nextConfidence - priorConfidence) >= 0.05) {
    historical.push({
      at: context.at || nowIso(),
      understanding: priorUnderstanding,
      reason: context.reason || 'Evidence changed business understanding',
      missionId: context.missionId || null,
      archived: true,
    });
  }

  const mergedSignals = [...(existing.buyingSignalHistory || [])];
  for (const signal of incoming.buyingSignalHistory || []) {
    if (!mergedSignals.some((s) => s.label === signal.label && s.type === signal.type)) {
      mergedSignals.push(signal);
    }
  }

  const mergedTimeline = [...(existing.evidenceTimeline || [])];
  for (const item of incoming.evidenceTimeline || []) {
    mergedTimeline.push(item);
  }

  const confidenceHistory = [...(existing.confidenceHistory || [])];
  if (nextConfidence !== priorConfidence) {
    confidenceHistory.push({
      at: context.at || nowIso(),
      confidence: nextConfidence,
      priorConfidence,
      delta: Number((nextConfidence - priorConfidence).toFixed(3)),
      reason: context.reason || 'Understanding revised',
      missionId: context.missionId || null,
    });
  }

  return buildBusinessMemory({
    ...existing,
    ...incoming,
    currentUnderstanding: incoming.currentUnderstanding || priorUnderstanding,
    historicalUnderstandings: historical,
    confidenceHistory,
    buyingSignalHistory: mergedSignals,
    evidenceTimeline: mergedTimeline,
    lastUpdated: context.at || nowIso(),
    missionId: context.missionId || existing.missionId,
    status:
      (incoming.currentUnderstanding?.contradictoryEvidence || []).length > 0
        ? MEMORY_STATUS.CONFLICT
        : existing.status || MEMORY_STATUS.ACTIVE,
  });
}

function mergeBusinessMemories(existingEntities = [], incomingEntities = [], context = {}) {
  const index = indexBusinesses(existingEntities);
  const merged = [];

  for (const incoming of incomingEntities) {
    const key = entityKeyFromBusiness(incoming);
    const existing = index.get(key);
    if (existing) {
      merged.push(reviseBusinessUnderstanding(existing, incoming, context));
      index.delete(key);
    } else {
      merged.push(incoming);
    }
  }

  for (const remaining of index.values()) {
    merged.push({
      ...remaining,
      status: remaining.status === MEMORY_STATUS.ACTIVE ? 'inactive' : remaining.status,
    });
  }

  return merged;
}

function detectEntityChanges(priorEntities = [], currentEntities = []) {
  const priorIndex = indexBusinesses(priorEntities);
  const currentIndex = indexBusinesses(currentEntities);

  const newOperators = [];
  const removedOperators = [];
  const businessesExpanded = [];
  const buyingSignalsIncreased = [];
  const understandingRevisions = [];
  const observations = [];

  for (const [key, current] of currentIndex.entries()) {
    const prior = priorIndex.get(key);
    if (!prior) {
      newOperators.push(current);
      observations.push({
        kind: 'entity_discovered',
        entity: current.name,
        at: nowIso(),
        detail: 'New operator discovered since last investigation',
      });
      continue;
    }

    const priorAssertions = prior.currentUnderstanding?.assertions || [];
    const currentAssertions = current.currentUnderstanding?.assertions || [];
    if (JSON.stringify(priorAssertions) !== JSON.stringify(currentAssertions)) {
      understandingRevisions.push({
        entity: current.name,
        prior: priorAssertions,
        current: currentAssertions,
        reason: 'Business understanding changed',
      });
      observations.push({
        kind: 'understanding_revised',
        entity: current.name,
        prior: priorAssertions,
        current: currentAssertions,
        reason: 'Evidence changed business understanding',
      });
    }

    const priorSignals = prior.buyingSignalHistory?.length || 0;
    const currentSignals = current.buyingSignalHistory?.length || 0;
    if (currentSignals > priorSignals) {
      buyingSignalsIncreased.push({
        entity: current.name,
        priorCount: priorSignals,
        currentCount: currentSignals,
        delta: currentSignals - priorSignals,
      });
      observations.push({
        kind: 'buying_signal_increase',
        entity: current.name,
        delta: currentSignals - priorSignals,
      });
    }

    const priorConf = prior.currentUnderstanding?.confidence || 0;
    const currentConf = current.currentUnderstanding?.confidence || 0;
    if (currentConf > priorConf + 0.05) {
      businessesExpanded.push({
        entity: current.name,
        priorConfidence: priorConf,
        currentConfidence: currentConf,
        assertionsAdded: Math.max(0, currentAssertions.length - priorAssertions.length),
      });
    }
  }

  for (const [key, prior] of priorIndex.entries()) {
    if (!currentIndex.has(key)) {
      removedOperators.push(prior);
      observations.push({
        kind: 'entity_removed',
        entity: prior.name,
        detail: 'Operator not observed in current investigation',
      });
    }
  }

  return {
    newOperators,
    removedOperators,
    businessesExpanded,
    buyingSignalsIncreased,
    understandingRevisions,
    observations,
  };
}

function detectMarketChanges(priorMarketMemory, currentState = {}) {
  if (!priorMarketMemory) {
    return {
      hasPriorMemory: false,
      marketGrowth: null,
      confidenceChange: null,
      entityChanges: detectEntityChanges([], currentState.entities || []),
      marketDrift: null,
      observations: [],
      duplicateInvestigationAvoided: false,
    };
  }

  const priorEntities = priorMarketMemory.entities || [];
  const currentEntities = currentState.entities || [];
  const entityChanges = detectEntityChanges(priorEntities, currentEntities);

  const priorCount =
    priorMarketMemory.coverage?.investigated ||
    priorEntities.filter((e) => e.status !== 'inactive').length ||
    0;
  const currentCount =
    currentState.coverage?.investigated ||
    currentEntities.filter((e) => e.status !== 'inactive').length ||
    0;
  const marketGrowth =
    priorCount || currentCount
      ? {
          prior: priorCount,
          current: currentCount,
          delta: currentCount - priorCount,
          label:
            currentCount > priorCount
              ? `+${currentCount - priorCount} operators`
              : currentCount < priorCount
                ? `${currentCount - priorCount} operators`
                : 'No operator count change',
        }
      : null;

  const priorConfidence = priorMarketMemory.confidence != null ? priorMarketMemory.confidence : 0;
  const currentConfidence =
    currentState.confidence != null ? Number(currentState.confidence) : priorConfidence;
  const confidenceDelta = Number((currentConfidence - priorConfidence).toFixed(3));

  const confidenceChange = {
    prior: priorConfidence,
    current: currentConfidence,
    delta: confidenceDelta,
    direction:
      confidenceDelta > 0.01 ? 'increased' : confidenceDelta < -0.01 ? 'decreased' : 'stable',
  };

  let marketDrift = null;
  const priorTerms = priorMarketMemory.marketUnderstanding?.dominantTerminology || [];
  const currentTerms = currentState.marketUnderstanding?.dominantTerminology || [];
  if (
    priorTerms.length &&
    currentTerms.length &&
    normalizeEntityName(priorTerms[0]) !== normalizeEntityName(currentTerms[0])
  ) {
    marketDrift = {
      prior: priorTerms[0],
      current: currentTerms[0],
      reason: 'Dominant market terminology shifted',
    };
    entityChanges.observations.push({
      kind: 'market_drift',
      prior: priorTerms[0],
      current: currentTerms[0],
      reason: 'Market drift detected — terminology shifted',
    });
  }

  const meaningfulChange =
    entityChanges.newOperators.length > 0 ||
    entityChanges.removedOperators.length > 0 ||
    entityChanges.understandingRevisions.length > 0 ||
    entityChanges.buyingSignalsIncreased.length > 0 ||
    marketDrift ||
    Math.abs(confidenceDelta) >= 0.05 ||
    (marketGrowth && marketGrowth.delta !== 0);

  const duplicateInvestigationAvoided =
    !meaningfulChange && priorMarketMemory.investigationCount > 0;

  if (duplicateInvestigationAvoided && confidenceChange.direction === 'stable') {
    confidenceChange.current = Math.min(0.99, priorConfidence + 0.03);
    confidenceChange.delta = Number((confidenceChange.current - priorConfidence).toFixed(3));
    confidenceChange.direction = 'increased';
    confidenceChange.reason = 'No meaningful changes — confidence reinforced';
    entityChanges.observations.push({
      kind: 'confidence_reinforced',
      detail: 'No meaningful market changes detected; prior understanding verified',
    });
  }

  return {
    hasPriorMemory: true,
    marketGrowth,
    confidenceChange,
    entityChanges,
    marketDrift,
    observations: entityChanges.observations,
    duplicateInvestigationAvoided,
    meaningfulChange,
  };
}

function buildMarketChangesSection(changes = {}, context = {}) {
  const entityChanges = changes.entityChanges || {};
  return {
    spec: 'SPEC-161',
    hasPriorMemory: changes.hasPriorMemory === true,
    marketChangesSinceLastInvestigation: {
      newOperators: (entityChanges.newOperators || []).map((e) => ({
        name: e.name,
        confidence: e.currentUnderstanding?.confidence,
      })),
      removedOperators: (entityChanges.removedOperators || []).map((e) => ({
        name: e.name,
        lastSeenAt: e.lastUpdated,
      })),
      businessesExpanded: entityChanges.businessesExpanded || [],
      buyingSignalsIncreased: entityChanges.buyingSignalsIncreased || [],
      understandingRevisions: entityChanges.understandingRevisions || [],
      marketGrowth: changes.marketGrowth,
      confidenceChange: changes.confidenceChange,
      marketDrift: changes.marketDrift,
      outstandingUnknowns: context.outstandingUnknowns || [],
      duplicateInvestigationAvoided: changes.duplicateInvestigationAvoided === true,
      observations: changes.observations || [],
    },
  };
}

function mergeMarketMemory(existing, incoming, context = {}) {
  if (!existing) return incoming;

  const mergedEntities = mergeBusinessMemories(
    existing.entities || [],
    incoming.entities || [],
    context
  );

  const mergedRelationships = [...(existing.relationships || [])];
  for (const rel of incoming.relationships || []) {
    const key = `${rel.from}:${rel.relation}:${rel.to}`;
    if (!mergedRelationships.some((r) => `${r.from}:${r.relation}:${r.to}` === key)) {
      mergedRelationships.push(rel);
    }
  }

  const mergedUnderstanding = {
    ...(existing.marketUnderstanding || {}),
    ...(incoming.marketUnderstanding || {}),
    outstandingUnknowns: [
      ...new Set([
        ...((existing.marketUnderstanding || {}).outstandingUnknowns || []),
        ...((incoming.marketUnderstanding || {}).outstandingUnknowns || []),
      ]),
    ],
  };

  return buildMarketMemoryRecord({
    ...existing,
    ...incoming,
    entities: mergedEntities,
    relationships: mergedRelationships,
    marketUnderstanding: mergedUnderstanding,
    industries: [
      ...new Set([...(existing.industries || []), ...(incoming.industries || [])]),
    ],
    historicalSnapshots: existing.historicalSnapshots || [],
    confidenceHistory: existing.confidenceHistory || [],
    investigationCount: existing.investigationCount || 0,
    updatedAt: context.at || nowIso(),
  });
}

function extractEntitiesFromDiscovery(pipelineResult = {}, context = {}) {
  const entities = [];
  const missionReport = pipelineResult.missionIntelligenceReport || {};
  const businessItems =
    missionReport.businessUnderstanding?.items ||
    pipelineResult.investigationState?.businessUnderstandings ||
    [];

  for (const item of businessItems) {
    const business = businessFromUnderstanding(item, context);
    if (business) entities.push(business);
  }

  const candidates =
    pipelineResult.intelligenceResult?.payload?.candidateUniverse ||
    pipelineResult.intelligenceReport?.candidateUniverse ||
    [];

  for (const candidate of candidates) {
    const key = companyEntityKey(candidate);
    if (entities.some((e) => entityKeyFromBusiness(e) === key)) continue;
    entities.push(
      buildBusinessMemory({
        entityId: key,
        name: asText(candidate.name),
        industry: asText(candidate.industry),
        currentUnderstanding: {
          assertions: [`Discovered operator: ${asText(candidate.name)}`],
          confidence: candidate.icpScore != null ? Number(candidate.icpScore) / 100 : 0.5,
          summary: `Operator in ${asText(candidate.location) || 'market'}`,
        },
        buyingSignalHistory: (candidate.signals || []).map((signal) => ({
          type: signal.type || signal.kind || 'signal',
          label: signal.label || String(signal),
          observedAt: context.at || nowIso(),
          source: signal.source || 'discovery',
        })),
        missionId: context.missionId || null,
      })
    );
  }

  return entities;
}

function extractMarketMemoryFromDiscovery(pipelineResult = {}, context = {}) {
  const marketDefinition =
    pipelineResult.marketDefinition ||
    pipelineResult.missionIntelligenceReport?.finalMarketDefinition ||
    {};
  const tenantId = asText(context.tenantId);
  const geography = asText(marketDefinition.geography || context.geography);
  const segment = asText(
    marketDefinition.segment ||
      marketDefinition.segments?.[0] ||
      context.segment ||
      marketDefinition.market
  );
  if (!tenantId || !geography) return null;

  const entityKey = marketEntityKey(geography, segment);
  const entities = extractEntitiesFromDiscovery(pipelineResult, context);
  const missionReport = pipelineResult.missionIntelligenceReport || {};
  const confidence =
    pipelineResult.confidence ||
    missionReport.currentConfidence ||
    pipelineResult.investigationState?.confidence ||
    0;

  const marketUnderstanding = {
    summary: missionReport.summary || null,
    dominantTerminology: marketDefinition.terminology || [],
    trends: [],
    buyingSignals: marketDefinition.buyingSignals || [],
    outstandingUnknowns: missionReport.remainingUnknowns || [],
  };

  const incoming = buildMarketMemoryRecord({
    tenantId,
    entityKey,
    label: [geography, segment].filter(Boolean).join(' — '),
    geography,
    segment,
    marketId: entityKey,
    industries: marketDefinition.customerTypes || marketDefinition.industries || [],
    entities,
    relationships: extractRelationshipsFromState(pipelineResult.investigationState),
    marketUnderstanding,
    confidence,
    coverage: {
      investigated: entities.filter((e) => e.status !== 'inactive').length,
      qualified: pipelineResult.qualifiedCount || 0,
      overallConfidence: confidence,
    },
    missionId: context.missionId || null,
    verifiedAt: context.completedAt || nowIso(),
    sourceCount: (pipelineResult.stages || []).length || 1,
    verificationSources: ['discovery_pipeline'],
  });

  const prior = context.priorMarketMemory || null;
  const changes = detectMarketChanges(prior, {
    entities,
    confidence,
    coverage: incoming.coverage,
    marketUnderstanding,
  });

  if (changes.confidenceChange?.current != null) {
    incoming.confidence = changes.confidenceChange.current;
  }

  let merged = prior ? mergeMarketMemory(prior, incoming, context) : incoming;
  merged = appendMarketSnapshot(merged, {
    missionId: context.missionId,
    at: context.completedAt || nowIso(),
    confidence: merged.confidence,
    entityCount: entities.length,
    marketUnderstanding,
    universeEstimate: pipelineResult.universeEstimate || null,
    coverage: merged.coverage,
    changesSincePrior: changes.hasPriorMemory ? changes : null,
    reason: changes.duplicateInvestigationAvoided
      ? 'No meaningful changes — confidence reinforced'
      : 'Investigation completed',
  });

  return {
    marketMemory: merged,
    changes,
    entities,
  };
}

function extractRelationshipsFromState(investigationState = {}) {
  const edges = investigationState?.evidenceGraph?.edges || [];
  return edges.slice(0, 50).map((edge) => ({
    from: edge.from,
    to: edge.to,
    relation: edge.relation || 'related',
  }));
}

function recallMarketMemoryForInvestigation(input = {}) {
  const priorMarketMemory = input.priorMarketMemory || input.memory?.market || null;
  if (!priorMarketMemory) {
    return {
      loaded: false,
      marketMemory: null,
      entities: [],
      confidence: 0,
      investigationStartsFromMemory: false,
    };
  }

  return {
    loaded: true,
    marketMemory: priorMarketMemory,
    entities: priorMarketMemory.entities || [],
    relationships: priorMarketMemory.relationships || [],
    marketUnderstanding: priorMarketMemory.marketUnderstanding || null,
    confidence: priorMarketMemory.confidence || 0,
    historicalSnapshots: priorMarketMemory.historicalSnapshots || [],
    investigationStartsFromMemory: true,
    lastInvestigationAt: priorMarketMemory.lastInvestigationAt || null,
  };
}

module.exports = {
  buildBusinessMemory,
  buildMarketMemoryRecord,
  buildMarketSnapshot,
  appendMarketSnapshot,
  businessFromUnderstanding,
  reviseBusinessUnderstanding,
  mergeBusinessMemories,
  detectEntityChanges,
  detectMarketChanges,
  buildMarketChangesSection,
  mergeMarketMemory,
  extractEntitiesFromDiscovery,
  extractMarketMemoryFromDiscovery,
  recallMarketMemoryForInvestigation,
  entityKeyFromBusiness,
  indexBusinesses,
};
