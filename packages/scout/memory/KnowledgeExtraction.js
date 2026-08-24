'use strict';

/**
 * SPEC-143 — Knowledge extraction from completed investigations.
 * Every completed investigation asks: what did we permanently learn?
 */

const {
  buildMarketMemory,
  buildCompanyMemory,
  buildPersonMemory,
  buildClaimMemory,
  buildInvestigationMemory,
  marketEntityKey,
  companyEntityKey,
  personEntityKey,
  claimEntityKey,
  asText,
} = require('./types');
const { mergeVerificationSources } = require('./MemoryConfidence');

function extractSourcesFromClaim(claim) {
  const sources = [];
  for (const item of claim.supportedBy || []) {
    if (typeof item === 'string') sources.push(item);
    else if (item && item.source) sources.push(item.source);
    else if (item && item.evidenceId) sources.push(item.evidenceId);
  }
  for (const item of claim.evidence || []) {
    if (typeof item === 'string') sources.push(item);
    else if (item && item.source) sources.push(item.source);
  }
  return mergeVerificationSources(sources);
}

function extractMarketMemory(investigationResult, context = {}) {
  const market = investigationResult.marketDefinition || {};
  const tenantId = asText(context.tenantId || context.mission?.tenantId);
  const geography = asText(market.geography || context.geography);
  const segment = asText(market.segment || market.segments?.[0]);
  if (!tenantId || !geography) return null;

  const candidates = investigationResult.candidateUniverse?.candidates || [];
  const industries = [
    ...new Set(candidates.map((c) => asText(c.industry)).filter(Boolean)),
  ];

  return buildMarketMemory({
    tenantId,
    entityKey: marketEntityKey(geography, segment),
    label: [geography, segment].filter(Boolean).join(' — '),
    geography,
    segment,
    knownIndustries: industries,
    marketSize: investigationResult.candidateUniverse?.estimatedMarket || null,
    coverage: {
      investigated: candidates.length,
      qualified: investigationResult.qualification?.qualifiedCount || 0,
      overallConfidence: investigationResult.overallConfidence || 0,
    },
    confidence: investigationResult.overallConfidence || 0,
    verifiedAt: context.completedAt || new Date().toISOString(),
    sourceCount: (investigationResult.iterations || []).length || 1,
    verificationSources: ['investigation'],
    missionId: context.missionId || investigationResult.missionId,
  });
}

function extractCompanyMemories(investigationResult, context = {}) {
  const tenantId = asText(context.tenantId || context.mission?.tenantId);
  if (!tenantId) return [];

  const candidates = investigationResult.candidateUniverse?.candidates || [];
  const claimsByEntity = new Map();
  for (const claim of investigationResult.claims || []) {
    const key = asText(claim.entityId);
    if (!key) continue;
    if (!claimsByEntity.has(key)) claimsByEntity.set(key, []);
    claimsByEntity.get(key).push(claim);
  }

  return candidates.map((candidate) => {
    const entityClaims = claimsByEntity.get(candidate.id) || [];
    const avgConf =
      entityClaims.length > 0
        ? entityClaims.reduce((s, c) => s + (c.confidence || 0), 0) / entityClaims.length
        : 0;

    return buildCompanyMemory({
      tenantId,
      entityKey: companyEntityKey(candidate),
      companyId: asText(candidate.id),
      label: asText(candidate.name),
      name: asText(candidate.name),
      industry: asText(candidate.industry),
      location: asText(candidate.location),
      website: asText(candidate.website),
      knownOffices: candidate.offices || (candidate.location ? [candidate.location] : []),
      decisionMakers: (candidate.people || []).map((p) => ({
        name: p.name,
        jobTitle: p.jobTitle,
        email: p.email,
      })),
      buyingSignals: (candidate.signals || []).map((s) => ({
        type: s.type || s.kind,
        label: s.label,
        observedAt: s.observedAt,
        source: s.source,
      })),
      evidence: candidate.evidence || [],
      confidence: avgConf,
      verifiedAt: context.completedAt || new Date().toISOString(),
      sourceCount: (candidate.evidence || []).length || 1,
      verificationSources: mergeVerificationSources(
        (candidate.evidence || []).map((e) => e.source).filter(Boolean)
      ),
      missionId: context.missionId,
    });
  });
}

function extractPersonMemories(investigationResult, context = {}) {
  const tenantId = asText(context.tenantId || context.mission?.tenantId);
  if (!tenantId) return [];

  const people = [];
  const candidates = investigationResult.candidateUniverse?.candidates || [];
  for (const candidate of candidates) {
    for (const person of candidate.people || []) {
      people.push(
        buildPersonMemory({
          tenantId,
          entityKey: personEntityKey({ ...person, companyId: candidate.id }),
          personId: asText(person.id),
          companyId: asText(candidate.id),
          label: asText(person.name),
          name: asText(person.name),
          jobTitle: asText(person.jobTitle),
          preferredChannel: person.preferredChannel || (person.email ? 'email' : null),
          responseHistory: person.responseHistory || [],
          relationshipHistory: person.relationshipHistory || [],
          confidence: person.confidence != null ? Number(person.confidence) : 0.7,
          verifiedAt: context.completedAt || new Date().toISOString(),
          sourceCount: 1,
          verificationSources: person.email ? ['email'] : ['investigation'],
          missionId: context.missionId,
        })
      );
    }
  }
  return people;
}

function extractClaimMemories(investigationResult, context = {}) {
  const tenantId = asText(context.tenantId || context.mission?.tenantId);
  if (!tenantId) return [];

  return (investigationResult.claims || []).map((claim) =>
    buildClaimMemory({
      tenantId,
      entityKey: claimEntityKey(claim),
      claimId: asText(claim.id),
      entityId: asText(claim.entityId),
      text: asText(claim.text),
      confidence: claim.confidence != null ? Number(claim.confidence) : 0,
      verified: (claim.confidence || 0) >= 0.8 && !(claim.missingEvidence || []).length,
      evidence: claim.supportedBy || [],
      supportedBy: claim.supportedBy || [],
      missingEvidence: claim.missingEvidence || [],
      contradictions: claim.contradictions || [],
      verifiedAt: context.completedAt || new Date().toISOString(),
      sourceCount: extractSourcesFromClaim(claim).length || 1,
      verificationSources: extractSourcesFromClaim(claim),
      missionId: context.missionId,
    })
  );
}

function extractInvestigationMemory(investigationResult, context = {}) {
  const market = investigationResult.marketDefinition || {};
  const tenantId = asText(context.tenantId || context.mission?.tenantId);
  const geography = asText(market.geography);
  const segment = asText(market.segment || market.segments?.[0]);
  if (!tenantId) return null;

  const attemptedSteps = [];
  for (const iteration of investigationResult.iterations || []) {
    if (iteration.nextStep) {
      attemptedSteps.push({
        gap: iteration.nextStep.gap,
        providerId: iteration.nextStep.providerId,
        capability: iteration.nextStep.capability,
        entityId: iteration.nextStep.entityId,
      });
    }
  }

  const resolvedGaps = investigationResult.investigationStatus?.completedSteps?.map((s) => s.gap).filter(Boolean) || [];
  const remainingGaps = investigationResult.missingEvidence?.missing || [];

  return buildInvestigationMemory({
    tenantId,
    entityKey: marketEntityKey(geography, segment),
    marketKey: marketEntityKey(geography, segment),
    geography,
    segment,
    attemptedSteps,
    resolvedGaps,
    remainingGaps,
    sourceChain: attemptedSteps.map((s) => s.providerId || s.capability).filter(Boolean),
    investigationPlan: investigationResult.investigationPlan || null,
    providerLearning: investigationResult.providerLearning?.effectiveness || null,
    providerConflictLearning: investigationResult.conflictResolution?.providerConflictLearning || null,
    conflictHistory: (investigationResult.conflictResolution?.conflicts || []).map((c) => ({
      subject: c.subject,
      resolved: c.resolution?.resolved,
      strategy: c.resolution?.strategy,
      workingEstimate: c.resolution?.workingEstimate,
    })),
    overallConfidence: investigationResult.overallConfidence || 0,
    verifiedAt: context.completedAt || new Date().toISOString(),
    sourceCount: attemptedSteps.length || 1,
    missionId: context.missionId,
  });
}

/**
 * Extract all durable knowledge from a completed investigation.
 * @param {object} investigationResult — output of runInvestigationEngine
 * @param {object} context — { tenantId, missionId, mission, completedAt }
 * @returns {object}
 */
function extractKnowledgeFromInvestigation(investigationResult, context = {}) {
  const market = extractMarketMemory(investigationResult, context);
  const companies = extractCompanyMemories(investigationResult, context);
  const people = extractPersonMemories(investigationResult, context);
  const claims = extractClaimMemories(investigationResult, context);
  const investigation = extractInvestigationMemory(investigationResult, context);

  return {
    market,
    companies,
    people,
    claims,
    investigation,
    extractedAt: context.completedAt || new Date().toISOString(),
    counts: {
      companies: companies.length,
      people: people.length,
      claims: claims.length,
    },
  };
}

module.exports = {
  extractKnowledgeFromInvestigation,
  extractMarketMemory,
  extractCompanyMemories,
  extractPersonMemories,
  extractClaimMemories,
  extractInvestigationMemory,
  extractSourcesFromClaim,
};
