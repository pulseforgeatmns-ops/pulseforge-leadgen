'use strict';

/**
 * SPEC-100 — Scout acquisition_intelligence adapter.
 * Intelligence only. Reuses existing company/prospect/evidence records.
 * Never broadens criteria. Never invokes outbound capabilities.
 */

const {
  asText,
  clone,
  nowIso,
  normalizeSignal,
  normalizeClaim,
  isTimely,
  SCOUT_SPECIALIST,
  SCOUT_CAPABILITY,
  FORBIDDEN_OUTBOUND,
  SOURCE_TYPES,
  REJECTION_REASONS,
} = require('./Types');
const { retrieveExistingIntelligence, loadRepository } = require('./ExistingIntelligence');
const {
  qualifyCandidate,
  incrementReason,
  rejectionSummaryFromMap,
  buildInvestigation,
  uniqueLocations,
} = require('./InvestigationProvenance');

const OUTBOUND_RE = new RegExp(
  `\\b(${FORBIDDEN_OUTBOUND.join('|')}|send|enroll|publish|twilio|brevo|bland)\\b`,
  'i'
);

function assertIntelligenceOnly(delegation) {
  const authority = asText(delegation && delegation.authority);
  if (authority && !['observe', 'recommend'].includes(authority)) {
    const err = new Error(
      `Scout acquisition_intelligence rejects authority "${authority}".`
    );
    err.code = 'unsupported_authority';
    throw err;
  }
  const blob = JSON.stringify(delegation || {});
  if (OUTBOUND_RE.test(blob) && /\b(execute|send|enroll)\b/i.test(blob)) {
    const err = new Error('Scout acquisition intelligence cannot execute outbound work.');
    err.code = 'platform_safety_conflict';
    throw err;
  }
}

function buildEvidence(company, signal, index) {
  const observedAt =
    (signal && (signal.observedAt || signal.observed_at)) ||
    company.updatedAt ||
    null;
  const label =
    (signal && (signal.label || signal.text)) ||
    `${company.name} — ${signal && (signal.type || 'company record')}`;
  return {
    id: asText((signal && signal.evidenceId) || `ev-scout-${company.id}-${index}`),
    kind: 'company',
    sourceKind: 'observed_fact',
    label,
    snapshot: {
      companyId: company.id,
      companyName: company.name,
      observedAt,
      source: (signal && signal.source) || 'existing_repository',
      evidenceType: (signal && (signal.type || signal.kind)) || 'company_record',
    },
  };
}

function classifySignals(company) {
  const observations = [];
  const inferences = [];
  const unknowns = [];
  const signals = [];
  const evidenceRefs = [];

  const rawSignals = Array.isArray(company.signals) ? company.signals : [];
  rawSignals.forEach((raw, i) => {
    const type = normalizeSignal(raw.type || raw.kind || raw.signal);
    if (!type) return;
    signals.push({
      type,
      observedAt: raw.observedAt || raw.observed_at || company.updatedAt || null,
      source: raw.source || 'existing_repository',
      label: raw.label || raw.text || null,
    });
    const ev = buildEvidence(company, { ...raw, type }, i);
    evidenceRefs.push(ev);
    observations.push(
      normalizeClaim(
        {
          kind: 'observation',
          text: raw.observation || raw.label || raw.text || `${company.name} shows ${type.replace(/_/g, ' ')}.`,
          entityId: company.id,
          observedAt: ev.snapshot.observedAt,
          evidenceId: ev.id,
        },
        'observation'
      )
    );
    if (raw.inference) {
      inferences.push(
        normalizeClaim(
          {
            kind: 'inference',
            text: raw.inference,
            entityId: company.id,
            evidenceId: ev.id,
          },
          'inference'
        )
      );
    }
  });

  const people = Array.isArray(company.people) ? company.people : [];
  const decisionMakers = people.filter(
    (p) =>
      p.decisionMaker === true ||
      /\b(owner|principal|partner|operations|office manager|director|president|founder)\b/i.test(
        String(p.jobTitle || '')
      )
  );
  if (decisionMakers.length) {
    signals.push({
      type: 'decision_maker',
      observedAt: decisionMakers[0].observedAt || company.updatedAt || null,
      source: 'existing_repository',
      label: decisionMakers.map((p) => p.name).join(', '),
    });
    observations.push(
      normalizeClaim(
        {
          kind: 'observation',
          text: `${company.name} has identifiable decision-maker${decisionMakers.length === 1 ? '' : 's'}: ${decisionMakers
            .map((p) => `${p.name}${p.jobTitle ? ` (${p.jobTitle})` : ''}`)
            .join(', ')}.`,
          entityId: company.id,
          observedAt: decisionMakers[0].observedAt || company.updatedAt || null,
        },
        'observation'
      )
    );
  } else {
    unknowns.push(
      normalizeClaim(
        {
          kind: 'unknown',
          text: `No identifiable operations decision-maker for ${company.name}.`,
          entityId: company.id,
        },
        'unknown'
      )
    );
  }

  const hasVendor = rawSignals.some((s) =>
    ['vendor_dissatisfaction', 'contract_timing'].includes(
      normalizeSignal(s.type || s.kind || s.signal)
    )
  );
  if (!hasVendor) {
    unknowns.push(
      normalizeClaim(
        {
          kind: 'unknown',
          text: `No evidence currently shows dissatisfaction with ${company.name}'s existing cleaning provider, and contract timing is unknown.`,
          entityId: company.id,
        },
        'unknown'
      )
    );
  }

  if (signals.some((s) => s.type === 'portfolio_growth' || s.type === 'expansion')) {
    inferences.push(
      normalizeClaim(
        {
          kind: 'inference',
          text: `Portfolio growth at ${company.name} may increase cleaning-vendor demand.`,
          entityId: company.id,
        },
        'inference'
      )
    );
  }

  if (!observations.length && company.website) {
    observations.push(
      normalizeClaim(
        {
          kind: 'observation',
          text: `${company.name} is a matching company in the current target geography with a public website.`,
          entityId: company.id,
          observedAt: company.updatedAt || null,
        },
        'observation'
      )
    );
    evidenceRefs.push(
      buildEvidence(
        company,
        { type: 'company_record', label: `${company.name} website ${company.website}` },
        'site'
      )
    );
  }

  const timely = signals.filter((s) => isTimely(s.observedAt));
  const fit = company.icpScore != null ? Number(company.icpScore) / 100 : observations.length ? 0.62 : 0.4;
  const timing = timely.length ? 0.78 : signals.length ? 0.35 : 0.2;
  const confidence = Math.max(
    0.2,
    Math.min(0.92, 0.35 + observations.length * 0.12 + timely.length * 0.1)
  );

  return {
    companyId: company.id,
    personIds: people.map((p) => p.id).filter(Boolean),
    fit: Number(fit.toFixed(2)),
    timing: Number(timing.toFixed(2)),
    signals,
    observations: observations.filter(Boolean),
    inferences: inferences.filter(Boolean),
    unknowns: unknowns.filter(Boolean),
    evidenceRefs,
    confidence: Number(confidence.toFixed(2)),
    name: company.name,
  };
}

function summarizeOpportunities(opportunities, criteria, investigation) {
  const timelyGrowth = opportunities.filter((o) =>
    o.signals.some(
      (s) =>
        (s.type === 'portfolio_growth' || s.type === 'expansion' || s.type === 'new_location') &&
        isTimely(s.observedAt)
    )
  );
  const withDm = opportunities.filter((o) =>
    o.signals.some((s) => s.type === 'decision_maker')
  );
  const hiring = opportunities.filter((o) =>
    o.signals.some((s) => s.type === 'hiring' && isTimely(s.observedAt))
  );
  const evaluated =
    investigation && investigation.coverage
      ? investigation.coverage.candidatesEvaluated
      : null;

  if (!opportunities.length) {
    return {
      summary: `No sufficiently supported opportunities found under the current criteria${
        criteria.geography ? ` (${criteria.geography}` : ''
      }${criteria.segments && criteria.segments.length ? `; ${criteria.segments.join(', ')})` : criteria.geography ? ')' : ''}.`,
      observations: [
        {
          kind: 'observation',
          text: 'Current criteria produced no sufficiently supported opportunities. Expanding geography or segment may produce additional results.',
        },
      ],
      uncertainties: [
        evaluated != null
          ? `Zero supported opportunities after evaluating ${evaluated} candidate${evaluated === 1 ? '' : 's'} — criteria were not weakened to manufacture prospects.`
          : 'Zero results are intelligence — criteria were not weakened to manufacture prospects.',
      ],
      recommendedNextAction: {
        type: 'review',
        text: 'Decide whether to broaden geography or segment. Scout will not do that automatically.',
      },
      confidence: evaluated != null && evaluated >= 12 ? 0.88 : 0.7,
    };
  }

  const parts = [`${opportunities.length} compan${opportunities.length === 1 ? 'y' : 'ies'} match the current acquisition objective.`];
  if (timelyGrowth.length) {
    parts.push(`${timelyGrowth.length} show recent portfolio-growth or expansion evidence.`);
  }
  if (withDm.length) {
    parts.push(`${withDm.length} have identifiable operations decision-makers.`);
  }
  if (hiring.length) {
    parts.push(`${hiring.length} show recent hiring related to operations.`);
  }

  return {
    summary: parts.join(' '),
    observations: [
      {
        kind: 'observation',
        text: `${opportunities.length} matching companies were retrieved under the delegated criteria.`,
      },
      timelyGrowth.length
        ? {
            kind: 'observation',
            text: `${timelyGrowth.length} show recent portfolio-growth evidence.`,
          }
        : null,
      withDm.length
        ? {
            kind: 'observation',
            text: `${withDm.length} have identifiable operations decision-makers.`,
          }
        : null,
    ].filter(Boolean),
    uncertainties: [
      opportunities.some((o) => o.unknowns.length)
        ? 'No direct vendor dissatisfaction evidence found for most companies. Contract timing remains unknown.'
        : null,
    ].filter(Boolean),
    recommendedNextAction: {
      type: 'review',
      text: timelyGrowth.length
        ? `Review the ${timelyGrowth.length} compan${timelyGrowth.length === 1 ? 'y' : 'ies'} with recent portfolio-growth signals before expanding discovery.`
        : 'Review the matching companies before deciding whether to expand discovery.',
    },
    confidence: Number(
      Math.min(
        0.9,
        0.55 + opportunities.length * 0.03 + timelyGrowth.length * 0.04
      ).toFixed(2)
    ),
  };
}

function toArtifact(opportunity) {
  return {
    kind: 'acquisition_opportunity',
    id: `opp-${opportunity.companyId}`,
    companyId: opportunity.companyId,
    personIds: opportunity.personIds,
    name: opportunity.name,
    fit: opportunity.fit,
    timing: opportunity.timing,
    signals: opportunity.signals,
    observations: opportunity.observations,
    inferences: opportunity.inferences,
    unknowns: opportunity.unknowns,
    evidenceRefs: opportunity.evidenceRefs,
    confidence: opportunity.confidence,
  };
}

/**
 * @param {object} delegation
 * @param {object} [opts]
 * @returns {Promise<object>}
 */
function requestedScopeFrom(delegation, criteria) {
  const target = (delegation && delegation.targetContext) || {};
  const business = (delegation && delegation.businessContext) || {};
  return {
    geography:
      asText(target.geography) ||
      asText(business.serviceGeography) ||
      (criteria && criteria.geography) ||
      null,
    segments: Array.isArray(target.segments)
      ? target.segments
      : (criteria && criteria.segments) || [],
    desiredSignals: Array.isArray(target.desiredSignals) ? target.desiredSignals : [],
    targetCriteria: {
      geography:
        asText(target.geography) ||
        asText(business.serviceGeography) ||
        (criteria && criteria.geography) ||
        null,
      segments: Array.isArray(target.segments)
        ? target.segments.slice()
        : ((criteria && criteria.segments) || []).slice(),
      businessType: asText(target.businessType) || asText(business.commercialCapability),
    },
  };
}

function collectObservedAt(classifiedList) {
  const values = [];
  for (const row of classifiedList || []) {
    for (const signal of row.signals || []) {
      if (signal.observedAt) values.push(signal.observedAt);
    }
    for (const ev of row.evidenceRefs || []) {
      const observedAt = ev.snapshot && ev.snapshot.observedAt;
      if (observedAt) values.push(observedAt);
    }
  }
  return values;
}

function emptyInvestigationResult(input) {
  const completedAt = nowIso();
  const requested = requestedScopeFrom(input.delegation, input.criteria);
  const investigation = buildInvestigation({
    requestedGeography: requested.geography,
    requestedSegments: requested.segments,
    desiredSignals: requested.desiredSignals,
    targetCriteria: requested.targetCriteria,
    evaluatedCompanies: input.evaluatedCompanies || [],
    investigatedGeographyList: uniqueLocations(input.evaluatedCompanies || []),
    candidatesDiscovered: input.candidatesDiscovered || 0,
    candidatesEvaluated: input.candidatesEvaluated || 0,
    basicFitCount: 0,
    signalBearingCount: 0,
    supportedOpportunityCount: 0,
    unresolvedCount: input.unresolvedCount || 0,
    sourceTypesChecked: input.sourceTypesChecked || [],
    sourceTypesUnavailable: input.sourceTypesUnavailable || [],
    enrichmentAttempted: input.enrichmentAttempted === true,
    enrichmentFailureRate: input.enrichmentFailureRate || 0,
    timelyEvidenceCount: 0,
    providerFailed: input.providerFailed === true,
    rejectionReasonCounts: input.rejectionReasonCounts || {},
    startedAt: input.startedAt,
    completedAt,
    extraLimitations: input.extraLimitations,
  });
  return {
    investigation,
    completedAt,
  };
}

async function runScoutAcquisitionIntelligence(delegation, opts = {}) {
  assertIntelligenceOnly(delegation);
  const startedAt = nowIso();
  const tenantId = asText(delegation.tenantId);
  const mode = asText(opts.mode || delegation._fixtureMode) || 'completed';
  const sourceTypesChecked = [];
  const sourceTypesUnavailable = [];
  const rejectionReasonCounts = {};

  if (mode === 'provider_failure') {
    sourceTypesUnavailable.push(SOURCE_TYPES.PUBLIC_BUSINESS_DATA);
    const packed = emptyInvestigationResult({
      delegation,
      startedAt,
      candidatesDiscovered: 0,
      sourceTypesUnavailable,
      providerFailed: true,
      extraLimitations: ['Discovery provider failed before opportunities could be confirmed.'],
    });
    return {
      status: 'blocked',
      summary: 'Discovery provider failed before opportunities could be confirmed.',
      observations: [],
      actionsTaken: [{ text: 'Attempted tenant-scoped acquisition investigation.' }],
      evidenceRefs: [],
      artifactRefs: [],
      confidence: null,
      uncertainties: ['Discovery provider failed. No opportunities were fabricated.'],
      recommendedNextAction: { type: 'retry', text: 'Retry Scout after the provider recovers.' },
      policyEvents: [],
      errors: [{ code: 'provider_error', message: 'Discovery provider failed.' }],
      startedAt,
      completedAt: packed.completedAt,
      payload: {
        opportunities: [],
        broadened: false,
        outboundInvoked: [],
        investigation: packed.investigation,
        coverageConfidence: packed.investigation.coverageConfidence,
      },
    };
  }

  let existing;
  try {
    existing = await loadRepository({
      authorizedTenantId: tenantId,
      tenantId,
      targetContext: delegation.targetContext,
      businessContext: delegation.businessContext,
      companies: opts.companies,
      people: opts.people,
      loadCompanies: opts.loadCompanies,
    });
    sourceTypesChecked.push(SOURCE_TYPES.EXISTING_PF);
  } catch (err) {
    sourceTypesUnavailable.push(SOURCE_TYPES.EXISTING_PF);
    const packed = emptyInvestigationResult({
      delegation,
      startedAt,
      sourceTypesUnavailable,
      providerFailed: true,
      extraLimitations: [err.message || 'Repository unavailable.'],
    });
    return {
      status: 'blocked',
      summary: 'Could not retrieve existing acquisition intelligence.',
      observations: [],
      actionsTaken: [{ text: 'Attempted to retrieve existing tenant-scoped records.' }],
      evidenceRefs: [],
      artifactRefs: [],
      confidence: null,
      uncertainties: [err.message || 'Repository unavailable.'],
      recommendedNextAction: { type: 'retry', text: 'Retry retrieval.' },
      policyEvents: [],
      errors: [{ code: 'repository_error', message: err.message || String(err) }],
      startedAt,
      completedAt: packed.completedAt,
      payload: {
        opportunities: [],
        broadened: false,
        outboundInvoked: [],
        investigation: packed.investigation,
        coverageConfidence: packed.investigation.coverageConfidence,
      },
    };
  }

  const criteria = existing.criteria;
  let companies = existing.companies.slice();
  let discoveredCount = (existing.discoveredCompanies || existing.companies || []).length;
  for (const row of existing.rejectedCandidates || []) {
    incrementReason(rejectionReasonCounts, row.reason);
  }
  const actionsTaken = [
    { text: 'Retrieved existing tenant-scoped company and prospect intelligence.' },
  ];
  let discoveryFailed = false;

  if (!companies.length && typeof opts.discover === 'function') {
    try {
      const discovered = await opts.discover({
        tenantId,
        targetContext: delegation.targetContext,
        businessContext: delegation.businessContext,
        criteria,
      });
      const discoveredCompanies = Array.isArray(discovered)
        ? discovered
        : (discovered && discovered.companies) || [];
      const merged = retrieveExistingIntelligence({
        authorizedTenantId: tenantId,
        tenantId,
        targetContext: delegation.targetContext,
        businessContext: delegation.businessContext,
        companies: discoveredCompanies,
        people: (discovered && discovered.people) || [],
      });
      companies = merged.companies;
      discoveredCount = (merged.discoveredCompanies || discoveredCompanies).length;
      for (const row of merged.rejectedCandidates || []) {
        incrementReason(rejectionReasonCounts, row.reason);
      }
      sourceTypesChecked.push(SOURCE_TYPES.PUBLIC_BUSINESS_DATA);
      actionsTaken.push({
        text: 'Existing intelligence was empty; ran bounded discovery under the delegated criteria.',
      });
    } catch (err) {
      discoveryFailed = true;
      sourceTypesUnavailable.push(SOURCE_TYPES.PUBLIC_BUSINESS_DATA);
      const packed = emptyInvestigationResult({
        delegation,
        criteria,
        startedAt,
        candidatesDiscovered: discoveredCount,
        sourceTypesChecked,
        sourceTypesUnavailable,
        providerFailed: true,
        rejectionReasonCounts,
        extraLimitations: ['Discovery provider failed. No opportunities were fabricated.'],
      });
      return {
        status: 'blocked',
        summary: 'Discovery provider failed.',
        observations: [],
        actionsTaken,
        evidenceRefs: [],
        artifactRefs: [],
        confidence: null,
        uncertainties: ['Discovery provider failed. No opportunities were fabricated.'],
        recommendedNextAction: { type: 'retry', text: 'Retry discovery after the provider recovers.' },
        policyEvents: [],
        errors: [{ code: 'provider_error', message: err.message || String(err) }],
        startedAt,
        completedAt: packed.completedAt,
        payload: {
          opportunities: [],
          broadened: false,
          outboundInvoked: [],
          criteria,
          investigation: packed.investigation,
          coverageConfidence: packed.investigation.coverageConfidence,
        },
      };
    }
  }

  const classified = companies.map(classifySignals);
  const enrichmentFailed = mode === 'enrichment_failure' || opts.enrichmentFailed === true;
  let unresolvedCount = 0;
  if (enrichmentFailed) {
    unresolvedCount = classified.length;
    sourceTypesUnavailable.push(SOURCE_TYPES.ENRICHMENT_PROVIDER);
    for (const opp of classified) {
      incrementReason(rejectionReasonCounts, REJECTION_REASONS.UNRESOLVED);
      opp.unknowns.push(
        normalizeClaim(
          {
            kind: 'unknown',
            text: `Decision-maker enrichment failed for ${opp.name || opp.companyId}.`,
            entityId: opp.companyId,
          },
          'unknown'
        )
      );
    }
    actionsTaken.push({ text: 'Attempted decision-maker enrichment; provider unavailable.' });
  }

  if (classified.some((row) => row.evidenceRefs.some((ev) => ev.snapshot && ev.snapshot.source === 'company_website') || (companies.find((c) => c.id === row.companyId) || {}).website)) {
    sourceTypesChecked.push(SOURCE_TYPES.COMPANY_WEBSITES);
  }

  const now = opts.now != null ? Number(opts.now) : Date.now();
  const supported = [];
  const nearThreshold = [];
  let basicFitCount = 0;
  let signalBearingCount = 0;
  let timelyEvidenceCount = 0;

  classified.forEach((row, index) => {
    const company = companies[index];
    const qualification = qualifyCandidate(row, company, now);
    if (qualification.basicFit) basicFitCount += 1;
    if (qualification.signalBearing) signalBearingCount += 1;
    if ((row.signals || []).some((s) => isTimely(s.observedAt, now))) timelyEvidenceCount += 1;
    if (qualification.supported) {
      supported.push(row);
      return;
    }
    incrementReason(rejectionReasonCounts, qualification.reason);
    if (qualification.nearThreshold) {
      nearThreshold.push({
        company,
        basicFit: qualification.basicFit,
        fitStrong: Number(row.fit || 0) >= 0.7,
        signal: qualification.signal,
        rejectedBecause: qualification.rejectedBecause,
        reason: qualification.reason,
      });
    }
  });

  const requested = requestedScopeFrom(delegation, criteria);
  const completedAt = nowIso();
  const investigation = buildInvestigation({
    requestedGeography: requested.geography,
    requestedSegments: requested.segments,
    desiredSignals: requested.desiredSignals,
    targetCriteria: requested.targetCriteria,
    evaluatedCompanies: companies,
    investigatedGeographyList: uniqueLocations(companies),
    candidatesDiscovered: discoveredCount,
    candidatesEvaluated: classified.length,
    basicFitCount,
    signalBearingCount,
    supportedOpportunityCount: supported.length,
    unresolvedCount,
    sourceTypesChecked,
    sourceTypesUnavailable,
    enrichmentAttempted: enrichmentFailed,
    enrichmentFailureRate: enrichmentFailed && classified.length ? 1 : 0,
    timelyEvidenceCount,
    providerFailed: discoveryFailed,
    rejectionReasonCounts,
    nearThreshold,
    startedAt,
    completedAt,
    observedAtValues: collectObservedAt(classified),
  });

  const rollup = summarizeOpportunities(supported, criteria, investigation);
  const evidenceRefs = [];
  const seen = new Set();
  for (const opp of supported) {
    for (const ev of opp.evidenceRefs) {
      if (!ev.id || seen.has(ev.id)) continue;
      seen.add(ev.id);
      evidenceRefs.push(ev);
    }
  }

  const status =
    mode === 'partial' || enrichmentFailed
      ? 'partial'
      : 'completed';

  return {
    status,
    summary: rollup.summary,
    observations: [
      ...rollup.observations,
      ...supported.flatMap((o) => o.observations),
    ],
    actionsTaken,
    evidenceRefs,
    artifactRefs: supported.map(toArtifact),
    confidence: rollup.confidence,
    uncertainties: [
      ...rollup.uncertainties,
      ...classified.flatMap((o) => o.unknowns.map((u) => u.text)),
    ].filter(Boolean),
    recommendedNextAction: rollup.recommendedNextAction,
    policyEvents: [],
    errors: enrichmentFailed
      ? [{ code: 'enrichment_unavailable', message: 'Enrichment provider unavailable.' }]
      : [],
    startedAt,
    completedAt,
    payload: {
      opportunities: supported,
      evaluatedCandidates: classified.map((row) => ({
        companyId: row.companyId,
        name: row.name,
        fit: row.fit,
        timing: row.timing,
      })),
      criteria,
      retrievedBeforeInvestigate: true,
      broadened: false,
      outboundInvoked: [],
      specialist: SCOUT_SPECIALIST,
      capability: SCOUT_CAPABILITY,
      investigation,
      coverageConfidence: investigation.coverageConfidence,
    },
  };
}

function isScoutAcquisition(specialist, capability) {
  return specialist === SCOUT_SPECIALIST && capability === SCOUT_CAPABILITY;
}

module.exports = {
  runScoutAcquisitionIntelligence,
  isScoutAcquisition,
  classifySignals,
  summarizeOpportunities,
  assertIntelligenceOnly,
  clone,
};
