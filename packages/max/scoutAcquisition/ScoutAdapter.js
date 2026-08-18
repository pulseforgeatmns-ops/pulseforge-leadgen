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
const { loadRepository } = require('./ExistingIntelligence');
const {
  incrementReason,
  buildInvestigation,
  uniqueLocations,
} = require('./InvestigationProvenance');
const { buildAcquisitionSearchDefinition, expansionSuggestion } = require('./SearchDefinition');
const { constructCandidateUniverse } = require('./CandidateUniverse');
const { attachFitToClassified, enrichPeopleSafe } = require('./FitEvaluation');
const { defaultDiscoveryAdapters } = require('./DiscoveryAdapters');
const { OPPORTUNITY_CLASSES } = require('./Types');

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

function summarizeOpportunities(opportunities, criteria, investigation, extras = {}) {
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
  const discovered =
    investigation && investigation.coverage
      ? investigation.coverage.candidatesDiscovered
      : extras.discovered;
  const basicFit =
    investigation && investigation.coverage
      ? investigation.coverage.basicFitCount
      : extras.basicFitCount || 0;
  const signalBearing =
    investigation && investigation.coverage
      ? investigation.coverage.signalBearingCount
      : extras.signalBearingCount || 0;
  const fitCandidates = extras.fitCandidates || [];

  if (!opportunities.length) {
    const fitLine =
      basicFit > 0
        ? ` ${basicFit} compan${basicFit === 1 ? 'y meets' : 'ies meet'} the target profile. Current vendor timing is unknown.`
        : '';
    const funnelLine =
      discovered != null && evaluated != null
        ? ` I discovered ${discovered} companies within the requested market. ${evaluated} had enough information for evaluation.`
        : '';
    return {
      summary: `No sufficiently supported opportunities found under the current criteria${
        criteria.geography ? ` (${criteria.geography}` : ''
      }${criteria.segments && criteria.segments.length ? `; ${criteria.segments.join(', ')})` : criteria.geography ? ')' : ''}.${fitLine}`,
      observations: [
        {
          kind: 'observation',
          text:
            basicFit > 0
              ? `No strongly timed opportunities found, but ${basicFit} companies meet the target profile. Current vendor timing is unknown.`
              : 'Current criteria produced no sufficiently supported opportunities. Expanding geography or segment may produce additional results.',
        },
        funnelLine
          ? {
              kind: 'observation',
              text: funnelLine.trim(),
            }
          : null,
      ].filter(Boolean),
      uncertainties: [
        evaluated != null
          ? `Zero supported opportunities after evaluating ${evaluated} candidate${evaluated === 1 ? '' : 's'} — criteria were not weakened to manufacture prospects.`
          : 'Zero results are intelligence — criteria were not weakened to manufacture prospects.',
      ],
      recommendedNextAction: {
        type: 'review',
        text:
          basicFit > 0 && basicFit < 8 && extras.expansionText
            ? extras.expansionText
            : 'Decide whether to broaden geography or segment. Scout will not do that automatically.',
      },
      confidence: evaluated != null && evaluated >= 12 ? 0.88 : 0.7,
    };
  }

  const parts = [];
  if (discovered != null && evaluated != null) {
    parts.push(
      `I discovered ${discovered} companies within the requested market. ${evaluated} had enough information for evaluation.`
    );
    if (basicFit) {
      parts.push(`${basicFit} meet the current business-fit criteria.`);
    }
    if (signalBearing) {
      parts.push(`${signalBearing} have recent signals worth further attention.`);
    }
    parts.push(
      `${opportunities.length} qualif${opportunities.length === 1 ? 'ies' : 'y'} as sufficiently supported near-term opportunit${opportunities.length === 1 ? 'y' : 'ies'}.`
    );
  } else {
    parts.push(`${opportunities.length} compan${opportunities.length === 1 ? 'y' : 'ies'} match the current acquisition objective.`);
  }
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
function buildConsumedContext(searchDefinition, extras = {}) {
  if (!searchDefinition) {
    return {
      geography: null,
      geographyResolved: false,
      segments: [],
      businessNeed: null,
      valid: false,
      invalidReason: extras.invalidReason || null,
      interpreted: null,
    };
  }
  const geo = searchDefinition.geography;
  return {
    geography: geo && geo.label ? geo.label : null,
    geographyResolved: Boolean(geo && geo.label),
    segments: Array.isArray(searchDefinition.segments) ? searchDefinition.segments.slice() : [],
    businessNeed: searchDefinition.businessNeed || null,
    valid: searchDefinition.valid === true,
    invalidReason: searchDefinition.invalidReason || extras.invalidReason || null,
    interpreted: geo
      ? {
          cities: geo.cities || [],
          state: geo.state || null,
        }
      : null,
  };
}

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
    candidatesResolved: input.candidatesResolved || 0,
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

function resolveAim(opts = {}, delegation = {}) {
  if (opts.aim) return opts.aim;
  const ctx = (delegation && delegation.businessContext) || {};
  if (ctx.aim) return ctx.aim;
  const key = asText(opts.aimClientKey || ctx.aimClientKey || ctx.clientKey);
  if (!key) return null;
  const store = opts.aimStore;
  if (store && typeof store.getAim === 'function') {
    return store.getAim(key);
  }
  return null;
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
        consumedContext: buildConsumedContext(null, {
          invalidReason: 'Discovery provider failed before opportunities could be confirmed.',
        }),
      },
    };
  }

  const searchDefinition = buildAcquisitionSearchDefinition({
    delegation,
    tenantId,
    authorizedTenantId: tenantId,
    targetContext: delegation.targetContext,
    businessContext: delegation.businessContext,
    operatorDirection:
      delegation.businessContext && delegation.businessContext.operatorDirection,
    aim: resolveAim(opts, delegation),
  });
  if (!searchDefinition.valid) {
    const packed = emptyInvestigationResult({
      delegation,
      startedAt,
      sourceTypesUnavailable,
      extraLimitations: [searchDefinition.invalidReason],
    });
    return {
      status: 'blocked',
      summary: searchDefinition.invalidReason,
      observations: [],
      actionsTaken: [{ text: 'Attempted to resolve the acquisition search definition.' }],
      evidenceRefs: [],
      artifactRefs: [],
      confidence: null,
      uncertainties: [searchDefinition.invalidReason],
      recommendedNextAction: { type: 'review', text: 'Supply a resolvable geography and target population.' },
      policyEvents: [],
      errors: [{ code: 'invalid_target', message: searchDefinition.invalidReason }],
      startedAt,
      completedAt: packed.completedAt,
      payload: {
        opportunities: [],
        fitCandidates: [],
        watchCandidates: [],
        searchDefinition,
        broadened: false,
        outboundInvoked: [],
        investigation: packed.investigation,
        coverageConfidence: packed.investigation.coverageConfidence,
        consumedContext: buildConsumedContext(searchDefinition),
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
      loadCompanies: opts.loadCompanies || opts.defaultLoadCompanies,
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
        searchDefinition,
        broadened: false,
        outboundInvoked: [],
        investigation: packed.investigation,
        coverageConfidence: packed.investigation.coverageConfidence,
        consumedContext: buildConsumedContext(searchDefinition),
      },
    };
  }

  const adapters = Array.isArray(opts.discoveryAdapters)
    ? opts.discoveryAdapters
    : defaultDiscoveryAdapters({
        discover: opts.discover,
        enablePlaces: opts.enablePlaces,
        placesProvider: opts.placesProvider,
        apiKey: opts.apiKey,
        fetchImpl: opts.fetchImpl,
        companies: opts.companies,
      });

  let universe;
  try {
    universe = await constructCandidateUniverse({
      searchDefinition,
      existing,
      companies: opts.companies,
      people: opts.people,
      adapters,
      adapterOpts: {
        discover: opts.discover,
        enablePlaces: opts.enablePlaces,
        placesProvider: opts.placesProvider,
      },
      discoveryStore: opts.discoveryStore,
      persistCompanies: opts.persistCompanies,
      now: opts.now,
      freshnessMs: opts.freshnessMs,
    });
  } catch (err) {
    const packed = emptyInvestigationResult({
      delegation,
      startedAt,
      sourceTypesChecked,
      sourceTypesUnavailable: [SOURCE_TYPES.PUBLIC_BUSINESS_DATA],
      providerFailed: true,
      extraLimitations: ['Discovery provider failed. No opportunities were fabricated.'],
    });
    return {
      status: 'blocked',
      summary: 'Discovery provider failed.',
      observations: [],
      actionsTaken: [{ text: 'Retrieved existing tenant-scoped records before discovery failed.' }],
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
        searchDefinition,
        broadened: false,
        outboundInvoked: [],
        criteria: existing.criteria,
        investigation: packed.investigation,
        coverageConfidence: packed.investigation.coverageConfidence,
        consumedContext: buildConsumedContext(searchDefinition),
      },
    };
  }

  const criteria = existing.criteria;
  const companies = universe.companies.slice();
  const discoveredCount = universe.candidatesDiscovered;
  for (const src of universe.sourceTypesChecked || []) {
    if (!sourceTypesChecked.includes(src)) sourceTypesChecked.push(src);
  }
  for (const src of universe.sourceTypesUnavailable || []) {
    if (!sourceTypesUnavailable.includes(src)) sourceTypesUnavailable.push(src);
  }
  for (const row of universe.rejectedFromRetrieve || []) {
    incrementReason(rejectionReasonCounts, row.reason);
  }
  const actionsTaken = universe.actionsTaken.slice();
  let discoveryFailed = universe.discoveryFailedCompletely === true;
  if ((universe.discoveryErrors || []).some((e) => e.code === 'provider_error') && !companies.length) {
    discoveryFailed = true;
    const packed = emptyInvestigationResult({
      delegation,
      criteria,
      startedAt,
      candidatesDiscovered: discoveredCount,
      candidatesResolved: universe.candidatesResolved,
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
      errors: [{ code: 'provider_error', message: (universe.discoveryErrors[0] && universe.discoveryErrors[0].message) || 'Discovery provider failed.' }],
      startedAt,
      completedAt: packed.completedAt,
      payload: {
        opportunities: [],
        searchDefinition,
        broadened: false,
        outboundInvoked: [],
        criteria,
        investigation: packed.investigation,
        coverageConfidence: packed.investigation.coverageConfidence,
        consumedContext: buildConsumedContext(searchDefinition),
      },
    };
  }

  const classified = [];
  for (const company of companies) {
    const peopleResult = await enrichPeopleSafe(company, opts.enrichPeople);
    if (peopleResult.failed) {
      company.people = peopleResult.people;
    } else if (peopleResult.people.length) {
      company.people = peopleResult.people;
    }
    const row = classifySignals(company);
    if (peopleResult.failed) {
      row.unknowns.push(
        normalizeClaim(
          {
            kind: 'unknown',
            text: `Decision-maker unknown for ${company.name}. Person enrichment failed; the company remains valid.`,
            entityId: company.id,
          },
          'unknown'
        )
      );
    }
    classified.push(row);
  }

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
  const fitCandidates = [];
  const watchCandidates = [];
  const nearThreshold = [];
  let basicFitCount = 0;
  let signalBearingCount = 0;
  let timelyEvidenceCount = 0;

  classified.forEach((row, index) => {
    const company = companies[index];
    const attached = attachFitToClassified(row, company, searchDefinition, now);
    const next = attached.classified;
    classified[index] = next;
    company.lastEvaluatedAt = attached.lastEvaluatedAt;
    company.evidenceObservedAt = attached.evidenceObservedAt;
    company.fitEvaluation = attached.fit;
    const qualification = attached.qualification;
    if (qualification.basicFit || attached.fit.basicFit) basicFitCount += 1;
    if (qualification.signalBearing) signalBearingCount += 1;
    if ((next.signals || []).some((s) => isTimely(s.observedAt, now))) timelyEvidenceCount += 1;
    if (next.classification === OPPORTUNITY_CLASSES.SUPPORTED || qualification.supported) {
      next.classification = OPPORTUNITY_CLASSES.SUPPORTED;
      supported.push(next);
      return;
    }
    incrementReason(rejectionReasonCounts, qualification.reason);
    if (next.classification === OPPORTUNITY_CLASSES.FIT) {
      fitCandidates.push(next);
    } else if (next.classification === OPPORTUNITY_CLASSES.WATCH) {
      watchCandidates.push(next);
    }
    if (qualification.nearThreshold || next.classification === OPPORTUNITY_CLASSES.FIT) {
      nearThreshold.push({
        company,
        basicFit: qualification.basicFit || attached.fit.basicFit,
        fitStrong: attached.fit.level === 'strong' || Number(next.fit || 0) >= 0.7,
        signal: qualification.signal,
        rejectedBecause: qualification.rejectedBecause,
        reason: qualification.reason,
      });
    }
  });

  if (opts.discoveryStore && typeof opts.discoveryStore.upsert === 'function' && companies.length) {
    await opts.discoveryStore.upsert(tenantId, companies);
  }

  const requested = requestedScopeFrom(delegation, criteria);
  const completedAt = nowIso();
  const zeroEvaluated = classified.length === 0;
  const coverageInsufficient = zeroEvaluated && searchDefinition.valid;
  const extraLimitations = [];
  if (coverageInsufficient) {
    extraLimitations.push(
      universe.discoveryRan
        ? 'Current discovery sources produced no candidate universe; investigation coverage insufficient.'
        : 'Candidate discovery provider unavailable.'
    );
  }

  const investigation = buildInvestigation({
    requestedGeography: requested.geography,
    requestedSegments: requested.segments,
    desiredSignals: requested.desiredSignals,
    targetCriteria: {
      ...requested.targetCriteria,
      populationStatement: searchDefinition.populationStatement,
    },
    evaluatedCompanies: companies,
    investigatedGeographyList: uniqueLocations(companies),
    candidatesDiscovered: discoveredCount,
    candidatesResolved: universe.candidatesResolved,
    candidatesEvaluated: classified.length,
    basicFitCount,
    signalBearingCount,
    supportedOpportunityCount: supported.length,
    unresolvedCount,
    sourceTypesChecked,
    sourceTypesUnavailable,
    enrichmentAttempted: enrichmentFailed || typeof opts.enrichPeople === 'function',
    enrichmentFailureRate: enrichmentFailed && classified.length ? 1 : 0,
    timelyEvidenceCount,
    providerFailed: discoveryFailed,
    rejectionReasonCounts,
    nearThreshold,
    startedAt,
    completedAt,
    observedAtValues: collectObservedAt(classified),
    extraLimitations,
  });

  const rollup = summarizeOpportunities(supported, criteria, investigation, {
    fitCandidates,
    basicFitCount,
    signalBearingCount,
    discovered: discoveredCount,
    expansionText: expansionSuggestion(searchDefinition, basicFitCount),
  });
  const evidenceRefs = [];
  const seen = new Set();
  for (const opp of [...supported, ...fitCandidates]) {
    for (const ev of opp.evidenceRefs) {
      if (!ev.id || seen.has(ev.id)) continue;
      seen.add(ev.id);
      evidenceRefs.push(ev);
    }
  }

  const status = coverageInsufficient
    ? universe.discoveryRan
      ? 'partial'
      : 'blocked'
    : mode === 'partial' || enrichmentFailed
      ? 'partial'
      : 'completed';

  return {
    status,
    summary: coverageInsufficient
      ? extraLimitations[0]
      : rollup.summary,
    observations: [
      ...rollup.observations,
      ...supported.flatMap((o) => o.observations),
      ...fitCandidates.map((o) => ({
        kind: 'observation',
        text: `${o.name} is a fit candidate. Timing unknown.`,
      })),
    ],
    actionsTaken,
    evidenceRefs,
    artifactRefs: supported.map(toArtifact),
    confidence: coverageInsufficient ? 0.35 : rollup.confidence,
    uncertainties: [
      ...rollup.uncertainties,
      ...classified.flatMap((o) => o.unknowns.map((u) => u.text)),
      coverageInsufficient
        ? 'Zero candidates evaluated is a discovery limitation, not a market-negative conclusion.'
        : null,
    ].filter(Boolean),
    recommendedNextAction: coverageInsufficient
      ? { type: 'retry', text: extraLimitations[0] }
      : rollup.recommendedNextAction,
    policyEvents: [],
    errors: [
      ...(enrichmentFailed
        ? [{ code: 'enrichment_unavailable', message: 'Enrichment provider unavailable.' }]
        : []),
      ...(coverageInsufficient
        ? [{ code: 'insufficient_coverage', message: extraLimitations[0] }]
        : []),
    ],
    startedAt,
    completedAt,
    payload: {
      opportunities: supported,
      fitCandidates,
      watchCandidates,
      searchDefinition,
      evaluatedCandidates: classified.map((row) => ({
        companyId: row.companyId,
        name: row.name,
        fit: row.fit,
        fitLevel: row.fitLevel,
        fitReasons: row.fitReasons,
        timing: row.timing,
        intent: row.intent,
        classification: row.classification,
      })),
      criteria,
      retrievedBeforeInvestigate: true,
      broadened: false,
      outboundInvoked: [],
      specialist: SCOUT_SPECIALIST,
      capability: SCOUT_CAPABILITY,
      investigation,
      coverageConfidence: investigation.coverageConfidence,
      consumedContext: buildConsumedContext(searchDefinition),
    },
  };
}

function isScoutAcquisition(specialist, capability) {
  return specialist === SCOUT_SPECIALIST && capability === SCOUT_CAPABILITY;
}

module.exports = {
  runScoutAcquisitionIntelligence,
  isScoutAcquisition,
  resolveAim,
  buildConsumedContext,
  classifySignals,
  summarizeOpportunities,
  assertIntelligenceOnly,
  clone,
};
