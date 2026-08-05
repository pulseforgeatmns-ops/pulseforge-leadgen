'use strict';

/**
 * SPEC-074 — Prospect Operating Brief v1.
 * Read-only synthesis for Jake: what to do next for a live prospect.
 * Combines company/prospect CRM context, committed Relationship Intelligence,
 * and Market Intelligence corpus patterns. isEvidence: false (synthesis).
 * Never invents facts. No outbound, CRM mutation, or autonomous execution.
 */

const defaultPool = require('../db');

const DEFAULT_DAYS = 30;
const MARKET_LIMIT = 5;

const ACTION_TYPES = Object.freeze([
  'send_follow_up',
  'prepare_proposal',
  'schedule_walkthrough',
  'ask_clarifying_question',
  'research_company',
  'wait_for_reply',
  'manual_review',
]);

const READINESS = Object.freeze({
  READY: 'ready',
  PARTIAL: 'partial',
  BLOCKED: 'blocked',
  UNKNOWN: 'unknown',
});

class ProspectOperatingBriefError extends Error {
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'ProspectOperatingBriefError';
    this.code = code;
    this.status = status;
  }
}

function asText(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s || null;
}

function clampDays(days, fallback = DEFAULT_DAYS) {
  const n = Number(days);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(Math.floor(n), 3650);
}

function parseTruthy(value, fallback = true) {
  if (value === undefined || value === null || value === '') return fallback;
  if (value === true || value === 1) return true;
  if (value === false || value === 0) return false;
  const s = String(value).trim().toLowerCase();
  if (s === '1' || s === 'true' || s === 'yes') return true;
  if (s === '0' || s === 'false' || s === 'no') return false;
  return fallback;
}

function uniq(values) {
  const out = [];
  const seen = new Set();
  for (const raw of values || []) {
    if (raw == null) continue;
    const v = String(raw);
    if (seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
}

function insightText(insight) {
  if (!insight) return '';
  const label = String(insight.label || '').trim();
  const value = String(insight.value || '').trim();
  if (label && value && label.toLowerCase() !== value.toLowerCase()) {
    return `${label}: ${value}`;
  }
  return value || label;
}

function mapInsightItem(insight, interactionId) {
  return {
    kind: insight.kind,
    label: insight.label || null,
    value: insight.value || null,
    confidence: insight.confidence == null ? null : Number(insight.confidence),
    sourceQuote: insight.sourceQuote || insight.source_quote || null,
    interactionId: interactionId || null,
  };
}

function defaultRelationshipService() {
  return require('./relationshipIntelligenceInterview');
}

function defaultMarketBriefingService() {
  return require('./marketIntelligenceBriefing');
}

/**
 * Lightweight CRM company/prospect snapshot. No new company-intelligence system.
 */
async function loadCompanySnapshot(options = {}) {
  const pool = options.pool || defaultPool;
  const prospectId = asText(options.prospectId);
  const companyId = asText(options.companyId);
  const contactId = asText(options.contactId);
  const opportunityId = asText(options.opportunityId);
  const clientId =
    options.clientId != null && Number.isFinite(Number(options.clientId))
      ? Number(options.clientId)
      : null;

  if (opportunityId) {
    try {
      const opp = await pool.query(
        `SELECT id, prospect_id, client_id, stage, company_id
         FROM opportunities
         WHERE id::text = $1
         LIMIT 1`,
        [opportunityId]
      );
      if (opp.rows[0] && opp.rows[0].prospect_id != null) {
        return loadCompanySnapshot({
          ...options,
          prospectId: String(opp.rows[0].prospect_id),
          companyId: options.companyId || (opp.rows[0].company_id != null
            ? String(opp.rows[0].company_id)
            : null),
          clientId: clientId != null ? clientId : opp.rows[0].client_id,
          opportunityId: null,
          _resolvedOpportunityId: opportunityId,
          _opportunityStage: opp.rows[0].stage || null,
        });
      }
    } catch (_) {
      // opportunities table may be absent — fall through
    }
  }

  const targetProspectId = prospectId || contactId;
  if (targetProspectId) {
    const params = [targetProspectId];
    let sql = `
      SELECT p.id AS prospect_id, p.client_id, p.company_id,
        p.first_name, p.last_name, p.email, p.phone, p.job_title,
        p.vertical, p.status, p.setter_status, p.source, p.notes,
        p.icp_score, p.do_not_contact, p.website AS prospect_website,
        c.name AS company_name, c.location AS company_location,
        c.website AS company_website, c.industry AS company_industry
      FROM prospects p
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.id::text = $1`;
    if (clientId != null) {
      params.push(clientId);
      sql += ` AND p.client_id = $2`;
    }
    sql += ' LIMIT 1';
    try {
      const result = await pool.query(sql, params);
      if (result.rows[0]) {
        return snapshotFromProspectRow(result.rows[0], {
          opportunityId: options._resolvedOpportunityId || opportunityId,
          opportunityStage: options._opportunityStage || null,
        });
      }
    } catch (err) {
      return {
        found: false,
        error: err && err.message ? String(err.message) : 'prospect_load_failed',
        companyId: companyId,
        prospectId: targetProspectId,
        contactId: contactId,
        opportunityId: opportunityId,
      };
    }
  }

  if (companyId) {
    const params = [companyId];
    let sql = `
      SELECT c.id AS company_id, c.client_id, c.name AS company_name,
        c.location AS company_location, c.website AS company_website,
        c.industry AS company_industry
      FROM companies c
      WHERE c.id::text = $1`;
    if (clientId != null) {
      params.push(clientId);
      sql += ` AND c.client_id = $2`;
    }
    sql += ' LIMIT 1';
    try {
      const result = await pool.query(sql, params);
      if (result.rows[0]) {
        const row = result.rows[0];
        let contact = null;
        try {
          const prospectParams = [row.company_id, row.client_id];
          const prospectRes = await pool.query(
            `SELECT id, first_name, last_name, email, phone, job_title,
               vertical, status, setter_status, source, notes, icp_score,
               do_not_contact, website
             FROM prospects
             WHERE company_id = $1 AND client_id = $2
             ORDER BY created_at DESC NULLS LAST
             LIMIT 1`,
            prospectParams
          );
          contact = prospectRes.rows[0] || null;
        } catch (_) {
          contact = null;
        }
        return {
          found: true,
          companyId: row.company_id != null ? String(row.company_id) : companyId,
          prospectId: contact ? String(contact.id) : null,
          contactId: contact ? String(contact.id) : contactId,
          opportunityId: options._resolvedOpportunityId || opportunityId,
          clientId: row.client_id != null ? Number(row.client_id) : null,
          companyName: row.company_name || null,
          contactName: contact
            ? `${contact.first_name || ''} ${contact.last_name || ''}`.trim() || null
            : null,
          contactRole: contact ? contact.job_title || null : null,
          email: contact ? contact.email || null : null,
          phone: contact ? contact.phone || null : null,
          website: row.company_website || (contact && contact.website) || null,
          location: row.company_location || null,
          industry: row.company_industry || (contact && contact.vertical) || null,
          vertical: contact ? contact.vertical || null : null,
          source: (contact && contact.source) || null,
          status: contact ? contact.status || null : null,
          setterStatus: contact ? contact.setter_status || null : null,
          score: contact && contact.icp_score != null ? Number(contact.icp_score) : null,
          doNotContact: contact ? Boolean(contact.do_not_contact) : false,
          notes: contact && contact.notes ? String(contact.notes).slice(0, 500) : null,
          opportunityStage: options._opportunityStage || null,
        };
      }
    } catch (err) {
      return {
        found: false,
        error: err && err.message ? String(err.message) : 'company_load_failed',
        companyId,
        prospectId: null,
        contactId,
        opportunityId,
      };
    }
  }

  return {
    found: false,
    companyId,
    prospectId: prospectId || null,
    contactId,
    opportunityId,
  };
}

function snapshotFromProspectRow(row, extras = {}) {
  const companyName = row.company_name || null;
  const contactName =
    `${row.first_name || ''} ${row.last_name || ''}`.trim() || null;
  return {
    found: true,
    companyId: row.company_id != null ? String(row.company_id) : null,
    prospectId: row.prospect_id != null ? String(row.prospect_id) : null,
    contactId: row.prospect_id != null ? String(row.prospect_id) : null,
    opportunityId: extras.opportunityId || null,
    clientId: row.client_id != null ? Number(row.client_id) : null,
    companyName,
    contactName,
    contactRole: row.job_title || null,
    email: row.email || null,
    phone: row.phone || null,
    website: row.company_website || row.prospect_website || null,
    location: row.company_location || null,
    industry: row.company_industry || row.vertical || null,
    vertical: row.vertical || null,
    source: row.source || null,
    status: row.status || null,
    setterStatus: row.setter_status || null,
    score: row.icp_score != null ? Number(row.icp_score) : null,
    doNotContact: Boolean(row.do_not_contact),
    notes: row.notes ? String(row.notes).slice(0, 500) : null,
    opportunityStage: extras.opportunityStage || null,
  };
}

function matchesRelationshipTarget(row, target) {
  const companyId = asText(target.companyId);
  const contactId = asText(target.contactId);
  const opportunityId = asText(target.opportunityId);
  const prospectId = asText(target.prospectId);

  // Soft refs: match any provided identifier. Company-scoped interactions
  // remain useful for a prospect brief even when contact_id differs.
  if (companyId && asText(row.companyId) === companyId) return true;
  if (opportunityId && asText(row.opportunityId) === opportunityId) return true;
  if (contactId && asText(row.contactId) === contactId) return true;
  if (prospectId && asText(row.contactId) === prospectId) return true;
  return false;
}

function emptyRelationshipContext() {
  return {
    interactionCount: 0,
    insights: [],
    relationshipSummary: {
      interactionCount: 0,
      insightCount: 0,
      latestOccurredAt: null,
      interactionTypes: [],
      summaries: [],
      averageConfidence: null,
    },
    buyingSignals: [],
    painsAndGoals: [],
    objectionsAndRisks: [],
    decisionProcess: [],
    commitmentsAndNextSteps: [],
    openQuestions: [],
    preferencesAndContext: [],
    sourceRefs: {
      relationshipInteractionIds: [],
      relationshipInsightIds: [],
    },
  };
}

function buildRelationshipFromPayloads(payloads) {
  const allInsights = [];
  const interactionIds = [];
  const insightIds = [];
  for (const { id, payload } of payloads) {
    interactionIds.push(id);
    for (const insight of payload.insights || []) {
      allInsights.push({
        ...mapInsightItem(insight, id),
        _id: insight.id || null,
      });
      if (insight.id) insightIds.push(String(insight.id));
    }
  }

  const byKind = (kinds) =>
    allInsights
      .filter((i) => kinds.includes(i.kind))
      .map(({ _id, ...rest }) => rest);

  const summaries = payloads
    .map(({ payload: p }) => {
      const raw =
        (p.interaction && p.interaction.rawSummary) ||
        p.rawSummary ||
        null;
      return raw ? String(raw).trim() : null;
    })
    .filter(Boolean);

  return {
    interactionCount: payloads.length,
    insights: allInsights.map(({ _id, ...rest }) => rest),
    relationshipSummary: {
      interactionCount: payloads.length,
      insightCount: allInsights.length,
      latestOccurredAt: payloads[0]?.payload?.interaction?.occurredAt || null,
      interactionTypes: uniq(
        payloads
          .map(({ payload: p }) => p.interaction?.interactionType)
          .filter(Boolean)
      ),
      summaries: summaries.slice(0, 3),
      averageConfidence:
        payloads.length === 0
          ? null
          : Number(
              (
                payloads.reduce(
                  (sum, { payload: p }) =>
                    sum +
                    (p.interaction?.confidence != null
                      ? Number(p.interaction.confidence)
                      : 0),
                  0
                ) / payloads.length
              ).toFixed(3)
            ),
    },
    buyingSignals: byKind(['buying_signal']),
    painsAndGoals: byKind(['pain', 'goal']),
    objectionsAndRisks: byKind(['objection', 'risk']),
    decisionProcess: byKind([
      'decision_maker',
      'stakeholder',
      'timeline',
      'budget',
    ]),
    commitmentsAndNextSteps: byKind(['next_step', 'commitment']),
    openQuestions: byKind(['open_question']),
    preferencesAndContext: byKind(['preference', 'context']),
    sourceRefs: {
      relationshipInteractionIds: uniq(interactionIds.filter(Boolean)),
      relationshipInsightIds: uniq(insightIds),
    },
  };
}

/**
 * Load a single committed relationship interaction by id.
 * @returns {{ relationship, softRefs, payload }}
 */
async function loadRelationshipContextByInteractionId(interactionId, options = {}) {
  const relationshipService =
    options.relationshipService || defaultRelationshipService();
  const storeOpts = {};
  if (options.store) storeOpts.store = options.store;
  if (options.pool) storeOpts.pool = options.pool;

  const id = asText(interactionId);
  if (!id) {
    throw new ProspectOperatingBriefError(
      'target_required',
      'relationshipInteractionId is required when used as target',
      400
    );
  }

  let payload;
  try {
    payload = await relationshipService.getInteraction(id, storeOpts);
  } catch (err) {
    if (err && (err.code === 'not_found' || err.status === 404)) {
      throw new ProspectOperatingBriefError(
        'relationship_interaction_not_found',
        `Relationship interaction not found: ${id}`,
        404
      );
    }
    throw err;
  }

  if (!payload || payload.status !== 'committed') {
    throw new ProspectOperatingBriefError(
      'relationship_interaction_not_committed',
      `Relationship interaction ${id} is not committed (status=${
        payload && payload.status ? payload.status : 'unknown'
      })`,
      409
    );
  }

  const interaction = payload.interaction || {};
  const softRefs = {
    companyId: asText(interaction.companyId),
    contactId: asText(interaction.contactId),
    opportunityId: asText(interaction.opportunityId),
  };

  return {
    relationship: buildRelationshipFromPayloads([{ id, payload }]),
    softRefs,
    payload,
  };
}

async function loadRelationshipContext(target, options = {}) {
  const relationshipService =
    options.relationshipService || defaultRelationshipService();
  const storeOpts = {};
  if (options.store) storeOpts.store = options.store;
  if (options.pool) storeOpts.pool = options.pool;

  const listFilters = {
    status: 'committed',
    limit: 50,
  };
  if (target.clientId != null) listFilters.clientId = target.clientId;
  if (target.companyId) listFilters.companyId = target.companyId;

  let listed = await relationshipService.listInteractions(listFilters, storeOpts);

  // If company-scoped list returned nothing but we have other soft refs,
  // broaden (client-scoped or unscoped) then filter client-side.
  const hasSoftTarget =
    target.contactId || target.opportunityId || target.prospectId;
  if ((!listed || !listed.length) && hasSoftTarget) {
    const broadFilters = { status: 'committed', limit: 100 };
    if (target.clientId != null) broadFilters.clientId = target.clientId;
    listed = await relationshipService.listInteractions(broadFilters, storeOpts);
  }

  const hasAnyTargetId =
    target.companyId ||
    target.contactId ||
    target.opportunityId ||
    target.prospectId;
  const interactions = hasAnyTargetId
    ? (listed || []).filter((row) => matchesRelationshipTarget(row, target))
    : listed || [];

  const payloads = [];
  for (const row of interactions) {
    const payload = await relationshipService.getInteraction(row.id, storeOpts);
    if (payload && payload.status === 'committed') {
      payloads.push({ id: row.id, payload });
    }
  }

  return buildRelationshipFromPayloads(payloads);
}

async function loadMarketContext(options = {}) {
  const marketService =
    options.marketBriefingService || defaultMarketBriefingService();
  const days = clampDays(options.days);
  const pool = options.pool;
  const limit = MARKET_LIMIT;

  const callOpts = { days, limit, pool };

  let topCtas = [];
  let topOffers = [];
  let messagingThemes = [];
  let corpus = null;
  let observationIds = [];
  const caveats = [];

  try {
    if (typeof marketService.getTopCtas === 'function') {
      topCtas = await marketService.getTopCtas(callOpts);
    }
    if (typeof marketService.getTopOffers === 'function') {
      topOffers = await marketService.getTopOffers(callOpts);
    }
    if (typeof marketService.getMessagingThemes === 'function') {
      const themes = await marketService.getMessagingThemes(callOpts);
      messagingThemes = Array.isArray(themes)
        ? themes
        : themes?.items || [];
    }
    if (typeof marketService.getCorpusSummary === 'function') {
      corpus = await marketService.getCorpusSummary(callOpts);
    }
  } catch (err) {
    caveats.push(
      `market_context_unavailable: ${err && err.message ? err.message : 'error'}`
    );
    return {
      available: false,
      days,
      topCtas: [],
      topOffers: [],
      messagingThemes: [],
      corpus: null,
      observationIds: [],
      caveats,
      generalCorpusOnly: true,
    };
  }

  for (const item of [...(topCtas || []), ...(topOffers || []), ...(messagingThemes || [])]) {
    for (const id of item.exampleObservationIds || []) {
      observationIds.push(id);
    }
  }

  if (!corpus || Number(corpus.observationCount || 0) === 0) {
    caveats.push(
      'market_corpus_empty_or_unavailable: market context may be incomplete'
    );
  }

  caveats.push(
    'market_context_general: patterns are corpus-level, not prospect-competitor claims'
  );

  return {
    available: true,
    days,
    topCtas: (topCtas || []).slice(0, limit).map((item) => ({
      cta: item.cta || item.label || null,
      count: item.count != null ? Number(item.count) : null,
      companies: (item.companies || []).slice(0, 3),
      exampleObservationIds: (item.exampleObservationIds || []).slice(0, 3),
    })),
    topOffers: (topOffers || []).slice(0, limit).map((item) => ({
      label: item.label || null,
      count: item.count != null ? Number(item.count) : null,
      companies: (item.companies || []).slice(0, 3),
      exampleObservationIds: (item.exampleObservationIds || []).slice(0, 3),
    })),
    messagingThemes: (messagingThemes || []).slice(0, limit).map((item) => ({
      theme: item.theme || item.label || null,
      field: item.field || null,
      count: item.count != null ? Number(item.count) : null,
      companies: (item.companies || []).slice(0, 3),
      exampleObservationIds: (item.exampleObservationIds || []).slice(0, 3),
    })),
    corpus: corpus
      ? {
          emailCount: Number(corpus.emailCount || 0),
          companyCount: Number(corpus.companyCount || 0),
          observationCount: Number(corpus.observationCount || 0),
          readinessStatus: corpus.readinessStatus || null,
        }
      : null,
    observationIds: uniq(observationIds).slice(0, 25),
    caveats,
    generalCorpusOnly: true,
  };
}

function textBlob(items) {
  return (items || [])
    .map((i) => `${insightText(i)} ${i.sourceQuote || ''}`.toLowerCase())
    .join(' ');
}

function suggestNextAction({
  snapshot,
  relationship,
  caveats,
}) {
  const cautions = [
    'Manual recommendation only — do not auto-send or mutate CRM from this brief',
  ];
  const requiredInputs = [];

  if (snapshot && snapshot.doNotContact) {
    return {
      actionType: 'manual_review',
      priority: 'high',
      rationale:
        'Prospect is marked do-not-contact. Review compliance before any outreach.',
      suggestedMessageAngle: null,
      requiredInputs: ['confirm DNC status with operator'],
      cautions: [
        ...cautions,
        'Do not contact until DNC is cleared',
      ],
    };
  }

  const buying = relationship.buyingSignals || [];
  const nextSteps = relationship.commitmentsAndNextSteps || [];
  const openQs = relationship.openQuestions || [];
  const objections = relationship.objectionsAndRisks || [];
  const blob = textBlob([...nextSteps, ...buying, ...objections]);

  if ((!snapshot || !snapshot.found) && relationship.interactionCount === 0) {
    requiredInputs.push('company or prospect identifier with CRM record');
    return {
      actionType: 'research_company',
      priority: 'medium',
      rationale:
        'Company/prospect CRM snapshot is missing. Gather basic firmographics before outreach.',
      suggestedMessageAngle: null,
      requiredInputs,
      cautions,
    };
  }

  if (
    relationship.interactionCount === 0 &&
    (!snapshot.notes || !String(snapshot.notes).trim())
  ) {
    return {
      actionType: 'research_company',
      priority: 'medium',
      rationale:
        'No committed relationship intelligence yet. Research the company and capture a discovery debrief before pitching.',
      suggestedMessageAngle: null,
      requiredInputs: [
        'committed relationship interview or discovery notes',
        'confirmed contact details',
      ],
      cautions: [
        ...cautions,
        'Avoid inventing buying intent without committed relationship evidence',
      ],
    };
  }

  if (/\bwalkthrough|site visit|on[- ]?site\b/.test(blob)) {
    return {
      actionType: 'schedule_walkthrough',
      priority: 'high',
      rationale:
        'Committed relationship insights reference a walkthrough or on-site next step.',
      suggestedMessageAngle:
        'Confirm walkthrough timing and what you will inspect on site',
      requiredInputs: uniq([
        ...requiredInputs,
        'available walkthrough windows',
        snapshot.contactName ? null : 'decision-maker contact',
      ].filter(Boolean)),
      cautions,
    };
  }

  if (/\bproposal|estimate|quote|pricing\b/.test(blob)) {
    return {
      actionType: 'prepare_proposal',
      priority: 'high',
      rationale:
        'Committed insights reference a proposal, estimate, quote, or pricing commitment.',
      suggestedMessageAngle:
        buying.length
          ? 'Lead with the stated buying signal and promised pricing/scope'
          : 'Deliver the promised estimate/proposal without new claims',
      requiredInputs: uniq([
        ...requiredInputs,
        'scope/notes from discovery',
        'pricing inputs if not already known',
      ]),
      cautions: [
        ...cautions,
        ...(objections.length
          ? ['Address recorded objections/risks explicitly in the proposal']
          : []),
      ],
    };
  }

  if (openQs.length >= 2 && nextSteps.length === 0) {
    return {
      actionType: 'ask_clarifying_question',
      priority: 'medium',
      rationale:
        'Multiple open questions remain and no concrete next step is recorded.',
      suggestedMessageAngle: insightText(openQs[0]) || 'Clarify open discovery questions',
      requiredInputs: openQs.slice(0, 3).map((q) => insightText(q)),
      cautions,
    };
  }

  if (/\bwait|they will (reply|email|send)|awaiting\b/.test(blob)) {
    return {
      actionType: 'wait_for_reply',
      priority: 'low',
      rationale:
        'Recorded next steps suggest the ball is in their court.',
      suggestedMessageAngle: null,
      requiredInputs: ['follow-up date if silence continues'],
      cautions,
    };
  }

  if (buying.length || nextSteps.length) {
    const angleParts = [];
    if (buying[0]) angleParts.push(insightText(buying[0]));
    if (nextSteps[0]) angleParts.push(insightText(nextSteps[0]));
    return {
      actionType: 'send_follow_up',
      priority: buying.length ? 'high' : 'medium',
      rationale: buying.length
        ? 'Buying signal(s) are recorded in committed relationship intelligence; a manual follow-up is the conservative next move.'
        : 'A committed next step exists; send a manual follow-up that honors the recorded commitment.',
      suggestedMessageAngle: angleParts.filter(Boolean).join(' — ') || null,
      requiredInputs: uniq([
        snapshot.email || snapshot.phone ? null : 'reachable contact channel',
        ...requiredInputs,
      ].filter(Boolean)),
      cautions,
    };
  }

  if (openQs.length) {
    return {
      actionType: 'ask_clarifying_question',
      priority: 'medium',
      rationale: 'Open questions remain after the latest committed interaction.',
      suggestedMessageAngle: insightText(openQs[0]),
      requiredInputs: openQs.slice(0, 3).map((q) => insightText(q)),
      cautions,
    };
  }

  if (caveats.length && relationship.interactionCount === 0) {
    return {
      actionType: 'manual_review',
      priority: 'medium',
      rationale:
        'Available context is thin. Review the brief caveats and decide the next operator move manually.',
      suggestedMessageAngle: null,
      requiredInputs: ['operator judgment'],
      cautions,
    };
  }

  return {
    actionType: 'manual_review',
    priority: 'low',
    rationale:
      'No strong automated recommendation from available evidence. Review the brief and choose the next manual step.',
    suggestedMessageAngle: null,
    requiredInputs: ['operator judgment'],
    cautions,
  };
}

function readinessFromParts({
  snapshot,
  relationship,
  market,
  includeMarket,
  includeRelationship,
  companyIntelligenceOverride = null,
}) {
  let companyIntelligence = READINESS.UNKNOWN;
  if (companyIntelligenceOverride) {
    companyIntelligence = companyIntelligenceOverride;
  } else if (snapshot && snapshot.found) {
    const hasCore =
      snapshot.companyName || snapshot.contactName || snapshot.website;
    companyIntelligence = hasCore ? READINESS.READY : READINESS.PARTIAL;
  } else if (snapshot && snapshot.error) {
    companyIntelligence = READINESS.BLOCKED;
  } else {
    companyIntelligence = READINESS.PARTIAL;
  }

  let relationshipIntelligence = READINESS.UNKNOWN;
  if (!includeRelationship) {
    relationshipIntelligence = READINESS.UNKNOWN;
  } else if (relationship.interactionCount > 0) {
    relationshipIntelligence =
      relationship.insights.length > 0 ? READINESS.READY : READINESS.PARTIAL;
  } else {
    relationshipIntelligence = READINESS.PARTIAL;
  }

  let marketIntelligence = READINESS.UNKNOWN;
  if (!includeMarket) {
    marketIntelligence = READINESS.UNKNOWN;
  } else if (!market || !market.available) {
    marketIntelligence = READINESS.BLOCKED;
  } else if (
    market.corpus &&
    Number(market.corpus.observationCount || 0) > 0 &&
    ((market.topCtas && market.topCtas.length) ||
      (market.topOffers && market.topOffers.length) ||
      (market.messagingThemes && market.messagingThemes.length))
  ) {
    marketIntelligence = READINESS.READY;
  } else {
    marketIntelligence = READINESS.PARTIAL;
  }

  return {
    marketIntelligence,
    relationshipIntelligence,
    companyIntelligence,
  };
}

/**
 * @param {object} options
 * @returns {Promise<object>}
 */
async function getProspectOperatingBrief(options = {}) {
  let companyId = asText(options.companyId);
  let prospectId = asText(options.prospectId);
  let opportunityId = asText(options.opportunityId);
  let contactId = asText(options.contactId);
  const relationshipInteractionId = asText(
    options.relationshipInteractionId || options.interactionId
  );
  const days = clampDays(options.days);
  const includeMarketContext = parseTruthy(options.includeMarketContext, true);
  const includeRelationshipContext = parseTruthy(
    options.includeRelationshipContext,
    true
  );

  if (
    !companyId &&
    !prospectId &&
    !opportunityId &&
    !contactId &&
    !relationshipInteractionId
  ) {
    throw new ProspectOperatingBriefError(
      'target_required',
      'At least one of companyId, prospectId, opportunityId, contactId, or relationshipInteractionId is required',
      400
    );
  }

  const caveats = [];
  const generatedAt = new Date().toISOString();
  let companyIntelligenceOverride = null;
  let relationship = emptyRelationshipContext();
  let interactionSoftRefs = null;

  // Prefer an explicit committed interaction when provided.
  if (relationshipInteractionId) {
    if (!includeRelationshipContext) {
      throw new ProspectOperatingBriefError(
        'relationship_context_required',
        'relationshipInteractionId requires includeRelationshipContext',
        400
      );
    }
    const loaded = await loadRelationshipContextByInteractionId(
      relationshipInteractionId,
      options
    );
    relationship = loaded.relationship;
    interactionSoftRefs = loaded.softRefs;

    // Fill target soft refs from the interaction when not already provided.
    if (!companyId && interactionSoftRefs.companyId) {
      companyId = interactionSoftRefs.companyId;
    }
    if (!contactId && interactionSoftRefs.contactId) {
      contactId = interactionSoftRefs.contactId;
      if (!prospectId) prospectId = interactionSoftRefs.contactId;
    }
    if (!opportunityId && interactionSoftRefs.opportunityId) {
      opportunityId = interactionSoftRefs.opportunityId;
    }

    if (!interactionSoftRefs.companyId && !companyId) {
      companyIntelligenceOverride = READINESS.UNKNOWN;
      caveats.push('target_not_matched_to_company_record');
    }
  }

  const loadSnapshot =
    typeof options.loadCompanySnapshot === 'function'
      ? options.loadCompanySnapshot
      : loadCompanySnapshot;

  let snapshot = { found: false };
  const hasCrmTarget = companyId || prospectId || opportunityId || contactId;
  if (hasCrmTarget) {
    snapshot = await loadSnapshot({
      pool: options.pool || defaultPool,
      companyId,
      prospectId,
      opportunityId,
      contactId,
      clientId: options.clientId,
    });

    if (!snapshot || !snapshot.found) {
      caveats.push(
        'company_snapshot_missing: CRM company/prospect record not found or incomplete'
      );
    }
    if (snapshot && snapshot.error) {
      caveats.push(`company_snapshot_error: ${snapshot.error}`);
    }
  } else if (relationshipInteractionId) {
    // Interaction-only target with no company/contact/opportunity soft refs.
    snapshot = { found: false };
  } else {
    caveats.push(
      'company_snapshot_missing: CRM company/prospect record not found or incomplete'
    );
  }

  const resolvedTarget = {
    companyId: (snapshot && snapshot.companyId) || companyId || null,
    prospectId: (snapshot && snapshot.prospectId) || prospectId || null,
    opportunityId:
      (snapshot && snapshot.opportunityId) || opportunityId || null,
    contactId:
      (snapshot && snapshot.contactId) ||
      contactId ||
      prospectId ||
      null,
    companyName: (snapshot && snapshot.companyName) || null,
    contactName: (snapshot && snapshot.contactName) || null,
    clientId: (snapshot && snapshot.clientId) || options.clientId || null,
    relationshipInteractionId: relationshipInteractionId || null,
  };

  if (includeRelationshipContext) {
    if (!relationshipInteractionId) {
      try {
        relationship = await loadRelationshipContext(resolvedTarget, options);
        if (relationship.interactionCount === 0) {
          caveats.push(
            'relationship_intelligence_missing: no committed relationship interactions for this target'
          );
        }
      } catch (err) {
        caveats.push(
          `relationship_intelligence_unavailable: ${
            err && err.message ? err.message : 'error'
          }`
        );
      }
    }
  } else {
    caveats.push('relationship_context_excluded_by_request');
  }

  let market = {
    available: false,
    days,
    topCtas: [],
    topOffers: [],
    messagingThemes: [],
    corpus: null,
    observationIds: [],
    caveats: [],
    generalCorpusOnly: true,
  };

  if (includeMarketContext) {
    market = await loadMarketContext({
      ...options,
      days,
      pool: options.pool || defaultPool,
    });
    for (const c of market.caveats || []) caveats.push(c);
  } else {
    caveats.push('market_context_excluded_by_request');
  }

  const companySnapshot = snapshot && snapshot.found
    ? {
        companyName: snapshot.companyName,
        website: snapshot.website,
        location: snapshot.location,
        industry: snapshot.industry,
        vertical: snapshot.vertical,
        source: snapshot.source,
        status: snapshot.status,
        setterStatus: snapshot.setterStatus,
        score: snapshot.score,
        contactName: snapshot.contactName,
        contactRole: snapshot.contactRole,
        email: snapshot.email,
        phone: snapshot.phone,
        notes: snapshot.notes,
        doNotContact: snapshot.doNotContact,
        opportunityStage: snapshot.opportunityStage || null,
      }
    : {};

  const suggestedNextAction = suggestNextAction({
    snapshot,
    relationship,
    caveats,
  });

  // Guard: never advertise execution
  suggestedNextAction.cautions = uniq([
    ...(suggestedNextAction.cautions || []),
    'No autonomous execution — Jake executes manually',
  ]);

  const readiness = readinessFromParts({
    snapshot,
    relationship,
    market,
    includeMarket: includeMarketContext,
    includeRelationship: includeRelationshipContext,
    companyIntelligenceOverride,
  });

  const openQuestions = [
    ...relationship.openQuestions,
  ];
  if (!snapshot || !snapshot.found) {
    openQuestions.push({
      kind: 'open_question',
      label: 'CRM identity',
      value: 'Confirm company/prospect CRM record for this target',
      confidence: null,
      sourceQuote: null,
      interactionId: relationshipInteractionId || null,
    });
  }

  return {
    ok: true,
    kind: 'prospect_operating_brief',
    isEvidence: false,
    generatedAt,
    target: {
      companyId: resolvedTarget.companyId,
      prospectId: resolvedTarget.prospectId,
      opportunityId: resolvedTarget.opportunityId,
      contactId: resolvedTarget.contactId,
      companyName: resolvedTarget.companyName,
      contactName: resolvedTarget.contactName,
      relationshipInteractionId: resolvedTarget.relationshipInteractionId,
    },
    readiness,
    sections: {
      companySnapshot,
      relationshipSummary: relationship.relationshipSummary,
      buyingSignals: relationship.buyingSignals,
      painsAndGoals: relationship.painsAndGoals,
      objectionsAndRisks: relationship.objectionsAndRisks,
      decisionProcess: relationship.decisionProcess,
      commitmentsAndNextSteps: relationship.commitmentsAndNextSteps,
      marketContext: includeMarketContext
        ? {
            days: market.days,
            generalCorpusOnly: true,
            topCtas: market.topCtas,
            topOffers: market.topOffers,
            messagingThemes: market.messagingThemes,
            corpus: market.corpus,
          }
        : {},
      openQuestions,
      suggestedNextAction,
    },
    sourceRefs: {
      relationshipInteractionIds:
        relationship.sourceRefs.relationshipInteractionIds,
      relationshipInsightIds: relationship.sourceRefs.relationshipInsightIds,
      marketObservationIds: market.observationIds || [],
      companyProfileIds: resolvedTarget.companyId
        ? [String(resolvedTarget.companyId)]
        : [],
    },
    caveats: uniq(caveats),
    autonomousExecution: false,
    internal: true,
  };
}

function formatList(title, items, formatter) {
  const lines = [`${title}:`];
  if (!items || !items.length) {
    lines.push('(none)');
    return lines;
  }
  items.forEach((item, i) => {
    lines.push(`${i + 1}. ${formatter(item)}`);
  });
  return lines;
}

function formatOperatingBriefReport(brief) {
  const t = brief.target || {};
  const s = brief.sections || {};
  const action = s.suggestedNextAction || {};
  const lines = [
    'Prospect Operating Brief',
    `Generated: ${brief.generatedAt || ''}`,
    `isEvidence: ${brief.isEvidence === false ? 'false' : String(brief.isEvidence)}`,
    '',
    'Target:',
    `- Company: ${t.companyName || t.companyId || 'n/a'}`,
    `- Contact: ${t.contactName || t.contactId || t.prospectId || 'n/a'}`,
    `- Prospect ID: ${t.prospectId || 'n/a'}`,
    `- Opportunity ID: ${t.opportunityId || 'n/a'}`,
  ];
  if (t.relationshipInteractionId) {
    lines.push(`- Relationship Interaction ID: ${t.relationshipInteractionId}`);
  }

  const snap = s.companySnapshot || {};
  if (Object.keys(snap).length) {
    lines.push(
      '',
      'Company Snapshot:',
      `- Location: ${snap.location || 'n/a'}`,
      `- Industry/Vertical: ${snap.industry || snap.vertical || 'n/a'}`,
      `- Website: ${snap.website || 'n/a'}`,
      `- Status: ${snap.status || 'n/a'}`,
      `- Contact: ${snap.contactName || 'n/a'}${snap.email ? ` <${snap.email}>` : ''}${snap.phone ? ` / ${snap.phone}` : ''}`
    );
  }

  lines.push(
    '',
    ...formatList('Buying Signals', s.buyingSignals, (item) => insightText(item) || '(empty)'),
    '',
    ...formatList(
      'Commitments / Next Steps',
      s.commitmentsAndNextSteps,
      (item) => insightText(item) || '(empty)'
    ),
    '',
    ...formatList(
      'Risks / Open Questions',
      [...(s.objectionsAndRisks || []), ...(s.openQuestions || [])],
      (item) => insightText(item) || '(empty)'
    )
  );

  const market = s.marketContext || {};
  if (market.topCtas || market.topOffers || market.messagingThemes) {
    lines.push('', 'Market Context (general corpus):');
    if (market.topCtas && market.topCtas.length) {
      lines.push(
        `- Top CTAs: ${market.topCtas
          .slice(0, 3)
          .map((c) => c.cta)
          .filter(Boolean)
          .join('; ') || 'n/a'}`
      );
    } else {
      lines.push('- Top CTAs: (none)');
    }
    if (market.topOffers && market.topOffers.length) {
      lines.push(
        `- Top Offers: ${market.topOffers
          .slice(0, 3)
          .map((o) => o.label)
          .filter(Boolean)
          .join('; ') || 'n/a'}`
      );
    }
    if (market.messagingThemes && market.messagingThemes.length) {
      lines.push(
        `- Themes: ${market.messagingThemes
          .slice(0, 3)
          .map((th) => th.theme)
          .filter(Boolean)
          .join('; ') || 'n/a'}`
      );
    }
  }

  lines.push(
    '',
    'Next Best Manual Action:',
    `- Type: ${action.actionType || 'manual_review'}`,
    `- Priority: ${action.priority || 'n/a'}`,
    `- Rationale: ${action.rationale || 'n/a'}`
  );
  if (action.suggestedMessageAngle) {
    lines.push(`- Message angle: ${action.suggestedMessageAngle}`);
  }
  if (action.requiredInputs && action.requiredInputs.length) {
    lines.push(`- Required inputs: ${action.requiredInputs.join('; ')}`);
  }
  if (action.cautions && action.cautions.length) {
    lines.push(`- Cautions: ${action.cautions.join('; ')}`);
  }

  if (brief.caveats && brief.caveats.length) {
    lines.push('', 'Caveats:');
    for (const caveat of brief.caveats) {
      lines.push(`- ${caveat}`);
    }
  }

  lines.push('', 'Autonomous execution: disabled');

  return lines.join('\n');
}

module.exports = {
  ACTION_TYPES,
  DEFAULT_DAYS,
  ProspectOperatingBriefError,
  clampDays,
  formatOperatingBriefReport,
  getProspectOperatingBrief,
  loadCompanySnapshot,
  loadRelationshipContextByInteractionId,
  suggestNextAction,
};
