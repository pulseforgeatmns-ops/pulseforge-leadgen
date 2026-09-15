'use strict';

/**
 * SPEC-106 — operator-reported operating evidence.
 *
 * Conversation is an input channel to intelligence. It is not itself
 * the source of truth. Operator assertions become durable only when
 * recognized, classified, provenance-preserved, policy-permitted, and
 * written to an appropriate canonical store.
 *
 * operator_attested ≠ system_observed ≠ inferred
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  isOperatingEvidenceQuestion,
  isOperatingGroundedRecommendation,
  isNewInvestigationRequest,
  resolveTenantId,
  resolveClientId,
} = require('./OperatingEvidenceRetrieval');
const { looksLikeRecommendation, looksLikeInvestigation } = require('../specialistDelegation/CognitiveMode');

const TURN_TYPE = 'operating_update';

const EPISTEMIC = Object.freeze({
  OPERATOR_ATTESTED: 'operator_attested',
  SYSTEM_OBSERVED: 'system_observed',
  INFERRED: 'inferred',
  PLANNED: 'planned',
  EXPECTED: 'expected',
});

const TEMPORAL = Object.freeze({
  HISTORICAL_COMPLETED: 'historical_completed',
  CURRENT_COMPLETED: 'current_completed',
  FUTURE_PLANNED: 'future_planned',
  FUTURE_EXPECTED: 'future_expected',
});

const SEMANTIC = Object.freeze({
  CAMPAIGN_EXECUTION: 'campaign_execution',
  INTERNAL_OPERATIONAL_EVENT: 'internal_operational_event',
  CAMPAIGN_FOLLOW_UP: 'campaign_follow_up',
  ACTIVITY_COUNT: 'activity_count',
});

const DISPOSITION = Object.freeze({
  PERSISTED: 'persisted',
  CONFIRMATION_REQUIRED: 'confirmation_required',
  ACKNOWLEDGED_ONLY: 'acknowledged_only',
  REJECTED: 'rejected',
});

const SOURCE_TYPE = 'operator_report';
const PREDICATE_MAIL = 'physical_mail_execution';
const PREDICATE_FOLLOW_UP = 'campaign_follow_up_expected';
const DEFAULT_TZ = 'America/New_York';

const OPERATING_CONCEPT_RE = new RegExp(
  [
    String.raw`\bcampaigns?\b`,
    String.raw`\bprospects?\b`,
    String.raw`\bleads?\b`,
    String.raw`\bcustomers?\b`,
    String.raw`\bwalkthroughs?\b`,
    String.raw`\bestimates?\b`,
    String.raw`\bjobs?\b`,
    String.raw`\bpayments?\b`,
    String.raw`\boutreach\b`,
    String.raw`\bmail(?:ed|ing)?\b`,
    String.raw`\bvisits?\b`,
    String.raw`\bphysical\s+visits?\b`,
    String.raw`\bcall(?:s| attempts)?\b`,
    String.raw`\bfollow[- ]ups?\b`,
    String.raw`\bmeetings?\b`,
    String.raw`\btraining\b`,
    String.raw`\bmissions?\b`,
    String.raw`\bconversions?\b`,
    String.raw`\bappointments?\b`,
    String.raw`\baccounts?\b`,
    String.raw`\bcontracts?\b`,
    String.raw`\bao\b`,
    String.raw`\bworkflow\b`,
  ].join('|'),
  'i'
);

const DECLARATIVE_UPDATE_RE = new RegExp(
  [
    String.raw`\boperating update\b`,
    String.raw`\bwas (?:physically )?mailed\b`,
    String.raw`\bactually went out\b`,
    String.raw`\bwent out\b`,
    String.raw`\bfinished training\b`,
    String.raw`\bmet with\b`,
    String.raw`\bwalked (?:him|her|them) through\b`,
    String.raw`\bshould begin\b`,
    String.raw`\bwill begin\b`,
    String.raw`\bis delayed\b`,
    String.raw`\bwe completed\b`,
    String.raw`\bwe haven'?t\b`,
    String.raw`\bthe client signed\b`,
    String.raw`\bdeclined\b`,
    String.raw`\bcorrection\b`,
  ].join('|'),
  'i'
);

const CORRECTION_RE =
  /\b(correction|actually (?:went out|mailed|was)|not august|not on august)\b/i;

const INTERROGATIVE_RE =
  /^(?:(?:max|please),?\s+)?(?:what|who|when|where|why|how|did|does|do|was|were|is|are|have|has|had|can|could|should|would)\b/i;

function present(text) {
  return String(text || '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function optsOf(input = {}) {
  return input.operatingUpdateOpts || input.opts || {};
}

function resolveTimeZone(input = {}) {
  const opts = optsOf(input);
  const session = input.session || {};
  const sessionCtx =
    session.context && typeof session.context === 'object' ? session.context : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};
  return (
    opts.timeZone ||
    input.timeZone ||
    envelope.timeZone ||
    envelope.timezone ||
    sessionCtx.timeZone ||
    sessionCtx.timezone ||
    DEFAULT_TZ
  );
}

function resolveNow(input = {}) {
  const opts = optsOf(input);
  const raw = opts.now || input.now || input.clock || null;
  if (raw instanceof Date && !Number.isNaN(raw.getTime())) return raw;
  if (typeof raw === 'string' && raw.trim()) {
    const parsed = new Date(raw);
    if (!Number.isNaN(parsed.getTime())) return parsed;
  }
  return new Date();
}

function resolveActor(input = {}) {
  const opts = optsOf(input);
  const session = input.session || {};
  const sessionCtx =
    session.context && typeof session.context === 'object' ? session.context : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};
  const user = envelope.user || session.user || sessionCtx.user || {};
  return String(
    opts.actorId ||
      input.actorId ||
      envelope.operatorId ||
      envelope.userId ||
      session.operator ||
      sessionCtx.operatorId ||
      user.id ||
      user.email ||
      'workspace_operator'
  ).trim();
}

function resolveMissionId(input = {}) {
  const session = input.session || {};
  const sessionCtx =
    session.context && typeof session.context === 'object' ? session.context : {};
  const context =
    input.context && typeof input.context === 'object' ? input.context : {};
  const candidates = [
    input.missionId,
    input.mission_id,
    context.missionId,
    context.mission_id,
    context.acquisitionMissionId,
    context.acquisition_mission_id,
    session.missionId,
    session.mission_id,
    sessionCtx.missionId,
    sessionCtx.mission_id,
    sessionCtx.acquisitionMissionId,
    sessionCtx.acquisition_mission_id,
  ];
  const value = candidates.find((candidate) => candidate != null && String(candidate).trim());
  return value == null ? null : String(value).trim();
}

function localDateKey(date, timeZone) {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: timeZone || DEFAULT_TZ,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  return fmt.format(date);
}

function addLocalDays(dateKey, days) {
  const [year, month, day] = String(dateKey).split('-').map(Number);
  const utc = new Date(Date.UTC(year, month - 1, day + Number(days || 0)));
  const y = utc.getUTCFullYear();
  const m = String(utc.getUTCMonth() + 1).padStart(2, '0');
  const d = String(utc.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function compareDateKeys(a, b) {
  if (!a || !b) return 0;
  return String(a).localeCompare(String(b));
}

const MONTHS = Object.freeze({
  january: 1,
  february: 2,
  march: 3,
  april: 4,
  may: 5,
  june: 6,
  july: 7,
  august: 8,
  september: 9,
  october: 10,
  november: 11,
  december: 12,
  jan: 1,
  feb: 2,
  mar: 3,
  apr: 4,
  jun: 6,
  jul: 7,
  aug: 8,
  sep: 9,
  sept: 9,
  oct: 10,
  nov: 11,
  dec: 12,
});

function parseAbsoluteDate(text, todayKey) {
  const q = String(text || '');
  const monthMatch = q.match(
    /\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sept?|oct|nov|dec)\.?\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s*(20\d{2}))?\b/i
  );
  if (monthMatch) {
    const month = MONTHS[monthMatch[1].toLowerCase().replace(/\.$/, '')];
    const day = Number(monthMatch[2]);
    const year = monthMatch[3] ? Number(monthMatch[3]) : Number(String(todayKey).slice(0, 4));
    if (month && day >= 1 && day <= 31) {
      return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    }
  }
  const iso = q.match(/\b(20\d{2})-(\d{2})-(\d{2})\b/);
  if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`;
  return null;
}

function resolveRelativeDate(text, todayKey) {
  const q = String(text || '').toLowerCase();
  if (/\b(earlier today|this morning|this afternoon|today)\b/.test(q)) return todayKey;
  if (/\btomorrow\b/.test(q)) return addLocalDays(todayKey, 1);
  if (/\byesterday\b/.test(q)) return addLocalDays(todayKey, -1);
  if (/\bnext week\b/.test(q)) return addLocalDays(todayKey, 7);
  return parseAbsoluteDate(text, todayKey);
}

function campaignKeyFromName(name) {
  const n = present(name).toLowerCase();
  const numbered = n.match(/campaign\s*0*(\d+)/);
  if (numbered) return `campaign_${numbered[1]}`;
  return n.replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '') || null;
}

function extractCampaignName(text) {
  const match = String(text || '').match(/\bcampaign\s*0*(\d+)\b/i);
  if (!match) return null;
  return `Campaign ${String(Number(match[1])).padStart(3, '0')}`;
}

function looksLikeQuestion(text) {
  const q = present(text);
  if (!q) return false;
  if (INTERROGATIVE_RE.test(q) && q.includes('?')) return true;
  if (INTERROGATIVE_RE.test(q) && !DECLARATIVE_UPDATE_RE.test(q)) return true;
  if (/^\s*(?:was|were|did|does|do|is|are|have|has)\b.+\?/.test(q)) return true;
  return false;
}

function isCorrectionMessage(text) {
  return CORRECTION_RE.test(String(text || ''));
}

/**
 * Recognize an operator-reported operating update.
 * Questions, recommendations, and investigations are not updates.
 */
function isOperatorOperatingUpdate(question) {
  const q = present(question);
  if (!q) return false;
  try {
    const challenge = require('./RecommendationClaimChallenge');
    if (challenge.isClaimChallenge(q) || challenge.isOperatorClaimCorrection(q)) {
      return false;
    }
  } catch (_) {
    // keep SPEC-106 classification if challenge helper is unavailable
  }
  if (isOperatingEvidenceQuestion(q)) return false;
  if (isOperatingGroundedRecommendation(q)) return false;
  if (isNewInvestigationRequest(q)) return false;
  if (looksLikeInvestigation(q)) return false;
  if (looksLikeRecommendation(q) && !DECLARATIVE_UPDATE_RE.test(q)) return false;
  if (looksLikeQuestion(q) && !isCorrectionMessage(q) && !/\boperating update\b/i.test(q)) {
    return false;
  }
  if (/\boperating update\b/i.test(q)) return true;
  if (isCorrectionMessage(q) && OPERATING_CONCEPT_RE.test(q)) return true;
  if (OPERATING_CONCEPT_RE.test(q) && DECLARATIVE_UPDATE_RE.test(q)) return true;
  return false;
}

function splitClauses(text) {
  return String(text || '')
    .replace(/\bquick operating update:\s*/i, '')
    .split(/(?<=[.!?])\s+/)
    .map((part) => present(part))
    .filter(Boolean);
}

function classifyActivityClause(clause, todayKey) {
  const text = String(clause || '');
  const physicalMatch = text.match(/\b(\d+)\s+physical(?:\s+business)?\s+visits?\b/i)
    || text.match(/\b(\d+)\s+visits?\b/i);
  if (physicalMatch) {
    return {
      type: 'operating_assertion',
      semanticType: SEMANTIC.ACTIVITY_COUNT,
      action: 'activity_count',
      predicate: 'physical_visits',
      value: Number(physicalMatch[1]),
      temporalState: TEMPORAL.CURRENT_COMPLETED,
      temporalClass: 'completed',
      epistemicState: EPISTEMIC.OPERATOR_ATTESTED,
      occurredAt: todayKey,
      expectedAt: null,
      originalWording: text,
      subject: { kind: 'activity', name: 'physical visits' },
      confidence: 0.85,
      correction: false,
    };
  }

  const outboundMatch = text.match(/\b(?:completed|made|did|logged)\s+(\d+)\s+(?:outbound(?:\s+business)?\s+)?call(?:\s+business)?\s+attempts?\b/i)
    || text.match(/\b(\d+)\s+outbound(?:\s+business)?\s+call\s+attempts?\b/i)
    || text.match(/\b(\d+)\s+calls?\b/i);
  if (outboundMatch) {
    return {
      type: 'operating_assertion',
      semanticType: SEMANTIC.ACTIVITY_COUNT,
      action: 'activity_count',
      predicate: 'outbound_call_attempts',
      value: Number(outboundMatch[1]),
      temporalState: TEMPORAL.CURRENT_COMPLETED,
      temporalClass: 'completed',
      epistemicState: EPISTEMIC.OPERATOR_ATTESTED,
      occurredAt: todayKey,
      expectedAt: null,
      originalWording: text,
      subject: { kind: 'activity', name: 'outbound call attempts' },
      confidence: 0.85,
      correction: false,
    };
  }

  return null;
}

function classifyMailClause(clause, todayKey) {
  if (!/\b(mailed|went out|sent out)\b/i.test(clause)) return null;
  if (!/\bcampaign\b/i.test(clause) && !/\bmail\b/i.test(clause)) return null;
  const occurredAt = resolveRelativeDate(clause, todayKey);
  const campaignName = extractCampaignName(clause) || 'Campaign 001';
  const temporalState =
    occurredAt && compareDateKeys(occurredAt, todayKey) < 0
      ? TEMPORAL.HISTORICAL_COMPLETED
      : TEMPORAL.CURRENT_COMPLETED;
  return {
    type: 'operating_assertion',
    semanticType: SEMANTIC.CAMPAIGN_EXECUTION,
    action: 'physical_mail',
    predicate: PREDICATE_MAIL,
    value: 'completed',
    temporalState,
    temporalClass: 'completed',
    epistemicState: EPISTEMIC.OPERATOR_ATTESTED,
    occurredAt,
    expectedAt: null,
    originalWording: clause,
    subject: { kind: 'campaign', name: campaignName, key: campaignKeyFromName(campaignName) },
    confidence: 0.7,
    correction: isCorrectionMessage(clause),
  };
}

function classifyTrainingClause(clause, todayKey) {
  const training =
    /\b(training|walked (?:him|her|them) through|finished training|met with)\b/i.test(clause);
  if (!training) return null;
  if (!/\b(ao|aos|account owners?|app|workflow|training)\b/i.test(clause)) return null;
  const personMatch = clause.match(
    /\b(?:met with|trained|walked)\s+([A-Z][a-z]+)\b|\b([A-Z][a-z]+)\s+(?:finished|completed)\b|\b([A-Z][a-z]+),?\s+one of our\b/
  );
  const personName = personMatch
    ? personMatch[1] || personMatch[2] || personMatch[3]
    : (clause.match(/\b([A-Z][a-z]+)\b/) || [])[1] || null;
  const occurredAt = resolveRelativeDate(clause, todayKey) || todayKey;
  return {
    type: 'operating_assertion',
    semanticType: SEMANTIC.INTERNAL_OPERATIONAL_EVENT,
    action: 'ao_training',
    event: 'ao_training',
    predicate: 'ao_training',
    value: 'completed',
    temporalState: TEMPORAL.CURRENT_COMPLETED,
    temporalClass: 'completed',
    epistemicState: EPISTEMIC.OPERATOR_ATTESTED,
    occurredAt,
    expectedAt: null,
    originalWording: clause,
    subject: { kind: 'person', name: personName, role: 'ao' },
    confidence: 0.65,
    correction: false,
  };
}

function classifyFollowUpClause(clause, todayKey) {
  if (!/\bfollow[- ]up\b/i.test(clause)) return null;
  const expected = /\bshould\b/i.test(clause);
  const planned = /\b(will|begins|is (?:scheduled|planned) to)\b/i.test(clause) && !expected;
  if (!expected && !planned && !/\bbegin|starts?|due\b/i.test(clause)) return null;
  const expectedAt = resolveRelativeDate(clause, todayKey);
  const campaignName = extractCampaignName(clause);
  const cohortMatch = clause.match(/\b(\d+)\s+(?:campaign\s+\d+\s+)?leads?\b/i);
  return {
    type: 'operating_assertion',
    semanticType: SEMANTIC.CAMPAIGN_FOLLOW_UP,
    action: 'follow_up',
    predicate: PREDICATE_FOLLOW_UP,
    value: expected ? 'expected' : 'planned',
    temporalState: expected ? TEMPORAL.FUTURE_EXPECTED : TEMPORAL.FUTURE_PLANNED,
    temporalClass: expected ? 'expected' : 'planned',
    epistemicState: EPISTEMIC.OPERATOR_ATTESTED,
    occurredAt: null,
    expectedAt,
    originalWording: clause,
    subject: {
      kind: 'campaign_cohort',
      name: campaignName,
      key: campaignName ? campaignKeyFromName(campaignName) : null,
      leadCount: cohortMatch ? Number(cohortMatch[1]) : null,
    },
    confidence: 0.7,
    correction: false,
  };
}

function extractOperatingAssertions(question, input = {}) {
  const now = resolveNow(input);
  const timeZone = resolveTimeZone(input);
  const todayKey = localDateKey(now, timeZone);
  const clauses = splitClauses(question);
  const assertions = [];
  const seen = new Set();

  for (const clause of clauses) {
    const candidates = [
      classifyActivityClause(clause, todayKey),
      classifyMailClause(clause, todayKey),
      classifyTrainingClause(clause, todayKey),
      classifyFollowUpClause(clause, todayKey),
    ].filter(Boolean);
    for (const candidate of candidates) {
      const key = `${candidate.semanticType}:${candidate.predicate}:${candidate.occurredAt || candidate.expectedAt || ''}`;
      if (seen.has(key)) continue;
      seen.add(key);
      assertions.push({
        ...candidate,
        recordedAt: now.toISOString(),
        observedAt: now.toISOString(),
        todayKey,
        timeZone,
        source: {
          type: SOURCE_TYPE,
          actor: resolveActor(input),
          originalAssertion: clause,
        },
      });
    }
  }

  return assertions;
}

function nameMatches(known, asked) {
  const a = present(asked).toLowerCase();
  const b = present(known).toLowerCase();
  if (!a || !b) return false;
  if (a === b) return true;
  const first = a.split(/\s+/)[0];
  const knownFirst = b.split(/\s+/)[0];
  return first.length >= 2 && first === knownFirst;
}

async function resolveKnownEntities(assertions, input = {}) {
  const opts = optsOf(input);
  const clientId = resolveClientId(input);
  const tenantId = resolveTenantId(input);
  let people = Array.isArray(opts.people) ? opts.people : [];
  if (!people.length && typeof opts.resolvePeople === 'function') {
    people = (await opts.resolvePeople({ tenantId, clientId, input })) || [];
  }
  let campaign = opts.campaign || null;
  if (!campaign && typeof opts.resolveCampaign === 'function') {
    campaign = await opts.resolveCampaign({ tenantId, clientId, input });
  }
  if (!campaign) {
    campaign = {
      name: 'Campaign 001',
      key: 'campaign_1',
      clientId,
      leadCount: opts.campaignLeadCount != null ? opts.campaignLeadCount : 20,
    };
  }

  return assertions.map((assertion) => {
    const next = { ...assertion, entityResolution: 'resolved' };
    if (assertion.subject && assertion.subject.kind === 'person') {
      const asked = assertion.subject.name;
      const matches = people.filter((person) => nameMatches(person.name, asked));
      if (matches.length > 1) {
        next.entityResolution = 'ambiguous';
        next.subject = {
          ...assertion.subject,
          id: null,
          matches: matches.map((p) => ({ id: p.id, name: p.name })),
        };
      } else if (matches.length === 1) {
        next.subject = {
          ...assertion.subject,
          id: matches[0].id,
          name: matches[0].name,
          role: matches[0].role || assertion.subject.role,
        };
      } else if (asked) {
        next.entityResolution = people.length ? 'unresolved' : 'unresolved';
        next.subject = { ...assertion.subject, id: null };
      }
    }
    if (assertion.subject && assertion.subject.kind === 'campaign') {
      next.subject = {
        ...assertion.subject,
        name: campaign.name || assertion.subject.name,
        key: campaign.key || assertion.subject.key,
        clientId,
        leadCount: campaign.leadCount,
      };
    }
    if (assertion.subject && assertion.subject.kind === 'campaign_cohort') {
      next.subject = {
        ...assertion.subject,
        name: assertion.subject.name || campaign.name,
        key: assertion.subject.key || campaign.key,
        clientId,
        leadCount: assertion.subject.leadCount || campaign.leadCount,
        resolvedLeadCount: campaign.leadCount,
      };
    }
    return next;
  });
}

function persistencePolicyFor(assertion) {
  if (assertion.entityResolution === 'ambiguous') return DISPOSITION.REJECTED;
  if (assertion.semanticType === SEMANTIC.CAMPAIGN_EXECUTION) return DISPOSITION.PERSISTED;
  if (assertion.semanticType === SEMANTIC.ACTIVITY_COUNT) return DISPOSITION.PERSISTED;
  if (assertion.semanticType === SEMANTIC.CAMPAIGN_FOLLOW_UP) {
    return DISPOSITION.CONFIRMATION_REQUIRED;
  }
  if (assertion.semanticType === SEMANTIC.INTERNAL_OPERATIONAL_EVENT) {
    return DISPOSITION.ACKNOWLEDGED_ONLY;
  }
  return DISPOSITION.ACKNOWLEDGED_ONLY;
}

async function getKnowledge(input = {}) {
  const opts = optsOf(input);
  if (opts.knowledge) return opts.knowledge;
  if (input.knowledge) return input.knowledge;
  if (typeof opts.getKnowledge === 'function') return opts.getKnowledge(input);
  if (opts.useKnowledgeBoot === true) {
    try {
      const { getKnowledgeBoot } = require('../../../utils/knowledgeRuntime');
      const boot = await getKnowledgeBoot();
      return boot && boot.knowledge ? boot.knowledge : null;
    } catch (_) {
      return null;
    }
  }
  return null;
}

function campaignSubjectId(tenantId, campaignKey) {
  return `op-campaign:${tenantId}:${campaignKey || 'campaign_1'}`;
}

async function ensureCampaignSubject(knowledge, tenantId, assertion) {
  const { NODE_TYPES } = require('../../knowledge');
  const key = (assertion.subject && assertion.subject.key) || 'campaign_1';
  const name = (assertion.subject && assertion.subject.name) || 'Campaign 001';
  const id = campaignSubjectId(tenantId, key);
  const existing = await knowledge.findNode(tenantId, id);
  if (existing) return existing;
  return knowledge.createNode({
    id,
    tenantId,
    type: NODE_TYPES.COMPANY,
    name,
    metadata: {
      operatingEntity: 'campaign',
      campaignKey: key,
      campaignName: name,
    },
  });
}

function claimStatement(assertion) {
  const campaign = (assertion.subject && assertion.subject.name) || 'Campaign 001';
  if (assertion.semanticType === SEMANTIC.CAMPAIGN_EXECUTION) {
    return `${campaign} was operator-reported as physically mailed on ${assertion.occurredAt}.`;
  }
  if (assertion.semanticType === SEMANTIC.ACTIVITY_COUNT) {
    return `${assertion.subject && assertion.subject.name ? assertion.subject.name : 'Activity'} was operator-reported at ${assertion.value}.`;
  }
  if (assertion.semanticType === SEMANTIC.CAMPAIGN_FOLLOW_UP) {
    return `Follow-up on ${campaign} leads is operator-reported as expected to begin ${assertion.expectedAt}.`;
  }
  if (assertion.semanticType === SEMANTIC.INTERNAL_OPERATIONAL_EVENT) {
    const person = (assertion.subject && assertion.subject.name) || 'an AO';
    return `${person} completed AO workflow training on ${assertion.occurredAt} (operator-reported).`;
  }
  return assertion.originalWording;
}

function assertionMetadata(assertion, extras = {}) {
  return {
    operatingUpdate: true,
    predicate: assertion.predicate,
    value: assertion.value,
    semanticType: assertion.semanticType,
    action: assertion.action || assertion.event || null,
    epistemicState: assertion.epistemicState,
    temporalState: assertion.temporalState,
    temporalClass: assertion.temporalClass,
    occurredAt: assertion.occurredAt,
    expectedAt: assertion.expectedAt,
    recordedAt: assertion.recordedAt,
    observedAt: assertion.observedAt,
    campaignName: assertion.subject && assertion.subject.name,
    campaignKey: assertion.subject && assertion.subject.key,
    clientId: extras.clientId,
    tenantId: extras.tenantId,
    missionId: extras.missionId,
    actorId: extras.actorId,
    originalAssertion: assertion.originalWording,
    originalWording: assertion.originalWording,
    entityResolution: assertion.entityResolution,
    correction: Boolean(assertion.correction || extras.correction),
    supersededBy: extras.supersededBy || null,
    supersedes: extras.supersedes || null,
  };
}

async function findActiveOperatingClaims(knowledge, tenantId, predicate, campaignKey) {
  const claims = await knowledge.findClaims({ tenantId, status: 'active', limit: 100 });
  return claims.filter((claim) => {
    const meta = claim.metadata || {};
    if (!meta.operatingUpdate) return false;
    if (predicate && meta.predicate !== predicate) return false;
    if (campaignKey && meta.campaignKey && meta.campaignKey !== campaignKey) return false;
    return true;
  });
}

async function persistAssertion(assertion, input, knowledge) {
  const tenantId = resolveTenantId(input);
  const clientId = resolveClientId(input);
  const actorId = resolveActor(input);
  const missionId = resolveMissionId(input);
  const disposition = persistencePolicyFor(assertion);
  const recordedAt = assertion.recordedAt || new Date().toISOString();

  if (!tenantId || clientId == null) {
    return {
      assertion,
      disposition: DISPOSITION.REJECTED,
      reason: 'missing_tenant',
      claim: null,
      evidence: null,
    };
  }

  if (assertion.entityResolution === 'ambiguous') {
    return {
      assertion,
      disposition: DISPOSITION.REJECTED,
      reason: 'ambiguous_identity',
      claim: null,
      evidence: null,
    };
  }

  if (disposition === DISPOSITION.ACKNOWLEDGED_ONLY) {
    return {
      assertion,
      disposition,
      reason: 'no_canonical_store',
      claim: null,
      evidence: null,
    };
  }

  if (!knowledge) {
    return {
      assertion,
      disposition: DISPOSITION.REJECTED,
      reason: 'knowledge_unavailable',
      claim: null,
      evidence: null,
    };
  }

  const persistExpected =
    assertion.semanticType === SEMANTIC.CAMPAIGN_FOLLOW_UP ||
    assertion.semanticType === SEMANTIC.CAMPAIGN_EXECUTION;

  if (!persistExpected) {
    return {
      assertion,
      disposition,
      reason: 'not_eligible',
      claim: null,
      evidence: null,
    };
  }

  const subject = await ensureCampaignSubject(knowledge, tenantId, assertion);
  const campaignKey = (assertion.subject && assertion.subject.key) || 'campaign_1';
  const isCorrection = Boolean(assertion.correction);
  const active = await findActiveOperatingClaims(
    knowledge,
    tenantId,
    assertion.predicate,
    campaignKey
  );

  let superseded = [];
  if (isCorrection || (assertion.semanticType === SEMANTIC.CAMPAIGN_EXECUTION && active.length)) {
    const incomingDate = assertion.occurredAt;
    for (const claim of active) {
      const priorDate = claim.metadata && claim.metadata.occurredAt;
      if (isCorrection || (priorDate && incomingDate && priorDate !== incomingDate)) {
        await knowledge.claims.invalidateClaim(
          tenantId,
          claim.id,
          isCorrection
            ? `Superseded by operator correction recorded ${recordedAt}`
            : `Superseded by later operator-attested claim recorded ${recordedAt}`
        );
        const updated = await knowledge.findNode(tenantId, claim.id);
        if (updated) {
          await knowledge.updateNode(tenantId, claim.id, {
            metadata: {
              ...(updated.metadata || {}),
              supersededAt: recordedAt,
              supersededByPending: true,
            },
          });
        }
        superseded.push(claim);
      } else if (priorDate && incomingDate && priorDate === incomingDate) {
        return {
          assertion,
          disposition: DISPOSITION.PERSISTED,
          reason: 'already_active',
          claim,
          evidence: null,
          superseded: [],
        };
      }
    }
  }

  const evidence = await knowledge.evidence.createEvidence({
    tenantId,
    sourceType: SOURCE_TYPE,
    sourceId: `operator:${actorId}:${recordedAt}:${assertion.predicate}`,
    summary: assertion.originalWording,
    confidence: assertion.confidence != null ? assertion.confidence : 0.7,
    payload: {
      originalAssertion: assertion.originalWording,
      normalizedAssertion: claimStatement(assertion),
      epistemicState: assertion.epistemicState,
      temporalState: assertion.temporalState,
      temporalClass: assertion.temporalClass,
      occurredAt: assertion.occurredAt,
      expectedAt: assertion.expectedAt,
      recordedAt,
      observedAt: assertion.observedAt,
      semanticType: assertion.semanticType,
      action: assertion.action || assertion.event || null,
      campaignName: assertion.subject && assertion.subject.name,
      campaignKey,
      clientId,
      tenantId,
      missionId,
      actorId,
      correction: isCorrection,
    },
    metadata: assertionMetadata(assertion, { clientId, tenantId, missionId, actorId, correction: isCorrection }),
  });
  await knowledge.evidence.attachEvidence(tenantId, evidence.id, subject.id);

  const claim = await knowledge.claims.createClaim({
    tenantId,
    statement: claimStatement(assertion),
    subjectId: subject.id,
    evidenceIds: [evidence.id],
    reason: 'Operator-attested operating update. Not independently verified.',
    metadata: assertionMetadata(assertion, {
      clientId,
      tenantId,
      missionId,
      actorId,
      correction: isCorrection,
      supersedes: superseded.map((c) => c.id),
    }),
  });

  for (const prior of superseded) {
    const current = await knowledge.findNode(tenantId, prior.id);
    if (current) {
      await knowledge.updateNode(tenantId, prior.id, {
        metadata: {
          ...(current.metadata || {}),
          supersededBy: claim.id,
          supersededAt: recordedAt,
        },
      });
    }
  }

  return {
    assertion,
    disposition:
      assertion.semanticType === SEMANTIC.CAMPAIGN_FOLLOW_UP
        ? DISPOSITION.CONFIRMATION_REQUIRED
        : DISPOSITION.PERSISTED,
    reason:
      assertion.semanticType === SEMANTIC.CAMPAIGN_FOLLOW_UP
        ? 'expected_claim_persisted_ao_mutation_blocked'
        : isCorrection
          ? 'corrected'
          : 'persisted',
    claim,
    evidence,
    superseded,
    aoMutated: false,
    externalAction: false,
  };
}

function composeOperatingUpdateAcknowledgement(results, input = {}) {
  const lines = [];
  const mail = results.find((r) => r.assertion.semanticType === SEMANTIC.CAMPAIGN_EXECUTION);
  const training = results.find(
    (r) => r.assertion.semanticType === SEMANTIC.INTERNAL_OPERATIONAL_EVENT
  );
  const followUp = results.find((r) => r.assertion.semanticType === SEMANTIC.CAMPAIGN_FOLLOW_UP);
  const ambiguous = results.filter((r) => r.disposition === DISPOSITION.REJECTED);

  if (mail && mail.assertion.correction) {
    lines.push(
      `Got it. I've applied the correction to Campaign 001 mailing. The current operator-reported date is ${mail.assertion.occurredAt}. The earlier report remains in history and is no longer the effective claim.`
    );
  } else {
    lines.push("Got it. I've updated my operating understanding from your report.");
  }

  const facts = [];
  if (mail) {
    facts.push(
      `Campaign 001 was operator-reported as physically mailed on ${mail.assertion.occurredAt}.`
    );
  }
  if (training) {
    const person = (training.assertion.subject && training.assertion.subject.name) || 'an AO';
    if (training.disposition === DISPOSITION.REJECTED) {
      facts.push(
        `You reported AO workflow training today, but I could not uniquely resolve ${person} so I have not attached that event to a person.`
      );
    } else {
      facts.push(`${person} completed AO workflow training today.`);
    }
  }
  if (followUp) {
    const count =
      (followUp.assertion.subject &&
        (followUp.assertion.subject.leadCount || followUp.assertion.subject.resolvedLeadCount)) ||
      20;
    facts.push(
      `Follow-up on the ${count} Campaign 001 leads is expected to begin ${followUp.assertion.expectedAt}.`
    );
  }
  if (facts.length) lines.push(facts.join(' '));

  const persistBits = [];
  if (mail && mail.disposition === DISPOSITION.PERSISTED) {
    persistBits.push(
      "I've recorded the mailing as operator-attested execution evidence — not as independently verified system observation."
    );
  }
  if (followUp) {
    persistBits.push(
      `I have not treated ${followUp.assertion.expectedAt ? "tomorrow's follow-up" : 'the follow-up'} as completed activity, and I have not marked those follow-ups as completed.`
    );
    persistBits.push(
      'I have not silently created or modified the Campaign 001 follow-up task cohort. Confirm if you want those planned follow-ups written to AO.'
    );
  }
  if (training && training.disposition === DISPOSITION.ACKNOWLEDGED_ONLY) {
    persistBits.push(
      'I understood the training update for this turn and have not forced it into an operational activity store.'
    );
  }
  if (persistBits.length) lines.push(persistBits.join(' '));

  if (ambiguous.length && !training) {
    lines.push(
      'One or more identities were ambiguous, so I did not attach those events to a specific person.'
    );
  }

  void input;
  return lines.join('\n\n');
}

function operatingUpdateStructured(prose, extras = {}) {
  const evidence = (extras.results || [])
    .filter((r) => r.assertion)
    .map((r, idx) => ({
      id: `op-update-${idx + 1}`,
      label: 'OPERATOR ATTESTED',
      detail: claimStatement(r.assertion),
      epistemic: r.assertion.epistemicState,
      temporal: r.assertion.temporalClass,
      disposition: r.disposition,
    }));
  return buildStructuredResponse({
    answer: prose,
    reasoning: extras.reasoning || [
      'Recognized an operator-reported operating update before CIE or Scout.',
      'Classified completed vs expected assertions and preserved operator provenance.',
    ],
    supportingEvidence: evidence,
    contradictingEvidence: [],
    confidence: 0.84,
    nextInvestigations: [],
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
    confidenceContributors: ['operator_operating_update', 'spec_106'],
    timelineReferences: [],
    relatedEntities: [],
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: true,
        memory: true,
        policy: true,
        knowledge: extras.persisted === true,
      },
      evidenceCount: evidence.length,
      asOf: new Date().toISOString(),
      unavailable: [],
      turnType: TURN_TYPE,
      operatingUpdate: true,
      specialistDelegated: false,
      scoutDelegated: false,
      cieClaimed: false,
      externalAction: false,
      aoMutated: false,
    },
  });
}

async function maybeRebuildOperatorContext(input, results) {
  const persisted = results.some((r) => r.disposition === DISPOSITION.PERSISTED && r.claim);
  if (!persisted) return false;
  const opts = optsOf(input);
  const clientId = resolveClientId(input);
  const tenantId = resolveTenantId(input);
  if (typeof opts.rebuildOperatorContext === 'function') {
    await opts.rebuildOperatorContext({
      tenantId,
      clientId,
      trigger: 'operating_evidence_recorded',
      results,
    });
    return true;
  }
  try {
    const events = require('../../../services/operatorContextEvents');
    events.scheduleOperatorContextRebuild({
      tenantId,
      clientId,
      trigger: events.REBUILD_TRIGGERS.OPERATING_EVIDENCE_RECORDED || 'operating_evidence_recorded',
      metadata: { source: 'spec_106' },
      opts: opts.operatorContextOpts || {},
    });
    return true;
  } catch (_) {
    return false;
  }
}

/**
 * SPEC-106 workspace turn handler. Returns a turn object or null.
 *
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleOperatorOperatingUpdate(input = {}) {
  const question = present(input.question);
  if (!question) return null;
  if (!isOperatorOperatingUpdate(question)) return null;

  const tenantId = resolveTenantId(input);
  const clientId = resolveClientId(input);
  const missionId = resolveMissionId(input);
  if (!tenantId || clientId == null) {
    const prose =
      'I heard an operating update, but I cannot record operator-attested evidence without an authorized tenant context.';
    return {
      reason: 'operator_operating_update',
      handled: true,
      turnType: TURN_TYPE,
      prose,
      structured: operatingUpdateStructured(prose, { persisted: false, results: [] }),
      assertions: [],
      results: [],
      delegated: false,
      launchedScout: false,
      cieClaimed: false,
      externalAction: false,
      aoMutated: false,
    };
  }

  const extracted = extractOperatingAssertions(question, input);
  if (!extracted.length) return null;

  const resolved = await resolveKnownEntities(extracted, input);
  for (const assertion of resolved) {
    assertion.missionId = missionId;
    assertion.tenantId = tenantId;
    assertion.clientId = clientId;
  }
  const knowledge = await getKnowledge(input);
  const results = [];
  for (const assertion of resolved) {
    results.push(await persistAssertion(assertion, input, knowledge));
  }

  await maybeRebuildOperatorContext(input, results);

  if (input.session && input.session.context && typeof input.session.context === 'object') {
    input.session.context.lastOperatingUpdate = {
      turnType: TURN_TYPE,
      at: new Date().toISOString(),
      assertionCount: resolved.length,
    };
  }

  const prose = composeOperatingUpdateAcknowledgement(results, input);
  return {
    reason: 'operator_operating_update',
    handled: true,
    turnType: TURN_TYPE,
    missionId,
    tenantId,
    clientId,
    prose,
    structured: operatingUpdateStructured(prose, {
      results,
      persisted: results.some((r) => r.claim),
    }),
    assertions: resolved,
    results,
    delegated: false,
    launchedScout: false,
    cieClaimed: false,
    externalAction: false,
    aoMutated: false,
    knowledgeUsed: Boolean(knowledge),
  };
}

module.exports = {
  TURN_TYPE,
  EPISTEMIC,
  TEMPORAL,
  SEMANTIC,
  DISPOSITION,
  SOURCE_TYPE,
  PREDICATE_MAIL,
  PREDICATE_FOLLOW_UP,
  isOperatorOperatingUpdate,
  extractOperatingAssertions,
  resolveKnownEntities,
  persistencePolicyFor,
  maybeHandleOperatorOperatingUpdate,
  composeOperatingUpdateAcknowledgement,
  resolveNow,
  localDateKey,
  addLocalDays,
  resolveRelativeDate,
  claimStatement,
};
