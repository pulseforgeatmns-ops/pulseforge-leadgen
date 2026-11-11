'use strict';

/**
 * P0 #1110 — Operational read intent.
 *
 * Recognizes requests to READ the active tenant's existing operational records
 * (prospects / leads / contacts, tasks / follow-ups, emails / replies /
 * outreach, pipeline state) so they are answered from tenant data before any
 * generic Blueprint (Client Intelligence) reasoning can claim them.
 *
 * Deliberately NOT a noun allowlist: a question must name a tenant record
 * object AND use a read form AND carry a tenant/state anchor. Discovery,
 * execution and record-free advisory questions are excluded so legitimate
 * Blueprint advice ("What campaign would you recommend?") and Scout discovery
 * ("Find new borrower prospects in Charleston") keep their owners.
 *
 * This module only classifies. It never authorizes discovery, outreach,
 * execution or any write.
 */

const PROSPECT_OBJECT_RE =
  /\b(?:prospects?|leads?|contacts?|borrowers?|(?:prospect|lead|contact|crm)\s+(?:list|records?|inventory|queue|database))\b/i;
const TASK_OBJECT_RE = /\b(?:tasks?|follow[- ]?ups?|callbacks?|to-?dos?)\b/i;
const EMAIL_OBJECT_RE =
  /\b(?:emails?|e-mails?|replies|replied|responses?|responded|outreach|touchpoints?|messages?\s+(?:sent|went\s+out))\b/i;
const PIPELINE_OBJECT_RE =
  /\b(?:pipeline|(?:prospect|lead|crm)\s+(?:stages?|status(?:es)?)|deals?|bookings?|booked\s+(?:calls?|meetings?)|records?)\b/i;
// "opportunities" counts as a tenant record object only when the operator
// anchors it to their own data (Failure A: "top five opportunities … use
// actual workspace data"). Unanchored it is Blueprint/market vocabulary.
const OPPORTUNITY_OBJECT_RE = /\bopportunit(?:y|ies)\b/i;
// Strong anchor required before "opportunities" means tenant records: explicit
// existing-data language or a numbered ranking ("top five opportunities").
const OPPORTUNITY_ANCHOR_RE =
  /\b(?:existing|current|stored|saved|uploaded|imported|recorded|actual)\b|\bworkspace\s+data\b|\b(?:in|from)\s+(?:this|the|my|our)\s+(?:workspace|crm|pipeline|database)\b|\btop\s+(?:\d+|three|five|ten)\s+opportunit|\bhighest[- ]priority\s+opportunit/i;

const READ_FORM_RE = new RegExp(
  [
    String.raw`\b(?:list|show|display|pull(?:\s+up)?|give\s+me|brief\s+me|briefing|summari[sz]e|summary|recap|review|walk\s+me\s+through|tell\s+me\s+about)\b`,
    String.raw`\b(?:how\s+many|count|number\s+of|total)\b`,
    String.raw`\b(?:status|update\s+on|where\s+(?:do|does|are)\s+.{0,20}\s+stand)\b`,
    String.raw`\b(?:which|who|what(?:'s|\s+is|\s+are|\s+was|\s+were)?|any)\b`,
    String.raw`\b(?:rank(?:ed|ing)?|prioriti[sz]e|top\s+(?:\d+|one|two|three|four|five|ten))\b`,
    String.raw`\b(?:identify)\b`,
  ].join('|'),
  'i'
);

// Tenant / existing-state anchors: the operator is asking about records that
// already exist, not about strategy or new discovery.
const TENANT_ANCHOR_RE = new RegExp(
  [
    String.raw`\b(?:my|our)\b`,
    String.raw`\b(?:existing|current|stored|saved|uploaded|imported|recorded|actual|real)\b`,
    String.raw`\b(?:in|from)\s+(?:this|the|my|our)\s+(?:workspace|crm|pipeline|database|account|system|list)\b`,
    String.raw`\bworkspace\s+data\b`,
    String.raw`\b(?:record\s+ids?|record\s+id)\b`,
    String.raw`\bon\s+file\b`,
  ].join('|'),
  'i'
);

const STATE_ANCHOR_RE = new RegExp(
  [
    String.raw`\b(?:how\s+many|count|number\s+of|total)\b`,
    String.raw`\b(?:status(?:es)?|stage|stages)\b`,
    String.raw`\b(?:due|overdue|outstanding|open|pending|scheduled)\b`,
    String.raw`\b(?:contact_now|qualify_first|hold|rejected|qualified|unqualified|booked|closed|hot|warm|cold|contacted|assigned)\b`,
    String.raw`\b(?:sent|went\s+out|delivered|opened|clicked|bounced|replied|responded)\b`,
    String.raw`\b(?:yet|today|yesterday|this\s+week|last\s+week|so\s+far|recently)\b`,
  ].join('|'),
  'i'
);

// New-discovery language belongs to Scout / missions.
const DISCOVERY_RE = new RegExp(
  [
    String.raw`\b(?:find|discover|search\s+for|look\s+for|locate|scrape|source|prospect\s+for)\b`,
    String.raw`\b(?:new|more|additional|fresh|other)\s+(?:prospects?|leads?|contacts?|borrowers?|companies|accounts|opportunit(?:y|ies)|markets?)\b`,
    String.raw`\bgenerate\s+(?:new\s+)?(?:prospects?|leads?)\b`,
  ].join('|'),
  'i'
);

// Anything that would change state or send something is never a read.
const EXECUTION_RE = new RegExp(
  [
    String.raw`\b(?:send|launch|enroll|delete|remove|assign|reassign|schedule|book|draft|write|compose|publish)\b`,
    String.raw`^(?:please\s+)?(?:import|upload)\b`,
    String.raw`\b(?:import|upload)\s+(?:these|this|them|the\s+following|my|our|a|the)\b`,
    String.raw`\bstart\s+(?:the\s+)?(?:outreach|campaign|sequence)\b`,
    String.raw`\b(?:create|add|make)\s+(?:a|an|the|new)?\s*(?:task|campaign|mission|sequence|list|follow[- ]?up)\b`,
    String.raw`\b(?:mark|update|change|set)\s+(?:the\s+|this\s+|that\s+|their\s+)?(?:review[- ]states?|status|stage|prospect|lead|contact|task)\b`,
    String.raw`\b(?:email|text|call|contact)\s+(?:them|those|these|him|her|each)\b`,
  ].join('|'),
  'i'
);

// Operator desk workflows (canary / preparation packages, letters, scripts,
// verification tables) produce artifacts; they keep their dedicated handlers.
const DESK_WORKFLOW_RE =
  /\b(?:canary|preparation[- ]only|packets?|package|letters?|handwritten|scorecard|call\s+scripts?|fillable|verification\s+table|work\s+order|checklist|direct\s+mail|mail\s+piece)\b/i;

// "Remember / save / track this" turns are objective persistence, not reads.
const PERSISTENCE_RE = /\b(?:remember|save|track|note)\s+(?:that|this)\b/i;

const RECORD_LIST_RE =
  /\b(?:list|show|display|pull(?:\s+up)?|give\s+me|return)\b[\s\S]{0,60}\b(?:records?|prospects?|leads?|contacts?|borrowers?|names?|ids?)\b|\brecord\s+ids?\b|\bwhich\s+(?:prospects?|leads?|contacts?)\b/i;
const RANKING_RE =
  /\b(?:rank(?:ed|ing|s)?|prioriti[sz]e|top\s+(?:\d+|one|two|three|four|five|ten)|best\s+(?:\d+|three|five)|highest|first\s+place)\b/i;

// Follow-up asking why something was ranked. Only an operational read when the
// immediately previous turn was an operational read (no Blueprint ranking).
const RANKING_FOLLOW_UP_RE =
  /\b(?:why|how\s+come|what\s+makes|explain)\b[\s\S]{0,80}\b(?:rank(?:ed|ing)?|top|first|#\s?1|number\s+one|highest|prioriti[sz]ed)\b/i;

function normalize(question) {
  return String(question || '')
    .replace(/[’‘]/g, "'")
    .replace(/\s+/g, ' ')
    .trim();
}

function objectsIn(q, anchored) {
  const objects = [];
  if (PROSPECT_OBJECT_RE.test(q)) objects.push('prospects');
  if (TASK_OBJECT_RE.test(q)) objects.push('tasks');
  if (EMAIL_OBJECT_RE.test(q)) objects.push('emails');
  if (PIPELINE_OBJECT_RE.test(q)) objects.push('pipeline');
  if (anchored && OPPORTUNITY_OBJECT_RE.test(q) && !objects.includes('prospects')) {
    objects.push('prospects');
  }
  return objects;
}

/**
 * Operator turn counter maintained by WorkspaceEngine (monotonic; the message
 * buffer is capped so its length cannot be used).
 */
function currentOperatorTurn(session) {
  const n = session && Number(session.operatorTurnSeq);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function previousTurnWasOperationalRead(session) {
  const prior =
    session && session.context && session.context.lastOperationalRead;
  if (!prior) return false;
  const now = currentOperatorTurn(session);
  if (now == null || prior.operatorTurn == null) return false;
  return Number(prior.operatorTurn) === now - 1;
}

/**
 * @param {string} question
 * @param {{ session?: object }} [opts]
 * @returns {{ isRead: boolean, reason: string, objects: string[], wantsRecordList: boolean, wantsRanking: boolean }}
 */
function classifyOperationalRead(question, opts = {}) {
  const q = normalize(question);
  const none = (reason) => ({
    isRead: false,
    reason,
    objects: [],
    wantsRecordList: false,
    wantsRanking: false,
  });
  if (!q) return none('empty');

  if (EXECUTION_RE.test(q)) return none('execution_language');
  if (PERSISTENCE_RE.test(q)) return none('objective_persistence');
  if (DESK_WORKFLOW_RE.test(q)) return none('desk_workflow');

  // Ranking follow-up after an operational read: answer from operating data
  // (which states that no record-level ranking exists) instead of letting
  // Blueprint synthesize a ranking that was never produced.
  if (
    RANKING_FOLLOW_UP_RE.test(q) &&
    previousTurnWasOperationalRead(opts.session)
  ) {
    return {
      isRead: true,
      reason: 'ranking_follow_up_after_operational_read',
      objects: ['prospects'],
      wantsRecordList: false,
      wantsRanking: true,
      reviewState: opts.session?.context?.lastOperationalRead?.reviewState || null,
      priorityMode: opts.session?.context?.lastOperationalRead?.investigationRanking ? 'investigation' : null,
      previousRanking: opts.session?.context?.lastOperationalRead?.investigationRanking || null,
    };
  }

  const tenantAnchored = TENANT_ANCHOR_RE.test(q);
  const objects = objectsIn(q, OPPORTUNITY_ANCHOR_RE.test(q));
  if (!objects.length) return none('no_tenant_record_object');
  if (!READ_FORM_RE.test(q)) return none('no_read_form');

  // Discovery wins unless the operator explicitly anchors to existing data
  // ("identify the top five … use actual workspace data").
  if (DISCOVERY_RE.test(q) && !tenantAnchored) return none('discovery_language');
  if (/\b(?:new|more|additional|fresh)\s+(?:prospects?|leads?|contacts?|borrowers?|companies|accounts|opportunit(?:y|ies))\b/i.test(q)) {
    return none('discovery_language');
  }

  const stateAnchored = STATE_ANCHOR_RE.test(q);
  if (!tenantAnchored && !stateAnchored) return none('no_tenant_anchor');

  // Strategy / improvement questions about an object are advisory unless they
  // also ask about existing state or records ("What's our pipeline strategy?").
  if (
    /\b(?:strategy|strategies|approach|positioning|messaging|pricing|ideal|icp|improve|grow|increase|boost|better|tips|ideas|best\s+way|how\s+(?:should|can|do|could)\s+(?:we|i))\b/i.test(q) &&
    !stateAnchored &&
    !RECORD_LIST_RE.test(q)
  ) {
    return none('advisory_strategy');
  }

  // Advisory "should we" without any existing-record anchor stays advisory.
  if (
    /\b(?:should\s+(?:we|i)|would\s+you\s+recommend|do\s+you\s+recommend)\b/i.test(q) &&
    !tenantAnchored && !(stateAnchored && RANKING_RE.test(q))
  ) {
    return none('advisory_without_records');
  }

  return {
    isRead: true,
    reason: 'operational_read',
    objects,
    cursor: q.match(/\bafter\s+record\s+id\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b/i)?.[1] || null,
    reviewState: q.match(/\b(contact_now|qualify_first|hold|reject)\b/i)?.[1]?.toLowerCase() || null,
    topCount: explicitTopCount(q),
    priorityMode: /\boutreach priority\b|\brank.*outreach[- ]eligible/i.test(q) ? 'outreach' :
      /\binvestigat|\banalyt|\bfinanc|\bfunded transaction|\bdeal readiness|\bsecurity position/i.test(q) ? 'investigation' : null,
    wantsRecordList: RECORD_LIST_RE.test(q),
    wantsRanking: RANKING_RE.test(q),
  };
}

function explicitTopCount(q) {
  const match = q.match(/\b(?:top|best)\s+(\d+|one|two|three|four|five|six|ten)\b/i) ||
    q.match(/\b(\d+|one|two|three|four|five|six|ten)\s+highest[- ]priority\b/i);
  if (!match) return null;
  const words = { one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, ten: 10 };
  return Math.max(1, Number(words[match[1].toLowerCase()] || match[1]));
}

function isOperationalReadRequest(question, opts = {}) {
  return classifyOperationalRead(question, opts).isRead;
}

/**
 * Rollback switch. Default on; set MAX_OPERATIONAL_READ_PRECEDENCE=false (or 0/off)
 * to restore pre-#1110 routing without a redeploy of code.
 */
function operationalReadPrecedenceEnabled(env = process.env) {
  const raw = String((env && env.MAX_OPERATIONAL_READ_PRECEDENCE) || '').trim().toLowerCase();
  return !['false', '0', 'off', 'no', 'disabled'].includes(raw);
}

/**
 * Rollback switch for the focused operational-read evidence presentation.
 * Default on; MAX_OPERATIONAL_READ_FOCUSED_EVIDENCE=false restores the full
 * legacy operating-evidence composition for operational reads (routing is
 * unaffected — that is MAX_OPERATIONAL_READ_PRECEDENCE).
 */
function operationalReadFocusedEvidenceEnabled(env = process.env) {
  const raw = String((env && env.MAX_OPERATIONAL_READ_FOCUSED_EVIDENCE) || '').trim().toLowerCase();
  return !['false', '0', 'off', 'no', 'disabled'].includes(raw);
}

/**
 * Record that this turn was answered as an operational read so the next turn
 * can resolve ranking follow-ups against it.
 */
function recordOperationalRead(session, read, structured) {
  if (!session || !session.context || typeof session.context !== 'object') return;
  session.context.lastOperationalRead = {
    operatorTurn: currentOperatorTurn(session),
    objects: (read && read.objects) || [],
    wantsRanking: Boolean(read && read.wantsRanking),
    reviewState: read?.reviewState || null,
    investigationRanking: structured?.metadata?.investigationPriority?.ranked ? {
      ...structured.metadata.investigationPriority,
      selectedRecordIds: structured.supportingEvidence.filter(item => item.recordRef).map(item => item.recordId),
      fingerprints: Object.fromEntries(structured.supportingEvidence.filter(item => item.recordRef).map(item => [item.recordId, item.recordFingerprint])),
    } : null,
    rankedRecordIds: (structured?.supportingEvidence || [])
      .filter(item => item.recordRef && item.priority?.position != null)
      .map(item => item.recordId),
    at: new Date().toISOString(),
  };
}

module.exports = {
  classifyOperationalRead,
  isOperationalReadRequest,
  operationalReadPrecedenceEnabled,
  operationalReadFocusedEvidenceEnabled,
  recordOperationalRead,
  currentOperatorTurn,
  previousTurnWasOperationalRead,
};
