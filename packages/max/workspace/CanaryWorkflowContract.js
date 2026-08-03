'use strict';

/**
 * Preparation-only canary workflow contracts.
 *
 * Parameterizes readiness fields, verification gates, outbound verbs, review
 * artifact labels, and safety language so desk canaries are not hardcoded to
 * direct-mail concepts.
 */

const CANARY_WORKFLOW_TYPES = Object.freeze({
  DIRECT_MAIL: 'direct_mail_canary',
  CALL_PREP: 'call_prep_canary',
});

const PROSPECT_ID_LINE_RE = /^([A-Za-z]{1,4}-\d{3})\s*:\s*(.+)$/;

/** @type {Readonly<Record<string, object>>} */
const CANARY_WORKFLOW_CONTRACTS = Object.freeze({
  [CANARY_WORKFLOW_TYPES.DIRECT_MAIL]: Object.freeze({
    type: CANARY_WORKFLOW_TYPES.DIRECT_MAIL,
    label: 'direct-mail',
    statusLabel: 'preparation-only canary',
    prospectIdPrefix: 'PM',
    primaryReadinessField: 'mail_readiness',
    draftReadinessField: 'draft_readiness',
    verificationGates: Object.freeze([
      'website_status',
      'mailing_address_status',
      'phone_status',
      'contact_role_status',
    ]),
    gateLabels: Object.freeze({
      website_status: 'website',
      mailing_address_status: 'address',
      phone_status: 'phone',
      contact_role_status: 'contact role',
    }),
    blockedOutboundVerbs: Object.freeze([
      'launch',
      'execution',
      'approval',
      'print',
      'mail',
    ]),
    maxMustNotOutboundLine: 'launch, execute, approve, print, or mail',
    reviewArtifactName: 'packet',
    reviewWorkOrderLabel: 'packet-content review',
    reviewWorkOrderShort: 'packet-content review',
    verificationWorkOrderLabel: 'verification work',
    verificationDeferredLabel: 'verification',
    draftArtifactLabel: 'provisional letter / handwritten note / scorecard cover',
    draftAllowedPhrase: 'provisional drafting',
    blockedFromLabel: 'printing/mailing',
    finalApprovalGateLine:
      'No outbound action can happen until the operator explicitly approves launch/mail in a future step. Packet-content review is not mail approval.',
    futureEligibilityLabel: 'Future mailing eligibility',
    futureEligibilityItems: Object.freeze([
      'packet-content review completed',
      'readiness remains complete at send time',
      'operator gives separate explicit launch/mail approval',
    ]),
    suggestionReviewChip: (prospectId) =>
      `Create a preparation-only packet review checklist for ${prospectId}.`,
    suggestionSummaryChip:
      'Summarize the Campaign 001 preparation-only canary status.',
    suggestionBlockedChip: 'Show what still blocks mailing.',
    safetyLine:
      'Preparation-only. No mission created. No launch, execution, approval, print, or mail.',
    readinessTableColumns: Object.freeze([
      'prospect_id',
      'company_name',
      'contact_name',
      'mail_readiness',
      'draft_readiness',
      'execution_readiness',
      'gate_summary',
    ]),
  }),
  [CANARY_WORKFLOW_TYPES.CALL_PREP]: Object.freeze({
    type: CANARY_WORKFLOW_TYPES.CALL_PREP,
    label: 'call-prep',
    statusLabel: 'call-prep canary',
    prospectIdPrefix: 'CP',
    primaryReadinessField: 'call_readiness',
    draftReadinessField: 'script_readiness',
    verificationGates: Object.freeze([
      'phone_status',
      'contact_role_status',
    ]),
    gateLabels: Object.freeze({
      phone_status: 'phone',
      contact_role_status: 'contact role',
    }),
    blockedOutboundVerbs: Object.freeze([
      'launch',
      'execution',
      'approval',
      'dial',
      'call',
      'text',
      'email',
    ]),
    maxMustNotOutboundLine: 'launch, execute, approve, dial, call, text, or email',
    reviewArtifactName: 'call-script',
    reviewWorkOrderLabel: 'call-script review',
    reviewWorkOrderShort: 'call-script review',
    verificationWorkOrderLabel: 'phone/contact-role verification',
    verificationDeferredLabel: 'phone/contact-role verification',
    draftArtifactLabel: 'provisional call script',
    draftAllowedPhrase: 'provisional script drafting',
    blockedFromLabel: 'dialing/calling/texting/emailing',
    finalApprovalGateLine:
      'No outbound action can happen until the operator explicitly approves dial/call in a future step. Call-script review is not call approval.',
    futureEligibilityLabel: 'Future call eligibility',
    futureEligibilityItems: Object.freeze([
      'call-script review completed',
      'call_readiness remains ready_for_review at dial time',
      'operator gives separate explicit dial/call approval',
    ]),
    suggestionReviewChip: (prospectId) =>
      `Create a preparation-only call-script review checklist for ${prospectId}.`,
    suggestionSummaryChip:
      'Summarize the Campaign 001 call-prep canary status.',
    suggestionBlockedChip: 'Show what still blocks dialing.',
    safetyLine:
      'Preparation-only. No mission created. No launch, execution, approval, dial, call, text, or email.',
    readinessTableColumns: Object.freeze([
      'prospect_id',
      'company_name',
      'contact_name',
      'call_readiness',
      'script_readiness',
      'execution_readiness',
      'gate_summary',
    ]),
  }),
});

/**
 * @param {string|null|undefined} value
 * @returns {boolean}
 */
function isReadyForReviewValue(value) {
  return /^ready(?:_for_review)?$/i.test(String(value || '').trim());
}

/**
 * @param {string|null|undefined} value
 * @returns {boolean}
 */
function isDraftAllowedValue(value) {
  return /^allowed$/i.test(String(value || '').trim());
}

/**
 * @param {string} line
 * @returns {{ prospectId: string, rest: string }|null}
 */
function matchCanaryStateLineProspect(line) {
  const cleaned = String(line || '')
    .replace(/^[-*•]\s*/, '')
    .trim();
  const match = PROSPECT_ID_LINE_RE.exec(cleaned);
  if (!match) return null;
  return {
    prospectId: String(match[1] || '')
      .trim()
      .toUpperCase(),
    rest: String(match[2] || '').trim(),
  };
}

/**
 * @param {string} prospectId
 * @returns {string|null}
 */
function prospectIdPrefix(prospectId) {
  const id = String(prospectId || '')
    .trim()
    .toUpperCase();
  const m = /^([A-Z]{1,4})-\d{3}$/.exec(id);
  return m ? m[1] : null;
}

/**
 * @param {object|null|undefined} row
 * @returns {boolean}
 */
function rowLooksLikeCallPrep(row) {
  if (!row || typeof row !== 'object') return false;
  if (Object.prototype.hasOwnProperty.call(row, 'call_readiness')) return true;
  if (Object.prototype.hasOwnProperty.call(row, 'script_readiness')) return true;
  const prefix = prospectIdPrefix(row.prospect_id);
  if (prefix === 'CP') return true;
  return false;
}

/**
 * @param {object|null|undefined} row
 * @returns {boolean}
 */
function rowLooksLikeDirectMail(row) {
  if (!row || typeof row !== 'object') return false;
  if (Object.prototype.hasOwnProperty.call(row, 'mail_readiness')) return true;
  if (Object.prototype.hasOwnProperty.call(row, 'draft_readiness')) {
    // draft_readiness alone is ambiguous; prefer mail when present elsewhere.
    if (Object.prototype.hasOwnProperty.call(row, 'mail_readiness')) return true;
  }
  const prefix = prospectIdPrefix(row.prospect_id);
  if (prefix === 'PM') return true;
  return false;
}

/**
 * Detect preparation-only canary workflow type from text, rows, and prior desk.
 * @param {string} [text]
 * @param {object[]} [rows]
 * @param {object|null} [prior]
 * @returns {string}
 */
function resolveCanaryWorkflowType(text = '', rows = [], prior = null) {
  const lower = String(text || '').toLowerCase();
  const list = Array.isArray(rows) ? rows.filter(Boolean) : [];

  if (
    /\bcall[-\s]?prep(?:aration)?\b/.test(lower) ||
    /\bcall[-\s]?prep\s+canary\b/.test(lower) ||
    /\bcall[-\s]?script\b/.test(lower) ||
    /\bcall_readiness\b/.test(lower) ||
    /\bscript_readiness\b/.test(lower)
  ) {
    return CANARY_WORKFLOW_TYPES.CALL_PREP;
  }

  if (
    /\bdirect[-\s]?mail\b/.test(lower) ||
    /\bmail_readiness\b/.test(lower) ||
    /\bdraft_readiness\b/.test(lower) ||
    /\bpacket(?:-content)?\s+review\b/.test(lower)
  ) {
    // Prefer call_prep when CP rows / call fields are also present.
    if (list.some(rowLooksLikeCallPrep)) {
      return CANARY_WORKFLOW_TYPES.CALL_PREP;
    }
    return CANARY_WORKFLOW_TYPES.DIRECT_MAIL;
  }

  if (list.some(rowLooksLikeCallPrep)) {
    return CANARY_WORKFLOW_TYPES.CALL_PREP;
  }
  if (list.some(rowLooksLikeDirectMail)) {
    return CANARY_WORKFLOW_TYPES.DIRECT_MAIL;
  }

  if (/\bCP-\d{3}\b/i.test(String(text || ''))) {
    return CANARY_WORKFLOW_TYPES.CALL_PREP;
  }
  if (/\bPM-\d{3}\b/i.test(String(text || ''))) {
    return CANARY_WORKFLOW_TYPES.DIRECT_MAIL;
  }

  const priorType =
    prior &&
    (prior.canaryWorkflowType ||
      (prior.constraints && prior.constraints.canaryWorkflowType) ||
      null);
  if (
    priorType &&
    Object.prototype.hasOwnProperty.call(CANARY_WORKFLOW_CONTRACTS, priorType)
  ) {
    return String(priorType);
  }

  return CANARY_WORKFLOW_TYPES.DIRECT_MAIL;
}

/**
 * @param {string} [type]
 * @returns {object}
 */
function getCanaryWorkflowContract(type) {
  const key = String(type || CANARY_WORKFLOW_TYPES.DIRECT_MAIL);
  return (
    CANARY_WORKFLOW_CONTRACTS[key] ||
    CANARY_WORKFLOW_CONTRACTS[CANARY_WORKFLOW_TYPES.DIRECT_MAIL]
  );
}

/**
 * Resolve contract from operator text / rows / prior desk memory.
 * @param {{ question?: string, rows?: object[], prior?: object|null }} [input]
 * @returns {object}
 */
function resolveCanaryWorkflowContract(input = {}) {
  const type = resolveCanaryWorkflowType(
    input.question || '',
    input.rows || [],
    input.prior || null
  );
  return getCanaryWorkflowContract(type);
}

/**
 * Format blocked outbound verbs for "What Max must not do".
 * @param {object} contract
 * @returns {string}
 */
function formatBlockedOutboundVerbList(contract) {
  const verbs = (contract && contract.blockedOutboundVerbs) || [];
  if (!verbs.length) return 'launch or execute';
  if (verbs.length === 1) return verbs[0];
  if (verbs.length === 2) return `${verbs[0]} or ${verbs[1]}`;
  return `${verbs.slice(0, -1).join(', ')}, or ${verbs[verbs.length - 1]}`;
}

/**
 * Safety line for the workflow (never invent print/mail for call-prep).
 * @param {object} contract
 * @returns {string}
 */
function formatCanarySafetyLine(contract) {
  if (contract && contract.safetyLine) return String(contract.safetyLine);
  return CANARY_WORKFLOW_CONTRACTS[CANARY_WORKFLOW_TYPES.DIRECT_MAIL].safetyLine;
}

module.exports = {
  CANARY_WORKFLOW_TYPES,
  CANARY_WORKFLOW_CONTRACTS,
  PROSPECT_ID_LINE_RE,
  isReadyForReviewValue,
  isDraftAllowedValue,
  matchCanaryStateLineProspect,
  prospectIdPrefix,
  rowLooksLikeCallPrep,
  rowLooksLikeDirectMail,
  resolveCanaryWorkflowType,
  getCanaryWorkflowContract,
  resolveCanaryWorkflowContract,
  formatBlockedOutboundVerbList,
  formatCanarySafetyLine,
};
