'use strict';

/**
 * Presentation for frame-directed reasoning (P0-MAX-REASONING-001 PR 5).
 *
 * The deterministic renderer states only what the ReasoningResult holds, in
 * separate sections: observed facts, calculations, inferences, contradictions,
 * unknowns, recommended next steps, and execution status. An optional
 * rephrasing presenter (e.g. the existing PresentationEngine or a model) may
 * restyle the text, but its output is rejected — and the deterministic text
 * used instead — if it claims an action was completed when nothing executed,
 * or mentions ids or dates that are not in the result.
 */

const { buildStructuredResponse } = require('../workspace/WorkspaceTypes');

// Agentive claims that something was done: "I/we/Max (have) sent…",
// "…have/has been enriched", "…was/were updated by Max / for you".
// Descriptive record facts ("record updated 5 days ago", "3 sent") are not claims of action.
const ACTION_VERBS = '(?:sent|emailed|enriched|updated|approved|launched|started|dispatched|executed|modified|changed|added|created|imported|transferred|scheduled|completed)';
const COMPLETION_RES = [
  new RegExp(`\\b(?:i|we|max)\\s*(?:'ve|'ll have|have|has)?\\s*(?:just\\s+|now\\s+|already\\s+)?${ACTION_VERBS}\\b`, 'i'),
  new RegExp(`\\b(?:have|has)\\s+(?:now\\s+|already\\s+)?been\\s+${ACTION_VERBS}\\b`, 'i'),
  new RegExp(`\\b(?:was|were|is|are)\\s+(?:now\\s+|already\\s+|successfully\\s+)?${ACTION_VERBS}\\s+(?:by max|for you|as requested|successfully)\\b`, 'i'),
  new RegExp(`\\b(?:done|complete)[.!]?\\s*$`, 'i'),
];
const NEGATION_RE = /\b(?:not|no|never|nothing|none|without|wasn'?t|weren'?t|isn'?t|hasn'?t|haven'?t|didn'?t)\b/i;

function section(title, lines) {
  return lines.length ? [`${title}`, ...lines.map((l) => `- ${l}`)].join('\n') : null;
}

const CLARIFY = {
  ambiguous_referent: 'Which one do you mean?',
  which_earlier_subject: 'Which earlier one do you mean?',
  no_current_referent: 'I have no current subject to apply that to. Which records or mission do you mean?',
  nothing_to_correct: 'I am not sure what to correct. What did you mean instead?',
  ambiguous_request: 'I cannot tell safely what you want me to do. Can you confirm exactly what should happen, and when?',
  clarification_required: 'Can you clarify what you mean?',
};

const VERB_LABEL = Object.freeze({
  modify: 'update records', send: 'send outreach', approve: 'approve a decision', reject: 'reject a decision', cancel: 'cancel',
  pause: 'pause a mission', resume: 'resume a mission', restart: 'restart a mission', create: 'create something new', run_agent: 'run an agent', unclear: 'act',
});

function executionLine(e) {
  if (!e || !e.requested) return null;
  if (e.status === 'held_by_operator') return `Held: you asked me not to ${e.requested}. Nothing was executed.`;
  const constraints = e.constraints.length ? ` Your constraints are recorded: no ${e.constraints.join(', no ')}.` : '';
  const path = e.pathway ? ` If you want this done, it goes through ${e.pathway}, with explicit authorization.` : '';
  const what = VERB_LABEL[e.requested] || `"${e.requested}"`;
  const reason = e.reason ? e.reason.charAt(0).toUpperCase() + e.reason.slice(1) : '';
  return `Not executed: I identified a request to ${what} but did not carry it out. ${reason}.${constraints}${path}`.replace(/\.\./g, '.');
}

/** Deterministic text: only what the result holds. */
function renderReasoning(result) {
  if (result.kind === 'refused') {
    return [`Not answered: ${result.refusal === 'foreign_tenant_reference'
      ? 'that refers to another client. I only read and act on the signed-in client\'s records, and nothing was read, transferred or changed.'
      : `the request could not be evaluated (${result.refusal}). Nothing was read or changed.`}`,
    executionLine(result.execution)].filter(Boolean).join('\n\n');
  }
  if (result.kind === 'clarification') {
    const opts = (result.clarification.options || []).map((o) => `${o.label || o.kind} (${o.id})`);
    return [CLARIFY[result.clarification.reason] || CLARIFY.clarification_required,
      opts.length ? `Options: ${opts.join('; ')}.` : null, executionLine(result.execution)].filter(Boolean).join('\n\n');
  }
  const lost = result.revalidation.filter((r) => r.status !== 'current').map((r) => `${r.kind} ${r.id}: ${r.status.replace(/_/g, ' ')}${r.detail ? ` — ${r.detail}` : ''}`);
  const parts = [
    executionLine(result.execution),
    result.referent && result.referent.remembered ? `Referring to ${result.referent.label || result.referent.kind} from this conversation, rechecked against current records.` : null,
    section('Observed (verified from current records)', result.observed.map((o) => `${o.statement}${o.missing.length ? ` Missing: ${o.missing.join(', ')}.` : ''}`)),
    section('Calculated from those observations', result.calculated.map((c) => c.claim)),
    section('Remembered references that could not be confirmed', lost),
    section('Inferred (not verified)', result.inferences.map((i) => `${i.claim} [based on ${i.basis.join(', ')}]`)),
    section('Contradictions (unresolved)', result.contradictions.map((c) => c.description)),
    section('Unknown or unavailable', result.unknowns.map((u) => `${u.what}: ${u.reason}`)),
    section('Recommended next steps (proposals only)', result.recommendations.map((r) => r.action)),
  ].filter(Boolean);
  return parts.length ? parts.join('\n\n') : 'No operational evidence was retrieved for this request.';
}

/** True when text claims a completed action the result does not record. */
function claimsUnrecordedExecution(text, result) {
  if (result.execution && result.execution.executed === true) return false;
  const sentences = String(text || '').split(/(?<=[.!?])\s+|\n/);
  return sentences.some((s) => COMPLETION_RES.some((re) => re.test(s)) && !NEGATION_RE.test(s) && !/\b(?:if|would|could|should|to carry out|goes through|requires|proposal|verify|confirm|before any)\b/i.test(s));
}

/** Ids and ISO dates the result actually contains. */
function groundedTokens(result) {
  const json = JSON.stringify(result);
  return {
    ids: new Set(json.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\b(?:m|mission|prog|pd)_[A-Za-z0-9_-]+/gi) || []),
    dates: new Set(json.match(/\d{4}-\d{2}-\d{2}/g) || []),
  };
}

function introducesUngroundedTokens(text, result) {
  const g = groundedTokens(result);
  const ids = String(text).match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\b(?:m|mission|prog|pd)_[A-Za-z0-9_-]+/gi) || [];
  const dates = String(text).match(/\d{4}-\d{2}-\d{2}/g) || [];
  return ids.some((x) => !g.ids.has(x)) || dates.some((x) => !g.dates.has(x));
}

/**
 * @param {object} result ReasoningResult
 * @param {{ presenter?: (input: { text: string, result: object }) => Promise<string>|string }} [options]
 */
async function presentReasoning(result, { presenter = null } = {}) {
  const deterministic = renderReasoning(result);
  if (claimsUnrecordedExecution(deterministic, result)) throw new Error('reasoning_renderer_claimed_execution');
  let prose = deterministic;
  let presentation = 'deterministic';
  if (typeof presenter === 'function') {
    try {
      const styled = await presenter({ text: deterministic, result });
      if (typeof styled === 'string' && styled.trim() && !claimsUnrecordedExecution(styled, result) && !introducesUngroundedTokens(styled, result)) {
        prose = styled;
        presentation = 'presenter';
      } else presentation = 'presenter_rejected';
    } catch (_) { presentation = 'presenter_failed'; }
  }
  // Record references for the turn's subject, in the shape the ConversationLedger adapter reads.
  const prospectItems = (result.subjectRecordIds || []).map((id) => ({ recordRef: `prospect:${result.tenantId}:${id}`, recordId: id, epistemic: 'verified' }));
  const structured = buildStructuredResponse({
    answer: prose,
    reasoning: ['P0-MAX-REASONING-001 frame-directed reasoning (read-only): frame → remembered referent → current evidence.'],
    supportingEvidence: result.observed.map((o) => ({ id: o.ref, summary: o.statement, epistemic: 'verified', recordRef: o.subject.type === 'prospect_record' ? `prospect:${result.tenantId}:${o.subject.id}` : null, recordId: o.subject.id, retrievalStatus: 'found' })),
    contradictingEvidence: result.contradictions.map((c, i) => ({ id: `contradiction:${i}`, summary: c.description, epistemic: 'contradiction' })),
    nextInvestigations: result.unknowns.map((u) => `${u.what}: ${u.reason}`),
    recommendedActions: [],
    confidence: null,
    metadata: { frameDirectedReasoning: true },
  });
  // Record references in the shape the ConversationLedger adapter reads.
  structured.metadata.items = prospectItems;
  structured.metadata.frameDirectedReasoning = true;
  structured.metadata.executionStatus = result.execution.status;
  return { prose, structured, presentation, result };
}

module.exports = { renderReasoning, presentReasoning, claimsUnrecordedExecution, introducesUngroundedTokens };
