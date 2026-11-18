'use strict';

/**
 * Discourse signals for the ConversationLedger's DEGRADED path only
 * (P0-MAX-REASONING-001 PR 3).
 *
 * A model TurnFrame's relationToPrior always takes precedence (see
 * ConversationLedger.relationFor). Without one, the ledger still needs to
 * notice corrections ("No, I meant…"), explicit topic switches/returns and the
 * record scope a correction names (existing vs new). The existing production
 * correction detector (MessageTypeClassifier.CORRECTION_RES) is reused; the
 * few extra markers below live here because widening the production
 * classifier would change routing. Nothing in routing reads this module.
 */

const { CORRECTION_RES } = require('../workspace/MessageTypeClassifier');
const { extractNegatedClauses } = require('../workspace/NegatedDirective');

const LEDGER_CORRECTION_RES = [
  /^\s*(?:no|nope|not quite|not that)[\s,.!-]+(?:i|we)\s+meant\b/i,
  /^\s*(?:i|we)\s+meant\b/i,
  /^\s*(?:sorry|actually)[\s,]+(?:i|we)\s+meant\b/i,
];
const REFINEMENT_RE = /^\s*(?:only|just)\b|\b(?:narrow (?:it|that|those) (?:down )?to)\b/i;
const TOPIC_SWITCH_RE = /\b(?:switch(?:ing)? (?:over )?to|let'?s (?:look at|move (?:on )?to|turn to|talk about)|moving on to|change (?:of )?topic)\b/i;
const RETURN_RE = /\b(?:back to|return(?:ing)? to|go(?:ing)? back to|where were we|pick up where we left off|resume (?:the|our|my) (?:investigation|review|work))\b/i;
const ANAPHOR_RE = /\b(?:those|these|them|that one|this one|which (?:one|of (?:those|them|these))|which (?:has|have|is|are)|of them|the (?:same|first|second|third|last) one)\b/i;
const EXISTING_RE = /\b(?:existing|current|already (?:have|on file)|on file|we (?:already )?have|in (?:the |our )?(?:crm|system|pipeline|database))\b/i;
const NEW_RE = /\b(?:new|fresh|additional|more)\s+(?:prospects?|leads?|ones|records|companies)\b/i;
const NOT_NEW_RE = /\bnot\s+(?:the\s+)?(?:new|fresh|additional)\b/i;
const KIND_HINTS = [[/\b(?:prospects?|leads?|records|borrowers?)\b/i, 'prospect_set'], [/\bmissions?\b/i, 'mission']];

const PROHIBITION_ACTIONS = [
  [/\b(?:send|e-?mail|outreach|contact|reach out|call|text|message)\w*\b/i, 'send'],
  [/\b(?:approv|proceed|go ahead|authori[sz])\w*\b/i, 'approve'],
  [/\bcancel\w*\b/i, 'cancel'],
  [/\b(?:creat|add|import)\w*\b/i, 'create'],
  [/\b(?:run|execut|launch|start|enrich)\w*\b/i, 'run_agent'],
  [/\b(?:chang|modif|updat|edit|writ|touch|delet|mov)\w*\b/i, 'modify'],
];

function extractDiscourse(text) {
  const q = String(text || '');
  const correction = CORRECTION_RES.some((re) => re.test(q)) || LEDGER_CORRECTION_RES.some((re) => re.test(q));
  const returnTo = RETURN_RE.test(q);
  const topicSwitch = !returnTo && TOPIC_SWITCH_RE.test(q);
  let relation = 'none';
  if (correction) relation = 'correction';
  else if (REFINEMENT_RE.test(q)) relation = 'refinement';
  else if (topicSwitch) relation = 'new_topic';
  else if (returnTo || ANAPHOR_RE.test(q)) relation = 'follow_up';
  let scope = null;
  if (EXISTING_RE.test(q)) scope = 'existing';
  else if (NEW_RE.test(q) && !NOT_NEW_RE.test(q)) scope = 'new';
  const kindHint = (KIND_HINTS.find(([re]) => re.test(q)) || [])[1] || null;
  return { relation, scope, topicSwitch, returnTo, kindHint };
}

/** Prohibited action classes from the operator's negated clauses (no text kept). */
function extractProhibitions(text) {
  const out = new Set();
  for (const clause of extractNegatedClauses(String(text || ''))) {
    // "Don't change/touch anything", "do not do anything" — not "Don't send anything".
    if (/\b(?:anything|nothing)\b/i.test(clause) && /\b(?:chang|modif|touch)\w*\b|\bdo\s+(?:anything|nothing)\b/i.test(clause)) { out.add('any_change'); continue; }
    for (const [re, action] of PROHIBITION_ACTIONS) if (re.test(clause)) out.add(action);
  }
  return [...out];
}

module.exports = { extractDiscourse, extractProhibitions };
