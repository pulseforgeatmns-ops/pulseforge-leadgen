'use strict';

/**
 * Shared routing patterns for workspace subject and ownership resolution.
 * Kept separate to avoid circular imports between ConversationSubject and
 * WorkspaceOwnershipResolver.
 */

/** Mission Engine keywords — bind immediately when not superseded by Blueprint topics. */
const MISSION_KEYWORD_RE =
  /\b(acquire|mission|operate|continue|resume|execute|progress|stage|blocker|approve|discovery|prioritization|outreach|execution|learning)\b/i;

const BLUEPRINT_TOPIC_RE =
  /\b(?:icp|ideal customer(?:s| profile)?|target customer|who (?:do|should) we (?:target|serve)|our goals?|(?:business )?objectives?|growth focus|pricing|price point|how much (?:do|should) we charge|our services?|what we (?:do|offer)|offerings?|positioning|brand voice|differentiation|value prop(?:osition)?)\b/i;

const LOCKED_CONVERSATION_SUBJECTS = Object.freeze([
  'identity',
  'reflection',
  'conversation',
  'knowledge',
]);

function normalizeSubjectValue(subject) {
  if (subject === 'reasoning') return 'reflection';
  return subject;
}

function isSubjectOwnerLocked(subjectResult) {
  if (!subjectResult) return false;
  const subject = normalizeSubjectValue(subjectResult.subject);
  return Boolean(subjectResult.locked && LOCKED_CONVERSATION_SUBJECTS.includes(subject));
}

function blocksBusinessSubsystemClaim(subjectResult) {
  return isSubjectOwnerLocked(subjectResult);
}

module.exports = {
  MISSION_KEYWORD_RE,
  BLUEPRINT_TOPIC_RE,
  LOCKED_CONVERSATION_SUBJECTS,
  normalizeSubjectValue,
  isSubjectOwnerLocked,
  blocksBusinessSubsystemClaim,
};
