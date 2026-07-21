'use strict';

const crypto = require('crypto');

const STRUCTURED_NOTE_DISPOSITIONS = new Set([
  'answered_interested',
  'answered_not_interested',
  'answered_callback',
  'qualified',
  'disqualified',
  'meeting_booked',
  'do_not_call',
]);

const DISPOSITION_CONTRACTS = Object.freeze({
  voicemail: contract('attempt', false, false, 'retry_call', 'unchanged', 'contacted'),
  no_answer: contract('attempt', false, false, 'retry_call', 'unchanged', 'contacted'),
  wrong_number: contract('attempt', false, false, 'find_phone', 'phone_only', 'data_remediation'),
  disconnected: contract('attempt', false, false, 'find_phone', 'phone_only', 'data_remediation'),
  gatekeeper_relayed: contract('connection', true, false, 'callback', 'unchanged', 'follow_up'),
  gatekeeper_blocked: contract('connection', true, false, 'retry_or_disqualify', 'unchanged', 'follow_up'),
  answered_interested: contract('conversation', true, true, 'qualify', 'unchanged', 'interested'),
  answered_not_interested: contract('conversation', true, true, 'nurture_callback', 'unchanged', 'nurture'),
  answered_callback: contract('conversation', true, true, 'callback', 'unchanged', 'callback_requested'),
  incumbent_all_set: contract('conversation', true, true, 'nurture_callback', 'unchanged', 'nurture'),
  qualified: contract('conversation', true, true, 'book_meeting', 'unchanged', 'qualified'),
  disqualified: contract('conversation', true, true, 'none', 'unchanged', 'disqualified'),
  do_not_call: contract('conversation', true, false, 'none', 'global', 'do_not_call'),
  meeting_booked: contract('conversation', true, true, 'closer_handoff', 'unchanged', 'booked'),
});

function contract(activity, connected, decisionMakerConversation, nextAction, suppressionState, lifecycleResult) {
  return Object.freeze({
    activity,
    connected,
    decision_maker_conversation: decisionMakerConversation,
    next_action: nextAction,
    suppression_state: suppressionState,
    lifecycle_result: lifecycleResult,
  });
}

function dispositionContract(disposition) {
  const value = DISPOSITION_CONTRACTS[String(disposition || '')];
  if (!value) throw qualityError(`Unsupported disposition: ${disposition}`, 'INVALID_DISPOSITION');
  return value;
}

function qualityError(message, code, status = 400) {
  const error = new Error(message);
  error.code = code;
  error.status = status;
  return error;
}

function cleanText(value, maxLength) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  return text ? text.slice(0, maxLength) : '';
}

function validateStructuredNotes(disposition, value) {
  if (!STRUCTURED_NOTE_DISPOSITIONS.has(disposition)) return null;
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw qualityError('This disposition requires structured notes', 'STRUCTURED_NOTES_REQUIRED');
  }
  const summary = cleanText(value.summary, 1000);
  const nextStep = cleanText(value.next_step, 500);
  const reason = cleanText(value.reason, 500);
  if (!summary) throw qualityError('Structured notes require an outcome summary', 'STRUCTURED_NOTES_REQUIRED');
  if (disposition === 'answered_callback' && !nextStep) {
    throw qualityError('Callback requests require a documented next step', 'STRUCTURED_NOTES_REQUIRED');
  }
  if (disposition === 'answered_interested' && !nextStep) {
    throw qualityError('Interested outcomes require a documented next step', 'STRUCTURED_NOTES_REQUIRED');
  }
  if (disposition === 'qualified' && !nextStep) {
    throw qualityError('Qualified outcomes require a documented meeting or next step', 'STRUCTURED_NOTES_REQUIRED');
  }
  if (disposition === 'meeting_booked' && !nextStep) {
    throw qualityError('Booked meetings require the meeting details as the next step', 'STRUCTURED_NOTES_REQUIRED');
  }
  if (['answered_not_interested', 'disqualified'].includes(disposition) && !reason) {
    throw qualityError('Disqualified outcomes require a reason', 'STRUCTURED_NOTES_REQUIRED');
  }
  if (disposition === 'do_not_call' && !reason) {
    throw qualityError('Do-not-call outcomes require the verbatim request as the reason', 'STRUCTURED_NOTES_REQUIRED');
  }
  return {
    summary,
    next_step: nextStep || null,
    reason: reason || null,
  };
}

function callbackSla(dueAt, now = new Date(), dueNowMinutes = 15, dueSoonHours = 24) {
  if (!dueAt) return null;
  const due = new Date(dueAt);
  if (Number.isNaN(due.getTime())) return null;
  const deltaMs = due.getTime() - new Date(now).getTime();
  const dueNowMs = dueNowMinutes * 60 * 1000;
  if (deltaMs < -dueNowMs) return 'overdue';
  if (Math.abs(deltaMs) <= dueNowMs) return 'due_now';
  if (deltaMs > dueNowMs && deltaMs <= dueSoonHours * 60 * 60 * 1000) return 'due_soon';
  return 'scheduled';
}

function shouldSampleCall({ clientId, dispositionId, samplePercent = 20 }) {
  const rate = Math.max(0, Math.min(100, Number(samplePercent) || 0));
  if (rate === 0) return false;
  if (rate === 100) return true;
  const digest = crypto.createHash('sha256').update(`${clientId}:${dispositionId}`).digest();
  return digest.readUInt32BE(0) % 100 < rate;
}

function syntheticContactProhibited(prospect) {
  return Boolean(prospect?.is_synthetic || prospect?.do_not_contact);
}

module.exports = {
  DISPOSITION_CONTRACTS,
  STRUCTURED_NOTE_DISPOSITIONS,
  callbackSla,
  dispositionContract,
  shouldSampleCall,
  syntheticContactProhibited,
  validateStructuredNotes,
};
