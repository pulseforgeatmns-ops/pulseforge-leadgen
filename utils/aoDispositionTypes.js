'use strict';

const AO_DEAD_REASONS = Object.freeze([
  { value: 'business_not_found', label: 'Business not found' },
  { value: 'closed_or_inactive', label: 'Closed / inactive' },
  { value: 'bad_address', label: 'Bad address' },
  { value: 'duplicate', label: 'Duplicate' },
  { value: 'not_a_fit', label: 'Not a fit' },
  { value: 'wrong_company_or_bad_data', label: 'Wrong company / bad data' },
  { value: 'do_not_contact', label: 'Do not contact' },
  { value: 'other', label: 'Other' },
]);

const AO_DEAD_REASON_VALUES = new Set(AO_DEAD_REASONS.map(r => r.value));

function isValidDeadReason(value) {
  return AO_DEAD_REASON_VALUES.has(String(value || ''));
}

function validateMarkDeadInput({ reason, note }) {
  if (!isValidDeadReason(reason)) {
    return { error: 'Valid dead reason required', status: 400, reasons: AO_DEAD_REASONS };
  }
  const trimmedNote = note != null ? String(note).trim() : '';
  if (reason === 'other' && !trimmedNote) {
    return { error: 'Note required when reason is Other', status: 400 };
  }
  return { reason, note: trimmedNote || null };
}

function isActiveDisposition(status) {
  return String(status || 'active') !== 'dead';
}

module.exports = {
  AO_DEAD_REASONS,
  AO_DEAD_REASON_VALUES,
  isValidDeadReason,
  validateMarkDeadInput,
  isActiveDisposition,
};
