'use strict';

// Phase A2 phone authority. One normalization path for every surface that
// renders or dials a prospect phone number (queue, pipeline, workspace,
// call preparation, outcome views). US/NANP-centric because every current
// tenant operates domestically; anything non-NANP falls back to raw display
// with callable=false rather than guessing a country code.

function digitsOnly(value) {
  return String(value || '').replace(/\D+/g, '');
}

function normalizePhone(raw) {
  const trimmed = String(raw || '').trim();
  if (!trimmed) return null;
  let digits = digitsOnly(trimmed);
  if (digits.length === 11 && digits.startsWith('1')) digits = digits.slice(1);
  if (digits.length !== 10) return null;
  // NANP area code and exchange cannot start with 0 or 1.
  if (/^[01]/.test(digits) || /^[01]/.test(digits.slice(3, 6))) return null;
  return `+1${digits}`;
}

function formatPhoneDisplay(raw) {
  const normalized = normalizePhone(raw);
  if (normalized) {
    const d = normalized.slice(2);
    return `(${d.slice(0, 3)}) ${d.slice(3, 6)}-${d.slice(6)}`;
  }
  const trimmed = String(raw || '').trim();
  return trimmed || null;
}

function telHref(raw) {
  const normalized = normalizePhone(raw);
  if (normalized) return `tel:${normalized}`;
  const digits = digitsOnly(raw);
  return digits ? `tel:${digits}` : null;
}

function describePhone(raw) {
  const value = String(raw || '').trim() || null;
  const normalized = normalizePhone(value);
  return {
    raw: value,
    normalized,
    display: formatPhoneDisplay(value),
    callable: Boolean(normalized || digitsOnly(value).length >= 7),
  };
}

module.exports = { describePhone, digitsOnly, formatPhoneDisplay, normalizePhone, telHref };
