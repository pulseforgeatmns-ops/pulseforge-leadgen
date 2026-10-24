'use strict';

const COLUMN_ALIASES = {
  company: ['company', 'account', 'account name', 'account_name', 'business', 'business name', 'organization', 'prospect'],
  contact: ['contact', 'person', 'contact name', 'name of contact', 'contact_name', 'name', 'decision maker', 'poc'],
  notes: ['notes', 'note', 'comment', 'comments', 'update', 'updates', 'details'],
  status: ['status', 'stage', 'pipeline status'],
  ao: ['ao', 'owner', 'assigned ao', 'assigned_ao', 'rep'],
  next_step: ['next step', 'next_step', 'next action', 'follow up', 'follow-up'],
  phone: ['phone', 'phone #', 'phone number', 'telephone', 'mobile', 'cell'],
  email: ['email', 'e-mail'],
  address: ['address', 'business address', 'street address'],
  website: ['website', 'web site', 'url'],
  first_call_date: ['first call date', 'date of 1st phone call', 'date of first phone call'],
  follow_up_call_date: ['follow up call date', 'date of follow-up call', 'date of follow up call'],
  first_visit_date: ['first visit date', 'date of 1st in-person visit', 'date of first in-person visit'],
  provider: ['provider', 'building management /housekeeping company', 'building management / housekeeping company', 'building management', 'housekeeping company'],
  follow_up_needed: ['follow up needed', 'follow-up needed?', 'follow up needed?', 'follow-up needed'],
};

function normalizeHeaderKey(raw) {
  return String(raw || '')
    .trim()
    .toLowerCase()
    .replace(/[_]+/g, ' ')
    .replace(/\s+/g, ' ');
}

function canonicalHeader(raw) {
  const key = normalizeHeaderKey(raw);
  return Object.entries(COLUMN_ALIASES).find(([, aliases]) => aliases.some(alias => normalizeHeaderKey(alias) === key))?.[0] || null;
}

function mapRowHeaders(rawRow = {}, sourceColumns = {}) {
  const normalized = Object.create(null);
  const provenance = { columns: Object.create(null) };
  for (const [key, value] of Object.entries(rawRow)) {
    const norm = normalizeHeaderKey(key);
    const canonical = canonicalHeader(key);
    const targetKey = canonical || norm.replace(/\s+/g, '_');
    if (Object.prototype.hasOwnProperty.call(normalized, targetKey)) {
      (provenance.conflicts ||= []).push({ field: targetKey, headers: [Object.keys(provenance.columns).find(header => (canonicalHeader(header) || normalizeHeaderKey(header).replace(/\s+/g, '_')) === targetKey), key].filter(Boolean), value });
      // Conflicting aliases must never silently replace an earlier column.
      normalized[`${targetKey}__${Object.keys(provenance.columns).length + 1}`] = value;
    } else normalized[targetKey] = value;
    provenance.columns[key] = { canonical, rawHeader: key, ...sourceColumns[key] };
  }
  return { values: normalized, provenance };
}

module.exports = {
  mapRowHeaders,
  normalizeHeaderKey,
  COLUMN_ALIASES,
  canonicalHeader,
};
