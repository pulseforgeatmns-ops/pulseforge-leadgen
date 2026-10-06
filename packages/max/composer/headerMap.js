'use strict';

const COLUMN_ALIASES = {
  company: ['company', 'account', 'account name', 'account_name', 'business', 'business name', 'organization'],
  contact: ['contact', 'person', 'contact name', 'contact_name', 'name'],
  notes: ['notes', 'note', 'comment', 'comments', 'update', 'updates'],
  status: ['status', 'stage', 'pipeline status'],
  ao: ['ao', 'owner', 'assigned ao', 'assigned_ao', 'rep'],
  next_step: ['next step', 'next_step', 'next action', 'follow up', 'follow-up'],
  phone: ['phone', 'mobile', 'cell'],
  email: ['email', 'e-mail'],
};

function normalizeHeaderKey(raw) {
  return String(raw || '')
    .trim()
    .toLowerCase()
    .replace(/[_]+/g, ' ')
    .replace(/\s+/g, ' ');
}

function mapRowHeaders(rawRow = {}) {
  const normalized = {};
  const provenance = { columns: {} };
  for (const [key, value] of Object.entries(rawRow)) {
    if (key.startsWith('__')) continue;
    const norm = normalizeHeaderKey(key);
    let canonical = null;
    for (const [field, aliases] of Object.entries(COLUMN_ALIASES)) {
      if (aliases.includes(norm)) {
        canonical = field;
        break;
      }
    }
    const targetKey = canonical || norm.replace(/\s+/g, '_');
    normalized[targetKey] = value;
    provenance.columns[key] = { canonical: canonical || null, rawHeader: key };
  }
  return { values: normalized, provenance };
}

module.exports = {
  mapRowHeaders,
  normalizeHeaderKey,
  COLUMN_ALIASES,
};
