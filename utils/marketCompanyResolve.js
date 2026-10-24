'use strict';

const MARKETING_SUBDOMAIN_PREFIXES = new Set([
  'mail', 'email', 'e', 'em', 'm', 'go', 'info', 'news', 'updates',
  'notifications', 'newsletter', 'marketing', 'reply', 'bounces',
  'track', 'click', 'links', 'cdn', 'static', 'www',
]);

const MULTI_PART_TLDS = new Set([
  'co.uk', 'com.au', 'co.nz', 'com.br', 'co.jp', 'com.mx', 'co.in',
  'com.sg', 'co.za', 'com.hk', 'org.uk', 'net.au',
]);

function normalizeDomain(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return null;
  try {
    return new URL(raw.includes('://') ? raw : `https://${raw}`)
      .hostname
      .replace(/^www\./i, '')
      .toLowerCase() || null;
  } catch {
    const domain = raw
      .replace(/^https?:\/\//i, '')
      .replace(/^www\./i, '')
      .split(/[/?#\s]/)[0]
      .replace(/[.,;:]+$/g, '')
      .toLowerCase();
    return domain || null;
  }
}

function normalizeEmailAddress(value) {
  const text = String(value || '').trim().toLowerCase();
  const emailMatch = text.match(/<\s*([^>]+)\s*>/) || text.match(/[\w.+-]+@[\w-]+(?:\.[\w-]+)+/);
  return (emailMatch?.[1] || emailMatch?.[0] || text).replace(/^<|>$/g, '').trim().toLowerCase();
}

function emailDomain(email) {
  const normalized = normalizeEmailAddress(email);
  const parts = normalized.split('@');
  return parts.length === 2 ? normalizeDomain(parts[1]) : null;
}

function registrableLabels(domain) {
  const parts = String(domain || '').toLowerCase().split('.').filter(Boolean);
  if (parts.length < 2) return parts;

  const lastTwo = parts.slice(-2).join('.');
  if (MULTI_PART_TLDS.has(lastTwo) && parts.length >= 3) {
    return parts.slice(0, -2);
  }
  return parts.slice(0, -1);
}

function companyNameFromDomain(domain) {
  const normalized = normalizeDomain(domain);
  if (!normalized) return null;

  let labels = registrableLabels(normalized);
  while (labels.length > 1 && MARKETING_SUBDOMAIN_PREFIXES.has(labels[0])) {
    labels = labels.slice(1);
  }

  const raw = labels[labels.length - 1] || normalized.split('.')[0];
  if (!raw) return null;
  return raw.charAt(0).toUpperCase() + raw.slice(1);
}

/**
 * Resolve a sender into a market-intel company key.
 * Never throws — unresolved senders become Unknown Company.
 */
function resolveMarketCompany({ fromEmail, fromDomain } = {}) {
  const domain = normalizeDomain(fromDomain) || emailDomain(fromEmail);
  if (!domain) {
    return { domain: null, name: 'Unknown Company', isUnknown: true };
  }
  const name = companyNameFromDomain(domain);
  if (!name) {
    return { domain: null, name: 'Unknown Company', isUnknown: true };
  }
  return { domain, name, isUnknown: false };
}

module.exports = {
  MARKETING_SUBDOMAIN_PREFIXES,
  companyNameFromDomain,
  emailDomain,
  normalizeDomain,
  normalizeEmailAddress,
  resolveMarketCompany,
};
