const dns = require('dns').promises;

const ROLE_PREFIXES = [
  'info', 'admin', 'contact', 'support', 'noreply', 'no-reply',
  'hello', 'postmaster', 'webmaster', 'sales', 'marketing', 'team', 'office',
];

const mxCache = new Map();

function extractDomain(email) {
  if (typeof email !== 'string') return null;
  const parts = email.trim().toLowerCase().split('@');
  if (parts.length !== 2 || !parts[1]) return null;
  return parts[1];
}

function clearMxCache() {
  mxCache.clear();
}

async function validateMxRecord(email) {
  const domain = extractDomain(email);
  if (!domain) return false;

  if (mxCache.has(domain)) return mxCache.get(domain);

  let hasMx = false;
  try {
    const records = await dns.resolveMx(domain);
    hasMx = Array.isArray(records) && records.length > 0;
  } catch {
    hasMx = false;
  }

  mxCache.set(domain, hasMx);
  return hasMx;
}

function isRolePattern(email) {
  if (typeof email !== 'string' || !email.includes('@')) return false;
  const local = email.trim().toLowerCase().split('@')[0];
  return ROLE_PREFIXES.some(prefix => local === prefix || local.startsWith(`${prefix}+`) || local.startsWith(`${prefix}.`));
}

async function validateEmail(email) {
  if (typeof email !== 'string' || !email.trim() || !email.includes('@')) {
    return { valid: false, reason: 'invalid_format', isRole: false };
  }

  const isRole = isRolePattern(email);
  const hasMx = await validateMxRecord(email);

  if (!hasMx) {
    return { valid: false, reason: 'no_mx_record', isRole };
  }

  return {
    valid: true,
    reason: isRole ? 'role_pattern' : 'mx_ok',
    isRole,
  };
}

module.exports = {
  validateMxRecord,
  isRolePattern,
  validateEmail,
  clearMxCache,
  extractDomain,
};
