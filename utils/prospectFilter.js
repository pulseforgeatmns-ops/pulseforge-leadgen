const pool = require('../db');

const CACHE_TTL_MS = 10 * 60 * 1000;

const PERSONAL_EMAIL_PROVIDERS = new Set([
  'gmail.com',
  'yahoo.com',
  'hotmail.com',
  'outlook.com',
  'aol.com',
  'icloud.com',
  'live.com',
  'msn.com',
  'comcast.net',
  'verizon.net',
  'att.net',
  'sbcglobal.net',
  'cox.net',
  'charter.net',
  'bellsouth.net',
  'me.com',
  'mac.com',
  'ymail.com',
  'rocketmail.com',
  'proton.me',
  'protonmail.com',
  'tutanota.com',
  'gmx.com',
  'fastmail.com',
]);

const MULTI_PART_SUFFIXES = new Set([
  'ac.uk',
  'co.uk',
  'gov.uk',
  'ltd.uk',
  'me.uk',
  'net.uk',
  'nhs.uk',
  'org.uk',
  'plc.uk',
  'police.uk',
  'sch.uk',
  'com.au',
  'net.au',
  'org.au',
  'edu.au',
  'gov.au',
  'co.nz',
  'org.nz',
  'net.nz',
  'govt.nz',
  'co.jp',
  'ne.jp',
  'or.jp',
  'com.br',
  'com.mx',
  'com.ar',
  'com.cn',
  'com.sg',
  'com.my',
  'com.ph',
  'com.tr',
  'co.in',
]);

let cachedLists = null;
let cacheLoadedAt = 0;
let cacheLoadPromise = null;

function normalizeHost(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;

  try {
    return new URL(raw.includes('://') ? raw : `https://${raw}`)
      .hostname
      .replace(/\.+$/g, '')
      .replace(/^www\./i, '')
      .toLowerCase();
  } catch {
    const host = raw
      .replace(/^https?:\/\//i, '')
      .replace(/^www\./i, '')
      .split(/[/?#\s]/)[0]
      .replace(/[.,;:]+$/g, '')
      .toLowerCase();
    return host || null;
  }
}

function normalizeRootDomain(value) {
  const host = normalizeHost(value);
  if (!host) return null;

  const parts = host.split('.').filter(Boolean);
  if (parts.length <= 2) return host;

  const suffix = parts.slice(-2).join('.');
  if (MULTI_PART_SUFFIXES.has(suffix) && parts.length >= 3) {
    return parts.slice(-3).join('.');
  }

  return parts.slice(-2).join('.');
}

function extractEmailDomain(email) {
  const value = String(email || '').trim().toLowerCase();
  const parts = value.split('@');
  if (parts.length !== 2) return null;
  return normalizeHost(parts[1]);
}

function extractTld(domain) {
  const host = normalizeHost(domain);
  if (!host) return null;
  const parts = host.split('.').filter(Boolean);
  return parts.length ? parts[parts.length - 1] : null;
}

function normalizeTld(value) {
  return String(value || '')
    .trim()
    .replace(/^\./, '')
    .toLowerCase();
}

function normalizeEmail(email) {
  return String(email || '').trim().toLowerCase();
}

function hasValidEmailSyntax(email) {
  const value = normalizeEmail(email);
  if (!value) return false;
  if ((value.match(/@/g) || []).length !== 1) return false;

  const [local, domain] = value.split('@');
  if (!local || !domain) return false;
  if (/\s/.test(value)) return false;
  if (!/^[^\s@<>()[\],;:"']+$/.test(local)) return false;
  if (!/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$/.test(domain)) {
    return false;
  }
  return /\.[a-z]{2,}$/.test(domain);
}

function compilePattern(row) {
  const rawPattern = typeof row === 'string' ? row : row.pattern;
  const pattern = String(rawPattern || '').trim();
  if (!pattern) return null;

  let regex;
  const slashMatch = pattern.match(/^\/(.+)\/([a-z]*)$/i);
  if (slashMatch) {
    const baseFlags = slashMatch[2].replace(/[gy]/g, '');
    const flags = baseFlags.includes('i') ? baseFlags : `${baseFlags}i`;
    regex = new RegExp(slashMatch[1], flags);
  } else {
    regex = new RegExp(pattern, 'i');
  }

  return {
    pattern,
    description: typeof row === 'string' ? null : row.description || null,
    regex,
  };
}

function buildExclusionLists({ domains = [], tlds = [], patterns = [] }) {
  const excludedDomains = new Map();
  for (const row of domains) {
    const rawDomain = typeof row === 'string' ? row : row.domain;
    const domain = normalizeHost(rawDomain);
    if (!domain) continue;
    excludedDomains.set(domain, typeof row === 'string' ? 'domain_block' : row.reason || 'domain_block');
  }

  const excludedTlds = new Set();
  for (const row of tlds) {
    const rawTld = typeof row === 'string' ? row : row.tld;
    const tld = normalizeTld(rawTld);
    if (tld) excludedTlds.add(tld);
  }

  const emailPatterns = [];
  for (const row of patterns) {
    try {
      const compiled = compilePattern(row);
      if (compiled) emailPatterns.push(compiled);
    } catch (err) {
      console.warn(`[prospectFilter] Skipping invalid email pattern: ${String(typeof row === 'string' ? row : row.pattern || '')}`);
    }
  }

  return { excludedDomains, excludedTlds, emailPatterns };
}

async function loadExclusionLists(options = {}) {
  const force = options.force === true;
  const now = Date.now();

  if (!force && cachedLists && now - cacheLoadedAt < CACHE_TTL_MS) {
    return cachedLists;
  }

  if (!force && cacheLoadPromise) return cacheLoadPromise;

  cacheLoadPromise = (async () => {
    const [domains, tlds, patterns] = await Promise.all([
      pool.query('SELECT domain, reason FROM excluded_domains'),
      pool.query('SELECT tld, reason FROM excluded_tlds'),
      pool.query('SELECT pattern, description FROM excluded_email_patterns'),
    ]);

    cachedLists = buildExclusionLists({
      domains: domains.rows,
      tlds: tlds.rows,
      patterns: patterns.rows,
    });
    cacheLoadedAt = Date.now();
    return cachedLists;
  })().finally(() => {
    cacheLoadPromise = null;
  });

  return cacheLoadPromise;
}

async function shouldExcludeProspect({ email, websiteUrl, source } = {}) {
  const lists = await loadExclusionLists();
  const cleanEmail = normalizeEmail(email);

  if (!hasValidEmailSyntax(cleanEmail)) {
    return {
      excluded: true,
      reason: 'invalid_email',
      detail: { email: cleanEmail || null, source: source || null },
    };
  }

  const emailDomain = extractEmailDomain(cleanEmail);
  const emailRoot = normalizeRootDomain(emailDomain);
  const tld = extractTld(emailDomain);

  if (tld && lists.excludedTlds.has(tld)) {
    return {
      excluded: true,
      reason: 'tld_block',
      detail: { email: cleanEmail, domain: emailDomain, root_domain: emailRoot, tld, source: source || null },
    };
  }

  for (const entry of lists.emailPatterns) {
    entry.regex.lastIndex = 0;
    if (!entry.regex.test(cleanEmail)) continue;
    return {
      excluded: true,
      reason: 'pattern_block',
      detail: {
        email: cleanEmail,
        domain: emailDomain,
        pattern: entry.pattern,
        description: entry.description,
        source: source || null,
      },
    };
  }

  const fullDomainReason = emailDomain ? lists.excludedDomains.get(emailDomain) : null;
  const rootDomainReason = emailRoot ? lists.excludedDomains.get(emailRoot) : null;
  if (fullDomainReason || rootDomainReason) {
    return {
      excluded: true,
      reason: fullDomainReason || rootDomainReason,
      detail: {
        email: cleanEmail,
        domain: emailDomain,
        root_domain: emailRoot,
        matched_domain: fullDomainReason ? emailDomain : emailRoot,
        source: source || null,
      },
    };
  }

  const websiteRoot = normalizeRootDomain(websiteUrl);
  const isPersonalProvider = PERSONAL_EMAIL_PROVIDERS.has(emailDomain) || PERSONAL_EMAIL_PROVIDERS.has(emailRoot);
  if (!isPersonalProvider && websiteUrl && websiteRoot && emailRoot && emailRoot !== websiteRoot) {
    return {
      excluded: true,
      reason: 'domain_mismatch',
      detail: {
        email: cleanEmail,
        domain: emailDomain,
        email_root_domain: emailRoot,
        website_url: websiteUrl,
        website_root_domain: websiteRoot,
        source: source || null,
      },
    };
  }

  return { excluded: false };
}

function __setExclusionListsForTest(lists) {
  cachedLists = buildExclusionLists(lists || {});
  cacheLoadedAt = Date.now();
}

function __clearExclusionCache() {
  cachedLists = null;
  cacheLoadedAt = 0;
  cacheLoadPromise = null;
}

module.exports = {
  loadExclusionLists,
  shouldExcludeProspect,
  extractEmailDomain,
  normalizeRootDomain,
  __clearExclusionCache,
  __setExclusionListsForTest,
};
