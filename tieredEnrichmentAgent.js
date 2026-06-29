require('dotenv').config();

const pool = require('./db');
const { normalizeClientId } = require('./utils/clientContext');
const { verifyEmail } = require('./utils/emailVerifier');
const { invalidOutreachEmailReason } = require('./utils/emailGuard');
const { ensureTieredEnrichmentSchema } = require('./utils/tieredEnrichmentSchema');

const AGENT_NAME = 'tiered_enrichment';
const DEFAULT_FETCH_DELAY_MS = 750;
const FETCH_TIMEOUT_MS = 10000;
const MAX_WEBSITE_PAGES = 8;
const MAX_PATTERN_CANDIDATES = 5;
const RESOLVING_NAME_CONFIDENCE = 0.7;
const VERIFIED_EMAIL_STATUSES = new Set(['valid', 'verified']);
const GENERIC_EMAIL_PREFIX_RE = /^(?:info|hello|contact|admin|support|sales|office|team|service|customerservice|customer\.?service|mail|inquir(?:y|ies))[\w.+-]*$/i;
const EMAIL_LOCAL_PROFESSIONAL_SUFFIX_RE = /(?:pa|pc|pllc|llc|llp|law|legal|esq|cpa)$/i;
const COMMON_FIRST_NAMES = new Set([
  'aaron', 'abby', 'abbygale', 'alexander', 'amy', 'andrea', 'andrew', 'anna',
  'avery', 'bill', 'bradford', 'brendan', 'brian', 'brianna', 'bryanna',
  'bruce', 'cassandra', 'charles', 'chloe', 'chris', 'christopher', 'coleen',
  'colleen', 'courtney', 'damon', 'david', 'douglas', 'elizabeth', 'emily',
  'eric', 'george', 'gregory', 'james', 'jane', 'jason', 'jennifer', 'jessica',
  'jin', 'john', 'jon', 'jonathan', 'joseph', 'karen', 'katherine', 'kenneth',
  'kimberly', 'lynn', 'madeline', 'margaret', 'mark', 'mary', 'matt', 'megan',
  'melaney', 'michael', 'michaila', 'neill', 'normand', 'patrick', 'paul',
  'peter', 'robert', 'ron', 'ryan', 'scott', 'stephen', 'steven', 'thomas',
  'victor',
]);
const PERSON_SUFFIX_RE = /\b(?:CPA|ESQ|ESQUIRE|ATTORNEY|LAWYER)\b\.?/i;
const FIRM_WORD_RE = /\b(?:law|office|offices|group|firm|pllc|llc|llp|pa|p\.a\.|professional|association|attorney|attorneys|lawyers|cpa|cpas|accounting|associates|partners|injury)\b/i;
const NAME_STOP_WORDS = new Set([
  'about', 'contact', 'email', 'phone', 'street', 'suite', 'manchester', 'hampshire',
  'practice', 'areas', 'legal', 'services', 'office', 'firm', 'law', 'attorney',
  'lawyers', 'group', 'pllc', 'llc', 'llp', 'pa', 'new', 'you', 'your', 'yours',
  'we', 'our', 'ours', 'trust', 'trusted', 'help', 'home', 'free', 'consultation',
  'rights', 'case', 'injury', 'defense', 'award', 'awards', 'association',
  'associations', 'nhaj', 'nhba', 'best', 'top', 'super', 'elected', 'elect',
  'not', 'vehicle', 'vehicles', 'accident', 'accidents', 'team', 'founding',
]);
const ROLE_SCORES = [
  { re: /\b(?:office manager|firm administrator|administrator|practice manager|operations manager)\b/i, score: 100, role: 'office manager' },
  { re: /\b(?:managing partner|managing attorney|principal|owner|founder|president)\b/i, score: 85, role: 'principal' },
  { re: /\b(?:partner|shareholder|member)\b/i, score: 75, role: 'partner' },
  { re: /\b(?:attorney|lawyer|counsel|associate)\b/i, score: 65, role: 'attorney' },
  { re: /\b(?:cpa|accountant|tax manager)\b/i, score: 65, role: 'accountant' },
];

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function clean(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function titleCase(value) {
  return clean(value).toLowerCase().replace(/\b[a-z]/g, c => c.toUpperCase());
}

function normalizeDomain(value) {
  const raw = clean(value);
  if (!raw) return null;
  try {
    return new URL(raw.includes('://') ? raw : `https://${raw}`)
      .hostname
      .replace(/^www\./i, '')
      .toLowerCase();
  } catch {
    return raw
      .replace(/^https?:\/\//i, '')
      .replace(/^www\./i, '')
      .split(/[/?#\s]/)[0]
      .replace(/[.,;:]+$/g, '')
      .toLowerCase() || null;
  }
}

function buildUrl(value, path = '/') {
  const domain = normalizeDomain(value);
  if (!domain) return null;
  return `https://${domain}${path}`;
}

function emailDomain(email) {
  return clean(email).toLowerCase().split('@')[1] || '';
}

function emailLocal(email) {
  return clean(email).toLowerCase().split('@')[0] || '';
}

function isGenericEmail(email) {
  return GENERIC_EMAIL_PREFIX_RE.test(emailLocal(email));
}

function emailMatchesName(email, name) {
  if (!email || !name) return false;
  if (isGenericEmail(email)) return true;
  const local = emailLocal(email).replace(/[^a-z]/g, '');
  const first = clean(name.first_name).toLowerCase().replace(/[^a-z]/g, '');
  const last = clean(name.last_name).toLowerCase().replace(/[^a-z]/g, '');
  if (!local || !first || !last) return false;
  return local === first
    || local === `${first}${last}`
    || local === `${first[0]}${last}`
    || local === `${first}${last[0]}`
    || local.includes(first)
    || local.includes(last);
}

function normalizeNamePart(value) {
  return clean(value).toLowerCase().replace(/[^a-z]/g, '');
}

function deriveNameFromVerifiedEmail(row, candidateNames = []) {
  if (hasResolvingName(row) || !hasResolvingEmail(row)) return null;
  const local = emailLocal(row.email);
  const normalizedLocal = local.replace(/[^a-z]/g, '');
  if (!normalizedLocal || normalizedLocal.length < 3 || /\d/.test(local) || isGenericEmail(row.email)) return null;

  const base = {
    tier: 0,
    source: 'tier0_email_localpart',
    email: clean(row.email).toLowerCase(),
  };

  if (local.includes('.') || local.includes('_') || local.includes('-')) {
    const firstToken = local.split(/[._-]+/).map(normalizeNamePart).find(Boolean);
    if (firstToken && firstToken.length >= 3 && COMMON_FIRST_NAMES.has(firstToken)) {
      return {
        ...base,
        first_name: titleCase(firstToken),
        last_name: null,
        full_name: titleCase(firstToken),
        confidence: 0.9,
        reason: 'delimited_first_name',
      };
    }
    return { ...base, rejected: true, confidence: 0.35, reason: 'delimited_localpart_not_known_first_name' };
  }

  if (EMAIL_LOCAL_PROFESSIONAL_SUFFIX_RE.test(normalizedLocal)) {
    return { ...base, rejected: true, confidence: 0.35, reason: 'professional_or_firm_suffix' };
  }

  const candidates = rankNames(candidateNames || []).filter(candidate => candidate?.first_name && candidate?.last_name);
  for (const candidate of candidates) {
    const first = normalizeNamePart(candidate.first_name);
    const last = normalizeNamePart(candidate.last_name);
    if (first && last && normalizedLocal === `${first[0]}${last}`) {
      return {
        ...base,
        first_name: candidate.first_name,
        last_name: candidate.last_name,
        full_name: `${candidate.first_name} ${candidate.last_name}`,
        confidence: 0.88,
        reason: 'first_initial_last_name_match',
        matched_candidate: candidate.full_name || `${candidate.first_name} ${candidate.last_name}`,
      };
    }
  }

  const existingLast = normalizeNamePart(row.last_name);
  if (existingLast && normalizedLocal === `${normalizedLocal[0]}${existingLast}`) {
    return { ...base, rejected: true, confidence: 0.45, reason: 'first_initial_last_name_without_first_name_candidate' };
  }

  if (COMMON_FIRST_NAMES.has(normalizedLocal)) {
    return {
      ...base,
      first_name: titleCase(normalizedLocal),
      last_name: null,
      full_name: titleCase(normalizedLocal),
      confidence: 0.82,
      reason: 'known_first_name_localpart',
    };
  }

  return { ...base, rejected: true, confidence: 0.35, reason: 'ambiguous_localpart' };
}

function isBouncerConfigured() {
  return String(process.env.BOUNCER_ENABLED || '').toLowerCase() === 'true' && Boolean(process.env.BOUNCER_API_KEY);
}

function isBouncerVerified(rowOrResult) {
  const status = clean(rowOrResult?.email_status || rowOrResult?.status).toLowerCase();
  const method = clean(rowOrResult?.email_verification_method || rowOrResult?.method || rowOrResult?.vendor).toLowerCase();
  return VERIFIED_EMAIL_STATUSES.has(status) && method === 'bouncer';
}

function hasResolvingName(row) {
  return Boolean(clean(row?.first_name));
}

function hasResolvingEmail(row) {
  return Boolean(clean(row?.email)) && isBouncerVerified(row);
}

function passesDataBar(row) {
  return hasResolvingName(row) && hasResolvingEmail(row);
}

function splitPersonName(value) {
  const parts = clean(value)
    .replace(/[^\w\s.'-]/g, ' ')
    .split(/\s+/)
    .map(part => part.replace(/^[.'-]+|[.'-]+$/g, ''))
    .filter(Boolean)
    .filter(part => !/^(?:the|and|of|at)$/i.test(part));
  if (parts.length < 2 || parts.length > 4) return null;
  if (parts.some(part => NAME_STOP_WORDS.has(part.toLowerCase()))) return null;
  if (parts.some(part => part.includes('.') && !/^[A-Z]\.?$/i.test(part))) return null;
  if (parts.some(part => /^[A-Z]{2,}$/.test(part))) return null;
  if (/^[A-Z]\.?$/i.test(parts[parts.length - 1])) return null;
  return {
    first_name: titleCase(parts[0]),
    last_name: titleCase(parts[parts.length - 1]),
    full_name: parts.map(titleCase).join(' '),
  };
}

function nameCandidate(fullName, tier, confidence, source, role = null) {
  const parsed = splitPersonName(fullName);
  if (!parsed) return null;
  return { ...parsed, tier, confidence, source, role };
}

function parseNameFromCompany(companyName) {
  const original = clean(companyName);
  if (!original) return null;
  const hasProfessionSuffix = PERSON_SUFFIX_RE.test(original);
  const hasSoloMarker = /^attorney\s+/i.test(original)
    || /^law offices? of\s+/i.test(original)
    || /,\s*(?:attorney at law|esq\.?|cpa)\b/i.test(original);

  const stripped = original
    .replace(/^law offices? of\s+/i, '')
    .replace(/^attorney\s+/i, '')
    .replace(/,\s*(?:attorney at law|esq\.?|cpa)\b.*$/i, '')
    .replace(/\b(?:law offices?|law office|attorney at law|pllc|llc|llp|p\.a\.|pa|pc)\b/ig, ' ')
    .replace(PERSON_SUFFIX_RE, ' ');

  const reversed = original
    .replace(PERSON_SUFFIX_RE, '')
    .replace(FIRM_WORD_RE, ' ')
    .split(/\s+/)
    .filter(Boolean);
  if (reversed.length >= 2 && reversed.length <= 3 && !/[&,]/.test(original)) {
    const [last, first, middle] = reversed;
    const lastFirstMiddle = reversed.length === 3 && /^[A-Z]\.?$/.test(middle || '');
    if ((hasProfessionSuffix || lastFirstMiddle) && /^[A-Z][a-z'-]+$/.test(last) && /^[A-Z][a-z'-]+$/.test(first)) {
      const full = [first, middle && /^[A-Z]\.?$/.test(middle) ? middle : null, last].filter(Boolean).join(' ');
      const candidate = nameCandidate(full, 0, 0.72, 'company_name_reversed');
      if (candidate) return candidate;
    }
  }

  const direct = nameCandidate(stripped, 0, 0.78, 'company_name_eponymous');
  if (hasSoloMarker && direct && !/[&]/.test(original) && !/\b(?:group|associates|partners)\b/i.test(original)) return direct;

  return null;
}

function parseNamesFromExistingData(row) {
  const candidates = [];
  const companyCandidate = parseNameFromCompany(row.company_name);
  if (companyCandidate) candidates.push(companyCandidate);

  const notes = clean(row.notes);
  const contactMatch = notes.match(/\b(?:contact|owner|attorney|principal|office manager)[:\s-]+([A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,3})/);
  const noteCandidate = contactMatch ? nameCandidate(contactMatch[1], 0, 0.74, 'prospect_notes') : null;
  if (noteCandidate) candidates.push(noteCandidate);

  return rankNames(candidates);
}

function decodeHtml(value) {
  return String(value || '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#039;|&apos;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function htmlToText(html) {
  return decodeHtml(String(html || '')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' '));
}

function extractEmailsFromHtml(html, domain) {
  const normalizedDomain = normalizeDomain(domain);
  const emails = new Set();
  const decoded = decodeHtml(html).replace(/\s*\[at\]\s*|\s*\(at\)\s*/gi, '@').replace(/\s*\[dot\]\s*|\s*\(dot\)\s*/gi, '.');
  for (const match of decoded.matchAll(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi)) {
    const email = match[0].replace(/[).,;:]+$/g, '').toLowerCase();
    if (!invalidOutreachEmailReason(email) && (!normalizedDomain || emailDomain(email) === normalizedDomain)) {
      emails.add(email);
    }
  }
  return [...emails].map(email => ({
    email,
    source: 'website_email',
    tier: 1,
    confidence: GENERIC_EMAIL_PREFIX_RE.test(emailLocal(email)) ? 0.62 : 0.86,
  }));
}

function roleForContext(context) {
  for (const role of ROLE_SCORES) {
    if (role.re.test(context)) return role;
  }
  return null;
}

function extractNamesFromText(text, source) {
  const candidates = [];
  const normalized = clean(text);
  const personPattern = String.raw`([A-Z][A-Za-z.'-]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][A-Za-z.'-]+){1,2})`;
  const rolePattern = String.raw`(?:office manager|firm administrator|administrator|practice manager|operations manager|managing partner|managing attorney|principal|owner|founder|president|partner|shareholder|member|attorney|lawyer|counsel|associate|cpa|accountant|tax manager)`;
  for (const role of ROLE_SCORES) {
    const roleNameRe = new RegExp(String.raw`\b${rolePattern}\s+${personPattern}\b`, 'gi');
    const nameRoleRe = new RegExp(String.raw`${personPattern}\s*(?:,|[-–|])\s*${rolePattern}\b`, 'gi');
    const nameIsRoleRe = new RegExp(String.raw`${personPattern}\s+(?:is|serves as)\s+(?:an?\s+)?${rolePattern}\b`, 'gi');
    for (const re of [roleNameRe, nameRoleRe, nameIsRoleRe]) {
      for (const match of normalized.matchAll(re)) {
        const fullName = match[1] || match[2];
        const contextRole = roleForContext(match[0]) || role;
        if (!role.re.test(match[0]) && contextRole.role !== role.role) continue;
        const candidate = nameCandidate(fullName, 1, Math.min(0.95, contextRole.score / 100), source, contextRole.role);
        if (candidate) candidates.push(candidate);
      }
    }
  }

  const profileCardRe = new RegExp(String.raw`${personPattern}\s+(?:view profile|bio|biography)\b`, 'gi');
  for (const match of normalized.matchAll(profileCardRe)) {
    const candidate = nameCandidate(match[1], 1, 0.7, source, null);
    if (candidate) candidates.push(candidate);
  }

  for (const email of normalized.matchAll(/\b([a-z][a-z]+)\.([a-z][a-z]+)@[a-z0-9.-]+\.[a-z]{2,}\b/gi)) {
    const candidate = nameCandidate(`${email[1]} ${email[2]}`, 1, 0.74, source, null);
    if (candidate) candidates.push(candidate);
  }

  return rankNames(candidates);
}

function rankNames(candidates) {
  const seen = new Map();
  for (const candidate of candidates.filter(candidate => candidate?.first_name && candidate?.last_name)) {
    const key = `${candidate.first_name.toLowerCase()} ${candidate.last_name.toLowerCase()}`;
    const current = seen.get(key);
    if (!current || candidate.confidence > current.confidence) seen.set(key, candidate);
  }
  return [...seen.values()].sort((a, b) => b.confidence - a.confidence);
}

function inferPracticeArea(text, vertical) {
  const lower = clean(text).toLowerCase();
  const signals = [
    ['personal injury', /\bpersonal injury|car accident|auto accident|injury lawyer\b/],
    ['criminal defense', /\bcriminal defense|dui|dwi|criminal law\b/],
    ['family law', /\bfamily law|divorce|custody\b/],
    ['estate planning', /\bestate planning|probate|wills?|trusts?\b/],
    ['business law', /\bbusiness law|corporate|commercial litigation\b/],
    ['real estate law', /\breal estate|landlord|tenant|zoning\b/],
    ['tax and accounting', /\btax planning|tax preparation|bookkeeping|accounting\b/],
  ];
  const match = signals.find(([, re]) => re.test(lower));
  if (match) return match[0];
  if (/law_firm|law firm/i.test(vertical || '')) return 'law firm';
  if (/account/i.test(vertical || '')) return 'accounting';
  return null;
}

function inferFirmSize(text) {
  const lower = clean(text).toLowerCase();
  const attorneyMatch = lower.match(/\b(\d{1,3})\s+(?:attorneys|lawyers|professionals|team members|staff)\b/);
  if (attorneyMatch) return `${attorneyMatch[1]} staff`;
  if (/\bsolo practitioner|sole practitioner|solo attorney\b/.test(lower)) return 'solo';
  if (/\bsmall firm|boutique firm\b/.test(lower)) return 'small firm';
  return null;
}

function detectPattern(email, names) {
  const local = emailLocal(email).replace(/[^a-z.]/g, '');
  for (const name of names) {
    const first = name.first_name.toLowerCase().replace(/[^a-z]/g, '');
    const last = name.last_name.toLowerCase().replace(/[^a-z]/g, '');
    if (!first || !last) continue;
    if (local === first) return 'first';
    if (local === `${first}.${last}`) return 'first.last';
    if (local === `${first}${last}`) return 'firstlast';
    if (local === `${first[0]}${last}`) return 'flast';
    if (local === `${first}.${last[0]}`) return 'first.li';
  }
  return null;
}

function candidateFromPattern(name, domain, pattern) {
  const first = name.first_name.toLowerCase().replace(/[^a-z]/g, '');
  const last = name.last_name.toLowerCase().replace(/[^a-z]/g, '');
  if (!first || !last || !domain) return null;
  const locals = {
    first,
    'first.last': `${first}.${last}`,
    firstlast: `${first}${last}`,
    flast: `${first[0]}${last}`,
    'first.li': `${first}.${last[0]}`,
  };
  return locals[pattern] ? `${locals[pattern]}@${domain}` : null;
}

function buildEmailCandidates({ existingEmail, foundEmails, names, domain }) {
  const candidates = [];
  if (existingEmail && !invalidOutreachEmailReason(existingEmail)) {
    candidates.push({ email: clean(existingEmail).toLowerCase(), tier: 0, source: 'existing_prospect_email', confidence: 0.8 });
  }
  for (const found of foundEmails) candidates.push(found);

  const patterns = foundEmails.map(found => detectPattern(found.email, names)).filter(Boolean);
  const uniquePatterns = [...new Set(patterns)];
  const fallbackPatterns = ['first.last', 'first', 'flast', 'firstlast', 'first.li'];
  const orderedPatterns = [...uniquePatterns, ...fallbackPatterns.filter(pattern => !uniquePatterns.includes(pattern))];
  const bestName = names.find(name => name.confidence >= RESOLVING_NAME_CONFIDENCE);
  if (bestName) {
    for (const pattern of orderedPatterns.slice(0, MAX_PATTERN_CANDIDATES)) {
      const email = candidateFromPattern(bestName, domain, pattern);
      if (email && !invalidOutreachEmailReason(email)) {
        candidates.push({ email, tier: 1, source: `pattern_${pattern}`, confidence: uniquePatterns.includes(pattern) ? 0.72 : 0.55 });
      }
    }
  }

  const seen = new Set();
  return candidates.filter(candidate => {
    const email = candidate.email.toLowerCase();
    if (seen.has(email)) return false;
    seen.add(email);
    return true;
  });
}

async function fetchWithTimeout(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const response = await fetch(url, {
      headers: {
        'user-agent': 'PulseforgeLeadGen/1.0 (+https://gopulseforge.com)',
        accept: 'text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.8',
      },
      redirect: 'follow',
      signal: controller.signal,
    });
    const text = await response.text();
    return { ok: response.ok, status: response.status, url: response.url || url, text };
  } finally {
    clearTimeout(timeout);
  }
}

function parseRobotsDisallow(robotsText) {
  const lines = String(robotsText || '').split(/\r?\n/);
  let active = false;
  const disallow = [];
  for (const line of lines) {
    const cleanLine = line.split('#')[0].trim();
    const ua = cleanLine.match(/^user-agent:\s*(.+)$/i);
    if (ua) {
      active = ua[1].trim() === '*';
      continue;
    }
    const blocked = cleanLine.match(/^disallow:\s*(.*)$/i);
    if (active && blocked && blocked[1].trim()) disallow.push(blocked[1].trim());
  }
  return disallow;
}

async function getRobots(domain) {
  try {
    const response = await fetchWithTimeout(buildUrl(domain, '/robots.txt'));
    return response.ok ? parseRobotsDisallow(response.text) : [];
  } catch {
    return [];
  }
}

function robotsAllows(url, disallow) {
  const path = new URL(url).pathname || '/';
  return !disallow.some(rule => rule !== '/' && path.startsWith(rule));
}

function extractRelevantLinks(html, baseUrl, domain) {
  const links = new Set();
  const relevant = /\b(?:about|team|staff|attorney|attorneys|people|professionals|contact|firm|our-firm|practice)\b/i;
  for (const match of String(html || '').matchAll(/<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)) {
    const href = decodeHtml(match[1]);
    const label = htmlToText(match[2]);
    if (!relevant.test(`${href} ${label}`)) continue;
    try {
      const url = new URL(href, baseUrl);
      if (normalizeDomain(url.hostname) === normalizeDomain(domain)) links.add(url.toString().split('#')[0]);
    } catch {
      // Ignore malformed links.
    }
  }
  return [...links];
}

async function scrapeWebsite(row, options = {}) {
  const domain = normalizeDomain(row.domain || row.website || row.website_url);
  if (!domain) return { names: [], emails: [], practice_area: null, firm_size: null, pages: [], errors: ['no_domain'] };

  const disallow = await getRobots(domain);
  const homepage = buildUrl(domain, '/');
  const seedPaths = ['/', '/about', '/about-us', '/team', '/staff', '/attorneys', '/our-firm', '/contact'];
  const urls = new Set(seedPaths.map(path => buildUrl(domain, path)).filter(Boolean));
  urls.add(homepage);

  const pages = [];
  const errors = [];
  const fetchDelayMs = Number.isFinite(Number(options.fetchDelayMs)) ? Number(options.fetchDelayMs) : DEFAULT_FETCH_DELAY_MS;

  for (const url of [...urls]) {
    if (pages.length >= MAX_WEBSITE_PAGES) break;
    if (!robotsAllows(url, disallow)) {
      errors.push(`robots_disallow:${new URL(url).pathname}`);
      continue;
    }
    try {
      const response = await fetchWithTimeout(url);
      if (!response.ok || !/html|text/i.test(response.text.slice(0, 300))) {
        errors.push(`fetch_${response.status}:${url}`);
        continue;
      }
      pages.push(response);
      if (url === homepage || new URL(url).pathname === '/') {
        for (const link of extractRelevantLinks(response.text, response.url, domain)) urls.add(link);
      }
      if (fetchDelayMs > 0) await delay(fetchDelayMs);
    } catch (err) {
      errors.push(`${err.name === 'AbortError' ? 'timeout' : 'fetch_error'}:${url}`);
    }
  }

  const allHtml = pages.map(page => page.text).join('\n');
  const allText = htmlToText(allHtml);
  return {
    names: extractNamesFromText(allText, 'website_pages'),
    emails: extractEmailsFromHtml(allHtml, domain),
    practice_area: inferPracticeArea(allText, row.vertical || row.industry),
    firm_size: inferFirmSize(allText),
    pages: pages.map(page => page.url),
    errors,
  };
}

async function verifyCandidate(candidate, verifier = verifyEmail) {
  if (!isBouncerConfigured()) {
    return { ...candidate, verified: false, status: 'unknown', reason: 'bouncer_not_configured', method: null };
  }
  const result = await verifier(candidate.email);
  const verified = Boolean(result.valid) && isBouncerVerified(result);
  return {
    ...candidate,
    verified,
    status: result.status,
    reason: result.reason,
    method: result.method || result.vendor || null,
    verifier_response: result.raw || null,
  };
}

function fieldConfidence(row, field) {
  const provenance = row.enrichment_provenance || {};
  const confidence = Number(provenance?.[field]?.confidence);
  if (Number.isFinite(confidence)) return confidence;
  return clean(row[field]) ? 1 : 0;
}

function maybeSetField(row, field, candidate, updates, provenance) {
  if (!candidate?.value) return;
  const currentValue = clean(row[field]);
  if (currentValue && currentValue !== candidate.value && candidate.confidence <= fieldConfidence(row, field)) return;
  updates[field] = candidate.value;
  provenance[field] = {
    tier: candidate.tier,
    source: candidate.source,
    confidence: candidate.confidence,
    ...(candidate.reason ? { reason: candidate.reason } : {}),
    ...(candidate.email ? { email: candidate.email } : {}),
    resolved_at: new Date().toISOString(),
  };
  row[field] = candidate.value;
  row.enrichment_provenance = { ...(row.enrichment_provenance || {}), ...provenance };
}

async function persistOutcome(row, outcome, dryRun = false) {
  if (dryRun) return;

  const updates = {};
  const provenance = { ...(row.enrichment_provenance || {}) };
  const shouldPersistName = outcome.selectedName && (outcome.selectedName.tier === 0 || outcome.resolved);
  if (shouldPersistName) {
    maybeSetField(row, 'first_name', {
      value: outcome.selectedName.first_name,
      tier: outcome.selectedName.tier,
      source: outcome.selectedName.source,
      confidence: outcome.selectedName.confidence,
    }, updates, provenance);
    const persistLastName = outcome.selectedName.last_name && (outcome.selectedName.tier === 0
      || isGenericEmail(outcome.selectedEmail?.email)
      || emailLocal(outcome.selectedEmail?.email).includes(clean(outcome.selectedName.last_name).toLowerCase().replace(/[^a-z]/g, '')));
    if (persistLastName) {
      maybeSetField(row, 'last_name', {
        value: outcome.selectedName.last_name,
        tier: outcome.selectedName.tier,
        source: outcome.selectedName.source,
        confidence: outcome.selectedName.confidence,
      }, updates, provenance);
    }
  }
  if (outcome.selectedEmail?.verified) {
    maybeSetField(row, 'email', {
      value: outcome.selectedEmail.email,
      tier: outcome.selectedEmail.tier,
      source: outcome.selectedEmail.source,
      confidence: outcome.selectedEmail.confidence,
    }, updates, provenance);
    updates.email_verified = true;
    updates.email_verification_method = 'bouncer';
    updates.email_status = outcome.selectedEmail.status;
    updates.verified_at = new Date();
    updates.verifier_checked_at = new Date();
    updates.verifier_response = outcome.selectedEmail.verifier_response || null;
    provenance.email = {
      ...(provenance.email || {}),
      tier: outcome.selectedEmail.tier,
      source: outcome.selectedEmail.source,
      confidence: outcome.selectedEmail.confidence,
      verifier: 'bouncer',
      status: outcome.selectedEmail.status,
      resolved_at: new Date().toISOString(),
    };
  }
  if (outcome.practice_area && !clean(row.practice_area)) {
    updates.practice_area = outcome.practice_area;
    provenance.practice_area = { tier: 1, source: 'website_pages', confidence: 0.7, resolved_at: new Date().toISOString() };
  }
  if (outcome.firm_size && !clean(row.firm_size)) {
    updates.firm_size = outcome.firm_size;
    provenance.firm_size = { tier: 1, source: 'website_pages', confidence: 0.65, resolved_at: new Date().toISOString() };
  }

  updates.enrichment_status = outcome.resolved ? 'resolved' : 'manual_review';
  updates.enrichment_resolved_tier = outcome.resolvedTier;
  updates.enrichment_checked_at = new Date();
  updates.enrichment_provenance = provenance;

  const setParts = [];
  const values = [];
  for (const [key, value] of Object.entries(updates)) {
    values.push(key === 'enrichment_provenance' || key === 'verifier_response' ? JSON.stringify(value) : value);
    const cast = key === 'enrichment_provenance' || key === 'verifier_response' ? '::jsonb' : '';
    setParts.push(`${key} = $${values.length}${cast}`);
  }
  values.push(row.prospect_id, row.client_id);
  await pool.query(`
    UPDATE prospects
    SET ${setParts.join(', ')},
        updated_at = NOW()
    WHERE id = $${values.length - 1}
      AND client_id = $${values.length}
  `, values);

  if ((outcome.practice_area || outcome.firm_size) && row.company_id) {
    await pool.query(`
      UPDATE companies
      SET practice_area = COALESCE(NULLIF(practice_area, ''), $1),
          firm_size = COALESCE(NULLIF(firm_size, ''), $2),
          enrichment_provenance = COALESCE(enrichment_provenance, '{}'::jsonb) || $3::jsonb,
          updated_at = NOW()
      WHERE id = $4
        AND client_id = $5
    `, [
      outcome.practice_area || null,
      outcome.firm_size || null,
      JSON.stringify({
        ...(outcome.practice_area ? { practice_area: { tier: 1, source: 'website_pages', confidence: 0.7 } } : {}),
        ...(outcome.firm_size ? { firm_size: { tier: 1, source: 'website_pages', confidence: 0.65 } } : {}),
      }),
      row.company_id,
      row.client_id,
    ]);
  }
}

async function upsertManualQueue(row, outcome, dryRun = false) {
  if (dryRun) return;
  const missing = [];
  if (!hasResolvingName(row) && !outcome.selectedName) missing.push('first_name');
  if (!hasResolvingEmail(row) && !outcome.selectedEmail?.verified) missing.push('verified_email');
  if (!missing.length) {
    await pool.query(`
      UPDATE enrichment_manual_queue
      SET status = 'resolved',
          resolved_at = NOW(),
          updated_at = NOW()
      WHERE client_id = $1
        AND prospect_id = $2
        AND status = 'open'
    `, [row.client_id, row.prospect_id]);
    return;
  }

  await pool.query(`
    INSERT INTO enrichment_manual_queue (
      client_id, prospect_id, company_name, website, missing_fields,
      candidate_names, candidate_emails, partial_data, status, last_attempted_at, updated_at
    )
    VALUES ($1, $2, $3, $4, $5::text[], $6::jsonb, $7::jsonb, $8::jsonb, 'open', NOW(), NOW())
    ON CONFLICT (client_id, prospect_id) DO UPDATE
      SET company_name = EXCLUDED.company_name,
          website = EXCLUDED.website,
          missing_fields = EXCLUDED.missing_fields,
          candidate_names = EXCLUDED.candidate_names,
          candidate_emails = EXCLUDED.candidate_emails,
          partial_data = EXCLUDED.partial_data,
          status = 'open',
          last_attempted_at = NOW(),
          updated_at = NOW()
  `, [
    row.client_id,
    row.prospect_id,
    row.company_name,
    row.website || row.website_url || row.domain,
    missing,
    JSON.stringify(outcome.names || []),
    JSON.stringify(outcome.emails || []),
    JSON.stringify({
      practice_area: outcome.practice_area || null,
      firm_size: outcome.firm_size || null,
      website_pages: outcome.pages || [],
      errors: outcome.errors || [],
    }),
  ]);
}

async function logAgentAction(clientId, payload, status = 'success') {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
    VALUES ($1, $2, $3::jsonb, $4, NOW(), $5)
  `, [AGENT_NAME, 'tiered_enrichment_run', JSON.stringify(payload), status, clientId]);
}

async function processProspect(row, options = {}) {
  const working = { ...row, enrichment_provenance: row.enrichment_provenance || {} };
  const beforePass = passesDataBar(working);
  const outcome = {
    prospect_id: row.prospect_id,
    company_name: row.company_name,
    beforePass,
    afterPass: beforePass,
    resolved: beforePass,
    resolvedTier: beforePass ? 0 : null,
    names: [],
    emails: [],
    selectedName: null,
    selectedEmail: null,
    practice_area: row.practice_area || null,
    firm_size: row.firm_size || row.employee_count_estimate || null,
    pages: [],
    errors: [],
  };

  if (beforePass) {
    await upsertManualQueue(working, outcome, options.dryRun);
    return outcome;
  }

  const existingNames = parseNamesFromExistingData(working);
  outcome.names.push(...existingNames);
  const tier0Name = existingNames.find(name => name.confidence >= RESOLVING_NAME_CONFIDENCE);
  if (!hasResolvingName(working) && tier0Name) {
    outcome.selectedName = tier0Name;
    working.first_name = tier0Name.first_name;
    working.last_name = tier0Name.last_name;
  }
  if (hasResolvingEmail(working)) {
    outcome.selectedEmail = {
      email: working.email,
      tier: 0,
      source: 'existing_bouncer_verified_email',
      confidence: 0.95,
      verified: true,
      status: working.email_status,
      method: working.email_verification_method,
    };
  }
  const emailLocalName = deriveNameFromVerifiedEmail(working, outcome.names);
  if (!hasResolvingName(working) && emailLocalName && !emailLocalName.rejected) {
    outcome.selectedName = emailLocalName;
    outcome.names.push(emailLocalName);
    working.first_name = emailLocalName.first_name;
    working.last_name = emailLocalName.last_name || working.last_name;
  } else if (emailLocalName?.rejected) {
    outcome.names.push(emailLocalName);
    outcome.errors.push(`email_localpart_name_rejected:${emailLocalName.reason}:${emailLocalName.email}`);
  }
  if (passesDataBar(working)) {
    outcome.afterPass = true;
    outcome.resolved = true;
    outcome.resolvedTier = 0;
    await persistOutcome(working, outcome, options.dryRun);
    await upsertManualQueue(working, outcome, options.dryRun);
    return outcome;
  }

  if (options.bucketAOnly) {
    outcome.afterPass = false;
    outcome.resolved = false;
    outcome.resolvedTier = null;
    await persistOutcome(working, outcome, options.dryRun);
    await upsertManualQueue(working, outcome, options.dryRun);
    return outcome;
  }

  const website = await scrapeWebsite(working, options);
  outcome.names.push(...website.names);
  outcome.practice_area = outcome.practice_area || website.practice_area;
  outcome.firm_size = outcome.firm_size || website.firm_size;
  outcome.pages = website.pages;
  outcome.errors = website.errors;

  const bestWebsiteName = rankNames(outcome.names).find(name => name.confidence >= RESOLVING_NAME_CONFIDENCE);
  if (!hasResolvingName(working) && bestWebsiteName) {
    outcome.selectedName = bestWebsiteName;
    working.first_name = bestWebsiteName.first_name;
    working.last_name = bestWebsiteName.last_name;
  }

  const emailCandidates = buildEmailCandidates({
    existingEmail: working.email,
    foundEmails: website.emails,
    names: rankNames(outcome.names),
    domain: normalizeDomain(working.domain || working.website || working.website_url),
  });

  for (const candidate of emailCandidates) {
    const verified = hasResolvingEmail(working) && candidate.email === clean(working.email).toLowerCase()
      ? { ...candidate, verified: true, status: working.email_status, method: working.email_verification_method }
      : await verifyCandidate(candidate, options.verifyEmail || verifyEmail);
    outcome.emails.push(verified);
    if (!outcome.selectedEmail && verified.verified) {
      outcome.selectedEmail = verified;
      working.email = verified.email;
      working.email_status = verified.status;
      working.email_verification_method = verified.method;
      working.email_verified = true;
    }
    if (passesDataBar(working)) break;
  }

  if (
    outcome.selectedName?.tier === 1
    && outcome.selectedEmail?.verified
    && !emailMatchesName(outcome.selectedEmail.email, outcome.selectedName)
  ) {
    outcome.errors.push(`name_email_mismatch:${outcome.selectedName.full_name}:${outcome.selectedEmail.email}`);
    working.first_name = row.first_name;
    working.last_name = row.last_name;
    outcome.selectedName = null;
  }

  outcome.afterPass = passesDataBar(working);
  outcome.resolved = outcome.afterPass;
  outcome.resolvedTier = outcome.afterPass ? 1 : null;
  await persistOutcome(working, outcome, options.dryRun);
  await upsertManualQueue(working, outcome, options.dryRun);
  return outcome;
}

function summarize(outcomes) {
  const summary = {
    attempted: outcomes.length,
    resolved_tier_0: 0,
    resolved_tier_1: 0,
    verified_email: 0,
    manual_queue: 0,
    readiness_before: 0,
    readiness_after: 0,
  };
  for (const outcome of outcomes) {
    if (outcome.beforePass) summary.readiness_before++;
    if (outcome.afterPass) summary.readiness_after++;
    if (outcome.resolvedTier === 0 && !outcome.beforePass) summary.resolved_tier_0++;
    if (outcome.resolvedTier === 1 && !outcome.beforePass) summary.resolved_tier_1++;
    if (outcome.selectedEmail?.verified || outcome.emails.some(email => email.verified)) summary.verified_email++;
    if (!outcome.afterPass) summary.manual_queue++;
  }
  return summary;
}

async function run(params = {}) {
  await ensureTieredEnrichmentSchema();
  const clientId = normalizeClientId(params.client_id || params.clientId || process.env.ACTIVE_CLIENT_ID || 1);
  const dryRun = Boolean(params.dryRun);
  const bucketAOnly = Boolean(params.bucketAOnly);
  const result = await pool.query(`
    SELECT
      p.id AS prospect_id,
      p.company_id,
      p.client_id,
      p.first_name,
      p.last_name,
      p.email,
      p.email_status,
      p.email_verified,
      p.email_verification_method,
      p.do_not_contact,
      p.notes,
      p.vertical,
      p.website_url,
      p.employee_count_estimate,
      p.practice_area,
      p.firm_size,
      p.enrichment_provenance,
      c.name AS company_name,
      c.website,
      c.domain,
      c.industry,
      c.size AS company_size,
      c.location,
      c.practice_area AS company_practice_area,
      c.firm_size AS company_firm_size
    FROM prospects p
    LEFT JOIN companies c
      ON c.id = p.company_id
      AND c.client_id = p.client_id
    WHERE p.client_id = $1
      AND (
        NULLIF(TRIM(COALESCE(p.first_name, '')), '') IS NULL
        OR NULLIF(TRIM(COALESCE(p.email, '')), '') IS NULL
        OR COALESCE(p.email_status, '') NOT IN ('valid', 'verified')
        OR COALESCE(p.email_verification_method, '') <> 'bouncer'
      )
    ORDER BY p.created_at ASC, p.id ASC
  `, [clientId]);

  const outcomes = [];
  for (const row of result.rows) {
    const outcome = await processProspect(row, params);
    outcomes.push(outcome);
  }
  const summary = summarize(outcomes);
  await logAgentAction(clientId, { dry_run: dryRun, bucket_a_only: bucketAOnly, summary }, 'success');
  console.log(`[tiered_enrichment] client_id=${clientId} ${JSON.stringify(summary)}`);
  return { client_id: clientId, dry_run: dryRun, summary, outcomes };
}

module.exports = {
  run,
  _test: {
    buildEmailCandidates,
    deriveNameFromVerifiedEmail,
    emailMatchesName,
    extractEmailsFromHtml,
    extractNamesFromText,
    hasResolvingEmail,
    parseNameFromCompany,
    passesDataBar,
    processProspect,
    rankNames,
  },
};

if (require.main === module) {
  const args = process.argv.slice(2);
  const params = {};
  for (const arg of args) {
    if (arg.startsWith('--client_id=')) params.client_id = arg.split('=')[1];
    if (arg === '--dry-run') params.dryRun = true;
    if (arg === '--bucket-a-only') params.bucketAOnly = true;
  }
  run(params).catch(err => {
    console.error(`[tiered_enrichment] Fatal: ${err.stack || err.message}`);
    process.exit(1);
  });
}
