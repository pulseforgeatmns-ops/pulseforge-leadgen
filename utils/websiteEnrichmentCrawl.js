'use strict';

/**
 * Bounded same-site crawl queue for website enrichment (tiered + leadgen scrape).
 * Discovered internal links are prioritized over guessed seed paths.
 */

const { resolveOfficialEnrichmentDomain } = require('./canonicalEmailEligibility');

const RELEVANT_LINK_RE = /\b(?:about|team|staff|attorney|attorneys|people|professionals|contacts?|firm|our-firm|practice|profile|profiles)\b/i;

const GUESSED_SEED_PATHS = Object.freeze([
  '/contact',
  '/contact-us',
  '/about',
  '/about-us',
  '/team',
  '/staff',
  '/attorneys',
  '/our-firm',
]);

const CRAWL_PRIORITY = Object.freeze({
  HOMEPAGE: 0,
  DISCOVERED: 1,
  DISCOVERED_NESTED: 2,
  GUESSED: 3,
});

function normalizeDomain(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    return new URL(raw.includes('://') ? raw : `https://${raw}`)
      .hostname
      .replace(/^www\./i, '')
      .toLowerCase();
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

function buildUrl(domain, path = '/') {
  const normalized = normalizeDomain(domain);
  if (!normalized) return null;
  return `https://${normalized}${path.startsWith('/') ? path : `/${path}`}`;
}

function canonicalizeUrl(url) {
  try {
    const parsed = new URL(url);
    parsed.hash = '';
    let out = parsed.toString();
    if (parsed.pathname !== '/' && out.endsWith('/')) out = out.slice(0, -1);
    return out;
  } catch {
    return null;
  }
}

/**
 * Prefer an official company domain; never use social/directory hosts for enrichment.
 * @param {object|null} row
 * @returns {string|null}
 */
function resolveEnrichmentDomain(row) {
  return resolveOfficialEnrichmentDomain(row);
}

function isSameSiteDomain(hostname, domain) {
  return normalizeDomain(hostname) === normalizeDomain(domain);
}

function isRelevantLink(href, label) {
  return RELEVANT_LINK_RE.test(`${href} ${label}`);
}

function htmlLinkLabel(fragment) {
  return String(fragment || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

function decodeHtml(value) {
  return String(value || '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#39;/g, "'");
}

function extractRelevantLinks(html, baseUrl, domain) {
  const links = [];
  const seen = new Set();
  for (const match of String(html || '').matchAll(/<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)) {
    const href = decodeHtml(match[1]);
    const label = htmlLinkLabel(match[2]);
    if (!isRelevantLink(href, label)) continue;
    try {
      const url = new URL(href, baseUrl);
      if (!isSameSiteDomain(url.hostname, domain)) continue;
      const canonical = canonicalizeUrl(url.toString());
      if (canonical && !seen.has(canonical)) {
        seen.add(canonical);
        links.push(canonical);
      }
    } catch {
      // Ignore malformed links.
    }
  }
  return links.sort();
}

class CrawlQueue {
  constructor() {
    this.items = [];
    this.enqueued = new Set();
    this.visited = new Set();
    this.seq = 0;
  }

  enqueue(url, priority) {
    const canonical = canonicalizeUrl(url);
    if (!canonical || this.enqueued.has(canonical) || this.visited.has(canonical)) return false;
    this.enqueued.add(canonical);
    this.items.push({ url: canonical, priority, seq: this.seq++ });
    return true;
  }

  dequeue() {
    if (!this.items.length) return null;
    this.items.sort((a, b) => a.priority - b.priority || a.seq - b.seq);
    return this.items.shift();
  }

  markVisited(url) {
    const canonical = canonicalizeUrl(url);
    if (canonical) {
      this.visited.add(canonical);
      this.enqueued.delete(canonical);
    }
  }

  hasVisited(url) {
    const canonical = canonicalizeUrl(url);
    return Boolean(canonical && this.visited.has(canonical));
  }
}

function buildSeedUrls(domain) {
  const urls = [];
  for (const path of GUESSED_SEED_PATHS) {
    const plain = buildUrl(domain, path);
    const www = buildUrl(`www.${domain}`, path);
    if (plain) urls.push(plain);
    if (www) urls.push(www);
  }
  return urls;
}

/**
 * @param {string} domain
 * @param {(url: string) => Promise<{ ok?: boolean, status?: number, text?: string, url?: string }>} fetchPage
 * @param {object} [options]
 */
async function crawlWebsite(domain, fetchPage, options = {}) {
  const normalizedDomain = normalizeDomain(domain);
  if (!normalizedDomain) {
    return { pages: [], errors: ['no_domain'] };
  }

  const maxSuccessfulPages = Number.isFinite(Number(options.maxSuccessfulPages))
    ? Number(options.maxSuccessfulPages)
    : 8;
  const fetchDelayMs = Number.isFinite(Number(options.fetchDelayMs)) ? Number(options.fetchDelayMs) : 0;
  const robotsAllows = typeof options.robotsAllows === 'function' ? options.robotsAllows : () => true;

  const queue = new CrawlQueue();
  const homepage = buildUrl(normalizedDomain, '/');
  queue.enqueue(homepage, CRAWL_PRIORITY.HOMEPAGE);
  for (const url of buildSeedUrls(normalizedDomain)) {
    queue.enqueue(url, CRAWL_PRIORITY.GUESSED);
  }

  const pages = [];
  const errors = [];
  const fetchCounts = new Map();

  while (pages.length < maxSuccessfulPages) {
    const next = queue.dequeue();
    if (!next) break;
    if (queue.hasVisited(next.url)) continue;
    queue.markVisited(next.url);

    const fetchKey = canonicalizeUrl(next.url);
    fetchCounts.set(fetchKey, (fetchCounts.get(fetchKey) || 0) + 1);

    if (!robotsAllows(next.url)) {
      errors.push(`robots_disallow:${new URL(next.url).pathname}`);
      continue;
    }

    try {
      const response = await fetchPage(next.url);
      const text = response?.text != null ? String(response.text) : '';
      const finalUrl = response?.url || next.url;
      const ok = response?.ok !== false && response?.status !== 404 && (response?.status == null || response.status < 400);

      if (!ok || !/html|text/i.test(text.slice(0, 300))) {
        errors.push(`fetch_${response?.status || 'error'}:${next.url}`);
        continue;
      }

      pages.push({ url: finalUrl, text, status: response?.status || 200 });

      const discoverPriority = next.priority === CRAWL_PRIORITY.HOMEPAGE
        ? CRAWL_PRIORITY.DISCOVERED
        : next.priority <= CRAWL_PRIORITY.DISCOVERED
          ? CRAWL_PRIORITY.DISCOVERED_NESTED
          : null;
      if (discoverPriority != null) {
        for (const link of extractRelevantLinks(text, finalUrl, normalizedDomain)) {
          queue.enqueue(link, discoverPriority);
        }
      }

      if (fetchDelayMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, fetchDelayMs));
      }
    } catch (err) {
      errors.push(`${err?.name === 'AbortError' ? 'timeout' : 'fetch_error'}:${next.url}`);
    }
  }

  return { pages, errors, fetchCounts };
}

module.exports = {
  RELEVANT_LINK_RE,
  CRAWL_PRIORITY,
  GUESSED_SEED_PATHS,
  CrawlQueue,
  buildSeedUrls,
  buildUrl,
  canonicalizeUrl,
  crawlWebsite,
  extractRelevantLinks,
  isRelevantLink,
  isSameSiteDomain,
  normalizeDomain,
  resolveEnrichmentDomain,
};
