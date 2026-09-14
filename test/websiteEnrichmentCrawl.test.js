'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  CRAWL_PRIORITY,
  CrawlQueue,
  crawlWebsite,
  extractRelevantLinks,
  isRelevantLink,
  isSameSiteDomain,
  resolveEnrichmentDomain,
} = require('../utils/websiteEnrichmentCrawl');
const tiered = require('../tieredEnrichmentAgent');
const { filterScrapedWebsiteEmails, scrapeWebsiteEmail } = require('../leadgen');

function mockSiteResponses(pagesByPath) {
  const fetchCounts = new Map();
  return {
    fetchCounts,
    fetchPage: async (url) => {
      const path = new URL(url).pathname;
      fetchCounts.set(path, (fetchCounts.get(path) || 0) + 1);
      const entry = pagesByPath[path];
      if (!entry) {
        return { ok: false, status: 404, text: 'not found', url };
      }
      return { ok: true, status: 200, text: entry, url };
    },
  };
}

describe('websiteEnrichmentCrawl', () => {
  it('matches contacts label and legacy .html paths as relevant links', () => {
    assert.equal(isRelevantLink('index-5.html', 'Contacts'), true);
    assert.equal(isRelevantLink('about.html', 'Meet Our Team'), true);
    assert.equal(isRelevantLink('index-5.html', 'Home'), false);
  });

  it('extractRelevantLinks keeps same-site links only', () => {
    const html = `
      <a href="about.html">About</a>
      <a href="https://evil.example.com/contact">Off domain</a>
      <a href="index-5.html">Contacts</a>
    `;
    const links = extractRelevantLinks(html, 'https://kluglawoffices.com/', 'kluglawoffices.com');
    assert.deepEqual(links, [
      'https://kluglawoffices.com/about.html',
      'https://kluglawoffices.com/index-5.html',
    ]);
    assert.equal(isSameSiteDomain('www.kluglawoffices.com', 'kluglawoffices.com'), true);
  });

  it('prefers provider-backed website_url over derived domain field', () => {
    assert.equal(
      resolveEnrichmentDomain({
        domain: 'lawofficeofmichaelstlouis.com',
        website_url: 'https://lawofficeofmichaelstlouis-com.webnode.page/',
      }),
      'lawofficeofmichaelstlouis-com.webnode.page'
    );
  });

  it('homepage discovers /about.html and crawl reaches it before guessed paths exhaust budget', async () => {
    const homepage = `<html><body><a href="about.html">About our firm</a></body></html>`;
    const about = `<html><body>Reach us at owner@examplelaw.com</body></html>`;
    const { fetchPage, fetchCounts } = mockSiteResponses({
      '/': homepage,
      '/about.html': about,
    });

    const result = await crawlWebsite('examplelaw.com', fetchPage, { maxSuccessfulPages: 8 });
    const paths = result.pages.map((page) => new URL(page.url).pathname);

    assert.ok(paths.includes('/'));
    assert.ok(paths.includes('/about.html'), `expected /about.html in ${paths.join(', ')}`);
    assert.ok(result.pages.some((page) => page.text.includes('owner@examplelaw.com')));
    assert.equal(fetchCounts.get('/about.html'), 1);
    assert.ok((fetchCounts.get('/contact') || 0) <= 1);
  });

  it('does not fetch duplicate discovered links', async () => {
    const homepage = `<html><body><a href="about.html">About</a><a href="/about.html">About us</a></body></html>`;
    const { fetchPage, fetchCounts } = mockSiteResponses({
      '/': homepage,
      '/about.html': '<html>info@examplelaw.com</html>',
    });

    await crawlWebsite('examplelaw.com', fetchPage, { maxSuccessfulPages: 4 });
    assert.equal(fetchCounts.get('/about.html'), 1);
  });

  it('does not follow off-domain links', async () => {
    const homepage = `<html><body><a href="https://other.com/contact">Contact</a></body></html>`;
    const { fetchPage, fetchCounts } = mockSiteResponses({ '/': homepage });

    const result = await crawlWebsite('examplelaw.com', fetchPage, { maxSuccessfulPages: 4 });
    assert.ok(result.pages.every((page) => new URL(page.url).hostname === 'examplelaw.com'));
    assert.equal(result.pages.some((page) => page.url.includes('other.com')), false);
  });

  it('CrawlQueue dequeues by priority then insertion order deterministically', () => {
    const queue = new CrawlQueue();
    queue.enqueue('https://example.com/contact', CRAWL_PRIORITY.GUESSED);
    queue.enqueue('https://example.com/about.html', CRAWL_PRIORITY.DISCOVERED);
    queue.enqueue('https://example.com/', CRAWL_PRIORITY.HOMEPAGE);
    assert.equal(queue.dequeue().url, 'https://example.com/');
    assert.equal(queue.dequeue().url, 'https://example.com/about.html');
    assert.equal(queue.dequeue().url, 'https://example.com/contact');
  });
});

describe('tiered scrapeWebsite crawl integration', () => {
  it('discovers legacy about.html email on Klug-like site layout', async () => {
    const homepage = `
      <html><body>
        <a href="about.html">Meet Our Team</a>
        <a href="index-5.html">Contacts</a>
      </body></html>
    `;
    const about = `<html><body>AKlug@kluglawoffices.com</body></html>`;
    const pagesByPath = {
      '/': homepage,
      '/about.html': about,
    };

    const outcome = await tiered._test.scrapeWebsite(
      { domain: 'kluglawoffices.com', vertical: 'law_firm' },
      {
        fetchDelayMs: 0,
        fetchPage: async (url) => {
          const path = new URL(url).pathname;
          const entry = pagesByPath[path];
          if (!entry) return { ok: false, status: 404, text: 'missing', url };
          return { ok: true, status: 200, text: entry, url };
        },
      }
    );

    assert.ok(outcome.pages.some((page) => page.includes('about.html')));
    assert.ok(outcome.emails.some((row) => row.email === 'aklug@kluglawoffices.com'));
  });

  it('still requires Bouncer verification before persistence', async () => {
    const prevEnabled = process.env.BOUNCER_ENABLED;
    const prevKey = process.env.BOUNCER_API_KEY;
    process.env.BOUNCER_ENABLED = 'true';
    process.env.BOUNCER_API_KEY = 'test-key';

    try {
      const mxOnly = await tiered._test.verifyCandidate(
        { email: 'aklug@kluglawoffices.com', tier: 1, source: 'website_email', confidence: 0.86 },
        async () => ({ valid: true, status: 'valid', method: 'mx_lookup', vendor: 'mx_lookup' })
      );
      assert.equal(mxOnly.verified, false);

      const bouncerVerified = await tiered._test.verifyCandidate(
        { email: 'aklug@kluglawoffices.com', tier: 1, source: 'website_email', confidence: 0.86 },
        async () => ({ valid: true, status: 'valid', method: 'bouncer', vendor: 'bouncer' })
      );
      assert.equal(bouncerVerified.verified, true);
      assert.equal(tiered._test.passesDataBar({
        first_name: 'Achsa',
        email: 'aklug@kluglawoffices.com',
        email_status: 'valid',
        email_verification_method: 'bouncer',
      }), true);
    } finally {
      if (prevEnabled == null) delete process.env.BOUNCER_ENABLED;
      else process.env.BOUNCER_ENABLED = prevEnabled;
      if (prevKey == null) delete process.env.BOUNCER_API_KEY;
      else process.env.BOUNCER_API_KEY = prevKey;
    }
  });
});

describe('leadgen scrapeWebsiteEmail crawl integration', () => {
  it('filterScrapedWebsiteEmails rejects placeholder domains', () => {
    assert.deepEqual(
      filterScrapedWebsiteEmails('contact@example.com and real@firm.com'),
      ['real@firm.com']
    );
  });
});
