const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const SITE_ROOT = path.join(__dirname, '..', 'sites', 'anchor-cleaning');
const COMMERCIAL = path.join(SITE_ROOT, 'index.html');
const RESIDENTIAL = path.join(SITE_ROOT, 'residential', 'index.html');
const THANK_YOU = path.join(SITE_ROOT, 'thank-you', 'index.html');
const SITEMAP = path.join(SITE_ROOT, 'sitemap.xml');

describe('Anchor thank-you conversion page (SPEC-ANCHOR-SITE-THANKYOU-001)', () => {
  it('serves a thank-you page at /thank-you/', () => {
    const html = fs.readFileSync(THANK_YOU, 'utf8');
    assert.match(html, /<title>Thank You \| Anchor Cleaning<\/title>/);
    assert.match(html, /rel="canonical" href="https:\/\/goanchorcleaning\.com\/thank-you\/"/);
    assert.match(html, /AW-18463870847/);
    assert.match(html, /id="thank-you-heading"/);
    assert.match(html, /href="\/residential\/"/);
    assert.match(html, /href="\/"/);
  });

  it('lists thank-you in sitemap.xml', () => {
    const xml = fs.readFileSync(SITEMAP, 'utf8');
    assert.match(xml, /https:\/\/goanchorcleaning\.com\/thank-you\//);
  });

  it('redirects commercial form success to thank-you without removing existing conversion hooks', () => {
    const html = fs.readFileSync(COMMERCIAL, 'utf8');
    assert.match(html, /window\.location\.replace\('\/thank-you\/\?from=commercial'\)/);
    assert.match(html, /sendAdsConversion\(window\.ANCHOR_ANALYTICS\.formConversion\)/);
    assert.match(html, /if \(!redirecting\) btn\.disabled = false/);
  });

  it('redirects residential quote form success to thank-you and keeps failures on-page', () => {
    const html = fs.readFileSync(RESIDENTIAL, 'utf8');
    assert.match(html, /window\.location\.replace\(THANK_YOU\)/);
    assert.match(html, /THANK_YOU='\/thank-you\/\?from=residential'/);
    assert.match(html, /Could not send the form/);
    assert.match(html, /if\(!redirecting\)btn\.disabled=false/);
  });

  it('includes the Service Assurance portal section on residential', () => {
    const html = fs.readFileSync(RESIDENTIAL, 'utf8');
    assert.match(html, /id="service-assurance"/);
    assert.match(html, /Service Assurance/);
    assert.match(html, /\/assets\/service-assurance\/client-dashboard\.png/);
    assert.match(html, /href="#service-assurance"/);
  });
});
