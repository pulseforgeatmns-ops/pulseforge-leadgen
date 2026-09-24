'use strict';

const { EVIDENCE_CLASS, buildFinding } = require('../types');

async function observeDomStructure(url, deps = {}) {
  if (deps.domObserver) return deps.domObserver(url, deps);
  if (deps.skipPuppeteer) return [];

  let puppeteer;
  try {
    puppeteer = require('puppeteer');
  } catch {
    return [];
  }

  let browser;
  try {
    browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });
    const page = await browser.newPage();
    await page.setViewport({ width: 390, height: 844, isMobile: true });
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: deps.timeoutMs || 20000 });

    const observations = await page.evaluate(() => {
      const navLinks = [...document.querySelectorAll('nav a, header a')].map((a) => ({
        text: (a.textContent || '').trim().slice(0, 80),
        href: a.getAttribute('href') || '',
      }));
      const ctas = [...document.querySelectorAll('a, button')].filter((el) => {
        const t = (el.textContent || '').toLowerCase();
        return /contact|quote|schedule|book|call|get started|request/i.test(t);
      }).slice(0, 5).map((el) => ({
        tag: el.tagName.toLowerCase(),
        text: (el.textContent || '').trim().slice(0, 80),
        href: el.getAttribute('href') || null,
      }));
      const phoneVisible = /(\(\d{3}\)|\d{3}[-.\s]?\d{3}[-.\s]?\d{4})/.test(document.body.innerText || '');
      return { navLinks, ctas, phoneVisible };
    });

    const findings = [];
    const observedAt = new Date().toISOString();
    const hasContactNav = observations.navLinks.some((l) => /contact/i.test(l.text));
    if (!hasContactNav) {
      findings.push(buildFinding({
        id: 'dom_no_contact_nav',
        evidence_class: EVIDENCE_CLASS.OBSERVED,
        category: 'conversion_structure',
        summary: 'Primary navigation contains no Contact link',
        source: 'puppeteer_dom',
        observed_at: observedAt,
        ref: 'conversion:nav_contact',
      }));
    }
    if (observations.ctas.length) {
      findings.push(buildFinding({
        id: 'dom_cta_present',
        evidence_class: EVIDENCE_CLASS.OBSERVED,
        category: 'conversion_structure',
        summary: `Primary CTA candidate(s) detected: ${observations.ctas.map((c) => c.text).join('; ')}`,
        source: 'puppeteer_dom',
        observed_at: observedAt,
        ref: 'conversion:primary_cta',
      }));
    }
    if (observations.phoneVisible) {
      findings.push(buildFinding({
        id: 'dom_phone_visible',
        evidence_class: EVIDENCE_CLASS.OBSERVED,
        category: 'conversion_structure',
        summary: 'Phone number visible in rendered page text',
        source: 'puppeteer_dom',
        observed_at: observedAt,
        ref: 'conversion:phone_visible',
      }));
    }
    return findings;
  } catch {
    return [];
  } finally {
    if (browser) await browser.close().catch(() => {});
  }
}

module.exports = {
  observeDomStructure,
};
