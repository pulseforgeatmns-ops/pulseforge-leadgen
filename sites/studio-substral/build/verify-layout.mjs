#!/usr/bin/env node
/**
 * Browser verification for the Studio Substral site.
 *
 * The node test suite checks the source. This checks the rendered result, which
 * is where the failures that matter actually live: a doctrine line break that
 * silently rewraps at some viewport, a `ch` measure resolving against the wrong
 * font size, content widening the document sideways, or the object leaving its
 * frame.
 *
 * Run it after any change to the stylesheet, the markup, or the object.
 *
 *   node verify-layout.mjs            layout and typography across widths
 *   node verify-layout.mjs states     progressive enhancement and the instrument
 *   node verify-layout.mjs all
 *
 * Exits non-zero on any failure, so it can gate a release.
 */

import puppeteer from 'puppeteer';
import { createServer } from 'node:http';
import { createReadStream, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const site = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const WIDTHS = [1920, 1512, 1366, 1280, 1024, 900, 820, 700, 600, 480, 430, 390, 360];

const mode = process.argv[2] || 'layout';
const run = (name) => mode === 'all' || mode === name;

let failures = 0;
const fail = (message) => {
  failures += 1;
  console.log(`  FAIL  ${message}`);
};
const pass = (message) => console.log(`  ok    ${message}`);

/* --- Static server -------------------------------------------------------- */

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css',
  '.js': 'text/javascript',
  '.png': 'image/png',
  '.webp': 'image/webp',
  '.svg': 'image/svg+xml',
  '.woff2': 'font/woff2',
  '.webmanifest': 'application/manifest+json',
  '.xml': 'application/xml',
  '.txt': 'text/plain',
};

const server = createServer((req, res) => {
  const url = decodeURIComponent((req.url || '/').split('?')[0]);
  let file = path.join(site, url);
  if (url.endsWith('/')) file = path.join(file, 'index.html');
  if (!file.startsWith(site)) {
    res.writeHead(403).end();
    return;
  }
  try {
    statSync(file);
  } catch {
    res.writeHead(404).end();
    return;
  }
  res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
  createReadStream(file).pipe(res);
});
await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
const base = `http://127.0.0.1:${server.address().port}/`;

const browser = await puppeteer.launch({
  args: [
    '--no-sandbox',
    '--disable-dev-shm-usage',
    '--use-gl=angle',
    '--use-angle=swiftshader',
    '--enable-unsafe-swiftshader',
    '--font-render-hinting=none',
  ],
});

/** Suppress WebGL so the heavy module never loads during layout checks. */
const blockWebGL = (page) =>
  page.evaluateOnNewDocument(() => {
    const original = HTMLCanvasElement.prototype.getContext;
    HTMLCanvasElement.prototype.getContext = function (type, ...rest) {
      const name = String(type);
      if (name.includes('webgl') || name.includes('experimental')) return null;
      return original.call(this, type, ...rest);
    };
  });

async function open({ width, height = 900, reduced = false, noWebGL = false }) {
  const page = await browser.newPage();
  await page.setViewport({ width, height, deviceScaleFactor: 1 });
  if (reduced) {
    await page.emulateMediaFeatures([{ name: 'prefers-reduced-motion', value: 'reduce' }]);
  }
  if (noWebGL) await blockWebGL(page);
  const problems = [];
  page.on('pageerror', (e) => problems.push(`pageerror: ${e.message}`));
  page.on('console', (m) => m.type() === 'error' && problems.push(`console: ${m.text()}`));
  page.on('requestfailed', (r) =>
    problems.push(`requestfailed: ${r.url()} ${r.failure()?.errorText}`)
  );
  page.problems = problems;
  await page.goto(base, { waitUntil: 'domcontentloaded' });
  await page.evaluateHandle('document.fonts.ready');
  await new Promise((resolve) => setTimeout(resolve, 600));
  return page;
}

/* --- Layout and typography ------------------------------------------------ */

if (run('layout')) {
  console.log('\nLayout and typography');
  for (const width of WIDTHS) {
    const page = await open({ width, noWebGL: true });
    const report = await page.evaluate(() => {
      const headings = [];
      for (const el of document.querySelectorAll('h1, h2')) {
        if (!el.innerHTML.includes('<br>')) continue;
        const intended = el.innerHTML.split('<br>').length;
        const range = document.createRange();
        range.selectNodeContents(el);
        const rows = new Set(
          [...range.getClientRects()]
            .filter((r) => r.height > 4)
            .map((r) => Math.round(r.top / 4))
        );
        range.detach();
        headings.push({
          text: el.textContent.trim().replace(/\s+/g, ' ').slice(0, 44),
          intended,
          actual: rows.size,
        });
      }

      const overflows = [];
      for (const el of document.querySelectorAll('h1, h2, h3, p, dd, li, figure, img')) {
        const parent = el.parentElement;
        if (!parent || !parent.clientWidth) continue;
        // The honeypot is deliberately parked outside the layout.
        if (el.closest('.field--hidden')) continue;
        if (el.scrollWidth > parent.clientWidth + 2) {
          overflows.push(`${el.tagName}.${el.className}`.slice(0, 48));
        }
      }

      return {
        headings,
        overflows,
        scrollWidth: document.documentElement.scrollWidth,
        clientWidth: document.documentElement.clientWidth,
      };
    });

    const label = `${width}px`;
    if (report.scrollWidth > report.clientWidth) {
      fail(`${label} scrolls sideways (${report.scrollWidth} > ${report.clientWidth})`);
    }
    for (const heading of report.headings) {
      if (heading.actual !== heading.intended) {
        fail(
          `${label} "${heading.text}" authored ${heading.intended} lines, rendered ${heading.actual}`
        );
      }
    }
    for (const overflow of report.overflows) {
      fail(`${label} ${overflow} overflows its container`);
    }
    if (
      report.scrollWidth <= report.clientWidth &&
      !report.overflows.length &&
      report.headings.every((h) => h.actual === h.intended)
    ) {
      pass(`${label} — ${report.headings.length} authored line breaks hold, nothing overflows`);
    }
    await page.close();
  }
}

/* --- Progressive enhancement and the instrument --------------------------- */

if (run('states')) {
  console.log('\nProgressive enhancement');

  {
    const page = await open({ width: 1512, noWebGL: true });
    const state = await page.evaluate(() => ({
      webgl: [...document.querySelectorAll('[data-stage]')].map((s) => s.dataset.webgl),
      labels: [...document.querySelectorAll('.layer__name')].map((e) => e.textContent),
      plates: document.querySelectorAll('.plate').length,
    }));
    if (state.webgl.some((v) => v !== 'off')) fail('WebGL reported on with no context available');
    else if (state.plates !== 18) fail(`expected 18 CSS plates, found ${state.plates}`);
    else if (state.labels.join() !== 'Performance,Accessibility,Conversion,Search,Trust,Design')
      fail(`layer labels wrong without WebGL: ${state.labels}`);
    else pass('without WebGL: CSS composition renders and all six labels remain');
    if (page.problems.length) fail(`console errors without WebGL: ${page.problems[0]}`);
    await page.close();
  }

  {
    const page = await open({ width: 1512, reduced: true });
    const state = await page.evaluate(() => ({
      canvas: getComputedStyle(document.querySelector('[data-stage-canvas]')).display,
      revealed: [...document.querySelectorAll('[data-reveal]')].every(
        (e) => e.dataset.revealed === 'true'
      ),
      sticky: getComputedStyle(document.querySelector('.decomposition__stage')).position,
    }));
    if (state.canvas !== 'none') fail('reduced motion still shows the canvas');
    else if (!state.revealed) fail('reduced motion hides revealable content');
    else if (state.sticky !== 'relative') fail('reduced motion keeps the stage pinned');
    else pass('reduced motion: canvas suppressed, stage released, all content visible');
    await page.close();
  }

  {
    const page = await open({ width: 390, noWebGL: false });
    const webgl = await page.evaluate(
      () => document.querySelector('[data-stage]').dataset.webgl
    );
    if (webgl !== 'off') fail('the object loaded on a small viewport');
    else pass('small viewport: CSS composition is the intended treatment');
    await page.close();
  }

  console.log('\nThe assessment instrument');
  {
    const page = await open({ width: 1512, noWebGL: true });
    const say = () =>
      page.$eval('[data-assessment-status]', (el) => el.textContent.trim());

    const cases = [
      ['https://www.google.com/search?q=cleaners', 'owner@example.com', /search engine/i],
      ['example', 'owner@example.com', /full domain/i],
      ['localhost:3000', 'owner@example.com', /not reachable/i],
      ['example.com', 'nope', /valid email address/i],
    ];
    let ok = true;
    for (const [domain, email, expected] of cases) {
      await page.$eval('#domain', (el) => (el.value = ''));
      await page.$eval('#email', (el) => (el.value = ''));
      await page.type('#domain', domain);
      await page.type('#email', email);
      await page.click('[data-assessment-submit]');
      await new Promise((resolve) => setTimeout(resolve, 350));
      const message = await say();
      if (!expected.test(message)) {
        fail(`"${domain}" / "${email}" produced: ${message}`);
        ok = false;
      }
    }
    if (ok) pass('rejects search engines, bare names, unroutable hosts and bad addresses');

    // No endpoint is reachable from here, so this exercises the offline route.
    await page.$eval('#domain', (el) => (el.value = 'HTTPS://WWW.Example.com/contact'));
    await page.$eval('#email', (el) => (el.value = 'owner@example.com'));
    await page.click('[data-assessment-submit]');
    await new Promise((resolve) => setTimeout(resolve, 4000));
    const normalized = await page.$eval('#domain', (el) => el.value);
    const message = await say();
    if (normalized !== 'example.com') fail(`input not normalized: ${normalized}`);
    else if (!/Nothing was lost/i.test(message)) fail(`no offline route offered: ${message}`);
    else pass('normalizes the domain and never loses a request when the queue is down');

    if (/\d{1,3}\s*\/\s*100/.test(message)) fail('the instrument produced a score');
    await page.close();
  }
}

await browser.close();
await new Promise((resolve) => server.close(resolve));

console.log(
  failures ? `\n${failures} failure${failures === 1 ? '' : 's'}.` : '\nAll checks passed.'
);
process.exit(failures ? 1 : 0);
