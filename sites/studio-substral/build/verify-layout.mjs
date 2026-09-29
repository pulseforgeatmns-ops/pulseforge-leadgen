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
 *   node verify-layout.mjs object     what the rendered signature object measures
 *   node verify-layout.mjs all
 *
 * Exits non-zero on any failure, so it can gate a release.
 *
 * `object` renders through swiftshader and settles the scroll state, so it takes
 * a couple of minutes. It is the only check that can see the thing the brief for
 * the signature object is actually about: whether the substrate reads as stone
 * and whether the six layers separate in grayscale. Neither is visible to a
 * source assertion — the previous two passes both satisfied every source rule
 * about material differentiation and still rendered six similar panes.
 */

import puppeteer from 'puppeteer';
import { createServer } from 'node:http';
import { createReadStream, statSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
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

/* --- The bundle the page will actually load -------------------------------- */

/* Nothing below this means anything if the committed bundle does not match the
   source. It is easy to miss: a build failure inside a shell pipeline reports the
   exit status of the pipeline, so a broken build looks like a quiet one, and every
   render after it silently measures the previous version. Check it first. */
{
  const build = spawnSync(process.execPath, ['build.mjs', '--check'], {
    cwd: path.dirname(fileURLToPath(import.meta.url)),
    encoding: 'utf8',
  });
  if (build.status !== 0) {
    console.log('\nThe committed object bundle does not match src/dimensional.js.');
    console.log((build.stdout || '') + (build.stderr || ''));
    process.exit(1);
  }
}

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
      const crushed = [];
      const fontSize = (el) => Number.parseFloat(getComputedStyle(el).fontSize) || 16;

      for (const el of document.querySelectorAll(
        'h1, h2, h3, p, dd, li, span, figure, img'
      )) {
        const parent = el.parentElement;
        if (!parent || !parent.clientWidth) continue;
        // The honeypot is deliberately parked outside the layout.
        if (el.closest('.field--hidden')) continue;
        /* The dimensional composition is a decorative 3D scene, clipped by its
           stage. Its parts are meant to exceed their boxes — the substrate is
           wider than the plates it carries — so layout rules do not apply. */
        if (el.closest('.strata')) continue;

        if (el.scrollWidth > parent.clientWidth + 2) {
          overflows.push(`${el.tagName}.${el.className}`.slice(0, 48));
        }

        /* A block of running text squeezed into a couple of characters' width
           renders one word per line. It does not overflow, so the check above
           never sees it, but it is catastrophic and easy to introduce with a
           stray grid child. */
        const words = (el.textContent || '').trim().split(/\s+/).filter(Boolean);
        if (
          words.length >= 4 &&
          el.children.length === 0 &&
          el.clientWidth > 0 &&
          el.clientWidth < fontSize(el) * 6
        ) {
          crushed.push(
            `${el.tagName}.${el.className}`.slice(0, 40) +
              ` (${el.clientWidth}px for ${words.length} words)`
          );
        }
      }

      return {
        headings,
        overflows,
        crushed,
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
    for (const crushed of report.crushed) {
      fail(`${label} ${crushed} is crushed to a sliver`);
    }
    if (
      report.scrollWidth <= report.clientWidth &&
      !report.overflows.length &&
      !report.crushed.length &&
      report.headings.every((h) => h.actual === h.intended)
    ) {
      pass(`${label} — ${report.headings.length} authored line breaks hold, text is not crushed`);
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
      plinths: document.querySelectorAll('.plinth').length,
      // Each plate must actually paint its own drawing, not inherit a default.
      drawings: new Set(
        [...document.querySelectorAll('.plate')].map(
          (p) => getComputedStyle(p.querySelector('.plate__face'), '::before').backgroundImage
        )
      ).size,
    }));
    if (state.webgl.some((v) => v !== 'off')) fail('WebGL reported on with no context available');
    else if (state.plates !== 18) fail(`expected 18 CSS plates, found ${state.plates}`);
    else if (state.plinths !== 3) fail(`expected 3 substrates, found ${state.plinths}`);
    else if (state.drawings !== 6) fail(`expected 6 distinct layer drawings, found ${state.drawings}`);
    else if (state.labels.join() !== 'Performance,Accessibility,Conversion,Search,Trust,Design')
      fail(`layer labels wrong without WebGL: ${state.labels}`);
    else pass('without WebGL: six distinct plates, the substrate, and all six labels');
    if (page.problems.length) fail(`console errors without WebGL: ${page.problems[0]}`);
    await page.close();
  }

  {
    const page = await open({ width: 1512, reduced: true });
    const state = await page.evaluate(() => {
      const layout = document.querySelector('.decomposition__layout');
      return {
        canvas: getComputedStyle(document.querySelector('[data-stage-canvas]')).display,
        revealed: [...document.querySelectorAll('[data-reveal]')].every(
          (e) => e.dataset.revealed === 'true'
        ),
        sticky: getComputedStyle(document.querySelector('.decomposition__stage')).position,
        // One column: with nothing pinned, two columns would leave an empty
        // gutter beside every chapter once the object had scrolled past.
        columns: getComputedStyle(layout).gridTemplateColumns.split(' ').length,
      };
    });
    if (state.canvas !== 'none') fail('reduced motion still shows the canvas');
    else if (!state.revealed) fail('reduced motion hides revealable content');
    else if (state.sticky !== 'relative') fail('reduced motion keeps the stage pinned');
    else if (state.columns !== 1)
      fail(`reduced motion leaves ${state.columns} columns and an empty gutter`);
    else pass('reduced motion: canvas suppressed, single column, all content visible');
    await page.close();
  }

  {
    const page = await open({ width: 390, noWebGL: false });
    const webgl = await page.evaluate(
      () => document.querySelector('[data-stage]').dataset.webgl
    );
    if (webgl !== 'on') fail('a capable phone did not receive the approved material object');
    else pass('capable phone: approved material object loaded in the mobile stage');
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

/* --- The rendered object -------------------------------------------------- */

if (run('object')) {
  console.log('\nThe signature object, as rendered');

  /* Pixels come back through a screenshot rather than off the canvas: the site's
     renderer does not preserve its drawing buffer, and it should not have to for
     a test. A second page decodes the PNG, which needs no dependency. */
  const analyst = await browser.newPage();
  await analyst.setContent('<!doctype html><canvas id=c></canvas>');
  const sample = async (page, clip) => {
    const png = await page.screenshot({ clip, encoding: 'base64' });
    return analyst.evaluate(
      async (data, w, h) =>
        new Promise((resolve) => {
          const image = new Image();
          image.onload = () => {
            const canvas = document.getElementById('c');
            canvas.width = w;
            canvas.height = h;
            const ctx = canvas.getContext('2d', { willReadFrequently: true });
            ctx.drawImage(image, 0, 0);
            const { data: rgba } = ctx.getImageData(0, 0, w, h);
            const luma = new Float32Array(w * h);
            for (let i = 0; i < w * h; i += 1) {
              luma[i] =
                0.2126 * rgba[i * 4] + 0.7152 * rgba[i * 4 + 1] + 0.0722 * rgba[i * 4 + 2];
            }
            resolve({ w, h, luma: Array.from(luma) });
          };
          image.src = `data:image/png;base64,${data}`;
        }),
      png,
      Math.round(clip.width),
      Math.round(clip.height)
    );
  };

  /* Where the object's canvas is, in page coordinates. A screenshot clip is
     measured from the top of the document, and the stage is pinned inside a
     scrolled act, so its client rect has to be offset or the clip lands somewhere
     else entirely — in the hero, as it happens, where the display type measures
     as a very high-contrast surface indeed. */
  const frame = (page, stage) =>
    page.evaluate((name) => {
      const canvas = document.querySelector(`[data-stage="${name}"] canvas`);
      if (!canvas) return null;
      const r = canvas.getBoundingClientRect();
      return {
        x: r.x + window.scrollX,
        y: Math.max(0, r.y + window.scrollY),
        width: r.width,
        height: Math.min(r.height, document.documentElement.scrollHeight - (r.y + window.scrollY)),
      };
    }, stage);

  const page = await open({ width: 1600, height: 900 });
  // The end of the decomposition act: the widest the stack ever opens.
  await page.evaluate(() => {
    const region = document.querySelector('.decomposition__layout');
    const r = region.getBoundingClientRect();
    window.scrollTo(0, scrollY + r.top + r.height - window.innerHeight - 2);
  });
  // Damping is 0.06 a frame and this renders in software: it needs the time.
  await new Promise((resolve) => setTimeout(resolve, 40000));

  const box = await frame(page, 'decomposition');
  if (!box || box.width < 100) {
    fail('the decomposition canvas never appeared, so nothing could be measured');
  } else {
    const shot = await sample(page, box);
    const { w, h, luma } = shot;
    const at = (x, y) => luma[y * w + x];

    /* The page's own background, read from a corner the object never reaches.
       Everything below is measured against it rather than against a constant. */
    let ground = 0;
    for (let y = 2; y < 12; y += 1) for (let x = 2; x < 12; x += 1) ground += at(x, y);
    ground /= 100;

    /* The substrate occupies the lower part of the frame with the stack lifted
       away above it. Measure the object's own pixels only: anything within a
       couple of levels of the background is air. */
    const top = Math.round(h * 0.55);
    let lit = 0;
    let texture = 0;
    let pairs = 0;
    const values = [];
    for (let y = top; y < h - 1; y += 1) {
      for (let x = 1; x < w - 1; x += 1) {
        const v = at(x, y);
        if (v - ground < 2.5) continue;
        lit += 1;
        values.push(v);
        const right = at(x + 1, y);
        if (right - ground >= 2.5) {
          texture += Math.abs(v - right);
          pairs += 1;
        }
      }
    }

    if (lit < w * h * 0.02) {
      fail(`the substrate covers only ${((lit / (w * h)) * 100).toFixed(1)}% of the frame`);
    } else {
      values.sort((a, b) => a - b);
      const at01 = values[Math.floor(values.length * 0.01)];
      const at99 = values[Math.floor(values.length * 0.99)];
      const range = at99 - at01;
      const grain = texture / Math.max(pairs, 1);

      /* A smooth extruded polygon is flat between its arrises, so neighbouring
         pixels agree and this number collapses toward zero however dark the
         surface is. Broken stone disagrees with itself everywhere. Both of the
         previous substrates — the extruded plate and the low-poly block — sat
         under 1.2 here. */
      if (grain < 2.2) fail(`substrate grain is ${grain.toFixed(2)}: the surface is too smooth`);
      else pass(`substrate grain ${grain.toFixed(2)} — the surface disagrees with itself`);

      /* And it has to have somewhere to be dark. Directional light catching high
         points over deep self-shadowing is a wide range; an evenly lit plate is a
         narrow one. */
      if (range < 45) fail(`substrate luminance range is ${range.toFixed(0)}: too even to read`);
      else pass(`substrate luminance range ${range.toFixed(0)} — lit crests over dark fracture`);
    }

    /* The layers, in grayscale. Six materials that answer light differently
       occupy many separated grey levels; six variants of one pane cluster into a
       couple, whatever their stated properties say. Measured over the stack — the
       upper part of the frame, above the block — as the number of 12-level
       buckets carrying a real share of the object's pixels, and how far the bulk
       of those pixels spread.

       Identifying which band is which layer was tried first and is not worth it:
       the plates are seen obliquely, they overlap, and reflections cross them, so
       any per-plate attribution is guesswork. The distribution is not. */
    const stack = [];
    for (let y = 1; y < Math.round(h * 0.55); y += 1) {
      for (let x = 1; x < w - 1; x += 1) {
        const v = at(x, y);
        if (v - ground >= 2.5) stack.push(v);
      }
    }
    if (stack.length < 4000) {
      fail(`the stack covers only ${stack.length} pixels: nothing to measure`);
    } else {
      const buckets = new Array(22).fill(0);
      for (const v of stack) buckets[Math.min(21, Math.floor(v / 12))] += 1;
      const occupied = buckets.filter((n) => n / stack.length >= 0.005).length;
      stack.sort((a, b) => a - b);
      const spread =
        stack[Math.floor(stack.length * 0.95)] - stack[Math.floor(stack.length * 0.05)];

      if (occupied < 6) fail(`the stack occupies only ${occupied} grey bands: the layers look alike`);
      else if (spread < 55) fail(`the stack's values span only ${spread.toFixed(0)} levels`);
      else
        pass(
          `the stack occupies ${occupied} grey bands over ${spread.toFixed(0)} levels — ` +
            'the layers separate without colour'
        );
    }
  }

  if (page.problems.length) fail(`console errors with the object running: ${page.problems[0]}`);
  await page.close();
  await analyst.close();
}

await browser.close();
await new Promise((resolve) => server.close(resolve));

console.log(
  failures ? `\n${failures} failure${failures === 1 ? '' : 's'}.` : '\nAll checks passed.'
);
process.exit(failures ? 1 : 0);
