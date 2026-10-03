#!/usr/bin/env node
/**
 * Generates the raster brand assets and the case-study capture.
 *
 * Everything here is derived from a committed source of truth — the SVG mark,
 * the self-hosted typefaces, and the live Anchor Cleaning source in this
 * repository — so no asset in the site is hand-exported from a design tool
 * and then drifts.
 *
 * Requires the repository's puppeteer (Node resolves it from /workspace).
 *
 *   node generate-assets.mjs            all assets
 *   node generate-assets.mjs icons      brand icons only
 *   node generate-assets.mjs social     open graph preview only
 *   node generate-assets.mjs work       case-study capture only
 */

import puppeteer from 'puppeteer';
import { readFile, mkdir, stat, writeFile, copyFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));
const site = path.resolve(here, '..');
const repo = path.resolve(site, '..', '..');
const brand = path.join(site, 'assets', 'brand');
const publicRoot = path.join(site, 'public');
const work = path.join(site, 'assets', 'work');

/** PNG-in-ICO container (16 + 32) — Safari and legacy browsers request /favicon.ico. */
function buildIcoFromPngs(pngBuffers, sizes) {
  const header = Buffer.alloc(6);
  header.writeUInt16LE(0, 0);
  header.writeUInt16LE(1, 2);
  header.writeUInt16LE(pngBuffers.length, 4);
  const entries = [];
  let offset = 6 + 16 * pngBuffers.length;
  pngBuffers.forEach((png, i) => {
    const entry = Buffer.alloc(16);
    const s = sizes[i];
    entry[0] = s >= 256 ? 0 : s;
    entry[1] = s >= 256 ? 0 : s;
    entry.writeUInt16LE(1, 4);
    entry.writeUInt16LE(32, 6);
    entry.writeUInt32LE(png.length, 8);
    entry.writeUInt32LE(offset, 12);
    offset += png.length;
    entries.push(entry);
  });
  return Buffer.concat([header, ...entries, ...pngBuffers]);
}

const only = process.argv[2] || 'all';
const wanted = (name) => only === 'all' || only === name;

const browser = await puppeteer.launch({
  args: ['--no-sandbox', '--disable-dev-shm-usage', '--font-render-hinting=none'],
});

async function shoot({
  html,
  width,
  height,
  out,
  fullPage = false,
  url = null,
  quality = null,
}) {
  const page = await browser.newPage();
  await page.setViewport({ width, height, deviceScaleFactor: 1 });
  if (url) {
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 60_000 });
  } else {
    await page.setContent(html, { waitUntil: 'load' });
  }
  await page.evaluateHandle('document.fonts.ready');
  await new Promise((resolve) => setTimeout(resolve, 400));

  const type = path.extname(out).slice(1) === 'webp' ? 'webp' : 'png';
  await page.screenshot({
    path: out,
    type,
    fullPage,
    ...(type === 'webp' && quality ? { quality } : {}),
  });
  await page.close();

  const { size } = await stat(out);
  console.log(
    `  ${path.relative(site, out)}  ${width}x${height}  ${(size / 1024).toFixed(0)} KB`
  );
}

/* --- Brand icons --------------------------------------------------------- */

if (wanted('icons')) {
  console.log('Brand icons');
  await mkdir(brand, { recursive: true });
  await mkdir(publicRoot, { recursive: true });
  const svg = await readFile(path.join(brand, 'favicon.svg'), 'utf8');
  const encoded = Buffer.from(svg).toString('base64');

  const rasterTargets = [
    ['favicon-16x16.png', 16],
    ['favicon-32x32.png', 32],
    ['favicon-32.png', 32],
    ['apple-touch-icon.png', 180],
    ['icon-192.png', 192],
    ['icon-512.png', 512],
  ];

  for (const [file, size] of rasterTargets) {
    await shoot({
      width: size,
      height: size,
      out: path.join(brand, file),
      html: `<!doctype html><meta charset="utf-8">
        <style>html,body{margin:0;background:#11110F}
        img{display:block;width:${size}px;height:${size}px;image-rendering:auto}</style>
        <img src="data:image/svg+xml;base64,${encoded}" alt="">`,
    });
  }

  const fav16 = await readFile(path.join(brand, 'favicon-16x16.png'));
  const fav32 = await readFile(path.join(brand, 'favicon-32x32.png'));
  const ico = buildIcoFromPngs([fav16, fav32], [16, 32]);
  await writeFile(path.join(brand, 'favicon.ico'), ico);

  const publicManifest = {
    name: 'Studio Substral',
    short_name: 'Substral',
    description: 'Website diagnosis before website design. Manchester, New Hampshire.',
    start_url: '/',
    display: 'browser',
    background_color: '#11110F',
    theme_color: '#11110F',
    icons: [
      { src: '/favicon-32x32.png', sizes: '32x32', type: 'image/png' },
      { src: '/apple-touch-icon.png', sizes: '180x180', type: 'image/png' },
      { src: '/assets/brand/icon-192.png', sizes: '192x192', type: 'image/png' },
      { src: '/assets/brand/icon-512.png', sizes: '512x512', type: 'image/png' },
    ],
  };
  await writeFile(
    path.join(brand, 'site.webmanifest'),
    JSON.stringify(publicManifest, null, 2) + '\n'
  );

  const rootIconBundle = [
    [path.join(brand, 'favicon.svg'), 'favicon.svg'],
    [path.join(brand, 'favicon.ico'), 'favicon.ico'],
    [path.join(brand, 'favicon-16x16.png'), 'favicon-16x16.png'],
    [path.join(brand, 'favicon-32x32.png'), 'favicon-32x32.png'],
    [path.join(brand, 'apple-touch-icon.png'), 'apple-touch-icon.png'],
    [path.join(brand, 'site.webmanifest'), 'site.webmanifest'],
  ];
  for (const [from, to] of rootIconBundle) {
    await copyFile(from, path.join(publicRoot, to));
    await copyFile(from, path.join(site, to));
  }
  console.log(`  ${path.relative(site, publicRoot)}/  (root favicon bundle)`);
  console.log(`  site root/  (GitHub Pages publish root — same bundle)`);
}

/* --- Open Graph preview -------------------------------------------------- */

if (wanted('social')) {
  console.log('Open Graph preview');
  const grotesk = await readFile(
    path.join(site, 'assets', 'fonts', 'archivo-var-latin.woff2')
  );
  const mono = await readFile(
    path.join(site, 'assets', 'fonts', 'plex-mono-400-latin.woff2')
  );
  const mark = await readFile(path.join(brand, 'favicon.svg'), 'utf8');

  await shoot({
    width: 1200,
    height: 630,
    out: path.join(brand, 'social-preview.png'),
    html: `<!doctype html><meta charset="utf-8"><style>
      @font-face{font-family:A;src:url(data:font/woff2;base64,${grotesk.toString(
        'base64'
      )}) format('woff2-variations');font-weight:100 900;font-stretch:62% 125%}
      @font-face{font-family:M;src:url(data:font/woff2;base64,${mono.toString(
        'base64'
      )}) format('woff2');font-weight:400}
      *{margin:0;box-sizing:border-box}
      body{width:1200px;height:630px;background:#11110F;color:#F0EDE5;
        font-family:A,sans-serif;display:flex;flex-direction:column;
        justify-content:space-between;padding:76px 84px;position:relative;overflow:hidden}
      body::before{content:'';position:absolute;inset:0;
        background:radial-gradient(58% 54% at 26% 34%,rgba(127,168,144,.10),transparent 70%)}
      .top{display:flex;align-items:flex-start;justify-content:space-between;position:relative}
      .name{font-size:40px;font-weight:600;font-stretch:112%;letter-spacing:.26em;
        text-transform:uppercase;line-height:1}
      .meta{font-family:M,monospace;font-size:14px;letter-spacing:.2em;
        text-transform:uppercase;color:#8A8578;margin-top:14px}
      .mark{width:56px;height:56px;flex:none}
      h1{position:relative;font-size:112px;font-weight:600;font-stretch:108%;
        line-height:.86;letter-spacing:-.03em;text-transform:uppercase}
      .foot{position:relative;display:flex;align-items:center;gap:22px;
        font-family:M,monospace;font-size:14px;letter-spacing:.16em;
        text-transform:uppercase;color:#8A8578}
      .rule{width:120px;height:1px;background:#7FA890}
    </style>
    <div class="top">
      <div>
        <div class="name">Substral</div>
        <div class="meta">Studio / Manchester, NH</div>
      </div>
      <div class="mark">${mark}</div>
    </div>
    <h1>Look beneath<br>the surface.</h1>
    <div class="foot"><span class="rule"></span><span>We measure what exists first</span></div>`,
  });
}

/* --- Case-study capture -------------------------------------------------- */

if (wanted('work')) {
  console.log('Case-study capture');
  await mkdir(work, { recursive: true });

  // Captured from the canonical source in this repository, served over HTTP so
  // its root-relative asset paths resolve. The live deployment can lag behind
  // main, and a case study should show the work as it currently stands.
  const root = path.join(repo, 'sites', 'anchor-cleaning');
  const server = await serveStatic(root);
  try {
    // WebP, not PNG: the capture is a photographic hero and a lossless
    // encode of it runs to megabytes, which the performance doctrine (§20)
    // does not permit for one below-the-fold illustration.
    await shoot({
      width: 1600,
      height: 1000,
      quality: 78,
      out: path.join(work, 'anchor-cleaning-home.webp'),
      url: `http://127.0.0.1:${server.port}/`,
    });
  } finally {
    await server.close();
  }
}

async function serveStatic(root) {
  const { createServer } = await import('node:http');
  const { createReadStream } = await import('node:fs');

  const types = {
    '.html': 'text/html; charset=utf-8',
    '.css': 'text/css',
    '.js': 'text/javascript',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon',
    '.webmanifest': 'application/manifest+json',
    '.woff2': 'font/woff2',
  };

  const server = createServer(async (req, res) => {
    const url = decodeURIComponent((req.url || '/').split('?')[0]);
    let file = path.join(root, url);
    if (url.endsWith('/')) file = path.join(file, 'index.html');
    if (!file.startsWith(root)) {
      res.writeHead(403).end();
      return;
    }
    try {
      await stat(file);
    } catch {
      res.writeHead(404).end();
      return;
    }
    res.writeHead(200, { 'Content-Type': types[path.extname(file)] || 'application/octet-stream' });
    createReadStream(file).pipe(res);
  });

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    port: server.address().port,
    close: () => new Promise((resolve) => server.close(resolve)),
  };
}

await browser.close();
console.log('Done.');
