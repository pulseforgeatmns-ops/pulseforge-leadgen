'use strict';

const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const site = path.join(__dirname, '..', 'sites', 'studio-substral');
const publicDir = path.join(site, 'public');
const PNG_SIG = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);

function assertValidIco(icoPath) {
  const buf = fs.readFileSync(icoPath);
  assert.equal(buf.readUInt16LE(0), 0);
  assert.equal(buf.readUInt16LE(2), 1);
  const count = buf.readUInt16LE(4);
  assert.ok(count >= 1, 'at least one icon frame');
  for (let i = 0; i < count; i++) {
    const entry = 6 + i * 16;
    const size = buf.readUInt32LE(entry + 8);
    const offset = buf.readUInt32LE(entry + 12);
    assert.ok(offset + size <= buf.length, `frame ${i} within file bounds`);
    assert.ok(buf.subarray(offset, offset + 8).equals(PNG_SIG), `frame ${i} is PNG-encoded`);
  }
}

const requiredRootIcons = [
  'favicon.svg',
  'favicon.ico',
  'favicon-16x16.png',
  'favicon-32x32.png',
  'apple-touch-icon.png',
  'site.webmanifest',
];

for (const file of requiredRootIcons) {
  test(`sites/studio-substral/${file} exists at publish root`, () => {
    assert.ok(
      fs.existsSync(path.join(site, file)),
      `${file} missing — run node sites/studio-substral/build/generate-assets.mjs icons`
    );
  });

  test(`sites/studio-substral/public/${file} exists`, () => {
    assert.ok(
      fs.existsSync(path.join(publicDir, file)),
      `${file} missing in public/ — run node sites/studio-substral/build/generate-assets.mjs icons`
    );
  });
}

test('publish-root favicon.ico is a valid ICO container', () => {
  assertValidIco(path.join(site, 'favicon.ico'));
});

test('index.html references versioned root favicon bundle for Safari and mobile', () => {
  const html = fs.readFileSync(path.join(site, 'index.html'), 'utf8');
  assert.match(html, /<link rel="icon" href="\/favicon\.ico\?v=6" sizes="any">/);
  assert.match(html, /<link rel="icon" href="\/favicon\.svg\?v=6" type="image\/svg\+xml">/);
  assert.match(html, /href="\/apple-touch-icon\.png\?v=6"/);
  assert.match(html, /href="\/site\.webmanifest\?v=6"/);
  assert.doesNotMatch(html, /assets\/brand\/favicon/);
  assert.doesNotMatch(html, /assets\/brand\/site\.webmanifest/);
});

test('prepare-release.mjs exports publish-root favicon bundle', () => {
  const src = fs.readFileSync(path.join(site, 'build', 'prepare-release.mjs'), 'utf8');
  assert.match(src, /'favicon\.ico'/);
  assert.match(src, /'favicon\.svg'/);
  assert.match(src, /'public'/);
  assert.match(src, /normalizeIndexFaviconHead/);
});

test('normalizeIndexFaviconHead replaces legacy assets/brand favicon links', async () => {
  const { normalizeIndexFaviconHead, FAVICON_CACHE_VERSION } = await import(
    '../sites/studio-substral/build/faviconHead.mjs'
  );
  const legacy = `<link rel="stylesheet" href="assets/css/substral.css">
<link rel="icon" href="assets/brand/favicon.svg" type="image/svg+xml">
<link rel="icon" href="assets/brand/favicon-32.png" type="image/png" sizes="32x32">
<link rel="apple-touch-icon" href="assets/brand/apple-touch-icon.png">
<link rel="manifest" href="assets/brand/site.webmanifest">
<meta name="theme-color" content="#11110F">`;
  const normalized = normalizeIndexFaviconHead(legacy);
  assert.match(normalized, new RegExp(`/favicon\\.ico\\?v=${FAVICON_CACHE_VERSION}`));
  assert.doesNotMatch(normalized, /assets\/brand/);
});
