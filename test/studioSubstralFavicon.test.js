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

test('index.html references root favicon bundle for Safari and mobile', () => {
  const html = fs.readFileSync(path.join(site, 'index.html'), 'utf8');
  assert.match(html, /<link rel="icon" href="\/favicon\.ico"/);
  assert.match(html, /<link rel="icon" href="\/favicon\.svg" type="image\/svg\+xml">/);
  assert.match(html, /href="\/apple-touch-icon\.png"/);
  assert.match(html, /href="\/site\.webmanifest"/);
});

test('prepare-release.mjs exports publish-root favicon bundle', () => {
  const src = fs.readFileSync(path.join(site, 'build', 'prepare-release.mjs'), 'utf8');
  assert.match(src, /'favicon\.ico'/);
  assert.match(src, /'favicon\.svg'/);
  assert.match(src, /'public'/);
});
