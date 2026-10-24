'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const sharp = require('sharp');

const BRAND_DIR = path.join(__dirname, '../sites/anchor-cleaning/assets/brand');
const VERSION = '20260930';

describe('Anchor Google Ads business logo assets', () => {
  it('includes square 1200px source and size test sheet', async () => {
    const logoPath = path.join(BRAND_DIR, `google-ads-business-logo-v${VERSION}.png`);
    const testPath = path.join(BRAND_DIR, `google-ads-logo-size-test-v${VERSION}.png`);
    assert.ok(fs.existsSync(logoPath), '1200px source must be generated');
    assert.ok(fs.existsSync(testPath), 'size test sheet must be generated');
    const meta = await sharp(logoPath).metadata();
    assert.equal(meta.width, 1200);
    assert.equal(meta.height, 1200);
    assert.equal(meta.format, 'png');
  });
});
