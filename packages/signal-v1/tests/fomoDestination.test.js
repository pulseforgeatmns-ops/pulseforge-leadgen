'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { buildFomoTokenUrl } = require('../destinations/fomo');

const SOLANA_TOKEN = 'Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump';

test('builds the observed canonical FOMO Solana token route', () => {
  assert.equal(
    buildFomoTokenUrl(SOLANA_TOKEN),
    `https://fomo.family/tokens/solana/${SOLANA_TOKEN}`
  );
});

test('normalizes surrounding whitespace without changing token identity', () => {
  assert.equal(
    buildFomoTokenUrl(`  ${SOLANA_TOKEN}\n`),
    `https://fomo.family/tokens/solana/${SOLANA_TOKEN}`
  );
});

test('fails closed for absent or invalid token identity', () => {
  for (const value of [null, undefined, '', 'not-a-token', '0x1234']) {
    assert.equal(buildFomoTokenUrl(value), null);
  }
});
