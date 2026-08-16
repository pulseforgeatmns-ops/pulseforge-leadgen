'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  buildNavigateUrl,
  buildNavigateUrls,
  resolveNavigateUrl,
  isUsableAddress,
} = require('../public/shared/aoNavigation.js');

test('client aoNavigation mirrors server URL formats', () => {
  const addr = '65 Middle Street Unit B, Manchester, NH 03101';
  assert.match(buildNavigateUrl(addr, 'google_maps'), /google\.com\/maps\/dir/);
  assert.match(buildNavigateUrl(addr, 'waze'), /waze\.com\/ul\?q=/);
  assert.match(buildNavigateUrl(addr, 'apple_maps'), /maps\.apple\.com\/\?daddr=/);
});

test('resolveNavigateUrl defaults to Google Maps', () => {
  const addr = '123 Main St, Manchester NH';
  const urls = buildNavigateUrls(addr);
  assert.equal(resolveNavigateUrl(addr, urls, 'google_maps'), urls.google_maps);
  assert.equal(resolveNavigateUrl(addr, urls, null), urls.google_maps);
});

test('resolveNavigateUrl returns null for ask every time', () => {
  const addr = '123 Main St, Manchester NH';
  const urls = buildNavigateUrls(addr);
  assert.equal(resolveNavigateUrl(addr, urls, 'ask_every_time'), null);
});

test('isUsableAddress rejects placeholders', () => {
  assert.equal(isUsableAddress('Address needed'), false);
  assert.equal(isUsableAddress('100 Main St, Manchester NH'), true);
});
