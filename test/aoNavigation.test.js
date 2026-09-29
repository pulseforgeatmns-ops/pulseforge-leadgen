'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  buildNavigateUrl,
  buildNavigateUrls,
  resolveNavigateUrl,
  isUsableAddress,
} = require('../public/shared/aoNavigation.js');
const {
  currentAoSurface,
  aoNavItemsForRole,
} = require('../public/shared/aoShellNav.js');
const fs = require('node:fs');
const path = require('node:path');

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

test('currentAoSurface maps AO routes', () => {
  assert.equal(currentAoSurface('/ao/field'), 'field');
  assert.equal(currentAoSurface('/ao/crm'), 'accounts');
  assert.equal(currentAoSurface('/ao/crm/manager'), 'manager');
  assert.equal(currentAoSurface('/ao/command-center'), 'command-center');
});

test('aoNavItemsForRole hides Manager View from field AOs', () => {
  const aoLabels = aoNavItemsForRole('ao').map(i => i.label);
  assert.deepEqual(aoLabels, ['Field Mode', 'My Accounts']);
  const mgrLabels = aoNavItemsForRole('manager').map(i => i.label);
  assert.match(mgrLabels.join(' '), /Manager View/);
});

test('AO shells mount shared top navigation', () => {
  const pages = [
    'ao-dashboard.html',
    'ao-crm.html',
    'ao-command-center.html',
    'ao-crm-manager.html',
  ];
  for (const file of pages) {
    const src = fs.readFileSync(path.join(__dirname, '..', 'public', file), 'utf8');
    assert.match(src, /aoShellNav\.js/, file);
    assert.doesNotMatch(src, /My Accounts \(CRM\)/, file);
  }
  const field = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-dashboard.html'), 'utf8');
  assert.doesNotMatch(field, /class="ao-logout" href="\/ao\/crm"/);
});
