'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  isUsableAddress,
  haversineKm,
  sortStopsByMode,
  greedyNearestNeighbor,
  buildMapsNavigateUrl,
  buildNavigateUrl,
  buildNavigateUrls,
  buildStopBrief,
  buildNextStopDebrief,
  formatSourceBadge,
} = require('../utils/aoRoutePlanner');

test('isUsableAddress rejects empty and placeholder addresses', () => {
  assert.equal(isUsableAddress(''), false);
  assert.equal(isUsableAddress('TBD'), false);
  assert.equal(isUsableAddress('n/a'), false);
  assert.equal(isUsableAddress('123 Main St, Manchester NH'), true);
});

test('haversineKm returns zero for same point', () => {
  assert.equal(haversineKm(42.99, -71.45, 42.99, -71.45), 0);
});

test('sortStopsByMode orders farthest first', () => {
  const start = { lat: 42.9956, lng: -71.4548 };
  const stops = [
    { id: 'a', lat: 42.99, lng: -71.45, sequence: 0 },
    { id: 'b', lat: 43.01, lng: -71.46, sequence: 1 },
  ];
  const ordered = sortStopsByMode(stops, start, 'farthest_first');
  assert.equal(ordered[0].id, 'b');
});

test('sortStopsByMode orders closest first', () => {
  const start = { lat: 42.9956, lng: -71.4548 };
  const stops = [
    { id: 'a', lat: 42.99, lng: -71.45, sequence: 0 },
    { id: 'b', lat: 43.01, lng: -71.46, sequence: 1 },
  ];
  const ordered = sortStopsByMode(stops, start, 'closest_first');
  assert.equal(ordered[0].id, 'a');
});

test('greedyNearestNeighbor visits nearest stop first', () => {
  const start = { lat: 42.9956, lng: -71.4548 };
  const stops = [
    { id: 'far', lat: 43.05, lng: -71.50, sequence: 0 },
    { id: 'near', lat: 42.996, lng: -71.455, sequence: 1 },
  ];
  const ordered = greedyNearestNeighbor(stops, start);
  assert.equal(ordered[0].id, 'near');
});

test('buildMapsNavigateUrl encodes destination', () => {
  const url = buildMapsNavigateUrl('123 Main St, Manchester NH');
  assert.match(url, /destination=123/);
  assert.match(url, /google\.com\/maps/);
});

test('buildNavigateUrl supports Waze and Apple Maps', () => {
  const addr = '123 Main St, Manchester NH';
  assert.match(buildNavigateUrl(addr, 'waze'), /waze\.com\/ul\?q=/);
  assert.match(buildNavigateUrl(addr, 'apple_maps'), /maps\.apple\.com\/\?daddr=/);
});

test('buildNavigateUrls returns all providers when address is usable', () => {
  const urls = buildNavigateUrls('123 Main St, Manchester NH');
  assert.ok(urls.google_maps);
  assert.ok(urls.waze);
  assert.ok(urls.apple_maps);
});

test('buildNavigateUrls returns null for missing address', () => {
  assert.equal(buildNavigateUrls('TBD'), null);
});

test('formatSourceBadge for direct mail includes campaign', () => {
  assert.equal(formatSourceBadge('direct_mail_campaign', 'Campaign 001'), 'Direct Mail · Campaign 001');
});

test('buildStopBrief for direct mail mentions campaign goal', () => {
  const brief = buildStopBrief({
    attribution_source: 'direct_mail_campaign',
    campaign_name: 'Campaign 001',
  });
  assert.match(brief, /Campaign 001/);
  assert.match(brief, /right person/i);
});

test('buildNextStopDebrief returns completion message when no next stop', () => {
  const msg = buildNextStopDebrief(null, 'Mike');
  assert.match(msg, /done with this route/i);
});

test('buildNextStopDebrief includes business name and opening', () => {
  const msg = buildNextStopDebrief({
    business_name: 'Lodgism',
    address: '100 Main St',
    attribution_source: 'direct_mail_campaign',
    campaign_name: 'Campaign 001',
    contact_name: null,
    suggested_message: null,
  }, 'Mike');
  assert.match(msg, /Lodgism/);
  assert.match(msg, /Anchor Cleaning/);
  assert.match(msg, /Navigate/);
});
