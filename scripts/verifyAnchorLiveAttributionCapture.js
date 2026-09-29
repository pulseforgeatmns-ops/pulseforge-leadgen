#!/usr/bin/env node
'use strict';

/**
 * SPEC-ANCHOR-SITE-ATTRIBUTION-001 — verify live Anchor homepage attribution capture.
 *
 * Read-only: fetches https://goanchorcleaning.com/ HTML. Does not submit forms.
 *
 * Usage:
 *   node scripts/verifyAnchorLiveAttributionCapture.js
 *   node scripts/verifyAnchorLiveAttributionCapture.js --url https://goanchorcleaning.com/
 */

const https = require('node:https');

const DEFAULT_URL = 'https://goanchorcleaning.com/';

const REQUIRED_QUERY_KEYS = Object.freeze([
  'gclid',
  'gbraid',
  'wbraid',
  'utm_source',
  'utm_medium',
  'utm_campaign',
  'utm_content',
  'utm_term',
]);

const REQUIRED_STORAGE_KEYS = Object.freeze([
  'landing_page_url',
  'referrer',
]);

function parseArgs(argv = process.argv.slice(2)) {
  const urlIdx = argv.indexOf('--url');
  const url = urlIdx >= 0 ? argv[urlIdx + 1] : DEFAULT_URL;
  return { url: url || DEFAULT_URL };
}

function fetchText(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        fetchText(new URL(res.headers.location, url).href).then(resolve).catch(reject);
        return;
      }
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        if ((res.statusCode || 0) >= 400) {
          reject(new Error(`HTTP ${res.statusCode} for ${url}`));
          return;
        }
        resolve(body);
      });
    }).on('error', reject);
  });
}

function keyPresentInAttributionBlock(html, key) {
  const quoted = new RegExp(`['"]${key}['"]`);
  return quoted.test(html);
}

function buildReport(html, url) {
  const queryKeys = Object.fromEntries(
    REQUIRED_QUERY_KEYS.map((key) => [key, keyPresentInAttributionBlock(html, key)])
  );
  const storageKeys = Object.fromEntries(
    REQUIRED_STORAGE_KEYS.map((key) => [key, keyPresentInAttributionBlock(html, key)])
  );
  const hasWhitelistLoop = /ATTRIBUTION_QUERY_KEYS\.forEach/.test(html);
  const hasSessionKey = /anchor_first_party_attribution/.test(html);

  const missing = [
    ...REQUIRED_QUERY_KEYS.filter((key) => !queryKeys[key]),
    ...REQUIRED_STORAGE_KEYS.filter((key) => !storageKeys[key]),
  ];
  if (!hasWhitelistLoop) missing.push('ATTRIBUTION_QUERY_KEYS.forEach whitelist loop');
  if (!hasSessionKey) missing.push('anchor_first_party_attribution session key');

  return {
    spec: 'SPEC-ANCHOR-SITE-ATTRIBUTION-001',
    url,
    queryKeys,
    storageKeys,
    hasWhitelistLoop,
    hasSessionKey,
    pass: missing.length === 0,
    missing,
  };
}

async function run(options = {}) {
  const html = await fetchText(options.url);
  return buildReport(html, options.url);
}

module.exports = { run, buildReport, REQUIRED_QUERY_KEYS, REQUIRED_STORAGE_KEYS };

if (require.main === module) {
  run(parseArgs())
    .then((report) => {
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = report.pass ? 0 : 2;
    })
    .catch((err) => {
      console.log(JSON.stringify({
        spec: 'SPEC-ANCHOR-SITE-ATTRIBUTION-001',
        error: err.message,
        pass: false,
      }, null, 2));
      process.exitCode = 1;
    });
}
