#!/usr/bin/env node
'use strict';

// Dry-run exact-match coverage for Chicago Business Owners (ezma-pppn) and
// Business Licenses (r5kz-chrr). Never persists, never prints entity names,
// never calls paid providers.

const { createHash } = require('node:crypto');
const { mapChicago, lookbackDate, sodaUrl, chicagoWhere, chicagoOwner, CHICAGO_URL, DISCOVERY_CAP } = require('../services/leviScoutBorrowerDiscovery');
const { measureChicagoExactMatchCoverage, isPaidEnrichmentUrl } = require('../services/leviScoutContactResearch');
const soda = require('../services/leviScoutSodaClient');

function hashName(value) {
  return createHash('sha256').update(String(value || '').toUpperCase().replace(/\s+/g, ' ')).digest('hex').slice(0, 12);
}

async function namesFromLivePermits({ fetchImpl, now = new Date(), cap = DISCOVERY_CAP } = {}) {
  const url = sodaUrl(CHICAGO_URL, chicagoWhere(lookbackDate(now)), 80);
  const result = await soda.readJson({ fetchImpl, url });
  const names = [];
  for (const row of result.rows) {
    const candidate = mapChicago(row, now);
    if (candidate && candidate.name) names.push(candidate.name);
    if (names.length >= cap) break;
  }
  return { names, permitRows: result.rows.length, permitStatus: result.status };
}

async function namesFromPermitUrls({ fetchImpl, urls = [] } = {}) {
  const names = [];
  for (const url of urls) {
    if (isPaidEnrichmentUrl(url)) throw Object.assign(new Error('paid_forbidden'), { code: 'levi_research_paid_provider_approval_required' });
    const result = await soda.readJson({ fetchImpl, url });
    const row = result.rows[0];
    if (!row) continue;
    const name = chicagoOwner(row);
    if (name) names.push(name);
  }
  return { names };
}

async function coverage({
  fetch: fetchImpl = globalThis.fetch,
  now = new Date(),
  names,
  permitUrls,
} = {}) {
  let resolved = Array.isArray(names) ? names : [];
  let permitStatus = null;
  let permitRows = null;
  if (!resolved.length && Array.isArray(permitUrls) && permitUrls.length) {
    resolved = (await namesFromPermitUrls({ fetchImpl, urls: permitUrls })).names;
  }
  if (!resolved.length) {
    const live = await namesFromLivePermits({ fetchImpl, now });
    resolved = live.names;
    permitStatus = live.permitStatus;
    permitRows = live.permitRows;
  }
  const measured = await measureChicagoExactMatchCoverage({ names: resolved, fetch: fetchImpl });
  return {
    dryRun: true,
    outbound: false,
    paidProvidersUsed: false,
    fabricatedContacts: 0,
    source: names ? 'supplied_names' : permitUrls ? 'permit_urls' : 'live_chicago_permits',
    permitStatus,
    permitRows,
    nameHashes: resolved.map(hashName),
    ...measured,
    results: undefined,
  };
}

module.exports = { coverage, namesFromLivePermits, hashName };

if (require.main === module) {
  Promise.resolve()
    .then(() => coverage({}))
    .then((report) => {
      console.log(JSON.stringify(report, null, 2));
      process.exit(0);
    })
    .catch((error) => {
      console.error(`[levi-scout-chicago-contact-coverage] ${error.code || error.message}`);
      process.exit(2);
    });
}
