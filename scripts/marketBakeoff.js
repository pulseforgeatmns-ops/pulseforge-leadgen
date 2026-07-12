#!/usr/bin/env node
// Read-only Scout market validation. It never inserts prospects or changes queue state.
require('dotenv').config({ quiet: true });

const axios = require('axios');
const pool = require('../db');
const { getClientConfig } = require('../utils/clientContext');
const { targetVerticalEntries } = require('../utils/verticalTiers');
const { _test: scoutTest } = require('../leadgen');

const CITIES = [
  { city: 'Boston', state: 'MA' },
  { city: 'Hartford', state: 'CT' },
  { city: 'Providence', state: 'RI' },
];
const VERTICALS = [
  'commercial_electrical', 'janitorial', 'property_management', 'staffing_recruiting', 'msp_it_services',
];
const DEFAULT_MAX_RESULTS_PER_PAIR = 5;
const PLACES_TEXTSEARCH = 'https://maps.googleapis.com/maps/api/place/textsearch/json';
const PLACES_DETAILS = 'https://maps.googleapis.com/maps/api/place/details/json';

function arg(name) {
  const prefix = `--${name}=`;
  const value = process.argv.find(item => item.startsWith(prefix));
  return value ? value.slice(prefix.length).trim().toLowerCase() : '';
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function mapWithConcurrency(values, limit, mapper) {
  const results = new Array(values.length);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, values.length) }, async () => {
    while (next < values.length) {
      const index = next++;
      results[index] = await mapper(values[index]);
    }
  }));
  return results;
}

function domainFromUrl(value) {
  try { return new URL(String(value || '').startsWith('http') ? value : `https://${value}`).hostname.replace(/^www\./i, '').toLowerCase(); } catch { return ''; }
}

function buildPairs(client) {
  const targets = new Map(targetVerticalEntries(client).map(entry => [entry.vertical, entry]));
  return CITIES.flatMap(({ city, state }) => VERTICALS.map(vertical => {
    const entry = targets.get(vertical);
    if (!entry?.autonomous_sourcing || entry.tier !== 'A' || !entry.seed_terms?.length) {
      throw new Error(`Missing Tier A seed configuration for ${vertical}`);
    }
    return {
      city,
      state,
      vertical,
      queries: entry.seed_terms.map(seed => seed.replace(/\{city\}/g, city).replace(/\{state\}/g, state)),
      query: entry.seed_terms[0].replace(/\{city\}/g, city).replace(/\{state\}/g, state),
    };
  }));
}

async function isFresh(clientId, name, website) {
  const domain = domainFromUrl(website);
  const { rows } = await pool.query(`
    SELECT 1
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.client_id = $1
      AND (
        ($2 <> '' AND (LOWER(COALESCE(c.domain, '')) = $2 OR LOWER(COALESCE(p.website_url, '')) LIKE '%' || $2 || '%'))
        OR LOWER(COALESCE(c.name, '')) = LOWER($3)
      )
    LIMIT 1
  `, [clientId, domain, name]);
  return rows.length === 0;
}

async function searchPlaces(query, apiKey, maxResults) {
  const hits = [];
  let pageToken = null;
  do {
    if (pageToken) await delay(2_000); // Google activates the next token asynchronously.
    const search = await axios.get(PLACES_TEXTSEARCH, {
      params: pageToken ? { pagetoken: pageToken, key: apiKey } : { query, key: apiKey }, timeout: 10_000,
    });
    hits.push(...(search.data.results || []));
    pageToken = search.data.next_page_token || null;
  } while (pageToken && hits.length < maxResults);
  return hits.slice(0, maxResults);
}

async function validatePair(pair, clientId, apiKey, maxResults) {
  const seenPlaceIds = new Set();
  const hits = [];
  let searches = 0;
  for (const query of pair.queries || [pair.query]) {
    const remaining = maxResults - hits.length;
    if (remaining <= 0) break;
    searches++;
    for (const hit of await searchPlaces(query, apiKey, remaining)) {
      if (seenPlaceIds.has(hit.place_id)) continue;
      seenPlaceIds.add(hit.place_id);
      hits.push(hit);
    }
  }
  const result = { ...pair, searches, returned: hits.length, b2b: 0, b2c: 0, ambiguous: 0, fresh: 0 };
  await mapWithConcurrency(hits, 5, async hit => {
    const detailsResponse = await axios.get(PLACES_DETAILS, {
      params: { place_id: hit.place_id, fields: 'name,website,types', key: apiKey }, timeout: 10_000,
    }).catch(() => null);
    const details = detailsResponse?.data?.result;
    if (!details?.website) {
      result.ambiguous++;
      return;
    }
    const classification = scoutTest.classifyPlacesB2B({
      company: details.name || hit.name || '', url: details.website, place_types: details.types || hit.types || [], snippet: '',
    }).classification;
    result[classification]++;
    if (classification === 'b2b' && await isFresh(clientId, details.name || hit.name || '', details.website)) result.fresh++;
  });
  result.fresh_yield_per_search = result.fresh / result.searches;
  return result;
}

function cityRanking(rows) {
  return CITIES.map(({ city, state }) => {
    const matching = rows.filter(row => row.city === city && row.state === state);
    const fresh = matching.reduce((total, row) => total + row.fresh, 0);
    const searches = matching.reduce((total, row) => total + row.searches, 0);
    return { city: `${city}, ${state}`, searches, fresh, fresh_yield_per_search: searches ? fresh / searches : 0 };
  }).sort((a, b) => b.fresh_yield_per_search - a.fresh_yield_per_search || b.fresh - a.fresh);
}

async function main() {
  const apiKey = process.env.GOOGLE_PLACES_KEY;
  if (!apiKey) throw new Error('GOOGLE_PLACES_KEY is required for the market bake-off');
  const client = await getClientConfig(1);
  if (!client) throw new Error('Client 1 is not active');
  const maxResults = Math.max(1, Math.min(Number(arg('max') || DEFAULT_MAX_RESULTS_PER_PAIR), 50));
  const cityFilter = arg('city');
  const pairs = buildPairs(client).filter(pair => !cityFilter || pair.city.toLowerCase() === cityFilter);
  if (!pairs.length) throw new Error(`No bake-off pairs match --city=${cityFilter}`);
  const results = [];
  for (const pair of pairs) {
    process.stderr.write(`[bake-off] ${pair.city} / ${pair.vertical}\n`);
    results.push(await validatePair(pair, 1, apiKey, maxResults));
  }
  process.stdout.write(`${JSON.stringify({
    dry_run: true,
    source: 'google_places',
    max_results_per_city_vertical: maxResults,
    pairs: results,
    market_ranking: cityRanking(results),
  }, null, 2)}\n`);
}

if (require.main === module) {
  main().catch(err => {
    console.error(err.stack || err.message);
    process.exitCode = 1;
  }).finally(() => pool.end());
}

module.exports = { buildPairs, cityRanking };
