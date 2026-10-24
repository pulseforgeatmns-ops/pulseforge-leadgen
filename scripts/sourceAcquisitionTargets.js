'use strict';

require('dotenv').config();

const axios = require('axios');
const pool = require('../db');
const { ensureAcquisitionTargetsSchema } = require('../utils/acquisitionTargetsSchema');

const PLACES_TEXTSEARCH = 'https://maps.googleapis.com/maps/api/place/textsearch/json';
const PLACES_DETAILS = 'https://maps.googleapis.com/maps/api/place/details/json';

const QUERIES = Object.freeze([
  'commercial cleaning',
  'janitorial services',
  'office cleaning',
  'cleaning service',
]);

const GEOS = Object.freeze([
  'Manchester NH',
  'Bedford NH',
  'Nashua NH',
  'Hooksett NH',
  'Goffstown NH',
  'Merrimack NH',
  'Derry NH',
  'Concord NH',
]);

const FRANCHISE_NAMES = Object.freeze([
  'Jan-Pro',
  'Jani-King',
  'Anago',
  'Coverall',
  'Vanguard Cleaning',
  'ServiceMaster',
  'Stanley Steemer',
  'Merry Maids',
  'Molly Maid',
  'The Cleaning Authority',
  'MaidPro',
  'Two Maids',
  'Office Pride',
  'Buildingstars',
  'Stratus Building Solutions',
  'City Wide',
  '360clean',
]);

const SPECIALTY_ONLY_TERMS = Object.freeze(['carpet', 'window', 'restoration', 'chem-dry', 'chemdry']);
const SPECIALTY_ALLOWED_TERMS = Object.freeze(['janitorial', 'office', 'commercial']);
const ACTIVE_REVIEW_WINDOW_MS = 365 * 24 * 60 * 60 * 1000;
const QUIET_REVIEW_WINDOW_MS = 18 * 30.4375 * 24 * 60 * 60 * 1000;

function parseArgs(argv) {
  const args = {
    dryRun: false,
    maxPerQuery: 20,
    skipWebsites: false,
  };

  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--dry-run') args.dryRun = true;
    else if (arg === '--skip-websites') args.skipWebsites = true;
    else if (arg === '--max-per-query') {
      args.maxPerQuery = Number(argv[i + 1] || 20);
      i += 1;
    }
  }

  if (!Number.isInteger(args.maxPerQuery) || args.maxPerQuery < 1 || args.maxPerQuery > 60) {
    throw new Error('--max-per-query must be an integer from 1 to 60');
  }

  return args;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function normalizeText(value) {
  return String(value || '').toLowerCase();
}

function normalizeKey(value) {
  return normalizeText(value).replace(/&/g, 'and').replace(/[^a-z0-9]/g, '');
}

function normalizeDomain(value) {
  if (!value) return null;
  try {
    const url = new URL(/^https?:\/\//i.test(value) ? value : `https://${value}`);
    return url.hostname.toLowerCase().replace(/^www\./, '');
  } catch (_err) {
    return null;
  }
}

function phoneDigits(value) {
  return String(value || '').replace(/\D/g, '');
}

function noteExcluded(stats, reason) {
  stats.excluded[reason] = (stats.excluded[reason] || 0) + 1;
}

function isAnchorCleaning(details) {
  const hay = `${details.name || ''} ${details.website || ''}`.toLowerCase();
  return hay.includes('goanchorcleaning.com') || /\banchor cleaning\b/i.test(details.name || '');
}

function franchiseMatch(name) {
  const key = normalizeKey(name);
  return FRANCHISE_NAMES.find(franchise => key.includes(normalizeKey(franchise))) || null;
}

function isSpecialtyOnly(name) {
  const hay = normalizeText(name);
  const hasSpecialty = SPECIALTY_ONLY_TERMS.some(term => hay.includes(term));
  const hasAllowed = SPECIALTY_ALLOWED_TERMS.some(term => hay.includes(term));
  return hasSpecialty && !hasAllowed;
}

function exclusionReason(details) {
  if (!details?.place_id) return 'missing_place_id';
  if (isAnchorCleaning(details)) return 'anchor_cleaning';

  const franchise = franchiseMatch(details.name);
  if (franchise) return `franchise:${franchise}`;

  const reviewCount = Number(details.user_ratings_total || 0);
  if (reviewCount > 150) return 'review_count_gt_150';
  if (isSpecialtyOnly(details.name)) return 'specialty_only';

  return null;
}

function parseAddress(formattedAddress) {
  const parts = String(formattedAddress || '').split(',').map(part => part.trim()).filter(Boolean);
  const country = parts[parts.length - 1]?.toUpperCase() === 'USA' ? parts.pop() : null;
  void country;

  const stateZipPart = parts.pop() || '';
  const city = parts.pop() || null;
  const address = parts.join(', ') || null;
  const stateZip = stateZipPart.match(/\b([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\b/);

  return {
    address,
    city,
    state: stateZip ? stateZip[1] : null,
    zip: stateZip ? stateZip[2] : null,
  };
}

function reviewDates(details, now = new Date()) {
  const dates = (details.reviews || [])
    .map(review => {
      if (review.time) return new Date(Number(review.time) * 1000);
      if (review.publishTime) return new Date(review.publishTime);
      return null;
    })
    .filter(date => date instanceof Date && !Number.isNaN(date.getTime()));

  const mostRecent = dates.length
    ? new Date(Math.max(...dates.map(date => date.getTime())))
    : null;
  const oldest = dates.length
    ? new Date(Math.min(...dates.map(date => date.getTime())))
    : null;

  return {
    mostRecentReviewDate: mostRecent ? mostRecent.toISOString().slice(0, 10) : null,
    reviewsLast12mo: dates.filter(date => now - date <= ACTIVE_REVIEW_WINDOW_MS).length,
    yearsOnGoogle: oldest ? Math.floor((now - oldest) / (365.25 * 24 * 60 * 60 * 1000)) : null,
  };
}

function resolveServiceType(details) {
  const hay = [
    details.name,
    details.website,
    details.formatted_address,
    ...(details.types || []),
    ...(details.reviews || []).map(review => review.text || review.originalText?.text || ''),
  ].filter(Boolean).join(' ').toLowerCase();

  const commercial = /\b(commercial|janitorial|office|facility|facilities|business|businesses|building maintenance)\b/.test(hay);
  const residential = /\b(residential|home|house|maid|apartment|move in|move-in|move out|move-out)\b/.test(hay);

  if (commercial && residential) return 'mixed';
  if (commercial) return 'commercial';
  if (residential) return 'residential';
  return 'unknown';
}

function classifyPhoneType(_phone) {
  return 'unknown';
}

function detectCopyrightYear(html) {
  const years = [...String(html || '').matchAll(/(?:copyright|&copy;|©)[^0-9]{0,30}(20[0-2][0-9]|19[8-9][0-9])|(?:20[0-2][0-9]|19[8-9][0-9])[^<]{0,30}(?:copyright|&copy;|©)/gi)]
    .flatMap(match => match.slice(1))
    .filter(Boolean)
    .map(Number);
  return years.length ? Math.max(...years) : null;
}

function hasTemplateArtifacts(html) {
  return /\b(lorem ipsum|coming soon|under construction|just another wordpress site|wp-content\/themes\/twenty|wixsite\.com|godaddysites\.com)\b/i.test(String(html || ''));
}

async function assessWebsiteStatus(websiteUrl) {
  if (!websiteUrl) return { status: 'none', notes: [] };

  const notes = [];
  const originalIsHttp = /^http:\/\//i.test(websiteUrl);

  try {
    const res = await axios.get(websiteUrl, {
      timeout: 12000,
      maxRedirects: 4,
      responseType: 'text',
      validateStatus: () => true,
      headers: {
        'User-Agent': 'Pulseforge acquisition research bot (+direct mail list; no automated outreach)',
      },
    });

    if (res.status >= 400) return { status: 'dead', notes: [`http_status:${res.status}`] };

    const html = String(res.data || '');
    const copyrightYear = detectCopyrightYear(html);
    const finalUrl = res.request?.res?.responseUrl || websiteUrl;
    const finalIsHttp = /^http:\/\//i.test(finalUrl);

    if (originalIsHttp || finalIsHttp) notes.push('http_only');
    if (copyrightYear && copyrightYear <= 2022) notes.push(`old_copyright:${copyrightYear}`);
    if (hasTemplateArtifacts(html)) notes.push('template_artifact');
    if (html.trim().length < 500) notes.push('thin_website');

    return {
      status: notes.length ? 'stale' : 'active',
      notes,
    };
  } catch (err) {
    return { status: 'dead', notes: [`fetch_error:${err.code || err.message}`] };
  }
}

function scoreAgingTarget(target, now = new Date()) {
  const signals = [];
  let score = 0;

  if (!target.website_url || ['none', 'dead'].includes(target.website_status)) {
    score += 25;
    signals.push('no_or_dead_website');
  } else if (target.website_status === 'stale') {
    score += 15;
    signals.push('stale_website');
  }

  if (target.most_recent_review_date) {
    const reviewAgeMs = now - new Date(`${target.most_recent_review_date}T00:00:00.000Z`);
    if (reviewAgeMs > QUIET_REVIEW_WINDOW_MS) {
      score += 20;
      signals.push('most_recent_review_over_18mo');
    }
  }

  if (Number(target.reviews_last_12mo || 0) === 0 && Number(target.review_count || 0) >= 5) {
    score += 10;
    signals.push('no_reviews_last_12mo');
  }

  if (target.phone_type === 'landline') {
    score += 10;
    signals.push('landline_phone');
  }

  if (Number(target.years_on_google || 0) >= 8) {
    score += 10;
    signals.push('established_8yr_google_floor');
  }

  const reviewCount = Number(target.review_count || 0);
  if (reviewCount >= 3 && reviewCount <= 40) {
    score += 10;
    signals.push('small_review_footprint');
  }

  return { aging_score: Math.min(score, 100), aging_signals: signals };
}

async function fetchTextSearch(query, apiKey) {
  const res = await axios.get(PLACES_TEXTSEARCH, {
    params: { query, key: apiKey },
    timeout: 30000,
  });
  const status = res.data.status;
  if (status !== 'OK' && status !== 'ZERO_RESULTS') {
    throw new Error(`Places text search ${status}: ${res.data.error_message || 'unknown error'}`);
  }
  return res.data.results || [];
}

async function fetchPlaceDetails(placeId, apiKey) {
  const res = await axios.get(PLACES_DETAILS, {
    params: {
      place_id: placeId,
      fields: 'name,formatted_address,formatted_phone_number,international_phone_number,website,place_id,rating,user_ratings_total,reviews,types,business_status',
      reviews_sort: 'newest',
      key: apiKey,
    },
    timeout: 30000,
  });

  if (res.data.status !== 'OK') {
    throw new Error(`Places details ${res.data.status}: ${res.data.error_message || placeId}`);
  }

  return res.data.result;
}

async function findPulseforgeProspectId(db, target) {
  const nameKey = normalizeKey(target.business_name);
  const digits = phoneDigits(target.phone);
  const domain = normalizeDomain(target.website_url);

  const result = await db.query(`
    SELECT p.id
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.client_id = 1
      AND (
        regexp_replace(lower(coalesce(c.name, '')), '[^a-z0-9]', '', 'g') = $1
        OR ($2 <> '' AND regexp_replace(coalesce(p.phone, ''), '\\D', '', 'g') = $2)
        OR ($3 <> '' AND (
          lower(regexp_replace(regexp_replace(coalesce(c.domain, ''), '^https?://', ''), '^www\\.', '')) = $3
          OR lower(regexp_replace(regexp_replace(coalesce(c.website, ''), '^https?://', ''), '^www\\.', '')) LIKE $4
        ))
      )
    ORDER BY p.created_at ASC
    LIMIT 1
  `, [nameKey, digits, domain || '', domain ? `%${domain}%` : '']);

  return result.rows[0]?.id || null;
}

function buildTarget(details, websiteStatus, now = new Date()) {
  const parsedAddress = parseAddress(details.formatted_address);
  const reviewInfo = reviewDates(details, now);
  const phone = details.formatted_phone_number || details.international_phone_number || null;

  const base = {
    business_name: details.name || 'Unknown',
    google_place_id: details.place_id,
    address: parsedAddress.address,
    city: parsedAddress.city,
    state: parsedAddress.state,
    zip: parsedAddress.zip,
    phone,
    phone_type: classifyPhoneType(phone),
    website_url: details.website || null,
    website_status: websiteStatus.status,
    google_rating: details.rating ?? null,
    review_count: details.user_ratings_total ?? 0,
    most_recent_review_date: reviewInfo.mostRecentReviewDate,
    reviews_last_12mo: reviewInfo.reviewsLast12mo,
    years_on_google: reviewInfo.yearsOnGoogle,
    service_type: resolveServiceType(details),
    notes: websiteStatus.notes.length ? `Website signals: ${websiteStatus.notes.join(', ')}` : null,
  };

  return {
    ...base,
    ...scoreAgingTarget(base, now),
  };
}

async function upsertTarget(db, target) {
  const result = await db.query(`
    INSERT INTO acquisition_targets (
      business_name, google_place_id, address, city, state, zip, phone, phone_type,
      website_url, website_status, google_rating, review_count, most_recent_review_date,
      reviews_last_12mo, years_on_google, service_type, aging_score, aging_signals,
      pulseforge_prospect_id, notes
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8,
      $9, $10, $11, $12, $13,
      $14, $15, $16, $17, $18::jsonb,
      $19, $20
    )
    ON CONFLICT (google_place_id) DO UPDATE SET
      business_name = EXCLUDED.business_name,
      address = EXCLUDED.address,
      city = EXCLUDED.city,
      state = EXCLUDED.state,
      zip = EXCLUDED.zip,
      phone = EXCLUDED.phone,
      phone_type = EXCLUDED.phone_type,
      website_url = EXCLUDED.website_url,
      website_status = EXCLUDED.website_status,
      google_rating = EXCLUDED.google_rating,
      review_count = EXCLUDED.review_count,
      most_recent_review_date = EXCLUDED.most_recent_review_date,
      reviews_last_12mo = EXCLUDED.reviews_last_12mo,
      years_on_google = EXCLUDED.years_on_google,
      service_type = EXCLUDED.service_type,
      aging_score = EXCLUDED.aging_score,
      aging_signals = EXCLUDED.aging_signals,
      pulseforge_prospect_id = EXCLUDED.pulseforge_prospect_id,
      notes = EXCLUDED.notes,
      updated_at = NOW()
    RETURNING (xmax = 0) AS inserted
  `, [
    target.business_name,
    target.google_place_id,
    target.address,
    target.city,
    target.state,
    target.zip,
    target.phone,
    target.phone_type,
    target.website_url,
    target.website_status,
    target.google_rating,
    target.review_count,
    target.most_recent_review_date,
    target.reviews_last_12mo,
    target.years_on_google,
    target.service_type,
    target.aging_score,
    JSON.stringify(target.aging_signals),
    target.pulseforge_prospect_id,
    target.notes,
  ]);

  return result.rows[0]?.inserted ? 'inserted' : 'updated';
}

function scoreBucket(score) {
  if (score >= 80) return '80-100';
  if (score >= 60) return '60-79';
  if (score >= 40) return '40-59';
  if (score >= 20) return '20-39';
  return '0-19';
}

async function printReviewQuery(db) {
  const sql = `
SELECT business_name, city, aging_score, aging_signals, review_count,
most_recent_review_date, website_status, phone
FROM acquisition_targets
WHERE aging_score >= 40
ORDER BY aging_score DESC, review_count ASC;`;

  console.log('\nPost-run review query:');
  console.log(sql);

  const result = await db.query(sql);
  console.log('\nRows for review:');
  console.table(result.rows);
}

async function run(options = parseArgs(process.argv)) {
  const apiKey = process.env.GOOGLE_PLACES_KEY;
  if (!apiKey) throw new Error('GOOGLE_PLACES_KEY is required');

  const stats = {
    combos: 0,
    detailsFetched: 0,
    uniquePlaceIds: 0,
    inserted: 0,
    updated: 0,
    dryRunEligible: 0,
    excluded: {},
    scoreDistribution: {},
  };
  const seenPlaceIds = new Set();

  await ensureAcquisitionTargetsSchema(pool);

  for (const geo of GEOS) {
    for (const seed of QUERIES) {
      stats.combos += 1;
      const textQuery = `${seed} ${geo}`;
      console.log(`[Places] ${textQuery}`);

      const hits = (await fetchTextSearch(textQuery, apiKey)).slice(0, options.maxPerQuery);
      for (const hit of hits) {
        if (!hit.place_id) {
          noteExcluded(stats, 'missing_place_id');
          continue;
        }

        if (seenPlaceIds.has(hit.place_id)) {
          noteExcluded(stats, 'duplicate_place_id');
          continue;
        }
        seenPlaceIds.add(hit.place_id);

        let details;
        try {
          details = await fetchPlaceDetails(hit.place_id, apiKey);
          stats.detailsFetched += 1;
        } catch (err) {
          noteExcluded(stats, 'place_details_error');
          console.warn(`[Places] Details skipped for ${hit.place_id}: ${err.message}`);
          continue;
        }

        const reason = exclusionReason(details);
        if (reason) {
          noteExcluded(stats, reason);
          continue;
        }

        const websiteStatus = options.skipWebsites
          ? { status: details.website ? 'active' : 'none', notes: ['website_check_skipped'] }
          : await assessWebsiteStatus(details.website);
        const target = buildTarget(details, websiteStatus);
        target.pulseforge_prospect_id = await findPulseforgeProspectId(pool, target);

        stats.scoreDistribution[scoreBucket(target.aging_score)] = (stats.scoreDistribution[scoreBucket(target.aging_score)] || 0) + 1;

        if (options.dryRun) {
          stats.dryRunEligible += 1;
          console.log(`[Dry run] ${target.business_name} (${target.city || 'unknown city'}) score=${target.aging_score}`);
        } else {
          const outcome = await upsertTarget(pool, target);
          stats[outcome] += 1;
          console.log(`[${outcome}] ${target.business_name} (${target.city || 'unknown city'}) score=${target.aging_score}`);
        }

        await sleep(150);
      }

      await sleep(500);
    }
  }

  stats.uniquePlaceIds = seenPlaceIds.size;

  console.log('\nAcquisition target sourcing summary');
  console.log(JSON.stringify(stats, null, 2));
  console.log(`Covered geos: ${GEOS.join(', ')}`);
  console.log(`Deduped place IDs seen: ${stats.uniquePlaceIds}`);

  if (!options.dryRun) await printReviewQuery(pool);

  return stats;
}

if (require.main === module) {
  run()
    .catch(err => {
      console.error(err.stack || err.message);
      process.exitCode = 1;
    })
    .finally(async () => {
      await pool.end();
    });
}

module.exports = {
  FRANCHISE_NAMES,
  GEOS,
  QUERIES,
  assessWebsiteStatus,
  buildTarget,
  detectCopyrightYear,
  exclusionReason,
  franchiseMatch,
  isSpecialtyOnly,
  normalizeDomain,
  parseAddress,
  reviewDates,
  resolveServiceType,
  scoreAgingTarget,
};
