'use strict';

/**
 * Scout sourcing quality gates (Anchor / NH property-manager campaigns).
 *
 * Hard-rejects UK / non-US geography, cleaning competitors, and large
 * institutional firms (unless operator-approved). Assigns each candidate
 * status: accepted | review_required | rejected.
 *
 * No CRM writes, outreach copy, sends, or account changes.
 */

const PRIORITY_TOWNS_NH = Object.freeze([
  'Bedford',
  'Hooksett',
  'Londonderry',
  'Auburn',
  'Goffstown',
]);

const NEARBY_FILL_TOWNS_NH = Object.freeze(['Manchester']);

/** NH towns outside the approved pilot cluster — never accepted unless explicitly allowed. */
const EXTENDED_REVIEW_TOWNS_NH = Object.freeze([
  'Concord',
  'Derry',
  'Nashua',
  'Salem',
  'Merrimack',
  'Amherst',
  'Windham',
  'Hudson',
  'Milford',
]);

const ALL_NH_PILOT_TOWNS = Object.freeze([
  ...PRIORITY_TOWNS_NH,
  ...NEARBY_FILL_TOWNS_NH,
]);

const ALL_KNOWN_NH_TOWNS = Object.freeze([
  ...ALL_NH_PILOT_TOWNS,
  ...EXTENDED_REVIEW_TOWNS_NH,
]);

/** UK-only place names that must never be treated as NH Manchester. */
const UK_MANCHESTER_PLACE_MARKERS = Object.freeze([
  'deansgate',
  'salford',
  'stockport',
  'bolton',
  'oldham',
  'rochdale',
  'bury',
  'wigan',
  'trafford',
  'tameside',
  'piccadilly',
  'spinningfields',
  'ancoats',
  'chorlton',
  'didsbury',
  'altrincham',
  'sale,',
  'eccles',
  'prestwich',
  'stalybridge',
  'ashton-under-lyne',
  'm1 ',
  'm2 ',
  'm3 ',
]);

const CANDIDATE_STATUS = Object.freeze({
  ACCEPTED: 'accepted',
  REVIEW_REQUIRED: 'review_required',
  REJECTED: 'rejected',
});

/** Machine rejection / risk reason codes (stable for tests + UI). */
const REJECTION_REASON = Object.freeze({
  OUTSIDE_MARKET_COUNTRY: 'outside_market_country',
  OUTSIDE_MARKET_REGION: 'outside_market_region',
  WRONG_SEGMENT_CLEANING_COMPETITOR: 'wrong_segment_cleaning_competitor',
  LARGE_INSTITUTIONAL_FIRM: 'large_institutional_firm',
  OUTSIDE_PRIMARY_TOWN_CLUSTER: 'outside_primary_town_cluster',
  OUTSIDE_NH_MARKET: 'outside_nh_market',
  WEAK_PROPERTY_MANAGEMENT_FIT: 'weak_property_management_fit',
});

/** US state name → code. Used to hard-reject non-NH regions before review_required. */
const US_STATE_NAME_TO_CODE = Object.freeze({
  alabama: 'AL',
  alaska: 'AK',
  arizona: 'AZ',
  arkansas: 'AR',
  california: 'CA',
  colorado: 'CO',
  connecticut: 'CT',
  delaware: 'DE',
  'district of columbia': 'DC',
  florida: 'FL',
  georgia: 'GA',
  hawaii: 'HI',
  idaho: 'ID',
  illinois: 'IL',
  indiana: 'IN',
  iowa: 'IA',
  kansas: 'KS',
  kentucky: 'KY',
  louisiana: 'LA',
  maine: 'ME',
  maryland: 'MD',
  massachusetts: 'MA',
  michigan: 'MI',
  minnesota: 'MN',
  mississippi: 'MS',
  missouri: 'MO',
  montana: 'MT',
  nebraska: 'NE',
  nevada: 'NV',
  'new hampshire': 'NH',
  'new jersey': 'NJ',
  'new mexico': 'NM',
  'new york': 'NY',
  'north carolina': 'NC',
  'north dakota': 'ND',
  ohio: 'OH',
  oklahoma: 'OK',
  oregon: 'OR',
  pennsylvania: 'PA',
  'rhode island': 'RI',
  'south carolina': 'SC',
  'south dakota': 'SD',
  tennessee: 'TN',
  texas: 'TX',
  utah: 'UT',
  vermont: 'VT',
  virginia: 'VA',
  washington: 'WA',
  'west virginia': 'WV',
  wisconsin: 'WI',
  wyoming: 'WY',
});

const US_STATE_CODES = new Set(Object.values(US_STATE_NAME_TO_CODE));

const CONFIDENCE = Object.freeze({
  HIGH: 'high',
  MEDIUM: 'medium',
  REVIEW_REQUIRED: 'review_required',
});

const MAPS_ONLY_SOURCE_RE =
  /google\.com\/maps|maps\.google\.|goo\.gl\/maps|maps\.app\.goo\.gl/i;

/**
 * True when the listing has a real company website (not a Google Maps URL).
 * Accepts either `website` or a non-maps `sourceUrl` as company-site evidence.
 */
function hasNonMapsCompanyWebsite(hit, opts = {}) {
  const candidates = [
    hit && hit.website,
    hit && hit.sourceUrl,
    hit && hit.url,
    opts.sourceUrl,
  ];
  for (const raw of candidates) {
    const url = String(raw || '').trim();
    if (!url) continue;
    if (MAPS_ONLY_SOURCE_RE.test(url)) continue;
    // Require an http(s) URL that is not a maps link.
    if (/^https?:\/\//i.test(url)) return true;
  }
  return false;
}

/**
 * Maps-only / directory-only public source — no company website evidence.
 * These must not stay `accepted` / `high` (e.g. Cedar Management Group).
 */
function isMapsOnlyPublicSource(hit, opts = {}) {
  if (hasNonMapsCompanyWebsite(hit, opts)) return false;
  const sourceUrl = String(
    (hit && (hit.sourceUrl || hit.url)) || opts.sourceUrl || ''
  ).trim();
  const website = String((hit && hit.website) || '').trim();
  if (!sourceUrl && !website) return false;
  return (
    MAPS_ONLY_SOURCE_RE.test(sourceUrl) || MAPS_ONLY_SOURCE_RE.test(website)
  );
}

/** Word/phrase UK markers checked only against candidate address/source geo text. */
const UK_GEO_PHRASE_MARKERS = Object.freeze([
  'united kingdom',
  'greater manchester',
  'manchester, england',
  'manchester england',
  'scotland',
  'wales',
  'salford',
  'stockport',
  'bolton',
  'oldham',
  'rochdale',
  'wigan',
  'trafford',
  'tameside',
  'lancashire',
  'cheshire',
]);

/** UK place tokens that need word-boundary matching (avoid "New England" → england). */
const UK_GEO_WORD_MARKERS = Object.freeze([
  'england',
  'scotland',
  'wales',
  'salford',
  'stockport',
  'uk',
  'u.k.',
  'u.k',
]);

const NON_US_MARKERS = Object.freeze([
  'canada',
  'ontario',
  'quebec',
  'australia',
  'new south wales',
  'ireland',
  'dublin',
  'germany',
  'france',
  'spain',
  'mexico',
  'india',
]);

/**
 * UK Manchester outward codes (M1–M90). Matched as postcode tokens on address text
 * only — never on campaign/brief prose.
 */
const UK_MANCHESTER_POSTCODE_RE =
  /\bM(?:[1-9]|[1-9]\d)\s*\d[A-Z]{2}\b|\bM(?:[1-9]|[1-9]\d)\b(?=\s|,|$)/i;
const CLEANING_COMPETITOR_PATTERNS = Object.freeze([
  /\bcleaning\s+(?:company|service|services|crew|co\.?|llc|inc)\b/i,
  /\b(?:company|service|services)\s+cleaning\b/i,
  /\bcleaners?\b/i,
  /\bclean\s+(?:co\.?|company|llc|inc|services?)\b/i,
  /\b(?:co\.?|company|llc|inc)\s+clean(?:ing)?\b/i,
  /\bmaid\s+service/i,
  /\bmaids?\b/i,
  /\bhousekeeping\b/i,
  /\bjanitorial\b/i,
  /\bjanitor\b/i,
  /\bcarpet\s+cleaning\b/i,
  /\boffice\s+cleaning\b/i,
  /\bcommercial\s+cleaning\b/i,
  /\bresidential\s+cleaning\b/i,
  /\bcleaning\s+competitor/i,
  /\bbuilding\s+maintenance\s+cleaning\b/i,
  /\bmerry\s+maids?\b/i,
  /\bmolly\s+maid/i,
  /\bthe\s+maids\b/i,
  /\bservice\s*master\s*clean/i,
  /\bsparkle\s+clean/i,
  /\bmaid\s*brigade\b/i,
  /\bhouse\s*clean(?:ing|ers?)?\b/i,
  /\bbuilding\s+cleaning\b/i,
  /\bpressure\s+washing\b/i,
  /\bwindow\s+cleaning\b/i,
]);

/** Place-types / industry tokens that mark cleaning competitors. */
const CLEANING_TYPE_TOKENS = Object.freeze([
  'cleaning',
  'maid',
  'housekeeping',
  'janitorial',
  'carpet_cleaning',
  'carpet cleaning',
  'laundry',
  'cleaner',
]);

const INSTITUTIONAL_PATTERNS = Object.freeze([
  /\bnationwide\b/i,
  /\bnational\b/i,
  /\bmulti[- ]state\b/i,
  /\bmultistate\b/i,
  /\bfranchise\b/i,
  /\binstitutional\b/i,
  /\bfortune\s*500\b/i,
  /\boffices?\s+nationwide\b/i,
  /\boffices?\s+across\b/i,
  /\bregional\s+offices\b/i,
  /\bpublicly\s+traded\b/i,
  /\bnyse\b/i,
  /\bnasdaq\b/i,
  /\bcbre\b/i,
  /\bjll\b/i,
  /\bjones\s+lang\s+lasalle\b/i,
  /\bcushman\b/i,
  /\bcolliers\b/i,
  /\bkennedy\s+wilson\b/i,
  /\bgreystar\b/i,
  /\blincoln\s+property\b/i,
  /\bbrookfield\b/i,
]);

const PROPERTY_MANAGEMENT_TOKENS = Object.freeze([
  'property management',
  'property manager',
  'property_management',
  'real estate management',
  'real_estate_management',
  'commercial property management',
  'real_estate_agency',
  'real estate agency',
  'real_estate',
  'apartment management',
  'hoa management',
  'facility management',
  'facilities management',
  'commercial property',
  'multi-family',
  'multifamily',
  'property_managers',
  'property mgmt',
  'prop management',
]);

function hitHaystack(hit) {
  return [
    hit && hit.companyName,
    hit && hit.name,
    hit && hit.address,
    hit && hit.location,
    hit && hit.marketTown,
    hit && hit.industry,
    hit && hit.snippet,
    hit && hit.website,
    hit && hit.sourceUrl,
    hit && hit.formatted_address,
    ...((hit && hit.placeTypes) || []),
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();
}

function paddedHay(hay) {
  return ` ${String(hay || '').toLowerCase()} `;
}

/**
 * Geography evidence from the candidate listing only.
 * Never includes campaign objective, market bounds, target segment, fit
 * rationale, inclusion/exclusion criteria, or other brief/plan prose.
 */
function candidateGeoEvidence(hit) {
  const h = hit || {};
  const addressParts = [
    h.formatted_address,
    h.formattedAddress,
    h.address,
    h.vicinity,
    h.location,
    h.city,
    h.town,
    h.marketTown,
    h.state,
    h.stateCode,
    h.state_code,
    h.administrative_area_level_1,
    h.administrativeArea,
    h.administrative_area,
    h.region,
    h.postal_code,
    h.postalCode,
    h.zip,
  ]
    .filter((v) => v != null && String(v).trim())
    .map((v) => String(v).trim());
  const country = String(
    h.country || h.countryCode || h.country_code || ''
  ).trim();
  const sourceUrl = String(h.sourceUrl || h.website || h.url || '').trim();
  const phone = String(
    h.phone || h.formatted_phone_number || h.international_phone_number || ''
  ).trim();
  return {
    addressText: addressParts.join(' '),
    country,
    sourceUrl,
    phone,
    combined: [addressParts.join(' '), country].filter(Boolean).join(' '),
  };
}

/** True when address text cites New Hampshire explicitly (not mere USA). */
function hasNhStateEvidence(text) {
  return /\bNew Hampshire\b|\bNH\b/i.test(String(text || ''));
}

/** True when address text cites USA / United States (country only — not NH). */
function hasUsCountryEvidence(text) {
  return /\bUSA\b|\bU\.S\.A\.?\b|\bUnited States(?:\s+of\s+America)?\b/i.test(
    String(text || '')
  );
}

/**
 * US/NH country-or-state evidence for UK gate early-exit only.
 * Does NOT prove New Hampshire market membership — use hasNhStateEvidence / extractUsStateCode.
 */
function hasUsOrNhAddressEvidence(text) {
  return hasNhStateEvidence(text) || hasUsCountryEvidence(text);
}

/**
 * Extract a US state/region code from candidate address text.
 * Prefers explicit NH / DC patterns so "Washington, DC, USA" never becomes NH.
 */
function extractUsStateCode(text) {
  const s = String(text || '').trim();
  if (!s) return null;

  if (
    /\bDistrict of Columbia\b/i.test(s) ||
    /\bWashington\s*,?\s*D\.?C\.?\b/i.test(s) ||
    /,\s*DC\b/i.test(s) ||
    /\bDC\s+\d{5}/i.test(s) ||
    /\bD\.C\.\b/i.test(s)
  ) {
    return 'DC';
  }

  if (/\bNew Hampshire\b/i.test(s) || /,\s*NH\b/i.test(s) || /\bNH\s+\d{5}/i.test(s)) {
    return 'NH';
  }
  // Bare "Town NH" / "Town, NH" without ZIP (common Maps short form).
  if (/\bNH\b/i.test(s)) return 'NH';

  const lower = s.toLowerCase();
  const names = Object.keys(US_STATE_NAME_TO_CODE).sort(
    (a, b) => b.length - a.length
  );
  for (const name of names) {
    if (name === 'washington') {
      if (/\bWashington\s+State\b/i.test(s) || /,\s*WA\b/i.test(s) || /\bWA\s+\d{5}/i.test(s)) {
        return 'WA';
      }
      continue;
    }
    if (new RegExp(`\\b${name.replace(/\s+/g, '\\s+')}\\b`, 'i').test(lower)) {
      return US_STATE_NAME_TO_CODE[name];
    }
  }

  const commaCode = s.match(/,\s*([A-Za-z]{2})\b(?:\s+\d{5}(?:-\d{4})?)?/);
  if (commaCode) {
    const code = commaCode[1].toUpperCase();
    if (US_STATE_CODES.has(code)) return code;
  }
  const zipCode = s.match(/\b([A-Za-z]{2})\s+\d{5}(?:-\d{4})?\b/);
  if (zipCode) {
    const code = zipCode[1].toUpperCase();
    if (US_STATE_CODES.has(code)) return code;
  }
  const trail = s.match(
    /\b([A-Za-z]{2})\s*,?\s*(?:USA|U\.S\.A\.?|United States(?:\s+of\s+America)?)?\s*$/i
  );
  if (trail) {
    const code = trail[1].toUpperCase();
    if (US_STATE_CODES.has(code)) return code;
  }
  return null;
}

/**
 * Hard region gate: non-NH US states/regions (DC, VA, MA, …) are outside market.
 * Must run before any review_required assignment.
 */
function detectOutsideMarketRegion(hit) {
  const geo = candidateGeoEvidence(hit);
  const hay = [geo.addressText, geo.combined, geo.country].filter(Boolean).join(' ');
  if (!String(hay || '').trim()) {
    return { rejected: false, reason: null, reasonCode: null, state: null };
  }

  const state = extractUsStateCode(hay);
  const code = REJECTION_REASON.OUTSIDE_MARKET_REGION;

  if (state === 'NH') {
    return { rejected: false, reason: null, reasonCode: null, state: 'NH' };
  }

  if (state && state !== 'NH') {
    return {
      rejected: true,
      reasonCode: code,
      reason: `Outside New Hampshire market region (${state}) — outside_market_region`,
      state,
    };
  }

  // Explicit DC / VA phrasing without a clean state code parse.
  if (
    /\bArlington\b/i.test(hay) &&
    /\bVA\b|Virginia/i.test(hay) &&
    !hasNhStateEvidence(hay)
  ) {
    return {
      rejected: true,
      reasonCode: code,
      reason: 'Outside New Hampshire market region (VA) — outside_market_region',
      state: 'VA',
    };
  }

  return { rejected: false, reason: null, reasonCode: null, state: null };
}

/** England as UK — never "New England". */
function containsUkEnglandToken(text) {
  const s = String(text || '').replace(/\bnew\s+england\b/gi, ' ');
  return /\bengland\b/i.test(s);
}

function containsUkWordMarker(text, marker) {
  if (marker === 'england') return containsUkEnglandToken(text);
  const escaped = String(marker).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`\\b${escaped}\\b`, 'i').test(String(text || ''));
}

/**
 * Anchor's market is New Hampshire, USA — never UK Greater Manchester.
 * Ambiguous "Greater Manchester" / bare "Manchester" resolve to NH pilot towns.
 */
function interpretAnchorMarket(marketBounds, opts = {}) {
  const raw = String(marketBounds || opts.location || '').trim();
  const lower = raw.toLowerCase();
  const looksUk =
    /\buk\b|united kingdom|england|salford|stockport/.test(lower) &&
    !/\bnh\b|new hampshire/.test(lower);

  const towns = [];
  for (const town of ALL_NH_PILOT_TOWNS) {
    if (new RegExp(`\\b${town}\\b`, 'i').test(raw)) {
      towns.push(town);
    }
  }

  const ambiguousGreaterManchester =
    /\bgreater\s+manchester\b/i.test(raw) && !/\bnh\b|new hampshire/i.test(raw);
  const forceNh =
    opts.forceNewHampshire === true ||
    opts.clientSlug === 'cleaning-co' ||
    opts.clientId === 10 ||
    opts.scoringProfile === 'cleaning_buyer' ||
    /new hampshire|\bnh\b|bedford|hooksett|londonderry|auburn|goffstown/i.test(
      raw
    ) ||
    ambiguousGreaterManchester ||
    (!looksUk && /\bmanchester\b/i.test(raw));

  const priorityTowns = PRIORITY_TOWNS_NH.filter((t) =>
    towns.length ? towns.includes(t) : true
  );
  const nearbyTowns = NEARBY_FILL_TOWNS_NH.filter((t) =>
    towns.length ? towns.includes(t) || !towns.some((x) => PRIORITY_TOWNS_NH.includes(x)) : true
  );

  // When towns were explicitly listed without Manchester, keep Manchester as fill-only.
  const explicitPriority = PRIORITY_TOWNS_NH.filter((t) => towns.includes(t));
  const usePriority = explicitPriority.length
    ? explicitPriority
    : forceNh
      ? [...PRIORITY_TOWNS_NH]
      : towns.filter((t) => PRIORITY_TOWNS_NH.includes(t));
  const useNearby =
    towns.includes('Manchester') || forceNh || !towns.length
      ? [...NEARBY_FILL_TOWNS_NH]
      : nearbyTowns;

  return {
    country: forceNh || !looksUk ? 'US' : looksUk ? 'UK' : 'US',
    state: 'NH',
    stateName: 'New Hampshire',
    marketLabel: 'New Hampshire, USA',
    rawMarketBounds: raw || null,
    interpretedAsNewHampshire: Boolean(forceNh || !looksUk),
    rejectedAsUkMarket: Boolean(looksUk && !forceNh),
    priorityTowns: usePriority.length ? usePriority : [...PRIORITY_TOWNS_NH],
    nearbyFillTowns: useNearby,
    allTowns: [
      ...(usePriority.length ? usePriority : [...PRIORITY_TOWNS_NH]),
      ...useNearby,
    ],
  };
}

/**
 * Build Places queries as `{segment} {town} NH` / `New Hampshire` every time.
 */
function buildNhScopedSearchQueries(workRequest, opts = {}) {
  const market = interpretAnchorMarket(
    (workRequest && (workRequest.marketBounds || workRequest.location)) || '',
    opts
  );
  const segment =
    (workRequest && (workRequest.targetSegment || workRequest.segment)) ||
    'property managers';
  const subtype =
    (workRequest && (workRequest.targetSubtype || workRequest.subtype)) || null;

  const queries = [];
  const push = (q) => {
    const text = String(q || '').trim();
    if (text && !queries.includes(text)) queries.push(text);
  };

  for (const town of market.priorityTowns) {
    push(`${segment} ${town} NH`);
    push(`${segment} ${town} New Hampshire`);
    if (subtype && String(subtype).length < 60) {
      push(`${subtype} ${town} NH`);
    }
  }

  // Manchester NH is fill-only — queried after priority towns.
  for (const town of market.nearbyFillTowns) {
    push(`${segment} ${town} NH`);
    push(`${segment} ${town} New Hampshire`);
  }

  const inclusions = Array.isArray(workRequest && workRequest.inclusionCriteria)
    ? workRequest.inclusionCriteria
    : [];
  for (const inc of inclusions.slice(0, 2)) {
    const text = String(inc || '').trim();
    if (!text || text.length > 70) continue;
    // Never re-query ambiguous "Greater Manchester" without NH.
    if (/greater\s+manchester/i.test(text) && !/\bnh\b|new hampshire/i.test(text)) {
      for (const town of market.priorityTowns.slice(0, 3)) {
        push(`property managers ${town} NH`);
      }
      continue;
    }
    for (const town of market.priorityTowns.slice(0, 2)) {
      push(`${text} ${town} NH`);
    }
  }

  return {
    market,
    queries: queries.length
      ? queries
      : [`property managers Bedford NH`, `property managers Hooksett NH`],
  };
}

function hasUsOrNhStateToken(text) {
  return hasUsOrNhAddressEvidence(text);
}

/**
 * Classify UK / non-US geography from candidate address/source fields only.
 * Campaign objective, market bounds, target segment, fit rationale, and
 * plan phrases like "Greater Manchester" must never drive this decision.
 */
function detectUkOrNonUs(hit) {
  const geo = candidateGeoEvidence(hit);
  const addressText = geo.addressText || '';
  const combined = geo.combined || '';
  const addressHay = paddedHay(combined);
  const website = String(geo.sourceUrl || '').toLowerCase();
  const phone = String(geo.phone || '').trim();
  const code = REJECTION_REASON.OUTSIDE_MARKET_COUNTRY;

  // Explicit NH / USA / New Hampshire on the candidate address → never UK reject.
  if (hasUsOrNhAddressEvidence(addressText) || hasUsOrNhAddressEvidence(geo.country)) {
    return { rejected: false, reason: null, reasonCode: null };
  }

  if (/\.co\.uk\b/i.test(website) || website.includes('.uk/') || /\.uk\b/i.test(website)) {
    return {
      rejected: true,
      reasonCode: code,
      reason: 'UK / non-US domain (.co.uk) — outside_market_country',
    };
  }
  if (/^\+44\b|^0044\b/.test(phone)) {
    return {
      rejected: true,
      reasonCode: code,
      reason: 'UK phone country code (+44) — outside_market_country',
    };
  }
  if (geo.country) {
    const country = String(geo.country).trim().toLowerCase();
    if (country && !/^(us|usa|united states|united states of america)$/.test(country)) {
      return {
        rejected: true,
        reasonCode: code,
        reason: `Non-US country (${geo.country}) — outside_market_country`,
      };
    }
  }

  for (const marker of UK_GEO_PHRASE_MARKERS) {
    if (addressHay.includes(marker)) {
      return {
        rejected: true,
        reasonCode: code,
        reason: `UK geography detected (${marker.trim()}) — outside_market_country`,
      };
    }
  }
  for (const marker of UK_GEO_WORD_MARKERS) {
    if (containsUkWordMarker(combined, marker)) {
      return {
        rejected: true,
        reasonCode: code,
        reason: `UK geography detected (${marker}) — outside_market_country`,
      };
    }
  }
  if (UK_MANCHESTER_POSTCODE_RE.test(combined)) {
    return {
      rejected: true,
      reasonCode: code,
      reason: 'UK Manchester M-postcode on candidate address — outside_market_country',
    };
  }
  for (const marker of UK_MANCHESTER_PLACE_MARKERS) {
    if (addressHay.includes(marker)) {
      return {
        rejected: true,
        reasonCode: code,
        reason: `UK Greater Manchester place marker (${marker.trim()}) — outside_market_country`,
      };
    }
  }

  // Bare / ambiguous "Manchester" on address without NH/USA token is UK risk.
  if (
    /\bmanchester\b/i.test(combined) &&
    !hasUsOrNhAddressEvidence(combined)
  ) {
    const otherPriorityNh = PRIORITY_TOWNS_NH.some((t) =>
      new RegExp(`\\b${t}\\b`, 'i').test(combined)
    );
    if (!otherPriorityNh) {
      return {
        rejected: true,
        reasonCode: code,
        reason:
          'Ambiguous Manchester without NH/USA token — outside_market_country',
      };
    }
  }

  for (const marker of NON_US_MARKERS) {
    if (addressHay.includes(marker)) {
      return {
        rejected: true,
        reasonCode: code,
        reason: `Non-US geography detected (${marker}) — outside_market_country`,
      };
    }
  }

  return { rejected: false, reason: null, reasonCode: null };
}

function isNhLocation(hit, market) {
  const geo = candidateGeoEvidence(hit);
  const loc = geo.addressText || geo.combined || '';
  const stateCode = extractUsStateCode(loc) || extractUsStateCode(geo.country);
  const hasNh = stateCode === 'NH' || hasNhStateEvidence(loc);
  const town = matchTown(loc);

  if (hasNh) {
    return {
      inNh: true,
      country: 'US',
      state: 'NH',
      town,
      tier: townTier(town || loc),
    };
  }

  // Non-NH US state/region — not New Hampshire (even if USA/United States present).
  if (stateCode && stateCode !== 'NH') {
    return {
      inNh: false,
      town,
      tier: 'out_of_market',
      country: 'US',
      state: stateCode,
      outsideMarketRegion: true,
    };
  }

  // Priority/extended NH town names without NH/USA token are not auto-accepted as NH.
  // Manchester without NH was already hard-rejected in detectUkOrNonUs.
  if (town && town !== 'Manchester') {
    return {
      inNh: false,
      town,
      tier: townTier(town),
      missingStateToken: true,
      country: null,
      state: null,
    };
  }

  if (market && market.interpretedAsNewHampshire) {
    return { inNh: false, town: null, tier: 'out_of_market', country: null, state: null };
  }
  return { inNh: false, town: null, tier: 'unknown', country: null, state: null };
}
function matchTown(text) {
  const s = String(text || '').toLowerCase();
  for (const town of ALL_KNOWN_NH_TOWNS) {
    if (new RegExp(`\\b${town.toLowerCase()}\\b`, 'i').test(s)) return town;
  }
  return null;
}

function townTier(textOrTown) {
  const town = matchTown(textOrTown) || textOrTown;
  const lower = String(town || '').toLowerCase();
  if (PRIORITY_TOWNS_NH.some((t) => t.toLowerCase() === lower)) {
    return 'priority';
  }
  if (NEARBY_FILL_TOWNS_NH.some((t) => t.toLowerCase() === lower)) {
    return 'nearby_fill';
  }
  if (EXTENDED_REVIEW_TOWNS_NH.some((t) => t.toLowerCase() === lower)) {
    return 'extended_review';
  }
  return 'other';
}

function extendedTownsExplicitlyAllowed(workRequest) {
  if (
    workRequest &&
    (workRequest.allowExtendedNhTowns === true ||
      workRequest.allowConcordDerry === true ||
      workRequest.allowOutOfClusterNh === true)
  ) {
    return true;
  }
  const blob = [
    workRequest && workRequest.marketBounds,
    ...((workRequest && workRequest.inclusionCriteria) || []),
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();
  return EXTENDED_REVIEW_TOWNS_NH.some((t) => blob.includes(t.toLowerCase()));
}

function detectCleaningCompetitor(hit, workRequest) {
  const segment = String(
    (workRequest && (workRequest.targetSegment || workRequest.segment)) || ''
  ).toLowerCase();
  // Only hard-reject cleaning companies when the campaign targets property managers
  // (Anchor buys cleaning — PMs are prospects; cleaning cos are competitors).
  const targetingPm = /property\s*manager/.test(segment) || !segment;
  if (!targetingPm && /cleaning/.test(segment)) {
    return { rejected: false, reason: null, reasonCode: null };
  }

  const name = String((hit && (hit.companyName || hit.name)) || '');
  const industry = String((hit && (hit.industry || hit.snippet)) || '');
  const website = String((hit && (hit.website || hit.sourceUrl || hit.url)) || '');
  const hay = hitHaystack(hit);
  const types = ((hit && hit.placeTypes) || []).map((t) => String(t).toLowerCase());
  const code = REJECTION_REASON.WRONG_SEGMENT_CLEANING_COMPETITOR;

  // Clear PM / real-estate companies are not cleaning competitors even if a
  // directory blurb mentions "cleaning".
  const clearPmName =
    /\bproperty\s+management\b|\bproperty\s+manager\b|\breal\s+estate\s+management\b|\breal\s+estate\b/i.test(
      name
    ) &&
    !/\bcleaning\b|\bmaid\b|\bjanitorial\b|\bhousekeeping\b|\bcleaners?\b/i.test(name);
  if (clearPmName) {
    return { rejected: false, reason: null, reasonCode: null };
  }

  for (const token of CLEANING_TYPE_TOKENS) {
    if (types.some((t) => t.includes(token.replace(/\s+/g, '_')))) {
      return {
        rejected: true,
        reasonCode: code,
        reason: `Cleaning competitor / service type (${token}) — wrong_segment_cleaning_competitor`,
      };
    }
  }

  for (const re of CLEANING_COMPETITOR_PATTERNS) {
    if (re.test(name) || re.test(industry)) {
      return {
        rejected: true,
        reasonCode: code,
        reason:
          'Cleaning service / maid / housekeeping / janitorial / carpet cleaning competitor — wrong_segment_cleaning_competitor',
      };
    }
  }

  // Website/domain signal only when there is no property-management token on the listing.
  const hasPmSignal = PROPERTY_MANAGEMENT_TOKENS.some((t) => hay.includes(t));
  if (
    !hasPmSignal &&
    (/maid|housekeep|janitor|carpet.?clean|clean(?:ing|ers?)/i.test(website) ||
      CLEANING_COMPETITOR_PATTERNS.some((re) => re.test(website)))
  ) {
    return {
      rejected: true,
      reasonCode: code,
      reason:
        'Cleaning competitor website/domain signal — wrong_segment_cleaning_competitor',
    };
  }

  // Name contains a standalone cleaning stem without PM signal.
  if (
    /\bclean(?:ing|ers?)?\b/i.test(name) &&
    !/\bproperty\b|\breal\s+estate\b/i.test(name)
  ) {
    return {
      rejected: true,
      reasonCode: code,
      reason:
        'Cleaning competitor name signal — wrong_segment_cleaning_competitor',
    };
  }

  return { rejected: false, reason: null, reasonCode: null };
}

function detectInstitutional(hit) {
  const hay = hitHaystack(hit);
  const name = String((hit && (hit.companyName || hit.name)) || '');
  for (const re of INSTITUTIONAL_PATTERNS) {
    if (re.test(name) || re.test(hay)) {
      return {
        rejected: true,
        reasonCode: REJECTION_REASON.LARGE_INSTITUTIONAL_FIRM,
        reason:
          'Large institutional / national firm — large_institutional_firm (rejected unless operator explicitly approves)',
        matched: String(re),
      };
    }
  }
  return { rejected: false, reason: null, reasonCode: null };
}

function hasPropertyManagementFit(hit, workRequest) {
  const hay = hitHaystack(hit);
  const segment = String(
    (workRequest && (workRequest.targetSegment || workRequest.segment)) || ''
  ).toLowerCase();
  if (/property\s*manager/.test(segment) || !segment) {
    return PROPERTY_MANAGEMENT_TOKENS.some((t) => hay.includes(t));
  }
  // Non-PM segments: token overlap with segment words.
  return segment
    .split(/\s+/)
    .filter((t) => t.length >= 4)
    .some((t) => hay.includes(t));
}

function hasReachableContactSignal(hit) {
  if (hit && (hit.phone || hit.formatted_phone_number)) return true;
  if (hit && (hit.contactName || hit.verifiedContactName || hit.decisionMakerName)) {
    return true;
  }
  if (hit && hit.email) return true;
  // Website contact page signal is weak — counts only with explicit flag.
  if (hit && hit.hasContactPage === true) return true;
  return false;
}

function hasVerifiedNamedContact(hit) {
  const name = hit && (hit.contactName || hit.verifiedContactName || hit.decisionMakerName);
  const title = hit && (hit.contactTitle || hit.verifiedContactTitle || hit.jobTitle);
  return Boolean(name && String(name).trim() && title && String(title).trim());
}

function formatSuggestedContactRole(workRequest, hit) {
  if (hasVerifiedNamedContact(hit)) {
    const name = String(
      hit.contactName || hit.verifiedContactName || hit.decisionMakerName
    ).trim();
    const title = String(
      hit.contactTitle || hit.verifiedContactTitle || hit.jobTitle
    ).trim();
    return `${name} — ${title}`;
  }
  let role =
    (hit && (hit.suggestedContactRole || hit.contactRole)) ||
    defaultRoleForSegment(workRequest);
  role = String(role || '')
    .replace(/^suggested contact role:\s*/i, '')
    .trim();
  // Never overclaim a verified owner/decision-maker without source person+title.
  if (/^owner\s*\/\s*decision-?maker$/i.test(role)) {
    role = defaultRoleForSegment(workRequest);
  }
  if (/^owner\s*\/\s*decision-?maker$/i.test(role)) {
    role = 'property / operations contact';
  }
  return `Suggested contact role: ${role}`;
}

function defaultRoleForSegment(workRequest) {
  const segment = String(
    (workRequest && (workRequest.targetSegment || workRequest.segment)) || ''
  ).toLowerCase();
  if (/property\s*manager/.test(segment)) return 'Owner / property manager';
  if (/law/.test(segment)) return 'Office manager / managing partner';
  if (/account/.test(segment)) return 'Office manager / principal';
  return 'property / operations contact';
}

/**
 * Fit rationale must cite source-specific evidence — not copy inclusion criteria.
 * Always includes location, category/type, website/source, and PM relevance when present.
 */
function buildSourceSpecificFitRationale(hit, workRequest, geo) {
  const company = String((hit && (hit.companyName || hit.name)) || 'Listing').trim();
  const location = String(
    (hit && (hit.location || hit.address || hit.formatted_address)) || ''
  ).trim();
  const types = ((hit && hit.placeTypes) || []).filter(Boolean);
  const industry = String((hit && (hit.industry || hit.snippet)) || '').trim();
  const website = hit && (hit.website || hit.sourceUrl);
  const pmFit = hasPropertyManagementFit(hit, workRequest);
  const parts = [];

  parts.push(`${company} sourced from public listing`);
  if (location) {
    parts.push(`address/location on source: ${location}`);
  } else {
    parts.push('location not confirmed on source');
  }
  if (types.length) {
    parts.push(`listing category/type: ${types.slice(0, 4).join(', ')}`);
  } else if (industry) {
    parts.push(`listing category/type: ${industry.slice(0, 80)}`);
  } else {
    parts.push('listing category/type not present on source');
  }
  if (website) {
    parts.push(`website/source signal: ${website}`);
  } else {
    parts.push('website/source signal missing');
  }
  if (pmFit) {
    const pmToken =
      PROPERTY_MANAGEMENT_TOKENS.find((t) => hitHaystack(hit).includes(t)) ||
      'property management';
    parts.push(`property-management relevance on source: ${pmToken}`);
  } else {
    parts.push('property-management relevance not evidenced on source');
  }
  if (geo && geo.town) {
    parts.push(
      geo.tier === 'priority'
        ? `priority NH town match: ${geo.town}`
        : geo.tier === 'nearby_fill' || geo.tier === 'extended_review'
          ? `${geo.town} NH outside_primary_town_cluster`
          : `town signal: ${geo.town}`
    );
  }
  if (hit && hit.googleRating != null) {
    parts.push(`Google rating ${hit.googleRating}`);
  }

  const rationale = parts.join(' — ');
  if (isGenericCriteriaCopy(rationale, workRequest) || parts.length < 4) {
    return {
      ok: false,
      rationale,
      reason: 'Fit rationale lacks source-specific evidence',
    };
  }
  return { ok: true, rationale, reason: null };
}

function isGenericCriteriaCopy(rationale, workRequest) {
  const text = String(rationale || '').toLowerCase().replace(/\s+/g, ' ').trim();
  if (!text) return true;
  const criteria = [
    ...((workRequest && workRequest.inclusionCriteria) || []),
    ...((workRequest && workRequest.exclusionCriteria) || []),
    workRequest && workRequest.targetSegment,
    workRequest && workRequest.targetSubtype,
    workRequest && workRequest.marketBounds,
  ]
    .map((c) => String(c || '').toLowerCase().replace(/\s+/g, ' ').trim())
    .filter((c) => c.length >= 12);

  for (const c of criteria) {
    if (text === c || text.includes(c) || (c.length >= 24 && c.includes(text))) {
      return true;
    }
  }

  // Template phrases that only restate the brief (no source facts).
  const hasSourceFacts =
    /\bhttps?:\/\//i.test(text) ||
    /\bnh\b|\bstreet\b|\broad\b|\bave\b|\bsuite\b|\blisting types\b|address\/location on source/i.test(
      text
    );
  if (
    /^(public listing matches|matches target segment|fits inclusion criteria|meets campaign criteria|local pm in market|why they fit)/i.test(
      text
    ) &&
    !hasSourceFacts
  ) {
    return true;
  }
  if (
    /manage offices|mixed-use buildings|recurring cleaning weekly|greater manchester/i.test(
      text
    ) &&
    !hasSourceFacts
  ) {
    return true;
  }

  return false;
}

function operatorApprovedInstitutional(workRequest) {
  return Boolean(
    workRequest &&
      (workRequest.allowInstitutional === true ||
        workRequest.operatorApprovedInstitutional === true ||
        workRequest.approveInstitutionalFirms === true)
  );
}

/**
 * Evaluate a mapped/public hit and return gate decision fields.
 *
 * @returns {{
 *   status: 'accepted'|'review_required'|'rejected',
 *   statusReason: string,
 *   confidence: string,
 *   fitRationale: string,
 *   suggestedContactRole: string,
 *   risks: string,
 *   exclusionRisk: boolean,
 *   geo: object,
 *   signals: object
 * }}
 */
function evaluateScoutCandidate(hit, workRequest, opts = {}) {
  const market =
    opts.market ||
    interpretAnchorMarket(
      (workRequest && (workRequest.marketBounds || workRequest.location)) || '',
      opts
    );

  const uk = detectUkOrNonUs(hit);
  if (uk.rejected) {
    return rejectResult(hit, workRequest, {
      statusReason: uk.reason,
      rejectionReason: uk.reasonCode || REJECTION_REASON.OUTSIDE_MARKET_COUNTRY,
      exclusionRisk: true,
      market,
    });
  }

  // Hard state/region gate BEFORE review_required — DC/VA/MA/etc. never usable.
  const region = detectOutsideMarketRegion(hit);
  if (region.rejected) {
    return rejectResult(hit, workRequest, {
      statusReason: region.reason,
      rejectionReason:
        region.reasonCode || REJECTION_REASON.OUTSIDE_MARKET_REGION,
      exclusionRisk: true,
      market,
    });
  }

  const cleaning = detectCleaningCompetitor(hit, workRequest);
  if (cleaning.rejected) {
    return rejectResult(hit, workRequest, {
      statusReason: cleaning.reason,
      rejectionReason:
        cleaning.reasonCode || REJECTION_REASON.WRONG_SEGMENT_CLEANING_COMPETITOR,
      exclusionRisk: true,
      market,
    });
  }

  const institutional = detectInstitutional(hit);
  if (institutional.rejected && !operatorApprovedInstitutional(workRequest)) {
    return rejectResult(hit, workRequest, {
      statusReason: institutional.reason,
      rejectionReason:
        institutional.reasonCode || REJECTION_REASON.LARGE_INSTITUTIONAL_FIRM,
      exclusionRisk: true,
      market,
    });
  }

  // Geography + segment classification first — fit rationale is generated after
  // these signals exist and must cite candidate source fields only (never brief
  // market-bounds / "Greater Manchester" plan prose).
  const geo = isNhLocation(hit, market);
  const pmFit = hasPropertyManagementFit(hit, workRequest);
  const contactSignal = hasReachableContactSignal(hit);
  const sourceUrl = Boolean(
    (hit && (hit.sourceUrl || hit.website || hit.url)) || opts.sourceUrl
  );
  const fit = buildSourceSpecificFitRationale(hit, workRequest, geo);
  const role = formatSuggestedContactRole(workRequest, hit);
  const targetingPm =
    /property\s*manager/i.test(
      String((workRequest && (workRequest.targetSegment || workRequest.segment)) || '')
    ) ||
    !(workRequest && (workRequest.targetSegment || workRequest.segment));
  const extendedAllowed = extendedTownsExplicitlyAllowed(workRequest);
  const outsidePrimaryCluster =
    geo.tier === 'nearby_fill' ||
    geo.tier === 'extended_review' ||
    geo.tier === 'other';

  const risks = [];
  let exclusionRisk = Boolean(
    institutional.rejected && operatorApprovedInstitutional(workRequest)
  );
  if (institutional.rejected && operatorApprovedInstitutional(workRequest)) {
    risks.push(
      `large_institutional_firm — included only because operator approved (never high confidence)`
    );
    exclusionRisk = true;
  }
  const exclusionCriteriaHit = softExclusionCriteriaMatch(hit, workRequest);
  if (exclusionCriteriaHit) {
    exclusionRisk = true;
    risks.push(`Exclusion criteria match: ${exclusionCriteriaHit}`);
  }
  const mapsOnly = isMapsOnlyPublicSource(hit, opts);
  const hasCompanyWebsite = !mapsOnly && hasNonMapsCompanyWebsite(hit);
  if (!hasCompanyWebsite) {
    risks.push(
      'No company website on listing — Google Maps / directory source only (downgrade to review_required / medium)'
    );
  }
  if (!geo.inNh) {
    risks.push('Location not confirmed in New Hampshire, USA');
  }
  if (geo.missingStateToken) {
    risks.push('Town matched but NH/New Hampshire state token missing on listing');
  }
  if (outsidePrimaryCluster) {
    risks.push(
      `${REJECTION_REASON.OUTSIDE_PRIMARY_TOWN_CLUSTER} — ${
        geo.town || 'town'
      } NH is not in Bedford/Hooksett/Londonderry/Auburn/Goffstown unless explicitly approved`
    );
  }
  if (!pmFit) {
    risks.push(
      `${REJECTION_REASON.WEAK_PROPERTY_MANAGEMENT_FIT} — weak property-management fit signal on public source`
    );
  }
  if (!contactSignal) {
    risks.push('No reachable contact signal (phone / named contact) on public listing');
  }
  if (!fit.ok) {
    risks.push(fit.reason);
  }
  if (!sourceUrl) {
    risks.push('Missing source URL');
  }

  // Hard reject out-of-NH when we can determine location at all.
  if (
    !geo.inNh &&
    !geo.missingStateToken &&
    (hit.location || hit.address || hit.formatted_address)
  ) {
    const regionReject =
      geo.outsideMarketRegion ||
      (geo.state && geo.state !== 'NH') ||
      Boolean(extractUsStateCode(geo.town ? `${geo.town} ${hit.location || hit.address || ''}` : (hit.location || hit.address || '')));
    return rejectResult(hit, workRequest, {
      statusReason: regionReject
        ? `Outside New Hampshire market region${
            geo.state && geo.state !== 'NH' ? ` (${geo.state})` : ''
          } — outside_market_region`
        : 'Outside New Hampshire, USA market — outside_market_country',
      rejectionReason: regionReject
        ? REJECTION_REASON.OUTSIDE_MARKET_REGION
        : REJECTION_REASON.OUTSIDE_MARKET_COUNTRY,
      exclusionRisk: true,
      market,
      fitRationale: fit.rationale,
      suggestedContactRole: role,
      risks,
    });
  }

  // Missing NH/USA state token on a known town → never accepted.
  if (geo.missingStateToken && geo.town) {
    const missingTokenRisks = risks.slice();
    if (!missingTokenRisks.some((r) => r.includes(REJECTION_REASON.OUTSIDE_PRIMARY_TOWN_CLUSTER))) {
      missingTokenRisks.push(REJECTION_REASON.OUTSIDE_PRIMARY_TOWN_CLUSTER);
    }
    return {
      ...reviewResult(hit, workRequest, {
        statusReason: `${geo.town} listing missing NH/USA state token — verify New Hampshire, USA`,
        confidence: CONFIDENCE.REVIEW_REQUIRED,
        fitRationale: fit.rationale,
        suggestedContactRole: role,
        risks: missingTokenRisks,
        exclusionRisk,
        geo,
        market,
        reasonCode: REJECTION_REASON.OUTSIDE_PRIMARY_TOWN_CLUSTER,
        signals: {
          sourceUrl,
          nhLocation: false,
          propertyManagementFit: pmFit,
          reachableContactSignal: contactSignal,
          exclusionRisk,
          priorityTown: false,
          nearbyFillTown: geo.tier === 'nearby_fill',
        },
      }),
    };
  }

  const signals = {
    sourceUrl,
    nhLocation: Boolean(geo.inNh && !geo.missingStateToken),
    propertyManagementFit: pmFit,
    reachableContactSignal: contactSignal,
    exclusionRisk,
    priorityTown: geo.tier === 'priority',
    nearbyFillTown: geo.tier === 'nearby_fill',
    extendedReviewTown: geo.tier === 'extended_review',
  };

  let confidence = CONFIDENCE.REVIEW_REQUIRED;
  const highEligible =
    signals.sourceUrl &&
    signals.nhLocation &&
    signals.propertyManagementFit &&
    signals.reachableContactSignal &&
    !signals.exclusionRisk &&
    fit.ok &&
    geo.tier === 'priority' &&
    !institutional.rejected;

  if (highEligible) {
    confidence = CONFIDENCE.HIGH;
  } else if (
    signals.sourceUrl &&
    signals.nhLocation &&
    signals.propertyManagementFit &&
    !signals.exclusionRisk &&
    geo.tier === 'priority' &&
    !institutional.rejected
  ) {
    confidence = CONFIDENCE.MEDIUM;
  } else {
    confidence = CONFIDENCE.REVIEW_REQUIRED;
  }

  // Institutional / exclusion matches must never be high confidence.
  if (signals.exclusionRisk || institutional.rejected) {
    confidence = CONFIDENCE.REVIEW_REQUIRED;
  }

  let status = CANDIDATE_STATUS.ACCEPTED;
  let statusReason = 'Passes NH property-manager quality gates';
  let reasonCode = null;

  if (!sourceUrl) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'Missing source URL';
  } else if (!geo.inNh) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'NH location not confirmed';
  } else if (targetingPm && !pmFit) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'Property-management fit not evidenced on source';
    reasonCode = REJECTION_REASON.WEAK_PROPERTY_MANAGEMENT_FIT;
  } else if (!fit.ok) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = fit.reason || 'Source-specific fit rationale incomplete';
  } else if (geo.tier === 'nearby_fill' || geo.tier === 'extended_review' || geo.tier === 'other') {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    reasonCode = REJECTION_REASON.OUTSIDE_PRIMARY_TOWN_CLUSTER;
    if (geo.tier === 'nearby_fill') {
      statusReason = `Manchester NH outside_primary_town_cluster — review_required unless primary town approval exists`;
    } else if (geo.tier === 'extended_review') {
      statusReason = extendedAllowed
        ? `${geo.town} NH outside_primary_town_cluster — explicitly allowed but still review_required (not a primary town)`
        : `${geo.town} NH outside_primary_town_cluster — review_required unless explicitly approved`;
    } else {
      statusReason =
        'Location outside_primary_town_cluster — review_required unless explicitly approved';
    }
  } else if (!contactSignal) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'Reachable contact signal missing — review before outreach';
  } else if (exclusionRisk) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = institutional.rejected
      ? 'large_institutional_firm — operator review required (never high confidence)'
      : 'Exclusion risk present — operator review required';
    if (institutional.rejected) {
      reasonCode = REJECTION_REASON.LARGE_INSTITUTIONAL_FIRM;
    }
  } else if (geo.tier !== 'priority') {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'Not in priority NH town cluster — outside_primary_town_cluster';
    reasonCode = REJECTION_REASON.OUTSIDE_PRIMARY_TOWN_CLUSTER;
  }

  // Accepted only for primary NH towns with property-management evidence.
  if (
    status === CANDIDATE_STATUS.ACCEPTED &&
    !(
      geo.tier === 'priority' &&
      signals.nhLocation &&
      pmFit &&
      sourceUrl &&
      fit.ok &&
      !exclusionRisk &&
      !institutional.rejected
    )
  ) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'Does not meet accepted primary-town property-manager quality bar';
    if (outsidePrimaryCluster) {
      reasonCode = REJECTION_REASON.OUTSIDE_PRIMARY_TOWN_CLUSTER;
    }
  }

  // Maps-only / no company website → never accepted, never high confidence.
  if (mapsOnly) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason =
      'Maps-only public source with no company website — review_required / medium';
    if (confidence === CONFIDENCE.HIGH || confidence === CONFIDENCE.REVIEW_REQUIRED) {
      confidence = CONFIDENCE.MEDIUM;
    }
  }

  const risksText = risks.length
    ? risks.join('; ')
    : 'Public-source only — verify contact before outreach';

  return {
    status,
    statusReason,
    rejectionReason: null,
    reasonCode,
    confidence,
    fitRationale: fit.rationale,
    suggestedContactRole: role,
    risks: risksText,
    exclusionRisk,
    geo,
    signals: {
      ...signals,
      hasCompanyWebsite,
      mapsOnly,
    },
    market,
  };
}

/**
 * Downgrade maps-only rows on an already-completed Scout batch (review artifact).
 * Does not invent candidates — only adjusts status/confidence/risks in place copies.
 */
function applyMapsOnlyDowngradeToCandidate(row) {
  if (!row || typeof row !== 'object') return row;
  const hit = {
    ...row,
    website: row.website,
    sourceUrl: row.sourceUrl || row.url || null,
  };
  if (!isMapsOnlyPublicSource(hit)) return { ...row };
  const risks = String(row.risks || '');
  const mapsRisk =
    'No company website on listing — Google Maps / directory source only (downgrade to review_required / medium)';
  return {
    ...row,
    status: CANDIDATE_STATUS.REVIEW_REQUIRED,
    statusReason:
      row.statusReason && /maps-only|no company website/i.test(String(row.statusReason))
        ? row.statusReason
        : 'Maps-only public source with no company website — review_required / medium',
    confidence:
      row.confidence === CONFIDENCE.HIGH ||
      row.confidence === CONFIDENCE.REVIEW_REQUIRED ||
      !row.confidence
        ? CONFIDENCE.MEDIUM
        : row.confidence,
    risks: risks.includes('No company website')
      ? risks
      : risks
        ? `${risks}; ${mapsRisk}`
        : mapsRisk,
    reviewStatus: 'review_required',
  };
}

function applyMapsOnlyDowngradeToBatch(batch) {
  if (!batch || typeof batch !== 'object') return batch;
  const candidates = Array.isArray(batch.candidates)
    ? batch.candidates.map(applyMapsOnlyDowngradeToCandidate)
    : [];
  const rejected = Array.isArray(batch.rejected)
    ? batch.rejected.map((r) => ({ ...r }))
    : [];
  const fromGroupsRejected =
    batch.groups && Array.isArray(batch.groups.rejected)
      ? batch.groups.rejected.map((r) => ({ ...r }))
      : [];
  const allRejected = rejected.length ? rejected : fromGroupsRejected;
  const groups = groupCandidatesByStatus(candidates, allRejected.slice());
  return {
    ...batch,
    candidates,
    rejected: groups.rejected,
    groups,
    acceptedCount: groups.accepted.length,
    reviewRequiredCount: groups.review_required.length,
    rejectedCount: groups.rejected.length,
  };
}

function softExclusionCriteriaMatch(hit, workRequest) {
  const exclusions = Array.isArray(workRequest && workRequest.exclusionCriteria)
    ? workRequest.exclusionCriteria
    : [];
  if (!exclusions.length) return null;
  const hay = hitHaystack(hit);
  const STOP = new Set([
    'large',
    'property',
    'properties',
    'managers',
    'manager',
    'companies',
    'company',
    'services',
    'service',
    'other',
    'results',
    'outside',
    'approved',
    'hampshire',
    'greater',
    'manchester',
  ]);
  for (const ex of exclusions) {
    const token = String(ex || '').trim().toLowerCase();
    if (!token || token.length < 8) continue;
    if (
      !/cleaning|maid|housekeeping|janitorial|carpet|institutional|national|franchise|salford|stockport|\buk\b/.test(
        token
      )
    ) {
      continue;
    }
    // Require distinctive exclusion tokens — not generic "property"/"managers".
    const words = token
      .split(/[^a-z0-9]+/i)
      .filter((w) => w.length >= 6 && !STOP.has(w));
    if (words.some((w) => hay.includes(w))) return ex;
  }
  return null;
}

function reviewResult(hit, workRequest, fields = {}) {
  const risks = Array.isArray(fields.risks)
    ? fields.risks.join('; ')
    : fields.risks || fields.statusReason || 'Review required';
  return {
    status: CANDIDATE_STATUS.REVIEW_REQUIRED,
    statusReason: fields.statusReason || 'Review required',
    rejectionReason: null,
    reasonCode: fields.reasonCode || null,
    confidence: fields.confidence || CONFIDENCE.REVIEW_REQUIRED,
    fitRationale:
      fields.fitRationale ||
      buildSourceSpecificFitRationale(hit, workRequest, fields.geo).rationale,
    suggestedContactRole:
      fields.suggestedContactRole || formatSuggestedContactRole(workRequest, hit),
    risks,
    exclusionRisk: Boolean(fields.exclusionRisk),
    geo: fields.geo || null,
    signals: fields.signals || null,
    market: fields.market || null,
  };
}

function rejectResult(hit, workRequest, extra = {}) {
  const geo =
    extra.geo ||
    isNhLocation(hit, extra.market || interpretAnchorMarket(workRequest && workRequest.marketBounds));
  const fit =
    extra.fitRationale ||
    buildSourceSpecificFitRationale(hit, workRequest, geo).rationale;
  const reason = extra.statusReason || 'Rejected by Scout quality gate';
  const rejectionReason =
    extra.rejectionReason ||
    extra.reasonCode ||
    reason;
  return {
    status: CANDIDATE_STATUS.REJECTED,
    statusReason: reason,
    rejectionReason,
    reasonCode:
      extra.reasonCode ||
      (typeof extra.rejectionReason === 'string' &&
      /^[a-z][a-z0-9_]+$/.test(extra.rejectionReason)
        ? extra.rejectionReason
        : null),
    confidence: CONFIDENCE.REVIEW_REQUIRED,
    fitRationale: fit,
    suggestedContactRole:
      extra.suggestedContactRole || formatSuggestedContactRole(workRequest, hit),
    risks: Array.isArray(extra.risks)
      ? extra.risks.join('; ')
      : extra.risks || reason,
    exclusionRisk: extra.exclusionRisk !== false,
    geo,
    signals: {
      sourceUrl: Boolean(hit && (hit.sourceUrl || hit.website)),
      nhLocation: Boolean(geo && geo.inNh),
      propertyManagementFit: hasPropertyManagementFit(hit, workRequest),
      reachableContactSignal: hasReachableContactSignal(hit),
      exclusionRisk: true,
    },
    market: extra.market || null,
  };
}

/**
 * Prefer priority-town accepted rows; use Manchester/Derry NH review_required
 * only when accepted primary-town count is still below target.
 * Do NOT defer Manchester because review_required rows filled batch.length.
 */
function selectBatchWithManchesterFill(evaluatedRows, targetMax = 25, targetMin = 15) {
  const acceptedPriority = [];
  const reviewPriority = [];
  const reviewManchester = [];
  const reviewExtended = [];
  const reviewOther = [];
  const rejected = [];

  for (const row of evaluatedRows) {
    if (!row) continue;
    if (row.status === CANDIDATE_STATUS.REJECTED) {
      rejected.push(ensureRejectionReason(row));
      continue;
    }
    const tier = (row.geo && row.geo.tier) || townTier(row.location || '');
    // Force non-priority accepted rows down to review_required.
    if (row.status === CANDIDATE_STATUS.ACCEPTED && tier !== 'priority') {
      row.status = CANDIDATE_STATUS.REVIEW_REQUIRED;
      row.statusReason =
        row.statusReason ||
        'Downgraded — only priority NH towns can be accepted';
    }
    if (row.status === CANDIDATE_STATUS.ACCEPTED) {
      acceptedPriority.push(row);
      continue;
    }
    if (tier === 'priority') reviewPriority.push(row);
    else if (tier === 'nearby_fill') reviewManchester.push(row);
    else if (tier === 'extended_review') reviewExtended.push(row);
    else reviewOther.push(row);
  }

  const batch = [];
  const take = (arr) => {
    for (const row of arr) {
      if (batch.length >= targetMax) break;
      batch.push(row);
    }
  };

  take(acceptedPriority);
  take(reviewPriority);

  // Expansion fill is gated on accepted primary count — not total batch.length.
  // review_required noise must not trigger "priority towns filled the batch".
  const acceptedPrimaryMeetsTarget = acceptedPriority.length >= targetMin;

  if (!acceptedPrimaryMeetsTarget) {
    // Accepted primary towns below target → keep Manchester/Derry as expansion.
    take(reviewManchester);
    take(reviewExtended);
    take(reviewOther);
  } else {
    for (const row of reviewManchester) {
      rejected.push(
        ensureRejectionReason({
          ...row,
          status: CANDIDATE_STATUS.REJECTED,
          statusReason:
            'Manchester NH deferred — outside_primary_town_cluster (priority towns filled the batch)',
          rejectionReason: REJECTION_REASON.OUTSIDE_PRIMARY_TOWN_CLUSTER,
          reasonCode: REJECTION_REASON.OUTSIDE_PRIMARY_TOWN_CLUSTER,
        })
      );
    }
    for (const row of reviewExtended.concat(reviewOther)) {
      rejected.push(
        ensureRejectionReason({
          ...row,
          status: CANDIDATE_STATUS.REJECTED,
          statusReason:
            row.statusReason ||
            `${REJECTION_REASON.OUTSIDE_PRIMARY_TOWN_CLUSTER} — deferred from batch (accepted primary towns met target)`,
          rejectionReason: REJECTION_REASON.OUTSIDE_PRIMARY_TOWN_CLUSTER,
          reasonCode: REJECTION_REASON.OUTSIDE_PRIMARY_TOWN_CLUSTER,
        })
      );
    }
  }

  const groups = groupCandidatesByStatus(batch, rejected.slice());
  return {
    candidates: batch,
    rejected: groups.rejected,
    groups,
    usableCount: batch.length,
    acceptedCount: (groups.accepted || []).length,
    reviewRequiredCount: (groups.review_required || []).length,
    rejectedCount: (groups.rejected || []).length,
  };
}

function ensureRejectionReason(row) {
  if (!row) return row;
  if (row.status === CANDIDATE_STATUS.REJECTED) {
    return {
      ...row,
      rejectionReason:
        row.rejectionReason ||
        row.reasonCode ||
        row.statusReason ||
        'Rejected',
    };
  }
  return { ...row, rejectionReason: row.rejectionReason || null };
}

/**
 * Count accepted + review_required candidates that may be reviewed.
 * Rejected rows (UK, cleaning competitors, deferred out-of-cluster, etc.)
 * never count toward the usable threshold.
 */
function countUsablePropertyManagerCandidates(candidates = [], workRequest = null) {
  void workRequest;
  let n = 0;
  for (const row of candidates || []) {
    if (!row || row.status === CANDIDATE_STATUS.REJECTED) continue;
    if (
      row.status === CANDIDATE_STATUS.ACCEPTED ||
      row.status === CANDIDATE_STATUS.REVIEW_REQUIRED
    ) {
      n += 1;
    }
  }
  return n;
}

function batchMeetsQualityThreshold(candidates, workRequest, opts = {}) {
  const targetMin =
    Number(opts.targetMin) ||
    Number(workRequest && workRequest.targetCountMin) ||
    15;
  const usable = countUsablePropertyManagerCandidates(candidates, workRequest);
  return {
    ok: usable >= targetMin,
    usableCount: usable,
    targetMin,
  };
}

function groupCandidatesByStatus(candidates = [], rejected = []) {
  const accepted = [];
  const reviewRequired = [];
  for (const row of candidates || []) {
    if (!row) continue;
    if (row.status === CANDIDATE_STATUS.ACCEPTED) accepted.push(row);
    else if (row.status === CANDIDATE_STATUS.REJECTED) {
      // Should not be in candidates; move defensively.
      rejected.push(ensureRejectionReason(row));
    } else reviewRequired.push(row);
  }
  return {
    accepted,
    review_required: reviewRequired,
    rejected: (rejected || []).map(ensureRejectionReason),
  };
}

/**
 * Apply quality gates to raw candidate rows (including injected scoutSourcingFn paths).
 */
function gateScoutCandidateRows(rows, workRequest, opts = {}) {
  const evaluated = [];
  const rejected = [];
  const list = Array.isArray(rows) ? rows : [];
  for (let i = 0; i < list.length; i += 1) {
    const row = list[i] || {};
    const companyName =
      row.companyName || row.name || row.propertyManagerName || row.company || null;
    const sourceUrl = row.sourceUrl || row.website || row.url || null;
    if (!companyName || !sourceUrl) continue;
    const hit = {
      ...row,
      companyName: String(companyName).trim(),
      sourceUrl,
      website: row.website || sourceUrl,
      location: row.location || row.marketTown || row.address || null,
      address: row.address || row.location || null,
      industry: row.industry || row.snippet || null,
      placeTypes: row.placeTypes || row.types || [],
      phone: row.phone || row.formatted_phone_number || null,
    };
    const gate = evaluateScoutCandidate(hit, workRequest, opts);
    const merged = {
      id: row.id || `scout-gated-${i + 1}`,
      companyName: hit.companyName,
      sourceUrl,
      website: hit.website,
      location: hit.location,
      address: hit.address,
      industry: hit.industry,
      placeTypes: hit.placeTypes,
      phone: hit.phone,
      marketTown: row.marketTown || (gate.geo && gate.geo.town) || hit.location,
      segment: row.segment || (workRequest && workRequest.targetSegment) || null,
      subtype: row.subtype || (workRequest && workRequest.targetSubtype) || null,
      fitRationale: gate.fitRationale,
      risks: gate.risks,
      suggestedContactRole: gate.suggestedContactRole,
      confidence: gate.confidence,
      status: gate.status,
      statusReason: gate.statusReason,
      reasonCode: gate.reasonCode || null,
      rejectionReason:
        gate.status === CANDIDATE_STATUS.REJECTED
          ? gate.rejectionReason || gate.reasonCode || gate.statusReason
          : null,
      exclusionRisk: Boolean(gate.exclusionRisk),
      signals: gate.signals || null,
      geo: gate.geo || null,
      reviewOnly: true,
      placeholder: false,
    };
    if (merged.status === CANDIDATE_STATUS.REJECTED) rejected.push(merged);
    else evaluated.push(merged);
  }

  const targetMax = Number(opts.targetMax) || Number(workRequest && workRequest.targetCountMax) || 25;
  const targetMin = Number(opts.targetMin) || Number(workRequest && workRequest.targetCountMin) || 15;
  const selected = selectBatchWithManchesterFill(evaluated, targetMax, targetMin);
  const allRejected = (selected.rejected || [])
    .concat(rejected)
    .map(ensureRejectionReason);
  const candidates = selected.candidates;
  const groups = groupCandidatesByStatus(candidates, allRejected.slice());
  const threshold = batchMeetsQualityThreshold(candidates, workRequest, {
    targetMin,
  });
  const acceptedCount = (groups.accepted || []).length;
  const reviewRequiredCount = (groups.review_required || []).length;
  const rejectedCount = (groups.rejected || []).length;
  return {
    candidates,
    rejected: groups.rejected,
    groups,
    acceptedCount,
    reviewRequiredCount,
    rejectedCount,
    usableCount: threshold.usableCount,
    targetMin: threshold.targetMin,
    meetsQualityThreshold: threshold.ok,
  };
}

function candidateIdentityKey(row) {
  const name = String(
    (row && (row.companyName || row.company || row.name)) || ''
  )
    .trim()
    .toLowerCase();
  const url = String(
    (row && (row.sourceUrl || row.website || row.url)) || ''
  )
    .trim()
    .toLowerCase();
  return `${name}||${url}`;
}

/**
 * True when a candidate row is evidenced as New Hampshire (not DC/VA/etc.).
 */
function isNhMarketCandidateRow(row) {
  if (!row) return false;
  if (row.geo && row.geo.inNh === true && row.geo.state === 'NH') return true;
  if (row.geo && row.geo.outsideMarketRegion) return false;
  if (row.geo && row.geo.state && row.geo.state !== 'NH') return false;
  const hay = [
    row.location,
    row.address,
    row.formatted_address,
    row.marketTown,
    row.geo && row.geo.town,
    row.geo && row.geo.state,
  ]
    .filter(Boolean)
    .join(' ');
  const state = extractUsStateCode(hay);
  if (state && state !== 'NH') return false;
  return hasNhStateEvidence(hay) || state === 'NH';
}

/**
 * Preserve valid NH primary-town (and usable NH expansion) candidates from a
 * prior completed Scout batch when a later result would otherwise drop them.
 * Out-of-state rows are never preserved into usable lists.
 * NH rows deferred only because "priority towns filled the batch" are restored
 * when the prior batch still had them as accepted / review_required.
 */
function mergePreservedNhCandidatesFromPriorBatch(latestBatch, priorBatch) {
  const latest =
    latestBatch && typeof latestBatch === 'object' ? { ...latestBatch } : {};
  const prior = priorBatch && typeof priorBatch === 'object' ? priorBatch : null;
  if (!prior) {
    // Still strip out-of-state from a lone latest batch.
    return stripNonNhFromBatch(latest);
  }

  const latestCandidates = Array.isArray(latest.candidates)
    ? latest.candidates.map((r) => ({ ...r }))
    : [];
  const latestRejected = Array.isArray(latest.rejected)
    ? latest.rejected.map((r) => ({ ...r }))
    : [];

  const candidateKeys = new Set(
    latestCandidates.map(candidateIdentityKey).filter((k) => k && k !== '||')
  );

  const priorRows = []
    .concat(Array.isArray(prior.candidates) ? prior.candidates : [])
    .concat(
      prior.groups && Array.isArray(prior.groups.accepted)
        ? prior.groups.accepted
        : []
    )
    .concat(
      prior.groups && Array.isArray(prior.groups.review_required)
        ? prior.groups.review_required
        : []
    );

  const restoredKeys = new Set();
  for (const row of priorRows) {
    if (!row || !isNhMarketCandidateRow(row)) continue;
    if (
      row.status !== CANDIDATE_STATUS.ACCEPTED &&
      row.status !== CANDIDATE_STATUS.REVIEW_REQUIRED
    ) {
      continue;
    }
    const key = candidateIdentityKey(row);
    if (!key || key === '||' || candidateKeys.has(key)) continue;
    candidateKeys.add(key);
    restoredKeys.add(key);
    latestCandidates.push({
      ...row,
      status:
        row.status === CANDIDATE_STATUS.ACCEPTED
          ? CANDIDATE_STATUS.ACCEPTED
          : CANDIDATE_STATUS.REVIEW_REQUIRED,
      rejectionReason: null,
    });
  }

  const usable = [];
  const forcedRejected = [];
  for (const row of latestCandidates) {
    if (!isNhMarketCandidateRow(row)) {
      forcedRejected.push(
        ensureRejectionReason({
          ...row,
          status: CANDIDATE_STATUS.REJECTED,
          statusReason:
            row.statusReason ||
            'Outside New Hampshire market region — outside_market_region',
          rejectionReason:
            row.rejectionReason || REJECTION_REASON.OUTSIDE_MARKET_REGION,
          reasonCode: REJECTION_REASON.OUTSIDE_MARKET_REGION,
        })
      );
      continue;
    }
    usable.push(row);
  }

  const allRejected = latestRejected
    .filter((r) => {
      const key = candidateIdentityKey(r);
      // Drop rejection audit rows that we restored into usable candidates.
      if (restoredKeys.has(key)) return false;
      return true;
    })
    .concat(forcedRejected)
    .map(ensureRejectionReason);
  const groups = groupCandidatesByStatus(usable, allRejected.slice());
  return {
    ...latest,
    candidates: usable,
    rejected: groups.rejected,
    groups,
    acceptedCount: groups.accepted.length,
    reviewRequiredCount: groups.review_required.length,
    rejectedCount: groups.rejected.length,
  };
}

function stripNonNhFromBatch(batch) {
  if (!batch || typeof batch !== 'object') return batch;
  const candidates = Array.isArray(batch.candidates)
    ? batch.candidates.map((r) => ({ ...r }))
    : [];
  const rejected = Array.isArray(batch.rejected)
    ? batch.rejected.map((r) => ({ ...r }))
    : [];
  const usable = [];
  const forcedRejected = [];
  for (const row of candidates) {
    if (!isNhMarketCandidateRow(row)) {
      forcedRejected.push(
        ensureRejectionReason({
          ...row,
          status: CANDIDATE_STATUS.REJECTED,
          statusReason:
            row.statusReason ||
            'Outside New Hampshire market region — outside_market_region',
          rejectionReason:
            row.rejectionReason || REJECTION_REASON.OUTSIDE_MARKET_REGION,
          reasonCode: REJECTION_REASON.OUTSIDE_MARKET_REGION,
        })
      );
      continue;
    }
    usable.push(row);
  }
  const groups = groupCandidatesByStatus(
    usable,
    rejected.concat(forcedRejected).slice()
  );
  return {
    ...batch,
    candidates: usable,
    rejected: groups.rejected,
    groups,
    acceptedCount: groups.accepted.length,
    reviewRequiredCount: groups.review_required.length,
    rejectedCount: groups.rejected.length,
  };
}

module.exports = {
  PRIORITY_TOWNS_NH,
  NEARBY_FILL_TOWNS_NH,
  EXTENDED_REVIEW_TOWNS_NH,
  ALL_NH_PILOT_TOWNS,
  ALL_KNOWN_NH_TOWNS,
  CANDIDATE_STATUS,
  CONFIDENCE,
  REJECTION_REASON,
  MAPS_ONLY_SOURCE_RE,
  interpretAnchorMarket,
  buildNhScopedSearchQueries,
  candidateGeoEvidence,
  detectUkOrNonUs,
  detectOutsideMarketRegion,
  detectCleaningCompetitor,
  detectInstitutional,
  hasPropertyManagementFit,
  hasReachableContactSignal,
  hasVerifiedNamedContact,
  formatSuggestedContactRole,
  buildSourceSpecificFitRationale,
  isGenericCriteriaCopy,
  evaluateScoutCandidate,
  selectBatchWithManchesterFill,
  groupCandidatesByStatus,
  gateScoutCandidateRows,
  countUsablePropertyManagerCandidates,
  batchMeetsQualityThreshold,
  isNhLocation,
  hasNhStateEvidence,
  hasUsCountryEvidence,
  hasUsOrNhAddressEvidence,
  extractUsStateCode,
  isNhMarketCandidateRow,
  mergePreservedNhCandidatesFromPriorBatch,
  extendedTownsExplicitlyAllowed,
  hasNonMapsCompanyWebsite,
  isMapsOnlyPublicSource,
  applyMapsOnlyDowngradeToCandidate,
  applyMapsOnlyDowngradeToBatch,
};
