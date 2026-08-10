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

const CONFIDENCE = Object.freeze({
  HIGH: 'high',
  MEDIUM: 'medium',
  REVIEW_REQUIRED: 'review_required',
});

const UK_GEO_MARKERS = Object.freeze([
  'united kingdom',
  ' u.k.',
  ' uk ',
  ', uk',
  'uk,',
  'england',
  'scotland',
  'wales',
  'greater manchester',
  'salford',
  'stockport',
  'bolton',
  'oldham',
  'rochdale',
  'bury',
  'wigan',
  'trafford',
  'tameside',
  'm1 ',
  'm2 ',
  'm3 ',
  'm4 ',
  'm5 ',
  'm6 ',
  'm7 ',
  'm8 ',
  'm9 ',
  'm13',
  'm14',
  'm15',
  'm16',
  'm17',
  'm18',
  'm19',
  'm20',
  'm21',
  'm22',
  'm23',
  'm24',
  'm25',
  'm26',
  'm27',
  'm28',
  'm29',
  'm30',
  'm31',
  'm32',
  'm33',
  'm34',
  'm35',
  'm38',
  'm40',
  'm41',
  'm43',
  'm44',
  'm45',
  'm46',
  'm50',
  'm60',
  'm90',
  'lancashire',
  'cheshire',
  '.co.uk',
  'manchester, england',
  'manchester england',
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
]);

/** Place-types / industry tokens that mark cleaning competitors. */
const CLEANING_TYPE_TOKENS = Object.freeze([
  'cleaning',
  'maid',
  'housekeeping',
  'janitorial',
  'carpet_cleaning',
  'carpet cleaning',
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
  return /\bnh\b|\bnew hampshire\b|\bunited states\b|\busa\b|\bu\.s\.a\.?\b/i.test(
    String(text || '')
  );
}

function detectUkOrNonUs(hit) {
  const hay = paddedHay(hitHaystack(hit));
  const loc = String(
    (hit && (hit.location || hit.address || hit.formatted_address || hit.marketTown)) ||
      ''
  ).toLowerCase();
  const website = String((hit && (hit.website || hit.sourceUrl)) || '').toLowerCase();
  const phone = String(
    (hit && (hit.phone || hit.formatted_phone_number)) || ''
  ).trim();

  if (/\.co\.uk\b/i.test(website) || website.includes('.uk/')) {
    return { rejected: true, reason: 'UK / non-US domain (.co.uk)' };
  }
  if (/^\+44\b|^0044\b/.test(phone)) {
    return { rejected: true, reason: 'UK phone country code (+44) — not USA' };
  }
  if (hit && hit.country) {
    const country = String(hit.country).trim().toLowerCase();
    if (country && !/^(us|usa|united states|united states of america)$/.test(country)) {
      return {
        rejected: true,
        reason: `Non-US country (${hit.country}) — Anchor market is New Hampshire, USA`,
      };
    }
  }

  for (const marker of UK_GEO_MARKERS) {
    if (hay.includes(marker)) {
      return {
        rejected: true,
        reason: `UK geography detected (${marker.trim()}) — Anchor market is New Hampshire, USA`,
      };
    }
  }
  for (const marker of UK_MANCHESTER_PLACE_MARKERS) {
    if (hay.includes(marker)) {
      return {
        rejected: true,
        reason: `UK Greater Manchester place marker (${marker.trim()}) — not New Hampshire, USA`,
      };
    }
  }

  // Bare / ambiguous "Manchester" without an explicit NH/USA state token is UK risk.
  // Never treat "Manchester" alone as Manchester NH.
  if (/\bmanchester\b/.test(hay) && !hasUsOrNhStateToken(hay) && !hasUsOrNhStateToken(loc)) {
    const otherPriorityNh = PRIORITY_TOWNS_NH.some((t) =>
      hay.includes(t.toLowerCase())
    );
    if (!otherPriorityNh) {
      return {
        rejected: true,
        reason:
          'Ambiguous Manchester without NH/USA token — rejected as UK / non-verified New Hampshire',
      };
    }
  }

  for (const marker of NON_US_MARKERS) {
    if (hay.includes(marker)) {
      return {
        rejected: true,
        reason: `Non-US geography detected (${marker}) — Anchor market is New Hampshire, USA`,
      };
    }
  }

  return { rejected: false, reason: null };
}

function isNhLocation(hit, market) {
  const loc = String(
    (hit && (hit.location || hit.address || hit.marketTown || hit.formatted_address)) ||
      ''
  ).toLowerCase();
  const hay = hitHaystack(hit);
  const combined = `${loc} ${hay}`;
  const hasState = hasUsOrNhStateToken(combined);
  const town = matchTown(combined);

  if (hasState) {
    return {
      inNh: true,
      country: 'US',
      state: 'NH',
      town,
      tier: townTier(town || combined),
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
    return { rejected: false, reason: null };
  }

  const name = String((hit && (hit.companyName || hit.name)) || '');
  const industry = String((hit && (hit.industry || hit.snippet)) || '');
  const types = ((hit && hit.placeTypes) || []).map((t) => String(t).toLowerCase());

  // Clear PM / real-estate companies are not cleaning competitors even if a
  // directory blurb mentions "cleaning".
  if (
    /\bproperty\s+management\b|\bproperty\s+manager\b|\breal\s+estate\s+management\b|\breal\s+estate\b/i.test(
      name
    ) &&
    !/\bcleaning\b|\bmaid\b|\bjanitorial\b|\bhousekeeping\b|\bcleaners?\b/i.test(name)
  ) {
    return { rejected: false, reason: null };
  }

  for (const token of CLEANING_TYPE_TOKENS) {
    if (types.some((t) => t.includes(token.replace(/\s+/g, '_')))) {
      return {
        rejected: true,
        reason: `Cleaning competitor / service type (${token}) — target is property managers`,
      };
    }
  }

  for (const re of CLEANING_COMPETITOR_PATTERNS) {
    if (re.test(name) || re.test(industry)) {
      return {
        rejected: true,
        reason:
          'Cleaning service / maid / housekeeping / janitorial / carpet cleaning competitor — not a property-manager prospect',
      };
    }
  }

  // Name contains a standalone cleaning stem without PM signal.
  if (
    /\bclean(?:ing|ers?)?\b/i.test(name) &&
    !/\bproperty\b|\breal\s+estate\b/i.test(name)
  ) {
    return {
      rejected: true,
      reason:
        'Cleaning competitor name signal — not a property-manager prospect',
    };
  }

  return { rejected: false, reason: null };
}

function detectInstitutional(hit) {
  const hay = hitHaystack(hit);
  const name = String((hit && (hit.companyName || hit.name)) || '');
  for (const re of INSTITUTIONAL_PATTERNS) {
    if (re.test(name) || re.test(hay)) {
      return {
        rejected: true,
        reason:
          'Large institutional / national firm — rejected unless operator explicitly approves',
        matched: String(re),
      };
    }
  }
  return { rejected: false, reason: null };
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
 */
function buildSourceSpecificFitRationale(hit, workRequest, geo) {
  const company = String((hit && (hit.companyName || hit.name)) || 'Listing').trim();
  const location = String(
    (hit && (hit.location || hit.address || hit.formatted_address)) || ''
  ).trim();
  const types = ((hit && hit.placeTypes) || []).filter(Boolean);
  const website = hit && (hit.website || hit.sourceUrl);
  const parts = [];

  parts.push(`${company} sourced from public listing`);
  if (location) {
    parts.push(`address/location on source: ${location}`);
  } else {
    parts.push('location not confirmed on source');
  }
  if (types.length) {
    parts.push(`listing types: ${types.slice(0, 4).join(', ')}`);
  }
  if (website) {
    parts.push(`source URL: ${website}`);
  }
  if (geo && geo.town) {
    parts.push(
      geo.tier === 'priority'
        ? `priority NH town match: ${geo.town}`
        : geo.tier === 'nearby_fill'
          ? `nearby NH fill town: ${geo.town} (review-required unless batch fill)`
          : `town signal: ${geo.town}`
    );
  }
  if (hit && hit.googleRating != null) {
    parts.push(`Google rating ${hit.googleRating}`);
  }

  const rationale = parts.join(' — ');
  if (isGenericCriteriaCopy(rationale, workRequest) || parts.length < 2) {
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
      exclusionRisk: true,
      market,
    });
  }

  const cleaning = detectCleaningCompetitor(hit, workRequest);
  if (cleaning.rejected) {
    return rejectResult(hit, workRequest, {
      statusReason: cleaning.reason,
      exclusionRisk: true,
      market,
    });
  }

  const institutional = detectInstitutional(hit);
  if (institutional.rejected && !operatorApprovedInstitutional(workRequest)) {
    return rejectResult(hit, workRequest, {
      statusReason: institutional.reason,
      exclusionRisk: true,
      market,
    });
  }

  const geo = isNhLocation(hit, market);
  const pmFit = hasPropertyManagementFit(hit, workRequest);
  const contactSignal = hasReachableContactSignal(hit);
  const sourceUrl = Boolean(
    (hit && (hit.sourceUrl || hit.website || hit.url)) || opts.sourceUrl
  );
  // Prefer source-built rationale; ignore generic criteria copies from upstream.
  let fit = buildSourceSpecificFitRationale(hit, workRequest, geo);
  if (hit && (hit.fitRationale || hit.fitReason || hit.rationale)) {
    const provided = hit.fitRationale || hit.fitReason || hit.rationale;
    if (!isGenericCriteriaCopy(provided, workRequest)) {
      fit = { ok: true, rationale: String(provided), reason: null };
    }
  }
  const role = formatSuggestedContactRole(workRequest, hit);
  const targetingPm = /property\s*manager/i.test(
    String((workRequest && (workRequest.targetSegment || workRequest.segment)) || '')
  );
  const extendedAllowed = extendedTownsExplicitlyAllowed(workRequest);

  const risks = [];
  let exclusionRisk = Boolean(
    institutional.rejected && operatorApprovedInstitutional(workRequest)
  );
  if (institutional.rejected && operatorApprovedInstitutional(workRequest)) {
    risks.push('Institutional/national firm — included only because operator approved');
    exclusionRisk = true;
  }
  const exclusionCriteriaHit = softExclusionCriteriaMatch(hit, workRequest);
  if (exclusionCriteriaHit) {
    exclusionRisk = true;
    risks.push(`Exclusion criteria match: ${exclusionCriteriaHit}`);
  }
  const hasCompanyWebsite = Boolean(
    hit &&
      hit.website &&
      !/google\.com\/maps/i.test(String(hit.website)) &&
      String(hit.website).trim()
  );
  if (!hasCompanyWebsite) {
    risks.push('No company website on listing — using maps listing as source URL');
  }
  if (!geo.inNh) {
    risks.push('Location not confirmed in New Hampshire, USA');
  }
  if (geo.missingStateToken) {
    risks.push('Town matched but NH/New Hampshire state token missing on listing');
  }
  if (geo.tier === 'nearby_fill') {
    risks.push(
      'Manchester NH is nearby/fill — review required unless needed to fill the batch'
    );
  }
  if (geo.tier === 'extended_review') {
    risks.push(
      `${geo.town || 'Town'} NH is outside the approved priority cluster — review required unless explicitly allowed`
    );
  }
  if (!pmFit) {
    risks.push('Weak property-management fit signal on public source');
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
    return rejectResult(hit, workRequest, {
      statusReason: 'Outside New Hampshire, USA market',
      exclusionRisk: true,
      market,
      fitRationale: fit.rationale,
      suggestedContactRole: role,
      risks,
    });
  }

  // Missing NH/USA state token on a known town → never accepted.
  if (geo.missingStateToken && geo.town) {
    return {
      ...reviewResult(hit, workRequest, {
        statusReason: `${geo.town} listing missing NH/USA state token — verify New Hampshire, USA`,
        confidence: CONFIDENCE.REVIEW_REQUIRED,
        fitRationale: fit.rationale,
        suggestedContactRole: role,
        risks,
        exclusionRisk,
        geo,
        market,
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
    geo.tier === 'priority';

  if (highEligible) {
    confidence = CONFIDENCE.HIGH;
  } else if (
    signals.sourceUrl &&
    signals.nhLocation &&
    signals.propertyManagementFit &&
    !signals.exclusionRisk &&
    (geo.tier === 'priority' || geo.tier === 'nearby_fill')
  ) {
    confidence = CONFIDENCE.MEDIUM;
  } else {
    confidence = CONFIDENCE.REVIEW_REQUIRED;
  }

  // Any exclusion match must never be high confidence.
  if (signals.exclusionRisk) {
    confidence = CONFIDENCE.REVIEW_REQUIRED;
  }

  let status = CANDIDATE_STATUS.ACCEPTED;
  let statusReason = 'Passes NH property-manager quality gates';

  if (!sourceUrl) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'Missing source URL';
  } else if (!geo.inNh) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'NH location not confirmed';
  } else if (targetingPm && !pmFit) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'Property-management fit not evidenced on source';
  } else if (!fit.ok) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = fit.reason || 'Source-specific fit rationale incomplete';
  } else if (geo.tier === 'nearby_fill') {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason =
      'Manchester NH nearby/fill — review required unless needed to fill the batch';
  } else if (geo.tier === 'extended_review') {
    if (!extendedAllowed) {
      status = CANDIDATE_STATUS.REVIEW_REQUIRED;
      statusReason = `${geo.town} NH is outside approved town cluster (Bedford/Hooksett/Londonderry/Auburn/Goffstown) — review required unless explicitly allowed`;
    } else {
      status = CANDIDATE_STATUS.REVIEW_REQUIRED;
      statusReason = `${geo.town} NH explicitly allowed but still review_required (not a priority town)`;
    }
  } else if (geo.tier === 'other') {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason =
      'Location outside approved priority town cluster — review required';
  } else if (!contactSignal) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'Reachable contact signal missing — review before outreach';
  } else if (exclusionRisk) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'Exclusion risk present — operator review required';
  } else if (geo.tier !== 'priority') {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'Not in priority NH town cluster';
  }

  // Accepted only for priority NH towns with full signals.
  if (
    status === CANDIDATE_STATUS.ACCEPTED &&
    !(
      geo.tier === 'priority' &&
      signals.nhLocation &&
      pmFit &&
      sourceUrl &&
      fit.ok &&
      !exclusionRisk
    )
  ) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'Does not meet accepted priority-town quality bar';
  }

  return {
    status,
    statusReason,
    rejectionReason: null,
    confidence,
    fitRationale: fit.rationale,
    suggestedContactRole: role,
    risks: risks.length
      ? risks.join('; ')
      : 'Public-source only — verify contact before outreach',
    exclusionRisk,
    geo,
    signals,
    market,
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
  return {
    status: CANDIDATE_STATUS.REVIEW_REQUIRED,
    statusReason: fields.statusReason || 'Review required',
    rejectionReason: null,
    confidence: fields.confidence || CONFIDENCE.REVIEW_REQUIRED,
    fitRationale:
      fields.fitRationale ||
      buildSourceSpecificFitRationale(hit, workRequest, fields.geo).rationale,
    suggestedContactRole:
      fields.suggestedContactRole || formatSuggestedContactRole(workRequest, hit),
    risks: Array.isArray(fields.risks)
      ? fields.risks.join('; ')
      : fields.risks || fields.statusReason || 'Review required',
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
  return {
    status: CANDIDATE_STATUS.REJECTED,
    statusReason: reason,
    rejectionReason: reason,
    confidence: CONFIDENCE.REVIEW_REQUIRED,
    fitRationale: fit,
    suggestedContactRole:
      extra.suggestedContactRole || formatSuggestedContactRole(workRequest, hit),
    risks: Array.isArray(extra.risks)
      ? extra.risks.join('; ')
      : reason,
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
 * Prefer priority-town accepted rows; use Manchester NH review_required only to fill.
 * Concord/Derry/extended towns stay review_required and never become accepted.
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
  // Extended towns (Concord/Derry/etc.) only if still short and not filling before priority.
  if (batch.length < targetMin) {
    take(reviewExtended);
    take(reviewOther);
  } else {
    for (const row of reviewExtended.concat(reviewOther)) {
      rejected.push(
        ensureRejectionReason({
          ...row,
          status: CANDIDATE_STATUS.REJECTED,
          statusReason:
            row.statusReason ||
            'Outside approved priority town cluster — deferred from batch',
          rejectionReason:
            row.rejectionReason ||
            row.statusReason ||
            'Outside approved priority town cluster — deferred from batch',
        })
      );
    }
  }

  // Manchester fill only if batch still short.
  if (batch.length < targetMin) {
    take(reviewManchester);
  } else {
    for (const row of reviewManchester) {
      rejected.push(
        ensureRejectionReason({
          ...row,
          status: CANDIDATE_STATUS.REJECTED,
          statusReason:
            'Manchester NH deferred — priority towns filled the batch (nearby/fill only)',
          rejectionReason:
            'Manchester NH deferred — priority towns filled the batch (nearby/fill only)',
        })
      );
    }
  }

  return {
    candidates: batch,
    rejected,
    groups: groupCandidatesByStatus(batch, rejected),
  };
}

function ensureRejectionReason(row) {
  if (!row) return row;
  if (row.status === CANDIDATE_STATUS.REJECTED) {
    return {
      ...row,
      rejectionReason: row.rejectionReason || row.statusReason || 'Rejected',
    };
  }
  return { ...row, rejectionReason: row.rejectionReason || null };
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
    };
    const gate = evaluateScoutCandidate(hit, workRequest, opts);
    const merged = {
      id: row.id || `scout-gated-${i + 1}`,
      companyName: hit.companyName,
      sourceUrl,
      website: hit.website,
      location: hit.location,
      marketTown: row.marketTown || (gate.geo && gate.geo.town) || hit.location,
      segment: row.segment || (workRequest && workRequest.targetSegment) || null,
      subtype: row.subtype || (workRequest && workRequest.targetSubtype) || null,
      fitRationale: gate.fitRationale,
      risks: gate.risks,
      suggestedContactRole: gate.suggestedContactRole,
      confidence: gate.confidence,
      status: gate.status,
      statusReason: gate.statusReason,
      rejectionReason:
        gate.status === CANDIDATE_STATUS.REJECTED
          ? gate.rejectionReason || gate.statusReason
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
  const allRejected = (selected.rejected || []).concat(rejected).map(ensureRejectionReason);
  return {
    candidates: selected.candidates,
    rejected: allRejected,
    groups: groupCandidatesByStatus(selected.candidates, allRejected),
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
  interpretAnchorMarket,
  buildNhScopedSearchQueries,
  detectUkOrNonUs,
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
  isNhLocation,
  extendedTownsExplicitlyAllowed,
};
