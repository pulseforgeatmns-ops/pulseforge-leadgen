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

const ALL_NH_PILOT_TOWNS = Object.freeze([
  ...PRIORITY_TOWNS_NH,
  ...NEARBY_FILL_TOWNS_NH,
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
  /\bmaid\s+service/i,
  /\bmaids?\b/i,
  /\bhousekeeping\b/i,
  /\bjanitorial\b/i,
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

function detectUkOrNonUs(hit) {
  const hay = paddedHay(hitHaystack(hit));
  const website = String((hit && (hit.website || hit.sourceUrl)) || '').toLowerCase();

  if (/\.co\.uk\b/i.test(website) || website.includes('.uk/')) {
    return { rejected: true, reason: 'UK / non-US domain (.co.uk)' };
  }

  for (const marker of UK_GEO_MARKERS) {
    if (hay.includes(marker)) {
      return {
        rejected: true,
        reason: `UK geography detected (${marker.trim()}) — Anchor market is New Hampshire, USA`,
      };
    }
  }

  // Bare "Manchester" without NH / New Hampshire / US state → treat as ambiguous UK risk
  // when paired with UK postal-ish tokens already handled; also catch ", Manchester" + UK words.
  if (
    /\bmanchester\b/.test(hay) &&
    !/\bnh\b|\bnew hampshire\b|\bunited states\b|\busa\b|\bbedford\b|\bhooksett\b/.test(
      hay
    ) &&
    (/\bsalford\b|\bstockport\b|\buk\b|\bengland\b|\bm\d{1,2}\b/.test(hay) ||
      /\bgreater manchester\b/.test(hay))
  ) {
    return {
      rejected: true,
      reason: 'UK Greater Manchester / Salford / Stockport — not New Hampshire, USA',
    };
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

  if (/\bnh\b|\bnew hampshire\b/.test(loc) || /\bnh\b|\bnew hampshire\b/.test(hay)) {
    return { inNh: true, town: matchTown(loc || hay), tier: townTier(loc || hay) };
  }

  const town = matchTown(loc || hay);
  if (town) {
    // Town match without NH still counts as NH pilot cluster but review-required.
    return { inNh: true, town, tier: townTier(town), missingStateToken: true };
  }

  if (market && market.interpretedAsNewHampshire) {
    return { inNh: false, town: null, tier: 'out_of_market' };
  }
  return { inNh: false, town: null, tier: 'unknown' };
}

function matchTown(text) {
  const s = String(text || '').toLowerCase();
  for (const town of ALL_NH_PILOT_TOWNS) {
    if (s.includes(town.toLowerCase())) return town;
  }
  return null;
}

function townTier(textOrTown) {
  const town = matchTown(textOrTown) || textOrTown;
  if (PRIORITY_TOWNS_NH.some((t) => t.toLowerCase() === String(town || '').toLowerCase())) {
    return 'priority';
  }
  if (NEARBY_FILL_TOWNS_NH.some((t) => t.toLowerCase() === String(town || '').toLowerCase())) {
    return 'nearby_fill';
  }
  return 'other';
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
    /\bproperty\s+management\b|\bproperty\s+manager\b|\breal\s+estate\b/i.test(name) &&
    !/\bcleaning\b|\bmaid\b|\bjanitorial\b|\bhousekeeping\b/i.test(name)
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
  const role =
    (hit && (hit.suggestedContactRole || hit.contactRole)) ||
    defaultRoleForSegment(workRequest);
  return `Suggested contact role: ${role}`;
}

function defaultRoleForSegment(workRequest) {
  const segment = String(
    (workRequest && (workRequest.targetSegment || workRequest.segment)) || ''
  ).toLowerCase();
  if (/property\s*manager/.test(segment)) return 'Owner / property manager';
  if (/law/.test(segment)) return 'Office manager / managing partner';
  if (/account/.test(segment)) return 'Office manager / principal';
  return 'Owner / decision-maker';
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
  ]
    .map((c) => String(c || '').toLowerCase().replace(/\s+/g, ' ').trim())
    .filter((c) => c.length >= 20);

  for (const c of criteria) {
    if (text === c || text.includes(c) || c.includes(text)) return true;
  }

  // Template phrases that only restate the brief (no source facts).
  const genericOnly =
    /^(public listing matches|matches target segment|fits inclusion criteria|meets campaign criteria)/i.test(
      text
    ) &&
    !/\bhttps?:\/\//i.test(text) &&
    !/\bnh\b|\bstreet\b|\broad\b|\bave\b|\bsuite\b|\blisting types\b/i.test(text);

  return Boolean(genericOnly);
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
  const fit = buildSourceSpecificFitRationale(hit, workRequest, geo);
  const role = formatSuggestedContactRole(workRequest, hit);

  const risks = [];
  let exclusionRisk = Boolean(institutional.rejected && operatorApprovedInstitutional(workRequest));
  if (institutional.rejected && operatorApprovedInstitutional(workRequest)) {
    risks.push('Institutional/national firm — included only because operator approved');
    exclusionRisk = true;
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
  if (!geo.inNh && (hit.location || hit.address || hit.formatted_address)) {
    return rejectResult(hit, workRequest, {
      statusReason: 'Outside New Hampshire, USA market',
      exclusionRisk: true,
      market,
      fitRationale: fit.rationale,
      suggestedContactRole: role,
      risks,
    });
  }

  if (!fit.ok && opts.requireSourceSpecificRationale !== false) {
    // Downgrade rather than hard-reject when other signals are strong — still not accepted.
  }

  const signals = {
    sourceUrl,
    nhLocation: Boolean(geo.inNh && !geo.missingStateToken),
    propertyManagementFit: pmFit,
    reachableContactSignal: contactSignal,
    exclusionRisk,
    priorityTown: geo.tier === 'priority',
    nearbyFillTown: geo.tier === 'nearby_fill',
  };

  let confidence = CONFIDENCE.REVIEW_REQUIRED;
  const highEligible =
    signals.sourceUrl &&
    signals.nhLocation &&
    signals.propertyManagementFit &&
    signals.reachableContactSignal &&
    !signals.exclusionRisk &&
    fit.ok;

  if (highEligible && geo.tier === 'priority') {
    confidence = CONFIDENCE.HIGH;
  } else if (
    signals.sourceUrl &&
    geo.inNh &&
    signals.propertyManagementFit &&
    !signals.exclusionRisk
  ) {
    confidence = CONFIDENCE.MEDIUM;
  } else {
    confidence = CONFIDENCE.REVIEW_REQUIRED;
  }

  // High confidence blocked whenever exclusion risk exists.
  if (signals.exclusionRisk && confidence === CONFIDENCE.HIGH) {
    confidence = CONFIDENCE.REVIEW_REQUIRED;
  }

  let status = CANDIDATE_STATUS.ACCEPTED;
  let statusReason = 'Passes NH property-manager quality gates';

  if (!geo.inNh || !pmFit || !fit.ok || !sourceUrl) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = !sourceUrl
      ? 'Missing source URL'
      : !geo.inNh
        ? 'NH location not confirmed'
        : !pmFit
          ? 'Property-management fit not evidenced on source'
          : fit.reason || 'Source-specific fit rationale incomplete';
  } else if (geo.tier === 'nearby_fill') {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason =
      'Manchester NH nearby/fill — review required unless needed to fill the batch';
  } else if (geo.missingStateToken || !contactSignal) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = geo.missingStateToken
      ? 'NH state token missing on listing — verify New Hampshire, USA'
      : 'Reachable contact signal missing — review before outreach';
  } else if (exclusionRisk) {
    status = CANDIDATE_STATUS.REVIEW_REQUIRED;
    statusReason = 'Exclusion risk present — operator review required';
  }

  return {
    status,
    statusReason,
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

function rejectResult(hit, workRequest, extra = {}) {
  const geo =
    extra.geo ||
    isNhLocation(hit, extra.market || interpretAnchorMarket(workRequest && workRequest.marketBounds));
  const fit =
    extra.fitRationale ||
    buildSourceSpecificFitRationale(hit, workRequest, geo).rationale;
  return {
    status: CANDIDATE_STATUS.REJECTED,
    statusReason: extra.statusReason || 'Rejected by Scout quality gate',
    confidence: CONFIDENCE.REVIEW_REQUIRED,
    fitRationale: fit,
    suggestedContactRole:
      extra.suggestedContactRole || formatSuggestedContactRole(workRequest, hit),
    risks: Array.isArray(extra.risks)
      ? extra.risks.join('; ')
      : extra.statusReason || 'Rejected',
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
 */
function selectBatchWithManchesterFill(evaluatedRows, targetMax = 25, targetMin = 15) {
  const acceptedPriority = [];
  const acceptedOther = [];
  const reviewPriority = [];
  const reviewManchester = [];
  const reviewOther = [];
  const rejected = [];

  for (const row of evaluatedRows) {
    if (!row) continue;
    if (row.status === CANDIDATE_STATUS.REJECTED) {
      rejected.push(row);
      continue;
    }
    const tier = (row.geo && row.geo.tier) || townTier(row.location || '');
    if (row.status === CANDIDATE_STATUS.ACCEPTED) {
      if (tier === 'priority') acceptedPriority.push(row);
      else acceptedOther.push(row);
      continue;
    }
    if (tier === 'priority') reviewPriority.push(row);
    else if (tier === 'nearby_fill') reviewManchester.push(row);
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
  take(acceptedOther);
  take(reviewPriority);
  take(reviewOther);

  // Manchester fill only if batch still short.
  if (batch.length < targetMin) {
    take(reviewManchester);
  } else {
    // Leave Manchester out of the approved-facing batch when not needed;
    // still report them in rejected/deferred via statusReason if we skip.
    for (const row of reviewManchester) {
      rejected.push({
        ...row,
        status: CANDIDATE_STATUS.REJECTED,
        statusReason:
          'Manchester NH deferred — priority towns filled the batch (nearby/fill only)',
      });
    }
  }

  return { candidates: batch, rejected };
}

module.exports = {
  PRIORITY_TOWNS_NH,
  NEARBY_FILL_TOWNS_NH,
  ALL_NH_PILOT_TOWNS,
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
  isNhLocation,
};
