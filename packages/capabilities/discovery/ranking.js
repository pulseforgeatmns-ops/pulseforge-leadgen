'use strict';

/**
 * Transparent ranking against a Discovery Profile (SPEC-024).
 * Every signal references the profile — nothing is a black box.
 */

const { buildRankingSignal, SIGNAL_WEIGHTS } = require('./types');

const INDUSTRY_KEYWORDS = Object.freeze({
  'Commercial Property Management': [
    'property management',
    'property manager',
    'commercial property',
  ],
  'Law Firms': ['law firm', 'law office', 'attorney', 'legal', 'lawyer', 'esq'],
  'CPA Firms': ['cpa', 'accountant', 'accounting', 'bookkeeping', 'tax firm'],
  'Medical Offices': [
    'medical',
    'dental',
    'dentist',
    'clinic',
    'physician',
    'chiropractic',
    'physical therapy',
  ],
  'Professional Offices': [
    'consulting',
    'architect',
    'engineering',
    'insurance agency',
    'financial advisor',
    'professional',
  ],
  'Commercial Cleaning': [
    'commercial cleaning',
    'janitorial',
    'office cleaning',
  ],
  'Janitorial Services': ['janitorial', 'commercial cleaning'],
  'Office Cleaning': ['office cleaning', 'commercial cleaning'],
  Attorneys: ['attorney', 'law firm', 'lawyer'],
  'Legal Offices': ['law office', 'legal office'],
  'Dental Practices': ['dental', 'dentist', 'orthodont'],
  'Property Management': ['property management', 'property manager'],
  'Commercial Offices': ['commercial office', 'office park', 'business center'],
  'Retail Centers': ['retail', 'shopping center', 'plaza'],
});

const RESIDENTIAL_PATTERNS = [
  /\bresidential\b/i,
  /\bhouse\s*cleaning\b/i,
  /\bhome\s*cleaning\b/i,
  /\bmaid\s*service\b/i,
  /\bapartment\s*complex\b/i,
];

const CLOSED_PATTERNS = [
  /\bpermanently\s+closed\b/i,
  /\bout\s+of\s+business\b/i,
  /\bclosed\s+permanently\b/i,
];

const MULTI_LOCATION_PATTERNS = [
  /\b\d+\s+locations?\b/i,
  /\bmultiple\s+(offices|locations)\b/i,
  /\boffices\s+in\b/i,
  /\bnationwide\b/i,
];

/**
 * Rank a candidate against a Discovery Profile.
 * @param {object} candidate
 * @param {object} profile
 * @returns {{ confidence: number, signals: object[], excluded: boolean, excludeReason: string|null }}
 */
function rankAgainstProfile(candidate, profile) {
  const weights = profile.rankingWeights || {};
  const hay = buildHaystack(candidate);
  const signals = [];
  let score = 0;
  let weightSum = 0;

  // Target industry (high)
  const industryMatch = matchIndustry(hay, profile.industryTargets || []);
  const industryWeight = weights.target_industry ?? SIGNAL_WEIGHTS.HIGH;
  signals.push(
    buildRankingSignal({
      signal: 'target_industry',
      weight: industryWeight,
      matched: !!industryMatch,
      profileId: profile.id,
      profileName: profile.name,
      profileVersion: profile.version,
      detail: industryMatch
        ? `Matched Profile Signal: ${profile.name} — ${industryMatch}`
        : 'No target industry match',
    })
  );
  if (industryMatch) {
    score += industryWeight;
  }
  weightSum += Math.abs(industryWeight);

  // Commercial office / professional services
  const commercial = isCommercialOffice(hay, candidate);
  const commercialWeight = weights.commercial_office ?? SIGNAL_WEIGHTS.HIGH;
  signals.push(
    buildRankingSignal({
      signal: 'commercial_office',
      weight: commercialWeight,
      matched: commercial,
      profileId: profile.id,
      profileName: profile.name,
      profileVersion: profile.version,
      detail: commercial
        ? `Matched Profile Signal: ${profile.name} — commercial office`
        : 'Not clearly commercial',
    })
  );
  if (commercial) score += commercialWeight;
  weightSum += Math.abs(commercialWeight);

  const professional = isProfessionalServices(hay);
  const profWeight = weights.professional_services ?? SIGNAL_WEIGHTS.MEDIUM;
  signals.push(
    buildRankingSignal({
      signal: 'professional_services',
      weight: profWeight,
      matched: professional,
      profileId: profile.id,
      profileName: profile.name,
      profileVersion: profile.version,
      detail: professional
        ? `Industry Weight: ${profWeight.toFixed(2)} — professional services`
        : 'Not professional services',
    })
  );
  if (professional) score += profWeight;
  weightSum += Math.abs(profWeight);

  // Website
  const hasWebsite = !!(candidate.website || candidate.url);
  const websiteWeight = weights.active_website ?? SIGNAL_WEIGHTS.HIGH;
  const missingWeight = weights.missing_website ?? SIGNAL_WEIGHTS.NEGATIVE;
  if (hasWebsite) {
    signals.push(
      buildRankingSignal({
        signal: 'active_website',
        weight: websiteWeight,
        matched: true,
        profileId: profile.id,
        profileName: profile.name,
        profileVersion: profile.version,
        detail: `Website present: ${candidate.website || candidate.url}`,
      })
    );
    score += websiteWeight;
    weightSum += Math.abs(websiteWeight);
  } else {
    signals.push(
      buildRankingSignal({
        signal: 'missing_website',
        weight: missingWeight,
        matched: true,
        profileId: profile.id,
        profileName: profile.name,
        profileVersion: profile.version,
        detail: 'Missing website — negative signal',
      })
    );
    score += missingWeight;
    weightSum += Math.abs(missingWeight);
  }

  // Address
  const hasAddress = !!(candidate.address && String(candidate.address).trim());
  const addrWeight = weights.verified_address ?? SIGNAL_WEIGHTS.HIGH;
  signals.push(
    buildRankingSignal({
      signal: 'verified_address',
      weight: addrWeight,
      matched: hasAddress,
      profileId: profile.id,
      profileName: profile.name,
      profileVersion: profile.version,
      detail: hasAddress ? candidate.address : 'Address missing',
    })
  );
  if (hasAddress) score += addrWeight;
  weightSum += Math.abs(addrWeight);

  // Multi-location (medium)
  const multi = MULTI_LOCATION_PATTERNS.some((re) => re.test(hay));
  const multiWeight = weights.multi_location ?? SIGNAL_WEIGHTS.MEDIUM;
  signals.push(
    buildRankingSignal({
      signal: 'multi_location',
      weight: multiWeight,
      matched: multi,
      profileId: profile.id,
      profileName: profile.name,
      profileVersion: profile.version,
      detail: multi ? 'Multi-location indicators present' : 'Single-location or unknown',
    })
  );
  if (multi) score += multiWeight;
  weightSum += Math.abs(multiWeight);

  // Negative: residential
  const residential = RESIDENTIAL_PATTERNS.some((re) => re.test(hay));
  const resWeight = weights.residential_only ?? SIGNAL_WEIGHTS.NEGATIVE;
  if (residential) {
    signals.push(
      buildRankingSignal({
        signal: 'residential_only',
        weight: resWeight,
        matched: true,
        profileId: profile.id,
        profileName: profile.name,
        profileVersion: profile.version,
        detail: 'Residential-only indicators — excluded signal',
      })
    );
    score += resWeight;
    weightSum += Math.abs(resWeight);
  }

  // Negative: generic residential cleaner
  const genericCleaner =
    /\b(maid|house\s*clean|home\s*clean|residential\s*clean)/i.test(hay) &&
    !/\bcommercial\b/i.test(hay);
  const genWeight = weights.generic_residential_cleaner ?? SIGNAL_WEIGHTS.NEGATIVE;
  if (genericCleaner) {
    signals.push(
      buildRankingSignal({
        signal: 'generic_residential_cleaner',
        weight: genWeight,
        matched: true,
        profileId: profile.id,
        profileName: profile.name,
        profileVersion: profile.version,
        detail: 'Generic residential cleaner — negative',
      })
    );
    score += genWeight;
    weightSum += Math.abs(genWeight);
  }

  // Closed business
  const closed = CLOSED_PATTERNS.some((re) => re.test(hay));
  const closedWeight = weights.closed_business ?? SIGNAL_WEIGHTS.NEGATIVE;
  if (closed) {
    signals.push(
      buildRankingSignal({
        signal: 'closed_business',
        weight: closedWeight,
        matched: true,
        profileId: profile.id,
        profileName: profile.name,
        profileVersion: profile.version,
        detail: 'Appears closed',
      })
    );
    score += closedWeight;
    weightSum += Math.abs(closedWeight);
  }

  // CRM duplicate flags from prior stage
  if (candidate._existingProspect) {
    const w = weights.existing_prospect ?? SIGNAL_WEIGHTS.NEGATIVE;
    signals.push(
      buildRankingSignal({
        signal: 'existing_prospect',
        weight: w,
        matched: true,
        profileId: profile.id,
        profileName: profile.name,
        profileVersion: profile.version,
        detail: 'Already in CRM as prospect',
      })
    );
    score += w;
    weightSum += Math.abs(w);
  }
  if (candidate._existingCustomer) {
    const w = weights.existing_customer ?? SIGNAL_WEIGHTS.NEGATIVE;
    signals.push(
      buildRankingSignal({
        signal: 'existing_customer',
        weight: w,
        matched: true,
        profileId: profile.id,
        profileName: profile.name,
        profileVersion: profile.version,
        detail: 'Existing customer',
      })
    );
    score += w;
    weightSum += Math.abs(w);
  }

  // Preferred signals soft boost
  for (const pref of profile.preferredSignals || []) {
    if (pref === 'professional_branding' && hasWebsite && professional) {
      const w = weights.professional_branding ?? SIGNAL_WEIGHTS.MEDIUM;
      signals.push(
        buildRankingSignal({
          signal: 'professional_branding',
          weight: w,
          matched: true,
          profileId: profile.id,
          profileName: profile.name,
          profileVersion: profile.version,
          detail: `Preferred signal: professional branding (weight ${w})`,
        })
      );
      score += w * 0.5;
      weightSum += Math.abs(w) * 0.5;
    }
    if (pref === 'recurring_facility_ops' && (commercial || industryMatch)) {
      const w = weights.recurring_facility_ops ?? SIGNAL_WEIGHTS.MEDIUM;
      signals.push(
        buildRankingSignal({
          signal: 'recurring_facility_ops',
          weight: w,
          matched: true,
          profileId: profile.id,
          profileName: profile.name,
          profileVersion: profile.version,
          detail: `Preferred signal: recurring facility operations (weight ${w})`,
        })
      );
      score += w * 0.4;
      weightSum += Math.abs(w) * 0.4;
    }
  }

  // Excluded signal hard reject
  let excluded = false;
  let excludeReason = null;
  const excludedSet = new Set(profile.excludedSignals || []);
  if (excludedSet.has('residential_only') && residential) {
    excluded = true;
    excludeReason = 'Matched excluded signal: residential_only';
  }
  if (excludedSet.has('closed_business') && closed) {
    excluded = true;
    excludeReason = 'Matched excluded signal: closed_business';
  }
  if (excludedSet.has('existing_prospect') && candidate._existingProspect) {
    excluded = true;
    excludeReason = 'Matched excluded signal: existing_prospect';
  }
  if (excludedSet.has('existing_customer') && candidate._existingCustomer) {
    excluded = true;
    excludeReason = 'Matched excluded signal: existing_customer';
  }

  const confidence =
    weightSum > 0 ? Math.max(0, Math.min(1, (score / weightSum + 1) / 2)) : 0;

  // Recalibrate: map raw contribution into 0–1 more intuitively
  const positiveMax = weightSum || 1;
  const normalized = Math.max(0, Math.min(1, (score + positiveMax * 0.15) / (positiveMax * 1.15)));

  return {
    confidence: Number(normalized.toFixed(4)),
    signals,
    excluded,
    excludeReason,
    industryMatch,
  };
}

function buildHaystack(candidate) {
  return [
    candidate.companyName || candidate.company || '',
    candidate.website || candidate.url || '',
    candidate.industry || '',
    candidate.address || '',
    candidate.snippet || '',
    (candidate.placeTypes || []).join(' '),
  ]
    .join(' ')
    .toLowerCase();
}

function matchIndustry(hay, targets) {
  for (const target of targets) {
    const keywords = INDUSTRY_KEYWORDS[target] || [String(target).toLowerCase()];
    if (keywords.some((k) => hay.includes(k))) return target;
  }
  return null;
}

function isCommercialOffice(hay, candidate) {
  const types = candidate.placeTypes || [];
  if (
    types.some((t) =>
      /lawyer|accounting|dentist|doctor|insurance_agency|real_estate|finance|establishment/i.test(
        t
      )
    )
  ) {
    return true;
  }
  return (
    /office|law|cpa|dental|medical|property management|professional|commercial/.test(
      hay
    ) && !RESIDENTIAL_PATTERNS.some((re) => re.test(hay))
  );
}

function isProfessionalServices(hay) {
  return /law|attorney|cpa|account|dental|medical|consult|architect|insurance|financial|property management|professional/.test(
    hay
  );
}

module.exports = {
  rankAgainstProfile,
  matchIndustry,
  buildHaystack,
  INDUSTRY_KEYWORDS,
};
