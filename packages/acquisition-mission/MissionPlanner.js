'use strict';

/**
 * SPEC-130 — Mission Planner.
 * Converts operator natural language into a structured acquisition mission contract.
 * Specialists must never parse English — they receive structured intent only.
 */

const { asText } = require('./types');
const {
  inferTargetSegmentFromObjective,
  extractGeography,
  segmentToSearchKey,
  BEACHHEAD_PATTERNS,
} = require('./MissionNaming');
const { createStructuredMission } = require('./StructuredMission');

const GREATER_MANCHESTER_CITIES = Object.freeze([
  'Manchester',
  'Hooksett',
  'Bedford',
  'Auburn',
  'Goffstown',
  'Londonderry',
]);

const SEGMENT_META = Object.freeze({
  short_term_rental: {
    industry: 'hospitality',
    buyer: 'property_operator',
    label: 'Short-term rental operators',
  },
  property_management: {
    industry: 'real_estate',
    buyer: 'property_manager',
    label: 'Property managers',
  },
  law_firm: {
    industry: 'professional_services',
    buyer: 'office_manager',
    label: 'Law firms',
  },
  accounting: {
    industry: 'professional_services',
    buyer: 'office_manager',
    label: 'Accounting firms',
  },
  facility_management: {
    industry: 'facilities',
    buyer: 'facility_manager',
    label: 'Facility managers',
  },
});

function extractCountObjective(text) {
  const match = asText(text).match(/\b(?:acquire|get|win|land|sign|close|add)\s+(?:one|a|an|\d+)\b/i);
  if (!match) return 1;
  const numMatch = match[0].match(/\d+/);
  if (numMatch) return Number(numMatch[0]);
  if (/one|a|an/i.test(match[0])) return 1;
  return 1;
}

function cleanObjective(text) {
  let objective = asText(text);
  objective = objective.replace(/\s+from\s+(?:a|an|one\s+)?[^.]+?(?:\s+in\s+[^.]+)?\.?$/i, '.');
  objective = objective.replace(/\s+in\s+(?:greater\s+)?[A-Za-z][A-Za-z\s,]+?\.?$/i, '.');
  objective = objective.replace(/\s+/g, ' ').trim();
  if (!objective.endsWith('.')) objective += '.';
  return objective;
}

function inferSegmentKey(text, targetSegment) {
  const segmentLabel = asText(targetSegment) || inferTargetSegmentFromObjective(text);
  if (segmentLabel) return segmentToSearchKey(segmentLabel);
  for (const { re } of BEACHHEAD_PATTERNS) {
    if (re.test(text)) return segmentToSearchKey(text.match(re)[0]);
  }
  return null;
}

function inferConstraints(text) {
  const constraints = [];
  const hay = asText(text).toLowerCase();
  if (/\brecurring\b/.test(hay)) constraints.push('recurring');
  if (/\bcommercial(?:\s+only|\s+cleaning|\s+client|\s+customer|\s+account)?\b/.test(hay)) {
    constraints.push('commercial_only');
  }
  if (/\bresidential\b/.test(hay) && !constraints.includes('commercial_only')) {
    constraints.push('residential');
  }
  return constraints;
}

function expandGeography(rawGeography, text) {
  const regionText = asText(rawGeography);
  const hay = `${regionText} ${asText(text)}`.toLowerCase();

  if (/greater\s+manchester|manchester\s+nh|manchester,\s*nh/.test(hay)) {
    const cities = [...GREATER_MANCHESTER_CITIES];
    const extraCities = regionText
      .split(/,|\band\b/i)
      .map((part) => part.trim())
      .filter((part) => /^[A-Z]/.test(part));
    for (const city of extraCities) {
      const normalized = city.replace(/\bNH\b/i, '').trim();
      if (normalized && !cities.includes(normalized)) cities.push(normalized);
    }
    return { region: 'Greater Manchester', cities };
  }

  if (/charleston|kanawha|west virginia|\bwv\b/.test(hay)) {
    return { region: 'Charleston WV', cities: ['Charleston', 'South Charleston', 'St. Albans'] };
  }

  if (/nashville/.test(hay)) {
    return { region: 'Nashville TN', cities: ['Nashville'] };
  }

  const cityMatches = regionText.split(/,|\band\b/i).map((part) => part.trim()).filter(Boolean);
  if (cityMatches.length > 1) {
    return { region: regionText, cities: cityMatches };
  }

  return {
    region: regionText || null,
    cities: cityMatches.length ? cityMatches : regionText ? [regionText] : [],
  };
}

function segmentMeta(segmentKey, segmentLabel) {
  const meta = SEGMENT_META[segmentKey] || {};
  return {
    segment: segmentKey,
    industry: meta.industry || 'general',
    buyer: meta.buyer || 'business_owner',
    label: meta.label || segmentLabel || segmentKey,
  };
}

/**
 * Plan a structured acquisition mission from operator natural language.
 * @param {string} sourceText
 * @param {object} [opts]
 * @returns {object}
 */
function planFromObjective(sourceText, opts = {}) {
  const text = asText(sourceText);
  if (!text) throw new Error('Objective text is required for mission planning.');

  const segmentLabel = asText(opts.targetSegment) || inferTargetSegmentFromObjective(text);
  const segmentKey = inferSegmentKey(text, segmentLabel);
  const geography = expandGeography(extractGeography(text), text);
  const constraints = [
    ...inferConstraints(text),
    ...(Array.isArray(opts.constraints) ? opts.constraints.map(asText).filter(Boolean) : []),
  ];

  const draft = createStructuredMission({
    missionType: 'acquisition',
    objective: cleanObjective(text),
    successMetric: {
      type: 'customers',
      target: extractCountObjective(text),
    },
    market: segmentMeta(segmentKey, segmentLabel),
    geography,
    constraints: [...new Set(constraints.map((row) => row.toLowerCase()))],
    priority: opts.priority != null ? opts.priority : 1,
    sourceText: text,
  });

  return {
    draft,
    understanding: require('./StructuredMission').formatMissionUnderstanding(draft),
  };
}

module.exports = {
  planFromObjective,
  inferSegmentKey,
  inferConstraints,
  expandGeography,
  cleanObjective,
  GREATER_MANCHESTER_CITIES,
};
