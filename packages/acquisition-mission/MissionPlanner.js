'use strict';

/**
 * SPEC-130 — Mission Planning Engine.
 * Interpret once. Execute many.
 *
 * Pipeline: Intent Analysis → Entity Extraction → Mission Structuring →
 * Ambiguity Detection → Operator Review → Mission Lock.
 * Never performs execution. Specialists never parse operator English.
 */

const { asText } = require('./types');
const {
  inferTargetSegmentFromObjective,
  extractGeography,
  segmentToSearchKey,
  BEACHHEAD_PATTERNS,
} = require('./MissionNaming');
const {
  createStructuredMission,
  formatMissionUnderstanding,
  formatOperatorConfirmation,
  formatAmbiguityPrompt,
  isReadyForLock,
  MISSION_TYPES,
  EXECUTION_STATES,
} = require('./StructuredMission');
const { mayAssign, pickByPrecedence } = require('./ContextPrecedence');

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

const INTENT_PATTERNS = Object.freeze([
  { type: MISSION_TYPES.ACQUISITION, re: /\b(?:acquire|win|land|sign|close|find|source|prospect|outreach|get (?:a |one |new )?client)/i },
  { type: MISSION_TYPES.RETENTION, re: /\b(?:retain|retention|churn|keep (?:the )?client)/i },
  { type: MISSION_TYPES.EXPANSION, re: /\b(?:expand|upsell|cross-sell|additional location)/i },
  { type: MISSION_TYPES.MARKETING, re: /\b(?:marketing|content campaign|social posts?|brand awareness)\b/i },
  { type: MISSION_TYPES.HIRING, re: /\b(?:hir(?:e|ing)|recruit|job opening)\b/i },
  { type: MISSION_TYPES.OPERATIONS, re: /\b(?:operations?|ops process|workflow)\b/i },
  { type: MISSION_TYPES.RESEARCH, re: /\b(?:research|investigate|market scan)\b/i },
  { type: MISSION_TYPES.SUPPORT, re: /\b(?:support ticket|customer support|help desk)\b/i },
  { type: MISSION_TYPES.KNOWLEDGE, re: /\b(?:knowledge base|playbook|document this)\b/i },
]);

function addProvenance(rows, field, value, confidence, reason, source) {
  if (value == null || value === '') return;
  const existing = rows.findIndex((row) => row.field === field);
  const entry = { field, value, confidence, reason, source };
  if (existing >= 0) {
    if (!mayAssign(rows[existing].source, source)) return;
    rows[existing] = entry;
    return;
  }
  rows.push(entry);
}

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
  if (/\brecurring(?:\s+only)?\b/.test(hay)) constraints.push('recurring');
  if (/\bcommercial(?:\s+only|\s+cleaning|\s+client|\s+customer|\s+account)?\b/.test(hay)) {
    constraints.push('commercial_only');
  }
  if (/\bresidential\b/.test(hay) && !constraints.includes('commercial_only')) {
    constraints.push('residential');
  }
  return constraints;
}

function greaterManchesterGeography() {
  return { region: 'Greater Manchester', cities: [...GREATER_MANCHESTER_CITIES] };
}

function expandGeography(rawGeography, text) {
  const regionText = asText(rawGeography);
  const hay = `${regionText} ${asText(text)}`.toLowerCase();

  if (/manchester\s+uk|manchester,\s*uk|greater\s+manchester(?:\s+uk)?\s*(?:england)?/.test(hay)
    && /uk|england|united kingdom/.test(hay)) {
    return { region: 'Manchester UK', cities: ['Manchester'] };
  }

  if (/greater\s+manchester|manchester\s+nh|manchester,\s*nh|new hampshire/.test(hay)) {
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

function analyzeIntent(text, opts = {}) {
  if (opts.missionType && Object.values(MISSION_TYPES).includes(opts.missionType)) {
    return {
      type: opts.missionType,
      confidence: 1,
      reason: 'Operator supplied mission type.',
      source: 'operator',
    };
  }
  const matches = INTENT_PATTERNS.filter((row) => row.re.test(text));
  if (matches.length === 1) {
    return {
      type: matches[0].type,
      confidence: 0.93,
      reason: `Matched ${matches[0].type} intent language.`,
      source: 'operator',
    };
  }
  if (matches.length > 1) {
    return {
      type: matches[0].type,
      confidence: 0.45,
      reason: `Multiple mission types matched: ${matches.map((row) => row.type).join(', ')}.`,
      source: 'operator',
      ambiguous: true,
      candidates: matches.map((row) => row.type),
    };
  }
  return {
    type: MISSION_TYPES.ACQUISITION,
    confidence: 0.55,
    reason: 'Defaulted to acquisition; no stronger mission-type language.',
    source: 'general_knowledge',
  };
}

function isBareManchester(text, regionText) {
  const hay = `${asText(regionText)} ${asText(text)}`.toLowerCase();
  if (!/\bmanchester\b/.test(hay)) return false;
  if (/manchester\s+nh|manchester,\s*nh|greater\s+manchester|new hampshire|\bnh\b/.test(hay)) {
    return false;
  }
  if (/manchester\s+uk|manchester,\s*uk|\buk\b|england|united kingdom/.test(hay)) {
    return false;
  }
  return true;
}

function isAmbiguousPropertyManager(text, segmentKey) {
  const hay = asText(text).toLowerCase();
  if (segmentKey && segmentKey !== 'property_management') return false;
  if (!/\bproperty managers?\b/.test(hay) && segmentKey !== 'property_management') return false;
  if (/\bshort[- ]term rental|\bstr\b|\bairbnb|\bvrbo|\bvacation rental/.test(hay)) return false;
  if (/\bresidential\b/.test(hay) || /\bcommercial\b/.test(hay) || /\bmixed\b/.test(hay)) return false;
  return /\bproperty managers?\b/.test(hay);
}

function blueprintGeography(context = {}) {
  const blueprint = (context && context.blueprint) || {};
  const raw = blueprint.geography || blueprint.region || blueprint.targetMarkets || null;
  if (!raw) return null;
  if (typeof raw === 'object') return expandGeography(raw.region || '', JSON.stringify(raw));
  return expandGeography(String(raw), String(raw));
}

function detectAmbiguities(extracted, text, opts = {}) {
  const ambiguities = [];
  const hay = asText(text);
  const resolutions = opts.resolutions || {};

  if (extracted.intent && extracted.intent.ambiguous && !resolutions.missionType) {
    ambiguities.push({
      field: 'missionType',
      question: 'Which kind of mission is this?',
      choices: (extracted.intent.candidates || []).map((type) => ({
        id: type,
        label: type.charAt(0).toUpperCase() + type.slice(1),
        value: type,
      })),
      reason: extracted.intent.reason,
    });
  }

  if (isAmbiguousPropertyManager(hay, extracted.segmentKey) && !resolutions.segment) {
    ambiguities.push({
      field: 'market.segment',
      question: 'Residential? Commercial? Short-term rental? Mixed?',
      choices: [
        { id: 'residential', label: 'Residential', value: { segment: 'property_management', constraint: 'residential' } },
        { id: 'commercial', label: 'Commercial', value: { segment: 'property_management', constraint: 'commercial_only' } },
        { id: 'short_term_rental', label: 'Short-term rental', value: { segment: 'short_term_rental' } },
        { id: 'mixed', label: 'Mixed', value: { segment: 'property_management' } },
      ],
      reason: 'Property manager is ambiguous without a market slice.',
    });
  }

  if (isBareManchester(hay, extracted.geography && extracted.geography.region) && !resolutions.geography) {
    const blueprintGeo = blueprintGeography(opts.context);
    const nhChoice = {
      id: 'manchester_nh',
      label: blueprintGeo && /manchester/i.test(blueprintGeo.region || '')
        ? 'Manchester NH (from Blueprint)'
        : 'Manchester NH',
      value: greaterManchesterGeography(),
    };
    ambiguities.push({
      field: 'geography.region',
      question: 'Manchester NH or Manchester UK?',
      choices: [
        nhChoice,
        { id: 'manchester_uk', label: 'Manchester UK', value: { region: 'Manchester UK', cities: ['Manchester'] } },
      ],
      reason: 'Manchester is ambiguous without a country or state.',
    });
  }

  const geo = extracted.geography || {};
  if (!geo.region && !(geo.cities && geo.cities.length) && !isBareManchester(hay, geo.region)) {
    const blueprintGeo = blueprintGeography(opts.context);
    const choices = [];
    if (blueprintGeo && blueprintGeo.region) {
      choices.push({
        id: 'blueprint_geography',
        label: `${blueprintGeo.region} (from Blueprint)`,
        value: blueprintGeo,
      });
    }
    choices.push(
      { id: 'manchester_nh', label: 'Greater Manchester NH', value: greaterManchesterGeography() },
      { id: 'charleston_wv', label: 'Charleston WV', value: { region: 'Charleston WV', cities: ['Charleston', 'South Charleston', 'St. Albans'] } }
    );
    ambiguities.push({
      field: 'geography.region',
      question: 'Which region should this mission cover?',
      choices,
      reason: 'No geography was stated.',
    });
  }

  if (!extracted.segmentKey && !resolutions.segment && !isAmbiguousPropertyManager(hay, extracted.segmentKey)) {
    ambiguities.push({
      field: 'market.segment',
      question: 'Which market should Scout pursue?',
      choices: Object.entries(SEGMENT_META).map(([id, meta]) => ({
        id,
        label: meta.label,
        value: { segment: id },
      })),
      reason: 'No market segment was stated.',
    });
  }

  return ambiguities;
}

function applyResolutions(extracted, resolutions = {}) {
  const next = { ...extracted, geography: { ...(extracted.geography || {}) }, constraints: [...(extracted.constraints || [])] };
  if (resolutions.missionType) next.intent = { type: resolutions.missionType, confidence: 1, reason: 'Operator chose mission type.', source: 'operator' };
  if (resolutions.segment) {
    next.segmentKey = resolutions.segment;
    next.segmentLabel = (SEGMENT_META[resolutions.segment] || {}).label || resolutions.segment;
  }
  if (resolutions.constraint && !next.constraints.includes(resolutions.constraint)) {
    next.constraints.push(resolutions.constraint);
  }
  if (resolutions.geography) {
    next.geography = { ...resolutions.geography };
    next.geographyResolved = true;
  }
  return next;
}

function applyContextPrecedence(extracted, context = {}) {
  const next = { ...extracted, geography: { ...(extracted.geography || {}) } };
  if (next.geography.region) return next;

  const safeContext = context || {};
  const blueprintGeo = blueprintGeography(safeContext);
  const workspaceGeo = safeContext.workspace && (safeContext.workspace.geography || safeContext.workspace.region);
  const picked = pickByPrecedence([
    blueprintGeo && blueprintGeo.region
      ? { source: 'blueprint', value: blueprintGeo }
      : null,
    workspaceGeo
      ? { source: 'workspace', value: typeof workspaceGeo === 'object' ? workspaceGeo : expandGeography(workspaceGeo, workspaceGeo) }
      : null,
  ]);
  if (picked && picked.value) {
    next.contextGeography = { ...picked.value, source: picked.source };
  }
  return next;
}

function inferEvidence(text, opts = {}) {
  const hay = asText(text).toLowerCase();
  if (opts.evidence) return opts.evidence;
  if (/\bhigh(?:er)? confidence\b|\bstrong evidence\b/.test(hay)) {
    return { minimumConfidence: 0.85, minimumBuyingSignals: 3, thresholdLabel: 'high' };
  }
  if (/\blow(?:er)? confidence\b|\blight evidence\b/.test(hay)) {
    return { minimumConfidence: 0.5, minimumBuyingSignals: 1, thresholdLabel: 'low' };
  }
  return { minimumConfidence: 0.7, minimumBuyingSignals: 2, thresholdLabel: 'medium' };
}

/**
 * Plan a structured mission from operator natural language.
 * Does not execute specialists. Asks when operator language is ambiguous.
 * @param {string} sourceText
 * @param {object} [opts]
 * @returns {object}
 */
function planFromObjective(sourceText, opts = {}) {
  const text = asText(sourceText);
  if (!text) throw new Error('Objective text is required for mission planning.');

  const provenance = [];
  const intent = analyzeIntent(text, opts);
  addProvenance(provenance, 'missionType', intent.type, intent.confidence, intent.reason, intent.source);

  const segmentLabel = asText(opts.targetSegment) || inferTargetSegmentFromObjective(text);
  let segmentKey = inferSegmentKey(text, segmentLabel);
  if (segmentKey) {
    const reason = /short[- ]term rental|\bstr\b|airbnb|vrbo/i.test(text)
      ? 'Matched STR operator taxonomy.'
      : `Matched ${segmentKey} taxonomy.`;
    addProvenance(provenance, 'market.segment', segmentKey, segmentKey === 'short_term_rental' ? 0.96 : 0.9, reason, 'operator');
  }

  const geographyMention = extractGeography(text)
    || asText(opts.geography)
    || ((asText(text).match(/\b(?:around|near|in)\s+([A-Za-z][A-Za-z\s,]+?)(?:\.|$)/i) || [])[1] || '');
  const extractedGeography = expandGeography(geographyMention, text);
  let extracted = {
    intent,
    segmentKey,
    segmentLabel,
    geography: extractedGeography,
    constraints: [
      ...inferConstraints(text),
      ...(Array.isArray(opts.constraints) ? opts.constraints.map(asText).filter(Boolean) : []),
    ],
  };

  extracted = applyContextPrecedence(extracted, opts.context || {});
  extracted = applyResolutions(extracted, opts.resolutions || {});

  const bareManchester = isBareManchester(text, extracted.geography && extracted.geography.region);
  if (bareManchester && !(opts.resolutions && opts.resolutions.geography)) {
    extracted.geography = { region: null, cities: [], mention: 'Manchester' };
  } else if (extracted.geography && extracted.geography.region) {
    addProvenance(
      provenance,
      'geography.region',
      extracted.geography.region,
      /greater manchester|manchester nh/i.test(extracted.geography.region) ? 0.95 : 0.88,
      'Normalized region from operator geography.',
      'operator'
    );
  } else if (extracted.contextGeography && extracted.contextGeography.region) {
    extracted.geography = extracted.contextGeography;
    addProvenance(
      provenance,
      'geography.region',
      extracted.geography.region,
      0.8,
      'Filled missing geography from context precedence. Blueprint informs; operator can still edit.',
      extracted.contextGeography.source
    );
  }

  if (extracted.segmentKey && extracted.segmentKey !== segmentKey) {
    segmentKey = extracted.segmentKey;
    addProvenance(provenance, 'market.segment', segmentKey, 1, 'Operator resolved market ambiguity.', 'operator');
  }

  const market = segmentMeta(extracted.segmentKey, extracted.segmentLabel);
  if (market.industry) {
    addProvenance(provenance, 'market.industry', market.industry, 0.9, 'Derived from segment taxonomy.', 'general_knowledge');
  }
  if (market.buyer) {
    addProvenance(provenance, 'market.buyer', market.buyer, 0.9, 'Derived from segment taxonomy.', 'general_knowledge');
  }

  const evidence = inferEvidence(text, opts);
  addProvenance(provenance, 'evidence.minimum_confidence', evidence.minimumConfidence, 0.8, `Evidence threshold ${evidence.thresholdLabel}.`, 'general_knowledge');

  const ambiguities = detectAmbiguities(extracted, text, opts);
  const draft = createStructuredMission({
    missionType: extracted.intent.type,
    objective: cleanObjective(text),
    successMetric: {
      type: /recurr/i.test(text) ? 'recurring_clients' : 'customers',
      target: extractCountObjective(text),
    },
    market,
    geography: extracted.geography,
    constraints: [...new Set(extracted.constraints.map((row) => asText(row).toLowerCase()).filter(Boolean))],
    priority: opts.priority != null ? opts.priority : 1,
    evidence,
    provenance,
    ambiguities,
    sourceText: text,
    execution: { state: ambiguities.length ? EXECUTION_STATES.DRAFTING : EXECUTION_STATES.PLANNED },
  }, { allowIncomplete: true });

  const readyForConfirmation = isReadyForLock(draft);
  return {
    draft,
    ambiguities,
    readyForConfirmation,
    understanding: formatMissionUnderstanding(draft),
    confirmation: readyForConfirmation ? formatOperatorConfirmation(draft) : null,
    clarification: ambiguities[0] || null,
    clarificationPrompt: ambiguities[0] ? formatAmbiguityPrompt(ambiguities[0]) : null,
    intent: extracted.intent,
    pipeline: [
      'intent_analysis',
      'entity_extraction',
      'mission_structuring',
      'ambiguity_detection',
      readyForConfirmation ? 'operator_review' : 'operator_clarification',
    ],
    resolutions: opts.resolutions || null,
    executed: false,
  };
}

function matchChoice(ambiguity, answer) {
  const hay = asText(answer).toLowerCase();
  if (!ambiguity || !hay) return null;
  const choices = ambiguity.choices || [];
  const byId = choices.find((choice) => hay === String(choice.id).toLowerCase());
  if (byId) return byId;
  const byLabel = choices.find((choice) => hay.includes(String(choice.label).toLowerCase()) || String(choice.label).toLowerCase().includes(hay));
  if (byLabel) return byLabel;
  if (ambiguity.field === 'geography.region') {
    if (/\bnh\b|new hampshire/.test(hay)) return choices.find((choice) => choice.id === 'manchester_nh') || null;
    if (/\buk\b|england/.test(hay)) return choices.find((choice) => choice.id === 'manchester_uk') || null;
  }
  if (ambiguity.field === 'market.segment') {
    if (/\bstr\b|short[- ]term/.test(hay)) return choices.find((choice) => choice.id === 'short_term_rental') || null;
    if (/\bcommercial\b/.test(hay)) return choices.find((choice) => choice.id === 'commercial') || null;
    if (/\bresidential\b/.test(hay)) return choices.find((choice) => choice.id === 'residential') || null;
    if (/\bmixed\b/.test(hay)) return choices.find((choice) => choice.id === 'mixed') || null;
  }
  return null;
}

function resolutionsFromChoice(ambiguity, choice, existing = {}) {
  const resolutions = { ...existing };
  const value = choice && choice.value;
  if (!ambiguity || !choice) return resolutions;
  if (ambiguity.field === 'missionType') resolutions.missionType = value || choice.id;
  if (ambiguity.field === 'market.segment' && value) {
    if (value.segment) resolutions.segment = value.segment;
    if (value.constraint) resolutions.constraint = value.constraint;
  }
  if (ambiguity.field === 'geography.region' && value) {
    resolutions.geography = value;
  }
  return resolutions;
}

/**
 * Apply an operator clarification and replan. Never guesses the unanswered remainder.
 */
function applyClarification(sourceText, answer, opts = {}) {
  const prior = opts.prior || planFromObjective(sourceText, opts);
  const ambiguity = (prior.ambiguities || [])[0];
  if (!ambiguity) return prior;
  const choice = matchChoice(ambiguity, answer);
  if (!choice) {
    return {
      ...prior,
      unmatchedClarification: true,
      clarificationPrompt: formatAmbiguityPrompt(ambiguity),
    };
  }
  const resolutions = resolutionsFromChoice(ambiguity, choice, opts.resolutions || prior.resolutions || {});
  const next = planFromObjective(sourceText, { ...opts, resolutions });
  next.resolutions = resolutions;
  return next;
}

/**
 * Operator edit of a draft plan — replans with explicit field overrides.
 */
function applyEdits(sourceText, edits = {}, opts = {}) {
  const resolutions = { ...(opts.resolutions || {}) };
  if (edits.missionType) resolutions.missionType = edits.missionType;
  if (edits.segment) resolutions.segment = edits.segment;
  if (edits.constraint) resolutions.constraint = edits.constraint;
  if (edits.geography) resolutions.geography = edits.geography;
  if (asText(edits.region) || asText(edits.geographyText)) {
    resolutions.geography = expandGeography(edits.region || edits.geographyText, edits.region || edits.geographyText);
  }
  return planFromObjective(edits.objective || sourceText, { ...opts, resolutions, targetSegment: edits.targetSegment || opts.targetSegment });
}

module.exports = {
  planFromObjective,
  applyClarification,
  applyEdits,
  matchChoice,
  inferSegmentKey,
  inferConstraints,
  expandGeography,
  cleanObjective,
  analyzeIntent,
  detectAmbiguities,
  GREATER_MANCHESTER_CITIES,
  SEGMENT_META,
};
