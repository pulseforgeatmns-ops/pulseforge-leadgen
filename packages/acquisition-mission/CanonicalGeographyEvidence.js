'use strict';

/**
 * Resolve mission geography from durable tenant evidence when the operator
 * objective does not state a local region. Fail closed — never invent geography.
 */

const { asText } = require('./types');
const { extractGeography } = require('./MissionNaming');
const { pickByPrecedence } = require('./ContextPrecedence');

function expandGeography(regionText, text) {
  // Lazy require avoids MissionPlanner ↔ CanonicalGeographyEvidence cycle.
  return require('./MissionPlanner').expandGeography(regionText, text);
}

const GEOGRAPHY_EVIDENCE_SOURCES = Object.freeze([
  'approved_blueprint_target_markets',
  'approved_blueprint_icp_geography',
  'approved_blueprint_geography',
  'client_intelligence_summary',
  'operator_objective_text',
]);

function presentText(value) {
  return asText(value).replace(/\s+/g, ' ').trim();
}

function summaryApproved(summary) {
  if (!summary || typeof summary !== 'object') return false;
  if (summary.approved === true) return true;
  return String(summary.status || '').toLowerCase() === 'approved';
}

function geographyFromRaw(raw, hintText = '') {
  const text = presentText(raw);
  if (!text) return null;
  return expandGeography(text, hintText || text);
}

function pickFromSummary(summary) {
  if (!summary || typeof summary !== 'object') return null;
  const geo = presentText(summary.geography || summary.targetMarkets);
  if (geo) {
    const fieldSources = summary.fieldSources || {};
    const srcMeta = fieldSources.geography || fieldSources.targetMarkets || {};
    return {
      raw: geo,
      source: srcMeta.source === 'canonical' ? 'approved_blueprint_target_markets' : 'approved_blueprint_target_markets',
      sourceId: summary.blueprintId || summary.canonicalSnapshotId || null,
      field: summary.geography ? 'geography' : 'targetMarkets',
      validationState: summaryApproved(summary) ? 'approved' : 'draft',
    };
  }

  const icpGeo = presentText(summary.idealCustomersGeography);
  if (icpGeo) {
    return {
      raw: icpGeo,
      source: 'approved_blueprint_icp_geography',
      sourceId: summary.blueprintId || summary.canonicalSnapshotId || null,
      field: 'idealCustomersGeography',
      validationState: summaryApproved(summary) ? 'approved' : 'draft',
    };
  }

  return null;
}

function pickFromBlueprint(blueprint) {
  if (!blueprint || typeof blueprint !== 'object') return null;
  if (String(blueprint.status || '').toLowerCase() !== 'approved' && !blueprint.geography && !blueprint.region) {
    return null;
  }
  const raw =
    blueprint.geography ||
    blueprint.region ||
    blueprint.targetMarkets ||
    null;
  if (!raw) return null;
  const text = typeof raw === 'object' ? presentText(raw.region || JSON.stringify(raw)) : presentText(raw);
  if (!text) return null;
  return {
    raw: text,
    source: 'approved_blueprint_geography',
    sourceId: blueprint.id || null,
    field: 'blueprint.geography',
    validationState: String(blueprint.status || '').toLowerCase() === 'approved' ? 'approved' : 'draft',
  };
}

function pickFromObjectiveText(objectiveText) {
  const text = presentText(objectiveText);
  if (!text) return null;
  const mention = extractGeography(text);
  if (mention) {
    return {
      raw: mention,
      source: 'operator_objective_text',
      sourceId: null,
      field: 'objective',
      validationState: 'operator',
    };
  }
  if (/\b(?:united states|u\.?s\.?a?\.?|usa)\b/i.test(text)) {
    return {
      raw: 'United States',
      source: 'operator_objective_text',
      sourceId: null,
      field: 'objective',
      validationState: 'operator',
    };
  }
  return null;
}

function buildEvidenceRow(picked, context = {}) {
  if (!picked || !picked.raw) return null;
  const geography = geographyFromRaw(picked.raw, picked.raw);
  if (!geography || !geography.region) return null;
  return {
    geography,
    source: picked.source,
    sourceId: picked.sourceId || null,
    field: picked.field,
    validationState: picked.validationState || null,
    tenantId:
      context.tenantId != null
        ? String(context.tenantId)
        : context.clientId != null
          ? String(context.clientId)
          : null,
  };
}

/**
 * @param {object} context
 * @returns {object|null}
 */
function extractCanonicalGeographyEvidence(context = {}) {
  const summary =
    context.summary ||
    context.clientIntelligence ||
    context.client_intelligence ||
    null;
  const blueprint = context.blueprint || null;
  const objectiveText =
    context.objectiveText ||
    context.canonicalObjective ||
    context.objective ||
    null;

  const explicit = presentText(context.geography || context.region);
  if (explicit) {
    return buildEvidenceRow(
      {
        raw: explicit,
        source: 'client_intelligence_summary',
        sourceId: null,
        field: 'explicit',
        validationState: 'explicit',
      },
      context
    );
  }

  const fromSummary = pickFromSummary(summary);
  const fromBlueprint = pickFromBlueprint(blueprint);
  const fromObjective = pickFromObjectiveText(objectiveText);

  const candidates = pickByPrecedence([
    fromSummary ? { source: 'workspace', value: fromSummary } : null,
    fromBlueprint ? { source: 'blueprint', value: fromBlueprint } : null,
    fromObjective ? { source: 'operator', value: fromObjective } : null,
  ]);

  if (!candidates || !candidates.value) return null;
  return buildEvidenceRow(candidates.value, context);
}

function buildMissingGeographyAmbiguity(context = {}) {
  const closest = extractCanonicalGeographyEvidence(context);
  return {
    field: 'geography.region',
    question: 'Which region should this mission cover?',
    choices: [],
    reason: closest
      ? 'Canonical geography evidence was present but could not be mapped to a mission region.'
      : 'No geography was stated in the objective and no approved tenant geography evidence is available.',
    closestEvidence: closest || null,
    schemaHint: {
      geography: { region: 'string', cities: ['string'], scope: 'nationwide|regional|local (optional)' },
      summaryFields: ['geography', 'targetMarkets', 'idealCustomersGeography'],
    },
  };
}

/**
 * Choices for operator clarification — tenant evidence only, never platform defaults.
 * @param {object} context
 * @returns {object[]}
 */
function tenantGeographyChoices(context = {}) {
  const choices = [];
  const evidence = extractCanonicalGeographyEvidence(context);
  if (evidence && evidence.geography && evidence.geography.region) {
    choices.push({
      id: 'canonical_geography',
      label: `${evidence.geography.region} (from canonical evidence)`,
      value: evidence.geography,
    });
  }
  return choices;
}

module.exports = {
  GEOGRAPHY_EVIDENCE_SOURCES,
  extractCanonicalGeographyEvidence,
  buildMissingGeographyAmbiguity,
  tenantGeographyChoices,
  geographyFromRaw,
};
