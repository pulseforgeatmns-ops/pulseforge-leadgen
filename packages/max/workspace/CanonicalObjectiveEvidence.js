'use strict';

/**
 * SPEC-168 extension — recover mission objective text from durable tenant evidence
 * when operator phrasing does not pass line classification (e.g. "Book discovery calls…").
 *
 * Fail closed: returns null when no canonical objective wording exists.
 */

const { asText } = require('../../acquisition-mission/types');

const OBJECTIVE_EVIDENCE_SOURCES = Object.freeze([
  'operator_objective',
  'approved_blueprint_campaign_goals',
  'approved_blueprint_growth_focus',
  'approved_blueprint_section',
  'blueprint_strategy',
  'client_intelligence_summary',
]);

function presentText(value) {
  return asText(value).replace(/\s+/g, ' ').trim();
}

function sectionSummary(sections, key) {
  if (!sections || typeof sections !== 'object') return '';
  const section = sections[key];
  if (!section || typeof section !== 'object') return '';
  return presentText(section.summary || section.text || '');
}

function pickOperatorObjective(objectives = [], clientId = null) {
  const rows = Array.isArray(objectives) ? objectives : [];
  if (!rows.length) return null;

  const scoped = rows.filter((row) => {
    if (!row || row.status !== 'active') return false;
    if (row.scope === 'client' && clientId != null) {
      return Number(row.clientId) === Number(clientId);
    }
    return row.scope === 'operator' || row.scope === 'client';
  });

  const clientScoped = scoped.filter(
    (row) => row.scope === 'client' && clientId != null && Number(row.clientId) === Number(clientId)
  );
  const ordered = (clientScoped.length ? clientScoped : scoped).slice();
  ordered.sort(
    (a, b) => new Date(b.updatedAt || 0).getTime() - new Date(a.updatedAt || 0).getTime()
  );

  for (const row of ordered) {
    const text = presentText(row.objectiveText || row.objective_text || row.title);
    if (text) {
      return {
        text,
        source: 'operator_objective',
        sourceId: row.id || null,
        field: 'objective_text',
        validationState: row.status || 'active',
        scope: row.scope || null,
      };
    }
  }
  return null;
}

function pickFromSummary(summary) {
  if (!summary || typeof summary !== 'object') return null;
  const campaignGoals = presentText(summary.campaignGoals);
  if (campaignGoals) {
    return {
      text: campaignGoals,
      source: 'approved_blueprint_campaign_goals',
      sourceId: summary.blueprintId || summary.canonicalSnapshotId || null,
      field: 'campaignGoals',
      validationState: summary.approved ? 'approved' : 'draft',
    };
  }
  const growthFocus = presentText(summary.growthFocus);
  if (growthFocus) {
    return {
      text: growthFocus,
      source: 'approved_blueprint_growth_focus',
      sourceId: summary.blueprintId || null,
      field: 'growthFocus',
      validationState: summary.approved ? 'approved' : 'draft',
    };
  }
  return null;
}

function pickFromBlueprint(blueprint) {
  if (!blueprint || typeof blueprint !== 'object') return null;
  if (String(blueprint.status || '').toLowerCase() !== 'approved') return null;

  const sections = blueprint.sections || {};
  const campaignGoals = sectionSummary(sections, 'campaignGoals');
  if (campaignGoals) {
    return {
      text: campaignGoals,
      source: 'approved_blueprint_section',
      sourceId: blueprint.id || null,
      field: 'sections.campaignGoals.summary',
      validationState: 'approved',
      blueprintVersion: blueprint.version || null,
    };
  }
  return null;
}

function pickFromStrategicEvidence(blueprintContext) {
  if (!blueprintContext || typeof blueprintContext !== 'object') return null;
  const strategy = presentText(blueprintContext.strategy);
  if (strategy) {
    return {
      text: strategy,
      source: 'blueprint_strategy',
      sourceId: null,
      field: 'strategy',
      validationState: 'approved',
    };
  }
  return null;
}

/**
 * @param {object} context
 * @param {object} [context.clientIntelligence]
 * @param {object} [context.summary]
 * @param {object} [context.blueprint]
 * @param {object} [context.operatorObjectives]
 * @param {number|string} [context.clientId]
 * @returns {{ text: string, source: string, sourceId: string|null, field: string, validationState: string }|null}
 */
function extractCanonicalObjectiveEvidence(context = {}) {
  const clientId =
    context.clientId != null
      ? context.clientId
      : context.client_id != null
        ? context.client_id
        : null;

  const explicit = presentText(context.canonicalObjective || context.objectiveText);
  if (explicit) {
    return {
      text: explicit,
      source: 'client_intelligence_summary',
      sourceId: null,
      field: 'canonicalObjective',
      validationState: 'explicit',
    };
  }

  const fromOperator = pickOperatorObjective(
    context.operatorObjectives || context.activeObjectives,
    clientId
  );
  if (fromOperator) return fromOperator;

  const summary = context.clientIntelligence || context.summary || null;
  const fromSummary = pickFromSummary(summary);
  if (fromSummary) return fromSummary;

  const blueprint = context.blueprint || null;
  const fromBlueprint = pickFromBlueprint(blueprint);
  if (fromBlueprint) return fromBlueprint;

  const fromStrategy = pickFromStrategicEvidence(context.blueprint);
  if (fromStrategy) return fromStrategy;

  return null;
}

module.exports = {
  OBJECTIVE_EVIDENCE_SOURCES,
  extractCanonicalObjectiveEvidence,
  pickOperatorObjective,
  pickFromSummary,
  pickFromBlueprint,
};
