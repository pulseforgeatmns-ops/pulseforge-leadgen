'use strict';

/**
 * Mission Plan IR — structured intermediate between NL and execution (SPEC-050 / ADR-034).
 * Only the Mission Plan may create executable nodes. Notes never execute.
 */

const { getStage, stageLabel } = require('./StageLibrary');
const { BUILTIN_IDS } = require('../capabilities');

const MISSION_PLAN_VERSION = '1.0.0';

/** Sentence / fragment classification categories (exactly one per unit). */
const PLAN_CATEGORIES = Object.freeze({
  OBJECTIVE: 'objective',
  PARAMETERS: 'parameters',
  EXECUTION: 'execution',
  OPTIONS: 'options',
  NOTES: 'notes',
});

/**
 * Fields that may only originate from runtime artifacts — never operator language.
 */
const RESERVED_RUNTIME_FIELDS = Object.freeze([
  'company',
  'recipient',
  'capability',
  'artifactName',
  'packageName',
  'stageName',
  'decisionMaker',
  'companyName',
  'recipientName',
  'decision_maker',
]);

/** Known parameter schemas for Mission Plan validation. */
const PARAMETER_SCHEMAS = Object.freeze({
  prospectList: { type: 'string', values: ['current', 'attached', 'operator'] },
  client: { type: 'string' },
  subject: { type: 'string' },
  market: { type: 'string' },
  campaign: { type: 'string' },
  budget: { type: 'string' },
  tenant: { type: 'string' },
  targetCount: { type: 'number' },
});

/**
 * Capability aliases resolved against Stage Library / Capability Registry ids.
 * Unknown aliases become Notes — never new runtime nodes.
 */
const EXECUTION_ALIASES = Object.freeze({
  campaign_builder: BUILTIN_IDS.CAMPAIGN_BUILDER,
  'campaign builder': BUILTIN_IDS.CAMPAIGN_BUILDER,
  'build campaign': BUILTIN_IDS.CAMPAIGN_BUILDER,
  'create campaign': BUILTIN_IDS.CAMPAIGN_BUILDER,
  prospect_discovery: BUILTIN_IDS.PROSPECT_DISCOVERY,
  'prospect discovery': BUILTIN_IDS.PROSPECT_DISCOVERY,
  'discover prospects': BUILTIN_IDS.PROSPECT_DISCOVERY,
  company_enrichment: BUILTIN_IDS.COMPANY_ENRICHMENT,
  'company intelligence': BUILTIN_IDS.COMPANY_ENRICHMENT,
  'analyze company': BUILTIN_IDS.COMPANY_ENRICHMENT,
  sales_intelligence: BUILTIN_IDS.SALES_INTELLIGENCE,
  'sales intelligence': BUILTIN_IDS.SALES_INTELLIGENCE,
  opportunity_ranking: BUILTIN_IDS.OPPORTUNITY_RANKING,
  'opportunity ranking': BUILTIN_IDS.OPPORTUNITY_RANKING,
  mail_package_generator: BUILTIN_IDS.MAIL_PACKAGE_GENERATOR,
  'mail package': BUILTIN_IDS.MAIL_PACKAGE_GENERATOR,
  'mail packages': BUILTIN_IDS.MAIL_PACKAGE_GENERATOR,
  'generate mail package': BUILTIN_IDS.MAIL_PACKAGE_GENERATOR,
  'generate mail packages': BUILTIN_IDS.MAIL_PACKAGE_GENERATOR,
  campaign_review: BUILTIN_IDS.CAMPAIGN_REVIEW,
  'campaign review': BUILTIN_IDS.CAMPAIGN_REVIEW,
  'review campaign': BUILTIN_IDS.CAMPAIGN_REVIEW,
  ready_to_print: 'ready_to_print',
  'ready to print': 'ready_to_print',
  proposal_generator: BUILTIN_IDS.PROPOSAL_GENERATOR,
  'generate proposal': BUILTIN_IDS.PROPOSAL_GENERATOR,
  direct_mail_execution: BUILTIN_IDS.DIRECT_MAIL_EXECUTION,
  'direct mail execution': BUILTIN_IDS.DIRECT_MAIL_EXECUTION,
  outcome_intelligence: BUILTIN_IDS.OUTCOME_INTELLIGENCE,
  operator_inbox: BUILTIN_IDS.OPERATOR_INBOX,
  knowledge_update: BUILTIN_IDS.KNOWLEDGE_UPDATE,
});

/**
 * @param {object} [partial]
 * @returns {object} mission_plan
 */
function buildMissionPlan(partial = {}) {
  const parameters =
    partial.parameters && typeof partial.parameters === 'object'
      ? { ...partial.parameters }
      : {};
  const options =
    partial.options && typeof partial.options === 'object'
      ? { ...partial.options }
      : {};
  const notes = normalizeNotes(partial.notes);
  const execution = normalizeExecution(partial.execution);
  const classifications = Array.isArray(partial.classifications)
    ? partial.classifications.map((c) => ({ ...c }))
    : [];

  return Object.freeze({
    version: partial.version || MISSION_PLAN_VERSION,
    objective: String(partial.objective || '').trim(),
    subject: partial.subject != null ? String(partial.subject).trim() : null,
    parameters: Object.freeze(parameters),
    execution: Object.freeze(execution),
    options: Object.freeze({
      review: Boolean(options.review),
      approvalRequired:
        options.approvalRequired != null
          ? Boolean(options.approvalRequired)
          : Boolean(options.review),
      dryRun: Boolean(options.dryRun),
      shadowMode: Boolean(options.shadowMode),
      readyToPrint: Boolean(options.readyToPrint),
      ...Object.fromEntries(
        Object.entries(options).filter(
          ([k]) =>
            ![
              'review',
              'approvalRequired',
              'dryRun',
              'shadowMode',
              'readyToPrint',
            ].includes(k)
        )
      ),
    }),
    notes: Object.freeze(notes),
    classifications: Object.freeze(classifications),
    sourceText: partial.sourceText != null ? String(partial.sourceText) : null,
    createdAt: partial.createdAt || new Date().toISOString(),
  });
}

/**
 * Resolve an execution request string against the capability/stage registry.
 * @param {string} text
 * @returns {{ capabilityId: string|null, stageId: string|null, known: boolean, note: string|null }}
 */
function resolveExecutionRequest(text) {
  const raw = String(text || '').trim();
  if (!raw) {
    return { capabilityId: null, stageId: null, known: false, note: null };
  }
  const lower = raw.toLowerCase().replace(/\s+/g, ' ').trim();

  // Direct id
  if (getStage(lower)) {
    const stage = getStage(lower);
    return {
      capabilityId: stage.capabilityId || stage.id,
      stageId: stage.id,
      known: true,
      note: null,
    };
  }

  // Alias table
  if (EXECUTION_ALIASES[lower]) {
    const id = EXECUTION_ALIASES[lower];
    const stage = getStage(id);
    return {
      capabilityId: (stage && stage.capabilityId) || id,
      stageId: (stage && stage.id) || id,
      known: true,
      note: null,
    };
  }

  // Fuzzy: look for known phrases inside the sentence
  for (const [alias, id] of Object.entries(EXECUTION_ALIASES)) {
    if (alias.length < 4) continue;
    if (lower.includes(alias)) {
      const stage = getStage(id);
      return {
        capabilityId: (stage && stage.capabilityId) || id,
        stageId: (stage && stage.id) || id,
        known: true,
        note: null,
      };
    }
  }

  return {
    capabilityId: null,
    stageId: null,
    known: false,
    note: `Unknown capability: ${raw}`,
  };
}

/**
 * Validate a Mission Plan before execution (SPEC-050).
 * @param {object} plan
 * @param {object} [opts]
 * @param {Set<string>|string[]} [opts.registeredCapabilityIds]
 * @returns {{ ok: boolean, errors: string[], warnings: string[] }}
 */
function validateMissionPlan(plan, opts = {}) {
  const errors = [];
  const warnings = [];
  if (!plan || typeof plan !== 'object') {
    return { ok: false, errors: ['Mission Plan is required'], warnings: [] };
  }
  if (!String(plan.objective || '').trim()) {
    errors.push('Mission Plan requires an objective');
  }

  const registered = new Set(
    opts.registeredCapabilityIds || Object.values(BUILTIN_IDS)
  );

  const execList = normalizeExecution(plan.execution);
  for (const item of execList) {
    const resolved =
      typeof item === 'string'
        ? resolveExecutionRequest(item)
        : {
            known: Boolean(item && item.stageId),
            stageId: item && item.stageId,
            capabilityId: item && item.capabilityId,
            note: item && item.note,
          };
    if (!resolved.known) {
      errors.push(
        resolved.note ||
          `Unknown capability in execution: ${JSON.stringify(item)}`
      );
      continue;
    }
    if (!getStage(resolved.stageId)) {
      errors.push(`Execution stage is not registered: ${resolved.stageId}`);
    }
    if (
      resolved.capabilityId &&
      registered.size &&
      !registered.has(resolved.capabilityId) &&
      resolved.capabilityId !== resolved.stageId
    ) {
      // ready_to_print has null capability — allow stage-only ids present in library
      if (!getStage(resolved.stageId)) {
        errors.push(
          `Execution capability is not registered: ${resolved.capabilityId}`
        );
      }
    }
  }

  const params = plan.parameters || {};
  for (const [key, value] of Object.entries(params)) {
    if (RESERVED_RUNTIME_FIELDS.includes(key)) {
      errors.push(
        `Reserved runtime field cannot be set from operator text: ${key}`
      );
      continue;
    }
    const schema = PARAMETER_SCHEMAS[key];
    if (!schema) {
      warnings.push(`Unknown parameter schema: ${key}`);
      continue;
    }
    if (schema.type === 'number' && value != null && !Number.isFinite(Number(value))) {
      errors.push(`Parameter ${key} must be a number`);
    }
    if (
      schema.values &&
      value != null &&
      !schema.values.includes(String(value).toLowerCase())
    ) {
      warnings.push(
        `Parameter ${key} value "${value}" is outside known values`
      );
    }
  }

  // Notes must never appear as execution entries
  for (const note of normalizeNotes(plan.notes)) {
    const lower = note.toLowerCase();
    if (
      RESERVED_RUNTIME_FIELDS.some(
        (f) => lower.startsWith(`${f.toLowerCase()}:`) || lower.startsWith(`${f.toLowerCase()} =`)
      )
    ) {
      errors.push(
        `Operator note attempts to set reserved runtime field: ${note.slice(0, 80)}`
      );
    }
  }

  return {
    ok: errors.length === 0,
    errors,
    warnings,
  };
}

/**
 * Operator-facing summary for Review Workspace.
 * @param {object} plan
 * @returns {object}
 */
function summarizeMissionPlan(plan) {
  const p = plan && typeof plan === 'object' ? plan : buildMissionPlan({});
  const exec = normalizeExecution(p.execution).map((item) => {
    if (typeof item === 'string') {
      const r = resolveExecutionRequest(item);
      return r.known ? stageLabel(r.stageId) : item;
    }
    return stageLabel(item.stageId) || item.capabilityId || String(item);
  });
  return {
    objective: p.objective || '',
    subject: p.subject || null,
    execution: exec,
    parameters: { ...(p.parameters || {}) },
    options: { ...(p.options || {}) },
    notes: normalizeNotes(p.notes),
    reviewEnabled: Boolean(p.options && p.options.review),
    approvalRequired: Boolean(p.options && p.options.approvalRequired),
  };
}

/**
 * Text used for stage keyword matching — excludes Notes (SPEC-050).
 * @param {object} plan
 * @returns {string}
 */
function executableObjectiveText(plan) {
  if (!plan) return '';
  const parts = [];
  if (plan.objective) parts.push(String(plan.objective));
  if (plan.subject) parts.push(`for ${plan.subject}`);
  for (const item of normalizeExecution(plan.execution)) {
    if (typeof item === 'string') parts.push(item);
    else if (item && item.label) parts.push(item.label);
    else if (item && item.stageId) parts.push(stageLabel(item.stageId));
  }
  if (plan.options) {
    // Do not inject bare "Review" into keyword text — Campaign Review stage is
    // selected only via explicit execution entries / Ready To Print (SPEC-050).
    if (plan.options.readyToPrint) parts.push('Ready to Print');
    if (plan.options.dryRun) parts.push('Dry Run');
    if (plan.options.shadowMode) parts.push('Shadow Mode');
  }
  return parts.filter(Boolean).join('. ');
}

/**
 * Ensure operator instruction fragments never appear in structured business outputs.
 * @param {string} text
 * @param {object} plan
 * @returns {boolean} true when text appears to contain operator objective/notes fragments
 */
function containsOperatorInstructionLeak(text, plan) {
  const hay = String(text || '').toLowerCase().trim();
  if (!hay || !plan) return false;
  const needles = [
    ...normalizeNotes(plan.notes),
    plan.sourceText,
  ]
    .filter(Boolean)
    .map((s) => String(s).toLowerCase().trim())
    .filter((s) => s.length >= 24);
  for (const n of needles) {
    if (hay.includes(n) || n.includes(hay)) return true;
  }
  // Distinctive note fragments (e.g. "generated letters", "human test")
  for (const note of normalizeNotes(plan.notes)) {
    const lower = note.toLowerCase();
    for (const frag of [
      'generated letters',
      'human test',
      'confidence scores',
      'inspect messaging',
    ]) {
      if (lower.includes(frag) && hay.includes(frag)) return true;
    }
    const words = lower.split(/\s+/).filter((w) => w.length > 4);
    if (words.length >= 3) {
      const phrase = words.slice(0, 4).join(' ');
      if (hay.includes(phrase)) return true;
    }
  }
  return false;
}

function normalizeNotes(notes) {
  if (notes == null) return [];
  if (Array.isArray(notes)) {
    return notes.map((n) => String(n).trim()).filter(Boolean);
  }
  return String(notes)
    .split(/\n+/)
    .map((n) => n.trim())
    .filter(Boolean);
}

function normalizeExecution(execution) {
  if (execution == null) return [];
  if (Array.isArray(execution)) return execution.filter(Boolean);
  if (typeof execution === 'string') {
    const t = execution.trim();
    return t ? [t] : [];
  }
  if (typeof execution === 'object' && execution.stageId) return [execution];
  return [];
}

module.exports = {
  MISSION_PLAN_VERSION,
  PLAN_CATEGORIES,
  RESERVED_RUNTIME_FIELDS,
  PARAMETER_SCHEMAS,
  EXECUTION_ALIASES,
  buildMissionPlan,
  resolveExecutionRequest,
  validateMissionPlan,
  summarizeMissionPlan,
  executableObjectiveText,
  containsOperatorInstructionLeak,
  normalizeNotes,
  normalizeExecution,
};
