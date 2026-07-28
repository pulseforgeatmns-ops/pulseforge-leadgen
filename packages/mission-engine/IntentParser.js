'use strict';

/**
 * Intent Parser — natural language → Mission Plan IR (SPEC-050 / ADR-034).
 * Classifies every sentence into exactly one category.
 * Unknown capability requests become Notes — never new runtime nodes.
 */

const {
  PLAN_CATEGORIES,
  RESERVED_RUNTIME_FIELDS,
  buildMissionPlan,
  resolveExecutionRequest,
  validateMissionPlan,
} = require('./MissionPlan');

/**
 * Parse operator natural language into a deterministic Mission Plan.
 * @param {string} text
 * @param {object} [opts]
 * @returns {object} mission_plan
 */
function parseIntent(text, opts = {}) {
  const sourceText = String(text || '').trim();
  const units = splitUnits(sourceText);
  /** @type {{ text: string, category: string, detail?: object }[]} */
  const classifications = [];

  let objective = '';
  let subject = null;
  /** @type {Record<string, unknown>} */
  const parameters = {};
  /** @type {object[]} */
  const execution = [];
  /** @type {string[]} */
  const notes = [];
  /** @type {Record<string, boolean>} */
  const options = {
    review: false,
    approvalRequired: false,
    dryRun: false,
    shadowMode: false,
    readyToPrint: false,
  };

  for (const unit of units) {
    const classified = classifyUnit(unit);
    classifications.push(classified);

    switch (classified.category) {
      case PLAN_CATEGORIES.OBJECTIVE: {
        if (!objective) objective = classified.detail.objective || unit;
        if (classified.detail.subject && !subject) {
          subject = classified.detail.subject;
        }
        if (classified.detail.campaign) {
          parameters.campaign = classified.detail.campaign;
        }
        // Objective sentences often embed parameters
        Object.assign(parameters, classified.detail.parameters || {});
        if (classified.detail.executionHint) {
          pushExecution(execution, classified.detail.executionHint, notes);
        }
        break;
      }
      case PLAN_CATEGORIES.PARAMETERS: {
        Object.assign(parameters, classified.detail.parameters || {});
        if (classified.detail.subject && !subject) {
          subject = classified.detail.subject;
        }
        break;
      }
      case PLAN_CATEGORIES.EXECUTION: {
        pushExecution(execution, classified.detail, notes);
        break;
      }
      case PLAN_CATEGORIES.OPTIONS: {
        Object.assign(options, classified.detail.options || {});
        if (classified.detail.requestCampaignReview) {
          pushExecution(
            execution,
            {
              stageId: 'campaign_review',
              capabilityId: 'campaign_review',
              label: 'Campaign Review',
            },
            notes
          );
        }
        if (classified.detail.requestReadyToPrint) {
          options.readyToPrint = true;
          pushExecution(
            execution,
            {
              stageId: 'ready_to_print',
              capabilityId: null,
              label: 'Ready To Print',
            },
            notes
          );
        }
        break;
      }
      case PLAN_CATEGORIES.NOTES:
      default: {
        notes.push(classified.detail.note || unit);
        break;
      }
    }
  }

  // Default objective from first unit if none classified
  if (!objective && units.length) {
    const first = units[0];
    objective = first.replace(/\s+/g, ' ').trim();
    classifications.unshift({
      text: first,
      category: PLAN_CATEGORIES.OBJECTIVE,
      detail: { objective, inferred: true },
    });
  }

  // Campaign creation implies campaign_builder when the objective is a build
  if (
    /\bbuild\s+campaign\b/i.test(objective) ||
    /\bcreate\s+campaign\b/i.test(objective) ||
    /\blaunch\s+campaign\b/i.test(objective)
  ) {
    const hasBuilder = execution.some(
      (e) => e.stageId === 'campaign_builder' || e.capabilityId === 'campaign_builder'
    );
    if (!hasBuilder) {
      pushExecution(
        execution,
        {
          stageId: 'campaign_builder',
          capabilityId: 'campaign_builder',
          label: 'Campaign Builder',
        },
        notes
      );
    }
  }

  // Pipeline-through phrases add BI + SI without treating guidance as nodes
  const lowerSource = sourceText.toLowerCase();
  if (
    /through\s+sales\s+intelligence/.test(lowerSource) ||
    /through\s+business\s+intelligence/.test(lowerSource) ||
    /complete\s+pipeline/.test(lowerSource)
  ) {
    const hasBi = execution.some(
      (e) =>
        e.stageId === 'business_intelligence' ||
        e.capabilityId === 'business_intelligence'
    );
    if (!hasBi) {
      pushExecution(
        execution,
        {
          stageId: 'business_intelligence',
          capabilityId: 'business_intelligence',
          label: 'Business Intelligence',
        },
        notes
      );
    }
    const hasSi = execution.some(
      (e) => e.stageId === 'sales_intelligence' || e.capabilityId === 'sales_intelligence'
    );
    if (!hasSi) {
      pushExecution(
        execution,
        {
          stageId: 'sales_intelligence',
          capabilityId: 'sales_intelligence',
          label: 'Sales Intelligence',
        },
        notes
      );
    }
  }

  // Campaign missions always require operator approval of the Mission Plan
  // before execution (ADR-034). This is not the Campaign Review stage.
  if (
    execution.some((e) => e.stageId === 'campaign_builder') ||
    /\bbuild\s+campaign\b/i.test(objective)
  ) {
    options.approvalRequired = true;
    options.review = true;
  }

  // Review option without a dedicated campaign-review execution request
  if (options.review && !options.approvalRequired) {
    options.approvalRequired = true;
  }

  // Strip reserved keys if any slipped into parameters
  for (const key of RESERVED_RUNTIME_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(parameters, key)) {
      notes.push(`Ignored reserved field from operator text: ${key}`);
      delete parameters[key];
    }
  }

  const plan = buildMissionPlan({
    objective,
    subject: subject || parameters.client || parameters.subject || null,
    parameters,
    execution,
    options,
    notes: dedupeNotes(notes),
    classifications,
    sourceText,
    createdAt: opts.now || new Date().toISOString(),
  });

  if (opts.validate !== false) {
    const validation = validateMissionPlan(plan, opts);
    if (!validation.ok && opts.failClosed) {
      const err = new Error(
        `Mission Plan validation failed: ${validation.errors.join('; ')}`
      );
      err.code = 'MISSION_PLAN_INVALID';
      err.validation = validation;
      err.missionPlan = plan;
      throw err;
    }
    return Object.freeze({
      ...plan,
      validation,
    });
  }

  return plan;
}

/**
 * Classify a single sentence / fragment into exactly one category.
 * @param {string} unit
 * @returns {{ text: string, category: string, detail: object }}
 */
function classifyUnit(unit) {
  const text = String(unit || '').trim();
  const lower = text.toLowerCase();

  // --- Options (before Notes so "Review." alone is an option) ---
  const matchedOptions = matchOptions(lower, text);
  if (matchedOptions) {
    return {
      text,
      category: PLAN_CATEGORIES.OPTIONS,
      detail: matchedOptions,
    };
  }

  // --- Notes: operator guidance that must never execute ---
  if (isOperatorGuidance(lower)) {
    return {
      text,
      category: PLAN_CATEGORIES.NOTES,
      detail: { note: ensurePeriod(text) },
    };
  }

  // --- Objective ---
  const objective = matchObjective(lower, text);
  if (objective) {
    return { text, category: PLAN_CATEGORIES.OBJECTIVE, detail: objective };
  }

  // --- Parameters-only fragments ---
  const params = extractParameters(text);
  if (params.hit && !looksLikeExecution(lower)) {
    return {
      text,
      category: PLAN_CATEGORIES.PARAMETERS,
      detail: params,
    };
  }

  // --- Execution / capability requests ---
  if (looksLikeExecution(lower) || resolveExecutionRequest(text).known) {
    const resolved = resolveExecutionFromUnit(text, lower);
    if (resolved.known) {
      return {
        text,
        category: PLAN_CATEGORIES.EXECUTION,
        detail: resolved,
      };
    }
    // Unknown capability → Notes (never a new node)
    return {
      text,
      category: PLAN_CATEGORIES.NOTES,
      detail: {
        note: resolved.note || `Unknown capability. ${ensurePeriod(text)}`,
      },
    };
  }

  // Parameter-bearing leftovers
  if (params.hit) {
    return { text, category: PLAN_CATEGORIES.PARAMETERS, detail: params };
  }

  // Default: Notes
  return {
    text,
    category: PLAN_CATEGORIES.NOTES,
    detail: { note: ensurePeriod(text) },
  };
}

function matchObjective(lower, text) {
  const buildVerb =
    /\b(build|create|launch|prepare|new)\s+(a\s+)?(q\d\s+)?(outreach\s+)?campaign\b/.test(
      lower
    );
  // "Campaign 001" alone is a parameter, not an objective — require a build verb
  // so "Generate mail packages for Campaign 001" stays mail-focused.
  const buildCampaign = buildVerb;

  const discover =
    /\b(find|discover)\s+.+\b(prospects?|leads|companies)\b/.test(lower) ||
    /\bprospect\s+discovery\b/.test(lower);

  const proposal =
    /\b(generate|create|draft|write|build)\s+(a\s+)?(sales\s+)?proposal\b/.test(
      lower
    );

  if (!buildCampaign && !discover && !proposal) return null;

  const campaign = extractCampaign(text);
  const subject = extractSubject(text);
  const parameters = extractParameters(text).parameters;

  let objective = text;
  if (buildCampaign && campaign) {
    objective = `Build Campaign ${campaign}`;
  } else if (discover) {
    objective = 'Discover Prospects';
  } else if (proposal) {
    objective = text.replace(/\s+/g, ' ').trim();
  }

  /** @type {object|null} */
  let executionHint = null;
  if (buildCampaign) {
    executionHint = {
      stageId: 'campaign_builder',
      capabilityId: 'campaign_builder',
      label: 'Campaign Builder',
    };
  } else if (discover) {
    executionHint = {
      stageId: 'prospect_discovery',
      capabilityId: 'prospect_discovery',
      label: 'Discovery',
    };
  } else if (proposal) {
    executionHint = {
      stageId: 'proposal_generator',
      capabilityId: 'proposal_generator',
      label: 'Proposal Generator',
    };
  }

  return {
    objective,
    subject,
    campaign,
    parameters,
    executionHint,
  };
}

function matchOptions(lower, text) {
  // Bare review / pause-at-review / require approval — not "Review Human Test…"
  const bareReview =
    /^(review|review\.|pause at review|pause at review\.)$/.test(lower);
  const requireApproval =
    /^require(s)?\s+approval\.?$/.test(lower) ||
    /^approval\s+required\.?$/.test(lower);

  const dryRun = /\bdry\s*run\b/.test(lower) && lower.length < 40;
  const shadow = /\bshadow\s*mode\b/.test(lower) && lower.length < 40;
  const ready =
    /^(ready\s+to\s+print|print[- ]ready)\.?$/.test(lower) ||
    (/ready\s+to\s+print/.test(lower) &&
      lower.length < 48 &&
      !/\bhuman\s+test\b/.test(lower));

  const pauseAtReview =
    /\bpause\s+at\s+review\b/.test(lower) &&
    !/\bhuman\s+test\b/.test(lower) &&
    !/\bgenerated\s+letters?\b/.test(lower) &&
    !/\bconfidence\b/.test(lower) &&
    !/\binspect\b/.test(lower);

  if (
    !bareReview &&
    !requireApproval &&
    !dryRun &&
    !shadow &&
    !ready &&
    !pauseAtReview
  ) {
    return null;
  }

  /** @type {Record<string, boolean>} */
  const options = {};
  let requestCampaignReview = false;
  let requestReadyToPrint = false;

  if (bareReview || pauseAtReview) {
    options.review = true;
    options.approvalRequired = true;
    // Explicit Review. / Pause at review → Campaign Review stage (SPEC-041)
    requestCampaignReview = true;
  }
  if (requireApproval) {
    options.approvalRequired = true;
    options.review = true;
  }
  if (dryRun) options.dryRun = true;
  if (shadow) options.shadowMode = true;
  if (ready) {
    options.readyToPrint = true;
    requestReadyToPrint = true;
    // Ready To Print implies a review gate
    options.review = true;
    options.approvalRequired = true;
    requestCampaignReview = true;
  }
  return { options, requestCampaignReview, requestReadyToPrint };
}

function isOperatorGuidance(lower) {
  if (/\breview\s+human\s+test\b/.test(lower)) return true;
  if (/\bhuman\s+test\s+results?\b/.test(lower)) return true;
  if (/\bgenerated\s+letters?\b/.test(lower)) return true;
  if (/\binspect\s+(the\s+)?(messaging|letters?|packages?|results?)\b/.test(lower))
    return true;
  if (/\bcompare\s+confidence\b/.test(lower)) return true;
  if (/\bconfidence\s+scores?\b/.test(lower)) return true;
  if (/\breview\s+the\s+human\s+test\b/.test(lower)) return true;
  if (/\bcheck\s+(the\s+)?(letters?|messaging|scores?)\b/.test(lower)) return true;
  // "Review X results" where X is not "campaign"
  if (
    /\breview\b/.test(lower) &&
    !/\breview\s+(the\s+)?campaign\b/.test(lower) &&
    !/\bcampaign\s+review\b/.test(lower) &&
    !/^(review|review\.)$/.test(lower) &&
    (/\bresults?\b/.test(lower) ||
      /\bletters?\b/.test(lower) ||
      /\bmessaging\b/.test(lower) ||
      /\bscores?\b/.test(lower))
  ) {
    return true;
  }
  return false;
}

function looksLikeExecution(lower) {
  return (
    /\b(execute|generate|run|produce|prepare|build|create|discover|analyze|rank)\b/.test(
      lower
    ) ||
    /\b(mail\s+packages?|sales\s+intelligence|campaign\s+builder|ready\s+to\s+print|direct\s+mail)\b/.test(
      lower
    ) ||
    /\bcomplete\s+pipeline\b/.test(lower)
  );
}

function resolveExecutionFromUnit(text, lower) {
  // "Execute the complete pipeline through Sales Intelligence" → SI (+ builder via objective)
  if (/\bcomplete\s+pipeline\b/.test(lower) || /\bthrough\s+sales\s+intelligence\b/.test(lower)) {
    return {
      known: true,
      stageId: 'sales_intelligence',
      capabilityId: 'sales_intelligence',
      label: 'Sales Intelligence',
      pipelineThrough: 'sales_intelligence',
    };
  }
  if (/\bready\s+to\s+print\b/.test(lower) || /\bprint[- ]ready\b/.test(lower)) {
    return {
      known: true,
      stageId: 'ready_to_print',
      capabilityId: null,
      label: 'Ready To Print',
    };
  }
  if (
    /\b(generate|create|build|prepare|print)\s+(a\s+)?(mail|direct\s*mail)\s+packages?\b/.test(
      lower
    ) ||
    /\bmail\s+packages?\b/.test(lower)
  ) {
    return {
      known: true,
      stageId: 'mail_package_generator',
      capabilityId: 'mail_package_generator',
      label: 'Mail Package',
    };
  }
  if (
    /\bcampaign\s+review\b/.test(lower) ||
    /\breview\s+(the\s+)?campaign\b/.test(lower) ||
    /\bapprove\s+(the\s+)?campaign\b/.test(lower)
  ) {
    return {
      known: true,
      stageId: 'campaign_review',
      capabilityId: 'campaign_review',
      label: 'Campaign Review',
    };
  }

  const resolved = resolveExecutionRequest(text);
  if (resolved.known) {
    return {
      known: true,
      stageId: resolved.stageId,
      capabilityId: resolved.capabilityId,
      label: resolved.stageId,
    };
  }
  return {
    known: false,
    note: `Unknown capability. ${ensurePeriod(text)}`,
  };
}

function extractParameters(text) {
  /** @type {Record<string, unknown>} */
  const parameters = {};
  let hit = false;
  let subject = null;

  const forMatch =
    /\bfor\s+([A-Z][\w]*(?:\s+[A-Z][\w]*){0,4})(?:\s+using\b|\s+with\b|[.,]|$)/.exec(
      text
    ) ||
    /\bfor\s+([A-Za-z][\w]*(?:\s+[A-Za-z][\w]*){0,3})\s+using\b/i.exec(text);
  if (forMatch) {
    const name = forMatch[1].trim();
    if (!/^(the|a|an|campaign|review|mail)$/i.test(name)) {
      subject = name;
      parameters.client = name;
      hit = true;
    }
  }

  if (/\bcurrent\s+prospect\s*lists?\b/i.test(text)) {
    parameters.prospectList = 'current';
    hit = true;
  } else if (/\b(using|with)\s+(the\s+)?(attached|operator)\s+prospect\s*lists?\b/i.test(text)) {
    parameters.prospectList = 'attached';
    hit = true;
  } else if (/\bprospect\s*lists?\b/i.test(text) && /\bcurrent\b/i.test(text)) {
    parameters.prospectList = 'current';
    hit = true;
  }

  const campaign = extractCampaign(text);
  if (campaign) {
    parameters.campaign = campaign;
    hit = true;
  }

  const market =
    /\bin\s+(?:the\s+)?([A-Za-z][\w\s]+?)\s+market\b/i.exec(text) ||
    /\bmarket:\s*([^.,;]+)/i.exec(text);
  if (market) {
    parameters.market = market[1].trim();
    hit = true;
  }

  return { hit, parameters, subject };
}

function extractCampaign(text) {
  const m = /campaign\s+(\d+)/i.exec(String(text || ''));
  return m ? m[1] : null;
}

function extractSubject(text) {
  const params = extractParameters(text);
  return params.subject || params.parameters.client || null;
}

function splitUnits(text) {
  if (!text) return [];
  // Prefer newline boundaries, then sentence terminators
  const rough = text
    .split(/\n+/)
    .flatMap((line) =>
      String(line)
        .split(/(?<=[.!?])\s+(?=[A-Z0-9])/)
        .flatMap((chunk) => chunk.split(/\s*;\s*/))
    )
    .map((s) => s.trim())
    .filter(Boolean);
  return rough;
}

function pushExecution(execution, detail, notes) {
  if (!detail) return;
  if (detail.known === false) {
    if (detail.note) notes.push(detail.note);
    return;
  }
  const stageId = detail.stageId || detail.capabilityId;
  if (!stageId) {
    if (detail.note) notes.push(detail.note);
    return;
  }
  if (execution.some((e) => e.stageId === stageId)) return;
  const resolved = resolveExecutionRequest(stageId);
  if (!resolved.known && !getStageSafe(stageId)) {
    notes.push(detail.note || `Unknown capability: ${stageId}`);
    return;
  }
  execution.push({
    stageId: resolved.stageId || stageId,
    capabilityId: resolved.capabilityId != null ? resolved.capabilityId : detail.capabilityId,
    label: detail.label || stageId,
  });
}

function getStageSafe(id) {
  try {
    const { getStage } = require('./StageLibrary');
    return getStage(id);
  } catch {
    return null;
  }
}

function dedupeNotes(notes) {
  const seen = new Set();
  const out = [];
  for (const n of notes) {
    const key = String(n).trim().toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(ensurePeriod(String(n).trim()));
  }
  return out;
}

function ensurePeriod(text) {
  const t = String(text || '').trim();
  if (!t) return t;
  if (/[.!?]$/.test(t)) return t;
  return `${t}.`;
}

module.exports = {
  parseIntent,
  classifyUnit,
  splitUnits,
  isOperatorGuidance,
  extractParameters,
  PLAN_CATEGORIES,
};
