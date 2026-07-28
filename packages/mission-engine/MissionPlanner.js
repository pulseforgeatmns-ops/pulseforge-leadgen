'use strict';

/**
 * MissionPlanner — objective → execution graph (SPEC-041 / ADR-027).
 * Selects Discovery Profiles for Prospect Discovery (SPEC-024).
 * Discovers capabilities from the registry. Never imports agent modules.
 * Never executes capabilities — only plans.
 */

const { BUILTIN_IDS } = require('../capabilities');
const {
  createProfileSelector,
  createDiscoveryProfileStore,
} = require('../capabilities/discovery');
const {
  createPlaybookSelector,
  createClientPlaybookStore,
} = require('../capabilities/playbook');
const {
  MISSION_TYPES,
  MISSION_STATUS,
  STAGE_LABELS,
  AUDIT_KINDS,
  newId,
} = require('./types');
const { matchMissionType } = require('./IntentRouter');
const { PLANNER_VERSION, getStage } = require('./StageLibrary');
const {
  createExecutionGraph,
  replanGraph,
  validateGraph,
  explainPlan,
  insertStage,
  removeStage,
  replaceStage,
} = require('./ExecutionGraph');

/**
 * @deprecated SPEC-041 — retained as read-only seed mirror for tests/compat.
 * Prefer createExecutionGraph / StageLibrary.TYPE_SEED_STAGES.
 */
const TYPE_CAPABILITY_CHAINS = Object.freeze({
  [MISSION_TYPES.CAMPAIGN_CREATION]: [
    BUILTIN_IDS.PROSPECT_DISCOVERY,
    BUILTIN_IDS.COMPANY_ENRICHMENT,
    BUILTIN_IDS.KNOWLEDGE_UPDATE,
    BUILTIN_IDS.OPPORTUNITY_RANKING,
    BUILTIN_IDS.SALES_INTELLIGENCE,
    BUILTIN_IDS.CAMPAIGN_BUILDER,
  ],
  [MISSION_TYPES.PROSPECT_DISCOVERY]: [
    BUILTIN_IDS.PROSPECT_DISCOVERY,
    BUILTIN_IDS.COMPANY_ENRICHMENT,
    BUILTIN_IDS.KNOWLEDGE_UPDATE,
    BUILTIN_IDS.OPPORTUNITY_RANKING,
    BUILTIN_IDS.SALES_INTELLIGENCE,
  ],
  [MISSION_TYPES.OVERFLOW_PARTNER_SEARCH]: [
    BUILTIN_IDS.PROSPECT_DISCOVERY,
    BUILTIN_IDS.COMPANY_ENRICHMENT,
    BUILTIN_IDS.KNOWLEDGE_UPDATE,
    BUILTIN_IDS.OPPORTUNITY_RANKING,
    BUILTIN_IDS.SALES_INTELLIGENCE,
  ],
  [MISSION_TYPES.ACQUISITION_SEARCH]: [
    BUILTIN_IDS.PROSPECT_DISCOVERY,
    BUILTIN_IDS.COMPANY_ENRICHMENT,
    BUILTIN_IDS.KNOWLEDGE_UPDATE,
    BUILTIN_IDS.OPPORTUNITY_RANKING,
    BUILTIN_IDS.SALES_INTELLIGENCE,
  ],
  [MISSION_TYPES.COMPETITOR_RESEARCH]: [
    BUILTIN_IDS.PROSPECT_DISCOVERY,
    BUILTIN_IDS.KNOWLEDGE_UPDATE,
    BUILTIN_IDS.OPPORTUNITY_RANKING,
  ],
  [MISSION_TYPES.MARKET_RESEARCH]: [
    BUILTIN_IDS.PROSPECT_DISCOVERY,
    BUILTIN_IDS.KNOWLEDGE_UPDATE,
    BUILTIN_IDS.OPPORTUNITY_RANKING,
  ],
  [MISSION_TYPES.WEEKLY_BRIEF]: [BUILTIN_IDS.KNOWLEDGE_UPDATE],
  [MISSION_TYPES.KNOWLEDGE_REFRESH]: [BUILTIN_IDS.KNOWLEDGE_UPDATE],
  [MISSION_TYPES.PROPOSAL_GENERATION]: [BUILTIN_IDS.PROPOSAL_GENERATOR],
  [MISSION_TYPES.MAIL_PACKAGE_GENERATION]: [BUILTIN_IDS.MAIL_PACKAGE_GENERATOR],
  [MISSION_TYPES.CAMPAIGN_REVIEW]: [BUILTIN_IDS.CAMPAIGN_REVIEW],
  [MISSION_TYPES.DIRECT_MAIL_EXECUTION]: [BUILTIN_IDS.DIRECT_MAIL_EXECUTION],
  [MISSION_TYPES.OUTCOME_INTELLIGENCE]: [BUILTIN_IDS.OUTCOME_INTELLIGENCE],
  [MISSION_TYPES.OPERATOR_INBOX]: [BUILTIN_IDS.OPERATOR_INBOX],
});

class MissionPlanner {
  /**
   * @param {object} deps
   * @param {import('@pulseforge/capabilities').CapabilityRegistry} deps.registry
   * @param {object} [deps.profileSelector]
   * @param {object} [deps.profileStore]
   * @param {object} [deps.playbookSelector]
   * @param {object} [deps.playbookStore]
   */
  constructor(deps) {
    if (!deps || !deps.registry) {
      throw new Error('MissionPlanner requires registry');
    }
    this._registry = deps.registry;
    this._profileStore =
      deps.profileStore || createDiscoveryProfileStore();
    this._profileSelector =
      deps.profileSelector ||
      createProfileSelector({ store: this._profileStore });
    this._playbookStore =
      deps.playbookStore || createClientPlaybookStore();
    this._playbookSelector =
      deps.playbookSelector ||
      createPlaybookSelector({ store: this._playbookStore });
  }

  get registry() {
    return this._registry;
  }

  get profileSelector() {
    return this._profileSelector;
  }

  get playbookSelector() {
    return this._playbookSelector;
  }

  /**
   * SPEC-041 API — build execution graph from mission-shaped input.
   * @param {object} mission
   */
  createExecutionGraph(mission) {
    return createExecutionGraph(mission);
  }

  /**
   * SPEC-041 API — incremental replan after operator modifications.
   * @param {object} mission
   * @param {object} [mods]
   */
  replanGraph(mission, mods) {
    return replanGraph(mission, mods);
  }

  /** @param {object} graph */
  validateGraph(graph) {
    return validateGraph(graph);
  }

  /** @param {object} graph */
  explainPlan(graph) {
    return explainPlan(graph);
  }

  /** @param {object} graph @param {string} stageId @param {object} [opts] */
  insertStage(graph, stageId, opts) {
    return insertStage(graph, stageId, opts);
  }

  /** @param {object} graph @param {string} stageId */
  removeStage(graph, stageId) {
    return removeStage(graph, stageId);
  }

  /** @param {object} graph @param {string} fromId @param {string} toId */
  replaceStage(graph, fromId, toId) {
    return replaceStage(graph, fromId, toId);
  }

  /**
   * @param {object} input
   * @param {string} input.objective
   * @param {string} [input.missionType]
   * @param {string|number} input.tenantId
   * @param {string|number} [input.clientId]
   * @param {object} [input.constraints]
   * @param {string} [input.createdBy]
   * @param {string} [input.priority]
   * @returns {object} mission draft (not yet persisted)
   */
  plan(input) {
    if (!input || !String(input.objective || '').trim()) {
      throw new Error('objective is required');
    }
    const objectiveText = String(input.objective).trim();
    const missionType =
      input.missionType ||
      matchMissionType(objectiveText.toLowerCase(), objectiveText) ||
      MISSION_TYPES.CAMPAIGN_CREATION;

    const graph = createExecutionGraph({
      objective: objectiveText,
      missionType,
      constraints: input.constraints,
      extraStages: input.extraStages,
      removeStages: input.removeStages,
    });

    if (!graph.validation.ok) {
      const err = new Error(
        `Mission plan validation failed: ${graph.validation.errors.join('; ')}`
      );
      err.code = 'MISSION_GRAPH_INVALID';
      err.validation = graph.validation;
      err.graph = graph;
      throw err;
    }

    const capabilityIds = graph.executableStages.map((s) => s.capabilityId);
    const baseConstraints =
      input.constraints && typeof input.constraints === 'object'
        ? { ...input.constraints }
        : { targetCount: 50 };

    if (graph.produceReadyToPrint) {
      baseConstraints.produceReadyToPrint = true;
    }

    // SPEC-024 / SPEC-040: bind Discovery Profile via deterministic resolver
    let profileSelection = null;
    if (capabilityIds.includes(BUILTIN_IDS.PROSPECT_DISCOVERY)) {
      profileSelection = this._profileSelector.select({
        objective: objectiveText,
        clientId: input.clientId != null ? input.clientId : input.tenantId,
        tenantId: input.tenantId,
        constraints: baseConstraints,
        missionType,
      });
      if (profileSelection.blocked || !profileSelection.profile) {
        return {
          id: input.id || newId('msn'),
          tenantId: String(input.tenantId),
          clientId:
            input.clientId != null
              ? Number(input.clientId) || input.clientId
              : input.tenantId,
          type: missionType,
          status: MISSION_STATUS.WAITING,
          objectiveText,
          title: deriveTitle(objectiveText, missionType),
          constraints: baseConstraints,
          discoveryProfile: null,
          discoveryProfileResolution: profileSelection.resolution || {
            blocked: true,
            reason: 'No Discovery Profile',
            blockingIssues: profileSelection.blockingIssues || [
              'No Discovery Profile',
            ],
          },
          plan: {
            steps: [],
            missingPrerequisites: ['discovery_profile'],
            blocked: true,
            blockingIssues: profileSelection.blockingIssues || [
              'No Discovery Profile',
            ],
            executionGraph: graph,
            explanation: explainPlan(graph),
            plannerVersion: PLANNER_VERSION,
          },
          progress: {
            completedSteps: 0,
            totalSteps: 0,
            currentStage: 'Blocked — No Discovery Profile',
            currentCapabilityId: null,
            percent: 0,
            counts: null,
            stageOutcome: 'blocked',
          },
          confidence: 0,
          createdBy: input.createdBy || 'operator',
          priority: input.priority || 'normal',
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          blockingIssues: profileSelection.blockingIssues || [
            'No Discovery Profile',
          ],
        };
      }
      baseConstraints.discoveryProfile = profileSelection.profile;
      baseConstraints.discoveryProfileId = profileSelection.profile.id;
      baseConstraints.discoveryProfileVersion = profileSelection.profile.version;
      baseConstraints.discoveryProfileBound = true;
      if (!baseConstraints.targetCount && profileSelection.profile.targetCount) {
        baseConstraints.targetCount = profileSelection.profile.targetCount;
      }
    }

    // SPEC-028 / ADR-015: pin Client Playbook for campaign + proposal missions
    let playbookSelection = null;
    const needsPlaybook =
      capabilityIds.includes(BUILTIN_IDS.CAMPAIGN_BUILDER) ||
      capabilityIds.includes(BUILTIN_IDS.PROPOSAL_GENERATOR) ||
      capabilityIds.includes(BUILTIN_IDS.MAIL_PACKAGE_GENERATOR) ||
      capabilityIds.includes(BUILTIN_IDS.CAMPAIGN_REVIEW) ||
      capabilityIds.includes(BUILTIN_IDS.DIRECT_MAIL_EXECUTION) ||
      capabilityIds.includes(BUILTIN_IDS.OUTCOME_INTELLIGENCE);
    if (needsPlaybook) {
      playbookSelection = this._playbookSelector.select({
        objective: objectiveText,
        clientId: input.clientId != null ? input.clientId : input.tenantId,
        tenantId: input.tenantId,
        constraints: baseConstraints,
      });
      if (playbookSelection.playbook) {
        baseConstraints.clientPlaybook = playbookSelection.playbook;
        baseConstraints.clientPlaybookId = playbookSelection.playbook.id;
        baseConstraints.clientPlaybookVersion =
          playbookSelection.playbook.version;
      }
    }

    // SPEC-027B: seed Discovery Summary from objective when generating a proposal
    if (
      capabilityIds.includes(BUILTIN_IDS.PROPOSAL_GENERATOR) &&
      !baseConstraints.discoverySummary &&
      !(input.inputs && input.inputs.discoverySummary)
    ) {
      const forMatch = /(?:proposal|quote|deck)\s+for\s+(.+)$/i.exec(
        objectiveText
      );
      if (forMatch) {
        baseConstraints.discoverySummary = {
          companyName: forMatch[1].replace(/[."]+$/, '').trim(),
        };
      }
    }

    const missing = [];
    const steps = [];
    let totalDuration = 0;
    let confidenceSum = 0;

    for (let i = 0; i < graph.executableStages.length; i += 1) {
      const exec = graph.executableStages[i];
      const capabilityId = exec.capabilityId;
      const cap = this._registry.get(capabilityId);
      if (!cap) {
        missing.push(capabilityId);
        continue;
      }
      const estimate = cap.estimate({
        missionId: '',
        tenantId: String(input.tenantId),
        clientId: input.clientId != null ? input.clientId : input.tenantId,
        objective: objectiveText,
        constraints: baseConstraints,
        inputs: {},
        knowledge: {},
      });
      totalDuration += estimate.durationMs || 0;
      confidenceSum += estimate.confidence || 0;
      const stageDef = getStage(exec.stageId);
      steps.push({
        index: i,
        stageId: exec.stageId,
        capabilityId,
        name: cap.name,
        stageLabel:
          (stageDef && stageDef.name) ||
          STAGE_LABELS[capabilityId] ||
          cap.name,
        status: 'queued',
        estimate,
        reason: exec.reason,
        reviewRequired: Boolean(stageDef && stageDef.reviewRequired),
        dependencies: (graph.nodes.find((n) => n.id === exec.stageId) || {})
          .dependencies || [],
      });
    }

    const title = deriveTitle(objectiveText, missionType);
    const now = new Date().toISOString();
    const explanation = explainPlan(graph);

    return {
      id: input.id || newId('msn'),
      tenantId: String(input.tenantId),
      clientId:
        input.clientId != null
          ? Number(input.clientId) || input.clientId
          : input.tenantId,
      type: missionType,
      status: MISSION_STATUS.PLANNING,
      objectiveText,
      title,
      constraints: baseConstraints,
      discoveryProfile: profileSelection
        ? {
            id: profileSelection.profile.id,
            name: profileSelection.profile.name,
            version: profileSelection.profile.version,
            selection: profileSelection.selection,
            message: profileSelection.message,
            alternatives: (profileSelection.alternatives || []).map((p) => ({
              id: p.id,
              name: p.name,
              version: p.version,
            })),
            reason:
              (profileSelection.resolution &&
                profileSelection.resolution.reason) ||
              null,
            geography:
              (profileSelection.resolution &&
                profileSelection.resolution.geography) ||
              profileSelection.profile.geography ||
              null,
            confidence:
              profileSelection.resolution &&
              profileSelection.resolution.confidence != null
                ? profileSelection.resolution.confidence
                : null,
            overridesApplied:
              (profileSelection.resolution &&
                profileSelection.resolution.overridesApplied) ||
              [],
          }
        : null,
      discoveryProfileResolution: profileSelection
        ? profileSelection.resolution || null
        : null,
      clientPlaybook:
        playbookSelection && playbookSelection.playbook
          ? {
              id: playbookSelection.playbook.id,
              name: playbookSelection.playbook.name,
              version: playbookSelection.playbook.version,
              selection: playbookSelection.selection,
              message: playbookSelection.message,
              alternatives: (playbookSelection.alternatives || []).map((p) => ({
                id: p.id,
                name: p.name,
                version: p.version,
              })),
            }
          : null,
      createdBy: input.createdBy || null,
      priority: input.priority || 'normal',
      plan: {
        steps,
        missingPrerequisites: missing,
        discoveryProfileMessage: profileSelection
          ? profileSelection.message
          : null,
        clientPlaybookMessage: playbookSelection
          ? playbookSelection.message
          : null,
        executionGraph: graph,
        explanation,
        plannerVersion: PLANNER_VERSION,
        selectedStages: graph.selectedStages,
        skippedStages: graph.skippedStages,
        reviewGates: graph.reviewGates,
        reasoning: graph.reasoning,
      },
      confidence: steps.length ? confidenceSum / steps.length : 0,
      durationEstimateMs: totalDuration,
      progress: {
        completedSteps: 0,
        totalSteps: steps.length,
        currentStage: STAGE_LABELS.planning,
        currentCapabilityId: null,
        percent: 0,
        counts: null,
      },
      deliverables: null,
      review: null,
      startedAt: null,
      completedAt: null,
      createdAt: now,
      updatedAt: now,
    };
  }

  /**
   * Apply operator modification and replan affected segments.
   * Completed valid stages remain intact unless listed in mods.staleCapabilityIds
   * or mods.staleAll.
   *
   * @param {object} mission
   * @param {object} mods
   * @returns {object} updated mission fields (plan + constraints patch)
   */
  replan(mission, mods = {}) {
    const result = replanGraph(mission, mods);
    const graph = result.graph;
    if (!graph.validation.ok) {
      const err = new Error(
        `Replan validation failed: ${graph.validation.errors.join('; ')}`
      );
      err.code = 'MISSION_GRAPH_INVALID';
      err.validation = graph.validation;
      throw err;
    }

    const staleCaps = new Set(mods.staleCapabilityIds || []);
    const staleAll = Boolean(mods.staleAll);
    const staleStages = new Set(mods.staleStageIds || []);

    // Drop preserved status for explicitly stale capabilities / stages
    const preservedStageIds = result.preservedStageIds.filter((stageId) => {
      if (staleAll) return false;
      if (staleStages.has(stageId)) return false;
      const node = graph.nodes.find((n) => n.id === stageId);
      if (node && node.capabilityId && staleCaps.has(node.capabilityId)) {
        return false;
      }
      return true;
    });

    // Cascade: once a non-preserved stage appears in order, later ones invalidate
    const ordered = graph.orderedStageIds || [];
    const finalPreserved = [];
    let cascade = false;
    for (const stageId of ordered) {
      if (!preservedStageIds.includes(stageId)) {
        cascade = true;
      }
      if (!cascade && preservedStageIds.includes(stageId)) {
        finalPreserved.push(stageId);
      }
    }

    const priorSteps = (mission.plan && mission.plan.steps) || [];
    const priorByKey = new Map();
    for (const s of priorSteps) {
      if (s.stageId) priorByKey.set(s.stageId, s);
      if (s.capabilityId) priorByKey.set(s.capabilityId, s);
    }

    const steps = [];
    let totalDuration = 0;
    let confidenceSum = 0;
    let i = 0;
    for (const exec of graph.executableStages) {
      const prior =
        priorByKey.get(exec.stageId) || priorByKey.get(exec.capabilityId);
      const preserved = finalPreserved.includes(exec.stageId);
      if (preserved && prior && prior.status === 'completed') {
        steps.push({
          ...prior,
          index: i,
          stageId: exec.stageId,
          status: 'completed',
        });
        i += 1;
        continue;
      }
      const cap = this._registry.get(exec.capabilityId);
      if (!cap) continue;
      const estimate = cap.estimate({
        missionId: mission.id || '',
        tenantId: String(mission.tenantId),
        clientId: mission.clientId != null ? mission.clientId : mission.tenantId,
        objective: mission.objectiveText || '',
        constraints: {
          ...(mission.constraints || {}),
          ...(mods.constraints || {}),
        },
        inputs: {},
        knowledge: {},
      });
      totalDuration += estimate.durationMs || 0;
      confidenceSum += estimate.confidence || 0;
      const stageDef = getStage(exec.stageId);
      steps.push({
        index: i,
        stageId: exec.stageId,
        capabilityId: exec.capabilityId,
        name: cap.name,
        stageLabel:
          (stageDef && stageDef.name) ||
          STAGE_LABELS[exec.capabilityId] ||
          cap.name,
        status: 'queued',
        estimate,
        reason: exec.reason,
        reviewRequired: Boolean(stageDef && stageDef.reviewRequired),
      });
      i += 1;
    }

    const invalidatedStageIds = (graph.orderedStageIds || []).filter(
      (id) => !finalPreserved.includes(id)
    );

    return {
      constraints: {
        ...(mission.constraints || {}),
        ...(mods.constraints || {}),
        ...(graph.produceReadyToPrint ? { produceReadyToPrint: true } : {}),
      },
      plan: {
        ...(mission.plan || {}),
        steps,
        executionGraph: graph,
        explanation: explainPlan(graph),
        plannerVersion: PLANNER_VERSION,
        selectedStages: graph.selectedStages,
        skippedStages: graph.skippedStages,
        reviewGates: graph.reviewGates,
        reasoning: graph.reasoning,
        replan: {
          preservedStageIds: finalPreserved,
          invalidatedStageIds,
          staleCapabilityIds: [...staleCaps],
          staleAll,
          at: new Date().toISOString(),
        },
      },
      durationEstimateMs: totalDuration,
      confidence: steps.length ? confidenceSum / steps.length : 0,
      auditKind: AUDIT_KINDS.PLAN,
    };
  }
}

/**
 * @param {string} objective
 * @param {string} type
 */
function deriveTitle(objective, type) {
  if (type === MISSION_TYPES.OPERATOR_INBOX) {
    return 'Operator Inbox';
  }
  if (type === MISSION_TYPES.OUTCOME_INTELLIGENCE) {
    const campaign = campaignLabel(objective);
    if (campaign) return `Outcome Intelligence — Campaign ${campaign}`;
    return 'Outcome Intelligence';
  }
  if (type === MISSION_TYPES.DIRECT_MAIL_EXECUTION) {
    const campaign = campaignLabel(objective);
    if (campaign) return `Direct Mail Execution — Campaign ${campaign}`;
    return 'Direct Mail Execution';
  }
  if (type === MISSION_TYPES.CAMPAIGN_REVIEW) {
    const campaign = campaignLabel(objective);
    if (campaign) return `Campaign Review — Campaign ${campaign}`;
    return 'Campaign Review';
  }
  if (type === MISSION_TYPES.MAIL_PACKAGE_GENERATION) {
    const campaign = campaignLabel(objective);
    if (campaign) return `Mail Packages — Campaign ${campaign}`;
    return 'Mail Package Generation';
  }
  const campaign = campaignLabel(objective);
  if (campaign) return `Campaign ${campaign}`;
  if (type === MISSION_TYPES.OVERFLOW_PARTNER_SEARCH) return 'Overflow Partner Search';
  if (type === MISSION_TYPES.ACQUISITION_SEARCH) return 'Acquisition Search';
  if (type === MISSION_TYPES.PROSPECT_DISCOVERY) return 'Prospect Discovery';
  if (type === MISSION_TYPES.COMPETITOR_RESEARCH) return 'Competitor Research';
  if (type === MISSION_TYPES.MARKET_RESEARCH) return 'Market Research';
  if (type === MISSION_TYPES.WEEKLY_BRIEF) return 'Weekly Brief';
  if (type === MISSION_TYPES.KNOWLEDGE_REFRESH) return 'Knowledge Refresh';
  if (type === MISSION_TYPES.PROPOSAL_GENERATION) {
    const forMatch = /(?:proposal|quote|deck)\s+for\s+(.+)$/i.exec(objective);
    if (forMatch) return `Proposal — ${forMatch[1].replace(/[."]+$/, '').trim()}`;
    return 'Commercial Growth Proposal';
  }
  const trimmed = objective.length > 60 ? `${objective.slice(0, 57)}…` : objective;
  return trimmed;
}

/**
 * Prefer numeric campaign ids ("Campaign 001") over incidental words
 * ("campaign outcomes").
 * @param {string} objective
 * @returns {string|null}
 */
function campaignLabel(objective) {
  const numeric = /campaign\s+(\d+)/i.exec(objective);
  if (numeric) return numeric[1];
  const named = /campaign\s+([a-z][\w-]*)/i.exec(objective);
  if (!named) return null;
  const token = named[1].toLowerCase();
  if (
    /^(outcomes?|recommendations?|review|execution|mail|packages?|builder|creation)$/.test(
      token
    )
  ) {
    return null;
  }
  return named[1];
}

function createMissionPlanner(deps) {
  return new MissionPlanner(deps);
}

module.exports = {
  MissionPlanner,
  createMissionPlanner,
  TYPE_CAPABILITY_CHAINS,
  deriveTitle,
  campaignLabel,
  PLANNER_VERSION,
  createExecutionGraph,
  replanGraph,
  validateGraph,
  explainPlan,
  insertStage,
  removeStage,
  replaceStage,
};
