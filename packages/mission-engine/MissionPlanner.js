'use strict';

/**
 * MissionPlanner — objective → mission type + capability plan (SPEC-022).
 * Selects Discovery Profiles for Prospect Discovery (SPEC-024).
 * Discovers capabilities from the registry. Never imports agent modules.
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
  newId,
} = require('./types');
const { matchMissionType } = require('./IntentRouter');

/** @type {Record<string, string[]>} */
const TYPE_CAPABILITY_CHAINS = Object.freeze({
  [MISSION_TYPES.CAMPAIGN_CREATION]: [
    BUILTIN_IDS.PROSPECT_DISCOVERY,
    BUILTIN_IDS.COMPANY_ENRICHMENT,
    BUILTIN_IDS.KNOWLEDGE_UPDATE,
    BUILTIN_IDS.OPPORTUNITY_RANKING,
    BUILTIN_IDS.CAMPAIGN_BUILDER,
  ],
  [MISSION_TYPES.PROSPECT_DISCOVERY]: [
    BUILTIN_IDS.PROSPECT_DISCOVERY,
    BUILTIN_IDS.COMPANY_ENRICHMENT,
    BUILTIN_IDS.KNOWLEDGE_UPDATE,
    BUILTIN_IDS.OPPORTUNITY_RANKING,
  ],
  [MISSION_TYPES.OVERFLOW_PARTNER_SEARCH]: [
    BUILTIN_IDS.PROSPECT_DISCOVERY,
    BUILTIN_IDS.COMPANY_ENRICHMENT,
    BUILTIN_IDS.KNOWLEDGE_UPDATE,
    BUILTIN_IDS.OPPORTUNITY_RANKING,
  ],
  [MISSION_TYPES.ACQUISITION_SEARCH]: [
    BUILTIN_IDS.PROSPECT_DISCOVERY,
    BUILTIN_IDS.COMPANY_ENRICHMENT,
    BUILTIN_IDS.KNOWLEDGE_UPDATE,
    BUILTIN_IDS.OPPORTUNITY_RANKING,
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

    const chain = TYPE_CAPABILITY_CHAINS[missionType];
    if (!chain) {
      throw new Error(`Unsupported mission type: ${missionType}`);
    }

    const baseConstraints =
      input.constraints && typeof input.constraints === 'object'
        ? { ...input.constraints }
        : { targetCount: 50 };

    // SPEC-024: bind an immutable Discovery Profile snapshot into constraints
    let profileSelection = null;
    if (chain.includes(BUILTIN_IDS.PROSPECT_DISCOVERY)) {
      profileSelection = this._profileSelector.select({
        objective: objectiveText,
        clientId: input.clientId != null ? input.clientId : input.tenantId,
        tenantId: input.tenantId,
        constraints: baseConstraints,
      });
      baseConstraints.discoveryProfile = profileSelection.profile;
      baseConstraints.discoveryProfileId = profileSelection.profile.id;
      baseConstraints.discoveryProfileVersion = profileSelection.profile.version;
      if (!baseConstraints.targetCount && profileSelection.profile.targetCount) {
        baseConstraints.targetCount = profileSelection.profile.targetCount;
      }
    }

    // SPEC-028 / ADR-015: pin Client Playbook for campaign + proposal missions
    let playbookSelection = null;
    const needsPlaybook =
      chain.includes(BUILTIN_IDS.CAMPAIGN_BUILDER) ||
      chain.includes(BUILTIN_IDS.PROPOSAL_GENERATOR) ||
      chain.includes(BUILTIN_IDS.MAIL_PACKAGE_GENERATOR) ||
      chain.includes(BUILTIN_IDS.CAMPAIGN_REVIEW);
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
      chain.includes(BUILTIN_IDS.PROPOSAL_GENERATOR) &&
      !baseConstraints.discoverySummary &&
      !(input.inputs && input.inputs.discoverySummary)
    ) {
      const forMatch = /(?:proposal|quote|deck)\s+for\s+(.+)$/i.exec(objectiveText);
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

    for (let i = 0; i < chain.length; i += 1) {
      const capabilityId = chain[i];
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
      steps.push({
        index: i,
        capabilityId,
        name: cap.name,
        stageLabel: STAGE_LABELS[capabilityId] || cap.name,
        status: 'queued',
        estimate,
      });
    }

    const title = deriveTitle(objectiveText, missionType);
    const now = new Date().toISOString();

    return {
      id: input.id || newId('msn'),
      tenantId: String(input.tenantId),
      clientId:
        input.clientId != null ? Number(input.clientId) || input.clientId : input.tenantId,
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
          }
        : null,
      clientPlaybook: playbookSelection && playbookSelection.playbook
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
}

/**
 * @param {string} objective
 * @param {string} type
 */
function deriveTitle(objective, type) {
  if (type === MISSION_TYPES.CAMPAIGN_REVIEW) {
    const campaign = /campaign\s+(\d+|[\w-]+)/i.exec(objective);
    if (campaign) return `Campaign Review — Campaign ${campaign[1]}`;
    return 'Campaign Review';
  }
  if (type === MISSION_TYPES.MAIL_PACKAGE_GENERATION) {
    const campaign = /campaign\s+(\d+|[\w-]+)/i.exec(objective);
    if (campaign) return `Mail Packages — Campaign ${campaign[1]}`;
    return 'Mail Package Generation';
  }
  const campaign = /campaign\s+(\d+|[\w-]+)/i.exec(objective);
  if (campaign) return `Campaign ${campaign[1]}`;
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

function createMissionPlanner(deps) {
  return new MissionPlanner(deps);
}

module.exports = {
  MissionPlanner,
  createMissionPlanner,
  TYPE_CAPABILITY_CHAINS,
  deriveTitle,
};
