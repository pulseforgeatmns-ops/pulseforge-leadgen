'use strict';

/**
 * Proposal Generator capability (SPEC-027B / ADR-014).
 * Core Mission capability — personalization engine, not a template engine.
 */

const {
  CAPABILITY_CATEGORIES,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  CAPABILITY_RESULT_STATUS,
  PROGRESS_KINDS,
} = require('../types');
const {
  PROPOSAL_PROGRESS_STAGES,
  PROPOSAL_STATUS,
  OPERATOR_ACTIONS,
  SECTION_IDS,
  buildDiscoverySummary,
  assertPersonalized,
} = require('./types');
const { listPricingPackages } = require('./pricing');
const { resolveStrategyContext, inventoryEvidence } = require('./evidence');
const { composeProposal } = require('./personalize');
const { renderProposalHtml } = require('./render');
const { createInMemoryProposalStore } = require('./ProposalStore');
const {
  resolvePlaybookFromContext,
  proposalExcerptFromPlaybook,
} = require('../playbook');

/**
 * @param {object} context
 * @returns {object}
 */
function resolveDiscoverySummary(context) {
  const inputs = context.inputs || {};
  const constraints = context.constraints || {};
  const raw =
    inputs.discoverySummary ||
    inputs.discovery ||
    constraints.discoverySummary ||
    null;
  if (!raw || typeof raw !== 'object') {
    // Allow thin extraction from objective: "Generate proposal for AS Cleaning Co."
    const objective =
      typeof context.objective === 'string'
        ? context.objective
        : (context.objective && context.objective.text) || '';
    const m = /(?:proposal|quote|deck)\s+for\s+(.+)$/i.exec(String(objective).trim());
    if (m) {
      return buildDiscoverySummary({ companyName: m[1].replace(/[."]+$/, '').trim() });
    }
    return null;
  }
  return buildDiscoverySummary(raw);
}

/**
 * @param {object} context
 * @returns {object|null}
 */
function resolveProfile(context) {
  const inputs = context.inputs || {};
  const constraints = context.constraints || {};
  const prior = inputs.priorOutputs || {};
  return (
    inputs.discoveryProfile ||
    prior.discoveryProfile ||
    constraints.discoveryProfile ||
    null
  );
}

/**
 * @param {object} [deps]
 */
function createProposalGeneratorCapability(deps = {}) {
  const store = deps.proposalStore || createInMemoryProposalStore();

  return {
    id: BUILTIN_IDS.PROPOSAL_GENERATOR,
    name: 'Generating Proposal',
    description:
      'Transform discovery into a personalized, reviewable commercial growth proposal',
    category: CAPABILITY_CATEGORIES.CAMPAIGN,
    outcomeTags: ['proposal_generated', 'proposal_ready_for_review'],
    retryable: true,
    timeoutMs: 60_000,
    supportsRollback: false,
    idempotent: false,
    inputSchema: {
      required: ['discoverySummary'],
      properties: {
        discoverySummary: 'DiscoverySummary',
        discoveryProfile: 'DiscoveryProfile?',
        clientPlaybook: 'ClientPlaybook?',
        pricingPackageId: 'string?',
        pricingOverrides: 'object?',
        recommendedStrategy: 'object?',
      },
    },
    outputSchema: {
      proposal: 'ProposalVersion',
      document: 'ProposalDocument',
      html: 'string',
      reviewPackage: 'object',
      pricingPackages: 'object[]',
      clientPlaybook: 'ClientPlaybook?',
    },

    canRun(context) {
      const summary = resolveDiscoverySummary(context || {});
      return Boolean(summary && summary.companyName);
    },

    estimate(context) {
      const summary = resolveDiscoverySummary(context || {});
      const filled = summary
        ? Object.values(summary).filter((v) =>
            Array.isArray(v) ? v.length : v != null && v !== ''
          ).length
        : 0;
      return buildCapabilityEstimate({
        durationMs: 2500 + filled * 40,
        confidence: summary && summary.companyName ? 0.9 : 0.4,
        notes: summary
          ? ['Personalized proposal from discovery summary']
          : ['Missing discovery summary'],
      });
    },

    /**
     * @param {object} context
     * @param {{ onProgress?: Function }} [runtime]
     */
    async execute(context, runtime = {}) {
      const started = Date.now();
      const emit = (stage, pct) => {
        if (typeof runtime.onProgress === 'function') {
          runtime.onProgress({
            kind: PROGRESS_KINDS.PROGRESS,
            stage,
            percent: pct,
            message: stage,
          });
        }
      };

      emit(PROPOSAL_PROGRESS_STAGES.GATHERING, 10);

      const summary = resolveDiscoverySummary(context);
      if (!summary || !summary.companyName) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          errors: [
            {
              code: 'missing_discovery_summary',
              message:
                'Proposal Generator requires a Discovery Summary with companyName',
            },
          ],
          duration: Date.now() - started,
        });
      }

      const profile = resolveProfile(context);
      const playbook = resolvePlaybookFromContext(context);
      const playbookExcerpt = proposalExcerptFromPlaybook(playbook);
      const inputs = context.inputs || {};
      const strategy = resolveStrategyContext(inputs);
      if (profile && !strategy.profile) strategy.profile = profile;
      if (playbook && !strategy.playbook) strategy.playbook = playbook;

      const inventory = inventoryEvidence(summary, profile, strategy, playbook);
      const pricingPackageId =
        inputs.pricingPackageId ||
        (context.constraints && context.constraints.pricingPackageId) ||
        'setup_monthly';

      emit(PROPOSAL_PROGRESS_STAGES.COMPOSING, 40);
      const document = composeProposal(summary, {
        profile,
        playbook,
        strategy,
        inputs,
        pricingPackageId,
        pricingOverrides: inputs.pricingOverrides || {},
        timelineOverrides: inputs.timelineOverrides || null,
      });

      emit(PROPOSAL_PROGRESS_STAGES.PRICING, 65);
      emit(PROPOSAL_PROGRESS_STAGES.RENDERING, 80);

      const rendered = renderProposalHtml(document);
      const personalization = assertPersonalized(document, summary);

      const opportunityId =
        inputs.opportunityId ||
        (context.constraints && context.constraints.opportunityId) ||
        null;

      const saved = await Promise.resolve(
        store.create({
          opportunityId,
          missionId: context.missionId || null,
          clientId: context.clientId != null ? context.clientId : null,
          tenantId: context.tenantId || '',
          status: PROPOSAL_STATUS.REVIEW,
          discoverySummary: summary,
          discoveryProfileId: profile && (profile.id || profile.profileId) || null,
          clientPlaybookId: playbook ? playbook.id : null,
          clientPlaybookVersion: playbook ? playbook.version : null,
          pricingPackageId,
          document,
          html: rendered.html,
        })
      );

      emit(PROPOSAL_PROGRESS_STAGES.COMPLETED, 100);

      const reviewPackage = {
        summary: `Proposal v${saved.version} for ${summary.companyName} ready for operator review`,
        editableFields: [
          'pricing',
          'timeline',
          'strategy',
          'recommendations',
          'closing',
          'notes',
        ],
        operatorActions: [...OPERATOR_ACTIONS],
        sectionIds: [...SECTION_IDS],
        personalization,
        evidenceInventory: inventory,
        warnings: document.warnings || [],
      };

      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          proposal: saved,
          proposalId: saved.id,
          proposalVersion: saved.version,
          document,
          html: rendered.html,
          printableHtml: rendered.printableHtml,
          reviewPackage,
          pricingPackages: listPricingPackages(),
          clientPlaybook: playbook,
          clientPlaybookId: playbook ? playbook.id : null,
          clientPlaybookVersion: playbook ? playbook.version : null,
          playbookExcerpt,
          confidence: personalization.ok ? 0.92 : 0.7,
        },
        evidence: [
          {
            kind: 'proposal',
            summary: playbook
              ? `Generated personalized proposal for ${summary.companyName} using playbook ${playbook.name} v${playbook.version}`
              : `Generated personalized proposal for ${summary.companyName} (v${saved.version})`,
            refs: personalization.specificSignals || [],
            playbookId: playbook ? playbook.id : null,
            playbookVersion: playbook ? playbook.version : null,
          },
        ],
        artifacts: [
          {
            type: 'proposal_document',
            id: saved.id,
            version: saved.version,
            companyName: summary.companyName,
          },
          {
            type: 'proposal_html',
            id: saved.id,
            format: 'web_printable',
          },
        ],
        warnings: document.warnings || [],
        nextRecommendations: [
          {
            action: 'review',
            summary: 'Review pricing, timeline, and strategy before sending to the client',
          },
          {
            action: 'edit_pricing',
            summary: 'Confirm setup / monthly amounts on the selected package',
          },
        ],
        duration: Date.now() - started,
      });
    },
  };
}

module.exports = {
  createProposalGeneratorCapability,
  resolveDiscoverySummary,
  resolveProfile,
};
