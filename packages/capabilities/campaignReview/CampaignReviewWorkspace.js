'use strict';

/**
 * Campaign Review Workspace capability (SPEC-034 / ADR-021).
 * Final human checkpoint before a campaign becomes executable.
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
  REVIEW_PROGRESS_STAGES,
  OPERATOR_ACTIONS,
  CAMPAIGN_REVIEW_STATUS,
  buildRevisionRecord,
  buildMissionDecision,
  buildMissionRevision,
} = require('./types');
const { assembleWorkspace, getProspectDetail } = require('./assemble');
const {
  normalizeActions,
  applyReviewActions,
  buildExecutionPackage,
  validateCampaignApproval,
} = require('./actions');
const { createInMemoryCampaignReviewStore } = require('./CampaignReviewStore');
const {
  resolvePlaybookFromContext,
} = require('../playbook');

/**
 * @param {object} [deps]
 */
function createCampaignReviewCapability(deps = {}) {
  const store = deps.campaignReviewStore || createInMemoryCampaignReviewStore();

  return {
    id: BUILTIN_IDS.CAMPAIGN_REVIEW,
    name: 'Campaign Review',
    description:
      'Review, edit, approve, and reject campaign artifacts before execution',
    category: CAPABILITY_CATEGORIES.CAMPAIGN,
    outcomeTags: [
      'campaign_review_ready',
      'campaign_ready_to_print',
      'human_approval_required',
    ],
    retryable: true,
    timeoutMs: 60_000,
    supportsRollback: false,
    idempotent: false,
    inputSchema: {
      required: [],
      properties: {
        campaign: 'Campaign?',
        mailBatch: 'MailBatch?',
        packages: 'MailPackage[]?',
        prospects: 'Prospect[]?',
        clientPlaybook: 'ClientPlaybook?',
        companyIntelligencePackages: 'object?',
        reviewActions: 'ReviewAction[]?',
        confidenceThreshold: 'number?',
        sort: 'string?',
        selectedProspectId: 'string?',
      },
    },
    outputSchema: {
      workspace: 'CampaignReviewWorkspace',
      summary: 'CampaignReviewSummary',
      queue: 'ProspectQueueRow[]',
      prospectDetail: 'object?',
      revisionHistory: 'RevisionRecord[]',
      executionPackage: 'ExecutionPackage?',
      missionDecisions: 'MissionDecision[]',
      missionRevisions: 'MissionRevision[]',
      reviewPackage: 'object',
    },

    canRun(context) {
      const inputs = (context && context.inputs) || {};
      const prior = inputs.priorOutputs || {};
      const campaign = inputs.campaign || prior.campaign;
      const packages =
        inputs.packages ||
        (inputs.mailBatch && inputs.mailBatch.packages) ||
        prior.packages ||
        [];
      const prospects =
        inputs.prospects ||
        (campaign && campaign.prospects) ||
        prior.prospects ||
        [];
      return (
        Boolean(campaign) ||
        (Array.isArray(packages) && packages.length > 0) ||
        (Array.isArray(prospects) && prospects.length > 0)
      );
    },

    estimate(context) {
      const inputs = (context && context.inputs) || {};
      const prospects =
        (inputs.campaign && inputs.campaign.prospects) ||
        inputs.prospects ||
        inputs.packages ||
        [];
      const n = Array.isArray(prospects) ? prospects.length : 1;
      return buildCapabilityEstimate({
        durationMs: 800 + n * 40,
        confidence: n ? 0.9 : 0.4,
        notes: n
          ? [`Campaign review workspace for ${n} prospect(s)`]
          : ['No campaign prospects provided'],
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

      emit(REVIEW_PROGRESS_STAGES.GATHERING, 10);

      const inputs = context.inputs || {};
      const operator = inputs.operator || context.createdBy || 'operator';
      const playbook = resolvePlaybookFromContext(context);

      // Ensure playbook is visible to assemble via constraints
      const enrichedContext = {
        ...context,
        constraints: {
          ...(context.constraints || {}),
          clientPlaybook:
            (context.constraints && context.constraints.clientPlaybook) ||
            playbook,
        },
      };

      const campaignKey =
        inputs.campaignId ||
        (inputs.campaign && (inputs.campaign.id || inputs.campaign.name)) ||
        context.missionId ||
        null;

      const existing = campaignKey ? store.getLatest(campaignKey) : null;
      const baseRevision = existing ? existing.revision : 0;

      emit(REVIEW_PROGRESS_STAGES.ASSEMBLING, 30);

      let workspace = assembleWorkspace(enrichedContext, {
        revision: baseRevision + 1,
        activeRevision: baseRevision + 1,
        sort: inputs.sort,
        selectedProspectId: inputs.selectedProspectId,
        campaignName:
          (inputs.campaign && inputs.campaign.name) ||
          inputs.campaignName ||
          undefined,
      });

      // Restore / duplicate / compare before applying other actions
      const actions = normalizeActions(inputs);
      const special = actions.filter((a) =>
        ['restore_revision', 'duplicate_revision', 'compare_revisions'].includes(
          String(a.type || a.action || '')
        )
      );
      const normal = actions.filter(
        (a) =>
          !['restore_revision', 'duplicate_revision', 'compare_revisions'].includes(
            String(a.type || a.action || '')
          )
      );

      let compareResult = null;
      let restoredFrom = null;

      for (const action of special) {
        const type = String(action.type || action.action || '');
        if (type === 'restore_revision' && campaignKey) {
          const restored = store.restore(campaignKey, action.revision, {
            operator,
            changeSummary: `Restored from revision ${action.revision}`,
          });
          if (restored && restored.workspace) {
            workspace = {
              ...restored.workspace,
              summary: {
                ...restored.workspace.summary,
                revision: restored.revision,
                activeRevision: restored.revision,
              },
            };
            restoredFrom = action.revision;
          }
        } else if (type === 'duplicate_revision' && campaignKey) {
          const dup = store.duplicate(campaignKey, action.revision, {
            operator,
          });
          if (dup && dup.workspace) {
            workspace = {
              ...dup.workspace,
              summary: {
                ...dup.workspace.summary,
                revision: dup.revision,
                activeRevision: dup.revision,
              },
            };
          }
        } else if (type === 'compare_revisions' && campaignKey) {
          compareResult = store.compare(
            campaignKey,
            action.revisionA,
            action.revisionB
          );
        }
      }

      emit(REVIEW_PROGRESS_STAGES.APPLYING, 55);

      const applied = applyReviewActions(workspace, normal, {
        operator,
        sort: inputs.sort,
      });
      workspace = applied.workspace;

      if (workspace.selectedProspectId) {
        workspace.prospectDetail = getProspectDetail(
          workspace,
          workspace.selectedProspectId
        );
      }

      emit(REVIEW_PROGRESS_STAGES.VALIDATING, 75);

      const executionPackage = buildExecutionPackage(workspace);
      if (executionPackage) {
        workspace.summary.status = CAMPAIGN_REVIEW_STATUS.READY_TO_PRINT;
      }

      // If approve_campaign was attempted and failed, surface errors
      const campaignGate = validateCampaignApproval(workspace);
      const approvalBlocked =
        normal.some(
          (a) => String(a.type || a.action) === 'approve_campaign'
        ) && !campaignGate.ok;

      const changeSummary =
        applied.changeSummary +
        (restoredFrom != null ? `; restored_from:${restoredFrom}` : '');

      const saved = await Promise.resolve(
        store.create({
          campaignId: campaignKey,
          missionId: context.missionId || null,
          clientId: context.clientId != null ? context.clientId : null,
          tenantId: context.tenantId || '',
          status: workspace.summary.status,
          campaignName: workspace.summary.campaignName,
          workspace,
          summary: {
            ...workspace.summary,
            revision: undefined, // stamped from store
          },
          queue: workspace.queue,
          decisions: applied.decisions,
          missionRevisions: applied.revisions,
          executionPackage,
          changeSummary,
          operator,
        })
      );

      // Stamp active revision onto workspace summary
      workspace.summary.revision = saved.revision;
      workspace.summary.activeRevision = saved.revision;

      const revisionHistory = store.listForCampaign(campaignKey).map((r) =>
        buildRevisionRecord({
          revision: r.revision,
          timestamp: r.createdAt,
          operator: r.operator,
          changeSummary: r.changeSummary,
          actions: (r.decisions || []).map((d) => d.action),
        })
      );

      // Ensure campaign approval decision is present when Ready to Print
      const missionDecisions = applied.decisions.slice();
      if (
        executionPackage &&
        !missionDecisions.some((d) => d.action === 'approve_campaign')
      ) {
        missionDecisions.push(
          buildMissionDecision({
            action: 'approve_campaign',
            operator,
            summary: 'Campaign Ready to Print',
            revision: saved.revision,
          })
        );
      }

      const missionRevisions = applied.revisions.map((r) =>
        buildMissionRevision({
          ...r,
          revision: saved.revision,
        })
      );
      if (
        normal.some((a) =>
          ['regenerate', 'regenerate_selected', 'edit_letter'].includes(
            String(a.type || a.action)
          )
        ) &&
        !missionRevisions.length
      ) {
        missionRevisions.push(
          buildMissionRevision({
            revision: saved.revision,
            reason: 'campaign_review_change',
            operator,
            changeSummary,
          })
        );
      }

      emit(REVIEW_PROGRESS_STAGES.COMPLETED, 100);

      const reviewPackage = {
        summary: `${workspace.summary.campaignName}: ${workspace.summary.readyCount} approved, ${workspace.summary.needsReviewCount} need review, ${workspace.summary.blockedCount} blocked`,
        operatorActions: [...OPERATOR_ACTIONS],
        campaignSummary: workspace.summary,
        confidenceThreshold: workspace.confidenceThreshold,
        status: workspace.summary.status,
        approvalBlocked,
        campaignApprovalErrors: approvalBlocked ? campaignGate.errors : [],
        warnings: workspace.queue
          .flatMap((r) => r.validationErrors || [])
          .slice(0, 20),
      };

      const artifacts = [
        {
          type: 'campaign_review_workspace',
          id: saved.id,
          revision: saved.revision,
          status: workspace.summary.status,
        },
      ];
      if (executionPackage) {
        artifacts.push(
          {
            type: 'execution_package',
            id: saved.id,
            revision: saved.revision,
            status: CAMPAIGN_REVIEW_STATUS.READY_TO_PRINT,
          },
          {
            type: 'print_package',
            id: saved.id,
            format: 'printable_html',
          },
          {
            type: 'mail_merge_csv',
            id: saved.id,
          },
          {
            type: 'address_label_csv',
            id: saved.id,
          }
        );
      }
      for (const exp of applied.exportArtifacts) {
        artifacts.push({
          type: exp.type,
          prospectIds: exp.prospectIds,
        });
      }

      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          workspace,
          summary: workspace.summary,
          queue: workspace.queue,
          prospectDetail: workspace.prospectDetail || null,
          reviewRevisionId: saved.id,
          reviewRevision: saved.revision,
          revisionHistory,
          compareResult,
          executionPackage,
          missionDecisions,
          missionRevisions,
          reviewPackage,
          campaignApproved:
            workspace.summary.status === CAMPAIGN_REVIEW_STATUS.READY_TO_PRINT,
          outboundBlocked:
            workspace.summary.status !== CAMPAIGN_REVIEW_STATUS.READY_TO_PRINT,
          clientPlaybook: playbook,
          clientPlaybookId: playbook ? playbook.id : null,
          clientPlaybookVersion: playbook ? playbook.version : null,
          exportArtifacts: applied.exportArtifacts,
        },
        evidence: [
          {
            kind: 'campaign_review',
            summary: reviewPackage.summary,
            revision: saved.revision,
            status: workspace.summary.status,
            decisions: missionDecisions.length,
            readyToPrint:
              workspace.summary.status ===
              CAMPAIGN_REVIEW_STATUS.READY_TO_PRINT,
          },
        ],
        artifacts,
        warnings: approvalBlocked
          ? [
              `Campaign approval blocked: ${campaignGate.errors.join(', ')}`,
              ...reviewPackage.warnings.slice(0, 5),
            ]
          : workspace.summary.status !== CAMPAIGN_REVIEW_STATUS.READY_TO_PRINT
            ? [
                'Campaign not Ready to Print — operator approval required (ADR-021)',
              ]
            : [],
        nextRecommendations: executionPackage
          ? [
              {
                action: 'execute',
                summary:
                  'Campaign Ready to Print — Execution may consume this approved revision only',
              },
            ]
          : [
              {
                action: 'review',
                summary:
                  'Approve required prospects, clear validation errors, then approve campaign',
              },
            ],
        duration: Date.now() - started,
      });
    },
  };
}

module.exports = {
  createCampaignReviewCapability,
};
