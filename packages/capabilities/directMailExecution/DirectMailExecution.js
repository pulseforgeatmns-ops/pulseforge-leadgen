'use strict';

/**
 * Direct Mail Execution capability (SPEC-035 / ADR-022).
 * Consumes approved campaign revision + mail batch + execution package.
 * Never generates content.
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
  EXECUTION_PROGRESS_STAGES,
  OPERATOR_ACTIONS,
  EXECUTION_STATUS,
} = require('./types');
const { assembleExecution } = require('./assemble');
const { normalizeActions, applyExecutionActions } = require('./actions');
const { validateApprovedRevision, validateArtifactMutation } = require('./validate');
const {
  createInMemoryDirectMailExecutionStore,
} = require('./DirectMailExecutionStore');

/**
 * @param {object} [deps]
 */
function createDirectMailExecutionCapability(deps = {}) {
  const store =
    deps.directMailExecutionStore ||
    createInMemoryDirectMailExecutionStore();

  return {
    id: BUILTIN_IDS.DIRECT_MAIL_EXECUTION,
    name: 'Direct Mail Execution',
    description:
      'Execute approved direct mail campaigns through a deterministic state machine with full audit history',
    category: CAPABILITY_CATEGORIES.EXECUTION,
    outcomeTags: [
      'direct_mail_executed',
      'mail_printed',
      'mail_assembled',
      'mail_mailed',
      'execution_audit',
    ],
    retryable: true,
    timeoutMs: 60_000,
    supportsRollback: false,
    idempotent: false,
    inputSchema: {
      required: [],
      properties: {
        campaign: 'Campaign?',
        approvedRevision: 'number?',
        executionPackage: 'ExecutionPackage?',
        mailBatch: 'MailBatch?',
        packages: 'MailPackage[]?',
        executionActions: 'ExecutionAction[]?',
        operator: 'string?',
      },
    },
    outputSchema: {
      execution: 'DirectMailExecution',
      summary: 'ExecutionSummary',
      prospects: 'ProspectExecution[]',
      printSessions: 'PrintSession[]',
      auditLog: 'AuditEntry[]',
      metrics: 'ExecutionMetrics',
      lock: 'CampaignLock',
      missionEvents: 'MissionExecutionEvent[]',
      timeline: 'MissionTimelineEntry[]',
    },

    canRun(context) {
      const inputs = (context && context.inputs) || {};
      const prior = inputs.priorOutputs || {};
      const gate = validateApprovedRevision({
        ...inputs,
        executionPackage: inputs.executionPackage || prior.executionPackage,
        mailBatch: inputs.mailBatch || prior.mailBatch,
        packages: inputs.packages || prior.packages,
        campaignApproved:
          inputs.campaignApproved === true || prior.campaignApproved === true,
        campaignStatus:
          inputs.campaignStatus ||
          (prior.summary && prior.summary.status) ||
          (inputs.campaign && inputs.campaign.status),
        reviewSummary: prior.summary || inputs.reviewSummary,
        reviewRevision: prior.reviewRevision || inputs.reviewRevision,
      });
      // Allow canRun when artifacts present even if not yet approved —
      // execute() will fail-closed on the approval gate.
      return (
        gate.approved ||
        Boolean(inputs.executionPackage || prior.executionPackage) ||
        Boolean(inputs.mailBatch || prior.mailBatch) ||
        (Array.isArray(inputs.packages) && inputs.packages.length > 0)
      );
    },

    estimate(context) {
      const inputs = (context && context.inputs) || {};
      const packages =
        inputs.packages ||
        (inputs.mailBatch && inputs.mailBatch.packages) ||
        [];
      const n = Array.isArray(packages) ? packages.length : 1;
      return buildCapabilityEstimate({
        durationMs: 600 + n * 30,
        confidence: n ? 0.9 : 0.4,
        notes: n
          ? [`Direct mail execution for ${n} package(s)`]
          : ['No mail packages provided'],
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

      emit(EXECUTION_PROGRESS_STAGES.GATHERING, 10);

      const inputs = {
        ...(context.inputs || {}),
      };
      // Merge priorOutputs for approved revision / packages
      const prior = inputs.priorOutputs || {};
      if (!inputs.executionPackage && prior.executionPackage) {
        inputs.executionPackage = prior.executionPackage;
      }
      if (!inputs.mailBatch && prior.mailBatch) {
        inputs.mailBatch = prior.mailBatch;
      }
      if (!inputs.packages && prior.packages) {
        inputs.packages = prior.packages;
      }
      if (!inputs.queue && prior.queue) {
        inputs.queue = prior.queue;
      }
      if (inputs.campaignApproved == null && prior.campaignApproved != null) {
        inputs.campaignApproved = prior.campaignApproved;
      }
      if (!inputs.reviewRevision && prior.reviewRevision != null) {
        inputs.reviewRevision = prior.reviewRevision;
      }
      if (!inputs.approvedRevision && prior.reviewRevision != null) {
        inputs.approvedRevision = prior.reviewRevision;
      }
      if (!inputs.campaignStatus && prior.summary && prior.summary.status) {
        inputs.campaignStatus = prior.summary.status;
      }

      const operator = inputs.operator || context.createdBy || 'operator';

      emit(EXECUTION_PROGRESS_STAGES.VALIDATING, 25);

      const gate = validateApprovedRevision(inputs);
      if (!gate.ok) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          outputs: {
            outboundBlocked: true,
            approvalErrors: gate.errors,
            execution: null,
          },
          errors: gate.errors.map((code) => ({
            code,
            message: humanGateError(code),
          })),
          warnings: [
            'Direct Mail Execution requires an approved campaign revision (ADR-021 / ADR-022)',
          ],
          duration: Date.now() - started,
        });
      }

      const campaignKey =
        inputs.campaignId ||
        (inputs.campaign && (inputs.campaign.id || inputs.campaign.name)) ||
        context.missionId ||
        null;

      const existing = campaignKey ? store.getLatest(campaignKey) : null;

      let execution;
      if (existing && existing.execution) {
        execution = existing.execution;
        // Ensure revision stays pinned
        if (
          execution.lock &&
          execution.lock.locked &&
          gate.revision != null &&
          execution.summary.revision !== gate.revision
        ) {
          const mutation = validateArtifactMutation(execution, {
            replaceRevision: true,
          });
          if (!mutation.ok) {
            return buildCapabilityResult({
              status: CAPABILITY_RESULT_STATUS.FAILED,
              outputs: {
                outboundBlocked: true,
                lockErrors: mutation.errors,
                execution,
                lock: execution.lock,
              },
              errors: mutation.errors.map((code) => ({
                code,
                message: humanGateError(code),
              })),
              warnings: [
                'Campaign artifacts are locked — create a new approved revision to change content (ADR-022)',
              ],
              duration: Date.now() - started,
            });
          }
        }
      } else {
        execution = assembleExecution(
          { ...context, inputs },
          {
            revision: gate.revision,
            status: EXECUTION_STATUS.DRAFT,
            campaignName:
              (inputs.campaign && inputs.campaign.name) ||
              inputs.campaignName,
          }
        );
      }

      emit(EXECUTION_PROGRESS_STAGES.APPLYING, 55);

      const actions = normalizeActions(inputs);
      // Default: start_execution if no actions and still draft
      const effectiveActions =
        actions.length > 0
          ? actions
          : execution.summary.status === EXECUTION_STATUS.DRAFT
            ? [{ type: 'start_execution', operator }]
            : [];

      const applied = applyExecutionActions(execution, effectiveActions, {
        operator,
      });
      execution = applied.execution;

      emit(EXECUTION_PROGRESS_STAGES.METRICS, 80);

      const saved = await Promise.resolve(
        store.create({
          campaignId: campaignKey,
          missionId: context.missionId || null,
          clientId: context.clientId != null ? context.clientId : null,
          tenantId: context.tenantId || '',
          status: execution.summary.status,
          campaignName: execution.summary.campaignName,
          revision: execution.summary.revision,
          execution,
          summary: execution.summary,
          prospects: execution.prospects,
          auditLog: execution.auditLog,
          printSessions: execution.printSessions,
          missionEvents: execution.missionEvents,
          timeline: execution.timeline,
          lock: execution.lock,
          pinnedArtifacts: execution.pinnedArtifacts,
          changeSummary: applied.changeSummary,
          operator,
        })
      );

      emit(EXECUTION_PROGRESS_STAGES.COMPLETED, 100);

      const artifacts = [
        {
          type: 'direct_mail_execution',
          id: saved.id,
          status: execution.summary.status,
          revision: execution.summary.revision,
          locked: Boolean(execution.lock && execution.lock.locked),
        },
      ];
      if (execution.printSessions && execution.printSessions.length) {
        artifacts.push({
          type: 'print_session',
          id: execution.printSessions[execution.printSessions.length - 1].id,
          revision: execution.summary.revision,
        });
      }

      return buildCapabilityResult({
        status:
          applied.errors.length > 0
            ? CAPABILITY_RESULT_STATUS.PARTIAL
            : CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          execution,
          summary: execution.summary,
          prospects: execution.prospects,
          printSessions: execution.printSessions,
          auditLog: execution.auditLog,
          metrics: execution.summary.metrics,
          lock: execution.lock,
          pinnedArtifacts: execution.pinnedArtifacts,
          missionEvents: execution.missionEvents,
          timeline: execution.timeline,
          executionSnapshotId: saved.id,
          executionSnapshot: saved.snapshot,
          changeSummary: applied.changeSummary,
          actionErrors: applied.errors,
          operatorActions: [...OPERATOR_ACTIONS],
          campaignStatus: execution.summary.status,
          outboundBlocked: false,
        },
        evidence: [
          {
            kind: 'direct_mail_execution',
            summary: `${execution.summary.campaignName}: ${execution.summary.status} — mailed ${execution.summary.metrics.mailed}/${execution.summary.prospectCount}`,
            status: execution.summary.status,
            revision: execution.summary.revision,
            locked: Boolean(execution.lock && execution.lock.locked),
            auditEntries: (execution.auditLog || []).length,
          },
        ],
        artifacts,
        warnings: applied.errors.length
          ? applied.errors.slice(0, 10)
          : execution.lock && execution.lock.locked
            ? [
                'Campaign artifacts locked after Printing — changes require a new approved revision (ADR-022)',
              ]
            : [],
        errors: applied.errors.map((code) => ({
          code,
          message: String(code),
        })),
        nextRecommendations: nextRecs(execution),
        duration: Date.now() - started,
      });
    },
  };
}

/**
 * @param {string} code
 * @returns {string}
 */
function humanGateError(code) {
  const map = {
    approved_revision_required:
      'An approved campaign revision is required before Direct Mail Execution',
    approved_revision_missing: 'Approved revision number is missing',
    execution_artifacts_missing:
      'Execution package or mail package batch is required',
    campaign_revision_locked:
      'Campaign revision is locked — create a new revision to change content',
    mail_package_batch_pinned: 'Mail package batch is pinned for this run',
    execution_package_pinned: 'Execution package is pinned for this run',
    execution_must_not_generate_content:
      'Execution must not generate content (ADR-022)',
  };
  return map[code] || code;
}

/**
 * @param {object} execution
 * @returns {object[]}
 */
function nextRecs(execution) {
  const status = execution.summary.status;
  if (status === EXECUTION_STATUS.COMPLETED) {
    return [
      {
        action: 'outcome_intelligence',
        summary:
          'Direct mail campaign completed — capture outcomes and generate learnings (SPEC-036)',
      },
    ];
  }
  if (status === EXECUTION_STATUS.DRAFT || status === EXECUTION_STATUS.READY_TO_PRINT) {
    return [
      {
        action: 'start_print_session',
        summary: 'Start a print session to lock artifacts and begin printing',
      },
    ];
  }
  if (status === EXECUTION_STATUS.PRINTING) {
    return [
      {
        action: 'complete_print_session',
        summary: 'Complete the print session when pages are printed',
      },
    ];
  }
  if (
    status === EXECUTION_STATUS.PRINTED ||
    status === EXECUTION_STATUS.ASSEMBLING
  ) {
    return [
      {
        action: 'assembly_complete',
        summary: 'Mark prospect packages assembled, then Ready to Mail',
      },
    ];
  }
  if (status === EXECUTION_STATUS.READY_TO_MAIL) {
    return [
      {
        action: 'mark_all_mailed',
        summary: 'Mark packages mailed (capture optional USPS batch ID)',
      },
    ];
  }
  if (
    status === EXECUTION_STATUS.MAILED ||
    status === EXECUTION_STATUS.DELIVERED ||
    status === EXECUTION_STATUS.RESPONDED
  ) {
    return [
      {
        action: 'set_response',
        summary: 'Record prospect responses as they arrive',
      },
    ];
  }
  return [];
}

module.exports = {
  createDirectMailExecutionCapability,
};
