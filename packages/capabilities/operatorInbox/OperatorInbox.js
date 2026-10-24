'use strict';

/**
 * Operator Inbox capability (SPEC-037 / ADR-024).
 * Coordinates human work — does not perform business workflows.
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
  INBOX_PROGRESS_STAGES,
  OPERATOR_ACTIONS,
  ACTIVE_STATUSES,
} = require('./types');
const { assembleOperatorInbox } = require('./assemble');
const { normalizeActions, applyInboxActions } = require('./actions');
const { validateCoordinationOnly } = require('./validate');
const {
  createInMemoryOperatorInboxStore,
} = require('./OperatorInboxStore');

/**
 * @param {object} [deps]
 */
function createOperatorInboxCapability(deps = {}) {
  const store =
    deps.operatorInboxStore || createInMemoryOperatorInboxStore();

  return {
    id: BUILTIN_IDS.OPERATOR_INBOX,
    name: 'Operator Inbox',
    description:
      'Single operational inbox that coordinates human work across capabilities — never performs workflows',
    category: CAPABILITY_CATEGORIES.MONITORING,
    outcomeTags: [
      'inbox_assembled',
      'work_prioritized',
      'work_completed',
      'inbox_audit',
    ],
    retryable: true,
    timeoutMs: 15_000,
    supportsRollback: false,
    idempotent: false,
    inputSchema: {
      required: [],
      properties: {
        capabilityEvents: 'CapabilityEvent[]?',
        workItems: 'InboxItem[]?',
        validationResults: 'ValidationResult[]?',
        execution: 'DirectMailExecution?',
        recommendations: 'Recommendation[]?',
        reviewSummary: 'CampaignReviewSummary?',
        inboxActions: 'InboxAction[]?',
        operator: 'string?',
      },
    },
    outputSchema: {
      items: 'InboxItem[]',
      activeItems: 'InboxItem[]',
      auditLog: 'AuditEntry[]',
      completionEvents: 'CompletionEvent[]',
      missionEvents: 'MissionInboxEvent[]',
      timeline: 'MissionTimelineEntry[]',
      summary: 'InboxSummary',
    },

    canRun() {
      return true;
    },

    estimate(context) {
      const inputs = (context && context.inputs) || {};
      const n =
        (Array.isArray(inputs.workItems) && inputs.workItems.length) ||
        (Array.isArray(inputs.capabilityEvents) &&
          inputs.capabilityEvents.length) ||
        1;
      return buildCapabilityEstimate({
        durationMs: 200 + n * 10,
        confidence: 0.9,
        notes: ['Operator Inbox coordination (no workflow processing)'],
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

      const inputs = { ...(context.inputs || {}) };
      const prior = inputs.priorOutputs || {};
      if (!inputs.execution && prior.execution) inputs.execution = prior.execution;
      if (!inputs.recommendations && prior.recommendations) {
        inputs.recommendations = prior.recommendations;
      }
      if (!inputs.outcomeSummary && prior.outcomeSummary) {
        inputs.outcomeSummary = prior.outcomeSummary;
      }
      if (!inputs.queue && prior.queue) inputs.queue = prior.queue;
      if (!inputs.reviewSummary && prior.summary) {
        inputs.reviewSummary = prior.summary;
      }

      const gate = validateCoordinationOnly(inputs);
      if (!gate.ok) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          outputs: { coordinationOnly: true },
          errors: gate.errors.map((code) => ({
            code,
            message:
              'Operator Inbox coordinates work and must not perform workflows (ADR-024)',
          })),
          warnings: [
            'Operator Inbox is a coordination layer — open the deep-linked workspace to process work',
          ],
          duration: Date.now() - started,
        });
      }

      emit(INBOX_PROGRESS_STAGES.INGESTING, 20);

      const operator = inputs.operator || context.createdBy || 'operator';
      const scopeKey =
        context.clientId != null
          ? String(context.clientId)
          : context.missionId || 'global';

      const existing = store.getLatest(scopeKey);
      const actions = normalizeActions(inputs);
      const actionOnly =
        actions.length > 0 &&
        actions.every((a) => {
          const t = String(a.type || a.action || '');
          return t && t !== 'ingest';
        }) &&
        !inputs.forceIngest;

      let pkg;
      if (actionOnly && existing && existing.package) {
        pkg = existing.package;
      } else {
        emit(INBOX_PROGRESS_STAGES.DEDUPING, 40);
        emit(INBOX_PROGRESS_STAGES.PRIORITIZING, 55);
        pkg = assembleOperatorInbox(
          { ...context, inputs },
          {
            operator,
            existingItems:
              existing && existing.package
                ? existing.package.items
                : existing
                  ? existing.items
                  : [],
            auditLog:
              existing && existing.package
                ? existing.package.auditLog
                : existing
                  ? existing.auditLog
                  : [],
            completionEvents:
              existing && existing.package
                ? existing.package.completionEvents
                : existing
                  ? existing.completionEvents
                  : [],
          }
        );
      }

      emit(INBOX_PROGRESS_STAGES.APPLYING, 75);

      const applied = applyInboxActions(pkg, actions, { operator });
      pkg = applied.package;

      // Ensure active list reflects post-action state
      pkg.activeItems = (pkg.items || []).filter((i) =>
        ACTIVE_STATUSES.has(i.status)
      );

      const saved = await Promise.resolve(
        store.create({
          clientId: context.clientId != null ? context.clientId : null,
          missionId: context.missionId || null,
          tenantId: context.tenantId || '',
          package: pkg,
          items: pkg.items,
          activeItems: pkg.activeItems,
          auditLog: pkg.auditLog,
          completionEvents: pkg.completionEvents,
          missionEvents: pkg.missionEvents,
          timeline: pkg.timeline,
          summary: pkg.summary,
          changeSummary: applied.changeSummary,
          operator,
        })
      );

      emit(INBOX_PROGRESS_STAGES.COMPLETED, 100);

      return buildCapabilityResult({
        status:
          applied.errors.length > 0
            ? CAPABILITY_RESULT_STATUS.PARTIAL
            : CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          items: pkg.items,
          activeItems: pkg.activeItems,
          auditLog: pkg.auditLog,
          completionEvents: pkg.completionEvents,
          missionEvents: pkg.missionEvents,
          timeline: pkg.timeline,
          summary: pkg.summary,
          inboxSnapshotId: saved.id,
          inboxSnapshot: saved,
          changeSummary: applied.changeSummary,
          actionErrors: applied.errors,
          operatorActions: [...OPERATOR_ACTIONS],
          coordinationOnly: true,
        },
        evidence: [
          {
            kind: 'operator_inbox',
            summary: `Operator Inbox: ${pkg.summary.activeCount} active (${pkg.summary.criticalCount} critical)`,
            activeCount: pkg.summary.activeCount,
            criticalCount: pkg.summary.criticalCount,
          },
        ],
        artifacts: [
          {
            type: 'operator_inbox',
            id: saved.id,
            activeCount: pkg.summary.activeCount,
          },
        ],
        warnings: applied.errors.length
          ? applied.errors.slice(0, 10)
          : [
              'Operator Inbox coordinates work only — deep-link to originating workspace to process (ADR-024)',
            ],
        errors: applied.errors.map((code) => ({
          code,
          message: String(code),
        })),
        nextRecommendations: nextRecs(pkg),
        duration: Date.now() - started,
      });
    },
  };
}

/**
 * @param {object} pkg
 * @returns {object[]}
 */
function nextRecs(pkg) {
  const active = pkg.activeItems || [];
  if (!active.length) {
    return [
      {
        action: 'idle',
        summary: 'No outstanding inbox items',
      },
    ];
  }
  const top = active[0];
  return [
    {
      action: 'review',
      itemId: top.id,
      summary: `Review ${top.priority} item: ${top.title}`,
      deepLink: top.deepLink,
    },
  ];
}

module.exports = {
  createOperatorInboxCapability,
};
