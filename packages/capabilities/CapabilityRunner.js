'use strict';

const {
  PROGRESS_KINDS,
  CAPABILITY_RESULT_STATUS,
  buildCapabilityResult,
  buildCapabilityContext,
} = require('./types');
const { formatMissingCapabilityError } = require('./CapabilityRegistry');
const {
  CAPABILITY_EXECUTION_MODES,
  resolveCapabilityExecutionMode,
  normalizeDiagnoseCanRun,
  buildPreconditionBlockedResult,
} = require('./executionMode');

/**
 * CapabilityRunner — execute only through the registry (SPEC-023 / SPEC-054 / SPEC-058).
 * No agent-specific branching.
 */
class CapabilityRunner {
  /**
   * @param {object} deps
   * @param {import('./CapabilityRegistry').CapabilityRegistry} deps.registry
   * @param {(event: object) => void} [deps.onProgress]
   */
  constructor(deps) {
    if (!deps || !deps.registry) {
      throw new Error('CapabilityRunner requires registry');
    }
    this._registry = deps.registry;
    this._onProgress = typeof deps.onProgress === 'function' ? deps.onProgress : null;
  }

  get registry() {
    return this._registry;
  }

  /**
   * @param {object} input
   * @param {string} input.capabilityId
   * @param {object} input.context - CapabilityContext fields
   * @param {string} [input.invocationId]
   * @returns {Promise<object>} CapabilityResult + meta
   */
  async run(input) {
    if (!input || !input.capabilityId) {
      throw new Error('capabilityId is required');
    }
    const capabilityId = String(input.capabilityId);
    const cap = this._registry.get(capabilityId);
    if (!cap) {
      throw new Error(formatMissingCapabilityError(capabilityId, this._registry));
    }
    if (cap.enabled === false) {
      throw new Error(
        [
          `Capability disabled: ${cap.name} (${capabilityId})`,
          'Status: Blocked',
          'Possible Causes: Capability disabled',
          `Recommended Action: Enable capability "${cap.name}" (${capabilityId}).`,
        ].join(' ')
      );
    }

    const context = buildCapabilityContext(input.context || {});
    const invocationId =
      input.invocationId ||
      `inv_${capabilityId}_${Date.now().toString(36)}`;
    const executionMode = resolveCapabilityExecutionMode(context, cap);
    context.executionMode = executionMode;

    context.emitProgress = (payload = {}) => {
      this._emit({
        kind: payload.kind || PROGRESS_KINDS.PROGRESS,
        capabilityId,
        invocationId,
        missionId: context.missionId,
        payload: {
          name: cap.name,
          stage: payload.stage || payload.message || null,
          message: payload.message || payload.stage || null,
          ...payload,
        },
      });
    };

    this._emit({
      kind: PROGRESS_KINDS.QUEUED,
      capabilityId,
      invocationId,
      missionId: context.missionId,
      payload: { name: cap.name, executionMode },
    });

    const canRun = Boolean(cap.canRun(context));
    const diagnosisRaw =
      typeof cap.diagnoseCanRun === 'function'
        ? cap.diagnoseCanRun(context)
        : null;
    const diagnosis = diagnosisRaw
      ? normalizeDiagnoseCanRun(diagnosisRaw, capabilityId)
      : null;

    // SPEC-058: Diagnostic mode explains blocked preconditions.
    // Execution mode preserves boolean canRun gating.
    if (executionMode === CAPABILITY_EXECUTION_MODES.DIAGNOSTIC) {
      const runnable = diagnosis
        ? diagnosis.runnable
        : canRun;
      if (!runnable) {
        const blocked = buildPreconditionBlockedResult({
          capabilityId,
          diagnosis:
            diagnosis ||
            normalizeDiagnoseCanRun(
              {
                runnable: false,
                failedPrecondition: 'canRun returned false',
                actualState: 'canRun=false (no diagnoseCanRun on capability)',
              },
              capabilityId
            ),
          diagnosticMode: true,
        });
        this._emit({
          kind: PROGRESS_KINDS.FAILED,
          capabilityId,
          invocationId,
          missionId: context.missionId,
          payload: {
            status: CAPABILITY_RESULT_STATUS.BLOCKED,
            errors: blocked.errors,
            preconditionDiagnostics: blocked.outputs.preconditionDiagnostics,
            executionMode,
          },
        });
        return {
          invocationId,
          capabilityId,
          name: cap.name,
          estimate: cap.estimate(context),
          result: blocked,
          executionMode,
        };
      }
    } else if (!canRun) {
      // Execution mode — preserve gate; enrich with diagnoseCanRun when present
      const failed = buildPreconditionBlockedResult({
        capabilityId,
        diagnosis:
          diagnosis ||
          normalizeDiagnoseCanRun(
            {
              runnable: false,
              failedPrecondition: 'canRun returned false',
              actualState: 'canRun=false (no diagnoseCanRun on capability)',
            },
            capabilityId
          ),
        diagnosticMode: false,
      });
      this._emit({
        kind: PROGRESS_KINDS.FAILED,
        capabilityId,
        invocationId,
        missionId: context.missionId,
        payload: {
          errors: failed.errors,
          preconditionDiagnostics: failed.outputs.preconditionDiagnostics,
          executionMode,
        },
      });
      return {
        invocationId,
        capabilityId,
        name: cap.name,
        estimate: cap.estimate(context),
        result: failed,
        executionMode,
      };
    }

    const estimate = cap.estimate(context);
    this._emit({
      kind: PROGRESS_KINDS.RUNNING,
      capabilityId,
      invocationId,
      missionId: context.missionId,
      payload: { name: cap.name, estimate, executionMode },
    });

    const started = Date.now();
    try {
      const raw = await Promise.resolve(cap.execute(context));
      const duration = Date.now() - started;
      const result = buildCapabilityResult({
        ...raw,
        duration: raw && Number.isFinite(Number(raw.duration)) ? raw.duration : duration,
      });

      const kind =
        result.status === CAPABILITY_RESULT_STATUS.COMPLETED
          ? PROGRESS_KINDS.COMPLETED
          : PROGRESS_KINDS.FAILED;

      this._emit({
        kind,
        capabilityId,
        invocationId,
        missionId: context.missionId,
        payload: {
          name: cap.name,
          status: result.status,
          duration: result.duration,
          executionMode,
        },
      });

      return {
        invocationId,
        capabilityId,
        name: cap.name,
        estimate,
        result,
        executionMode,
      };
    } catch (err) {
      const duration = Date.now() - started;
      const result = buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.FAILED,
        duration,
        errors: [{ message: err && err.message ? String(err.message) : 'execute failed' }],
      });
      this._emit({
        kind: PROGRESS_KINDS.FAILED,
        capabilityId,
        invocationId,
        missionId: context.missionId,
        payload: { name: cap.name, errors: result.errors, executionMode },
      });
      return {
        invocationId,
        capabilityId,
        name: cap.name,
        estimate,
        result,
        executionMode,
      };
    }
  }

  /**
   * @param {object} event
   */
  _emit(event) {
    if (!this._onProgress) return;
    this._onProgress({
      ...event,
      at: new Date().toISOString(),
    });
  }
}

/**
 * @param {object} deps
 */
function createCapabilityRunner(deps) {
  return new CapabilityRunner(deps);
}

module.exports = {
  CapabilityRunner,
  createCapabilityRunner,
};
