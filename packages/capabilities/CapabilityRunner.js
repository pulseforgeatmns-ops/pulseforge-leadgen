'use strict';

const {
  PROGRESS_KINDS,
  CAPABILITY_RESULT_STATUS,
  buildCapabilityResult,
  buildCapabilityContext,
} = require('./types');
const { formatMissingCapabilityError } = require('./CapabilityRegistry');

/**
 * CapabilityRunner — execute only through the registry (SPEC-023 / SPEC-054).
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
      payload: { name: cap.name },
    });

    if (!cap.canRun(context)) {
      const failed = buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.FAILED,
        errors: [{ message: 'canRun returned false', capabilityId }],
      });
      this._emit({
        kind: PROGRESS_KINDS.FAILED,
        capabilityId,
        invocationId,
        missionId: context.missionId,
        payload: { errors: failed.errors },
      });
      return {
        invocationId,
        capabilityId,
        name: cap.name,
        estimate: cap.estimate(context),
        result: failed,
      };
    }

    const estimate = cap.estimate(context);
    this._emit({
      kind: PROGRESS_KINDS.RUNNING,
      capabilityId,
      invocationId,
      missionId: context.missionId,
      payload: { name: cap.name, estimate },
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
        },
      });

      return {
        invocationId,
        capabilityId,
        name: cap.name,
        estimate,
        result,
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
        payload: { name: cap.name, errors: result.errors },
      });
      return {
        invocationId,
        capabilityId,
        name: cap.name,
        estimate,
        result,
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
