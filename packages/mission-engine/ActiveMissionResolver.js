'use strict';

/**
 * ActiveMissionResolver — first routing layer (SPEC-039 / ADR-025).
 *
 * Operator prompt → resolve → Resume/Modify/Diagnose OR IntentRouter (new only).
 */

const { routeIntent, ROUTE_KINDS } = require('./IntentRouter');
const { classifyMessage, MESSAGE_CLASS } = require('./classifyMessage');
const {
  createInMemoryActiveMissionBindingStore,
} = require('./ActiveMissionBindingStore');
const {
  AUDIT_KINDS,
  RESOLUTION_PATHS,
  MISSION_EVENTS,
  REVIEW_ACTIONS,
  isActiveMissionStatus,
  isTerminalStatus,
  activeMissionResolverEnabled,
  newId,
} = require('./types');

class ActiveMissionResolver {
  /**
   * @param {object} deps
   * @param {import('./MissionEngine').MissionEngine} deps.missionEngine
   * @param {object} [deps.bindings]
   * @param {boolean} [deps.enabled]
   */
  constructor(deps) {
    if (!deps || !deps.missionEngine) {
      throw new Error('ActiveMissionResolver requires missionEngine');
    }
    this._engine = deps.missionEngine;
    this._bindings =
      deps.bindings || createInMemoryActiveMissionBindingStore();
    this._enabled =
      deps.enabled != null
        ? deps.enabled !== false
        : activeMissionResolverEnabled();
    /** @type {object[]} */
    this._resolutionAudit = [];
  }

  get enabled() {
    return this._enabled;
  }

  get bindings() {
    return this._bindings;
  }

  get missionEngine() {
    return this._engine;
  }

  /**
   * @param {string} sessionId
   */
  async resolveActiveMission(sessionId) {
    if (!sessionId) return null;
    const binding = await this._bindings.get(sessionId);
    if (!binding || !binding.activeMissionId) return null;
    const mission = await this._engine.get(binding.activeMissionId);
    if (!mission || !isActiveMissionStatus(mission.status)) {
      await this._bindings.clear(sessionId);
      return null;
    }
    return mission;
  }

  /**
   * @param {string} sessionId
   */
  async clearActiveMission(sessionId) {
    return this._bindings.clear(sessionId);
  }

  /**
   * Bind session to a Mission (after create or explicit open).
   * @param {object} input
   */
  async bindSession(input) {
    const mission = input.mission || (await this._engine.get(input.missionId));
    if (!mission) throw new Error('mission required to bind');
    if (isTerminalStatus(mission.status)) {
      await this.clearActiveMission(input.sessionId);
      return null;
    }
    return this._bindings.set({
      sessionId: input.sessionId,
      activeMissionId: mission.id,
      tenantId: mission.tenantId,
      clientId: mission.clientId,
      operatorId: input.operatorId || mission.createdBy || null,
    });
  }

  /**
   * Append operator message to Mission audit (Mission Memory thin slice).
   * @param {string} missionId
   * @param {object} message
   */
  async attachMessage(missionId, message) {
    const text =
      typeof message === 'string'
        ? message
        : String((message && message.text) || '');
    const role = (message && message.role) || 'operator';
    const classification = (message && message.classification) || null;
    await this._engine.store.appendAudit({
      missionId,
      kind: AUDIT_KINDS.MESSAGE,
      payload: {
        role,
        text,
        classification,
        at: new Date().toISOString(),
      },
    });
  }

  /**
   * Surface / optionally re-run the active Mission.
   * @param {string} missionId
   * @param {object} [options]
   */
  async resumeMission(missionId, options = {}) {
    let mission = await this._engine.get(missionId);
    if (!mission) throw new Error(`Unknown mission: ${missionId}`);

    const runAgain =
      options.runAgain === true ||
      /\brun\s+again\b/i.test(String(options.message || ''));

    if (runAgain) {
      mission = await this._engine.review({
        missionId,
        action: REVIEW_ACTIONS.RUN_AGAIN,
        actor: options.operatorId || null,
      });
    }

    await this._engine.store.appendAudit({
      missionId: mission.id,
      kind: AUDIT_KINDS.RESUMED,
      payload: {
        event: MISSION_EVENTS.RESUMED,
        runAgain,
        message: options.message || null,
      },
    });
    return mission;
  }

  /**
   * Create via IntentRouter + MissionEngine; bind session.
   * @param {object} input
   */
  async startNewMission(input) {
    if (!input || !input.sessionId) {
      throw new Error('sessionId is required');
    }
    await this.clearActiveMission(input.sessionId);

    const decision = routeIntent(input.objective);
    if (decision.kind !== ROUTE_KINDS.MISSION) {
      return {
        kind: 'intelligence',
        route: decision,
        mission: null,
        classification: MESSAGE_CLASS.NEW_MISSION,
        resolutionPath: RESOLUTION_PATHS.NO_ACTIVE,
      };
    }

    const mission = await this._engine.createFromObjective({
      objective: input.objective,
      tenantId: input.tenantId,
      clientId: input.clientId != null ? input.clientId : input.tenantId,
      createdBy: input.createdBy || null,
      missionType: input.missionType || decision.missionType,
      constraints: input.constraints,
      execute: input.execute,
    });

    if (isActiveMissionStatus(mission.status)) {
      await this.bindSession({
        sessionId: input.sessionId,
        mission,
        operatorId: input.createdBy || null,
      });
    }

    return {
      kind: 'mission',
      route: decision,
      mission,
      classification: MESSAGE_CLASS.NEW_MISSION,
      resolutionPath:
        input.resolutionPath || RESOLUTION_PATHS.NO_ACTIVE,
    };
  }

  /**
   * Primary entry — resolve before IntentRouter.
   * @param {object} input
   * @param {string} input.sessionId
   * @param {string} input.message
   * @param {string|number} input.tenantId
   * @param {string|number} [input.clientId]
   * @param {string} [input.operatorId]
   * @param {object} [input.constraints]
   */
  async resolve(input) {
    if (!input || !input.sessionId) {
      throw new Error('sessionId is required');
    }
    const message = String(input.message || '').trim();
    if (!message) throw new Error('message is required');

    if (!this._enabled) {
      const decision = routeIntent(message);
      this._recordResolution({
        sessionId: input.sessionId,
        missionId: null,
        message,
        classification: MESSAGE_CLASS.NEW_MISSION,
        resolutionPath: RESOLUTION_PATHS.DISABLED,
      });
      if (decision.kind !== ROUTE_KINDS.MISSION) {
        return {
          action: 'intelligence',
          classification: MESSAGE_CLASS.NEW_MISSION,
          resolutionPath: RESOLUTION_PATHS.DISABLED,
          route: decision,
          mission: null,
        };
      }
      const mission = await this._engine.createFromObjective({
        objective: message,
        tenantId: input.tenantId,
        clientId: input.clientId != null ? input.clientId : input.tenantId,
        createdBy: input.operatorId || null,
        missionType: decision.missionType,
      });
      return {
        action: 'created',
        classification: MESSAGE_CLASS.NEW_MISSION,
        resolutionPath: RESOLUTION_PATHS.DISABLED,
        route: decision,
        mission,
      };
    }

    const active = await this.resolveActiveMission(input.sessionId);
    const { classification, reason } = classifyMessage(message, active);

    if (classification === MESSAGE_CLASS.NEW_MISSION) {
      const path = active
        ? RESOLUTION_PATHS.EXPLICIT_NEW
        : RESOLUTION_PATHS.NO_ACTIVE;
      this._recordResolution({
        sessionId: input.sessionId,
        missionId: active ? active.id : null,
        message,
        classification,
        resolutionPath: path,
        reason,
      });
      const started = await this.startNewMission({
        sessionId: input.sessionId,
        objective: message,
        tenantId: input.tenantId,
        clientId: input.clientId,
        createdBy: input.operatorId,
        constraints: input.constraints,
        resolutionPath: path,
      });
      if (started.kind === 'intelligence') {
        return {
          action: 'intelligence',
          classification,
          resolutionPath: path,
          route: started.route,
          mission: null,
          reason,
        };
      }
      return {
        action: 'created',
        classification,
        resolutionPath: path,
        route: started.route,
        mission: started.mission,
        reason,
      };
    }

    // Active Mission path — never IntentRouter
    if (!active) {
      // Should not happen (classify returns NEW without active); safety net
      return this.resolve({
        ...input,
        message: `New Mission. ${message}`,
      });
    }

    await this.attachMessage(active.id, {
      role: 'operator',
      text: message,
      classification,
    });

    if (classification === MESSAGE_CLASS.DIAGNOSE) {
      const diagnosis = await this._diagnose(active, message);
      this._recordResolution({
        sessionId: input.sessionId,
        missionId: active.id,
        message,
        classification,
        resolutionPath: RESOLUTION_PATHS.DIAGNOSE,
        reason,
      });
      await this._engine.store.appendAudit({
        missionId: active.id,
        kind: AUDIT_KINDS.DIAGNOSED,
        payload: {
          event: MISSION_EVENTS.DIAGNOSED,
          message,
          summary: diagnosis.summary,
        },
      });
      return {
        action: 'diagnosed',
        classification,
        resolutionPath: RESOLUTION_PATHS.DIAGNOSE,
        route: { kind: 'mission', missionType: active.type, reason: 'active_diagnose' },
        mission: diagnosis.mission,
        diagnosis,
        reason,
      };
    }

    if (classification === MESSAGE_CLASS.MODIFY) {
      const modified = await this._modify(active, message, input.operatorId);
      this._recordResolution({
        sessionId: input.sessionId,
        missionId: active.id,
        message,
        classification,
        resolutionPath: RESOLUTION_PATHS.MODIFY,
        reason,
      });
      return {
        action: 'modified',
        classification,
        resolutionPath: RESOLUTION_PATHS.MODIFY,
        route: { kind: 'mission', missionType: active.type, reason: 'active_modify' },
        mission: modified.mission,
        modification: modified,
        reason,
      };
    }

    // Resume (default)
    const runAgain = /\brun\s+again\b/i.test(message);
    const mission = await this.resumeMission(active.id, {
      message,
      runAgain,
      operatorId: input.operatorId,
    });
    this._recordResolution({
      sessionId: input.sessionId,
      missionId: mission.id,
      message,
      classification: MESSAGE_CLASS.RESUME,
      resolutionPath: RESOLUTION_PATHS.RESUME,
      reason,
    });
    return {
      action: 'resumed',
      classification: MESSAGE_CLASS.RESUME,
      resolutionPath: RESOLUTION_PATHS.RESUME,
      route: { kind: 'mission', missionType: mission.type, reason: 'active_resume' },
      mission,
      reason,
    };
  }

  /**
   * @param {object} mission
   * @param {string} message
   */
  async _diagnose(mission, message) {
    const audit = await this._engine.listAudit(mission.id);
    const failures = audit.filter((e) => e.kind === AUDIT_KINDS.STEP_FAIL);
    const lastFail = failures[failures.length - 1] || null;
    const waiting =
      mission.status === 'waiting' ||
      (mission.progress &&
        /paused|fail/i.test(String(mission.progress.currentStage || '')));

    const failedStep =
      ((mission.plan && mission.plan.steps) || []).find(
        (s) => s.status === 'failed'
      ) || null;

    const lines = [
      `Diagnosing active Mission "${mission.title || mission.id}" (${mission.status}).`,
      mission.progress && mission.progress.currentStage
        ? `Current stage: ${mission.progress.currentStage}.`
        : null,
      lastFail
        ? `Last failure: capability ${lastFail.capabilityId} — ${summarizeErrors(lastFail.payload && lastFail.payload.errors)}.`
        : waiting
          ? 'Mission is waiting after a step failure.'
          : 'No step_fail audit events recorded.',
      failedStep
        ? `Failed step: ${failedStep.name || failedStep.capabilityId}.`
        : null,
      `Audit events: ${audit.length}. Plan steps: ${((mission.plan && mission.plan.steps) || []).length}.`,
    ].filter(Boolean);

    return {
      mission,
      audit,
      failures,
      lastFail,
      failedStep,
      summary: lines.join(' '),
      message,
    };
  }

  /**
   * Apply heuristic constraint patches; mark stale steps; rerun stale+.
   * @param {object} mission
   * @param {string} message
   * @param {string} [operatorId]
   */
  async _modify(mission, message, operatorId) {
    const patch = extractConstraintPatch(message);
    const constraints = {
      ...(mission.constraints || {}),
      ...patch.constraints,
    };

    // SPEC-041: replan graph for objective / constraint modifications
    let updated = mission;
    const replanMods = {
      constraints,
      objective: mission.objectiveText,
      insertStages: patch.insertStages || [],
      removeStages: patch.removeStages || [],
      staleCapabilityIds: patch.staleCapabilityIds || [],
      staleAll: Boolean(patch.staleAll),
    };

    if (
      this._engine.planner &&
      typeof this._engine.planner.replan === 'function'
    ) {
      const replanned = this._engine.planner.replan(mission, replanMods);
      updated = await this._engine.store.update({
        id: mission.id,
        constraints: replanned.constraints,
        plan: replanned.plan,
        durationEstimateMs: replanned.durationEstimateMs,
        confidence: replanned.confidence,
      });
    } else {
      const steps = ((mission.plan && mission.plan.steps) || []).map((s) => {
        const stale = patch.staleCapabilityIds.includes(s.capabilityId);
        if (!stale && !patch.staleAll) return s;
        return { ...s, status: 'stale', error: undefined };
      });

      let seenStale = false;
      const cascaded = steps.map((s) => {
        if (s.status === 'stale') seenStale = true;
        if (seenStale && s.status !== 'stale') {
          return { ...s, status: 'stale', error: undefined };
        }
        return s;
      });

      updated = await this._engine.store.update({
        id: mission.id,
        constraints,
        plan: { ...mission.plan, steps: cascaded },
      });
    }

    await this._engine.store.appendAudit({
      missionId: mission.id,
      kind: AUDIT_KINDS.MODIFIED,
      payload: {
        event: MISSION_EVENTS.MODIFIED,
        message,
        patch,
        actor: operatorId || null,
        replan:
          (updated.plan && updated.plan.replan) || null,
        plannerVersion:
          (updated.plan && updated.plan.plannerVersion) || null,
      },
    });

    // Rerun from first non-completed step
    const stepsNow = (updated.plan && updated.plan.steps) || [];
    const firstQueued = stepsNow.findIndex(
      (s) => s.status === 'queued' || s.status === 'stale'
    );
    if (firstQueued >= 0) {
      const resetSteps = stepsNow.map((s, idx) =>
        idx >= firstQueued
          ? { ...s, status: 'queued', error: undefined }
          : { ...s, status: s.status === 'completed' ? 'completed' : s.status }
      );
      updated = await this._engine.store.update({
        id: mission.id,
        status: 'planning',
        plan: { ...updated.plan, steps: resetSteps },
        review: null,
        deliverables: null,
        completedAt: null,
        progress: {
          completedSteps: firstQueued,
          totalSteps: resetSteps.length,
          currentStage: 'Planning Mission',
          currentCapabilityId: null,
          percent: Math.round(
            (firstQueued / Math.max(resetSteps.length, 1)) * 100
          ),
          counts: null,
        },
      });
      updated = await this._engine.executor.execute(mission.id);
    }

    return {
      mission: updated,
      patch,
      summary: patch.summary || `Mission updated from: "${message}"`,
    };
  }

  _recordResolution(row) {
    const entry = {
      id: newId('res'),
      sessionId: row.sessionId,
      missionId: row.missionId || null,
      message: row.message,
      classification: row.classification,
      resolutionPath: row.resolutionPath,
      reason: row.reason || null,
      at: new Date().toISOString(),
    };
    this._resolutionAudit.push(entry);
    if (row.missionId) {
      this._engine.store.appendAudit({
        missionId: row.missionId,
        kind: AUDIT_KINDS.RESOLUTION,
        payload: entry,
      }).catch(() => {});
    }
    return entry;
  }

  /** Test helper */
  listResolutions() {
    return this._resolutionAudit.map((r) => ({ ...r }));
  }
}

function summarizeErrors(errors) {
  if (!errors) return 'unknown error';
  if (typeof errors === 'string') return errors;
  if (Array.isArray(errors)) {
    return errors
      .map((e) => (typeof e === 'string' ? e : e.message || JSON.stringify(e)))
      .join('; ');
  }
  if (errors.message) return errors.message;
  try {
    return JSON.stringify(errors);
  } catch {
    return 'unknown error';
  }
}

/**
 * @param {string} message
 */
function extractConstraintPatch(message) {
  const constraints = {};
  const staleCapabilityIds = [];
  let staleAll = false;
  const notes = [];

  const count =
    /(?:target\s+count|increase|decrease|to)\s*(?:to|=|:)?\s*(\d{1,4})\b/i.exec(
      message
    ) || /\b(\d{2,4})\s+prospects?\b/i.exec(message);
  if (count) {
    constraints.targetCount = Number(count[1]);
    staleCapabilityIds.push(
      'prospect_discovery',
      'opportunity_ranking',
      'business_intelligence',
      'sales_intelligence'
    );
    notes.push(`targetCount=${constraints.targetCount}`);
  }

  const usePlace =
    /\buse\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)?)\b/.exec(message) ||
    /\binstead\s+of\s+.+\buse\s+([A-Za-z][A-Za-z\s]+)/i.exec(message);
  if (usePlace) {
    constraints.locationHint = usePlace[1].trim();
    staleCapabilityIds.push('prospect_discovery');
    notes.push(`locationHint=${constraints.locationHint}`);
  }

  if (/\b(remove|exclude)\s+/i.test(message)) {
    const rem = /\b(?:remove|exclude)\s+(.+)$/i.exec(message);
    if (rem) {
      constraints.exclusions = [
        ...((constraints.exclusions) || []),
        rem[1].replace(/[."]+$/, '').trim(),
      ];
      staleCapabilityIds.push(
        'prospect_discovery',
        'opportunity_ranking',
        'business_intelligence',
        'sales_intelligence'
      );
      notes.push(`exclude=${constraints.exclusions.join(',')}`);
    }
  }

  if (/\bchange\s+(the\s+)?(discovery\s+)?profile\b/i.test(message)) {
      staleCapabilityIds.push(
        'prospect_discovery',
        'company_enrichment',
        'opportunity_ranking',
        'business_intelligence',
        'sales_intelligence',
        'campaign_builder'
      );
    notes.push('discovery profile change requested');
  }

  if (!notes.length) {
    staleAll = true;
    notes.push('generic modification — full chain marked stale');
  }

  return {
    constraints,
    staleCapabilityIds: [...new Set(staleCapabilityIds)],
    staleAll,
    summary: `Applied: ${notes.join('; ')}`,
  };
}

function createActiveMissionResolver(deps) {
  return new ActiveMissionResolver(deps);
}

module.exports = {
  ActiveMissionResolver,
  createActiveMissionResolver,
  extractConstraintPatch,
  summarizeErrors,
};
