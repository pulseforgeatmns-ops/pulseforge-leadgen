'use strict';

/**
 * Apply Direct Mail Execution operator actions (SPEC-035 / ADR-022).
 */

const {
  EXECUTION_STATUS,
  PRINT_SESSION_STATUS,
  RESPONSE_STATUS,
  buildAssemblyChecklist,
  isAssemblyComplete,
  buildPrintSession,
  buildAuditEntry,
  buildCampaignLock,
  buildMissionExecutionEvent,
  buildMissionTimelineEntry,
} = require('./types');
const {
  canTransition,
  assertTransition,
  isLockedStatus,
  nextPrimaryStatus,
} = require('./transitions');
const { computeMetrics } = require('./assemble');
const { validateArtifactMutation } = require('./validate');

function newAuditId() {
  return `aud_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
}

function newSessionId() {
  return `ps_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
}

/**
 * @param {object} inputs
 * @returns {object[]}
 */
function normalizeActions(inputs = {}) {
  if (Array.isArray(inputs.executionActions)) return inputs.executionActions.slice();
  if (Array.isArray(inputs.actions)) return inputs.actions.slice();
  if (inputs.action && typeof inputs.action === 'object') return [inputs.action];
  return [];
}

/**
 * @param {object[]} prospects
 * @param {string} prospectId
 * @returns {object|null}
 */
function findProspect(prospects, prospectId) {
  return (
    prospects.find((p) => String(p.prospectId) === String(prospectId)) || null
  );
}

/**
 * @param {object} action
 * @param {object} execution
 * @returns {string[]}
 */
function selectedIds(action, execution) {
  if (Array.isArray(action.prospectIds) && action.prospectIds.length) {
    return action.prospectIds.map(String);
  }
  if (action.prospectId != null) return [String(action.prospectId)];
  if (Array.isArray(action.selected) && action.selected.length) {
    return action.selected.map(String);
  }
  return (execution.prospects || [])
    .filter((p) => !p.skipped)
    .map((p) => p.prospectId);
}

/**
 * Append immutable audit entry.
 * @param {object} execution
 * @param {object} partial
 */
function appendAudit(execution, partial) {
  const entry = buildAuditEntry({
    id: newAuditId(),
    ...partial,
  });
  execution.auditLog = [...(execution.auditLog || []), entry];
  return entry;
}

/**
 * Append mission memory shapes.
 * @param {object} execution
 * @param {object} eventPartial
 * @param {object} [timelinePartial]
 */
function appendMission(execution, eventPartial, timelinePartial) {
  const event = buildMissionExecutionEvent({
    revision: execution.summary && execution.summary.revision,
    ...eventPartial,
  });
  execution.missionEvents = [...(execution.missionEvents || []), event];
  if (timelinePartial) {
    execution.timeline = [
      ...(execution.timeline || []),
      buildMissionTimelineEntry(timelinePartial),
    ];
  }
  return event;
}

/**
 * Transition campaign status with audit + mission events.
 * @param {object} execution
 * @param {string} to
 * @param {object} ctx
 * @returns {{ ok: boolean, error?: string }}
 */
function transitionStatus(execution, to, ctx = {}) {
  const from = execution.summary.status;
  if (!canTransition(from, to)) {
    return { ok: false, error: `illegal_transition:${from}->${to}` };
  }
  assertTransition(from, to);
  const now = ctx.now || new Date().toISOString();
  const operator = ctx.operator || 'operator';

  execution.summary.status = to;
  execution.summary.updatedAt = now;

  // Lock artifacts when entering Printing
  if (to === EXECUTION_STATUS.PRINTING || isLockedStatus(to)) {
    if (!execution.lock.locked) {
      execution.lock = buildCampaignLock({
        locked: true,
        lockedAt: now,
        lockedBy: operator,
        campaignRevision: execution.summary.revision,
        mailPackageBatchId: execution.lock.mailPackageBatchId,
        executionPackageId: execution.lock.executionPackageId,
      });
      execution.summary.locked = true;
    }
  }

  appendAudit(execution, {
    previousState: from,
    newState: to,
    timestamp: now,
    operator,
    notes: ctx.notes || `Transition ${from} → ${to}`,
    action: ctx.action || 'advance_state',
  });

  appendMission(
    execution,
    {
      eventType: 'state_transition',
      previousState: from,
      newState: to,
      operator,
      timestamp: now,
      summary: ctx.notes || `Execution ${from} → ${to}`,
    },
    {
      stage: 'direct_mail_execution',
      status: to,
      timestamp: now,
      summary: `Campaign ${execution.summary.campaignName}: ${to}`,
      operator,
    }
  );

  return { ok: true };
}

/**
 * Apply a single action.
 * @param {object} execution
 * @param {object} action
 * @param {object} ctx
 * @returns {{ errors: string[], changeParts: string[] }}
 */
function applyAction(execution, action, ctx = {}) {
  const errors = [];
  const changeParts = [];
  const now = ctx.now || new Date().toISOString();
  const operator = action.operator || ctx.operator || 'operator';
  const type = String(action.type || action.action || '').trim();

  const refreshMetrics = () => {
    execution.summary.metrics = computeMetrics(execution.prospects);
    execution.summary.updatedAt = now;
  };

  if (type === 'start_execution') {
    if (execution.summary.status === EXECUTION_STATUS.DRAFT) {
      const r = transitionStatus(execution, EXECUTION_STATUS.READY_TO_PRINT, {
        operator,
        now,
        notes: action.notes || 'Execution started — Ready to Print',
        action: type,
      });
      if (!r.ok) errors.push(r.error);
      else changeParts.push('start_execution');
    } else {
      changeParts.push('start_execution:noop');
    }
  } else if (type === 'advance_state') {
    const to =
      action.to ||
      action.targetStatus ||
      nextPrimaryStatus(execution.summary.status);
    if (!to) {
      errors.push('no_next_state');
    } else {
      const r = transitionStatus(execution, to, {
        operator,
        now,
        notes: action.notes || '',
        action: type,
      });
      if (!r.ok) errors.push(r.error);
      else changeParts.push(`advance:${to}`);
    }
  } else if (type === 'start_print_session') {
    // Ensure Ready to Print then Printing
    if (execution.summary.status === EXECUTION_STATUS.DRAFT) {
      transitionStatus(execution, EXECUTION_STATUS.READY_TO_PRINT, {
        operator,
        now,
        notes: 'Auto-advance to Ready to Print before print session',
        action: type,
      });
    }
    if (execution.summary.status === EXECUTION_STATUS.READY_TO_PRINT) {
      const r = transitionStatus(execution, EXECUTION_STATUS.PRINTING, {
        operator,
        now,
        notes: action.notes || 'Print session started — artifacts locked',
        action: type,
      });
      if (!r.ok) errors.push(r.error);
    } else if (execution.summary.status !== EXECUTION_STATUS.PRINTING) {
      // Already past printing — still allow session record if assembling etc.
      if (
        ![
          EXECUTION_STATUS.PRINTING,
          EXECUTION_STATUS.PRINTED,
          EXECUTION_STATUS.ASSEMBLING,
        ].includes(execution.summary.status)
      ) {
        errors.push(`cannot_print_from:${execution.summary.status}`);
      }
    }

    if (!errors.length) {
      const session = buildPrintSession({
        id: newSessionId(),
        campaignId: execution.summary.campaignId,
        campaignName: execution.summary.campaignName,
        revision: execution.summary.revision,
        operator,
        timestamp: now,
        prospectCount: (execution.prospects || []).filter((p) => !p.skipped)
          .length,
        printStatus: PRINT_SESSION_STATUS.OPEN,
      });
      execution.printSessions = [...(execution.printSessions || []), session];
      changeParts.push(`print_session:${session.id}`);
    }
  } else if (type === 'complete_print_session') {
    const sessions = execution.printSessions || [];
    const open = [...sessions].reverse().find(
      (s) => s.printStatus === PRINT_SESSION_STATUS.OPEN
    );
    if (open) {
      open.printStatus = PRINT_SESSION_STATUS.COMPLETED;
      open.completedAt = now;
      execution.printSessions = sessions.map((s) =>
        s.id === open.id ? { ...open } : s
      );
    }
    for (const p of execution.prospects) {
      if (!p.skipped) {
        p.printed = true;
        p.lastModified = now;
      }
    }
    if (execution.summary.status === EXECUTION_STATUS.PRINTING) {
      transitionStatus(execution, EXECUTION_STATUS.PRINTED, {
        operator,
        now,
        notes: action.notes || 'Print session completed',
        action: type,
      });
    }
    if (
      action.advanceToAssembling !== false &&
      execution.summary.status === EXECUTION_STATUS.PRINTED
    ) {
      transitionStatus(execution, EXECUTION_STATUS.ASSEMBLING, {
        operator,
        now,
        notes: 'Entering assembly',
        action: type,
      });
    }
    refreshMetrics();
    changeParts.push('complete_print_session');
  } else if (
    type === 'assembly_complete' ||
    type === 'complete' ||
    (type === 'assembly' && action.mode === 'complete')
  ) {
    const ids = selectedIds(action, execution);
    for (const id of ids) {
      const row = findProspect(execution.prospects, id);
      if (!row || row.skipped) continue;
      const checklist = buildAssemblyChecklist({
        letterInserted: true,
        envelopeAddressed: true,
        insertsAdded: true,
        sealed: true,
        postageApplied: true,
        ...(action.checklist || {}),
      });
      row.assembly = checklist;
      row.assemblyComplete = isAssemblyComplete(checklist);
      row.lastModified = now;
      appendAudit(execution, {
        previousState: execution.summary.status,
        newState: execution.summary.status,
        timestamp: now,
        operator,
        notes: `Assembly complete for ${row.company || id}`,
        action: 'assembly_complete',
        prospectId: id,
      });
      appendMission(execution, {
        eventType: 'assembly_complete',
        operator,
        timestamp: now,
        summary: `Assembled package for ${row.company || id}`,
        prospectId: id,
        newState: execution.summary.status,
      });
      changeParts.push(`assembly_complete:${id}`);
    }
    maybeAdvanceAfterAssembly(execution, { operator, now });
    refreshMetrics();
  } else if (type === 'assembly_skip' || type === 'skip') {
    const ids = selectedIds(action, execution);
    for (const id of ids) {
      const row = findProspect(execution.prospects, id);
      if (!row) continue;
      row.skipped = true;
      row.lastModified = now;
      appendAudit(execution, {
        previousState: execution.summary.status,
        newState: execution.summary.status,
        timestamp: now,
        operator,
        notes: `Skipped assembly for ${row.company || id}`,
        action: 'assembly_skip',
        prospectId: id,
      });
      changeParts.push(`assembly_skip:${id}`);
    }
    maybeAdvanceAfterAssembly(execution, { operator, now });
    refreshMetrics();
  } else if (type === 'assembly_reopen' || type === 'reopen') {
    const ids = selectedIds(action, execution);
    for (const id of ids) {
      const row = findProspect(execution.prospects, id);
      if (!row) continue;
      row.skipped = false;
      row.assembly = buildAssemblyChecklist(action.checklist || {});
      row.assemblyComplete = isAssemblyComplete(row.assembly);
      row.lastModified = now;
      changeParts.push(`assembly_reopen:${id}`);
    }
    // If we had advanced past assembling, reopen does not roll back campaign status
    // (deterministic forward machine). Operator must complete remaining work.
    refreshMetrics();
  } else if (type === 'mark_selected_mailed' || type === 'mark_all_mailed') {
    const ids =
      type === 'mark_all_mailed'
        ? (execution.prospects || [])
            .filter((p) => !p.skipped)
            .map((p) => p.prospectId)
        : selectedIds(action, execution);

    // Ensure Ready to Mail before marking mailed
    if (execution.summary.status === EXECUTION_STATUS.ASSEMBLING) {
      maybeAdvanceAfterAssembly(execution, { operator, now, force: true });
    }
    if (execution.summary.status === EXECUTION_STATUS.READY_TO_MAIL) {
      // stay; mark then advance
    } else if (
      ![
        EXECUTION_STATUS.READY_TO_MAIL,
        EXECUTION_STATUS.MAILED,
        EXECUTION_STATUS.DELIVERED,
        EXECUTION_STATUS.RESPONDED,
      ].includes(execution.summary.status)
    ) {
      // try auto-advance through ready_to_mail if printed/assembled
      if (execution.summary.status === EXECUTION_STATUS.PRINTED) {
        transitionStatus(execution, EXECUTION_STATUS.ASSEMBLING, {
          operator,
          now,
          notes: 'Auto-advance for mailing',
          action: type,
        });
      }
      if (execution.summary.status === EXECUTION_STATUS.ASSEMBLING) {
        maybeAdvanceAfterAssembly(execution, {
          operator,
          now,
          force: true,
        });
      }
    }

    const mailedAt = action.date || action.mailedAt || now;
    for (const id of ids) {
      const row = findProspect(execution.prospects, id);
      if (!row || row.skipped) continue;
      row.mailed = true;
      row.mailedAt = mailedAt;
      row.printed = true;
      if (action.uspsBatchId != null) {
        row.uspsBatchId = String(action.uspsBatchId);
      }
      row.lastModified = now;
      appendAudit(execution, {
        previousState: execution.summary.status,
        newState: execution.summary.status,
        timestamp: now,
        operator,
        notes:
          action.notes ||
          `Mailed ${row.company || id}${
            action.uspsBatchId ? ` (USPS ${action.uspsBatchId})` : ''
          }`,
        action: type,
        prospectId: id,
      });
      appendMission(execution, {
        eventType: 'mailed',
        operator,
        timestamp: mailedAt,
        summary: `Mailed package for ${row.company || id}`,
        prospectId: id,
        newState: EXECUTION_STATUS.MAILED,
      });
      changeParts.push(`mailed:${id}`);
    }

    const active = (execution.prospects || []).filter((p) => !p.skipped);
    const allMailed =
      active.length > 0 && active.every((p) => p.mailed === true);
    if (
      allMailed &&
      execution.summary.status === EXECUTION_STATUS.READY_TO_MAIL
    ) {
      transitionStatus(execution, EXECUTION_STATUS.MAILED, {
        operator,
        now: mailedAt,
        notes:
          action.notes ||
          (action.uspsBatchId
            ? `All mailed — USPS ${action.uspsBatchId}`
            : 'All packages marked mailed'),
        action: type,
      });
    }
    refreshMetrics();
  } else if (type === 'mark_delivered') {
    const ids = selectedIds(action, execution);
    for (const id of ids) {
      const row = findProspect(execution.prospects, id);
      if (!row || row.skipped) continue;
      row.delivered = true;
      row.deliveredAt = action.date || now;
      row.lastModified = now;
      changeParts.push(`delivered:${id}`);
    }
    if (execution.summary.status === EXECUTION_STATUS.MAILED) {
      transitionStatus(execution, EXECUTION_STATUS.DELIVERED, {
        operator,
        now,
        notes: action.notes || 'Marked delivered',
        action: type,
      });
    }
    refreshMetrics();
  } else if (type === 'set_response') {
    const status = String(action.responseStatus || action.status || '').trim();
    if (!Object.values(RESPONSE_STATUS).includes(status)) {
      errors.push(`invalid_response_status:${status}`);
    } else {
      const ids = selectedIds(action, execution);
      for (const id of ids) {
        const row = findProspect(execution.prospects, id);
        if (!row) continue;
        row.responseStatus = status;
        row.responseNotes =
          action.notes != null ? String(action.notes) : row.responseNotes;
        row.responseAt = action.date || now;
        row.lastModified = now;
        appendAudit(execution, {
          previousState: execution.summary.status,
          newState: execution.summary.status,
          timestamp: now,
          operator,
          notes: `Response ${status} for ${row.company || id}`,
          action: 'set_response',
          prospectId: id,
        });
        appendMission(execution, {
          eventType: 'response',
          operator,
          timestamp: now,
          summary: `Response ${status}: ${row.company || id}`,
          prospectId: id,
          newState: EXECUTION_STATUS.RESPONDED,
        });
        changeParts.push(`response:${id}:${status}`);
      }
      if (
        [
          EXECUTION_STATUS.MAILED,
          EXECUTION_STATUS.DELIVERED,
        ].includes(execution.summary.status)
      ) {
        transitionStatus(execution, EXECUTION_STATUS.RESPONDED, {
          operator,
          now,
          notes: 'First response recorded',
          action: type,
        });
      }
      refreshMetrics();
    }
  } else if (type === 'complete_campaign') {
    if (execution.summary.status === EXECUTION_STATUS.RESPONDED) {
      const r = transitionStatus(execution, EXECUTION_STATUS.COMPLETED, {
        operator,
        now,
        notes: action.notes || 'Campaign execution completed',
        action: type,
      });
      if (!r.ok) errors.push(r.error);
      else changeParts.push('complete_campaign');
    } else if (execution.summary.status === EXECUTION_STATUS.COMPLETED) {
      changeParts.push('complete_campaign:noop');
    } else {
      // Allow complete from mailed/delivered by stepping through responded
      if (
        [EXECUTION_STATUS.MAILED, EXECUTION_STATUS.DELIVERED].includes(
          execution.summary.status
        )
      ) {
        transitionStatus(execution, EXECUTION_STATUS.RESPONDED, {
          operator,
          now,
          notes: 'Advance to responded before complete',
          action: type,
        });
      }
      if (execution.summary.status === EXECUTION_STATUS.RESPONDED) {
        transitionStatus(execution, EXECUTION_STATUS.COMPLETED, {
          operator,
          now,
          notes: action.notes || 'Campaign execution completed',
          action: type,
        });
        changeParts.push('complete_campaign');
      } else {
        errors.push(`cannot_complete_from:${execution.summary.status}`);
      }
    }
  } else if (
    type === 'replace_revision' ||
    type === 'replace_mail_batch' ||
    type === 'replace_execution_package' ||
    type === 'generate_content'
  ) {
    const attempt = {
      replaceRevision: type === 'replace_revision',
      replaceMailBatch: type === 'replace_mail_batch',
      replaceExecutionPackage: type === 'replace_execution_package',
      generateContent: type === 'generate_content',
    };
    const gate = validateArtifactMutation(execution, attempt);
    if (!gate.ok) {
      errors.push(...gate.errors);
    } else {
      errors.push('artifact_mutation_requires_new_revision');
    }
  } else if (type) {
    errors.push(`unknown_action:${type}`);
  }

  return { errors, changeParts };
}

/**
 * Advance Assembling → Ready to Mail when all non-skipped are assembled or force.
 * @param {object} execution
 * @param {object} ctx
 */
function maybeAdvanceAfterAssembly(execution, ctx = {}) {
  if (execution.summary.status !== EXECUTION_STATUS.ASSEMBLING) return;
  const active = (execution.prospects || []).filter((p) => !p.skipped);
  const allDone =
    ctx.force === true ||
    (active.length > 0 && active.every((p) => p.assemblyComplete === true));
  if (allDone) {
    transitionStatus(execution, EXECUTION_STATUS.READY_TO_MAIL, {
      operator: ctx.operator || 'operator',
      now: ctx.now || new Date().toISOString(),
      notes: 'Assembly complete — Ready to Mail',
      action: 'assembly_complete',
    });
  }
}

/**
 * Apply a list of actions to an execution workspace.
 * @param {object} execution
 * @param {object[]} actions
 * @param {object} [ctx]
 * @returns {{ execution: object, errors: string[], changeSummary: string }}
 */
function applyExecutionActions(execution, actions, ctx = {}) {
  const allErrors = [];
  const allParts = [];
  for (const action of actions) {
    const { errors, changeParts } = applyAction(execution, action, ctx);
    allErrors.push(...errors);
    allParts.push(...changeParts);
  }
  execution.summary.metrics = computeMetrics(execution.prospects);
  return {
    execution,
    errors: allErrors,
    changeSummary: allParts.join('; ') || 'no_changes',
  };
}

module.exports = {
  normalizeActions,
  applyAction,
  applyExecutionActions,
  transitionStatus,
  appendAudit,
  maybeAdvanceAfterAssembly,
};
