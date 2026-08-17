'use strict';

/**
 * SPEC-104 — Event-driven operator context rebuild hooks.
 *
 * Call from routes/agents when meaningful business events occur.
 * Rebuilds are async and fail-soft — never block the primary action.
 */

const {
  REBUILD_TRIGGERS,
  triggerOperatorContextRebuild,
} = require('./operatorContext');

/**
 * Schedule a non-blocking operator context rebuild.
 *
 * @param {object} input
 * @param {string} input.clientId
 * @param {string} input.trigger — REBUILD_TRIGGERS value
 * @param {object} [input.metadata]
 * @param {object} [input.opts] — passed to rebuild (store, pool, etc.)
 */
function scheduleOperatorContextRebuild(input = {}) {
  const clientId = input.clientId ?? input.client_id;
  if (clientId == null || clientId === '') return;

  const tenantId = String(input.tenantId || clientId);
  const payload = {
    tenantId,
    clientId: Number(clientId),
    trigger: input.trigger || REBUILD_TRIGGERS.MANUAL,
    metadata: input.metadata || {},
    ...(input.opts || {}),
  };

  setImmediate(() => {
    triggerOperatorContextRebuild(payload).catch((err) => {
      console.error('[operatorContextEvents]', err.message);
    });
  });
}

function onBlueprintApproved(clientId, metadata = {}, opts = {}) {
  scheduleOperatorContextRebuild({
    clientId,
    trigger: REBUILD_TRIGGERS.BLUEPRINT_APPROVED,
    metadata,
    opts,
  });
}

function onInterviewCompleted(clientId, metadata = {}, opts = {}) {
  scheduleOperatorContextRebuild({
    clientId,
    trigger: REBUILD_TRIGGERS.INTERVIEW_COMPLETED,
    metadata,
    opts,
  });
}

function onPlaybookUpdated(clientId, metadata = {}, opts = {}) {
  scheduleOperatorContextRebuild({
    clientId,
    trigger: REBUILD_TRIGGERS.PLAYBOOK_UPDATED,
    metadata,
    opts,
  });
}

function onMissionCompleted(clientId, metadata = {}, opts = {}) {
  scheduleOperatorContextRebuild({
    clientId,
    trigger: REBUILD_TRIGGERS.MISSION_COMPLETED,
    metadata,
    opts,
  });
}

function onCampaignLaunched(clientId, metadata = {}, opts = {}) {
  scheduleOperatorContextRebuild({
    clientId,
    trigger: REBUILD_TRIGGERS.CAMPAIGN_LAUNCHED,
    metadata,
    opts,
  });
}

function onContentPublished(clientId, metadata = {}, opts = {}) {
  scheduleOperatorContextRebuild({
    clientId,
    trigger: REBUILD_TRIGGERS.CONTENT_PUBLISHED,
    metadata,
    opts,
  });
}

function onOutcomeRecorded(clientId, metadata = {}, opts = {}) {
  scheduleOperatorContextRebuild({
    clientId,
    trigger: REBUILD_TRIGGERS.OUTCOME_RECORDED,
    metadata,
    opts,
  });
}

function onOperatorObjectiveChanged(clientId, metadata = {}, opts = {}) {
  scheduleOperatorContextRebuild({
    clientId,
    trigger: REBUILD_TRIGGERS.OBJECTIVE_CHANGED,
    metadata,
    opts,
  });
}

function onWalkthroughBooked(clientId, metadata = {}, opts = {}) {
  scheduleOperatorContextRebuild({
    clientId,
    trigger: REBUILD_TRIGGERS.WALKTHROUGH_BOOKED,
    metadata,
    opts,
  });
}

function onJobWon(clientId, metadata = {}, opts = {}) {
  scheduleOperatorContextRebuild({
    clientId,
    trigger: REBUILD_TRIGGERS.JOB_WON,
    metadata,
    opts,
  });
}

module.exports = {
  scheduleOperatorContextRebuild,
  onBlueprintApproved,
  onInterviewCompleted,
  onPlaybookUpdated,
  onMissionCompleted,
  onCampaignLaunched,
  onContentPublished,
  onOutcomeRecorded,
  onOperatorObjectiveChanged,
  onWalkthroughBooked,
  onJobWon,
  REBUILD_TRIGGERS,
};
