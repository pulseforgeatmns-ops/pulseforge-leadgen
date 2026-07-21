'use strict';

// Phase A2 §11 — single authority for the setter qualification thresholds.
//
// DOCUMENTED PRODUCTION DISCREPANCY (do not "fix" silently):
//   - Visibility promotion (utils/setterVisibility.js) requires icp_score >= 70
//     plus a service-area match before setter_visible becomes true.
//   - The setter queue/metrics SQL additionally filters displayed leads to
//     icp_score >= 40. A lead can therefore be setter_visible (via manual or
//     engagement override) yet hidden from the queue when its score is < 40,
//     and mid-band (40–69) leads appear only when an override promoted them.
//
// Both numbers now live here so every consumer reads one module. The
// tenant-configurable override column is clients.setter_qualification_threshold
// (added by utils/lifecycleSchema.js). Until the shadow comparison report
// (scripts/thresholdDeltaReport.js) is reviewed and a change is explicitly
// approved, production queue membership MUST NOT change: the defaults below
// are exactly the current production values.

const SETTER_VISIBILITY_THRESHOLD = 70;   // promotion gate (setterVisibility.js)
const SETTER_QUEUE_DISPLAY_THRESHOLD = 40; // queue/metrics display filter

function getQueueDisplayThreshold(clientRow = null) {
  const configured = Number(clientRow?.setter_qualification_threshold);
  if (Number.isInteger(configured) && configured >= 0 && configured <= 100) return configured;
  return SETTER_QUEUE_DISPLAY_THRESHOLD;
}

function getVisibilityThreshold(_clientRow = null) {
  // Tenant overrides intentionally do NOT apply to the visibility gate yet;
  // that change requires the threshold-delta report approval first.
  return SETTER_VISIBILITY_THRESHOLD;
}

module.exports = {
  SETTER_QUEUE_DISPLAY_THRESHOLD,
  SETTER_VISIBILITY_THRESHOLD,
  getQueueDisplayThreshold,
  getVisibilityThreshold,
};
