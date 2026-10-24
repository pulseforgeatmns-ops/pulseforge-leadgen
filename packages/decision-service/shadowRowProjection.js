'use strict';

const { messagePatternFlags } = require('./messagePatternFlags');
const { classifyShadowMismatch } = require('./shadowMismatchClassification');

function firstErrorReason(row = {}) {
  if (row.fallback_reason) return String(row.fallback_reason);
  const code = row.errors?.[0]?.code;
  return code ? String(code) : null;
}

function missionContextFromState(state = {}) {
  const mission = state.context?.mission || null;
  const pending = mission?.pending_decision || null;
  return {
    mission_stage: mission?.stage || null,
    pending_decision_present: Boolean(pending),
    pending_decision_kind: pending?.kind || null,
  };
}

function prodProjection(currentRoute = {}) {
  return {
    prod_route: currentRoute.route ?? null,
    prod_action: currentRoute.action ?? null,
    prod_kind: currentRoute.kind ?? null,
  };
}

function enrichShadowRow(row, { question, state } = {}) {
  const missionCtx = state ? missionContextFromState(state) : {};
  const prod = prodProjection(row.current_route || {});
  let routeComparable = Boolean(
    row.status === 'evaluated'
    && prod.prod_route != null
    && row.recommended_route != null
    && row.recommended_route !== 'unknown',
  );
  if (row.comparison === 'unavailable') routeComparable = false;
  const mismatch_classification = row.status === 'evaluated'
    ? classifyShadowMismatch({ ...row, ...prod, route_comparable: routeComparable })
    : null;
  const flags = question ? messagePatternFlags(question) : null;
  return {
    ...row,
    ...missionCtx,
    ...prod,
    route_comparable: routeComparable,
    mismatch_classification,
    error_reason: firstErrorReason(row),
    message_pattern_flags: flags,
  };
}

module.exports = {
  enrichShadowRow,
  missionContextFromState,
  prodProjection,
  firstErrorReason,
};
