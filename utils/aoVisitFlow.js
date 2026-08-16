const { selectVisitProbes } = require('./aoMessageTemplates');

const MAX_PROBES = 2;

function initProbeState(payload = {}) {
  return {
    probe_queue: Array.isArray(payload.probe_queue) ? payload.probe_queue : [],
    probe_index: Number(payload.probe_index) || 0,
    probe_answers: payload.probe_answers && typeof payload.probe_answers === 'object' ? payload.probe_answers : {},
    in_probe_mode: Boolean(payload.in_probe_mode),
    _probes_after_visit: Boolean(payload._probes_after_visit),
    _probes_after_interest: Boolean(payload._probes_after_interest),
    _probes_after_next_action: Boolean(payload._probes_after_next_action),
  };
}

function enqueueProbes(payload, phase) {
  const state = initProbeState(payload);
  const existingKeys = state.probe_queue.map(p => p.key);
  const remaining = MAX_PROBES - state.probe_queue.length;
  if (remaining <= 0) return state;
  const newProbes = selectVisitProbes(payload, {
    phase,
    maxTotal: Math.min(1, remaining),
    existingKeys,
  });
  state.probe_queue = [...state.probe_queue, ...newProbes].slice(0, MAX_PROBES);
  return state;
}

function currentProbe(state) {
  if (!state.in_probe_mode) return null;
  return state.probe_queue[state.probe_index] || null;
}

function startProbeModeIfNeeded(state) {
  if (state.probe_index < state.probe_queue.length) {
    state.in_probe_mode = true;
    return state.probe_queue[state.probe_index];
  }
  state.in_probe_mode = false;
  return null;
}

function advanceAfterBaseStep(payload, stepKey) {
  let state = initProbeState(payload);

  if (stepKey === 'visit_note' && !state._probes_after_visit) {
    state._probes_after_visit = true;
    state = enqueueProbes({ ...payload, ...state }, 'after_visit_note');
  }

  if (stepKey === 'interest_level' && !state._probes_after_interest) {
    state._probes_after_interest = true;
    state = enqueueProbes({ ...payload, ...state }, 'after_interest_level');
  }

  if (stepKey === 'next_action' && !state._probes_after_next_action) {
    state._probes_after_next_action = true;
    state = enqueueProbes({ ...payload, ...state }, 'after_interest_level');
  }

  const nextProbe = startProbeModeIfNeeded(state);
  return { state, nextProbe };
}

module.exports = {
  MAX_PROBES,
  initProbeState,
  enqueueProbes,
  currentProbe,
  startProbeModeIfNeeded,
  advanceAfterBaseStep,
};
