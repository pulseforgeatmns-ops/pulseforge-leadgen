'use strict';

// Pulseforge shared lifecycle vocabulary (Phase A2).
// One client-side source of truth for canonical stages, labels, colors, and
// the disposition catalog — mirrors services/lifecycleService.js.

(function () {
  const STAGES = ['new', 'contacted', 'follow_up', 'booked', 'dead'];

  const STAGE_LABELS = {
    new: 'New',
    contacted: 'Contacted',
    follow_up: 'Follow-up',
    booked: 'Booked',
    dead: 'Dead',
  };

  // Structured lifecycle reasons (Phase B, additive) — mirror
  // services/lifecycleService.js LIFECYCLE_REASONS.
  const LIFECYCLE_REASONS = ['nurture', 'data_remediation', 'terminal_suppression'];

  const LIFECYCLE_REASON_LABELS = {
    nurture: 'Nurture',
    data_remediation: 'Needs new contact info',
    terminal_suppression: 'Do not call',
  };

  const DISPOSITIONS = [
    { value: 'no_answer', label: 'No answer', stage: 'contacted' },
    { value: 'voicemail', label: 'Voicemail left', stage: 'contacted' },
    { value: 'gatekeeper_relayed', label: 'Gatekeeper relayed', stage: 'follow_up' },
    { value: 'gatekeeper_blocked', label: 'Gatekeeper blocked', stage: 'follow_up' },
    { value: 'answered_callback', label: 'Callback requested', stage: 'follow_up', needsNotes: true },
    { value: 'answered_interested', label: 'Interested', stage: 'follow_up', needsNotes: true },
    { value: 'qualified', label: 'Qualified', stage: 'follow_up', needsNotes: true },
    { value: 'meeting_booked', label: 'Meeting booked', stage: 'booked', needsNotes: true },
    { value: 'incumbent_all_set', label: 'All set with current vendor', stage: 'follow_up', lifecycleReason: 'nurture' },
    { value: 'answered_not_interested', label: 'Not interested', stage: 'follow_up', needsNotes: true, lifecycleReason: 'nurture' },
    { value: 'disqualified', label: 'Disqualified', stage: 'dead', needsNotes: true },
    { value: 'wrong_number', label: 'Wrong number', stage: 'follow_up', lifecycleReason: 'data_remediation' },
    { value: 'disconnected', label: 'Disconnected', stage: 'follow_up', lifecycleReason: 'data_remediation' },
    { value: 'do_not_call', label: 'Do not call', stage: 'dead', needsNotes: true, lifecycleReason: 'terminal_suppression' },
  ];

  function stageLabel(stage) {
    return STAGE_LABELS[stage] || String(stage || 'Unknown');
  }

  function stageChip(stage) {
    const safe = STAGES.includes(stage) ? stage : 'new';
    return `<span class="pf-stage-chip pf-stage-${safe}">${stageLabel(safe)}</span>`;
  }

  function lifecycleReasonLabel(reason) {
    return LIFECYCLE_REASON_LABELS[reason] || null;
  }

  window.PulseforgeLifecycle = {
    DISPOSITIONS,
    LIFECYCLE_REASONS,
    LIFECYCLE_REASON_LABELS,
    STAGES,
    STAGE_LABELS,
    lifecycleReasonLabel,
    stageChip,
    stageLabel,
  };
})();
