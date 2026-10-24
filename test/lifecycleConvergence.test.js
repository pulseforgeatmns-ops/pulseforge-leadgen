'use strict';

// Phase A2 canonical lifecycle convergence — always-run coverage.
//
// Verifies (without a database):
//   1. the canonical stage map and legacy write mapping,
//   2. every production disposition has a reviewed canonical stage mapping,
//   3. transition validation guards,
//   4. deriveCanonicalStage precedence over legacy fields,
//   5. the client-side disposition catalog stays in lockstep with the server,
//   6. all lifecycle writers converge on the canonical service (source-level).

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const {
  BLOCKED_TRANSITIONS,
  CANONICAL_STAGES,
  DISPOSITIONS_UNDER_REVIEW,
  DISPOSITION_STAGE_MAP,
  LIFECYCLE_LEGACY_MAP,
  LIFECYCLE_REASONS,
  deriveCanonicalStage,
  dispositionStageEffects,
  validateTransition,
} = require('../services/lifecycleService');
const { DISPOSITION_VALUES } = require('../utils/callDispositions');
const {
  SETTER_QUEUE_DISPLAY_THRESHOLD,
  SETTER_VISIBILITY_THRESHOLD,
  getQueueDisplayThreshold,
} = require('../utils/qualificationThreshold');

const root = path.join(__dirname, '..');
const setterRoute = fs.readFileSync(path.join(root, 'routes', 'setter.js'), 'utf8');
const apiRoute = fs.readFileSync(path.join(root, 'routes', 'api.js'), 'utf8');
const workspaceRoute = fs.readFileSync(path.join(root, 'routes', 'workspace.js'), 'utf8');

test('canonical stage map is exactly the five reviewed stages', () => {
  assert.deepEqual(CANONICAL_STAGES, ['new', 'contacted', 'follow_up', 'booked', 'dead']);
  assert.deepEqual(Object.keys(LIFECYCLE_LEGACY_MAP).sort(), [...CANONICAL_STAGES].sort());
  // Production behavior: setter stage moves never rewrote prospects.status
  // except dead. The mapping layer must preserve that.
  for (const stage of ['new', 'contacted', 'follow_up', 'booked']) {
    assert.equal(LIFECYCLE_LEGACY_MAP[stage].status, null, `${stage} must preserve legacy status`);
    assert.equal(LIFECYCLE_LEGACY_MAP[stage].setter_status, stage);
  }
  assert.deepEqual(LIFECYCLE_LEGACY_MAP.dead, { status: 'dead', setter_status: 'dead' });
});

test('every production disposition has a reviewed canonical mapping', () => {
  assert.deepEqual(
    Object.keys(DISPOSITION_STAGE_MAP).sort(),
    [...DISPOSITION_VALUES].sort(),
    'DISPOSITION_STAGE_MAP must cover exactly the production disposition inventory'
  );
  for (const [disposition, effects] of Object.entries(DISPOSITION_STAGE_MAP)) {
    assert.ok(CANONICAL_STAGES.includes(effects.stage), `${disposition} maps to unknown stage`);
  }
  // Spec-critical rows of the mapping table.
  assert.equal(DISPOSITION_STAGE_MAP.meeting_booked.stage, 'booked');
  assert.equal(DISPOSITION_STAGE_MAP.no_answer.stage, 'contacted');
  assert.equal(DISPOSITION_STAGE_MAP.voicemail.stage, 'contacted');
  assert.equal(DISPOSITION_STAGE_MAP.answered_callback.stage, 'follow_up');
  assert.equal(DISPOSITION_STAGE_MAP.answered_interested.stage, 'follow_up');
  // Phase B: answered_not_interested is NURTURE, not permanent Dead.
  assert.equal(DISPOSITION_STAGE_MAP.answered_not_interested.stage, 'follow_up');
  assert.equal(DISPOSITION_STAGE_MAP.answered_not_interested.statusOverride, 'cold');
  assert.equal(DISPOSITION_STAGE_MAP.answered_not_interested.isHot, false);
  assert.equal(DISPOSITION_STAGE_MAP.answered_not_interested.lifecycleReason, 'nurture');
  // Phase B: wrong_number / disconnected are DATA REMEDIATION — the prospect
  // survives with the phone cleared for repair.
  for (const disposition of ['wrong_number', 'disconnected']) {
    assert.equal(DISPOSITION_STAGE_MAP[disposition].stage, 'follow_up');
    assert.equal(DISPOSITION_STAGE_MAP[disposition].clearPhone, true);
    assert.equal(DISPOSITION_STAGE_MAP[disposition].preserveStatus, true);
    assert.equal(DISPOSITION_STAGE_MAP[disposition].lifecycleReason, 'data_remediation');
    assert.notEqual(DISPOSITION_STAGE_MAP[disposition].suppress, true);
  }
  // Phase B: do_not_call is TERMINAL SUPPRESSION — dead AND do_not_contact.
  assert.equal(DISPOSITION_STAGE_MAP.do_not_call.stage, 'dead');
  assert.equal(DISPOSITION_STAGE_MAP.do_not_call.suppress, true);
  assert.equal(DISPOSITION_STAGE_MAP.do_not_call.lifecycleReason, 'terminal_suppression');
  // disqualified stays permanent Dead but is NOT global suppression.
  assert.equal(DISPOSITION_STAGE_MAP.disqualified.stage, 'dead');
  assert.notEqual(DISPOSITION_STAGE_MAP.disqualified.suppress, true);
  // Structured reason codes are the closed Phase B set.
  assert.deepEqual([...LIFECYCLE_REASONS].sort(), ['data_remediation', 'nurture', 'terminal_suppression']);
  // The Phase A2 review rows are resolved by Phase B product rules, and the
  // resolution record is kept auditable.
  assert.deepEqual(
    Object.keys(DISPOSITIONS_UNDER_REVIEW).sort(),
    ['answered_not_interested', 'disconnected', 'wrong_number']
  );
  for (const note of Object.values(DISPOSITIONS_UNDER_REVIEW)) {
    assert.match(note, /RESOLVED \(Phase B\)/);
  }
  assert.throws(() => dispositionStageEffects('made_up'), /No canonical stage mapping/);
});

test('transition validation blocks booked→new and unknown stages', () => {
  assert.ok(BLOCKED_TRANSITIONS.has('booked>new'));
  assert.throws(() => validateTransition('booked', 'new'), /not allowed/);
  assert.throws(() => validateTransition('new', 'archived'), /Invalid canonical stage/);
  // The permissive production matrix stays reachable.
  validateTransition('new', 'booked');
  validateTransition('dead', 'follow_up');
  validateTransition('booked', 'booked');
});

test('deriveCanonicalStage prefers setter_status, falls back to legacy status', () => {
  assert.equal(deriveCanonicalStage({ setter_status: 'follow_up', status: 'dead' }), 'follow_up');
  assert.equal(deriveCanonicalStage({ setter_status: 'closed', status: null }), 'booked');
  assert.equal(deriveCanonicalStage({ setter_status: null, status: 'dead' }), 'dead');
  assert.equal(deriveCanonicalStage({ setter_status: null, status: 'disqualified' }), 'dead');
  assert.equal(deriveCanonicalStage({ setter_status: null, status: 'closed' }), 'booked');
  assert.equal(deriveCanonicalStage({ setter_status: null, status: 'warm' }), 'contacted');
  assert.equal(deriveCanonicalStage({ setter_status: null, status: 'cold' }), 'new');
  assert.equal(deriveCanonicalStage({}), 'new');
});

test('client-side disposition catalog matches the server mapping', () => {
  const source = fs.readFileSync(path.join(root, 'public', 'shared', 'lifecycle.js'), 'utf8');
  const sandbox = { window: {} };
  vm.createContext(sandbox);
  vm.runInContext(source, sandbox);
  const catalog = sandbox.window.PulseforgeLifecycle;
  assert.deepEqual([...catalog.STAGES], [...CANONICAL_STAGES]);
  assert.deepEqual([...catalog.LIFECYCLE_REASONS].sort(), [...LIFECYCLE_REASONS].sort());
  for (const item of catalog.DISPOSITIONS) {
    assert.ok(DISPOSITION_VALUES.includes(item.value), `client disposition ${item.value} unknown to server`);
    assert.equal(
      item.stage,
      DISPOSITION_STAGE_MAP[item.value].stage,
      `client stage for ${item.value} drifted from the canonical map`
    );
    assert.equal(
      item.lifecycleReason || null,
      DISPOSITION_STAGE_MAP[item.value].lifecycleReason || null,
      `client lifecycle reason for ${item.value} drifted from the canonical map`
    );
  }
  // Every disposition the server maps is offered by the client catalog.
  const clientValues = [...catalog.DISPOSITIONS].map((item) => item.value).sort();
  assert.deepEqual(clientValues, Object.keys(DISPOSITION_STAGE_MAP).sort());
});

test('all lifecycle writers converge on the canonical service (no independent writers)', () => {
  // Setter stage moves, dispositions, and callbacks route through the service.
  const statusBlock = setterRoute.slice(setterRoute.indexOf("'/api/leads/:id/status'"));
  assert.match(statusBlock.slice(0, 4000), /transitionProspectLifecycle\(/);
  const dispositionBlock = setterRoute.slice(setterRoute.indexOf("'/api/leads/:id/call-disposition'"));
  assert.match(dispositionBlock.slice(0, 8000), /transitionProspectLifecycle\(/);
  assert.match(dispositionBlock.slice(0, 8000), /dispositionStageEffects\(/);
  const callbackBlock = setterRoute.slice(setterRoute.indexOf("'/api/leads/:id/callback'"));
  assert.match(callbackBlock.slice(0, 2500), /scheduleProspectCallback\(/);
  // Dashboard dead/disqualified writes converge too.
  assert.match(apiRoute, /transitionProspectLifecycle\(/);
  // Workspace lifecycle endpoint uses the same service and canonical stages.
  assert.match(workspaceRoute, /transitionProspectLifecycle\(/);
  assert.match(workspaceRoute, /CANONICAL_STAGES/);
  // The legacy independent lifecycle writer must be gone from the setter
  // status endpoint: no direct UPDATE of setter_status outside the service.
  assert.doesNotMatch(
    statusBlock.slice(0, 4000),
    /UPDATE prospects\s+SET setter_status/i,
    'setter status endpoint must not write setter_status directly'
  );
});

test('qualification threshold is centralized and documented (70 visibility / 40 queue)', () => {
  assert.equal(SETTER_VISIBILITY_THRESHOLD, 70);
  assert.equal(SETTER_QUEUE_DISPLAY_THRESHOLD, 40);
  // Tenant-configurable with a safe fallback; production default unchanged.
  assert.equal(getQueueDisplayThreshold(null), 40);
  assert.equal(getQueueDisplayThreshold({ setter_qualification_threshold: 70 }), 70);
  assert.equal(getQueueDisplayThreshold({ setter_qualification_threshold: 999 }), 40);
  // Route SQL reads the tenant threshold, not a hardcoded 40.
  assert.match(setterRoute, /setter_qualification_threshold/);
  assert.doesNotMatch(setterRoute, /icp_score,\s*0\)\s*>=\s*40/);
});
