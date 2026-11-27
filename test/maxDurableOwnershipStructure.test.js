'use strict';

/**
 * MAX-P0-004 criterion 1 — structural enforcement. Every production call site
 * that can execute a specialist, mutate a mission or send must be classified
 * here with the evidence of how it is owned. A new caller, or a classified
 * caller that loses its ownership wiring, fails CI.
 *
 * Classifications:
 *   task_store         passes a durable task store (and key) to the router
 *   offline            non-durable evaluation under the OFFLINE_EVALUATION exemption
 *   router_handler     runs only as a handler dispatched by the router (fenced there)
 *   task_owned_work    wraps the mutation in runTaskOwnedWork / ownedMissionWork
 *   after_dispatch     persists inside the router's task via afterDispatch
 *   exemption:<reason> an audited, closed write exemption (leased writes only)
 *   definition         the primitive itself
 */
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { ALL_EXEMPTIONS, WRITE_EXEMPTIONS, OFFLINE_EVALUATION } = require('../services/maxReliability/executionOwnershipPolicy');

const ROOT = path.join(__dirname, '..');
const SKIP_DIRS = new Set(['node_modules', 'test', 'tests', 'docs', '.git', 'artifacts', 'sites', 'public', 'eval']);

function sourceFiles(dir = ROOT, out = []) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.name.startsWith('.') && entry.name !== '.') continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) { if (!SKIP_DIRS.has(entry.name)) sourceFiles(full, out); continue; }
    if (/\.(c?js)$/.test(entry.name)) out.push(full);
  }
  return out;
}
const FILES = sourceFiles().map((f) => ({ rel: path.relative(ROOT, f).split(path.sep).join('/'), text: fs.readFileSync(f, 'utf8') }));
function codeLines(text) {
  return text.split('\n').filter((l) => !/^\s*(\/\/|\*|\/\*)/.test(l)).join('\n');
}
function callers(pattern) {
  return FILES.filter((f) => codeLines(f.text).includes(pattern)).map((f) => f.rel).sort();
}
const has = (rel, needle) => FILES.find((f) => f.rel === rel).text.includes(needle);

const ROUTER = {
  'packages/acquisition-mission/ExecutionRouter.js': ['definition'],
  'packages/max/workspace/AcquisitionMissionExecution.js': ['task_store', 'durableTasks: resolveDurableTasks('],
  'services/acquisitionMission.js': ['task_store', 'durableTasks: resolveDurableTasks(opts)'],
  'services/anchorScopedResumeCrmPreparation.js': ['task_store', 'durableTaskKey: `anchor-crm-prep:'],
  'services/governedOutboundAdapters.js': ['task_store', 'durableTasks: resolveDurableTasks('],
  'scripts/executeAnchorOneOutbound.js': ['task_store', "operatorRunKey('executeAnchorOneOutbound'"],
  'scripts/regenerateAnchorCapacityRevision.js': ['task_store', "operatorRunKey('regenerateAnchorCapacityRevision'"],
  'scripts/regenerateAnchorPaigeCopyRevision.js': ['task_store', "operatorRunKey('regenerateAnchorPaigeCopyRevision'"],
  'scripts/audit062WarmRoutingObservation.js': ['offline', 'runWithOwnershipExemption(OFFLINE_EVALUATION'],
};
const TME = {
  'packages/acquisition-mission/TransactionalExecution.js': ['definition'],
  'packages/max/workspace/AmoOperatorApproval.js': ['router_handler'],
  'packages/max/workspace/EmmettOutboundExecution.js': ['router_handler'],
  // Called only from services/acquisitionMission.amendMissionAuthorization (checked below).
  'packages/acquisition-mission/AuthorizationAmendment.js': ['task_owned_work'],
};
const WRITERS = {
  'services/acquisitionMissionPersistence.js': ['definition'],
  'services/acquisitionMissionRuntime.js': ['task_owned_work', 'ownedMissionWork('],
  'packages/max/workspace/AcquisitionOwnership.js': ['exemption:operator_restriction_tightening', "runWithOwnershipExemption('operator_restriction_tightening'"],
  // Inbound events persist through runtime.persistInbound: an audited
  // inbound_observation write, or task-owned work if they advanced the mission.
  'services/acquisitionMissionProviderObservation.js': ['inbound', 'runtime.persistInbound('],
  'services/acquisitionMissionRileyInterpretation.js': ['inbound', 'runtime.persistInbound('],
  'services/governedOutboundAdapters.js': ['task_owned_work', 'logicalKey: `governed-complete:'],
  'routes/acquisitionMissions.js': ['task_owned_work', 'logicalKey: `outcome-learning:'],
  'services/acquisitionMission.js': ['after_dispatch', 'afterDispatch: intent === amo.EXECUTION_INTENTS.CANCEL_PLAN'],
  'scripts/auditAnchorCapacitySupersession.js': ['task_owned_work', "operatorRunKey('auditAnchorCapacitySupersession'"],
};
const SENDS = {
  'packages/max/workspace/OutboundExecutionAdapter.js': ['definition'],
  'packages/max/workspace/EmmettOutboundExecution.js': ['router_handler'],
  'services/maxReliability/guardedDispatch.js': ['definition'],
};

function checkInventory(name, pattern, manifest) {
  const found = callers(pattern);
  const unknown = found.filter((f) => !manifest[f]);
  assert.deepEqual(unknown, [], `${name}: unclassified caller(s) of ${pattern} — classify them in test/maxDurableOwnershipStructure.test.js with how they are task-owned`);
  for (const [file, [kind, evidence]] of Object.entries(manifest)) {
    if (!found.includes(file)) continue; // a removed caller is fine
    if (evidence) assert.ok(has(file, evidence), `${name}: ${file} is classified ${kind} but lost its wiring (${evidence})`);
  }
  return found;
}

test('every router caller passes a task store with a stable key (or is offline evaluation)', () => {
  const found = checkInventory('router', 'routeExecutionRequest(', ROUTER);
  assert.ok(found.length >= 5, 'inventory sanity');
});

test('every TME caller is a router handler or task-owned work', () => {
  checkInventory('tme', 'executeMissionStage(', TME);
  assert.ok(has('services/acquisitionMission.js', 'logicalKey: `amendment:'), 'amendments run as task-owned work keyed by amendmentId');
});

test('every mission writer is task-owned, after-dispatch, or under a closed audited exemption', () => {
  checkInventory('writer:persistMissionState', 'persistMissionState(', WRITERS);
  checkInventory('writer:persistStageCommit', 'persistStageCommit(', WRITERS);
  for (const kind of ['contribute', 'progress', 'autonomous', 'side-effects']) {
    assert.ok(has('services/acquisitionMissionRuntime.js', `ownedMissionWork('${kind}'`), `runtime ${kind} is task-owned`);
  }
  assert.ok(has('services/acquisitionMissionRuntime.js', "runWithOwnershipExemption('mission_creation'"), 'creation uses its exemption');
  assert.ok(has('services/acquisitionMissionRuntime.js', "this.ownedMissionWork('inbound'"), 'an inbound event that advanced the mission is task-owned');
});

test('every send path is the fenced adapter, a router handler or the ledger primitive', () => {
  checkInventory('send:executeOutboundBundle', 'executeOutboundBundle(', SENDS);
  checkInventory('send:guardedDispatch', 'guardedDispatch(', SENDS);
});

test('exemptions are a closed allowlist, used only where classified, and none covers a send or a stage', () => {
  const uses = [];
  for (const f of FILES) {
    for (const m of codeLines(f.text).matchAll(/runWithOwnershipExemption\(\s*'([a-z_]+)'/g)) uses.push([f.rel, m[1]]);
    for (const m of codeLines(f.text).matchAll(/runWithOwnershipExemption\(\s*OFFLINE_EVALUATION/g)) { void m; uses.push([f.rel, OFFLINE_EVALUATION]); }
  }
  const allowedSites = {
    mission_creation: ['services/acquisitionMissionRuntime.js'],
    operator_restriction_tightening: ['packages/max/workspace/AcquisitionOwnership.js'],
    inbound_observation: ['services/acquisitionMissionRuntime.js'],
    [OFFLINE_EVALUATION]: ['scripts/audit062WarmRoutingObservation.js'],
  };
  for (const [file, reason] of uses) {
    assert.ok(ALL_EXEMPTIONS.includes(reason), `${file}: unknown exemption ${reason}`);
    assert.ok((allowedSites[reason] || []).includes(file), `${file}: exemption ${reason} is not permitted here`);
  }
  assert.deepEqual(Object.keys(WRITE_EXEMPTIONS).sort(), ['inbound_observation', 'mission_creation', 'operator_restriction_tightening']);
  // The send adapter and TME admit no write exemption.
  assert.ok(!has('packages/max/workspace/OutboundExecutionAdapter.js', 'isWriteExempt'));
  assert.ok(!has('packages/acquisition-mission/TransactionalExecution.js', 'isWriteExempt'));
});
