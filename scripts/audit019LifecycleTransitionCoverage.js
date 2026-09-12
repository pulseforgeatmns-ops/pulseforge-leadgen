'use strict';

/**
 * AUDIT-019 — Lifecycle Transition Coverage
 *
 * Determines whether an inconsistent mission originated from:
 *   (A) stale persisted state before SPEC-137, or
 *   (B) an uncovered lifecycle transition after SPEC-137.
 *
 * Usage:
 *   DATABASE_URL=... node scripts/audit019LifecycleTransitionCoverage.js [--mission-id <id>] [--tenant-id <id>]
 *
 * Without DATABASE_URL, prints code-path analysis only.
 */

const SPEC137_DEPLOYED_AT = '2026-08-22T12:52:50.000Z'; // PR #373 merge
const SPEC136_DEPLOYED_AT = '2026-08-22T03:13:37.000Z'; // PR #372 merge (putMission validation + progress pending clear)

const STALE_PATTERNS = [
  {
    id: 'stage_pending_stage_mismatch',
    test(m) {
      const pending = m.pendingOperatorDecision;
      return Boolean(pending && pending.stage && m.stage && pending.stage !== m.stage);
    },
    message: 'pendingOperatorDecision.stage !== mission.stage',
    origin: 'A',
    transition: 'discover → understand via Engine.progress() before SPEC-136 (stage advanced without clearing pending)',
  },
  {
    id: 'discovery_approval_off_discover',
    test(m) {
      const pending = m.pendingOperatorDecision;
      return pending?.kind === 'discovery_approval' && m.stage && m.stage !== 'discover';
    },
    message: 'discovery_approval advertised outside discover stage',
    origin: 'A',
    transition: 'discover → understand via Engine.progress() before SPEC-136',
  },
  {
    id: 'plan_decision_off_discover',
    test(m) {
      const pending = m.pendingOperatorDecision;
      const planKinds = new Set(['plan_approval', 'plan_edit', 'plan_clarification']);
      return planKinds.has(pending?.kind) && m.stage && m.stage !== 'discover';
    },
    message: 'plan decision advertised outside discover stage',
    origin: 'A',
    transition: 'discover → * via Engine.progress() before SPEC-136',
  },
  {
    id: 'approved_plan_stale_pending',
    test(m) {
      const pending = m.pendingOperatorDecision;
      const planKinds = new Set(['plan_approval', 'plan_edit', 'plan_clarification']);
      return m.structuredMissionApproved === true && planKinds.has(pending?.kind);
    },
    message: 'structuredMissionApproved with stale plan pending',
    origin: 'A',
    transition: 'plan approval commit without atomic pending swap (pre-SPEC-136 persist)',
  },
];

function parseArgs(argv) {
  const out = { missionId: null, tenantId: null };
  for (let i = 2; i < argv.length; i += 1) {
    if (argv[i] === '--mission-id') out.missionId = argv[++i];
    else if (argv[i] === '--tenant-id') out.tenantId = argv[++i];
  }
  return out;
}

function missionFromRow(row) {
  const payload = row.payload && typeof row.payload === 'object' ? row.payload : {};
  return {
    ...payload,
    id: row.id,
    tenantId: row.tenant_id,
    stage: row.stage,
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function classifyMission(mission) {
  const hits = STALE_PATTERNS.filter((p) => p.test(mission));
  if (!hits.length) {
    return { category: 'consistent', hits: [] };
  }
  const createdBeforeSpec137 = mission.createdAt && new Date(mission.createdAt) < new Date(SPEC137_DEPLOYED_AT);
  return {
    category: createdBeforeSpec137 ? 'A_stale_persisted_state' : 'B_uncovered_transition_candidate',
    createdBeforeSpec137,
    hits,
  };
}

async function loadStageTransitions(pool, missionId) {
  const { rows } = await pool.query(
    `SELECT kind, specialist, label, payload, at
     FROM acquisition_mission_events
     WHERE mission_id = $1 AND kind = 'stage_transition'
     ORDER BY at ASC`,
    [missionId]
  );
  return rows;
}

function printCodePathAnalysis() {
  console.log('\n=== AUDIT-019 code-path analysis (no DATABASE_URL) ===\n');
  console.log('SPEC-136 deployed:', SPEC136_DEPLOYED_AT);
  console.log('SPEC-137 deployed:', SPEC137_DEPLOYED_AT);
  console.log('');
  console.log('Post-SPEC-137 stage mutations:');
  console.log('  - packages/acquisition-mission/Lifecycle.js → applyStageTransition() ONLY');
  console.log('  - packages/acquisition-mission/Engine.js → progress() delegates to applyStageTransition()');
  console.log('  - packages/max/workspace/AmoOperatorApproval.js → commitDiscoveryStage() calls engine.progress()');
  console.log('');
  console.log('Uncovered post-SPEC-137 transition path: NONE FOUND.');
  console.log('All production stage writes route through applyStageTransition since PR #373.');
  console.log('');
  console.log('Pre-SPEC-136 stale origin:');
  console.log('  Engine.progress() set stage/status only; pendingOperatorDecision was not cleared.');
  console.log('  putMission validation did not exist until SPEC-136.');
  console.log('  Rows persisted with stage=understand + pendingOperatorDecision.stage=discover.');
  console.log('');
  console.log('First lifecycle transition that produced stale state:');
  console.log('  discover → understand via Engine.progress() (direct API or Max advance), pre-SPEC-136.');
  console.log('  commitDiscoveryStage() cleared pending before progress since SPEC-128 — that path was safe.');
  console.log('');
  console.log('Engine.progress() from SPEC-137 on stale missions:');
  console.log('  No — hydrate/putMission rejects MISSION_STATE_INCONSISTENT before progress can run.');
  console.log('');
  console.log('Conclusion (code analysis): category A — stale persisted state.');
  console.log('Run with DATABASE_URL to attach a specific mission id and event timeline.');
}

async function auditFromDatabase(opts) {
  const pool = require('../db');
  let missions;
  if (opts.missionId) {
    const { rows } = await pool.query(
      `SELECT * FROM acquisition_missions WHERE id = $1`,
      [opts.missionId]
    );
    missions = rows.map(missionFromRow);
  } else if (opts.tenantId) {
    const { rows } = await pool.query(
      `SELECT * FROM acquisition_missions WHERE tenant_id = $1 ORDER BY created_at ASC`,
      [String(opts.tenantId)]
    );
    missions = rows.map(missionFromRow);
  } else {
    const { rows } = await pool.query(
      `SELECT * FROM acquisition_missions ORDER BY created_at ASC`
    );
    missions = rows.map(missionFromRow);
  }

  const inconsistent = missions.filter((m) => classifyMission(m).category !== 'consistent');
  if (!inconsistent.length) {
    console.log('No inconsistent missions found in database.');
    return;
  }

  for (const mission of inconsistent) {
    const verdict = classifyMission(mission);
    const transitions = await loadStageTransitions(pool, mission.id);
    const spec137Progress = transitions.some((row) => {
      const at = row.at && new Date(row.at);
      return at && at >= new Date(SPEC137_DEPLOYED_AT);
    });

    console.log('\n--- Mission', mission.id, '---');
    console.log('Created:', mission.createdAt);
    console.log('Updated:', mission.updatedAt);
    console.log('Stage:', mission.stage);
    console.log('Pending:', JSON.stringify(mission.pendingOperatorDecision, null, 2));
    console.log('Created before SPEC-137:', verdict.createdBeforeSpec137);
    console.log('Verdict:', verdict.category);
    console.log('Patterns:', verdict.hits.map((h) => h.message).join('; '));
    console.log('First offending transition:', verdict.hits[0]?.transition || 'unknown');
    console.log('Stage transitions:', transitions.length);
    console.log('Engine.progress (SPEC-137 era) events after deploy:', spec137Progress);
    console.log('SPEC-137 progress executed for this mission:', spec137Progress ? 'possibly (see events)' : 'no post-SPEC-137 stage_transition events');
  }
}

async function main() {
  const opts = parseArgs(process.argv);
  if (!process.env.DATABASE_URL) {
    printCodePathAnalysis();
    return;
  }
  await auditFromDatabase(opts);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
