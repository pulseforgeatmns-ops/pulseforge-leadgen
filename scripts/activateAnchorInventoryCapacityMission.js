'use strict';

/**
 * Creates an immutable successor to Anchor's current acquisition mission and
 * activates it only after the current prepared envelope has drained.
 *
 * Review: node scripts/activateAnchorInventoryCapacityMission.js
 * Apply:  node scripts/activateAnchorInventoryCapacityMission.js --apply --confirm=anchor-inventory-capacity-2026-10-08
 */

require('dotenv').config();
const pool = require('../db');
const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');

const TENANT_ID = '10';
const CONFIRMATION = 'anchor-inventory-capacity-2026-10-08';

function successorMission(row) {
  const now = new Date().toISOString();
  const payload = structuredClone(row.payload || {});
  const structured = payload.structuredMission || {};
  const market = structured.market || {};
  const eligible = [...new Set([...(market.eligibleSubsegments || []), 'restaurant_foh'])];
  const constraints = [...new Set([
    ...(Array.isArray(payload.constraints)
      ? payload.constraints.filter(value => !/restaurants?\s+(?:are\s+)?excluded/i.test(String(value)))
      : []),
    'Manchester restaurant acquisition is front-of-house only.',
    'Back-of-house, commissary, ghost-kitchen, food-production, and catering-only work remains outside standard scope.',
  ])];
  const id = `mission_anchor_inventory_capacity_${hash([row.id, eligible, '2026-10-08']).slice(0, 20)}`;
  const objective = `${row.objective} Maintain enough qualified inventory for governed outbound and AO books; include bounded Manchester restaurant front-of-house acquisition.`;
  return {
    id,
    tenantId: TENANT_ID,
    clientId: row.client_id,
    stage: row.stage,
    status: row.status,
    objective,
    targetSegment: row.target_segment,
    campaign: row.campaign,
    title: `${row.title || 'Anchor acquisition'} — inventory capacity`,
    priority: row.priority,
    confidence: row.confidence,
    owner: row.owner,
    createdBy: 'operator_authorized_inventory_capacity_recovery',
    orchestrationMissionId: row.orchestration_mission_id,
    structuredMission: {
      ...structured,
      market: { ...market, eligibleSubsegments: eligible },
      immutable: true,
    },
    constraints,
    predecessorMissionId: row.id,
    authorizationEvidence: {
      kind: 'operator_instruction',
      receivedAt: now,
      scope: 'Anchor inventory capacity; Manchester restaurant front-of-house only; BOH separate/premium',
    },
    createdAt: now,
    updatedAt: now,
  };
}

async function plan(db = pool) {
  const program = (await db.query(`SELECT * FROM acquisition_outbound_programs
    WHERE tenant_id=$1 AND mode='active' ORDER BY authorized_at DESC LIMIT 1`, [TENANT_ID])).rows[0];
  if (!program) throw new Error('active_anchor_program_not_found');
  const source = (await db.query('SELECT * FROM acquisition_missions WHERE tenant_id=$1 AND id=$2', [TENANT_ID, program.source_mission_id])).rows[0];
  if (!source) throw new Error('anchor_source_mission_not_found');
  const pending = (await db.query(`SELECT count(*)::int AS n FROM acquisition_outbound_items i
    JOIN acquisition_outbound_envelopes e ON e.id=i.envelope_id
    WHERE i.tenant_id=$1 AND e.program_id=$2 AND i.status IN ('pending','attempted','uncertain')`, [TENANT_ID, program.id])).rows[0]?.n || 0;
  const mission = successorMission(source);
  const policy = { ...(program.policy || {}), sourceMissionId: mission.id };
  return {
    program,
    source,
    mission,
    policy,
    pending,
    policyHash: hash(policy),
    scopeHash: hash(missionScope(mission)),
  };
}

async function apply(db = pool) {
  const review = await plan(db);
  if (review.pending > 0) throw new Error(`prepared_envelope_not_drained:${review.pending}`);
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    await client.query(`INSERT INTO acquisition_missions (
      id,tenant_id,client_id,stage,status,objective,target_segment,campaign,title,priority,
      confidence,owner,created_by,orchestration_mission_id,payload,created_at,updated_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16,$17)
    ON CONFLICT (id) DO NOTHING`, [
      review.mission.id, TENANT_ID, review.mission.clientId, review.mission.stage,
      review.mission.status, review.mission.objective, review.mission.targetSegment,
      review.mission.campaign, review.mission.title, review.mission.priority,
      review.mission.confidence, review.mission.owner, review.mission.createdBy,
      review.mission.orchestrationMissionId, JSON.stringify(review.mission),
      review.mission.createdAt, review.mission.updatedAt,
    ]);
    const updated = await client.query(`UPDATE acquisition_outbound_programs
      SET source_mission_id=$2, policy=$3::jsonb, policy_hash=$4, scope_hash=$5, authorized_at=NOW()
      WHERE id=$1 AND tenant_id=$6 AND source_mission_id=$7 RETURNING id`, [
      review.program.id, review.mission.id, JSON.stringify(review.policy), review.policyHash,
      review.scopeHash, TENANT_ID, review.source.id,
    ]);
    if (updated.rowCount !== 1) throw new Error('anchor_program_changed_during_activation');
    await client.query(`INSERT INTO acquisition_outbound_events
      (id,tenant_id,program_id,event_type,payload,created_at)
      VALUES ($1,$2,$3,'source_mission_succeeded',$4::jsonb,NOW()) ON CONFLICT DO NOTHING`, [
      hash(['source_mission_succeeded', review.program.id, review.mission.id]), TENANT_ID, review.program.id,
      JSON.stringify({ priorMissionId: review.source.id, sourceMissionId: review.mission.id,
        policyHash: review.policyHash, scopeHash: review.scopeHash }),
    ]);
    await client.query('COMMIT');
    return { activated: true, sourceMissionId: review.mission.id, pending: 0 };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

if (require.main === module) {
  const doApply = process.argv.includes('--apply');
  const confirm = process.argv.find(arg => arg.startsWith('--confirm='))?.split('=')[1];
  if (doApply && confirm !== CONFIRMATION) throw new Error(`Refusing apply without --confirm=${CONFIRMATION}`);
  (doApply ? apply(pool) : plan(pool).then(row => ({
    apply: false,
    currentMissionId: row.source.id,
    successorMissionId: row.mission.id,
    eligibleSubsegments: row.mission.structuredMission.market.eligibleSubsegments,
    pending: row.pending,
    policyHash: row.policyHash,
    scopeHash: row.scopeHash,
  })))
    .then(result => console.log(JSON.stringify(result, null, 2)))
    .catch(err => { console.error(err.message || err); process.exitCode = 1; })
    .finally(() => pool.end().catch(() => {}));
}

module.exports = { successorMission, plan, apply };
