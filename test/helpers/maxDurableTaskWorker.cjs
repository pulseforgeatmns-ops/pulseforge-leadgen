'use strict';

/**
 * MAX-P0-004 E2E helper: a separate process that routes one Canonical
 * Execution Request under durable task ownership and then hangs inside the
 * specialist, so the parent can kill it (SIGKILL) mid-execution and prove that
 * a crashed owner leaves an explicit, recoverable state and no mission write.
 *
 * argv[2] JSON: { connectionString, missionId, taskKey, leaseMs }
 * stdout: "STARTED <taskId> <fenceEpoch>" once the task is running.
 */
const args = JSON.parse(process.argv[2]);
const { Pool } = require('pg');
const amo = require('../../packages/acquisition-mission');
const persistence = require('../../services/acquisitionMissionPersistence');
const { createDurableExecutionOwnership } = require('../../services/maxReliability/durableExecutionOwnership');
const { executeMissionStage } = require('../../packages/acquisition-mission/TransactionalExecution');
const { currentTaskFence } = require('../../services/maxReliability/taskExecutionContext');

(async () => {
  const pool = new Pool({ connectionString: args.connectionString, max: 4 });
  const engine = amo.createAcquisitionMissionEngine();
  const loaded = (await pool.query('SELECT payload FROM acquisition_missions WHERE id = $1', [args.missionId])).rows[0].payload;
  engine.store.putMission(loaded);
  engine.durableBaseline = new Map([[loaded.id, persistence.missionFingerprint(loaded)]]);
  const request = amo.createExecutionRequestFromApi({
    intent: amo.EXECUTION_INTENTS.EDIT_PLAN, missionId: loaded.id, mission: loaded, operatorId: 'worker',
    stage: loaded.stage, objective: loaded.objective, runtimeOwner: amo.resolveMissionRuntimeOwner(loaded),
    permissions: { canExecute: true }, payload: { question: 'crash test' },
  });
  await amo.routeExecutionRequest(request, {
    engine, tenantId: '10', pool,
    durableTasks: createDurableExecutionOwnership(pool),
    durableTaskKey: args.taskKey,
    durableTaskLeaseMs: args.leaseMs,
    handlers: {
      [request.intent]: (ctx) => executeMissionStage({
        engine, missionId: loaded.id, tenantId: '10', pool, specialist: 'operator', stage: loaded.stage,
        validatePreconditions: () => ({}),
        execute: async () => {
          const fence = currentTaskFence();
          process.stdout.write(`STARTED ${fence.taskId} ${fence.fenceEpoch}\n`);
          await new Promise(() => {}); // hang until killed
        },
        commit: async () => ({}),
        persistDurable: persistence.bindStagePersistDurable({ pool, preinitializedSchema: true }, engine, '10'),
      }),
    },
  });
})().catch((err) => {
  process.stderr.write(`WORKER_ERROR ${err.code || ''} ${err.message}\n`);
  process.exit(1);
});
