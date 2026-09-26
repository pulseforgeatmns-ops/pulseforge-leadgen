#!/usr/bin/env node
'use strict';

/**
 * AUDIT-062 runtime observation.
 * Instruments whether Warm Routing is loaded or invoked on a production
 * AMO discovery path, then measures the Warm Routing disabled-path cost.
 */

const Module = require('module');
const { performance } = require('perf_hooks');

const loadedModules = [];
const originalLoad = Module._load;
Module._load = function audit062Load(request, parent, isMain) {
  if (request === 'dotenv') {
    return { config() { return {}; } };
  }
  if (request === 'axios') {
    return {
      async post() { return { data: { result: { message_id: 1 } } }; },
      async get() { return { data: {} }; },
    };
  }
  const resolved = (() => {
    try {
      return Module._resolveFilename(request, parent, isMain);
    } catch (_err) {
      return request;
    }
  })();
  if (/warmRoutingAgent/.test(String(resolved)) || /warmRoutingAgent/.test(String(request))) {
    loadedModules.push({ request, resolved, parent: parent && parent.filename });
  }
  return originalLoad.apply(this, arguments);
};

function wrapExports(modPath, names) {
  const abs = require.resolve(modPath);
  const exported = require(abs);
  const calls = [];
  for (const name of names) {
    if (typeof exported[name] !== 'function') continue;
    const original = exported[name];
    exported[name] = function wrapped(...args) {
      calls.push({ name, at: new Date().toISOString() });
      return original.apply(this, args);
    };
  }
  return { exported, calls };
}

async function observeAcquisitionMission() {
  const amo = require('../packages/acquisition-mission');
  const Scout = require('../packages/scout');
  const {
    EXECUTION_INTENTS,
    createExecutionRequestFromChat,
    routeExecutionRequest,
    clearExecutionRouterAudit,
    listExecutionRouterAudit,
  } = amo;
  const { advancePlanAfterApproval, advanceDiscoveryAfterApproval } = require('../packages/max/workspace/AmoOperatorApproval');

  clearExecutionRouterAudit();

  const objective =
    'Acquire one recurring commercial cleaning client from a short-term rental operator in Hooksett and Auburn.';

  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({
    tenantId: '10',
    objective,
    targetSegment: 'Short-Term Rental Operators',
  });

  const t0 = performance.now();
  const planResult = await advancePlanAfterApproval({
    engine,
    mission,
    tenantId: '10',
    question: 'Approved. Proceed with this plan.',
  });

  const request = createExecutionRequestFromChat({
    intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
    missionId: mission.id,
    operatorId: 'audit-062',
    objective,
  });

  const routed = await routeExecutionRequest(request, {
    engine,
    tenantId: '10',
    operatorId: 'audit-062',
    allowFixtureFallback: true,
    persist: false,
  });
  const routerMs = performance.now() - t0;

  const scoutStarted = performance.now();
  const scoutResult = await Scout.discover({
    mission: {
      id: mission.id,
      tenantId: '10',
      clientId: '10',
      objective,
      structuredMissionApproved: true,
      stage: 'discover',
    },
    missionEngine: null,
    scoutPayload: {},
    opts: {
      runtimeOwner: 'amo',
      amoMissionId: mission.id,
      attachScoutDiscovery: false,
      allowFixtureFallback: true,
      persistMemory: false,
      enablePlaces: false,
      discoveryAdapters: [],
      delegation: {
        tenantId: '10',
        authority: 'observe',
        businessContext: { operatorDirection: objective, missionObjectiveImmutable: true },
        targetContext: { missionBound: true, segments: ['short_term_rental'], geography: 'Hooksett and Auburn' },
      },
    },
  });
  const scoutMs = performance.now() - scoutStarted;

  const directApproval = await advanceDiscoveryAfterApproval({
    engine,
    mission: planResult.snapshot.mission,
    tenantId: '10',
    question: 'Approved. Begin Discovery.',
    allowFixtureFallback: true,
    persist: false,
  });

  return {
    missionId: mission.id,
    router: {
      specialist: routed.specialist,
      action: routed.action,
      runtimeOwner: routed.runtimeOwner,
      outcome: routed.audit && routed.audit.outcome,
      executionOutcome: routed.executionResult && routed.executionResult.executionOutcome,
      ms: Math.round(routerMs),
    },
    scout: {
      outcome: scoutResult.outcome || (scoutResult.discoveryReport && scoutResult.discoveryReport.outcome) || null,
      keys: Object.keys(scoutResult || {}),
      stages: (scoutResult.stages || (scoutResult.pipeline && scoutResult.pipeline.stages) || []).map((row) => row.stage || row),
      hasMarketDefinition: Boolean(scoutResult.marketDefinition || (scoutResult.pipeline && scoutResult.pipeline.marketDefinition)),
      hasInvestigationPlan: Boolean(scoutResult.investigationPlan || (scoutResult.pipeline && scoutResult.pipeline.investigationPlan)),
      ms: Math.round(scoutMs),
    },
    directApproval: {
      executionOutcome: directApproval.executionOutcome,
      specialist: directApproval.discovery && directApproval.discovery.specialist,
    },
    routerAudit: listExecutionRouterAudit({ missionId: mission.id }).map((row) => ({
      intent: row.intent,
      specialist: row.specialist,
      outcome: row.outcome,
    })),
    warmRoutingLoadedDuringMission: loadedModules.slice(),
  };
}

async function observeWarmRoutingDisabledPath() {
  const queries = [];
  const dbPath = require.resolve('../db');
  require.cache[dbPath] = {
    id: dbPath,
    filename: dbPath,
    loaded: true,
    exports: {
      async query(sql, params = []) {
        queries.push({ sql, params });
        return { rows: [] };
      },
    },
  };

  delete require.cache[require.resolve('../warmRoutingAgent')];
  require('../warmRoutingAgent');
  const wrapped = wrapExports('../warmRoutingAgent', [
    'run',
    'isWarmRoutingEnabled',
    'startWarmRoutingScheduler',
    'processProspectEvents',
    'evaluateWarmTriggers',
    'handleWarmTelegramCallback',
  ]);

  delete process.env.WARM_ROUTING_ENABLED;
  const t0 = performance.now();
  const disabled = await wrapped.exported.run({ client_id: 1 });
  const disabledMs = performance.now() - t0;
  const queriesAfterDisabled = queries.length;

  process.env.WARM_ROUTING_ENABLED = 'true';
  const scheduler = wrapped.exported.startWarmRoutingScheduler({ intervalMs: 60 * 60 * 1000 });
  if (scheduler && typeof scheduler.unref === 'function') scheduler.unref();
  if (scheduler && typeof scheduler.close === 'function') scheduler.close();
  else if (scheduler) clearInterval(scheduler);

  process.env.WARM_ROUTING_ENABLED = 'true';
  const seededGateStarted = performance.now();
  const enabledUnseeded = await wrapped.exported.run({ client_id: 1 });
  const enabledUnseededMs = performance.now() - seededGateStarted;

  return {
    disabled,
    disabledMs: Number(disabledMs.toFixed(3)),
    enabledUnseeded,
    enabledUnseededMs: Number(enabledUnseededMs.toFixed(3)),
    dbQueriesOnDisabledPath: queriesAfterDisabled,
    dbQueries: queries.map((row) => String(row.sql).replace(/\s+/g, ' ').trim().slice(0, 120)),
    wrappedCalls: wrapped.calls,
    llmImports: ['anthropic', 'openai', '@anthropic-ai/sdk'].filter((name) => {
      try {
        require.resolve(name);
        return Boolean(require.cache[require.resolve(name)]);
      } catch (_err) {
        return false;
      }
    }),
    agentHasAnthropicRequire: /anthropic|openai|claude/i.test(
      require('fs').readFileSync(require.resolve('../warmRoutingAgent'), 'utf8')
    ),
  };
}

(async () => {
  const mission = await observeAcquisitionMission();
  let routing = null;
  let routingError = null;
  try {
    routing = await observeWarmRoutingDisabledPath();
  } catch (err) {
    routingError = err.stack || err.message;
  }
  const report = {
    audit: 'AUDIT-062',
    at: new Date().toISOString(),
    env: {
      WARM_ROUTING_ENABLED: process.env.WARM_ROUTING_ENABLED || null,
      DATABASE_URL: Boolean(process.env.DATABASE_URL),
    },
    mission,
    routing,
    routingError,
    warmRoutingModuleLoads: loadedModules,
  };
  const outPath = '/tmp/audit062-report.json';
  require('fs').writeFileSync(outPath, JSON.stringify(report, null, 2));
  process.stderr.write(`wrote ${outPath}\n`);
})().catch((err) => {
  console.error(err.stack || err.message);
  process.exit(1);
});
