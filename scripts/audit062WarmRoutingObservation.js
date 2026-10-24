#!/usr/bin/env node
'use strict';

/**
 * AUDIT-062 offline observation, not a production diagnostic or benchmark.
 * Real AMO routing/approval uses injected Scout results; direct Scout discovery
 * uses an empty repository fixture and no providers. Only Warm Routing's two
 * early gates are exercised. Never starts its scheduler or changes its wiring.
 * Run in a dedicated Node process, never inside a running application server.
 */
const Module = require('node:module');
const path = require('node:path');
const fs = require('node:fs');
const { performance } = require('node:perf_hooks');

const ROOT = path.resolve(__dirname, '..');
const TENANT_ID = '9062'; // synthetic, not a production tenant
let sandboxActive = false;

/** All application imports happen inside this scope, after I/O isolation. */
async function withOfflineSandbox(observe) {
  if (sandboxActive) throw new Error('AUDIT-062 requires exclusive process use');
  sandboxActive = true;
  const originalEnv = process.env; // retain the reference; never enumerate/read credentials
  const originalLoad = Module._load;
  const originalCache = Module._cache;
  const originalChildren = module.children.slice();
  const restore = [];
  const state = { blocked: [], queries: [], loadedModules: [], fixtureCalls: [], allowSeedQuery: false };
  const deny = name => function () {
    state.blocked.push(name);
    throw new Error(`AUDIT-062 prohibited I/O: ${name}`);
  };
  const replace = (object, key, value) => {
    const descriptor = Object.getOwnPropertyDescriptor(object, key);
    Object.defineProperty(object, key, { configurable: true, writable: true, value });
    restore.push(() => descriptor
      ? Object.defineProperty(object, key, descriptor)
      : delete object[key]);
  };
  const database = {
    async query(sql, params = []) {
      const normalized = String(sql).replace(/\s+/g, ' ').trim();
      if (!state.allowSeedQuery || normalized !== 'SELECT 1 FROM warm_routing_control WHERE client_id = $1 AND seed_version = $2 AND seeded_at IS NOT NULL LIMIT 1'
        || String(params[0]) !== TENANT_ID || params[1] !== '2026-07-04-edge-v1') {
        return deny('unapproved database query')();
      }
      state.queries.push({ sql: normalized, params });
      return { rows: [] }; // explicit empty seed fixture, never a real connection
    },
    connect: deny('database connection'),
    end: deny('database end'),
  };
  try {
    process.env = { NODE_ENV: 'test', ACTIVE_CLIENT_ID: TENANT_ID };
    Module._cache = Object.create(null); // don't reuse or mutate ambient application modules
    for (const protocol of ['http', 'https']) {
      const transport = require(`node:${protocol}`);
      for (const method of ['request', 'get']) replace(transport, method, deny(`${protocol}.${method}`));
    }
    replace(require('node:net').Socket.prototype, 'connect', deny('net.Socket.connect'));
    replace(require('node:tls'), 'connect', deny('tls.connect'));
    replace(require('node:http2'), 'connect', deny('http2.connect'));
    const datagram = require('node:dgram').Socket.prototype;
    for (const method of ['connect', 'send']) replace(datagram, method, deny(`dgram.${method}`));
    const dns = require('node:dns');
    for (const object of [dns, dns.promises]) {
      for (const name of Object.keys(object).filter(name => /^(lookup|resolve|reverse)/.test(name))) {
        if (typeof object[name] === 'function') replace(object, name, deny(`dns.${name}`));
      }
      for (const name of Object.getOwnPropertyNames(object.Resolver.prototype).filter(name => /^(resolve|reverse)/.test(name))) {
        replace(object.Resolver.prototype, name, deny(`dns.Resolver.${name}`));
      }
    }
    for (const method of ['fetch', 'WebSocket', 'EventSource', 'setInterval']) {
      replace(globalThis, method, deny(method));
    }
    replace(require('node:timers'), 'setInterval', deny('timers.setInterval'));
    const subprocess = require('node:child_process');
    for (const method of ['exec', 'execSync', 'execFile', 'execFileSync', 'spawn', 'spawnSync', 'fork']) {
      replace(subprocess, method, deny(`child_process.${method}`));
    }
    replace(require('node:worker_threads'), 'Worker', deny('worker_threads.Worker'));

    // Imports may read source/package metadata, not .env, credential files or
    // arbitrary local data. No file writes are needed until after cleanup.
    const dependencyRoot = fs.realpathSync(path.join(ROOT, 'node_modules'));
    const sourceRead = file => {
      if (typeof file !== 'string') return deny('non-path file read')();
      const absolute = path.resolve(file);
      const inSource = absolute.startsWith(`${ROOT}${path.sep}`)
        || absolute.startsWith(`${dependencyRoot}${path.sep}`);
      const sourceName = /\.(?:c?js|mjs)$/.test(absolute) || path.basename(absolute) === 'package.json';
      if (!inSource || !sourceName) deny('non-source file read')();
    };
    for (const [object, methods] of [
      [fs, ['readFileSync', 'readFile', 'createReadStream']],
      [fs.promises, ['readFile']],
    ]) {
      for (const name of methods) {
        const original = object[name];
        replace(object, name, function (file, ...args) {
          sourceRead(file);
          return original.call(this, file, ...args);
        });
      }
    }
    for (const [object, methods] of [
      [fs, ['open', 'openSync']], [fs.promises, ['open']],
    ]) {
      for (const name of methods) {
        const original = object[name];
        replace(object, name, function (file, flags, ...args) {
          sourceRead(file);
          if (flags !== 'r' && flags !== 0) return deny('file write open')();
          return original.call(this, file, flags, ...args);
        });
      }
    }
    for (const object of [fs, fs.promises]) {
      for (const name of ['writeFile', 'appendFile', 'unlink', 'rm', 'rmdir', 'mkdir', 'rename', 'truncate', 'copyFile', 'createWriteStream']) {
        for (const method of [name, `${name}Sync`]) {
          if (typeof object[method] === 'function') replace(object, method, deny(`fs.${method}`));
        }
      }
    }

    Module._load = function offlineLoad(request, parent, isMain) {
      if (/^dotenv(?:\/|$)/.test(request)) return { config: () => ({ parsed: {} }) };
      if (/^(?:pg|pg-native)(?:\/|$)/.test(request)) {
        return { Pool: deny('pg.Pool'), Client: deny('pg.Client') };
      }
      // Do not initialize provider SDKs or allow their constructors to read env.
      if (/^(?:axios|node-fetch|undici|got|openai|anthropic|@anthropic-ai\/sdk)(?:\/|$)/.test(request)) {
        const blockedProvider = deny(`provider ${request}`);
        return new Proxy(blockedProvider, { get: () => blockedProvider });
      }
      const resolved = Module._resolveFilename(request, parent, isMain);
      if (resolved === path.join(ROOT, 'db.js')) return database;
      if (resolved === path.join(ROOT, 'warmRoutingAgent.js')) {
        state.loadedModules.push({ request, parent: parent && path.relative(ROOT, parent.filename) });
      }
      return originalLoad.apply(this, arguments);
    };
    const result = await observe(state);
    if (state.blocked.length) throw new Error(`AUDIT-062 blocked unexpected I/O: ${state.blocked.join(', ')}`);
    return result;
  } finally {
    Module._load = originalLoad;
    Module._cache = originalCache;
    module.children = originalChildren;
    process.env = originalEnv;
    for (const undo of restore.reverse()) undo();
    sandboxActive = false;
  }
}

async function observeAcquisitionMission(state) {
  const amo = require('../packages/acquisition-mission');
  const Scout = require('../packages/scout');
  const approval = require('../packages/max/workspace/AmoOperatorApproval');
  const { createAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
  const runtime = createAcquisitionMissionRuntime({ persist: false, pool: null, production: false });
  const engine = runtime.engine();
  const objective = 'Acquire one recurring commercial cleaning client from a short-term rental operator in Hooksett and Auburn.';
  const fixtureScout = label => async () => {
    state.fixtureCalls.push(label);
    return approval.fixtureScoutDiscoveryResult();
  };
  const newApprovedMission = async () => {
    const mission = engine.create({ tenantId: TENANT_ID, objective, targetSegment: 'Short-Term Rental Operators' });
    const plan = await approval.advancePlanAfterApproval({
      engine, mission, tenantId: TENANT_ID, question: 'Approved. Proceed with this plan.',
      persist: false, pool: null,
    });
    return plan.snapshot.mission;
  };
  const mission = await newApprovedMission();
  const request = amo.createExecutionRequestFromChat({
    intent: amo.EXECUTION_INTENTS.APPROVE_DISCOVERY, missionId: mission.id,
    operatorId: 'offline-audit-062', objective,
  });
  const routerStarted = performance.now();
  const routed = await amo.routeExecutionRequest(request, {
    engine, tenantId: TENANT_ID, operatorId: 'offline-audit-062',
    runScout: fixtureScout('router'), persist: false, pool: null,
  });
  const routerMs = performance.now() - routerStarted;

  const scoutStarted = performance.now();
  const scoutResult = await Scout.discover({
    mission: { ...mission, clientId: TENANT_ID }, missionEngine: null, scoutPayload: {},
    opts: {
      runtimeOwner: 'amo', amoMissionId: mission.id, attachScoutDiscovery: false,
      persistMemory: false, enablePlaces: false, discoveryAdapters: [],
      companies: [], people: [],
      loadCompanies: async () => {
        state.fixtureCalls.push('direct_scout_repository');
        return { companies: [], people: [] };
      },
      delegation: {
        tenantId: TENANT_ID, authority: 'observe',
        businessContext: { operatorDirection: objective, missionObjectiveImmutable: true },
        targetContext: { missionBound: true, segments: ['short_term_rental'], geography: 'Hooksett and Auburn' },
      },
    },
  });
  const scoutMs = performance.now() - scoutStarted;
  // Separate mission: reusing the router's mission would take an already-executed
  // shortcut and would not actually exercise the direct-approval fixture.
  const directMission = await newApprovedMission();
  const directApproval = await approval.advanceDiscoveryAfterApproval({
    engine, mission: directMission, tenantId: TENANT_ID, question: 'Approved. Begin Discovery.',
    runScout: fixtureScout('direct_approval'), persist: false, pool: null,
  });
  const expected = ['router', 'direct_scout_repository', 'direct_approval'];
  if (JSON.stringify(state.fixtureCalls) !== JSON.stringify(expected)) {
    throw new Error('AUDIT-062 did not exercise every required fixture boundary');
  }
  return {
    missionId: mission.id, directApprovalMissionId: directMission.id,
    router: {
      specialist: routed.specialist, action: routed.action, runtimeOwner: routed.runtimeOwner,
      outcome: routed.audit && routed.audit.outcome,
      executionOutcome: routed.executionResult && routed.executionResult.executionOutcome,
      fixture: 'injected Scout result, not live Scout/provider execution', ms: Math.round(routerMs),
    },
    scout: {
      outcome: scoutResult.outcome, fixture: 'empty repository; no discovery providers configured',
      stages: (scoutResult.pipeline?.stages || []).map(row => row.stage || row),
      hasMarketDefinition: Boolean(scoutResult.marketDefinition || scoutResult.pipeline?.marketDefinition),
      ms: Math.round(scoutMs),
    },
    directApproval: {
      executionOutcome: directApproval.executionOutcome,
      specialist: directApproval.discovery && directApproval.discovery.specialist,
      fixture: 'injected Scout result, not live Scout/provider execution',
    },
    routerAudit: amo.listExecutionRouterAudit({ missionId: mission.id }).map(row => ({
      intent: row.intent, specialist: row.specialist, outcome: row.outcome,
    })),
    warmRoutingLoadedDuringMission: state.loadedModules.slice(),
  };
}

async function observeWarmRoutingDisabledPath(state) {
  const agent = require('../warmRoutingAgent');
  const disabledStarted = performance.now();
  const disabled = await agent.run({ client_id: Number(TENANT_ID) });
  const disabledMs = performance.now() - disabledStarted;
  const dbQueriesOnDisabledPath = state.queries.length;
  process.env.WARM_ROUTING_ENABLED = 'true'; // isolated environment, restored in finally
  state.allowSeedQuery = true;
  const seededStarted = performance.now();
  let enabledUnseeded;
  try {
    enabledUnseeded = await agent.run({ client_id: Number(TENANT_ID) });
  } finally {
    state.allowSeedQuery = false;
    delete process.env.WARM_ROUTING_ENABLED;
  }
  return {
    disabled, disabledMs: Number(disabledMs.toFixed(3)), enabledUnseeded,
    enabledUnseededMs: Number((performance.now() - seededStarted).toFixed(3)),
    dbQueriesOnDisabledPath, fixtureDbQueries: state.queries.slice(), schedulerStarted: false,
  };
}

async function runAudit() {
  return withOfflineSandbox(async state => {
    const mission = await observeAcquisitionMission(state);
    const routing = await observeWarmRoutingDisabledPath(state);
    return {
      audit: 'AUDIT-062', mode: 'offline-fixtures', at: new Date().toISOString(),
      limitations: [
        'Fixture-path observations only; not production behavior, costs, or latency guarantees.',
        'Router and direct approval inject Scout results; direct Scout has an empty repository and no providers.',
        'Enabled/seeded worker, scheduler, cron, webhook callbacks and external providers were not executed.',
        'Absence of a module load on these paths does not authorize removing production wiring.',
      ],
      mission, routing, fixtureCalls: state.fixtureCalls.slice(), blockedIoAttempts: state.blocked.slice(),
      warmRoutingModuleLoads: state.loadedModules.slice(),
    };
  });
}

async function main(argv = process.argv.slice(2)) {
  if (argv.length !== 2 || argv[0] !== '--output' || !argv[1] || argv[1].startsWith('--')) {
    throw new Error('Usage: node scripts/audit062WarmRoutingObservation.js --output <new-report.json>');
  }
  const output = path.resolve(argv[1]);
  const report = await runAudit();
  // Exclusive creation: never silently overwrite an existing report or a symlink.
  fs.writeFileSync(output, `${JSON.stringify(report, null, 2)}\n`, { flag: 'wx', mode: 0o600 });
  return output;
}

module.exports = { runAudit, main, withOfflineSandbox };
if (require.main === module) {
  main().then(output => process.stderr.write(`Wrote offline fixture report: ${output}\n`)).catch(err => {
    process.stderr.write(`${err.message}\n`);
    process.exitCode = 1;
  });
}
