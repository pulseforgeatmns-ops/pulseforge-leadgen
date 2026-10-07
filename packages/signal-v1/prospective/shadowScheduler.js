'use strict';

const { ShadowModeService } = require('./ShadowModeService');

/**
 * Idempotent, bounded production scheduler tick.
 */
async function runShadowSchedulerTick(service, options = {}) {
  const poll = options.pollCollectors !== false;
  const jobs = options.processJobs !== false;
  const out = { polled: null, jobs: null, at: new Date().toISOString() };
  if (poll) {
    out.polled = await service.pollCollectorsOnce();
  }
  if (jobs) {
    out.jobs = await service.runDueJobs(options.jobLimit ?? 50);
  }
  return out;
}

function startShadowScheduler(service, options = {}) {
  const intervalMs = options.intervalMs ?? Number(process.env.SIGNAL_SHADOW_POLL_MS || 60000);
  const captureIntervalMs = options.captureIntervalMs ?? Number(process.env.SIGNAL_CAPTURE_POLL_MS || 1000);
  for (const value of [intervalMs, captureIntervalMs]) {
    if (!Number.isFinite(value) || value < 1) throw new Error('Invalid Signal scheduler interval');
  }
  const setTimer = options.setInterval || setInterval;
  const clearTimer = options.clearInterval || clearInterval;
  const tasks = [
    { ms: intervalMs, run: () => service.pollCollectorsOnce(), enabled: options.pollCollectors !== false },
    { ms: captureIntervalMs, run: () => service.runDueJobs(options.jobLimit ?? 50, { jobType: 'DELAY_CAPTURE' }), enabled: options.processJobs !== false },
    { ms: intervalMs, run: () => service.runDueJobs(options.jobLimit ?? 50, { jobType: 'OUTCOME_24H' }), enabled: options.processJobs !== false },
  ];
  const handles = tasks.filter(task => task.enabled).map(task => {
    let running = false;
    const handle = setTimer(async () => {
      if (running || (options.gate && !options.gate().ok)) return;
      running = true;
      try { await task.run(); }
      catch (err) { console.error('[signal-v1-shadow] scheduler tick error:', err.message); }
      finally { running = false; }
    }, task.ms);
    if (handle.unref) handle.unref();
    return handle;
  });
  return () => handles.forEach(clearTimer);
}

async function createShadowModeServiceFromStore(store, options = {}) {
  const service = new ShadowModeService(store, options);
  if (options.startProspectiveCohortImmediately) {
    await service.ensureProspectiveCohortStarted(options.providerVersions || {});
  }
  if (store.loadProspectiveJobs) {
    const jobs = await store.loadProspectiveJobs();
    service.restoreJobsFromStore(jobs);
  }
  return service;
}

module.exports = {
  runShadowSchedulerTick,
  startShadowScheduler,
  createShadowModeServiceFromStore,
};
