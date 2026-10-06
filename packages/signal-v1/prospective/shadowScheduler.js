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
  let running = false;
  const handle = setInterval(async () => {
    if (running) return;
    running = true;
    try {
      await runShadowSchedulerTick(service, options);
    } catch (err) {
      console.error('[signal-v1-shadow] scheduler tick error:', err.message);
    } finally {
      running = false;
    }
  }, intervalMs);
  if (handle.unref) handle.unref();
  return () => clearInterval(handle);
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
