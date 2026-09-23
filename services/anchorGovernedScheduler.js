'use strict';

// Governed Anchor only. Poll first; the dispatcher independently proves mailbox
// freshness, ownership, suppression, policy window, spacing and attempt budget.
// Max inventory oversight runs after dispatch so Scout work never delays an
// eligible Emmett send.
function startAnchorGovernedScheduler(options = {}) {
  const enabled = options.enabled ?? process.env.ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED === 'true';
  if (!enabled) return null;
  const cron = options.cron || require('../anchorDailyOutboundCron');
  const logger = options.logger || console;
  const schedule = options.setInterval || setInterval;
  const cancel = options.clearInterval || clearInterval;
  const maxControlEnabled = options.maxControlEnabled
    ?? process.env.ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED === 'true';
  const maxControl = options.maxControl
    || (() => require('./maxOutboundControlLoop').runMaxOutboundControlLoop({
      pool: options.pool,
      logger,
    }));
  let busy = false;
  let stopped = false;
  let cycles = 0;
  async function cycle() {
    if (busy || stopped) return;
    busy = true;
    const current = cycles++;
    try {
      await cron.poll({ pool: options.pool });
      if (current % 5 === 0) {
        const result = await cron.run({ pool: options.pool });
        logger.log('[anchor-governed]', JSON.stringify(result));
      }
      if (maxControlEnabled && current % 60 === 0) {
        const control = await maxControl();
        logger.log('[anchor-max-control]', JSON.stringify(control));
      }
    } catch (error) {
      logger.error('[anchor-governed]', error.code || error.message);
    } finally { busy = false; }
  }
  const timer = schedule(cycle, 60000);
  timer.unref?.();
  void cycle();
  return { cycle, stop() { stopped = true; cancel(timer); } };
}
module.exports = { startAnchorGovernedScheduler };
