'use strict';

// Governed Anchor only. Poll first; the dispatcher independently proves mailbox
// freshness, ownership, suppression, policy window, spacing and attempt budget.
function startAnchorGovernedScheduler(options = {}) {
  const enabled = options.enabled ?? process.env.ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED === 'true';
  if (!enabled) return null;
  const cron = options.cron || require('../anchorDailyOutboundCron');
  const logger = options.logger || console;
  const schedule = options.setInterval || setInterval;
  const cancel = options.clearInterval || clearInterval;
  let busy = false;
  let stopped = false;
  let cycles = 0;
  async function cycle() {
    if (busy || stopped) return;
    busy = true;
    try {
      await cron.poll({ pool: options.pool });
      if (cycles++ % 5 === 0) {
        const result = await cron.run({ pool: options.pool });
        logger.log('[anchor-governed]', JSON.stringify(result));
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
