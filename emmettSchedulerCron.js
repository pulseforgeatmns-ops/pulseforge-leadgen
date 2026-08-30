'use strict';

/**
 * SPEC-189 — Emmett Infrastructure Scheduler Cron Adapter
 *
 * Cron entry point for scheduled Emmett infrastructure assessment.
 * Wraps emmettScheduler.assessInfrastructure to provide cron-compatible interface.
 */

const { assessInfrastructure } = require('./utils/emmettScheduler');

/**
 * Cron-compatible run() function.
 * Delegates to infrastructure scheduler.
 */
async function run(context = {}) {
  const result = await assessInfrastructure(context);
  
  // Transform scheduler result to cron-compatible format
  return {
    ...result,
    acquired: false, // No acquisition performed
    executed: false, // No sends executed
    candidates_evaluated: 0,
    infrastructure_ready: result.status === 'operational',
  };
}

module.exports = {
  run,
};

if (require.main === module) {
  run().catch(async (err) => {
    try {
      const db = require('../dbClient');
      await db.logAgentAction(
        'emmett_scheduler',
        'cron_run',
        null,
        null,
        { error: err.message },
        'failed',
        err.message
      );
    } catch (logErr) {
      console.error('Failed to log scheduler fatal error:', logErr.message);
    }
    console.error(err);
    process.exit(1);
  });
}
