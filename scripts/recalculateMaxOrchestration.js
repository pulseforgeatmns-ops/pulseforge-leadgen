require('dotenv').config();

const pool = require('../db');
const { calculateProspectShadow, evaluateProspectShadow } = require('../utils/maxOrchestration');
const { loadClientOrchestrationConfig } = require('../utils/maxSignalIngestion');
const { assertAllowed, boundedInteger, optionalPositiveInteger, optionalTimestamp, optionalUuid, tokenizeArgs } = require('../utils/maxCli');
const { recordMaxMetric } = require('../utils/maxOrchestrationObservability');

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, {
    values: ['--prospect-id','--client-id','--after-id','--changed-since','--limit'],
    flags: ['--apply'],
  });
  return {
    apply: parsed.flags.has('--apply'),
    prospectId: optionalUuid(parsed.values.get('--prospect-id'), '--prospect-id'),
    clientId: optionalPositiveInteger(parsed.values.get('--client-id'), '--client-id'),
    afterId: optionalUuid(parsed.values.get('--after-id'), '--after-id'),
    changedSince: optionalTimestamp(parsed.values.get('--changed-since'), '--changed-since'),
    limit: boundedInteger(parsed.values.get('--limit'), '--limit', { defaultValue: 100, max: 2000 }),
  };
}

async function findRecalculationCandidates(db, options) {
  return db.query(`
    SELECT id, client_id
    FROM prospects
    WHERE ($1::uuid IS NULL OR id = $1)
      AND ($2::int IS NULL OR client_id = $2)
      AND ($3::uuid IS NULL OR id > $3)
      AND ($4::timestamptz IS NULL OR updated_at >= $4)
    ORDER BY id
    LIMIT $5
  `, [options.prospectId, options.clientId, options.afterId, options.changedSince, options.limit]);
}

async function run(options = parseArgs(), db = pool) {
  const rows = await findRecalculationCandidates(db, options);
  const report = {
    mode: options.apply ? 'shadow-write' : 'dry-run',
    scanned: 0, evaluated: 0, decisions_created: 0, duplicates: 0, skipped: 0,
    errors: [], results: [], last_prospect_id: options.afterId || null,
    side_effects: { status_updates: 0, messages: 0, sequence_changes: 0, enrichment_retries: 0, tasks: 0 },
  };
  const configs = new Map();
  for (const row of rows.rows) {
    const evaluationStarted = Date.now();
    report.scanned++;
    report.last_prospect_id = row.id;
    try {
      if (!configs.has(row.client_id)) configs.set(row.client_id, await loadClientOrchestrationConfig(db, row.client_id));
      const args = { db, prospectId: row.id, clientId: row.client_id, clientConfig: configs.get(row.client_id), env: process.env, now: new Date(), ignoreFeatureFlags: !options.apply };
      const result = options.apply ? await evaluateProspectShadow(args) : await calculateProspectShadow(args);
      if (result.skipped) { report.skipped++; continue; }
      report.evaluated++;
      if (options.apply && result.duplicate) report.duplicates++;
      if (options.apply && !result.duplicate) report.decisions_created++;
      if (options.apply && !result.duplicate) {
        await recordMaxMetric('manual_recalculation_processing_latency', {
          db, clientId: row.client_id, prospectId: row.id, decisionId: result.decision?.id || null,
          value: Date.now() - evaluationStarted,
          dimensions: { provenance: 'manual_recalculation' },
        }).catch(() => {});
      }
      const decision = result.decision;
      report.results.push({
        prospect_id: row.id,
        score: options.apply ? result.score?.score : result.scoreResult?.score,
        current_state: decision?.current_state,
        recommended_state: decision?.recommended_state,
        transition_recommended: decision?.transition_recommended,
      });
    } catch (error) {
      report.errors.push({ prospect_id: row.id, error: error.message });
    }
  }
  return report;
}

module.exports = { findRecalculationCandidates, parseArgs, run };

if (require.main === module) {
  run().then(report => {
    console.log(JSON.stringify(report, null, 2));
    process.exitCode = report.errors.length ? 1 : 0;
  }).catch(error => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }).finally(() => pool.end());
}
