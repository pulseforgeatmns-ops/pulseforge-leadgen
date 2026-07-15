'use strict';

require('dotenv').config();
const pool = require('../db');
const { recordRecommendationReview, sampleRecommendations } = require('../utils/maxReviewSampling');
const { assertAllowed, boundedInteger, optionalPositiveInteger, tokenizeArgs } = require('../utils/maxCli');

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, {
    values: ['--client-id','--limit','--max-age-days','--decision-id','--reviewer','--outcome','--notes',
      '--score-explanation','--source-trustworthy','--source-notes'],
  });
  const options = {
    clientId: optionalPositiveInteger(parsed.values.get('--client-id'), '--client-id'),
    limit: boundedInteger(parsed.values.get('--limit'), '--limit', { defaultValue: 25, max: 250 }),
    maxAgeDays: boundedInteger(parsed.values.get('--max-age-days'), '--max-age-days', { defaultValue: 30, max: 365 }),
    decisionId: parsed.values.get('--decision-id') || null,
    reviewerIdentity: parsed.values.get('--reviewer') || null,
    outcome: parsed.values.get('--outcome') || null,
    notes: parsed.values.get('--notes') || null,
    scoreComponentExplanation: parsed.values.get('--score-explanation')
      ? JSON.parse(parsed.values.get('--score-explanation')) : null,
    sourceDataTrustworthy: parsed.values.has('--source-trustworthy')
      ? ({ true:true, false:false })[parsed.values.get('--source-trustworthy')] : null,
    sourceDataNotes: parsed.values.get('--source-notes') || null,
  };
  if (parsed.values.has('--source-trustworthy') && options.sourceDataTrustworthy == null) {
    throw new Error('--source-trustworthy must be true or false');
  }
  const recordValues = [options.decisionId, options.reviewerIdentity, options.outcome].filter(Boolean).length;
  if (recordValues > 0 && recordValues < 3) throw new Error('--decision-id, --reviewer, and --outcome are required together');
  return options;
}

async function run(options = parseArgs(), db = pool) {
  if (options.decisionId) return { mode: 'review_recorded', review: await recordRecommendationReview(options, db) };
  return { mode: 'review_sample', rows: await sampleRecommendations(options, db) };
}

module.exports = { parseArgs, run };

if (require.main === module) {
  run().then(result => console.log(JSON.stringify(result, null, 2)))
    .catch(error => { console.error(error.stack || error.message); process.exitCode=1; })
    .finally(() => pool.end());
}
