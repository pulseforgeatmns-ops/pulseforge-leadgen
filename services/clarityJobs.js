'use strict';
const crypto = require('node:crypto');
const { enabled } = require('../packages/max/clarity/common');
async function runClarityJobs({ pool, env = process.env, store, gmail, now, sleep, loadAds, loadLeads } = {}) {
  if (!enabled(env) || env.CLARITY_INGESTION_ENABLED !== 'true') return { skipped: true, reason: 'disabled' };
  const { ClarityStore } = require('../packages/max/clarity/store');
  const { createClarityGmail } = require('./clarityGmail');
  const { createClarityIngestion } = require('./clarityIngestion');
  const { prepareRecommendations } = require('./clarityRecommendations');
  const db = pool || require('../db');
  return createClarityIngestion({ store: store || new ClarityStore(db), gmail: gmail || createClarityGmail({ env }), env, now, sleep,
    onSnapshot: input => prepareRecommendations({ ...input, pool: db, loadAds, loadLeads }) }).runDue({ limit: 1 });
}
function createClarityCronHandler(options = {}) {
  return async(req,res) => {
    const env = options.env || process.env;
    const expected = Buffer.from(env.CRON_SECRET || ''), actual = Buffer.from(String(req.get('authorization') || '').replace(/^Bearer /,''));
    if (!expected.length || expected.length !== actual.length || !crypto.timingSafeEqual(expected,actual)) return res.status(401).json({error:'Unauthorized'});
    try { return res.set('Cache-Control','no-store').json(await runClarityJobs(options)); }
    catch (_) { return res.status(503).json({error:'clarity_ingestion_failed'}); }
  };
}
module.exports = { runClarityJobs, createClarityCronHandler };
