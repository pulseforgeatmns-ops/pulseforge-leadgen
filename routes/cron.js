const express = require('express');
const router = express.Router();
const { normalizeClientId } = require('../utils/clientContext');
const { runScoutExpansionCron } = require('../scoutExpansion');

const CRON_MODULES = {
  scout:     '../leadgen',
  emmett:    '../emmettAgent',
  max:       '../maxAgent',
  rex:       '../rexAgent',
  sketch:    '../sketchAgent',
  paige:     '../paigeAgent',
  faye:      '../facebookAgent',
  link:      '../linkedinAgent',
  sam:       '../samAgent',
  vera:      '../veraAgent',
  cal:       '../calAgent',
  cal_batch: '../calBatchAgent',
  penny:     '../pennyAgent',
  analytics: '../analyticsAgent',
  riley:       '../rileyAgent',
  warm_signal: '../warmSignalAgent',
  setter_handoff: '../setterHandoffAgent',
  sync_setter:    '../syncSetterLeadList',
  handoff_utility: '../setterHandoffAgent',
};

function runCronAgent(agent, res, query = {}) {
  if (!CRON_MODULES[agent]) return res.status(400).json({ error: `Unknown agent: ${agent}` });
  const clientId = normalizeClientId(query.client_id || query.clientId);
  res.json({ success: true, agent, client_id: clientId });
  try {
    delete require.cache[require.resolve(CRON_MODULES[agent])];
    process.env.ACTIVE_CLIENT_ID = String(clientId);
    const mod = require(CRON_MODULES[agent]);
    if (agent === 'scout' && typeof mod.run === 'function') {
      mod.run({
        industry: query.industry,
        location: query.location,
        maxResults: query.maxResults || query.max || query.limit,
        client_id: clientId,
      }).catch(err => {
        console.error(`[cron] scout run error:`, err.message);
      });
    } else if ((agent === 'setter_handoff' || agent === 'handoff_utility') && typeof mod.run === 'function') {
      mod.run({ lookbackDays: query.lookbackDays, client_id: clientId }).catch(err => {
        console.error(`[cron] ${agent} run error:`, err.message);
      });
    } else if (typeof mod.run === 'function') {
      mod.run({ client_id: clientId }).catch(err => {
        console.error(`[cron] ${agent} run error:`, err.message);
      });
    }
  } catch (err) {
    console.error(`[cron] ${agent} error:`, err.message);
  }
}

async function handleScoutExpansionCron(req, res) {
  const secret = req.body?.secret || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const clientId = normalizeClientId(req.query.client_id || req.query.clientId);
  try {
    const result = await runScoutExpansionCron(clientId);
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[cron] scoutExpansion error:', err.message);
    res.status(500).json({ error: err.message });
  }
}

router.post('/cron/scoutExpansion', handleScoutExpansionCron);
router.get('/cron/scoutExpansion', handleScoutExpansionCron);

router.post('/cron/:agent', async (req, res) => {
  const { agent } = req.params;
  const secret = req.body?.secret || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  runCronAgent(agent, res, req.query);
});

router.get('/cron/:agent', async (req, res) => {
  const { agent } = req.params;
  const secret = req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  runCronAgent(agent, res, req.query);
});

module.exports = router;
