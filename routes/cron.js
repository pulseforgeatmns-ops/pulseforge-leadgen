const express = require('express');
const router = express.Router();

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
  res.json({ success: true, agent });
  try {
    delete require.cache[require.resolve(CRON_MODULES[agent])];
    const mod = require(CRON_MODULES[agent]);
    if (agent === 'scout' && typeof mod.run === 'function') {
      mod.run({ industry: query.industry, location: query.location }).catch(err => {
        console.error(`[cron] scout run error:`, err.message);
      });
    } else if ((agent === 'setter_handoff' || agent === 'handoff_utility') && typeof mod.run === 'function') {
      mod.run({ lookbackDays: query.lookbackDays }).catch(err => {
        console.error(`[cron] ${agent} run error:`, err.message);
      });
    }
  } catch (err) {
    console.error(`[cron] ${agent} error:`, err.message);
  }
}

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
