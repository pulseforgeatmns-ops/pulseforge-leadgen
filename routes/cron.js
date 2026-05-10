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
  penny:     '../pennyAgent',
  analytics: '../analyticsAgent',
  riley:     '../rileyAgent',
};

function runCronAgent(agent, res) {
  if (!CRON_MODULES[agent]) return res.status(400).json({ error: `Unknown agent: ${agent}` });
  res.json({ success: true, agent });
  try {
    delete require.cache[require.resolve(CRON_MODULES[agent])];
    require(CRON_MODULES[agent]);
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
  runCronAgent(agent, res);
});

router.get('/cron/:agent', async (req, res) => {
  const { agent } = req.params;
  const secret = req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  runCronAgent(agent, res);
});

module.exports = router;
