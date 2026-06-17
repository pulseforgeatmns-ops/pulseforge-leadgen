const pool = require('../db');

const AGENT_ALIASES = {
  scout: 'scout',
  scout_agent: 'scout',
  email: 'emmett',
  email_agent: 'emmett',
  emmett: 'emmett',
  emmett_agent: 'emmett',
  riley: 'riley',
  riley_agent: 'riley',
  paige: 'paige',
  paige_agent: 'paige',
  facebook: 'faye',
  facebook_agent: 'faye',
  faye: 'faye',
  faye_agent: 'faye',
  vera: 'vera',
  vera_agent: 'vera',
  max: 'max',
  max_agent: 'max',
  rex: 'rex',
  rex_agent: 'rex',
  cal: 'cal',
  cal_agent: 'cal',
  calbatch: 'cal',
  cal_batch: 'cal',
  cal_batch_agent: 'cal',
  linkedin: 'link',
  linkedin_agent: 'link',
  link: 'link',
  link_agent: 'link',
  instagram: 'ivy',
  instagram_agent: 'ivy',
  ivy: 'ivy',
  ivy_agent: 'ivy',
  sam: 'sam',
  sam_agent: 'sam',
  sms: 'sam',
  sms_agent: 'sam',
  sketch: 'sketch',
  sketch_agent: 'sketch',
  penny: 'penny',
  penny_agent: 'penny',
};

const CHANNEL_AGENTS = {
  call: ['cal'],
  phone: ['cal'],
  sms: ['sam'],
  text: ['sam'],
  email: ['emmett'],
  gbp: ['vera', 'paige'],
  google_business: ['vera', 'paige'],
  content: ['paige', 'faye', 'link', 'ivy', 'penny'],
  facebook: ['faye', 'paige'],
  linkedin: ['link', 'paige'],
  instagram: ['ivy'],
  ads: ['penny'],
};

function normalizeAgentName(name = '') {
  const cleaned = String(name).toLowerCase().replace(/[^a-z0-9_]/g, '');
  return AGENT_ALIASES[cleaned] || AGENT_ALIASES[cleaned.replace(/_agent$/, '')] || cleaned.replace(/_agent$/, '');
}

async function getEnabledAgents(clientId) {
  const { rows } = await pool.query(
    'SELECT enabled_agents FROM clients WHERE id = $1 LIMIT 1',
    [clientId]
  );
  const enabled = Array.isArray(rows[0]?.enabled_agents) ? rows[0].enabled_agents : [];
  return [...new Set(enabled.map(normalizeAgentName).filter(Boolean))];
}

async function isAgentEnabled(clientId, agentName) {
  const normalized = normalizeAgentName(agentName);
  if (!normalized) return false;
  const enabled = await getEnabledAgents(clientId);
  return enabled.includes(normalized);
}

async function getAvailableActionAgents(clientId, channel) {
  const normalizedChannel = String(channel || '').toLowerCase().replace(/[^a-z0-9_]/g, '');
  const candidates = CHANNEL_AGENTS[normalizedChannel] || [];
  const enabled = await getEnabledAgents(clientId);
  return candidates.filter(agent => enabled.includes(agent));
}

module.exports = {
  CHANNEL_AGENTS,
  normalizeAgentName,
  getEnabledAgents,
  isAgentEnabled,
  getAvailableActionAgents,
};
