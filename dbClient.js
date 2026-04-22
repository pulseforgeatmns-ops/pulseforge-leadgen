const pool = require('./db');

// Check do not contact before any agent acts
async function checkDNC(prospectId) {
  const res = await pool.query(
    'SELECT do_not_contact FROM prospects WHERE id = $1',
    [prospectId]
  );
  return res.rows[0]?.do_not_contact ?? true;
}

// Get full prospect record
async function getProspect(prospectId) {
  const res = await pool.query(
    'SELECT * FROM prospects WHERE id = $1',
    [prospectId]
  );
  return res.rows[0];
}

// Get prospect summary for manager agent
async function getProspectSummary(prospectId) {
  const res = await pool.query(
    'SELECT * FROM prospect_summary WHERE id = $1',
    [prospectId]
  );
  return res.rows[0];
}

// Update prospect status
async function updateProspectStatus(prospectId, status) {
  await pool.query(
    'UPDATE prospects SET status = $1, updated_at = NOW() WHERE id = $2',
    [status, prospectId]
  );
}

// Log a touchpoint after any agent action
async function logTouchpoint(prospectId, channel, actionType, contentSummary, outcome, sentiment, agentId, externalRef = null) {
  await pool.query(
    `INSERT INTO touchpoints 
      (prospect_id, channel, action_type, content_summary, outcome, sentiment, agent_id, external_ref)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
    [prospectId, channel, actionType, contentSummary, outcome, sentiment, agentId, externalRef]
  );
  // Update last contacted timestamp
  await pool.query(
    'UPDATE prospects SET last_contacted_at = NOW() WHERE id = $1',
    [prospectId]
  );
}

// Log every agent action to audit trail
async function logAgentAction(agentName, action, prospectId, targetUrl, payload, status, errorMsg = null, durationMs = null) {
  await pool.query(
    `INSERT INTO agent_log 
      (agent_name, action, prospect_id, target_url, payload, status, error_msg, duration_ms)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
    [agentName, action, prospectId, targetUrl, JSON.stringify(payload), status, errorMsg, durationMs]
  );
}

// Add a new prospect
async function addProspect(data) {
  const res = await pool.query(
    `INSERT INTO prospects 
      (company_id, first_name, last_name, email, phone, job_title, decision_maker, linkedin_url, facebook_url, source, icp_score)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
     ON CONFLICT (email) DO NOTHING
     RETURNING id`,
    [
      data.company_id || null,
      data.first_name,
      data.last_name,
      data.email,
      data.phone || null,
      data.job_title || null,
      data.decision_maker || false,
      data.linkedin_url || null,
      data.facebook_url || null,
      data.source || 'manual',
      data.icp_score || 0
    ]
  );
  return res.rows[0]?.id;
}

// Add a company
async function addCompany(data) {
  const res = await pool.query(
    `INSERT INTO companies 
      (name, industry, size, location, website, icp_score, tech_stack)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     RETURNING id`,
    [
      data.name,
      data.industry || null,
      data.size || null,
      data.location || null,
      data.website || null,
      data.icp_score || 0,
      data.tech_stack || []
    ]
  );
  return res.rows[0]?.id;
}

// Get all active prospects for a given status
async function getProspectsByStatus(status) {
  const res = await pool.query(
    'SELECT * FROM prospect_summary WHERE status = $1 AND do_not_contact = false ORDER BY icp_score DESC',
    [status]
  );
  return res.rows;
}

async function savePendingComment(data) {
  const res = await pool.query(
    `INSERT INTO pending_comments 
      (author_name, author_title, post_content, comment, post_url, channel)
     VALUES ($1, $2, $3, $4, $5, $6)
     RETURNING id`,
    [data.authorName, data.authorTitle, data.postContent, data.comment, data.postUrl, data.channel || 'linkedin']
  );
  return res.rows[0]?.id;
}

async function getPendingComments() {
  const res = await pool.query(
    `SELECT * FROM pending_comments WHERE status = 'pending' ORDER BY created_at DESC`
  );
  return res.rows;
}

async function updateCommentStatus(id, status) {
  await pool.query(
    `UPDATE pending_comments SET status = $1 WHERE id = $2`,
    [status, id]
  );
}

module.exports = {
  checkDNC,
  getProspect,
  getProspectSummary,
  updateProspectStatus,
  logTouchpoint,
  logAgentAction,
  addProspect,
  addCompany,
  getProspectsByStatus,
  savePendingComment,
  getPendingComments,
  updateCommentStatus
};