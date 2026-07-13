const pool = require('./db');
const { getRuntimeClientId } = require('./utils/clientContext');
const { deriveBusinessNameShort, ensureBusinessNameShortColumns } = require('./utils/businessNameShort');

let pendingCommentPublishSchemaPromise;

function ensurePendingCommentPublishSchema() {
  if (!pendingCommentPublishSchemaPromise) {
    pendingCommentPublishSchemaPromise = pool.query(`
      ALTER TABLE pending_comments
        ADD COLUMN IF NOT EXISTS posted_at TIMESTAMPTZ
    `).catch(err => {
      pendingCommentPublishSchemaPromise = null;
      throw err;
    });
  }
  return pendingCommentPublishSchemaPromise;
}

// Check do not contact before any agent acts
async function checkDNC(prospectId) {
  const clientId = getRuntimeClientId();
  const res = await pool.query(
    'SELECT do_not_contact FROM prospects WHERE id = $1 AND client_id = $2',
    [prospectId, clientId]
  );
  return res.rows[0]?.do_not_contact ?? true;
}

// Get full prospect record
async function getProspect(prospectId) {
  const clientId = getRuntimeClientId();
  const res = await pool.query(
    'SELECT * FROM prospects WHERE id = $1 AND client_id = $2',
    [prospectId, clientId]
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
  const clientId = getRuntimeClientId();
  await pool.query(
    'UPDATE prospects SET status = $1, updated_at = NOW() WHERE id = $2 AND client_id = $3',
    [status, prospectId, clientId]
  );
}

// Log a touchpoint after any agent action
async function logTouchpoint(prospectId, channel, actionType, contentSummary, outcome, sentiment, agentId, externalRef = null) {
  const clientId = getRuntimeClientId();
  await pool.query(
    `INSERT INTO touchpoints 
      (prospect_id, channel, action_type, content_summary, outcome, sentiment, agent_id, external_ref, client_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
    [prospectId, channel, actionType, contentSummary, outcome, sentiment, agentId, externalRef, clientId]
  );
  // Update last contacted timestamp
  await pool.query(
    'UPDATE prospects SET last_contacted_at = NOW() WHERE id = $1 AND client_id = $2',
    [prospectId, clientId]
  );
}

// Log every agent action to audit trail
async function logAgentAction(agentName, action, prospectId, targetUrl, payload, status, errorMsg = null, durationMs = null) {
  const clientId = getRuntimeClientId();
  await pool.query(
    `INSERT INTO agent_log 
      (agent_name, action, prospect_id, target_url, payload, status, error_msg, duration_ms, client_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
    [agentName, action, prospectId, targetUrl, JSON.stringify(payload), status, errorMsg, durationMs, clientId]
  );
}

// Add a new prospect
async function addProspect(data) {
  const clientId = getRuntimeClientId(data);
  const res = await pool.query(
    `INSERT INTO prospects 
      (company_id, first_name, last_name, email, phone, job_title, decision_maker, linkedin_url, facebook_url, source, icp_score, client_id, service_area_match)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
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
      data.icp_score || 0,
      clientId,
      data.service_area_match || null
    ]
  );
  return res.rows[0]?.id;
}

// Add a company
async function addCompany(data) {
  const clientId = getRuntimeClientId(data);
  await ensureBusinessNameShortColumns(pool);
  const shortName = deriveBusinessNameShort(data.name);
  const res = await pool.query(
    `INSERT INTO companies 
      (name, business_name_short, business_name_short_confidence, business_name_short_flags, industry, size, location, website, icp_score, tech_stack, client_id)
     VALUES ($1, $2, $3, $4::text[], $5, $6, $7, $8, $9, $10, $11)
     RETURNING id`,
    [
      data.name,
      shortName.business_name_short,
      shortName.confidence,
      shortName.flags,
      data.industry || null,
      data.size || null,
      data.location || null,
      data.website || null,
      data.icp_score || 0,
      data.tech_stack || [],
      clientId
    ]
  );
  return res.rows[0]?.id;
}

// Get all active prospects for a given status
async function getProspectsByStatus(status) {
  const clientId = getRuntimeClientId();
  const res = await pool.query(
    'SELECT * FROM prospects WHERE status = $1 AND do_not_contact = false AND client_id = $2 ORDER BY icp_score DESC',
    [status, clientId]
  );
  return res.rows;
}

async function savePendingComment(data) {
  const clientId = getRuntimeClientId(data);
  const res = await pool.query(
    `INSERT INTO pending_comments 
      (author_name, author_title, post_content, comment, post_url, channel, client_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (client_id, post_url) WHERE post_url IS NOT NULL AND status = 'pending' DO NOTHING
     RETURNING id`,
    [data.authorName, data.authorTitle, data.postContent, data.comment, data.postUrl, data.channel || 'linkedin', clientId]
  );
  return res.rows[0]?.id;
}

async function getPendingComments() {
  const clientId = getRuntimeClientId();
  const res = await pool.query(
    `SELECT * FROM pending_comments WHERE status = 'pending' AND client_id = $1 ORDER BY created_at DESC`,
    [clientId]
  );
  return res.rows;
}

async function updateCommentStatus(id, status) {
  const clientId = getRuntimeClientId();
  await ensurePendingCommentPublishSchema();
  await pool.query(
    `UPDATE pending_comments
     SET status = $1,
         posted_at = CASE
           WHEN $1 = 'posted' THEN COALESCE(posted_at, NOW())
           ELSE posted_at
         END
     WHERE id = $2 AND client_id = $3`,
    [status, id, clientId]
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
