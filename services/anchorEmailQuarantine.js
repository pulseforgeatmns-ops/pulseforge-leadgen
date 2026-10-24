'use strict';

// SPEC-261. Additive no-resend decisions, never provider evidence or row release.
// No imports bootstrap the application or select a database. Only applyQuarantine
// writes; production callers must never invoke it implicitly.
const { createHash } = require('node:crypto');
const RECOVERY_LOCK_KEY = 'anchor-email-recovery:10';
const DECISION_KEYS = Object.freeze([
  'tenantId', 'itemId', 'envelopeId', 'programId', 'missionId', 'canonicalId',
  'attemptToken', 'recipientEmail', 'prospectId', 'companyId', 'candidateId', 'expectedRevision',
  'companyNameKey', 'companyDomain', 'expectedStatus', 'expectedReason', 'approvalRef',
]);
const COLUMNS = Object.freeze({ tenantId: 'tenant_id', itemId: 'item_id', envelopeId: 'envelope_id',
  programId: 'program_id', missionId: 'mission_id', canonicalId: 'canonical_id',
  attemptToken: 'attempt_token', recipientEmail: 'recipient_email', prospectId: 'prospect_id',
  companyId: 'company_id', candidateId: 'candidate_id', expectedRevision: 'expected_revision', companyNameKey: 'company_name_key', companyDomain: 'company_domain', expectedStatus: 'expected_status', expectedReason: 'expected_reason', approvalRef: 'approval_ref' });
function fail(code) { throw Object.assign(new Error(code), { code, retryAllowed: false }); }
function text(value) { return typeof value === 'string' && value.length > 0 && value.length <= 1024 && value === value.trim() && !/[\x00-\x1f\x7f]/.test(value); }
function validAttemptToken(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{0,5}[1-9])?\+00$/.test(value)) return false;
  // Validate calendar/time without converting the precision-bearing token to Date.
  const [year, month, day, hour, minute, second] = value.slice(0, 19).match(/\d+/g).map(Number);
  const date = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
  return year >= 1000 && year <= 9999 && date.getUTCFullYear() === year && date.getUTCMonth() + 1 === month
    && date.getUTCDate() === day && date.getUTCHours() === hour && date.getUTCMinutes() === minute && date.getUTCSeconds() === second;
}
const SHARED_DOMAINS = Object.freeze(['gmail.com','googlemail.com','outlook.com','hotmail.com','live.com','msn.com','yahoo.com','aol.com','icloud.com','proton.me','protonmail.com','facebook.com','linkedin.com','instagram.com','yelp.com','google.com','wixsite.com','squarespace.com','wordpress.com','linktr.ee']);
function authoritativeDomain(domain) {
  return typeof domain === 'string' && /^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\.[a-z]{2,}$/.test(domain)
    && !SHARED_DOMAINS.some(shared => domain === shared || domain.endsWith(`.${shared}`));
}
function companyIdentity(row) {
  const { ownershipNameKey, ownershipDomain } = require('./governedOutboundStore');
  const companyNameKey = ownershipNameKey(row?.name);
  const supplied = [row?.domain, row?.website].filter(value => typeof value === 'string' && value.trim());
  const domains = supplied.map(ownershipDomain);
  if (companyNameKey.length < 3 || !domains.length || domains.some(domain => !authoritativeDomain(domain))
      || new Set(domains).size !== 1) fail('anchor_quarantine_company_identity_unknown');
  return { companyNameKey, companyDomain: domains[0] };
}
function normalizeDecision(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)
      || Object.keys(input).some(key => !DECISION_KEYS.includes(key))
      || DECISION_KEYS.some(key => !Object.hasOwn(input, key) || !text(input[key]))) fail('anchor_quarantine_invalid_decision');
  if (input.tenantId !== '10' || !['attempted', 'uncertain'].includes(input.expectedStatus)
      || !validAttemptToken(input.attemptToken)
      || input.recipientEmail !== input.recipientEmail.toLowerCase()
      || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(input.recipientEmail)
      || DECISION_KEYS.some(key => /^(?:todo|tbd|placeholder|replace[_ -]?me|<.*>)$/i.test(input[key]))) fail('anchor_quarantine_invalid_decision');
  const identity = companyIdentity({ name: input.companyNameKey, domain: input.companyDomain });
  if (identity.companyNameKey !== input.companyNameKey || identity.companyDomain !== input.companyDomain) fail('anchor_quarantine_invalid_company_identity');
  return Object.freeze(Object.fromEntries(DECISION_KEYS.map(key => [key, input[key]])));
}
function decisionHash(input) { return createHash('sha256').update(JSON.stringify(normalizeDecision(input))).digest('hex'); }
function decisionFromRow(row) { return normalizeDecision(Object.fromEntries(DECISION_KEYS.map(key => [key, row?.[COLUMNS[key]]]))); }
function verifiedQuarantine(row) {
  try {
    const decision = decisionFromRow(row);
    const hash = decisionHash(decision);
    return row.decision_hash === hash && row.id === `quarantine_${hash}` ? decision : null;
  } catch { return null; }
}
async function requireQuarantine(db, expectedDecisionHash) {
  if (typeof expectedDecisionHash !== 'string' || !/^[a-f0-9]{64}$/.test(expectedDecisionHash)) fail('anchor_quarantine_hash_required');
  const { rows } = await db.query('SELECT * FROM anchor_email_quarantines WHERE tenant_id=$1 AND decision_hash=$2', ['10', expectedDecisionHash]);
  if (rows.length !== 1 || !verifiedQuarantine(rows[0])) fail('anchor_quarantine_required');
  return rows[0];
}
async function assertNotQuarantined(db, { tenantId, prospectId, companyId, email, itemId, lockCompanyRows = false } = {}) {
  if (!text(String(tenantId || '')) || !text(prospectId) || !text(companyId) || !text(email)) fail('anchor_quarantine_identity_required');
  const normalizedEmail = email.trim().toLowerCase();
  const { rows } = await db.query('SELECT * FROM anchor_email_quarantines WHERE tenant_id=$1 ORDER BY item_id', [String(tenantId)]);
  if (rows.some(q => q.prospect_id === prospectId || q.company_id === companyId || q.recipient_email === normalizedEmail || (itemId && q.item_id === itemId))) fail('anchor_email_quarantined');
  if (!rows.length) return true;
  if (rows.some(q => !verifiedQuarantine(q))) fail('anchor_quarantine_invalid_record');
  const ids = [...new Set([companyId, ...rows.map(q => q.company_id)])].sort();
  const companies = (await db.query(`SELECT c.id::text AS id,c.name,to_jsonb(c)->>'domain' AS domain,to_jsonb(c)->>'website' AS website
    FROM companies c WHERE c.client_id=$1 AND c.id::text=ANY($2::text[]) ORDER BY c.id${lockCompanyRows ? ' FOR SHARE' : ''}`,
  [Number(tenantId), ids])).rows;
  const candidate = companyIdentity(companies.find(c => c.id === companyId));
  for (const q of rows) {
    const original = companyIdentity(companies.find(c => c.id === q.company_id));
    if (original.companyNameKey !== q.company_name_key || original.companyDomain !== q.company_domain) fail('anchor_quarantine_company_identity_changed');
    if (candidate.companyNameKey === q.company_name_key || candidate.companyDomain === q.company_domain) fail('anchor_email_quarantined_company_alias');
  }
  return true;
}
// Explicit UTC text preserves PostgreSQL microseconds independently of session TZ
// and the driver's lossy JavaScript Date timestamp parser.
const TOKEN_SQL = "((i.attempted_at AT TIME ZONE 'UTC')::text || '+00')";
async function listUnresolved(db, tenantId) {
  if (!text(String(tenantId || ''))) fail('anchor_quarantine_tenant_required');
  const rows = (await db.query(`SELECT i.id AS item_id,i.tenant_id,i.envelope_id,e.program_id,e.mission_id,
    c.canonical_id,c.canonical_count,${TOKEN_SQL} AS attempt_token,
    i.prospect_id,i.company_id,i.candidate_id,e.revision AS expected_revision,lower(btrim(i.email)) AS recipient_email,i.status,i.reason,co.name AS company_name,to_jsonb(co)->>'domain' AS company_domain_raw,to_jsonb(co)->>'website' AS company_website
    FROM acquisition_outbound_items i
    LEFT JOIN acquisition_outbound_envelopes e ON e.id=i.envelope_id AND e.tenant_id=i.tenant_id
    LEFT JOIN companies co ON co.id::text=i.company_id AND co.client_id::text=i.tenant_id
    LEFT JOIN LATERAL (SELECT CASE WHEN count(*)=1 THEN min(x.id) END AS canonical_id,count(*)::int AS canonical_count
      FROM acquisition_mission_outbound_executions x
      WHERE x.tenant_id=i.tenant_id AND x.mission_id=e.mission_id AND x.prospect_id=i.candidate_id) c ON true
    WHERE i.tenant_id=$1 AND i.status IN ('attempted','uncertain') ORDER BY i.id`, [String(tenantId)])).rows;
  return rows.map(row => {
    let identity;
    try { identity = companyIdentity({ name: row.company_name, domain: row.company_domain_raw, website: row.company_website }); } catch { identity = {}; }
    const { company_name, company_domain_raw, company_website, ...result } = row;
    return { ...result, company_name_key: identity.companyNameKey || null, company_domain: identity.companyDomain || null };
  });
}
function sameIdentity(row, decision) {
  return ['tenantId', 'itemId', 'envelopeId', 'programId', 'missionId', 'canonicalId', 'attemptToken', 'recipientEmail', 'prospectId', 'companyId', 'candidateId', 'expectedRevision', 'companyNameKey', 'companyDomain']
    .every(key => text(row?.[COLUMNS[key]]) && row[COLUMNS[key]] === decision[key])
    && validAttemptToken(row.attempt_token) && row.status === decision.expectedStatus && row.reason === decision.expectedReason;
}
function ownMatches(row, own) {
  return Boolean(own && row.status === 'attempted' && validAttemptToken(own.attemptToken)
    && ['tenantId', 'programId', 'envelopeId', 'itemId', 'attemptToken'].every(key => text(own[key]) && row?.[COLUMNS[key]] === own[key]));
}
function identityQuarantined(row, quarantine) {
  return row.tenant_id === quarantine.tenant_id && (
    row.item_id === quarantine.item_id || row.prospect_id === quarantine.prospect_id
    || row.company_id === quarantine.company_id || row.recipient_email === quarantine.recipient_email
    || (text(row.company_name_key) && row.company_name_key === quarantine.company_name_key)
    || (text(row.company_domain) && row.company_domain === quarantine.company_domain));
}
function classifyBlockingRows(rows, quarantines, { ownAttempt } = {}) {
  if (!Array.isArray(rows) || !Array.isArray(quarantines)) fail('anchor_quarantine_rows_required');
  const decisions = quarantines.map(row => ({ row, decision: verifiedQuarantine(row) })).filter(entry => entry.decision);
  const result = { rawCount: rows.length, quarantinedCount: 0, ownAttemptCount: 0, blockingCount: 0,
    blockingRows: [], quarantinedRows: [], ownAttemptRows: [] };
  for (const row of rows) {
    const own = ownMatches(row, ownAttempt);
    if (own && quarantines.some(q => identityQuarantined(row, q))) result.blockingRows.push(row);
    else if (own) result.ownAttemptRows.push(row);
    else if (decisions.some(({ decision }) => sameIdentity(row, decision))) result.quarantinedRows.push(row);
    else result.blockingRows.push(row);
  }
  result.quarantinedCount = result.quarantinedRows.length;
  result.ownAttemptCount = result.ownAttemptRows.length;
  result.blockingCount = result.blockingRows.length;
  return result;
}
function identityLockKeys(decision) {
  return [`max:call:${decision.tenantId}:${decision.prospectId}`,
    `anchor-email-recovery:recipient:${decision.tenantId}:${decision.recipientEmail}`,
    `anchor-email-recovery:company:${decision.tenantId}:${decision.companyId}`].sort();
}
async function readAttempt(db, decision, lock = false) {
  const item = (await db.query(`SELECT i.*,${TOKEN_SQL} AS attempt_token FROM acquisition_outbound_items i
    WHERE i.id=$1 AND i.tenant_id=$2${lock ? ' FOR UPDATE' : ''}`, [decision.itemId, decision.tenantId])).rows[0];
  const envelope = (await db.query(`SELECT * FROM acquisition_outbound_envelopes WHERE id=$1 AND tenant_id=$2${lock ? ' FOR UPDATE' : ''}`, [decision.envelopeId, decision.tenantId])).rows[0];
  const canonical = (await db.query(`SELECT * FROM acquisition_mission_outbound_executions
    WHERE id=$1 AND tenant_id=$2${lock ? ' FOR UPDATE' : ''}`, [decision.canonicalId, decision.tenantId])).rows[0];
  return { item, envelope, canonical };
}
function assertAttempt(snapshot, decision) {
  const { item, envelope, canonical } = snapshot;
  if (!item || item.id !== decision.itemId || item.tenant_id !== decision.tenantId || item.envelope_id !== decision.envelopeId
      || item.prospect_id !== decision.prospectId || item.company_id !== decision.companyId || item.candidate_id !== decision.candidateId
      || String(item.email).trim().toLowerCase() !== decision.recipientEmail
      || item.attempt_token !== decision.attemptToken || item.status !== decision.expectedStatus
      || item.reason !== decision.expectedReason || item.provider_message_id !== null
      || !envelope || envelope.program_id !== decision.programId || envelope.mission_id !== decision.missionId || envelope.revision !== decision.expectedRevision
      || !canonical || canonical.mission_id !== decision.missionId || canonical.prospect_id !== decision.candidateId
      || canonical.prepared_artifact_revision !== decision.expectedRevision
      || (canonical.payload?.email !== undefined && String(canonical.payload.email).trim().toLowerCase() !== decision.recipientEmail)
      || canonical.status !== 'attempted' || canonical.provider_message_id !== null || canonical.sent_at !== null) fail('anchor_quarantine_attempt_changed');
}
async function inspectQuarantine(db, input) {
  const decision = normalizeDecision(input);
  const hash = decisionHash(decision);
  // SELECTs only. Safe even inside a caller's database-enforced read-only
  // transaction. Inspection never substitutes for the locked apply CAS.
  const snapshot = await readAttempt(db, decision);
  let matches = true;
  try { assertAttempt(snapshot, decision); } catch { matches = false; }
  const exists = (await db.query("SELECT to_regclass('public.anchor_email_quarantines') IS NOT NULL AS present")).rows[0]?.present === true;
  const prior = exists ? (await db.query('SELECT * FROM anchor_email_quarantines WHERE item_id=$1', [decision.itemId])).rows[0] : null;
  return { outcome: 'UNKNOWN', retryAllowed: false, applied: false, decisionHash: hash,
    exactAttemptMatches: matches, quarantinePresent: Boolean(prior),
    sameDecision: Boolean(prior && verifiedQuarantine(prior) && prior.decision_hash === hash) };
}
async function applyQuarantine(pool, input) {
  const decision = normalizeDecision(input);
  const hash = decisionHash(decision);
  const { GovernedOutboundStore } = require('./governedOutboundStore');
  const { withTenantOutreachExecutorLock } = require('./tenantOutreachExecutorLock');
  const result = await new GovernedOutboundStore(pool, decision.tenantId).lock(() =>
    withTenantOutreachExecutorLock(pool, async () => {
      const db = await pool.connect();
      let transaction = false;
      let discard = false;
      try {
        await db.query('BEGIN'); transaction = true;
        await db.query("SET LOCAL lock_timeout='5s'");
        await db.query("SET LOCAL statement_timeout='5s'");
        await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [RECOVERY_LOCK_KEY]);
        const control = (await db.query('SELECT * FROM anchor_email_recovery_control WHERE singleton=true FOR UPDATE')).rows;
        if (control.length !== 1 || control[0].maintenance_armed !== true || control[0].manifest !== null) fail('anchor_quarantine_maintenance_required');
        for (const key of identityLockKeys(decision)) await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [key]);
        const prior = (await db.query('SELECT * FROM anchor_email_quarantines WHERE item_id=$1 FOR UPDATE', [decision.itemId])).rows[0];
        if (prior) {
          if (!verifiedQuarantine(prior) || prior.decision_hash !== hash) fail('anchor_quarantine_decision_conflict');
          // Permanent rows outlive receipts/status changes. An identical repeat
          // acknowledges the original decision without mutating either ledger.
          await db.query('COMMIT'); transaction = false;
          return { outcome: 'UNKNOWN', retryAllowed: false, applied: false, idempotent: true, decisionHash: hash, quarantine: prior };
        }
        const snapshot = await readAttempt(db, decision, true);
        assertAttempt(snapshot, decision);
        const contact = (await db.query('SELECT id,client_id,company_id,email FROM prospects WHERE id=$1 AND client_id=$2 FOR UPDATE', [decision.prospectId, 10])).rows[0];
        const company = (await db.query("SELECT c.id,c.client_id,c.name,to_jsonb(c)->>'domain' AS domain,to_jsonb(c)->>'website' AS website FROM companies c WHERE id=$1 AND client_id=$2 FOR UPDATE", [decision.companyId, 10])).rows[0];
        if (!contact || String(contact.company_id) !== decision.companyId || String(contact.email).trim().toLowerCase() !== decision.recipientEmail || !company) fail('anchor_quarantine_identity_changed');
        const companySnapshot = companyIdentity(company);
        if (companySnapshot.companyNameKey !== decision.companyNameKey || companySnapshot.companyDomain !== decision.companyDomain) fail('anchor_quarantine_company_identity_changed');
        const columns = DECISION_KEYS.map(key => COLUMNS[key]);
        const values = DECISION_KEYS.map(key => decision[key]);
        const inserted = await db.query(`INSERT INTO anchor_email_quarantines(id,${columns.join(',')},decision_hash)
          VALUES($1,${columns.map((_, i) => `$${i + 2}`).join(',')},$${columns.length + 2}) RETURNING *`, [`quarantine_${hash}`, ...values, hash]);
        if (inserted.rows.length !== 1 || !verifiedQuarantine(inserted.rows[0])) fail('anchor_quarantine_commit_verification_failed');
        await db.query('COMMIT'); transaction = false;
        return { outcome: 'UNKNOWN', retryAllowed: false, applied: true, idempotent: false, decisionHash: hash, quarantine: inserted.rows[0] };
      } catch (error) { discard = true; throw error; }
      finally {
        if (transaction) { try { await db.query('ROLLBACK'); } catch { discard = true; } }
        db.release(discard);
      }
    }));
  return result?.halted ? { ...result, outcome: 'UNKNOWN', retryAllowed: false, applied: false } : result;
}
module.exports = { RECOVERY_LOCK_KEY, DECISION_KEYS, normalizeDecision, decisionHash, validAttemptToken,
  requireQuarantine, assertNotQuarantined, listUnresolved, classifyBlockingRows, inspectQuarantine, applyQuarantine, identityLockKeys, companyIdentity, authoritativeDomain };
