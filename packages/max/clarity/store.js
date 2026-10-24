'use strict';
const { randomUUID } = require('node:crypto');
const { failure, tenant, digest, id } = require('./common');
const READONLY = 'https://www.googleapis.com/auth/gmail.readonly';
function date(value) { return value instanceof Date ? value.toISOString().slice(0,10) : String(value).slice(0,10); }
function snapshotRow(row) { return row ? { ...row, window_start: date(row.window_start), window_end: date(row.window_end) } : null; }
function recommendationRow(row) {
  return row ? { ...row.content, id: row.id, client_id: row.client_id, snapshot_id: row.snapshot_id,
    digest: row.digest, rank: row.rank, status: row.status, decided_at: row.decided_at,
    review_url: `/clarity-review?id=${encodeURIComponent(row.id)}&client_id=${row.client_id}` } : null;
}
class ClarityStore {
  constructor(pool, { connection = false } = {}) { this.pool = pool; this.connection = connection; }
  async getIntegration({ clientId }) {
    return (await this.pool.query('SELECT * FROM clarity_integrations WHERE client_id=$1', [tenant(clientId)])).rows[0] || null;
  }
  async saveIntegration({ clientId, integration: x }) {
    const cid = tenant(clientId);
    if (cid !== 10 || Number(x.client_id) !== cid || x.mailbox_email !== 'jacob@goanchorcleaning.com'
      || x.consent_status !== 'authorized' || !Array.isArray(x.oauth_scopes) || x.oauth_scopes.length !== 1
      || x.oauth_scopes[0] !== READONLY || !x.refresh_token_encrypted || !x.consented_at || !x.token_key_id || !x.oauth_client_id
      || !/^[a-zA-Z0-9_-]{3,100}$/.test(x.project_id || '')
      || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(x.digest_sender || '')) throw failure('clarity_invalid_grant');
    const saved = await this.pool.query(`INSERT INTO clarity_integrations
      (client_id,mailbox_email,digest_sender,enabled,consent_status,oauth_scopes,refresh_token_encrypted,token_key_id,consented_at,oauth_client_id,project_id)
      VALUES ($1,$2,$3,FALSE,$4,$5::jsonb,$6::jsonb,$7,$8,$9,$10)
      ON CONFLICT(client_id) DO UPDATE SET mailbox_email=EXCLUDED.mailbox_email,digest_sender=EXCLUDED.digest_sender,
        enabled=FALSE,consent_status=EXCLUDED.consent_status,oauth_scopes=EXCLUDED.oauth_scopes,
        refresh_token_encrypted=EXCLUDED.refresh_token_encrypted,token_key_id=EXCLUDED.token_key_id,
        consented_at=EXCLUDED.consented_at,oauth_client_id=EXCLUDED.oauth_client_id,project_id=EXCLUDED.project_id,ingestion_cursor=NULL,updated_at=NOW()
      WHERE clarity_integrations.project_id=EXCLUDED.project_id RETURNING client_id`,
    [cid,x.mailbox_email,x.digest_sender,x.consent_status,JSON.stringify(x.oauth_scopes),JSON.stringify(x.refresh_token_encrypted),x.token_key_id,x.consented_at,x.oauth_client_id,x.project_id]);
    if (!saved.rows.length) throw failure('clarity_project_change_forbidden',409);
    return { client_id: cid, enabled: false, consent_status: 'authorized' };
  }
  async listDueIntegrations({ now = new Date(), limit = 10 } = {}) {
    return (await this.pool.query(`SELECT * FROM clarity_integrations WHERE client_id=10 AND enabled=TRUE
      AND consent_status='authorized' AND next_run_at <= $1 ORDER BY next_run_at LIMIT $2`, [now,Math.min(10,Math.max(1,limit))])).rows;
  }
  async withIngestionLock({ clientId }, fn) {
    const cid = tenant(clientId), db = await this.pool.connect();
    let held = false, discard = false;
    try {
      held = (await db.query('SELECT pg_try_advisory_lock(976,$1) AS locked', [cid])).rows[0].locked;
      if (!held) return { skipped: true, reason: 'already_running' };
      return await fn(new ClarityStore(db, { connection: true }));
    } finally {
      if (held) { try { await db.query('SELECT pg_advisory_unlock(976,$1)', [cid]); } catch (_) { discard = true; } }
      db.release(discard);
    }
  }
  async transaction(fn) {
    const db = this.connection ? this.pool : await this.pool.connect();
    let discard = false;
    try { await db.query('BEGIN'); const out = await fn(db); await db.query('COMMIT'); return out; }
    catch (err) { try { await db.query('ROLLBACK'); } catch (_) { discard = true; } throw err; }
    finally { if (!this.connection) db.release(discard); }
  }
  async recordIngestionRun({ clientId, status, counts = {}, errorCode, nextRunAt, cursor = null, rejections = [], expectedConsentAt = null }) {
    const cid = tenant(clientId);
    const safeCounts = Object.fromEntries(Object.entries(counts).filter(([key,value]) => /^[a-z_]{1,40}$/i.test(key) && Number.isSafeInteger(value) && value >= 0));
    const safeCode = errorCode && /^[a-zA-Z0-9_]{1,80}$/.test(errorCode) ? errorCode : null;
    const safeRejections = rejections.slice(0,50).map(item => ({ code: /^[a-zA-Z0-9_]{1,80}$/.test(item.code || '') ? item.code : 'clarity_parse_failed',
      provenance: Object.fromEntries(['gmail_message_id','gmail_thread_id','received_at','content_sha256','parser_version','source_type','project_id']
        .filter(key => typeof item.provenance?.[key] === 'string').map(key => [key,item.provenance[key].slice(0,200)])) }));
    await this.transaction(async db => {
      await db.query(`INSERT INTO clarity_ingestion_runs(id,client_id,status,counts,error_code,next_run_at,rejections)
        VALUES($1,$2,$3,$4::jsonb,$5,$6,$7::jsonb)`, [randomUUID(),cid,String(status).slice(0,40),JSON.stringify(safeCounts),safeCode,nextRunAt,JSON.stringify(safeRejections)]);
      await db.query(`UPDATE clarity_integrations SET next_run_at=COALESCE($2,next_run_at),ingestion_cursor=$4::jsonb,updated_at=NOW(),
        last_success_at=CASE WHEN $3 THEN NOW() ELSE last_success_at END,
        consecutive_failures=CASE WHEN $3 THEN 0 ELSE consecutive_failures+1 END WHERE client_id=$1 AND ($5::timestamptz IS NULL OR consented_at=$5)`,
      [cid,nextRunAt,['success','completed','no_digest','partial'].includes(status),JSON.stringify(cursor),expectedConsentAt]);
    });
  }
  async saveSnapshot({ clientId, snapshot: s, expectedConsentAt }) {
    const cid = tenant(clientId);
    if (!s?.provenance?.gmail_message_id || !s.window_start || !s.window_end || !s.metrics || !s.deltas_pct) throw failure('clarity_invalid_snapshot');
    return this.transaction(async db => {
      if (expectedConsentAt) {
        const grant=(await db.query('SELECT * FROM clarity_integrations WHERE client_id=$1 FOR SHARE',[cid])).rows[0];
        if (!grant?.enabled || grant.consent_status !== 'authorized' || new Date(grant.consented_at).getTime() !== new Date(expectedConsentAt).getTime()
          || grant.project_id !== s.project_id) throw failure('clarity_grant_changed',409);
      }
      const inserted = await db.query(`INSERT INTO clarity_snapshots(id,client_id,window_start,window_end,metrics,deltas_pct,provenance,synthetic)
        VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7::jsonb,$8)
        ON CONFLICT(client_id,window_start,window_end) DO NOTHING RETURNING *`,
      [randomUUID(),cid,s.window_start,s.window_end,JSON.stringify(s.metrics),JSON.stringify(s.deltas_pct),JSON.stringify(s.provenance),s.synthetic === true]);
      const snapshot = snapshotRow(inserted.rows[0] || (await db.query(`SELECT * FROM clarity_snapshots WHERE client_id=$1 AND window_start=$2 AND window_end=$3`,[cid,s.window_start,s.window_end])).rows[0]);
      await db.query(`INSERT INTO clarity_digest_deliveries(client_id,gmail_message_id,snapshot_id,window_start,window_end,provenance)
        VALUES($1,$2,$3,$4,$5,$6::jsonb) ON CONFLICT DO NOTHING`,
      [cid,s.provenance.gmail_message_id,snapshot.id,s.window_start,s.window_end,JSON.stringify(s.provenance)]);
      return { inserted: inserted.rows.length > 0, snapshot };
    });
  }
  async listSnapshots({ clientId, limit = 12 }) {
    return (await this.pool.query('SELECT * FROM clarity_snapshots WHERE client_id=$1 ORDER BY window_end DESC LIMIT $2',
      [tenant(clientId),Math.min(52,Math.max(1,limit))])).rows.map(snapshotRow);
  }
  async getSnapshot({ clientId, snapshotId }) {
    return snapshotRow((await this.pool.query('SELECT * FROM clarity_snapshots WHERE client_id=$1 AND id=$2', [tenant(clientId),id(snapshotId)])).rows[0]);
  }
  async saveRecommendations({ clientId, snapshotId, recommendations, crossEvidence, expectedConsentAt }) {
    const cid = tenant(clientId);
    if (!Array.isArray(recommendations) || recommendations.length > 3) throw failure('clarity_recommendation_limit');
    await this.transaction(async db => {
      if (expectedConsentAt) {
        const grant=(await db.query('SELECT enabled,consent_status,consented_at FROM clarity_integrations WHERE client_id=$1 FOR SHARE',[cid])).rows[0];
        if (!grant?.enabled || grant.consent_status !== 'authorized' || new Date(grant.consented_at).getTime() !== new Date(expectedConsentAt).getTime()) throw failure('clarity_grant_changed',409);
      }
      const snapshot = (await db.query('SELECT id FROM clarity_snapshots WHERE client_id=$1 AND id=$2 FOR UPDATE', [cid,id(snapshotId)])).rows[0];
      if (!snapshot) throw failure('clarity_not_found',404);
      for (const [index,content] of recommendations.entries()) {
        const hash = digest({ client_id: cid, snapshot_id: snapshotId, content });
        await db.query(`INSERT INTO clarity_recommendations(id,client_id,snapshot_id,recommendation_key,rank,digest,content)
          VALUES($1,$2,$3,$4,$5,$6,$7::jsonb) ON CONFLICT(client_id,snapshot_id,recommendation_key) DO NOTHING`,
        [randomUUID(),cid,snapshotId,content.key,index+1,hash,JSON.stringify(content)]);
      }
      await db.query('UPDATE clarity_snapshots SET cross_evidence=COALESCE(cross_evidence,$3::jsonb) WHERE client_id=$1 AND id=$2',
        [cid,snapshotId,JSON.stringify(crossEvidence || {})]);
    });
    return this.listRecommendations({ clientId: cid, snapshotId });
  }
  async listRecommendations({ clientId, snapshotId }) {
    return (await this.pool.query('SELECT * FROM clarity_recommendations WHERE client_id=$1 AND snapshot_id=$2 ORDER BY rank LIMIT 3',
      [tenant(clientId),id(snapshotId)])).rows.map(recommendationRow);
  }
  async getRecommendation({ clientId, recommendationId }) {
    return recommendationRow((await this.pool.query('SELECT * FROM clarity_recommendations WHERE client_id=$1 AND id=$2',
      [tenant(clientId),id(recommendationId)])).rows[0]);
  }
  async decide({ clientId, recommendationId, operatorId, decision, expectedDigest, idempotencyKey, requestId }) {
    const cid = tenant(clientId), rid = id(recommendationId);
    if (!['approve','reject'].includes(decision) || !/^[a-f0-9]{64}$/.test(expectedDigest || '')
      || !/^[\w-]{16,128}$/.test(idempotencyKey || '') || !/^cer_/.test(requestId || '')) throw failure('clarity_decision_binding_required');
    return this.transaction(async db => {
      const grant=(await db.query('SELECT enabled,consent_status FROM clarity_integrations WHERE client_id=$1 FOR SHARE',[cid])).rows[0];
      if (!grant?.enabled || grant.consent_status !== 'authorized') throw failure('clarity_disabled',403);
      const actor = (await db.query('SELECT id,role,client_id,active FROM users WHERE id=$1 FOR SHARE', [tenant(operatorId)])).rows[0];
      if (!actor || actor.active !== true || !['admin','manager'].includes(actor.role)
        || (actor.role === 'manager' && Number(actor.client_id) !== cid)) throw failure('clarity_approval_forbidden',403);
      const row = (await db.query('SELECT * FROM clarity_recommendations WHERE client_id=$1 AND id=$2 FOR UPDATE',[cid,rid])).rows[0];
      if (!row) throw failure('clarity_not_found',404);
      if (row.digest !== expectedDigest) throw failure('clarity_stale_review',409);
      const previous = (await db.query('SELECT * FROM clarity_review_decisions WHERE client_id=$1 AND idempotency_key=$2',[cid,idempotencyKey])).rows[0];
      if (previous) {
        if (previous.recommendation_id !== rid || previous.operator_id !== Number(operatorId) || previous.decision !== decision || previous.expected_digest !== expectedDigest) throw failure('clarity_idempotency_conflict',409);
        return { recommendation: recommendationRow(row), decision: { ...previous, status: row.status }, replay: true };
      }
      if (row.status !== 'pending') throw failure('clarity_already_reviewed',409);
      const audit = (await db.query(`INSERT INTO clarity_review_decisions(id,client_id,recommendation_id,operator_id,decision,expected_digest,idempotency_key,execution_request_id)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,[randomUUID(),cid,rid,operatorId,decision,expectedDigest,idempotencyKey,requestId])).rows[0];
      const updated = (await db.query(`UPDATE clarity_recommendations SET status=$3,decided_at=NOW() WHERE client_id=$1 AND id=$2 RETURNING *`,[cid,rid,decision === 'approve' ? 'approved' : 'rejected'])).rows[0];
      return { recommendation: recommendationRow(updated), decision: { ...audit, status: updated.status }, replay: false };
    });
  }
}
module.exports = { ClarityStore, snapshotRow, recommendationRow };
