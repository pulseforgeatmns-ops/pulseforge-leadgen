'use strict';

const { hash, fail } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { resolveOperatorDelegatedMaximumDailyCapacity } = require('../packages/emmett-outbound/OperatorDelegatedCapacity');
const { createGovernedOutboundTenantContext } = require('./governedOutboundTenant');

// Conservative ownership matching: a likely alias is held for review, never
// used to merge CRM records or to transfer an AO's account.
function ownershipNameKey(value) {
  return String(value || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim()
    .replace(/ (commercial|residential) (division|department)$/, '').split(' ')
    .filter(word => word && !['llc','inc','incorporated','ltd','limited','corp','corporation','co','company','properties','property','management'].includes(word)).join('');
}
function ownershipDomain(value) {
  const text = String(value || '').trim().toLowerCase();
  if (!text) return null;
  if (text.includes('@') && !text.includes('://')) return text.split('@').pop();
  try { return new URL(text.includes('://') ? text : `https://${text}`).hostname.replace(/^www\./, ''); }
  catch { return null; }
}

class GovernedOutboundStore {
  constructor(pool, tenantId) {
    this.pool = pool;
    this.ctx = createGovernedOutboundTenantContext(
      require('./governedOutboundContext').assertGovernedOutboundTenantRequired(tenantId)
    );
    this.tenantId = this.ctx.tenantId;
    this.clientId = this.ctx.clientId;
  }
  async one(sql, args = []) { return (await this.pool.query(sql, args)).rows[0] || null; }
  async event(type, key, data = {}, db = this.pool) {
    const event = await db.query(`INSERT INTO acquisition_outbound_events(id,tenant_id,program_id,envelope_id,item_id,event_type,payload)
      VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING RETURNING id`,
    [hash([type, key]), this.tenantId, data.programId || null, data.envelopeId || null, data.itemId || null, type, data]);
    const quiet = new Set(['spacing','outside_business_hours','weekend','cap_reached','not_started','environment_kill_switch','program_disabled','preparation_backoff']);
    if (event.rows.length && type === 'tick_blocked' && !quiet.has(data.reason)) {
      await db.query(`INSERT INTO agent_actions(created_by,action_type,title,description,payload,status,client_id)
        VALUES('max','governed_outbound_attention',$1,$2,$3,'pending',$4)`,
      [this.ctx.attentionTitle, data.reason, { ...data, eventId: event.rows[0].id, tenantId: this.tenantId }, this.clientId]);
    }
  }
  async program() {
    return this.one('SELECT * FROM acquisition_outbound_programs WHERE tenant_id=$1 AND mode<>\'revoked\' ORDER BY authorized_at DESC LIMIT 1', [this.tenantId]);
  }
  async createProgram(p, scopeHash, actor) {
    const id = `outbound_${hash([p, scopeHash, actor]).slice(0, 24)}`;
    const row = await this.one(`INSERT INTO acquisition_outbound_programs
      (id,tenant_id,source_mission_id,policy,policy_hash,scope_hash,authorized_by)
      VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO UPDATE SET id=EXCLUDED.id RETURNING *`,
    [id, this.tenantId, p.sourceMissionId, p, hash(p), scopeHash, actor]);
    await this.event('program_authorized', id, { programId: id, policy: p, scopeHash, actor });
    return row;
  }
  async migrateProgramPolicy(program, nextPolicy, actor, authorization = {}) {
    if (!program?.id) fail('program_not_found');
    const policyHash = hash(nextPolicy);
    const row = await this.one(`UPDATE acquisition_outbound_programs
      SET policy=$2, policy_hash=$3, authorized_by=$4, authorized_at=now()
      WHERE id=$1 AND tenant_id=$5 RETURNING *`,
    [program.id, nextPolicy, policyHash, String(actor), this.tenantId]);
    if (!row) fail('program_not_found');
    await this.event('program_policy_migrated', [program.id, policyHash], {
      programId: program.id,
      previousPolicyHash: program.policy_hash,
      policy: nextPolicy,
      policyHash,
      actor,
      authorization,
    });
    return row;
  }
  async mode(program, mode, actor) {
    if (!['shadow', 'active', 'paused', 'revoked'].includes(mode)) fail('invalid_mode');
    const db = await this.pool.connect();
    try {
      await db.query('BEGIN');
      await db.query('UPDATE acquisition_outbound_programs SET mode=$2 WHERE id=$1 AND mode<>\'revoked\'', [program.id, mode]);
      if (mode === 'paused' || mode === 'revoked') {
        await db.query(`UPDATE acquisition_outbound_items i SET status='suppressed', reason='operator_kill'
          FROM acquisition_outbound_envelopes e WHERE e.program_id=$1 AND i.envelope_id=e.id AND i.status='pending'`, [program.id]);
        await db.query("UPDATE acquisition_outbound_envelopes SET status='cancelled' WHERE program_id=$1 AND status IN ('frozen','authorized')", [program.id]);
      }
      await this.event('program_mode_changed', [program.id, mode, Date.now()], { programId: program.id, mode, actor }, db);
      await db.query('COMMIT');
    } catch (e) { await db.query('ROLLBACK'); throw e; } finally { db.release(); }
  }
  async lock(fn) {
    const db = await this.pool.connect();
    let locked = false;
    try {
      locked = (await db.query('SELECT pg_try_advisory_lock($1,$2) AS locked', [this.ctx.advisoryLockNamespace, this.ctx.advisoryLockKey])).rows[0].locked;
      if (!locked) return { halted: 'overlap' };
      return await fn();
    } finally {
      if (locked) await db.query('SELECT pg_advisory_unlock($1,$2)', [this.ctx.advisoryLockNamespace, this.ctx.advisoryLockKey]);
      db.release();
    }
  }
  async envelope(day) {
    return this.one('SELECT * FROM acquisition_outbound_envelopes WHERE tenant_id=$1 AND local_day=$2::date', [this.tenantId, day]);
  }
  async items(id) {
    return (await this.pool.query('SELECT * FROM acquisition_outbound_items WHERE envelope_id=$1 ORDER BY id', [id])).rows;
  }
  async ensurePreparation(program, day, db = this.pool) {
    const missionId = `mission_daily_${hash([program.id, day]).slice(0, 24)}`;
    const inserted = await db.query(`INSERT INTO acquisition_outbound_preparation(program_id,local_day,mission_id)
      VALUES($1,$2,$3) ON CONFLICT DO NOTHING RETURNING *`, [program.id, day, missionId]);
    const progress = inserted.rows[0] || (await db.query(
      'SELECT * FROM acquisition_outbound_preparation WHERE program_id=$1 AND local_day=$2', [program.id, day])).rows[0];
    return { created: inserted.rows.length === 1, progress };
  }
  async freeze(program, day, missionId, revision, manifest, options = {}) {
    const id = `daily_${hash([this.tenantId, day]).slice(0, 24)}`;
    const db = await this.pool.connect();
    try {
      await db.query('BEGIN');
      if (options.requireShadow) {
        const current = (await db.query('SELECT mode,policy_hash,scope_hash FROM acquisition_outbound_programs WHERE id=$1 FOR UPDATE', [program.id])).rows[0];
        if (current?.mode !== 'shadow' || current.policy_hash !== program.policy_hash
          || current.scope_hash !== program.scope_hash) fail('replenishment_grant_changed');
      }
      await db.query(`INSERT INTO acquisition_outbound_envelopes
        (id,program_id,tenant_id,local_day,mission_id,revision,manifest,manifest_hash,status)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,'frozen')`, [id, program.id, this.tenantId, day, missionId, revision, JSON.stringify(manifest), hash(manifest)]);
      for (const [n, item] of manifest.entries()) {
        await db.query(`INSERT INTO acquisition_outbound_items
          (id,envelope_id,tenant_id,candidate_id,prospect_id,company_id,email,snapshot)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
        [`${id}_${n}`, id, this.tenantId, item.candidateId, item.prospectId, item.companyId, item.email, item]);
      }
      await this.event('envelope_frozen', id, { programId: program.id, envelopeId: id,
        missionId, revision, manifestHash: hash(manifest), count: manifest.length }, db);
      await db.query('COMMIT');
    } catch (e) { await db.query('ROLLBACK'); throw e; } finally { db.release(); }
    return this.envelope(day);
  }
  async appendToEnvelope(envelope, extraManifest = [], revision = null) {
    const current = Array.isArray(envelope.manifest) ? envelope.manifest : [];
    const extra = Array.isArray(extraManifest) ? extraManifest : [];
    if (!extra.length) return this.one('SELECT * FROM acquisition_outbound_envelopes WHERE id=$1', [envelope.id]);
    const combined = [...current, ...extra];
    const db = await this.pool.connect();
    try {
      await db.query('BEGIN');
      const start = current.length;
      for (const [n, item] of extra.entries()) {
        await db.query(`INSERT INTO acquisition_outbound_items
          (id,envelope_id,tenant_id,candidate_id,prospect_id,company_id,email,snapshot)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
        [`${envelope.id}_${start + n}`, envelope.id, this.tenantId, item.candidateId, item.prospectId, item.companyId, item.email, item]);
      }
      const nextRevision = revision || envelope.revision;
      await db.query(`UPDATE acquisition_outbound_envelopes
        SET manifest=$2::jsonb, manifest_hash=$3, revision=$4,
            status=CASE WHEN status='complete' THEN 'authorized' ELSE status END
        WHERE id=$1`,
      [envelope.id, JSON.stringify(combined), hash(combined), nextRevision]);
      await this.event('envelope_refilled', [envelope.id, extra.length, Date.now()], {
        programId: envelope.program_id, envelopeId: envelope.id,
        added: extra.length, revision: nextRevision, manifestHash: hash(combined),
      }, db);
      await db.query('COMMIT');
    } catch (e) { await db.query('ROLLBACK'); throw e; } finally { db.release(); }
    return this.one('SELECT * FROM acquisition_outbound_envelopes WHERE id=$1', [envelope.id]);
  }
  async approve(envelope, approvalId) {
    await this.pool.query("UPDATE acquisition_outbound_envelopes SET status='authorized',approval_id=$2 WHERE id=$1 AND status='frozen'", [envelope.id, approvalId]);
    await this.event('envelope_authorized', envelope.id, { envelopeId: envelope.id, approvalId });
  }
  async expire(day) {
    const db = await this.pool.connect();
    try {
      await db.query('BEGIN');
      const expired = await db.query(`UPDATE acquisition_outbound_items i SET status='expired',reason='day_expired'
        FROM acquisition_outbound_envelopes e WHERE i.envelope_id=e.id AND e.local_day<$1::date AND i.tenant_id=$2 AND i.status='pending' RETURNING i.*`, [day, this.tenantId]);
      await db.query("UPDATE acquisition_outbound_envelopes SET status='expired' WHERE local_day<$1::date AND tenant_id=$2 AND status IN ('frozen','authorized')", [day, this.tenantId]);
      // An abandoned attempt is never returned to pending after a worker crash.
      const abandoned = await db.query("UPDATE acquisition_outbound_items SET status='uncertain',reason='abandoned_attempt' WHERE tenant_id=$1 AND status='attempted' AND attempted_at<now()-interval '5 minutes' RETURNING *", [this.tenantId]);
      for (const item of [...expired.rows, ...abandoned.rows]) {
        await this.event(`send_${item.status}`, item.id, { itemId: item.id, envelopeId: item.envelope_id, reason: item.reason }, db);
      }
      await db.query('COMMIT');
    } catch (e) { await db.query('ROLLBACK'); throw e; } finally { db.release(); }
  }
  async counts(program, day) {
    return this.one(`SELECT
      count(*) FILTER(WHERE e.local_day=$2::date)::int AS today,
      count(*) FILTER(WHERE e.program_id=$1)::int AS total,
      count(*) FILTER(WHERE i.status IN ('attempted','uncertain'))::int AS uncertain,
      max(i.attempted_at) AS last_attempt
      FROM acquisition_outbound_items i JOIN acquisition_outbound_envelopes e ON e.id=i.envelope_id
      WHERE i.tenant_id=$3 AND i.attempted_at IS NOT NULL`, [program.id, day, this.tenantId]);
  }
  async rampMetrics(program, day) {
    const row = await this.one(`SELECT
      count(*) FILTER (WHERE i.status='sent' AND i.provider_message_id IS NOT NULL)::int AS sent,
      count(*) FILTER (WHERE i.status='sent' AND i.provider_message_id IS NOT NULL
        AND (p.policy->>'maxSequenceStep')::int=1)::int AS new_first_touches,
      count(*) FILTER (WHERE i.status='pending')::int AS pending
      FROM acquisition_outbound_items i JOIN acquisition_outbound_envelopes e ON e.id=i.envelope_id
      JOIN acquisition_outbound_programs p ON p.id=e.program_id
      WHERE i.tenant_id=$1 AND e.local_day=$2::date AND e.program_id=$3`, [this.tenantId, day, program.id]);
    return { firstTouchDailyTarget: 5, firstTouchOnly: program.policy.maxSequenceStep === 1,
      providerAcceptedSends: row.sent, newFirstTouches: row.new_first_touches,
      followUps: program.policy.maxSequenceStep === 1 ? 0 : null, pending: row.pending,
      firstTouchDeficit: Math.max(0, 5 - row.new_first_touches) };
  }
  async candidateOwnership(candidate) {
    const company = candidate.companyId
      ? await this.one("SELECT name,to_jsonb(c)->>'domain' AS domain,to_jsonb(c)->>'website' AS website FROM companies c WHERE client_id=$2 AND id::text=$1", [String(candidate.companyId), this.clientId])
      : null;
    const names = new Set([candidate.company, company?.name].map(ownershipNameKey).filter(Boolean));
    const domains = new Set([candidate.domain, candidate.website, candidate.email, company?.domain, company?.website].map(ownershipDomain).filter(Boolean));
    const related = await this.pool.query(`SELECT p.id,p.company_id,p.email,p.assigned_ao_id,p.last_contacted_at,
      p.do_not_contact,p.closer_id,p.last_reply_at,c.name,c.domain,c.website,
      EXISTS(SELECT 1 FROM ao_prospect_tasks t WHERE t.client_id=$1 AND t.prospect_id=p.id) AS has_ao_task,
      EXISTS(SELECT 1 FROM touchpoints t WHERE t.client_id=$1 AND t.prospect_id=p.id
        AND t.action_type IN ('email_sent','sent','outbound_email','call','call_attempt','inbound_reply','reply','email_reply','reply_received')) AS prior_touch
      FROM prospects p JOIN companies c ON c.id=p.company_id AND c.client_id=$1
      WHERE p.client_id=$1 AND (p.assigned_ao_id IS NOT NULL OR p.last_contacted_at IS NOT NULL
        OR p.do_not_contact OR p.closer_id IS NOT NULL OR p.last_reply_at IS NOT NULL
        OR EXISTS(SELECT 1 FROM ao_prospect_tasks t WHERE t.client_id=$1 AND t.prospect_id=p.id)
        OR EXISTS(SELECT 1 FROM touchpoints t WHERE t.client_id=$1 AND t.prospect_id=p.id
          AND t.action_type IN ('email_sent','sent','outbound_email','call','call_attempt','inbound_reply','reply','email_reply','reply_received')))`, [this.clientId]);
    if (related.rows.some(row => String(row.id) === String(candidate.prospectId || '')
      || String(row.company_id) === String(candidate.companyId || '') || names.has(ownershipNameKey(row.name))
      || [row.email,row.domain,row.website].some(value => domains.has(ownershipDomain(value))))) return 'prior_contact_or_human_owned';
    const { rows } = await this.pool.query(`SELECT l.id,l.business_name,p.email,c.name AS linked_company,
      to_jsonb(c)->>'domain' AS domain,to_jsonb(c)->>'website' AS website,
      COALESCE((SELECT jsonb_agg(a.email) FROM ao_contacts a WHERE a.lead_id=l.id AND a.email IS NOT NULL),'[]'::jsonb) AS emails
      FROM ao_leads l LEFT JOIN prospects p ON p.id=l.crm_prospect_id AND p.client_id=$1
      LEFT JOIN companies c ON c.id=p.company_id AND c.client_id=$1 WHERE l.client_id=$1`, [this.clientId]);
    return rows.some(row => names.has(ownershipNameKey(row.business_name)) || names.has(ownershipNameKey(row.linked_company))
      || [row.email, row.domain, row.website, ...(row.emails || [])].some(value => domains.has(ownershipDomain(value))))
      ? 'ao_owned_alias' : null;
  }
  async suppression(item, ignoreMissionId = '', ignoreItemId = '') {
    const hit = await this.one(`SELECT state FROM acquisition_outbound_lifecycle WHERE tenant_id=$3 AND suppressed=true
      AND (email=$1 OR company_id=$2) LIMIT 1`, [item.email.toLowerCase(), String(item.companyId), this.tenantId]);
    if (hit) return hit.state;
    const prior = await this.one(`SELECT id FROM acquisition_outbound_items WHERE tenant_id=$3
      AND attempted_at IS NOT NULL AND id<>$4 AND (email=$1 OR company_id=$2) LIMIT 1`, [item.email.toLowerCase(), String(item.companyId), this.tenantId, ignoreItemId]);
    if (prior) return 'already_attempted';
    const canonical = await this.one(`SELECT id FROM acquisition_mission_outbound_executions WHERE tenant_id=$4
      AND status IN ('sent','attempted','failed') AND mission_id<>$3
      AND (lower(payload->>'email')=$1 OR prospect_id=$2) LIMIT 1`,
    [item.email.toLowerCase(), String(item.candidateId), ignoreMissionId, this.tenantId]);
    if (canonical) return 'prior_canonical_execution';
    const history = await this.one(`SELECT p.id FROM prospects p WHERE p.client_id=$2 AND p.id::text=$1 AND (
      lower(COALESCE(to_jsonb(p)->>'operational_status','')) IN ('booked','converted','client','do_not_email','bounced','replied')
      OR lower(COALESCE(to_jsonb(p)->>'setter_status','')) IN ('booked','appointment_set','won')
      OR NULLIF(to_jsonb(p)->>'closer_status','') IS NOT NULL
      OR EXISTS(SELECT 1 FROM touchpoints t WHERE t.prospect_id=p.id AND t.client_id=$2
        AND t.action_type IN ('inbound_reply','reply','email_reply','reply_received','unsubscribed','out_of_office'))
    )`, [String(item.prospectId), this.clientId]);
    if (history) return 'prior_reply_or_booked';
    const ao = await this.one(`SELECT l.id FROM ao_leads l JOIN prospects p ON
      (p.id=l.crm_prospect_id OR EXISTS(SELECT 1 FROM companies c WHERE c.id=p.company_id AND c.client_id=$3
        AND lower(trim(c.name))=lower(trim(l.business_name))))
      WHERE l.client_id=$3 AND p.client_id=$3 AND (p.id::text=$1 OR p.company_id::text=$2) LIMIT 1`,
    [String(item.prospectId), String(item.companyId), this.clientId]);
    return ao ? 'ao_owned' : this.candidateOwnership(item);
  }
  async finish(item, status, reason, providerMessageId = null) {
    const db = await this.pool.connect();
    try {
      await db.query('BEGIN');
      await db.query(`UPDATE acquisition_outbound_items SET status=$2,reason=$3,
        provider_message_id=COALESCE($4,provider_message_id) WHERE id=$1`, [item.id, status, reason, providerMessageId]);
      await this.event(`send_${status}`, item.id, { itemId: item.id, envelopeId: item.envelope_id, reason, providerMessageId }, db);
      await db.query('COMMIT');
    } catch (e) { await db.query('ROLLBACK'); throw e; } finally { db.release(); }
  }
  async releaseUnsent(item, reason, extras = {}) {
    const db = await this.pool.connect();
    try {
      await db.query('BEGIN');
      const allowTerminalPreProviderReconciliation = extras.reconciled === true
        && extras.providerOutcome === 'PROVIDER_CONFIRMED_NOT_SENT';
      if (extras.canonicalPreProviderRejection === true && extras.missionId) {
        await db.query(`UPDATE acquisition_mission_outbound_executions
          SET status='reconciled_not_sent',
              provider_error_code=COALESCE(provider_error_code,$5),
              provider_error_message=COALESCE(provider_error_message,'Provider boundary not crossed; eligibility released.'),
              updated_at=now()
          WHERE tenant_id=$1 AND mission_id=$2
            AND status IN ('attempted','failed')
            AND provider_message_id IS NULL AND sent_at IS NULL
            AND (prospect_id=$3 OR lower(payload->>'email')=$4)`,
        [this.tenantId, String(extras.missionId), String(item.prospect_id || item.candidate_id || ''),
          String(item.email || '').toLowerCase(), String(reason || 'pre_provider_rejected')]);
      }
      const row = (await db.query(`UPDATE acquisition_outbound_items SET status='pending',reason=$2,
        attempted_at=NULL,provider_message_id=NULL WHERE id=$1
        AND (status IN ('attempted','uncertain') OR ($3::boolean AND status='suppressed')) RETURNING *`,
      [item.id, reason, allowTerminalPreProviderReconciliation])).rows[0];
      if (!row) fail('item_not_uncertain');
      const eventType = extras.reconciled ? 'send_reconciled' : 'send_released_unsent';
      await this.event(eventType, extras.reconciled ? [item.id, 'not_accepted'] : [item.id, reason, Date.now()], {
        itemId: item.id, envelopeId: item.envelope_id, outcome: 'not_accepted', reason,
        providerMessageId: null, evidence: extras.evidence || null, actor: extras.actor || null,
        providerOutcome: extras.providerOutcome || 'PROVIDER_CONFIRMED_NOT_SENT',
      }, db);
      await db.query('COMMIT');
      return row;
    } catch (e) { await db.query('ROLLBACK'); throw e; } finally { db.release(); }
  }
  async claim(item, program, day, at = new Date()) {
    // All counters and the final enabled/suppression checks share this short transaction.
    // The durable attempted marker commits BEFORE the irreversible provider call.
    const db = await this.pool.connect();
    try {
      await db.query('BEGIN');
      const p = (await db.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1 FOR UPDATE', [program.id])).rows[0];
      if (p?.mode !== 'active') fail('program_not_active');
      const counts = (await db.query(`SELECT count(*) FILTER(WHERE e.local_day=$2::date)::int AS today,
        count(*) FILTER(WHERE e.program_id=$1)::int AS total,max(i.attempted_at) AS last_attempt,
        count(*) FILTER(WHERE i.status IN ('attempted','uncertain'))::int AS uncertain
        FROM acquisition_outbound_items i JOIN acquisition_outbound_envelopes e ON e.id=i.envelope_id
        WHERE i.tenant_id=$3 AND i.attempted_at IS NOT NULL`, [p.id, day, this.tenantId])).rows[0];
      const operatorDailyCeiling = resolveOperatorDelegatedMaximumDailyCapacity(p.policy) ?? p.policy.dailyCap;
      if (counts.today >= operatorDailyCeiling || counts.total >= p.policy.totalCap || counts.uncertain) fail('budget_or_uncertain_block');
      if (counts.last_attempt && +at - +new Date(counts.last_attempt) < p.policy.spacingMinutes * 60000) fail('spacing');
      const row = (await db.query(`UPDATE acquisition_outbound_items i SET status='attempted',attempted_at=$2
        WHERE i.id=$1 AND i.status='pending' AND NOT EXISTS
        (SELECT 1 FROM acquisition_outbound_lifecycle s WHERE s.tenant_id=i.tenant_id AND s.suppressed
          AND (s.email=i.email OR s.company_id=i.company_id)) RETURNING *`, [item.id, at])).rows[0];
      if (!row) fail('suppressed_or_claimed');
      await this.event('send_attempted', item.id, { itemId: item.id, envelopeId: item.envelope_id }, db);
      await db.query('COMMIT');
      return row;
    } catch (e) { await db.query('ROLLBACK'); throw e; } finally { db.release(); }
  }
  async health(program, error = null) {
    await this.pool.query('UPDATE acquisition_outbound_programs SET last_tick_at=now(),last_error=$2 WHERE id=$1', [program.id, error]);
  }
  async status() {
    const program = await this.program();
    const events = (await this.pool.query('SELECT * FROM acquisition_outbound_events WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 50', [this.tenantId])).rows;
    const envelopes = (await this.pool.query(`SELECT e.*, (SELECT jsonb_object_agg(status,n) FROM (SELECT status,count(*) AS n FROM acquisition_outbound_items WHERE envelope_id=e.id GROUP BY status) s) AS counts FROM acquisition_outbound_envelopes e WHERE tenant_id=$1 ORDER BY local_day DESC LIMIT 10`, [this.tenantId])).rows;
    const inboxHealth = (await this.pool.query('SELECT * FROM acquisition_outbound_inbox_health WHERE tenant_id=$1', [this.tenantId])).rows;
    const replyBacklog = (await this.pool.query('SELECT id,prospect_id,email,attempts,last_error,created_at FROM acquisition_outbound_replies WHERE tenant_id=$1 AND classified_at IS NULL ORDER BY created_at LIMIT 50', [this.tenantId])).rows;
    return { tenantId: this.tenantId, program, envelopes, events, inboxHealth, replyBacklog };
  }
}
module.exports = { GovernedOutboundStore, ownershipNameKey, ownershipDomain };
