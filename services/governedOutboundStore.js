'use strict';

const { hash, fail } = require('../packages/acquisition-mission/DailyOutboundPolicy');

class GovernedOutboundStore {
  constructor(pool) { this.pool = pool; }
  async one(sql, args = []) { return (await this.pool.query(sql, args)).rows[0] || null; }
  async event(type, key, data = {}, db = this.pool) {
    const event = await db.query(`INSERT INTO acquisition_outbound_events(id,tenant_id,program_id,envelope_id,item_id,event_type,payload)
      VALUES($1,'10',$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING id`,
    [hash([type, key]), data.programId || null, data.envelopeId || null, data.itemId || null, type, data]);
    const quiet = new Set(['spacing','outside_business_hours','weekend','cap_reached','not_started','environment_kill_switch','program_disabled','preparation_backoff']);
    if (event.rows.length && type === 'tick_blocked' && !quiet.has(data.reason)) {
      await db.query(`INSERT INTO agent_actions(created_by,action_type,title,description,payload,status,client_id)
        VALUES('max','governed_outbound_attention','Anchor outbound needs attention',$1,$2,'pending',10)`,
      [data.reason, { ...data, eventId: event.rows[0].id }]);
    }
  }
  async program() { return this.one("SELECT * FROM acquisition_outbound_programs WHERE tenant_id='10' AND mode<>'revoked'"); }
  async createProgram(p, scopeHash, actor) {
    const id = `outbound_${hash([p, scopeHash, actor]).slice(0, 24)}`;
    const row = await this.one(`INSERT INTO acquisition_outbound_programs
      (id,tenant_id,source_mission_id,policy,policy_hash,scope_hash,authorized_by)
      VALUES($1,'10',$2,$3,$4,$5,$6) ON CONFLICT(id) DO UPDATE SET id=EXCLUDED.id RETURNING *`,
    [id, p.sourceMissionId, p, hash(p), scopeHash, actor]);
    await this.event('program_authorized', id, { programId: id, policy: p, scopeHash, actor });
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
      locked = (await db.query('SELECT pg_try_advisory_lock(261018,10) AS locked')).rows[0].locked;
      if (!locked) return { halted: 'overlap' };
      return await fn();
    } finally {
      if (locked) await db.query('SELECT pg_advisory_unlock(261018,10)');
      db.release();
    }
  }
  async envelope(day) {
    return this.one('SELECT * FROM acquisition_outbound_envelopes WHERE tenant_id=\'10\' AND local_day=$1::date', [day]);
  }
  async items(id) {
    return (await this.pool.query('SELECT * FROM acquisition_outbound_items WHERE envelope_id=$1 ORDER BY id', [id])).rows;
  }
  async freeze(program, day, missionId, revision, manifest) {
    const id = `daily_${hash(['10', day]).slice(0, 24)}`;
    const db = await this.pool.connect();
    try {
      await db.query('BEGIN');
      await db.query(`INSERT INTO acquisition_outbound_envelopes
        (id,program_id,tenant_id,local_day,mission_id,revision,manifest,manifest_hash,status)
        VALUES($1,$2,'10',$3,$4,$5,$6,$7,'frozen')`, [id, program.id, day, missionId, revision, JSON.stringify(manifest), hash(manifest)]);
      for (const [n, item] of manifest.entries()) {
        await db.query(`INSERT INTO acquisition_outbound_items
          (id,envelope_id,tenant_id,candidate_id,prospect_id,company_id,email,snapshot)
          VALUES($1,$2,'10',$3,$4,$5,$6,$7)`,
        [`${id}_${n}`, id, item.candidateId, item.prospectId, item.companyId, item.email, item]);
      }
      await this.event('envelope_frozen', id, { programId: program.id, envelopeId: id,
        missionId, revision, manifestHash: hash(manifest), count: manifest.length }, db);
      await db.query('COMMIT');
    } catch (e) { await db.query('ROLLBACK'); throw e; } finally { db.release(); }
    return this.envelope(day);
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
        FROM acquisition_outbound_envelopes e WHERE i.envelope_id=e.id AND e.local_day<$1::date AND i.status='pending' RETURNING i.*`, [day]);
      await db.query("UPDATE acquisition_outbound_envelopes SET status='expired' WHERE local_day<$1::date AND status IN ('frozen','authorized')", [day]);
      // An abandoned attempt is never returned to pending after a worker crash.
      const abandoned = await db.query("UPDATE acquisition_outbound_items SET status='uncertain',reason='abandoned_attempt' WHERE status='attempted' AND attempted_at<now()-interval '5 minutes' RETURNING *");
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
      WHERE i.tenant_id='10' AND i.attempted_at IS NOT NULL`, [program.id, day]);
  }
  async suppression(item, ignoreMissionId = '') {
    const hit = await this.one(`SELECT state FROM acquisition_outbound_lifecycle WHERE tenant_id='10' AND suppressed=true
      AND (email=$1 OR company_id=$2) LIMIT 1`, [item.email.toLowerCase(), String(item.companyId)]);
    if (hit) return hit.state;
    const prior = await this.one(`SELECT id FROM acquisition_outbound_items WHERE tenant_id='10'
      AND attempted_at IS NOT NULL AND (email=$1 OR company_id=$2) LIMIT 1`, [item.email.toLowerCase(), String(item.companyId)]);
    if (prior) return 'already_attempted';
    const canonical = await this.one(`SELECT id FROM acquisition_mission_outbound_executions WHERE tenant_id='10'
      AND status IN ('sent','attempted','failed') AND mission_id<>$3
      AND (lower(payload->>'email')=$1 OR prospect_id=$2) LIMIT 1`,
    [item.email.toLowerCase(), String(item.candidateId), ignoreMissionId]);
    if (canonical) return 'prior_canonical_execution';
    const history = await this.one(`SELECT p.id FROM prospects p WHERE p.client_id=10 AND p.id::text=$1 AND (
      lower(COALESCE(to_jsonb(p)->>'operational_status','')) IN ('booked','converted','client','do_not_email','bounced','replied')
      OR lower(COALESCE(to_jsonb(p)->>'setter_status','')) IN ('booked','appointment_set','won')
      OR NULLIF(to_jsonb(p)->>'closer_status','') IS NOT NULL
      OR EXISTS(SELECT 1 FROM touchpoints t WHERE t.prospect_id=p.id AND t.client_id=10
        AND t.action_type IN ('inbound_reply','reply','email_reply','reply_received','unsubscribed','out_of_office'))
    )`, [String(item.prospectId)]);
    if (history) return 'prior_reply_or_booked';
    const ao = await this.one(`SELECT l.id FROM ao_leads l JOIN prospects p ON
      (p.id=l.crm_prospect_id OR EXISTS(SELECT 1 FROM companies c WHERE c.id=p.company_id AND c.client_id=10
        AND lower(trim(c.name))=lower(trim(l.business_name))))
      WHERE l.client_id=10 AND p.client_id=10 AND (p.id::text=$1 OR p.company_id::text=$2) LIMIT 1`,
    [String(item.prospectId), String(item.companyId)]);
    return ao ? 'ao_owned' : null;
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
        WHERE i.tenant_id='10' AND i.attempted_at IS NOT NULL`, [p.id, day])).rows[0];
      if (counts.today >= p.policy.dailyCap || counts.total >= p.policy.totalCap || counts.uncertain) fail('budget_or_uncertain_block');
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
    const events = (await this.pool.query("SELECT * FROM acquisition_outbound_events WHERE tenant_id='10' ORDER BY created_at DESC LIMIT 50")).rows;
    const envelopes = (await this.pool.query("SELECT e.*, (SELECT jsonb_object_agg(status,n) FROM (SELECT status,count(*) AS n FROM acquisition_outbound_items WHERE envelope_id=e.id GROUP BY status) s) AS counts FROM acquisition_outbound_envelopes e WHERE tenant_id='10' ORDER BY local_day DESC LIMIT 10")).rows;
    const inboxHealth = (await this.pool.query("SELECT * FROM acquisition_outbound_inbox_health WHERE tenant_id='10'")).rows;
    const replyBacklog = (await this.pool.query("SELECT id,prospect_id,email,attempts,last_error,created_at FROM acquisition_outbound_replies WHERE tenant_id='10' AND classified_at IS NULL ORDER BY created_at LIMIT 50")).rows;
    return { program, envelopes, events, inboxHealth, replyBacklog };
  }
}
module.exports = { GovernedOutboundStore };
