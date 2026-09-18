'use strict';

const { hash, nextAction } = require('../packages/acquisition-mission/DailyOutboundPolicy');

async function installed(pool) {
  return Boolean((await pool.query("SELECT to_regclass('acquisition_outbound_programs') AS installed")).rows[0]?.installed);
}
function address(value) {
  const text = String(value?.address || value || '').trim().toLowerCase();
  return text.match(/<([^<>]+)>/)?.[1] || text;
}

async function captureRaw(pool, integration, raw) {
  if (String(integration.tenantId) !== '10' || !await installed(pool)) return;
  const program = (await pool.query("SELECT * FROM acquisition_outbound_programs WHERE tenant_id='10' AND policy->>'inboxIntegrationId'=$1 ORDER BY authorized_at DESC LIMIT 1", [integration.id])).rows[0];
  if (!program || program.policy.inboxIntegrationId !== integration.id) return;
  const email = address(raw.from || raw.sender);
  const contacts = (await pool.query('SELECT id,company_id FROM prospects WHERE client_id=10 AND lower(email)=$1', [email])).rows;
  // From-address matching intentionally stops all generic contact to this account,
  // even if In-Reply-To is absent or a human responds on a new thread.
  for (const contact of contacts) {
    const id = `reply_${hash([integration.id, raw.rfcMessageId || raw.messageId || raw.uid, email, contact.id]).slice(0, 32)}`;
    const db = await pool.connect();
    try {
      await db.query('BEGIN');
      await db.query("SELECT acquisition_outbound_suppress('10',$1,$2,$3,$4,'reply_received',$5)",
        [email, String(contact.id), String(contact.company_id), id, { integrationId: integration.id }]);
      await db.query(`INSERT INTO acquisition_outbound_replies(id,tenant_id,prospect_id,email,payload)
        VALUES($1,'10',$2,$3,$4) ON CONFLICT DO NOTHING`, [id, String(contact.id), email,
        { from: email, subject: raw.subject || '', body: raw.body || raw.text || '', inReplyTo: raw.inReplyTo || null }]);
      await db.query('COMMIT');
    } catch (e) { await db.query('ROLLBACK'); throw e; } finally { db.release(); }
  }
}

async function markHealthy(pool, integration) {
  if (String(integration.tenantId) !== '10' || !await installed(pool)) return;
  await pool.query(`INSERT INTO acquisition_outbound_inbox_health(integration_id,tenant_id,last_success_at)
    VALUES($1,'10',now()) ON CONFLICT(integration_id) DO UPDATE SET last_success_at=now()`, [integration.id]);
}

async function classifyPending(pool, options = {}) {
  const classify = options.classify || (email => require('../rileyAgent').classifyReply(email, { anchor: true }));
  const db = await pool.connect();
  let locked = false;
  let classified = 0;
  try {
    locked = (await db.query('SELECT pg_try_advisory_lock(261019,10) AS locked')).rows[0].locked;
    if (!locked) return { classified, halted: 'overlap' };
    const rows = (await db.query("SELECT * FROM acquisition_outbound_replies WHERE tenant_id='10' AND classified_at IS NULL AND attempts<3 ORDER BY created_at LIMIT 20")).rows;
    for (const row of rows) {
      await db.query('UPDATE acquisition_outbound_replies SET attempts=attempts+1 WHERE id=$1', [row.id]);
      try {
        const result = await classify(row.payload);
        const [state, action] = nextAction(result.classification);
        await db.query('BEGIN');
        await db.query(`UPDATE acquisition_outbound_lifecycle SET
          state=CASE WHEN state='dnc' THEN state ELSE $2 END,
          next_action=CASE WHEN state='dnc' OR $2='dnc' THEN 'stop' WHEN next_action='ao_owned' THEN 'ao_owned' ELSE $3 END,
          classification=$4,last_event_at=now(),suppressed=true WHERE tenant_id='10' AND email=$1`,
        [row.email, state, action, result.classification]);
        if (state === 'dnc') await db.query('UPDATE prospects SET do_not_contact=true WHERE client_id=10 AND lower(email)=$1', [row.email]);
        const current = (await db.query("SELECT * FROM acquisition_outbound_lifecycle WHERE tenant_id='10' AND email=$1", [row.email])).rows[0];
        const next = current?.next_action || action;
        if (next !== 'stop') {
          await db.query(`INSERT INTO agent_actions(created_by,action_type,title,description,payload,status,client_id)
            VALUES('max','governed_outbound_handoff',$1,$2,$3,'pending',10)`,
          [`Anchor reply: ${result.classification}`, `Max next action: ${next}. Generic follow-up is suppressed.`,
            { replyId: row.id, prospectId: row.prospect_id, email: row.email, classification: result.classification, nextAction: next }]);
          if (next === 'ao_handoff') {
            const program = (await db.query("SELECT * FROM acquisition_outbound_programs WHERE tenant_id='10' ORDER BY (mode<>'revoked') DESC,authorized_at DESC LIMIT 1")).rows[0];
            const existing = (await db.query('SELECT id,ao_owner_id FROM ao_leads WHERE client_id=10 AND crm_prospect_id::text=$1 LIMIT 1', [row.prospect_id])).rows[0];
            const owner = existing?.ao_owner_id || (await db.query(`SELECT id FROM users WHERE client_id=10 AND active=true
              AND id=ANY($1::int[]) ORDER BY (SELECT count(*) FROM ao_follow_up_tasks WHERE ao_owner_id=users.id AND status='open'),id LIMIT 1`,
            [program?.policy.aoOwnerIds || []])).rows[0]?.id;
            if (owner) {
              const lead = existing || (await db.query(`INSERT INTO ao_leads(client_id,business_name,ao_owner_id,crm_prospect_id,attribution_source,original_visit_note)
                SELECT 10,COALESCE(c.name,p.email),$2,p.id,'governed_outbound',$3 FROM prospects p
                LEFT JOIN companies c ON c.id=p.company_id AND c.client_id=10 WHERE p.id::text=$1 AND p.client_id=10 RETURNING id`,
              [row.prospect_id, owner, `Email reply classified by Riley: ${result.classification}`])).rows[0];
              await db.query(`INSERT INTO ao_follow_up_tasks(lead_id,ao_owner_id,due_date,next_action,last_interaction_summary)
                VALUES($1,$2,(now() AT TIME ZONE 'America/New_York')::date,$3,$4)`,
              [lead.id, owner, 'Review the reply and personally continue the conversation.', result.reason || result.classification]);
            }
          }
        }
        await db.query(`INSERT INTO acquisition_outbound_events(id,tenant_id,event_type,payload)
          VALUES($1,'10','reply_classified',$2) ON CONFLICT DO NOTHING`,
        [`classified:${row.id}`, { replyId: row.id, prospectId: row.prospect_id, classification: result.classification, state: current?.state || state, nextAction: next }]);
        await db.query('UPDATE acquisition_outbound_replies SET classification=$2,classified_at=now(),last_error=NULL WHERE id=$1', [row.id, result.classification]);
        await db.query('COMMIT');
        classified++;
      } catch (e) {
        await db.query('ROLLBACK');
        await db.query('UPDATE acquisition_outbound_replies SET last_error=$2 WHERE id=$1', [row.id, e.code || 'classification_failed']);
        if (row.attempts >= 2) {
          const event = await db.query(`INSERT INTO acquisition_outbound_events(id,tenant_id,event_type,payload)
            VALUES($1,'10','reply_classification_failed',$2) ON CONFLICT DO NOTHING RETURNING id`,
          [`reply-failed:${row.id}`, { replyId: row.id, prospectId: row.prospect_id, reason: e.code || 'classification_failed' }]);
          if (event.rows.length) await db.query(`INSERT INTO agent_actions(created_by,action_type,title,description,payload,status,client_id)
            VALUES('max','governed_outbound_attention','Anchor reply needs manual review',
            'Riley classification failed three times. Contact remains suppressed.',$1,'pending',10)`, [{ replyId: row.id, prospectId: row.prospect_id }]);
        }
        // The reply remains suppressed even when Riley or AO projection fails.
      }
    }
    return { classified };
  } finally {
    if (locked) await db.query('SELECT pg_advisory_unlock(261019,10)');
    db.release();
  }
}
module.exports = { installed, address, captureRaw, markHealthy, classifyPending };
