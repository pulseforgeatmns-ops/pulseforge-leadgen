#!/usr/bin/env node
'use strict';

// MAX-P0-002 — READ-ONLY provider-side evidence for unresolved AMO Brevo attempts.
// DB: one READ ONLY transaction (plus PGOPTIONS default_transaction_read_only=on).
// Brevo: GET /v3/smtp/statistics/events only (event log lookup; never sends).
// Output: record id, event types, whether an event carries this record's
// mission AND prospect tags, and an outcome. No e-mail address, subject or
// message id is printed (message ids are md5-prefixed).
//
// A record becomes SENT_CONFIRMED only when Brevo returns an event whose tag
// carries mission:<id> AND prospect:<id> (and revision:<rev> when Brevo shows a
// revision tag). Absence of events is NOT proof of non-delivery (retention,
// filtering): such records stay UNKNOWN.
//
// Render Shell (cwd /app):
//   PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=30s' \
//     NODE_PATH=/app/node_modules node /tmp/p0002-brevo-evidence.cjs

const crypto = require('node:crypto');
const { Client } = require('pg');

const EXPECTED_ATTEMPTS = 20;
const EXPECTED_FAILED_MAILBOX = 1;
const h = (v) => (v ? crypto.createHash('md5').update(String(v)).digest('hex').slice(0, 10) : null);
const day = (d) => new Date(d).toISOString().slice(0, 10);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ACCEPTED = new Set(['requests', 'delivered', 'opened', 'uniqueOpened', 'clicks', 'softBounces', 'hardBounces',
  'deferred', 'blocked', 'spam', 'unsubscribed', 'loadedByProxy']);

// Exact tag tokens (Brevo returns a string, a JSON-array string or an array).
function tagSet(ev) {
  const t = ev.tag ?? ev.tags ?? '';
  const list = Array.isArray(t) ? t : String(t).split(/[\s,\[\]"']+/);
  return new Set(list.map((x) => String(x).trim()).filter(Boolean));
}

async function main() {
  const db = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: String(process.env.DATABASE_SSL || '').toLowerCase() === 'false' ? false : { rejectUnauthorized: false },
  });
  await db.connect();
  let rows;
  try {
    await db.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const ro = (await db.query("SELECT current_setting('transaction_read_only') AS ro")).rows[0].ro;
    if (ro !== 'on') throw new Error('session_not_read_only');
    rows = (await db.query(`
      SELECT e.id, e.tenant_id, e.mission_id, e.prospect_id, e.prepared_artifact_revision AS revision,
             e.provider, e.attempted_at, lower(coalesce(e.payload->>'email','')) AS email
        FROM acquisition_mission_outbound_executions e
       WHERE e.status = 'attempted'
         AND NOT EXISTS (SELECT 1 FROM acquisition_mission_outbound_executions f
                          WHERE f.execution_identity = e.execution_identity AND f.status IN ('sent','failed'))
       ORDER BY e.tenant_id, e.attempted_at`)).rows;
    // Fail-closed precondition: no provider lookup unless the population is exactly the reviewed one.
    const failed = Number((await db.query(
      "SELECT count(*) AS n FROM tenant_outreach_messages WHERE direction = 'OUTBOUND' AND status = 'failed'")).rows[0].n);
    if (rows.length !== EXPECTED_ATTEMPTS || failed !== EXPECTED_FAILED_MAILBOX) {
      throw Object.assign(new Error(`P0002_PRECONDITION_FAILED: unresolved_attempts=${rows.length} (expected ${EXPECTED_ATTEMPTS}), failed_mailbox=${failed} (expected ${EXPECTED_FAILED_MAILBOX}); no provider lookup made`), { code: 'P0002_PRECONDITION_FAILED' });
    }
  } finally {
    await db.query('ROLLBACK').catch(() => {});
    await db.end();
  }

  const key = process.env.BREVO_API_KEY;
  const out = [];
  for (const r of rows) {
    const base = { record_id: r.id, tenant_id: r.tenant_id, provider: r.provider, attempted_day: day(r.attempted_at) };
    if (r.provider !== 'brevo') { out.push({ ...base, outcome: 'UNKNOWN', reason: 'not_a_brevo_record' }); continue; }
    if (!key) { out.push({ ...base, outcome: 'UNKNOWN', reason: 'brevo_key_absent_lookup_not_run' }); continue; }
    if (!r.email) { out.push({ ...base, outcome: 'UNKNOWN', reason: 'record_has_no_recipient' }); continue; }
    const start = new Date(r.attempted_at);
    const end = new Date(Math.min(Date.now(), start.getTime() + 7 * 86400000));
    const qs = new URLSearchParams({ email: r.email, startDate: day(start), endDate: day(end), limit: '500', sort: 'asc' });
    let res;
    try {
      res = await fetch(`https://api.brevo.com/v3/smtp/statistics/events?${qs}`, {
        method: 'GET', headers: { 'api-key': key, accept: 'application/json' }, signal: AbortSignal.timeout(15000),
      });
    } catch (err) {
      out.push({ ...base, outcome: 'UNKNOWN', reason: `brevo_lookup_failed:${err.name}` }); continue;
    }
    if (!res.ok) { out.push({ ...base, outcome: 'UNKNOWN', reason: `brevo_http_${res.status}` }); if (res.status === 401 || res.status === 403) break; continue; }
    const events = ((await res.json().catch(() => ({}))).events) || [];
    const mTag = `mission:${r.mission_id}`, pTag = `prospect:${r.prospect_id}`, rTag = `revision:${r.revision}`;
    const matched = events.filter((ev) => {
      const t = tagSet(ev);
      const hasRevision = [...t].some((x) => x.startsWith('revision:'));
      return t.has(mTag) && t.has(pTag) && (!hasRevision || t.has(rTag));
    });
    const types = (list) => [...new Set(list.map((e) => e.event))].sort();
    const accepted = matched.filter((e) => ACCEPTED.has(e.event));
    out.push({
      ...base,
      events_for_recipient_in_window: events.length,
      event_types_for_recipient: types(events),
      tag_matched_events: matched.length,
      tag_matched_types: types(matched),
      message_id_h: [...new Set(matched.map((e) => h(e.messageId)).filter(Boolean))],
      outcome: accepted.length ? 'SENT_CONFIRMED' : 'UNKNOWN',
      reason: accepted.length ? 'brevo_event_tagged_with_mission_and_prospect'
        : matched.length ? 'brevo_tagged_events_without_acceptance_type'
          : events.length ? 'brevo_events_for_recipient_not_tagged_to_record'
            : 'no_brevo_event_found_in_window_not_proof',
    });
    await sleep(300);
  }
  console.log(JSON.stringify({ status: 'P0002_BREVO_COMPLETE', observed_at: new Date().toISOString(), records: out.length, results: out }, null, 2));
}

main().catch((err) => { console.error(JSON.stringify({ error: err.code || 'error', message: err.message })); process.exitCode = 1; });
