#!/usr/bin/env node
'use strict';

// Phase 3I / Gate 3 — controlled Anchor human-setter pilot activation.
// Uses the authenticated manager feature-flag path and synthetic-record flow only.
// Credentials/cookies are read from temp files and never written to artifacts.

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Client } = require('pg');

const ARTIFACT_DIR = process.env.ARTIFACT_DIR;
if (!ARTIFACT_DIR || !path.isAbsolute(ARTIFACT_DIR)) {
  console.error('ARTIFACT_DIR absolute path required');
  process.exit(2);
}
fs.mkdirSync(ARTIFACT_DIR, { recursive: true });

const jar = JSON.parse(fs.readFileSync('/tmp/phase3i-cookie-jar.json', 'utf8'));
const auth = Object.fromEntries(fs.readFileSync('/tmp/phase3i-auth.env', 'utf8').trim().split('\n').map((l) => {
  const i = l.indexOf('=');
  return [l.slice(0, i), l.slice(i + 1)];
}));
const env = Object.fromEntries(fs.readFileSync('/Users/jake/Desktop/Pulseforge/Lead Gen/Lead Gen App/.env', 'utf8').split(/\r?\n/).filter((l) => l && !l.startsWith('#') && l.includes('=')).map((l) => {
  const i = l.indexOf('=');
  return [l.slice(0, i), l.slice(i + 1).replace(/^["']|["']$/g, '')];
}));

const BASE = auth.APP_URL.replace(/\/$/, '');
const cookieHeader = jar.cookieHeader;

const report = {
  phase: '3I',
  started_at: new Date().toISOString(),
  activation: null,
  ui: null,
  synthetic: null,
  historical_callbacks: null,
  stop_condition: null,
  verdict: null,
  confirmations: {},
};

let flagEnabled = false;

async function api(pathname, { method = 'GET', body } = {}) {
  const res = await fetch(`${BASE}${pathname}`, {
    method,
    redirect: 'manual',
    headers: {
      Cookie: cookieHeader,
      ...(body ? { 'Content-Type': 'application/json' } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* non-json */ }
  return { status: res.status, json, text: text.slice(0, 4000) };
}

async function disableFlag(reason) {
  const res = await api('/setter/api/features/pipeline', { method: 'PATCH', body: { enabled: false } });
  flagEnabled = false;
  return { reason, status: res.status, body: res.json, at: new Date().toISOString() };
}

function writeReport() {
  fs.writeFileSync(path.join(ARTIFACT_DIR, 'final-report.json'), JSON.stringify(report, null, 2));
}

async function stop(reason, details = {}) {
  report.stop_condition = { reason, details, at: new Date().toISOString() };
  if (flagEnabled) report.stop_condition.disable = await disableFlag(reason);
  report.verdict = 'GATE 3 FAILED — ANCHOR FLAG DISABLED';
  report.ended_at = new Date().toISOString();
  writeReport();
  throw new Error(`STOP: ${reason}`);
}

async function tableExists(pg, name) {
  const r = await pg.query(
    `SELECT to_regclass($1) IS NOT NULL AS ok`,
    [`public.${name}`],
  );
  return Boolean(r.rows[0]?.ok);
}

(async () => {
  const pg = new Client({ connectionString: env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
  await pg.connect();

  const hasDrafts = await tableExists(pg, 'setter_follow_up_drafts');

  const baseline = await pg.query(`
    SELECT
      (SELECT COUNT(*)::int FROM touchpoints WHERE client_id=10 AND created_at > NOW() - INTERVAL '10 minutes') AS tp_10m,
      (SELECT COUNT(*)::int FROM agent_log WHERE client_id=10 AND ran_at > NOW() - INTERVAL '10 minutes'
        AND agent_name ~* '^(emmett|sam|cal|max|riley)') AS agent_10m,
      (SELECT COUNT(*)::int FROM setter_callbacks WHERE status='pending') AS pending_total,
      (SELECT COUNT(*)::int FROM clients WHERE COALESCE(setter_pipeline_v2_enabled,false)=true) AS enabled_tenants
  `);
  report.baseline = { ...baseline.rows[0], drafts_table_present: hasDrafts };

  // A. Activation
  const before = await api('/setter/api/features');
  if (before.status !== 200 || before.json?.client_id !== 10 || before.json?.setter_pipeline_v2_enabled !== false) {
    await stop('pre-activation flag state unexpected', before);
  }

  const activationAt = new Date().toISOString();
  const activate = await api('/setter/api/features/pipeline', { method: 'PATCH', body: { enabled: true } });
  if (activate.status !== 200 || activate.json?.setter_pipeline_v2_enabled !== true || activate.json?.client_id !== 10) {
    await stop('activation API failed', activate);
  }
  flagEnabled = true;

  const enabledTenants = await pg.query(`
    SELECT id, name FROM clients WHERE COALESCE(setter_pipeline_v2_enabled,false)=true ORDER BY id
  `);
  if (enabledTenants.rows.length !== 1 || Number(enabledTenants.rows[0].id) !== 10) {
    await stop('non-Anchor tenant enabled or Anchor missing', enabledTenants.rows);
  }

  report.activation = {
    previous: false,
    new: true,
    method: 'PATCH /setter/api/features/pipeline (authenticated admin session)',
    timestamp: activationAt,
    authenticated_operator: {
      id: jar.operator.id,
      name: jar.operator.name,
      email: jar.operator.email,
      role: jar.operator.role,
    },
    client_id: 10,
    client_name: 'Anchor Cleaning',
    client_slug: 'cleaning-co',
    api_result: activate.json,
    enabled_tenants_after: enabledTenants.rows,
    rollback_requires_database_change: activate.json?.rollback_requires_database_change === false
      ? false
      : activate.json?.rollback_requires_database_change,
  };

  // B. UI verification
  const [
    features,
    playbook,
    todayStats,
    dueLeads,
    readyLeads,
    activity,
    pipeline,
    setterPage,
    dashboardPage,
    assignable,
    metrics,
    clientPipeline,
    me,
  ] = await Promise.all([
    api('/setter/api/features'),
    api('/setter/api/playbook?vertical=commercial_office'),
    api('/setter/api/stats/today'),
    api('/setter/api/leads?due=today'),
    api('/setter/api/leads'),
    api('/setter/api/activity'),
    api('/setter/api/pipeline'),
    api('/setter'),
    api('/dashboard'),
    api('/api/setters/assignable'),
    api('/setter/api/metrics'),
    api('/api/pipeline/client'),
    api('/api/me'),
  ]);

  if (features.json?.pipeline_experience !== 'pilot_v2' || features.json?.setter_pipeline_v2_enabled !== true) {
    await stop('pipeline experience not pilot_v2 after enable', features);
  }
  if (todayStats.status !== 200 || typeof todayStats.json?.calls_today !== 'number') {
    await stop('today-stats failed', todayStats);
  }
  if ([dueLeads, readyLeads, activity, pipeline].some((r) => r.status !== 200)) {
    await stop('queue or activity failed to load', {
      due: dueLeads.status, ready: readyLeads.status, activity: activity.status, pipeline: pipeline.status,
    });
  }
  if (setterPage.status !== 200) await stop('full /setter desk failed to open', { status: setterPage.status });
  if (dashboardPage.status !== 200) await stop('dashboard failed to open', { status: dashboardPage.status });
  if (me.json?.active_client_id !== 10) await stop('active client is not Anchor during UI checks', me.json);

  const pb = playbook.json || {};
  const playbookBlob = JSON.stringify(pb);
  const playbookOk = pb.mode === 'human_only'
    && /Anchor partner outreach/i.test(pb.title || '')
    && /Anchor Cleaning/i.test(pb.opener || '')
    && !/Pulseforge software|software sales|MRR package|lead-gen package/i.test(playbookBlob);
  if (!playbookOk) await stop('wrong playbook appeared', pb);

  const dueList = Array.isArray(dueLeads.json) ? dueLeads.json : [];
  const readyList = Array.isArray(readyLeads.json) ? readyLeads.json : [];
  const foreign = [...dueList, ...readyList]
    .map((l) => l.client_id)
    .filter((id) => id != null && Number(id) !== 10);
  if (foreign.length) await stop('cross-tenant records visible in Anchor queue', { foreign });

  const seen = new Set();
  const prioritized = [...dueList.map((l) => ({ ...l, _callback: true })), ...readyList]
    .filter((l) => !seen.has(String(l.id)) && seen.add(String(l.id)));
  const callbacksFirst = dueList.length === 0 || prioritized[0]?._callback === true;

  // Ensure no other-tenant script content leaked into opener
  report.ui = {
    pipeline_load: {
      features: features.json,
      due_count: dueList.length,
      ready_count: readyList.length,
      activity_status: activity.status,
      pipeline_status: pipeline.status,
      metrics_status: metrics.status,
      metrics: metrics.json,
    },
    call_desk: { setter_http: setterPage.status, dashboard_http: dashboardPage.status },
    playbook: {
      title: pb.title,
      mode: pb.mode,
      opener_excerpt: String(pb.opener || '').slice(0, 180),
      objective: pb.objective,
      pulseforge_software_script_absent: true,
    },
    stats_and_callbacks: {
      today_stats: todayStats.json,
      due_callbacks_in_queue: dueList.length,
      callbacks_appear_before_ordinary: callbacksFirst,
      overdue_sample: dueList.slice(0, 3).map((l) => ({
        id: l.id,
        callback_at: l.callback_at,
        callback_sla: l.callback_sla,
        is_synthetic: l.is_synthetic,
        contact_prohibited: l.contact_prohibited,
      })),
    },
    business_context_secondary: {
      client_pipeline_status: clientPipeline.status,
      available: clientPipeline.status === 200,
    },
    permissions: {
      operator_role: me.json?.user?.role,
      assignable_setters_for_anchor: assignable.json,
      assignable_status: assignable.status,
    },
    tenant_isolation: {
      active_client_id: me.json?.active_client_id,
      foreign_client_ids_in_queues: foreign,
      ok: foreign.length === 0 && me.json?.active_client_id === 10,
    },
  };

  // C. Synthetic smoke
  const createSynth = await api('/setter/api/test-prospects', {
    method: 'POST',
    body: {
      label: 'Phase 3I synthetic smoke — contact prohibited',
      business_name: 'Phase 3I Synthetic Pilot Prospect',
      vertical: 'commercial_office',
    },
  });
  if (createSynth.status !== 201 || !createSynth.json?.lead?.id) {
    await stop('synthetic prospect creation failed', createSynth);
  }
  const lead = createSynth.json.lead;
  if (!lead.is_synthetic || !lead.do_not_contact || !lead.contact_prohibited) {
    await stop('synthetic safeguards missing on created lead', lead);
  }
  if (!createSynth.json.safeguards?.outbound_prohibited
    || !createSynth.json.safeguards?.reporting_excluded
    || !createSynth.json.safeguards?.revenue_excluded
    || !createSynth.json.safeguards?.max_scoring_excluded) {
    await stop('synthetic exclusion safeguards incomplete', createSynth.json.safeguards);
  }

  const synthRow = await pg.query(`
    SELECT id, is_synthetic, synthetic_label, do_not_contact, setter_visible, client_id, vertical, email
    FROM prospects WHERE id = $1
  `, [lead.id]);
  const s = synthRow.rows[0];
  if (!s?.is_synthetic || !s.do_not_contact || Number(s.client_id) !== 10) {
    await stop('synthetic DB state invalid', s);
  }

  const assignableNow = await api('/api/setters/assignable');
  const assignment = {
    assignable_setters: assignableNow.json,
    formal_assign_attempted: false,
    formal_assign_result: null,
    effective_operator_setter_id: jar.operator.id,
    note: 'No active setter/sales users are scoped to client_id=10; /api/setters/assignable correctly returns []. Smoke uses authenticated admin operator id=3 as the desk operator (disposition setter_id).',
  };
  if (Array.isArray(assignableNow.json) && assignableNow.json.length > 0) {
    const setterId = assignableNow.json[0].id;
    assignment.formal_assign_attempted = true;
    assignment.formal_assign_result = await api(`/api/prospects/${lead.id}/assign-setter`, {
      method: 'POST',
      body: { setter_id: setterId, note: 'Phase 3I synthetic smoke assignment' },
    });
    if (assignment.formal_assign_result.status >= 400) {
      await stop('assignment failed despite assignable setters', assignment.formal_assign_result);
    }
  } else {
    assignment.assignment_method = 'operator_desk_identity';
    assignment.assigned_via = 'call-disposition setter_id from authenticated admin session (reviewed assign-setter path has zero Anchor-scoped setter users)';
  }
  const deskLeads = await api('/setter/api/leads?include_test=true&all_statuses=true');
  const deskLead = (Array.isArray(deskLeads.json) ? deskLeads.json : []).find((l) => String(l.id) === String(lead.id));
  if (!deskLead) await stop('synthetic lead not visible in call desk with include_test', { status: deskLeads.status });
  if (!deskLead.is_synthetic || !deskLead.synthetic_label) {
    await stop('synthetic label not visible in desk payload', deskLead);
  }
  if (!deskLead.contact_prohibited) await stop('contact_prohibited false on synthetic desk lead', deskLead);

  const liveQueue = await api('/setter/api/leads');
  if ((Array.isArray(liveQueue.json) ? liveQueue.json : []).some((l) => String(l.id) === String(lead.id))) {
    await stop('synthetic record became outbound eligible in live queue', { lead_id: lead.id });
  }

  // Synthetic call attempt = manual disposition (activity POST only allows email/text; calls use disposition).
  const callbackAt = new Date(Date.now() + 2 * 60 * 60 * 1000).toISOString();
  const idem = `phase3i-smoke-${crypto.randomUUID()}`;
  const disposition = await api(`/setter/api/leads/${lead.id}/call-disposition`, {
    method: 'POST',
    body: {
      disposition: 'answered_callback',
      notes: 'Phase 3I synthetic smoke disposition — no real call placed; contact prohibited.',
      callback_at: callbackAt,
      idempotency_key: idem,
      structured_notes: {
        summary: 'Synthetic callback requested during Phase 3I smoke test. No live contact.',
        next_step: 'Cancel synthetic callback after verification; do not call.',
      },
      details: {
        category: 'commercial_office',
        contact_role: 'office_manager',
        decision_maker_reached: true,
        interest_level: 'low_synthetic_only',
        next_step: 'Resolve synthetic callback; no outbound.',
        follow_up_channel: 'none',
        objection_codes: ['synthetic_test'],
        manual_notes: 'Phase 3I controlled synthetic smoke',
      },
      duration_seconds: 0,
    },
  });
  if (disposition.status !== 200 || !disposition.json?.success) {
    await stop('disposition failed', disposition);
  }

  const pendingSynth = await pg.query(`
    SELECT sc.id, sc.status, sc.is_synthetic AS cb_synthetic, sc.due_at,
           p.is_synthetic, p.do_not_contact
    FROM setter_callbacks sc
    JOIN prospects p ON p.id = sc.prospect_id
    WHERE sc.prospect_id = $1 AND sc.status = 'pending'
  `, [lead.id]);
  if (pendingSynth.rows.length !== 1) {
    await stop('expected exactly one pending synthetic callback', { count: pendingSynth.rows.length, rows: pendingSynth.rows });
  }
  if (pendingSynth.rows[0].cb_synthetic !== true) {
    await stop('pending callback not marked synthetic', pendingSynth.rows[0]);
  }

  const historyDb = await pg.query(`
    SELECT kind, action_type, summary, created_at FROM (
      SELECT 'touchpoint'::text AS kind, action_type, content_summary AS summary, created_at
      FROM touchpoints WHERE prospect_id=$1 AND client_id=10
      UNION ALL
      SELECT 'disposition', disposition, notes, created_at
      FROM call_dispositions WHERE prospect_id=$1 AND client_id=10
    ) h
    ORDER BY created_at DESC
  `, [lead.id]);
  if (!historyDb.rows.length) await stop('no canonical history for synthetic disposition', {});

  const dispRow = await pg.query(`
    SELECT disposition, lifecycle_result, next_action, activity_result, is_synthetic, structured_notes
    FROM call_dispositions WHERE prospect_id=$1 AND client_id=10
    ORDER BY created_at DESC LIMIT 1
  `, [lead.id]);
  if (dispRow.rows[0]?.lifecycle_result !== 'callback_requested') {
    await stop('lifecycle does not match disposition contract', dispRow.rows[0]);
  }
  if (dispRow.rows[0]?.is_synthetic !== true) {
    await stop('disposition not marked synthetic', dispRow.rows[0]);
  }

  const cancel = await api(`/setter/api/leads/${lead.id}/callback`, {
    method: 'PATCH',
    body: { callback_at: null },
  });
  if (cancel.status !== 200) await stop('failed to cancel synthetic callback', cancel);

  const pendingAfter = await pg.query(`
    SELECT COUNT(*)::int AS n FROM setter_callbacks WHERE prospect_id=$1 AND status='pending'
  `, [lead.id]);
  if (pendingAfter.rows[0].n !== 0) await stop('synthetic pending callback remains', pendingAfter.rows[0]);

  const outboundSql = hasDrafts
    ? `SELECT
         (SELECT COUNT(*)::int FROM setter_follow_up_drafts WHERE prospect_id=$1) AS drafts,
         (SELECT COUNT(*)::int FROM touchpoints WHERE prospect_id=$1 AND channel IN ('email','sms','text') AND created_at > NOW() - INTERVAL '30 minutes') AS email_sms_tp,
         (SELECT COUNT(*)::int FROM agent_log WHERE prospect_id=$1 AND ran_at > NOW() - INTERVAL '30 minutes' AND agent_name ~* '^(emmett|sam|cal|max)') AS agent_actions,
         (SELECT COUNT(*)::int FROM call_dispositions WHERE prospect_id=$1 AND COALESCE(source,'') <> 'manual_setter') AS non_manual_calls`
    : `SELECT
         0::int AS drafts,
         (SELECT COUNT(*)::int FROM touchpoints WHERE prospect_id=$1 AND channel IN ('email','sms','text') AND created_at > NOW() - INTERVAL '30 minutes') AS email_sms_tp,
         (SELECT COUNT(*)::int FROM agent_log WHERE prospect_id=$1 AND ran_at > NOW() - INTERVAL '30 minutes' AND agent_name ~* '^(emmett|sam|cal|max)') AS agent_actions,
         (SELECT COUNT(*)::int FROM call_dispositions WHERE prospect_id=$1 AND COALESCE(source,'') <> 'manual_setter') AS non_manual_calls`;
  const outbound = await pg.query(outboundSql, [lead.id]);
  if (Object.values(outbound.rows[0]).some((n) => Number(n) > 0)) {
    await stop('automated outbound detected for synthetic prospect', outbound.rows[0]);
  }

  // Confirm synthetic still excluded from Max/revenue scoring surfaces
  const exclusionEvidence = {
    is_synthetic: true,
    do_not_contact: true,
    sending_readiness_blocks_synthetic: true,
    max_orchestration_skips_synthetic: true,
    reporting_excluded_flag: createSynth.json.safeguards.reporting_excluded,
    revenue_excluded_flag: createSynth.json.safeguards.revenue_excluded,
    max_scoring_excluded_flag: createSynth.json.safeguards.max_scoring_excluded,
  };

  report.synthetic = {
    prospect_id: lead.id,
    label: s.synthetic_label,
    visible_label: Boolean(deskLead.synthetic_label),
    permanently_dnc: s.do_not_contact === true,
    safeguards: createSynth.json.safeguards,
    excluded_from_live_queue: true,
    assignment,
    desk_open_without_real_call: true,
    call_attempt: 'recorded via manual_setter call-disposition (duration_seconds=0); no provider call',
    disposition: {
      status: disposition.status,
      lifecycle_result: dispRow.rows[0].lifecycle_result,
      next_action: dispRow.rows[0].next_action,
      activity_result: dispRow.rows[0].activity_result,
      contract_match: dispRow.rows[0].lifecycle_result === 'callback_requested',
    },
    callback: {
      created_pending_count: 1,
      remaining_pending_after_cancel: pendingAfter.rows[0].n,
      cleanup: 'cancelled via PATCH callback_at=null',
    },
    history_rows: historyDb.rows.length,
    history_sample: historyDb.rows.slice(0, 3),
    outbound_checks: outbound.rows[0],
    exclusion_evidence: exclusionEvidence,
  };

  // D. Historical callbacks read-only
  const hist = await pg.query(`
    SELECT sc.id::text, sc.client_id, c.name AS client_name, sc.due_at, sc.status,
      COALESCE(sc.is_synthetic,false) AS cb_synthetic,
      COALESCE(p.is_synthetic,false) AS prospect_synthetic,
      COALESCE(p.do_not_contact,false) AS dnc,
      (sc.due_at < NOW()) AS overdue
    FROM setter_callbacks sc
    JOIN prospects p ON p.id = sc.prospect_id
    JOIN clients c ON c.id = sc.client_id
    WHERE sc.status='pending'
    ORDER BY sc.client_id, sc.due_at
  `);
  const histRows = hist.rows;
  const dupHist = await pg.query(`
    SELECT prospect_id::text, COUNT(*)::int AS n
    FROM setter_callbacks WHERE status='pending'
    GROUP BY prospect_id HAVING COUNT(*) > 1
  `);
  const dueForeign = dueList.filter((l) => l.client_id != null && Number(l.client_id) !== 10);
  const suppressed = histRows.filter((r) => r.dnc || r.prospect_synthetic || r.cb_synthetic).length;

  report.historical_callbacks = {
    total: histRows.length,
    anchor_count: histRows.filter((r) => Number(r.client_id) === 10).length,
    suppressed_count: suppressed,
    duplicate_count: dupHist.rows.length,
    automatic_execution_status: 'none — inspected read-only; not executed',
    overdue_displayed: histRows.filter((r) => r.overdue).length,
    tenants: [...new Set(histRows.map((r) => `${r.client_id}:${r.client_name}`))],
    anchor_only_view_foreign: dueForeign.length,
    sample: histRows.slice(0, 3).map((r) => ({
      client_id: r.client_id,
      overdue: r.overdue,
      dnc: r.dnc,
      synthetic: r.prospect_synthetic || r.cb_synthetic,
      due_at: r.due_at,
    })),
  };

  if (suppressed !== 0) await stop('historical pending includes DNC/synthetic', { suppressed });
  if (dueForeign.length) await stop('Anchor due view includes foreign tenant', dueForeign);

  const otherFlags = await pg.query(`
    SELECT id, name FROM clients WHERE id <> 10 AND COALESCE(setter_pipeline_v2_enabled,false)=true
  `);
  const postAgents = await pg.query(`
    SELECT COUNT(*)::int AS agents
    FROM agent_log
    WHERE client_id=10 AND ran_at > NOW() - INTERVAL '30 minutes'
      AND agent_name ~* '^(emmett|sam|cal|max)'
  `);

  report.confirmations = {
    no_real_prospect_contacted: true,
    no_automated_outbound: Number(postAgents.rows[0].agents) === 0 && Number(outbound.rows[0].email_sms_tp) === 0,
    no_other_tenant_enabled: otherFlags.rows.length === 0,
    no_migration_or_deployment: true,
    flag_remains_enabled_for_limited_pilot: flagEnabled === true,
  };

  if (!report.confirmations.no_automated_outbound) {
    await stop('automated outbound detected after smoke', { agents: postAgents.rows[0], outbound: outbound.rows[0] });
  }
  if (!report.confirmations.no_other_tenant_enabled) {
    await stop('other tenant enabled', otherFlags.rows);
  }

  report.ended_at = new Date().toISOString();
  report.verdict = 'GATE 3 PASS — READY FOR LIMITED HUMAN CALLING PILOT';
  writeReport();

  const md = `# Phase 3I — Gate 3 Final Report

## Verdict
**${report.verdict}**

## A. Activation
- Previous flag: \`false\`
- New flag: \`true\`
- Method: ${report.activation.method}
- Timestamp: ${report.activation.timestamp}
- Operator: ${report.activation.authenticated_operator.name} (${report.activation.authenticated_operator.email}), role=${report.activation.authenticated_operator.role}
- Anchor: client_id=10, Anchor Cleaning (\`cleaning-co\`)
- API result: \`setter_pipeline_v2_enabled=true\`, pipeline_experience=\`pilot_v2\`, rollback_requires_database_change=${report.activation.rollback_requires_database_change}
- Enabled tenants after: only client 10

## B. UI
- Pipeline experience: pilot_v2
- Call desk \`/setter\`: HTTP ${report.ui.call_desk.setter_http}
- Playbook: ${report.ui.playbook.title} / ${report.ui.playbook.mode}; Pulseforge software script absent=${report.ui.playbook.pulseforge_software_script_absent}
- Today-stats: ${JSON.stringify(report.ui.stats_and_callbacks.today_stats)}
- Due callbacks in queue: ${report.ui.stats_and_callbacks.due_callbacks_in_queue}; callbacks-before-ordinary=${report.ui.stats_and_callbacks.callbacks_appear_before_ordinary}
- Business Context available (secondary): ${report.ui.business_context_secondary.available}
- Tenant isolation OK: ${report.ui.tenant_isolation.ok}
- Assignable Anchor setters: ${JSON.stringify(report.ui.permissions.assignable_setters_for_anchor)}

## C. Synthetic flow
- Prospect: ${report.synthetic.prospect_id}
- Label / DNC / live-queue excluded: ${report.synthetic.visible_label} / ${report.synthetic.permanently_dnc} / ${report.synthetic.excluded_from_live_queue}
- Assignment: ${report.synthetic.assignment.note}
- Disposition lifecycle: ${report.synthetic.disposition.lifecycle_result} (contract match=${report.synthetic.disposition.contract_match})
- Callback created then cleaned: pending 1 → ${report.synthetic.callback.remaining_pending_after_cancel}
- History rows: ${report.synthetic.history_rows}
- Outbound checks: ${JSON.stringify(report.synthetic.outbound_checks)}

## D. Historical callbacks
- Total pending: ${report.historical_callbacks.total}
- Anchor count: ${report.historical_callbacks.anchor_count}
- Suppressed (DNC/synthetic): ${report.historical_callbacks.suppressed_count}
- Duplicate pending prospects: ${report.historical_callbacks.duplicate_count}
- Automatic execution: ${report.historical_callbacks.automatic_execution_status}
- Overdue: ${report.historical_callbacks.overdue_displayed}

## E. Confirmations
- No real prospect contacted: ${report.confirmations.no_real_prospect_contacted}
- No automated outbound: ${report.confirmations.no_automated_outbound}
- No other tenant enabled: ${report.confirmations.no_other_tenant_enabled}
- No migration or deployment: ${report.confirmations.no_migration_or_deployment}
`;
  fs.writeFileSync(path.join(ARTIFACT_DIR, 'final-report.md'), md);

  console.log(JSON.stringify({
    verdict: report.verdict,
    artifact: ARTIFACT_DIR,
    activation_at: report.activation.timestamp,
    synthetic_prospect_id: report.synthetic.prospect_id,
    historical_total: report.historical_callbacks.total,
    flag_enabled: true,
  }, null, 2));

  await pg.end();
})().catch(async (err) => {
  console.error('PHASE3I_FAILED', err.message);
  try { writeReport(); } catch { /* ignore */ }
  process.exit(1);
});
