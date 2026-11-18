'use strict';

/**
 * EvidenceRegistry (P0-MAX-REASONING-001 PR 4) — one tenant-scoped, read-only
 * interface over Max's existing evidence loaders. Not wired into routing.
 *
 * Reuse, not duplication:
 *   - OperationalInspection.loadOperationalSnapshot / loadMissionById /
 *     classifyMission / checkNamedClientScope / readOnlyQueryable;
 *   - ProspectRecordIntelligence.loadRecords (bounded keyset read) and
 *     evaluate (missing information, eligibility, recorded engagement tier).
 * Three narrow reads had no existing reader and live here, bounded and behind
 * the same SELECT-only guard: mission event history (LIMIT 50 per mission,
 * at most 10 missions), outbound program authorization (LIMIT 3) and the
 * campaign email_performance aggregate (one row).
 *
 * Every result separates OBSERVED items (with source, tenant, observation
 * and retrieval timestamps, freshness) from CONTRADICTIONS (never reconciled)
 * and UNKNOWNS (missing or unretrievable — never treated as proof that
 * something did not happen). Inferences are produced only by
 * EvidenceDiagnosis and always cite the observed items they rest on.
 *
 * Callers choose domains from TurnFrame.EVIDENCE_DOMAINS and pass validated
 * ids; no caller (and no model) supplies SQL.
 */

const OI = require('../workspace/OperationalInspection');
const PRI = require('../workspace/ProspectRecordIntelligence');
const { EVIDENCE_DOMAINS } = require('../interpretation/TurnFrame');

const DAY_MS = 86400000;
const ID_RE = /^[A-Za-z0-9_.:-]{1,160}$/;
const MAX_REFERENT_IDS = 25;
const MAX_EVENT_MISSIONS = 10;
const EVENT_LIMIT = 50;
const FRESHNESS_DAYS = Object.freeze({
  prospect_record: 30, contact: 60, mission: OI.STALE_AFTER_DAYS, agent_run: 7, outbound_channel: 7,
  outbound_gate: 1, outbound_program: 7, mission_event: OI.STALE_AFTER_DAYS, prospect_aggregate: 14, campaign: 30,
});
const ACTIVE_MISSION_STATUSES = new Set(['active', 'running', 'in_progress', 'executing', 'live']);
const TERMINAL_PIPELINE = new Set(['closed', 'dead', 'disqualified', 'lost', 'won', 'bounced', 'rejected']);
const ACTIVE_SETTER = new Set(['new', 'contacted', 'follow_up', 'booked']);
const EXPECTED_AGENTS = Object.freeze({ scout_execution: ['scout'], outbound_delivery: ['emmett'] });

const iso = (v) => {
  if (v == null || v === '') return null;
  const d = v instanceof Date ? v : new Date(v);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
};

function freshness(observedAt, now, staleAfterDays) {
  const at = iso(observedAt);
  if (!at) return { ageDays: null, stale: null, staleAfterDays, basis: 'no recorded timestamp — freshness unknown' };
  const ageDays = Math.max(0, Math.floor((now.getTime() - Date.parse(at)) / DAY_MS));
  return { ageDays, stale: ageDays > staleAfterDays, staleAfterDays, basis: `last recorded ${ageDays} day(s) ago` };
}

function observed({ domain, tenantId, source, provenance, type, id, facts, observedAt, now, missing = [] }) {
  return {
    ref: `${domain}:${type}:${id}`,
    kind: 'observed',
    domain,
    tenantId,
    source: { name: source, provenance },
    subject: { type, id: String(id) },
    facts,
    observedAt: iso(observedAt),
    retrievedAt: now.toISOString(),
    freshness: freshness(observedAt, now, FRESHNESS_DAYS[type] ?? 30),
    missing,
  };
}

const unknown = (domain, what, reason) => ({ kind: 'unknown', domain, what, reason });

function domainResult(status, source, provenance) {
  return { status, sources: source ? [{ name: source, provenance: provenance || null, status }] : [], items: [] };
}

function relationMissing(err) { return /relation .* does not exist/i.test(String(err && err.message)); }

// ---------------------------------------------------------------------------
// The three narrow reads with no existing reader (bounded, tenant-scoped).
// ---------------------------------------------------------------------------

async function loadMissionEvents({ db, runtime, tenantId, missionId }) {
  if (db) {
    try {
      const { rows } = await db.query(
        `SELECT id, mission_id, kind, specialist, label, at
           FROM acquisition_mission_events
          WHERE tenant_id = $1 AND mission_id = $2
          ORDER BY at DESC
          LIMIT ${EVENT_LIMIT}`,
        [tenantId, missionId]
      );
      return { status: rows.length ? 'found' : 'empty', provenance: 'Postgres acquisition_mission_events (tenant-scoped, newest 50)', rows };
    } catch (err) {
      if (relationMissing(err)) return { status: 'empty', provenance: 'Postgres acquisition_mission_events (table absent)', rows: [] };
      return { status: 'failed', reason: err && err.code === 'OPERATIONAL_INSPECTION_READ_ONLY' ? 'read_only_violation' : 'mission_event_query_failed', rows: [] };
    }
  }
  if (runtime && typeof runtime.engine === 'function') {
    try {
      const engine = runtime.engine();
      const m = engine.list(tenantId).find((x) => String(x.id) === missionId && String(x.tenantId || x.clientId || '') === tenantId);
      if (!m) return { status: 'empty', provenance: 'in-process acquisition mission runtime (non-durable view)', rows: [] };
      const events = (engine.store.listEvents(m.id) || []).slice(-EVENT_LIMIT).reverse()
        .map((e) => ({ id: e.id, mission_id: m.id, kind: e.kind, specialist: e.specialist || null, label: e.label || null, at: e.at }));
      return { status: events.length ? 'found' : 'empty', provenance: 'in-process acquisition mission runtime (non-durable view)', rows: events };
    } catch (_) {
      return { status: 'failed', reason: 'mission_runtime_unavailable', rows: [] };
    }
  }
  return { status: 'unavailable', reason: 'mission_event_store_not_configured', rows: [] };
}

async function loadOutboundPrograms({ db, tenantId }) {
  if (!db) return { status: 'unavailable', reason: 'outbound_program_store_not_configured', rows: [] };
  try {
    const { rows } = await db.query(
      `SELECT id, mode, authorized_at, policy->>'expiresAt' AS expires_at, last_tick_at, last_error IS NOT NULL AS has_error
         FROM acquisition_outbound_programs
        WHERE tenant_id = $1
        ORDER BY authorized_at DESC NULLS LAST
        LIMIT 3`,
      [tenantId]
    );
    return { status: rows.length ? 'found' : 'empty', provenance: 'Postgres acquisition_outbound_programs (tenant-scoped, newest 3)', rows };
  } catch (err) {
    if (relationMissing(err)) return { status: 'empty', provenance: 'Postgres acquisition_outbound_programs (table absent — no program)', rows: [] };
    return { status: 'failed', reason: 'outbound_program_query_failed', rows: [] };
  }
}

async function loadCampaignPerformance({ db, clientId }) {
  if (!db) return { status: 'unavailable', reason: 'campaign_store_not_configured' };
  try {
    const { rows } = await db.query(
      `SELECT COALESCE(SUM(sends), 0)::int AS sends, COALESCE(SUM(opens), 0)::int AS opens,
              COALESCE(SUM(replies), 0)::int AS replies, COALESCE(SUM(bounces), 0)::int AS bounces,
              COUNT(*)::int AS rows, MAX(updated_at) AS last_at
         FROM email_performance
        WHERE client_id = $1`,
      [clientId]
    );
    const r = rows[0] || {};
    return { status: Number(r.rows) ? 'found' : 'empty', provenance: 'Postgres email_performance (aggregate)', row: r };
  } catch (err) {
    if (relationMissing(err)) return { status: 'unavailable', reason: 'email_performance_not_present_in_this_deployment' };
    return { status: 'failed', reason: 'campaign_query_failed' };
  }
}

// ---------------------------------------------------------------------------
// Prospect record normalization (no names, emails, phones or notes leave here)
// ---------------------------------------------------------------------------

function opportunityState(row, now) {
  const md = row.acquisition_metadata || {};
  if (row.closed_at || TERMINAL_PIPELINE.has(String(row.status || '').toLowerCase())) return 'closed';
  const deadline = iso(md.opportunityExpiresAt || md.expiresAt || md.deadline);
  if (deadline && Date.parse(deadline) < now.getTime()) return 'expired';
  if (!row.status && !row.setter_status) return 'unknown';
  return 'open';
}

function prospectItem(row, tenantId, now) {
  const e = PRI.evaluate(row);
  const contactAge = freshness(row.last_contacted_at, now, FRESHNESS_DAYS.contact);
  const missing = [...e.missing];
  if (!row.last_contacted_at) missing.push('last contact date');
  return observed({
    domain: 'crm_pipeline', tenantId, source: 'prospects', provenance: 'Postgres prospects (bounded tenant-scoped read, ProspectRecordIntelligence.loadRecords)',
    type: 'prospect_record', id: row.id, now, missing: [...new Set(missing)],
    observedAt: row.updated_at || row.created_at,
    facts: {
      pipelineStatus: row.status || null, setterStatus: row.setter_status || null, reviewState: e.review || null,
      isHot: row.is_hot === true, qualificationScore: row.icp_score ?? null, serviceAreaMatch: row.service_area_match || null,
      contactChannels: { email: Boolean(row.email), phone: Boolean(row.phone), linkedin: Boolean(row.linkedin_url) },
      doNotContact: row.do_not_contact !== false, lastContactedAt: iso(row.last_contacted_at), contactStale: contactAge.stale,
      closedAt: iso(row.closed_at), bookedAt: iso(row.booked_at), opportunityState: opportunityState(row, now),
      eligible: e.eligible, readyForProposal: e.ready, engagementTier: e.tier, nameRecorded: Boolean(row.first_name || row.last_name),
    },
  });
}

function prospectContradictions(item) {
  const f = item.facts;
  const out = [];
  const pipe = String(f.pipelineStatus || '').toLowerCase();
  const setter = String(f.setterStatus || '').toLowerCase();
  if (TERMINAL_PIPELINE.has(pipe) && ACTIVE_SETTER.has(setter)) {
    out.push({ kind: 'status_conflict', refs: [item.ref], description: `pipeline status "${f.pipelineStatus}" is terminal but setter status "${f.setterStatus}" is active`, resolution: 'unresolved' });
  }
  if (f.closedAt && !TERMINAL_PIPELINE.has(pipe)) {
    out.push({ kind: 'status_conflict', refs: [item.ref], description: `a close date is recorded but pipeline status is "${f.pipelineStatus || 'not recorded'}"`, resolution: 'unresolved' });
  }
  if (f.bookedAt && setter && setter !== 'booked') {
    out.push({ kind: 'status_conflict', refs: [item.ref], description: `a booking date is recorded but setter status is "${f.setterStatus}"`, resolution: 'unresolved' });
  }
  return out;
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

class EvidenceRegistry {
  /**
   * @param {{ pool?: object|null, now?: () => Date, env?: object, acquisitionMissionRuntime?: object|null,
   *   loaders?: { snapshot?: object, loadProspectRecords?: Function, loadMissionEvents?: Function,
   *     loadOutboundPrograms?: Function, loadCampaignPerformance?: Function, loadMissionById?: Function } }} options
   *   pool: a pg-like pool (always wrapped read-only). Omit to use db.js only
   *   when DATABASE_URL is set; null for no database.
   */
  constructor({ pool, now = () => new Date(), env = process.env, acquisitionMissionRuntime = null, loaders = {} } = {}) {
    this.pool = pool;
    this.now = now;
    this.env = env;
    this.runtime = acquisitionMissionRuntime;
    this.loaders = loaders;
  }

  rawPool() {
    if (this.pool === null) return null;
    if (this.pool && typeof this.pool.query === 'function') return this.pool;
    if (!this.env.DATABASE_URL) return null;
    try { return require('../../../db'); } catch (_) { return null; }
  }

  /**
   * @param {{ tenantId: string|number, domains: string[], question?: string|null,
   *   referents?: Array<{ kind: 'prospect_set'|'prospect'|'mission', memberIds?: string[], id?: string, missionId?: string }> }} request
   */
  async retrieve({ tenantId, domains = [], question = null, referents = [] } = {}) {
    const now = this.now();
    const tenant = String(tenantId ?? '');
    const report = {
      kind: 'evidence_report', tenantId: tenant, retrievedAt: now.toISOString(), refused: null,
      domains: {}, contradictions: [], unknowns: [], revalidation: null, rejectedDomains: [],
      readOnly: { reads: 0, writesBlocked: 0 },
      authority: { grantsAuthority: false, executionPermitted: false },
    };
    if (!ID_RE.test(tenant)) { report.refused = 'invalid_tenant'; return report; }
    const requested = [...new Set(domains)];
    report.rejectedDomains = requested.filter((d) => !EVIDENCE_DOMAINS.includes(d));
    const wanted = requested.filter((d) => EVIDENCE_DOMAINS.includes(d));
    const clientId = Number(tenant);
    const clientScoped = Number.isSafeInteger(clientId) && clientId > 0;

    const raw = this.rawPool();
    const boundary = { enforced: true, reads: 0, writesBlocked: 0 };
    const db = raw ? OI.readOnlyQueryable(raw, boundary) : null;
    const merge = (b) => { if (b) { report.readOnly.reads += b.reads || 0; report.readOnly.writesBlocked += b.writesBlocked || 0; } };

    // Authorization first: a different named client is refused before any read.
    if (question) {
      const checked = await OI.checkNamedClientScope({ question, tenantId: tenant, opts: { pool: raw, loaders: this.loaders.snapshot } });
      merge(checked.mutationBoundary);
      if (!checked.scope.ok) {
        report.refused = 'tenant_mismatch';
        report.unknowns.push(unknown(null, 'evidence about another client', 'refused: evidence is only read for the authenticated tenant'));
        return report;
      }
    }

    // CRM records alone need no snapshot; every other domain is built on it.
    const needsSnapshot = wanted.some((d) => d !== 'crm_pipeline');
    const snapshot = needsSnapshot
      ? await OI.loadOperationalSnapshot({ tenantId: tenant, opts: { pool: raw, now, env: this.env, loaders: this.loaders.snapshot }, acquisitionMissionRuntime: this.runtime })
      : null;
    if (snapshot) merge(snapshot.mutationBoundary);
    const sourceState = (name) => (snapshot ? snapshot.sources.find((s) => s.source === name) : null);

    const ids = (referents || []).flatMap((r) => (r && r.kind === 'prospect_set' ? r.memberIds || [] : r && r.kind === 'prospect' ? [r.id] : []))
      .map(String).filter((id) => ID_RE.test(id));
    const prospectIds = [...new Set(ids)].slice(0, MAX_REFERENT_IDS);
    const missionIds = [...new Set((referents || []).filter((r) => r && r.kind === 'mission' && ID_RE.test(String(r.missionId || ''))).map((r) => String(r.missionId)))].slice(0, MAX_REFERENT_IDS);

    let records = null;
    if (wanted.includes('crm_pipeline') || prospectIds.length) {
      if (!clientScoped) records = { status: 'unavailable', rows: [], complete: false, reason: 'tenant_has_no_client_scope' };
      else {
        const load = this.loaders.loadProspectRecords || ((ctx) => PRI.loadRecords(ctx.db, ctx.clientId));
        try { records = await load({ db, clientId }); } catch (_) { records = { status: 'failed', rows: [], complete: false }; }
        if (!records || !['found', 'empty', 'failed', 'unavailable'].includes(records.status)) records = { status: 'failed', rows: [], complete: false };
        // Second boundary: never accept a row from another client.
        records.rows = (records.rows || []).filter((r) => r && r.id && String(r.client_id) === String(clientId));
      }
    }

    for (const d of wanted) report.domains[d] = await this.domain(d, { tenant, clientId, clientScoped, db, now, snapshot, sourceState, records, report });
    if (prospectIds.length || missionIds.length) report.revalidation = await this.revalidate({ tenant, prospectIds, missionIds, records, raw, now, report, merge });

    merge(boundary);
    return report;
  }

  /** Each mission's bounded event history, loaded once per retrieval. */
  async eventsFor(ctx, missionId) {
    ctx.eventCache = ctx.eventCache || new Map();
    if (!ctx.eventCache.has(missionId)) {
      const load = this.loaders.loadMissionEvents || loadMissionEvents;
      ctx.eventCache.set(missionId, await load({ db: ctx.db, runtime: this.runtime, tenantId: ctx.tenant, missionId }));
    }
    return ctx.eventCache.get(missionId);
  }

  async domain(d, ctx) {
    const { tenant, clientScoped, db, now, snapshot, sourceState, records, report } = ctx;
    const unavailableFrom = (name) => {
      const s = sourceState(name);
      return !s || !s.available ? (s ? s.reason : 'source_not_read') : null;
    };
    if (d === 'crm_pipeline') {
      const r = domainResult(records.status, 'prospects', 'Postgres prospects (bounded tenant-scoped read)');
      if (records.status === 'failed' || records.status === 'unavailable') {
        report.unknowns.push(unknown(d, 'prospect records', `${records.status}: ${records.reason || 'query did not complete'} — no records were verified`));
        return r;
      }
      r.items = records.rows.map((row) => prospectItem(row, tenant, now));
      r.complete = records.complete === true;
      if (!r.complete) report.unknowns.push(unknown(d, 'prospect records beyond the bounded read', `read stopped at ${PRI.LIMIT} records; others may exist`));
      for (const item of r.items) report.contradictions.push(...prospectContradictions(item));
      return r;
    }
    if (d === 'mission_lifecycle') {
      const reason = unavailableFrom('acquisition_missions');
      const r = domainResult(reason && !(snapshot.missions || []).length ? 'unavailable' : (snapshot.missions.length ? 'found' : 'empty'), 'acquisition_missions', 'OperationalInspection snapshot (tenant-scoped)');
      if (reason) report.unknowns.push(unknown(d, 'acquisition missions', reason));
      r.items = snapshot.missions.map((m) => observed({
        domain: d, tenantId: tenant, source: m.system === 'mission_engine' ? 'missions' : 'acquisition_missions', provenance: 'OperationalInspection snapshot',
        type: 'mission', id: m.id, now, observedAt: m.classification.lastActivityAt,
        facts: { system: m.system, stage: m.stage, status: m.status, classification: m.classification.category, certainty: m.classification.certainty,
          classificationBasis: m.classification.basis, lastEventKind: m.lastEvent ? m.lastEvent.kind : null, lastEventAt: m.lastEvent ? m.lastEvent.at : null,
          awaitingOperatorDecision: m.blockers.some((b) => b.kind === 'awaiting_operator_decision'),
          pendingDecisionKind: (m.blockers.find((b) => b.kind === 'awaiting_operator_decision')?.label.match(/\(([a-z_]+)\)\s*$/) || [])[1] || null,
          synthetic: m.synthetic, blockerKinds: m.blockers.map((b) => b.kind) },
      }));
      for (const item of r.items) {
        if (ACTIVE_MISSION_STATUSES.has(String(item.facts.status || '').toLowerCase()) && item.facts.classification === OI.CATEGORIES.STALE) {
          report.contradictions.push({ kind: 'status_vs_activity', refs: [item.ref], description: `mission status "${item.facts.status}" but no recorded activity for ${item.freshness.ageDays} days`, resolution: 'unresolved' });
        }
      }
      return r;
    }
    if (d === 'operator_decisions') {
      const r = domainResult('empty', 'acquisition_mission_events', null);
      const missions = (snapshot.missions || []).slice(0, MAX_EVENT_MISSIONS);
      for (const m of missions) {
        if (m.blockers.some((b) => b.kind === 'awaiting_operator_decision')) {
          r.items.push(observed({ domain: d, tenantId: tenant, source: 'acquisition_missions', provenance: 'pending operator decision on the mission record (read only, not resolved)',
            type: 'pending_decision', id: m.id, now, observedAt: m.updatedAt,
            facts: { missionId: m.id, state: 'pending', kind: (m.blockers.find((b) => b.kind === 'awaiting_operator_decision').label.match(/\(([a-z_]+)\)\s*$/) || [])[1] || null } }));
        }
        const ev = await this.eventsFor(ctx, String(m.id));
        if (ev.status === 'failed' || ev.status === 'unavailable') { report.unknowns.push(unknown(d, `event history for mission ${m.id}`, ev.reason || ev.status)); continue; }
        r.sources[0].provenance = ev.provenance || r.sources[0].provenance;
        for (const e of ev.rows) {
          r.items.push(observed({ domain: d, tenantId: tenant, source: 'acquisition_mission_events', provenance: ev.provenance, type: 'mission_event', id: e.id || `${m.id}:${iso(e.at)}`,
            now, observedAt: e.at, facts: { missionId: String(m.id), eventKind: e.kind, specialist: e.specialist || null, label: e.label ? String(e.label).slice(0, 120) : null } }));
        }
      }
      r.status = r.items.length ? 'found' : (unavailableFrom('acquisition_missions') ? 'unavailable' : 'empty');
      return r;
    }
    if (d === 'agent_execution' || d === 'scout_execution' || d === 'outbound_delivery') {
      const reason = unavailableFrom('agent_log');
      const agents = reason ? [] : (snapshot.agentRuns.agents || []);
      const filter = EXPECTED_AGENTS[d];
      const r = domainResult(reason ? 'unavailable' : 'found', 'agent_log', 'OperationalInspection snapshot (last run per agent)');
      if (reason) report.unknowns.push(unknown(d, 'agent execution log', reason));
      const relevant = filter ? agents.filter((a) => filter.some((f) => a.agent.toLowerCase().includes(f))) : agents;
      r.items = relevant.map((a) => observed({ domain: d, tenantId: tenant, source: 'agent_log', provenance: 'Postgres agent_log', type: 'agent_run', id: a.agent, now, observedAt: a.lastRunAt,
        facts: { agent: a.agent, lastRunAt: a.lastRunAt, runs7d: a.runs7d, errors7d: a.errors7d } }));
      if (!reason && filter) {
        for (const f of filter) {
          if (!relevant.some((a) => a.agent.toLowerCase().includes(f))) {
            report.unknowns.push(unknown(d, `${f} execution log`, `no ${f} runs recorded in agent_log — absence of a log does not establish that ${f} never ran`));
          }
        }
      }
      if (d === 'scout_execution') {
        const p = snapshot.prospecting;
        if (p && p.available) {
          r.items.push(observed({ domain: d, tenantId: tenant, source: 'prospects', provenance: p.provenance, type: 'prospect_aggregate', id: 'prospects', now, observedAt: p.lastAddedAt,
            facts: { total: p.counts.total, qualified: p.counts.qualified, setterVisible: p.counts.setterVisible, lastAddedAt: p.lastAddedAt } }));
        } else report.unknowns.push(unknown(d, 'prospect counts', p ? p.reason : 'not read'));
        report.unknowns.push(unknown(d, 'Scout acquisition state', 'not consulted: its store creates its table on read, which the read-only guard refuses'));
      }
      if (d === 'outbound_delivery') {
        const o = snapshot.outbound;
        if (o && o.available) {
          for (const c of o.channels) r.items.push(observed({ domain: d, tenantId: tenant, source: 'touchpoints', provenance: o.provenance, type: 'outbound_channel', id: c.channel, now, observedAt: c.lastAt, facts: { ...c } }));
          if (!o.channels.length) report.unknowns.push(unknown(d, 'outbound touchpoints', 'no touchpoints recorded for this tenant'));
        } else report.unknowns.push(unknown(d, 'outbound touchpoints', o ? o.reason : 'not read'));
      }
      if (d === 'agent_execution' && !reason && !agents.length) report.unknowns.push(unknown(d, 'agent execution log', 'no agent runs recorded for this tenant'));
      r.status = r.items.length ? 'found' : (reason ? 'unavailable' : 'empty');
      // A mission records a specialist event in the last 7 days (any event in
      // its bounded history, not only the latest), but agent_log has no run of
      // that agent within 7 days of it.
      if (d === 'agent_execution' && !reason) {
        const seen = new Set();
        for (const m of (snapshot.missions || []).slice(0, MAX_EVENT_MISSIONS)) {
          const ev = await this.eventsFor(ctx, String(m.id));
          if (ev.status === 'failed' || ev.status === 'unavailable') { report.unknowns.push(unknown(d, `event history for mission ${m.id}`, ev.reason || ev.status)); continue; }
          for (const e of ev.rows || []) {
            const at = iso(e.at);
            const text = `${e.specialist || ''} ${e.kind || ''}`.toLowerCase();
            const agent = ['scout', 'emmett', 'paige'].find((a) => text.includes(a));
            if (!agent || !at || (now.getTime() - Date.parse(at)) / DAY_MS > 7 || seen.has(`${m.id}:${agent}`)) continue;
            const run = agents.find((a) => a.agent.toLowerCase().includes(agent));
            if (!run || !run.lastRunAt || Math.abs(Date.parse(run.lastRunAt) - Date.parse(at)) > 7 * DAY_MS) {
              seen.add(`${m.id}:${agent}`);
              report.contradictions.push({ kind: 'mission_vs_agent_log', refs: [`mission_lifecycle:mission:${m.id}`, `agent_execution:agent_run:${run ? run.agent : agent}`],
                description: `mission records a ${agent} event at ${at}, but agent_log has no ${agent} run within 7 days of it`, resolution: 'unresolved' });
            }
          }
        }
      }
      return r;
    }
    if (d === 'outbound_authorization') {
      const r = domainResult('empty', 'outbound_gate', null);
      const gate = snapshot.outboundGate;
      if (gate && gate.available && gate.applicable) {
        r.items.push(observed({ domain: d, tenantId: tenant, source: 'outbound_gate', provenance: gate.provenance, type: 'outbound_gate', id: 'gate', now, observedAt: now,
          facts: { mode: gate.mode, reason: gate.reason, sendingBlocked: gate.mode !== 'active' } }));
      } else if (gate && !gate.available) report.unknowns.push(unknown(d, 'outbound gate', gate.reason));
      const load = this.loaders.loadOutboundPrograms || loadOutboundPrograms;
      const programs = await load({ db, tenantId: tenant });
      if (programs.status === 'failed' || programs.status === 'unavailable') report.unknowns.push(unknown(d, 'outbound program authorization', programs.reason));
      for (const p of programs.rows || []) {
        const expiresAt = iso(p.expires_at);
        r.items.push(observed({ domain: d, tenantId: tenant, source: 'acquisition_outbound_programs', provenance: programs.provenance, type: 'outbound_program', id: p.id, now,
          observedAt: p.last_tick_at || p.authorized_at,
          facts: { mode: p.mode, authorizedAt: iso(p.authorized_at), expiresAt, authorizationExpired: expiresAt ? Date.parse(expiresAt) < now.getTime() : null,
            lastTickAt: iso(p.last_tick_at), lastErrorRecorded: p.has_error === true } }));
      }
      if (programs.status === 'empty') report.unknowns.push(unknown(d, 'outbound program authorization', 'no outbound program recorded for this tenant'));
      r.status = r.items.length ? 'found' : 'empty';
      return r;
    }
    if (d === 'campaign_performance') {
      const load = this.loaders.loadCampaignPerformance || loadCampaignPerformance;
      const c = clientScoped ? await load({ db, clientId: ctx.clientId }) : { status: 'unavailable', reason: 'tenant_has_no_client_scope' };
      const r = domainResult(c.status, 'email_performance', c.provenance);
      if (c.status === 'found') {
        r.items.push(observed({ domain: d, tenantId: tenant, source: 'email_performance', provenance: c.provenance, type: 'campaign', id: 'email', now, observedAt: c.row.last_at,
          facts: { sends: Number(c.row.sends), opens: Number(c.row.opens), replies: Number(c.row.replies), bounces: Number(c.row.bounces) } }));
      } else report.unknowns.push(unknown(d, 'campaign performance', c.reason || (c.status === 'empty' ? 'no campaign performance recorded' : c.status)));
      return r;
    }
    return domainResult('unavailable', null, null);
  }

  /**
   * Revalidate remembered ids (e.g. a ConversationLedger focus) against the
   * tenant's CURRENT records. Never substitutes another record.
   */
  async revalidate({ tenant, prospectIds, missionIds, records, raw, now, report, merge }) {
    const results = [];
    const byId = new Map((records ? records.rows : []).map((r) => [String(r.id), r]));
    for (const id of prospectIds) {
      if (!records || records.status === 'failed' || records.status === 'unavailable') {
        results.push({ kind: 'prospect', id, status: 'retrieval_failed', detail: 'current records could not be read; the remembered reference is unverified' });
        continue;
      }
      const row = byId.get(id);
      if (row) { results.push({ kind: 'prospect', id, status: 'current', evidence: prospectItem(row, tenant, now) }); continue; }
      results.push(records.complete === true
        ? { kind: 'prospect', id, status: 'not_found_in_tenant', detail: 'no current record with this id for this tenant: deleted, transferred or not accessible (indistinguishable by design); no replacement is inferred' }
        : { kind: 'prospect', id, status: 'not_verifiable_within_bound', detail: 'the bounded read was incomplete; this id may exist beyond it' });
    }
    for (const missionId of missionIds) {
      const load = this.loaders.loadMissionById || OI.loadMissionById;
      let found;
      try { found = await load({ tenantId: tenant, missionId, opts: { pool: raw, now }, acquisitionMissionRuntime: this.runtime }); } catch (_) { found = null; }
      if (found && found.mutationBoundary) merge(found.mutationBoundary);
      if (found && found.found) {
        results.push({ kind: 'mission', id: missionId, status: 'current', evidence: { status: found.mission.status, stage: found.mission.stage, classification: found.mission.classification.category, lastActivityAt: found.mission.classification.lastActivityAt } });
      } else if (!found || (found.sources || []).some((x) => x.available === false)) {
        results.push({ kind: 'mission', id: missionId, status: 'retrieval_failed', detail: 'mission store unavailable; the remembered reference is unverified' });
      } else {
        results.push({ kind: 'mission', id: missionId, status: 'not_found_in_tenant', detail: 'no current mission with this id for this tenant; no replacement is inferred' });
      }
    }
    for (const r of results) if (r.status !== 'current') report.unknowns.push(unknown('revalidation', `${r.kind} ${r.id}`, r.status));
    return { requested: results.length, results, verifiedAt: now.toISOString() };
  }

  /**
   * Retrieval directed by a validated TurnFrame. The frame chooses enum
   * domains and candidate referents; it never supplies SQL and never widens
   * scope: a foreign-tenant frame is refused before any read.
   */
  async retrieveForFrame(frame, { tenantId, question = null, referents = [] } = {}) {
    if (!frame || frame.authority?.grantsAuthority !== false || String(frame.tenantId) !== String(tenantId)) {
      return { kind: 'evidence_report', tenantId: String(tenantId), refused: 'invalid_frame', domains: {}, contradictions: [], unknowns: [], authority: { grantsAuthority: false, executionPermitted: false } };
    }
    if (frame.foreignTenantReference) {
      return { kind: 'evidence_report', tenantId: String(tenantId), refused: 'foreign_tenant_reference', domains: {}, contradictions: [],
        unknowns: [unknown(null, 'evidence about another tenant', 'refused before any read')], authority: { grantsAuthority: false, executionPermitted: false } };
    }
    return this.retrieve({ tenantId, domains: frame.evidenceNeeds, question, referents });
  }
}

module.exports = { EvidenceRegistry, FRESHNESS_DAYS, MAX_REFERENT_IDS, loadMissionEvents, loadOutboundPrograms, loadCampaignPerformance, prospectItem };
