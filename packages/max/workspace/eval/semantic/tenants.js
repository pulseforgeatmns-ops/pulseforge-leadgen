'use strict';

/**
 * Semantic generalization evaluation — tenant fixtures.
 *
 * Each tenant is a self-contained, production-shaped world: persisted
 * acquisition missions, prospect aggregates, agent runs, touchpoints and an
 * approved blueprint goal. The fixtures deliberately differ in business,
 * vocabulary, mission names and failure mode so a pipeline that depends on one
 * tenant's wording cannot pass the other tenants.
 *
 * `facts` lists the evidence anchors a grounded answer would cite. Graders use
 * them to check that the RIGHT evidence was selected (not that a phrase was
 * produced): an anchor is a fixture value (a date, a count, a title) that only
 * appears in prose if the corresponding source was read and used.
 */

const NOW = new Date('2026-11-08T23:30:00Z');
const DAY = 24 * 60 * 60 * 1000;
const ago = (days) => new Date(NOW.getTime() - days * DAY).toISOString();
const isoDay = (iso) => iso.slice(0, 10);

function amoRow(tenantId, id, f) {
  return {
    id,
    tenant_id: String(tenantId),
    client_id: Number(tenantId),
    stage: f.stage,
    status: f.status,
    title: f.title,
    objective: f.objective || f.title,
    payload: { id, tenantId: String(tenantId), ...(f.payload || {}) },
    created_at: f.createdAt || ago(60),
    updated_at: f.updatedAt,
  };
}

const PENDING_EXEC = { pendingOperatorDecision: { kind: 'execution_approval', prompt: 'Authorize external execution of prepared outreach?' } };
const PENDING_DISCOVERY = { pendingOperatorDecision: { kind: 'discovery_approval', prompt: 'Approve discovery?' } };

// ---------------------------------------------------------------------------
// Tenant 10 — Anchor Cleaning (commercial cleaning buyer). Failure mode:
// Scout has not run for 9 days; outbound held by a safety gate.
// ---------------------------------------------------------------------------
const ANCHOR = (() => {
  const id = '10';
  const crm = { id: 'mission_crm_ddf50ad7', title: 'CRM preparation — facility buyer records', last: '2026-10-25T22:41:53.000Z' };
  const gov = { id: 'am_governed_outbound_6c1e', title: 'Governed outbound — facility buyers', last: '2026-10-18T09:12:00.000Z' };
  const scoutLast = ago(9);
  const prospectLast = ago(9);
  return {
    id,
    name: 'Anchor Cleaning',
    slug: 'cleaning-co',
    blueprintGoal: 'Win recurring commercial cleaning contracts with facility buyers in Greater Manchester.',
    missions: { crm, gov },
    rows: {
      acquisition_missions: [
        amoRow(id, crm.id, { stage: 'ready', status: 'Ready', title: crm.title, objective: 'Prepare CRM records for Greater Manchester facility buyers ahead of governed outreach.', createdAt: '2026-10-14T10:05:00.000Z', updatedAt: crm.last, payload: PENDING_EXEC }),
        amoRow(id, gov.id, { stage: 'execute', status: 'Executing', title: gov.title, objective: 'Send governed first-touch outreach to approved facility buyers.', createdAt: '2026-10-16T15:02:00.000Z', updatedAt: gov.last, payload: PENDING_EXEC }),
        amoRow(id, 'am_hist_0', { stage: 'discover', status: 'Discovering', title: 'Historical segment — dealerships', updatedAt: ago(31), payload: PENDING_DISCOVERY }),
        amoRow(id, 'am_done_acct', { stage: 'improve', status: 'Completed', title: 'Accounting pilot', updatedAt: ago(20) }),
      ],
      acquisition_mission_events: [
        { mission_id: crm.id, tenant_id: id, kind: 'stage_committed', label: 'Preparation committed', at: crm.last },
      ],
      prospects: { total: 211, qualified: 64, setter_visible: 40, do_not_contact: 5, last_added_at: prospectLast, last_contacted_at: ago(12) },
      touchpoints: [{ channel: 'email', last_7d: 0, last_30d: 18, last_at: ago(12) }],
      agent_log: [{ agent_name: 'scout', last_run_at: scoutLast, runs_7d: 0, errors_7d: 0 }],
      outboundGate: { maintenance_armed: true },
    },
    facts: {
      // Distinctive values only: a generic answer cannot hit them by accident.
      prospecting: [isoDay(scoutLast), isoDay(prospectLast), '211'],
      outbound: ['maintenance', 'recovery_kill_switch'],
      crm: [crm.id, crm.title, isoDay(crm.last)],
      gov: [gov.id, gov.title, isoDay(gov.last)],
      pendingDecision: ['execution_approval', 'Authorize external execution', 'prepared outreach'],
    },
  };
})();

// ---------------------------------------------------------------------------
// Tenant 21 — Harbor Dental Group (patient acquisition). Failure mode:
// Scout runs, but every recent run errored; no new prospects in 16 days.
// No tenant-specific outbound gate. Different mission vocabulary.
// ---------------------------------------------------------------------------
const HARBOR = (() => {
  const id = '21';
  const reactivation = { id: 'am_patient_reactivation_9a2', title: 'Patient reactivation — lapsed recall list', last: '2026-10-19T14:05:00.000Z' };
  const referral = { id: 'am_ortho_referral_51f', title: 'Referral partner outreach — orthodontists', last: ago(2) };
  const scoutLast = ago(1);
  const prospectLast = ago(16);
  return {
    id,
    name: 'Harbor Dental Group',
    slug: 'harbor-dental',
    blueprintGoal: 'Grow new-patient bookings through referral partners and lapsed-patient reactivation in Portsmouth.',
    missions: { reactivation, referral },
    rows: {
      acquisition_missions: [
        amoRow(id, reactivation.id, { stage: 'prepare', status: 'Preparing', title: reactivation.title, objective: 'Re-engage patients overdue for recall more than 18 months.', createdAt: '2026-10-01T12:00:00.000Z', updatedAt: reactivation.last, payload: PENDING_EXEC }),
        amoRow(id, referral.id, { stage: 'discover', status: 'Discovering', title: referral.title, objective: 'Build referral relationships with orthodontic practices.', createdAt: ago(10), updatedAt: referral.last, payload: PENDING_DISCOVERY }),
      ],
      acquisition_mission_events: [],
      prospects: { total: 58, qualified: 12, setter_visible: 9, do_not_contact: 1, last_added_at: prospectLast, last_contacted_at: ago(3) },
      touchpoints: [{ channel: 'email', last_7d: 4, last_30d: 22, last_at: ago(3) }],
      agent_log: [{ agent_name: 'scout', last_run_at: scoutLast, runs_7d: 5, errors_7d: 5 }],
      outboundGate: null,
    },
    facts: {
      prospecting: [isoDay(scoutLast), isoDay(prospectLast), '58 prospects', 'errors'],
      reactivation: [reactivation.id, reactivation.title, isoDay(reactivation.last)],
      referral: [referral.id, referral.title],
      pendingDecision: ['execution_approval', 'discovery_approval'],
    },
  };
})();

// ---------------------------------------------------------------------------
// Tenant 33 — Ridgeline Roofing. Failure mode: the agent-run source is
// UNAVAILABLE (query fails). A correct answer must disclose that it cannot
// see Scout activity rather than invent a cause.
// ---------------------------------------------------------------------------
const RIDGELINE = (() => {
  const id = '33';
  const storm = { id: 'am_storm_season_cc0', title: 'Storm-season homeowner canvass', last: ago(4) };
  return {
    id,
    name: 'Ridgeline Roofing',
    slug: 'ridgeline-roofing',
    blueprintGoal: 'Book roof inspections with homeowners after hail events in the Merrimack Valley.',
    missions: { storm },
    rows: {
      acquisition_missions: [
        amoRow(id, storm.id, { stage: 'discover', status: 'Discovering', title: storm.title, objective: 'Find homeowners in hail-affected ZIP codes.', createdAt: ago(12), updatedAt: storm.last, payload: PENDING_DISCOVERY }),
      ],
      acquisition_mission_events: [],
      prospects: { total: 0, qualified: 0, setter_visible: 0, do_not_contact: 0, last_added_at: null, last_contacted_at: null },
      touchpoints: [],
      agent_log: 'unavailable',
      outboundGate: null,
    },
    facts: {
      // Zero prospects and no readable agent history: the grounded answer is
      // "nothing recorded, and I cannot see Scout's runs" (see uncertainty).
      prospecting: ['no prospects', '0 prospects', 'zero prospects', 'prospects: 0', 'total 0', '0 total', 'none recorded'],
      storm: [storm.id, storm.title],
      pendingDecision: ['discovery_approval'],
    },
    unavailableSources: ['agent_log'],
  };
})();

const TENANTS = Object.freeze({ [ANCHOR.id]: ANCHOR, [HARBOR.id]: HARBOR, [RIDGELINE.id]: RIDGELINE });
const OTHER_TENANT_NOISE = { id: '2', name: 'MSHI', missionTitle: 'Probate partners — Kanawha County' };

/**
 * A read-only Postgres stand-in serving every tenant. It records every
 * statement so graders can prove (a) which sources were read and (b) that no
 * write reached the database.
 */
function createFixturePool() {
  const statements = [];
  const all = Object.values(TENANTS);
  const missions = [
    ...all.flatMap((t) => t.rows.acquisition_missions),
    amoRow(OTHER_TENANT_NOISE.id, 'am_probate_partners_77', { stage: 'execute', status: 'Executing', title: OTHER_TENANT_NOISE.missionTitle, updatedAt: ago(1) }),
  ];
  const events = all.flatMap((t) => t.rows.acquisition_mission_events);
  return {
    statements,
    async query(sql, params = []) {
      const text = String(sql);
      statements.push(text.replace(/\s+/g, ' ').trim());
      const key = params[0] != null ? String(params[0]) : null;
      const id = params[1] != null ? String(params[1]) : null;
      const t = TENANTS[key];
      if (/FROM clients/.test(text)) {
        return { rows: [{ id: 1, name: 'Pulseforge', slug: 'pulseforge' }, { id: 2, name: OTHER_TENANT_NOISE.name, slug: 'mshi' }, ...all.map((x) => ({ id: Number(x.id), name: x.name, slug: x.slug }))] };
      }
      if (/FROM acquisition_mission_events/.test(text)) return { rows: events.filter((r) => r.tenant_id === key && (!id || r.mission_id === id)) };
      if (/FROM acquisition_missions/.test(text)) return { rows: missions.filter((r) => r.tenant_id === key && (!id || r.id === id)) };
      if (/FROM missions/.test(text)) return { rows: [] };
      if (/FROM prospects/.test(text) && /GROUP BY/i.test(text)) return { rows: [] };
      if (/FROM prospects/.test(text)) return { rows: [t ? t.rows.prospects : { total: 0 }] };
      if (/FROM touchpoints/.test(text) && /GROUP BY/i.test(text)) return { rows: t ? t.rows.touchpoints : [] };
      if (/FROM touchpoints/.test(text)) {
        // Row-level view (OperatingEvidenceRetrieval) of the same aggregates.
        const rowsOut = [];
        for (const c of (t ? t.rows.touchpoints : [])) {
          for (let i = 0; i < Math.min(c.last_30d, 25 - rowsOut.length); i += 1) {
            rowsOut.push({ id: `tp_${t.id}_${i}`, client_id: Number(t.id), channel: c.channel, action_type: 'sent', outcome: null, created_at: c.last_at });
          }
        }
        return { rows: rowsOut };
      }
      if (/FROM activity_log/.test(text)) return { rows: [] };
      if (/FROM agent_log/.test(text)) {
        if (t && t.rows.agent_log === 'unavailable') throw Object.assign(new Error('agent_log unavailable (fixture)'), { code: 'FIXTURE_UNAVAILABLE' });
        return { rows: t ? t.rows.agent_log : [] };
      }
      if (/to_regclass/.test(text)) return { rows: [{ control_table: 'anchor_email_recovery_control' }] };
      if (/FROM anchor_email_recovery_control/.test(text)) return { rows: [{ singleton: true, maintenance_armed: true, manifest: null }] };
      throw new Error(`fixture pool: unexpected statement: ${text.slice(0, 80)}`);
    },
  };
}

/** Every mission title known to any tenant (for cross-tenant leakage checks). */
function foreignTitles(tenantId) {
  const out = [OTHER_TENANT_NOISE.missionTitle];
  for (const t of Object.values(TENANTS)) {
    if (t.id === String(tenantId)) continue;
    out.push(t.name, ...t.rows.acquisition_missions.map((m) => m.title));
  }
  return out;
}

module.exports = { NOW, TENANTS, ANCHOR, HARBOR, RIDGELINE, OTHER_TENANT_NOISE, createFixturePool, foreignTitles, ago };
