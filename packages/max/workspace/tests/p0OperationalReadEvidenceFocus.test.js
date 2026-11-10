'use strict';

/**
 * P0 #1110 follow-up — focused operating-evidence presentation for
 * operational reads.
 *
 * Production smoke (tenant 21, 2026-11-10 21:48 UTC, pipeline OperatingEvidence)
 * returned correct tenant-21 counts but also generic absence statements written
 * for Anchor's Campaign 001 (AO leads, walkthroughs, physical mail, Yelp,
 * jobs/payments, Business Intelligence unknowns) and no pipeline breakdown.
 * Investigation classified this as shared generic templates, not cross-tenant
 * data. These tests pin the corrected behaviour and that legacy Anchor
 * operating-evidence turns are unchanged.
 */

const { describe, it, afterEach } = require('node:test');
const assert = require('node:assert/strict');

const { createHermeticWorkspaceEngine: createWorkspaceEngine } = require('./workspaceTestHarness');
const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
} = require('../../../../services/clientIntelligenceInterview');
const { loadOperatingEvidence } = require('../OperatingEvidenceRetrieval');

const LEVI = 21;
const ANCHOR = 10;

// Exact production utterance from the smoke test.
const SMOKE_QUESTION =
  'How many prospects do we currently have? Break them down by qualification and pipeline status. Identify which data sources you queried, and distinguish missing data from retrieval failures.';
const FAILURE_A =
  "Brief me on my prospects. Identify the top five opportunities, explain why they're ranked that way, and recommend the next action for each. Use actual workspace data and cite the supporting evidence.";
const FAILURE_B =
  'List the actual prospect records currently stored in this workspace. For each, return its name, record ID, prospect type, current status, and source. Do not use the Business Blueprint as a substitute for prospect data. If no records are available, explicitly state which data source was queried and why it returned no records.';

// Terms that belong to Anchor's Campaign 001 / cleaning workflows and must not
// appear as generic absence statements in a lending tenant's operational read.
const UNRELATED_RE =
  /Campaign 001|\bAO\b|walkthrough|Yelp|physically mailed|physical mail|job, or payment|conversion|Business Intelligence|Existing Scout intelligence/i;

const LENDING_ANSWERS = [
  'Private lending for active real-estate investors.',
  'Asset-backed bridge and rehab loans.',
  'Active real-estate operators and borrowers.',
  'Speculators without collateral.',
  'Charleston WV and surrounding counties.',
  'Fast, clear, asset-backed decisions.',
  'Direct and professional.',
  'Grow a qualified borrower pipeline.',
  'Funded loans with a credible repayment or exit plan.',
];

async function approve(store, clientId) {
  const opts = { store };
  const started = await startClientInterview({ clientId, forceNew: true }, opts);
  let turn = started;
  for (const answer of LENDING_ANSWERS) turn = await postInterviewMessage(started.interviewId, answer, opts);
  await approveBlueprint(turn.blueprint.id, opts);
}

const LEVI_BREAKDOWN = {
  status: 'found',
  rows: [
    { setter_status: 'unset', review_state: 'qualify_first', count: 12 },
    { setter_status: 'unset', review_state: 'contact_now', count: 6 },
    { setter_status: 'unset', review_state: 'hold', count: 1 },
  ],
};

/** Loaders shaped like tenant 21 in production: prospects + imported companies, nothing else. */
function leviLoaders(overrides = {}) {
  const calls = [];
  const companies = Array.from({ length: 19 }, (_, i) => ({ id: `c${i}`, tenantId: String(LEVI), name: `Co ${i}` }));
  const opts = {
    loadProspects: async ({ clientId }) => {
      calls.push(['prospects', Number(clientId)]);
      return {
        available: true,
        counts: { total: 19, qualified: 6, cold: 19, warm: 0, hot: 0, setter_visible: 0, booked: 0, closed: 0 },
        segments: [{ vertical: 'private_lending', count: 19 }],
        breakdown: LEVI_BREAKDOWN,
      };
    },
    // Real AO service shape for a tenant with no AO leads: always "Campaign 001".
    loadCampaignAo: async ({ clientId }) => {
      calls.push(['campaign', Number(clientId)]);
      return { available: true, progress: { campaign_name: 'Campaign 001', total: 0, visited: 0, walkthrough_requests: 0 }, leads: [], campaignName: 'Campaign 001' };
    },
    loadScout: async ({ tenantId }) => {
      calls.push(['scout', Number(tenantId)]);
      return { available: true, intelligence: { companies, counts: { matched: 19 } }, state: null, launchedNewWork: false };
    },
    loadMissions: async () => ({ available: true, rows: [] }),
    loadObjectives: async () => ({ available: true, rows: [] }),
    loadActivity: async ({ clientId }) => {
      calls.push(['activity', Number(clientId)]);
      return { available: true, touchpoints: [], activity: [], activityLogStatus: 'found' };
    },
    loadOutcomes: async () => ({ available: true, jobs: 0, payments: 0 }),
    loadOperatorAttested: async () => ({ available: false, reason: 'knowledge_unavailable', claims: [], evidence: [] }),
    loadCapability: async () => ({ available: true }),
    ...overrides,
  };
  return { calls, opts };
}

/** Anchor-shaped: Campaign 001 AO leads and real Scout acquisition state. */
function anchorLoaders() {
  return {
    calls: [],
    opts: {
      ...leviLoaders().opts,
      loadProspects: async () => ({
        available: true,
        counts: { total: 18, qualified: 7, cold: 11, warm: 5, hot: 2, setter_visible: 7, booked: 0, closed: 0 },
        segments: [{ vertical: 'property_management', count: 8 }],
        breakdown: { status: 'found', rows: [{ setter_status: 'new', review_state: 'none', count: 18 }] },
      }),
      loadCampaignAo: async ({ clientId }) => ({
        available: true,
        progress: { campaign_name: 'Campaign 001', total: 18, visited: 3, walkthrough_requests: 1 },
        leads: Array.from({ length: 18 }, (_, i) => ({ id: `ao${i}`, client_id: clientId, operational_state: i < 3 ? 'visited' : 'not_started' })),
        campaignName: 'Campaign 001',
      }),
      loadScout: async () => ({
        available: true,
        intelligence: { counts: { considered: 4, matched: 2 }, companies: [{ id: 'co-1', tenantId: '10', name: 'Granite State PM' }] },
        state: { tenantId: '10', opportunityCount: 2, summary: 'Existing Scout state' },
        launchedNewWork: false,
      }),
    },
  };
}

async function open({ tenantId = LEVI, loaders = leviLoaders() } = {}) {
  const store = createMemoryStore();
  await approve(store, tenantId);
  const engine = createWorkspaceEngine({
    disableLlm: true,
    missionsEnabled: true,
    clientIntelligenceOpts: { store },
    operatingEvidenceOpts: loaders.opts,
  });
  const opened = engine.open({ tenantId: String(tenantId), page: 'command-deck' });
  return { loaders, ask: (question) => engine.ask({ sessionId: opened.sessionId, question }) };
}

function assertFocusedRead(result, label) {
  assert.equal(result.workspaceOwnership.owner, 'knowledge_retrieval', label);
  assert.match(String(result.workspaceOwnership.reason), /^operational_read/, label);
  assert.equal(result.routingTrace.pipeline, 'OperatingEvidence', label);
  assert.doesNotMatch(result.prose, UNRELATED_RE, `${label}: unrelated generic wording`);
  assert.doesNotMatch(result.prose, /Unavailable in current context/, `${label}: raw source tokens`);
  assert.equal(result.mission, null, label);
}

afterEach(() => {
  delete process.env.MAX_OPERATIONAL_READ_FOCUSED_EVIDENCE;
});

describe('Levi-shaped workspace (tenant 21)', () => {
  it('production smoke question: counts + full breakdown, no unrelated wording', async () => {
    const { ask, loaders } = await open();
    const r = await ask(SMOKE_QUESTION);
    assertFocusedRead(r, 'smoke');
    assert.match(r.prose, /19 existing prospects, 6 at or above qualification threshold/);
    assert.match(r.prose, /Prospect status: 19 cold, 0 warm, 0 hot; 0 setter-visible, 0 booked, 0 closed/);
    assert.match(r.prose, /Prospects by vertical: private_lending 19/);
    assert.match(r.prose, /Setter status: not set 19/);
    assert.match(r.prose, /Review state: qualify_first 12, contact_now 6, hold 1/);
    assert.match(r.prose, /19 companies are on file for this workspace \(source: companies table\)/);
    assert.match(r.prose, /Sources queried[\s\S]*prospects table: found[\s\S]*companies table: found/);
    assert.equal(r.structured.metadata.sourceStatuses.prospects, 'found');
    assert.equal(r.retrieval.retrievalStatus, 'found');
    // Provenance preserved on every rendered evidence item.
    // Provenance preserved on every rendered evidence item (normalized into
    // `summary` by buildStructuredResponse), and only from requested sources.
    assert.ok(r.structured.supportingEvidence.length > 0);
    for (const ev of r.structured.supportingEvidence) {
      assert.match(String(ev.summary), /^(prospect repository|company records)$/, JSON.stringify(ev));
    }
    // Tenant isolation: every loader saw tenant 21 only.
    assert.ok(loaders.calls.length > 0);
    for (const [, id] of loaders.calls) assert.equal(id, LEVI);
  });

  it('Failure A and Failure B keep the record-listing disclosure without unrelated wording', async () => {
    const { ask } = await open();
    for (const q of [FAILURE_A, FAILURE_B]) {
      const r = await ask(q);
      assertFocusedRead(r, q.slice(0, 30));
      assert.match(r.prose, /Record-level listing and ranking are not available/);
      assert.match(r.prose, /19 existing prospects/);
    }
  });

  it('emails: reports the touchpoint source status only', async () => {
    const { ask } = await open();
    const r = await ask('how many emails went out?');
    assertFocusedRead(r, 'emails');
    assert.match(r.prose, /No touchpoints are recorded for this workspace \(touchpoints table queried: 0 rows\)/);
    assert.match(r.prose, /touchpoints and setter activity log: empty/);
    assert.doesNotMatch(r.prose, /existing prospects/);
  });

  it('tasks: discloses that task/callback records are outside this read', async () => {
    const { ask } = await open();
    const r = await ask('what tasks are due?');
    assertFocusedRead(r, 'tasks');
    assert.match(r.prose, /Task and callback records are not part of this read/);
    assert.match(r.prose, /No setter activity events are recorded/);
  });

  it('pipeline: AO absence is not rendered when the tenant has no AO data', async () => {
    const { ask } = await open();
    const r = await ask('pipeline status?');
    assertFocusedRead(r, 'pipeline');
    assert.match(r.prose, /Prospect status:/);
  });
});

describe('requested-source failures stay explicit', () => {
  it('prospect query failure → failed, never "nothing there"', async () => {
    const { ask } = await open({
      loaders: leviLoaders({ loadProspects: async () => { throw Object.assign(new Error('boom'), { code: 'ECONNRESET' }); } }),
    });
    const r = await ask(SMOKE_QUESTION);
    assertFocusedRead(r, 'prospects failed');
    assert.match(r.prose, /retrieval failure, not an empty result/);
    assert.match(r.prose, /prospects table: failed/);
    assert.doesNotMatch(r.prose, /No prospects are currently recorded/);
    assert.equal(r.retrieval.retrievalStatus, 'failed');
  });

  it('breakdown query failure is isolated from the totals', async () => {
    const { ask } = await open({
      loaders: leviLoaders({
        loadProspects: async () => ({
          available: true,
          counts: { total: 19, qualified: 6, cold: 19 },
          segments: [],
          breakdown: { status: 'failed', errorClass: '42703', rows: [] },
        }),
      }),
    });
    const r = await ask(SMOKE_QUESTION);
    assertFocusedRead(r, 'breakdown failed');
    assert.match(r.prose, /19 existing prospects/);
    assert.match(r.prose, /Setter-status and review-state breakdown could not be read/);
    assert.match(r.prose, /prospects table: found/);
  });

  it('activity failure on an email question → failed', async () => {
    const { ask } = await open({
      loaders: leviLoaders({ loadActivity: async () => { throw new Error('timeout'); } }),
    });
    const r = await ask('how many emails went out?');
    assertFocusedRead(r, 'activity failed');
    assert.match(r.prose, /could not be read: the query failed/);
    assert.match(r.prose, /touchpoints and setter activity log: failed/);
    assert.equal(r.retrieval.retrievalStatus, 'failed');
  });

  it('activity store unavailable on an email question → unavailable, not empty', async () => {
    const { ask } = await open({
      loaders: leviLoaders({ loadActivity: async () => ({ available: false, status: 'unavailable', reason: 'activity_store_unavailable', rows: [] }) }),
    });
    const r = await ask('how many emails went out?');
    assertFocusedRead(r, 'activity unavailable');
    assert.match(r.prose, /This is not an empty result/);
    assert.match(r.prose, /touchpoints and setter activity log: unavailable/);
  });

  it('a failure in an UNrequested source is not surfaced', async () => {
    const { ask } = await open({
      loaders: leviLoaders({ loadCampaignAo: async () => { throw new Error('ao down'); } }),
    });
    const r = await ask(SMOKE_QUESTION);
    assertFocusedRead(r, 'unrequested failure');
    assert.match(r.prose, /19 existing prospects/);
  });

  it('cross-tenant rows injected by a loader are not counted', async () => {
    const { ask } = await open({
      loaders: leviLoaders({
        loadActivity: async () => ({
          available: true,
          touchpoints: [
            { id: 1, client_id: LEVI, channel: 'email' },
            { id: 2, client_id: ANCHOR, channel: 'email' },
            { id: 3, client_id: ANCHOR, channel: 'email' },
          ],
          activity: [],
        }),
      }),
    });
    const r = await ask('how many emails went out?');
    assertFocusedRead(r, 'isolation');
    assert.match(r.prose, /Email touchpoints: 1 of the 1 most recent touchpoints/);
  });
});

describe('Anchor-shaped workspace (tenant 10) keeps its behaviour', () => {
  it('pipeline read shows verified Campaign 001 AO data and established Scout provenance', async () => {
    const { ask } = await open({ tenantId: ANCHOR, loaders: anchorLoaders() });
    const r = await ask('pipeline status?');
    assert.equal(r.routingTrace.pipeline, 'OperatingEvidence');
    assert.match(r.prose, /Campaign 001 has 18 AO leads attributed/);
    assert.match(r.prose, /Existing Scout intelligence/);
    assert.match(r.prose, /AO account records: found/);
    assert.match(r.prose, /Prospect status: 11 cold, 5 warm, 2 hot/);
    assert.doesNotMatch(r.prose, /Yelp|job, or payment/);
  });

  it('legacy operating-evidence questions are unchanged (full SPEC-105 composition)', async () => {
    const { ask } = await open({ tenantId: ANCHOR, loaders: anchorLoaders() });
    const r = await ask('What acquisition activity is already recorded?');
    assert.notEqual(String(r.workspaceOwnership.reason), 'operational_read');
    assert.match(r.prose, /Campaign 001/);
    assert.match(r.prose, /Existing Scout intelligence/);
  });
});

describe('rollback switch', () => {
  it('MAX_OPERATIONAL_READ_FOCUSED_EVIDENCE=false restores the full legacy composition (routing unchanged)', async () => {
    process.env.MAX_OPERATIONAL_READ_FOCUSED_EVIDENCE = 'false';
    const { ask } = await open();
    const r = await ask(SMOKE_QUESTION);
    assert.equal(r.routingTrace.pipeline, 'OperatingEvidence');
    assert.equal(r.workspaceOwnership.reason, 'operational_read');
    assert.match(r.prose, /Campaign 001|Yelp/);
  });
});

describe('loaders (fake pool, real SQL paths)', () => {
  function fakePool({ failBreakdown = false, failActivityLog = false, failTouchpoints = false } = {}) {
    const seen = [];
    return {
      seen,
      async query(sql, params) {
        seen.push({ sql, params });
        if (/GROUP BY 1, 2/.test(sql)) {
          if (failBreakdown) throw Object.assign(new Error('column "acquisition_metadata" does not exist'), { code: '42703' });
          return { rows: [{ setter_status: 'unset', review_state: 'qualify_first', count: 4 }] };
        }
        if (/COUNT\(\*\)::int AS total/.test(sql)) return { rows: [{ total: 4, qualified: 1, cold: 4 }] };
        if (/GROUP BY 1\s+ORDER BY count DESC\s+LIMIT 8/.test(sql)) return { rows: [{ vertical: 'private_lending', count: 4 }] };
        if (/FROM touchpoints/.test(sql)) {
          if (failTouchpoints) throw new Error('touchpoints down');
          return { rows: [] };
        }
        if (/FROM activity_log/.test(sql)) {
          if (failActivityLog) throw new Error('activity_log down');
          return { rows: [] };
        }
        return { rows: [] };
      },
    };
  }
  const nr = async () => ({ available: false, reason: 'unit_fixture_not_recorded' });
  const others = { loadCampaignAo: nr, loadScout: nr, loadMissions: nr, loadObjectives: nr, loadOutcomes: nr, loadOperatorAttested: nr, loadCapability: nr };

  it('breakdown aggregate is tenant-parameterized and its failure does not fail the totals', async () => {
    const pool = fakePool({ failBreakdown: true });
    const bundle = await loadOperatingEvidence({ tenantId: '21', clientId: 21, pool, operatingEvidenceOpts: others });
    assert.equal(bundle.prospects.status, 'found');
    assert.equal(bundle.prospects.breakdown.status, 'failed');
    assert.equal(bundle.prospects.breakdown.errorClass, '42703');
    const breakdownSql = pool.seen.find((q) => /GROUP BY 1, 2/.test(q.sql));
    assert.match(breakdownSql.sql, /WHERE client_id = \$1/);
    assert.deepEqual(breakdownSql.params, [21]);
    for (const q of pool.seen) assert.match(q.sql.trim(), /^SELECT/i);
  });

  it('breakdown found when the aggregate succeeds', async () => {
    const bundle = await loadOperatingEvidence({ tenantId: '21', clientId: 21, pool: fakePool(), operatingEvidenceOpts: others });
    assert.equal(bundle.prospects.breakdown.status, 'found');
    assert.equal(bundle.prospects.breakdown.rows[0].review_state, 'qualify_first');
  });

  it('activity: touchpoint failure is failed; activity_log failure is recorded, not empty', async () => {
    const failed = await loadOperatingEvidence({ tenantId: '21', clientId: 21, pool: fakePool({ failTouchpoints: true }), operatingEvidenceOpts: others });
    assert.equal(failed.activity.status, 'failed');
    const partial = await loadOperatingEvidence({ tenantId: '21', clientId: 21, pool: fakePool({ failActivityLog: true }), operatingEvidenceOpts: others });
    assert.equal(partial.activity.available, true);
    assert.equal(partial.activity.activityLogStatus, 'failed');
  });
});
