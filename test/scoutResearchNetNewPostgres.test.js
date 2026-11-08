'use strict';
// SPEC-SCOUT-P0-007 (#1072), research-only path on disposable PostgreSQL via
// the real Max control loop: known identities (CRM prospect, contactless CRM
// company, enrichment queue, previously retained research candidate) are never
// retained or counted as net-new; the receipt separates accepted from known;
// research retention is never outreach-ready; recovery holds stay enforced.
// Synthetic Places only; no website, provider or production database.
const deny = require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');
const { startResearchOnlyHarness, placesFetch, REVISION } = require('./helpers/scoutResearchOnlyFixture');

const quiet = { log() {}, warn() {}, error() {} };
const iso = ms => new Date(Math.floor((Date.now() + ms) / 1000) * 1000).toISOString();

// Wrap the synthetic Places transport: every text search also returns the
// extra known businesses below, typed like the query's own classifiable result.
function withKnownBusinesses(base, extras) {
  const details = new Map();
  const fetchImpl = async input => {
    const url = new URL(input);
    if (url.pathname.endsWith('/details/json') && details.has(url.searchParams.get('place_id'))) {
      base.calls.push({ path: url.pathname, placeId: url.searchParams.get('place_id') });
      const place = details.get(url.searchParams.get('place_id'));
      const data = { status: 'OK', result: { ...place, formatted_address: '1 Main St, Manchester, NH 03101',
        formatted_phone_number: '(603) 555-0101', business_status: 'OPERATIONAL',
        address_components: [{ long_name: 'Manchester', types: ['locality'] }, { short_name: 'NH', types: ['administrative_area_level_1'] }] } };
      return { ok: true, status: 200, json: async () => data, text: async () => JSON.stringify(data) };
    }
    const response = await base.fetchImpl(input);
    if (!url.pathname.endsWith('/textsearch/json')) return response;
    const data = await response.json();
    const types = data.results[1]?.types || ['point_of_interest'];
    for (const extra of extras()) {
      details.set(extra.place_id, { place_id: extra.place_id, name: extra.name, website: extra.website, types });
      data.results.push({ place_id: extra.place_id, name: extra.name, formatted_address: '1 Main St, Manchester, NH 03101', types });
    }
    return { ok: true, status: 200, json: async () => data, text: async () => JSON.stringify(data) };
  };
  return { calls: base.calls, fetchImpl };
}

test('SPEC-SCOUT-P0-007 research-only: known identities never become net-new and research is never outreach-ready',
  { timeout: 600000 }, async t => {
    const h = await startResearchOnlyHarness(); t.after(() => h.close());
    const { pool } = h;
    const grants = require('../services/scoutResearchOnlyGrant');
    const missions = require('../services/scoutResearchMission');
    const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
    require('../utils/placesQueryCache').clearPlacesQueryCache();
    const extras = [
      // Already in Tenant 10's enrichment queue under a different URL shape.
      { place_id: 'p007-queued', name: 'Known Queue Facility', website: 'https://www.Known-Queue.example/visit' },
      // A CRM company with no prospect row, stored only as a full URL.
      { place_id: 'p007-contactless', name: 'Contactless Customer Facility', website: 'https://contactless-customer.example' },
    ];
    const places = withKnownBusinesses(placesFetch(), () => extras);
    globalThis.fetch = places.fetchImpl;
    await pool.query(`INSERT INTO api_spend_policies(provider,daily_request_limit,monthly_request_limit,daily_usd_limit,monthly_usd_limit,paused,updated_by)
      VALUES('google_places',NULL,NULL,50,500,false,'operator')`);
    await pool.query(`INSERT INTO scout_unenriched(client_id,company,website_url,domain,vertical,location,source,enrichment_attempts)
      VALUES(10,'Known Queue Facility','https://known-queue.example','known-queue.example','school','Manchester, NH','max_buffer_replenishment',0)`);
    await pool.query(`INSERT INTO companies(id,client_id,name,domain,website) VALUES('p007-contactless-co',10,'Contactless Customer Facility',NULL,
      'HTTP://WWW.CONTACTLESS-CUSTOMER.EXAMPLE/')`);

    const count = async (sql, values = []) => (await pool.query(sql, values)).rows[0].n;
    const crm = async () => ({ queue: await count('SELECT count(*)::int n FROM scout_unenriched'),
      companies: await count('SELECT count(*)::int n FROM companies'), prospects: await count('SELECT count(*)::int n FROM prospects') });
    const ticketReason = async placeId => (await pool.query(`SELECT w.state,w.reason FROM scout_candidate_work w
      JOIN scout_candidate_memberships m USING(tenant_id,search_fingerprint,plan_version,ticket_version,candidate_key)
      WHERE w.tenant_id='10' AND m.raw_candidate->>'placeId'=$1 LIMIT 1`, [placeId])).rows[0];
    const retainedDomains = async () => (await pool.query('SELECT lower(domain) d FROM scout_research_candidates ORDER BY id')).rows.map(r => r.d);
    const cycle = id => runMaxOutboundControlLoop({ pool, tenantId: '10', cycleId: id, logger: quiet,
      runPreparationRefill: async () => assert.fail('preparation must never run') });

    const missionPreview = await missions.previewResearchMission(pool, { approvalRef: 'p007-mission' }, h.actor);
    const research = await missions.installResearchMission(pool, { approvalRef: 'p007-mission',
      mission: missionPreview.mission, missionHash: missionPreview.missionHash }, h.actor);
    const grantPreview = await grants.previewResearchOnlyGrant(pool, { approvalRef: 'p007-grant', expiresAt: iso(86400000),
      statement: 'Synthetic operator approval for research-only Scout discovery.', researchMissionId: research.missionId }, h.actor);
    assert.deepEqual(grantPreview.blockers, []);
    const grant = await grants.installResearchOnlyGrant(pool, { manifest: grantPreview.manifest, manifestHash: grantPreview.manifestHash }, h.actor);
    const crmBefore = await crm();

    let first;
    await t.test('known CRM, contactless CRM and queued identities are resolved, not retained', async () => {
      first = await cycle('p007-1');
      assert.equal(first.researchOnly.status, 'completed', JSON.stringify(first.researchOnly));
      const retained = await retainedDomains();
      assert.ok(retained.length > 0, 'genuinely new facilities are still retained');
      for (const domain of ['known-queue.example', 'contactless-customer.example', 'owned-anchor-customer.example']) {
        assert.ok(!retained.includes(domain), `${domain} must not be retained`);
      }
      assert.deepEqual(await ticketReason('p007-queued'), { state: 'handled', reason: 'already_queued' });
      assert.deepEqual(await ticketReason('p007-contactless'), { state: 'deferred', reason: 'ownership_policy_observed' });
      const r = first.researchOnly;
      assert.ok(r.previouslyKnownCandidates >= 3, JSON.stringify(r));
      assert.equal(r.netNewRetained, retained.length);
      assert.equal(r.retainedCandidates, retained.length);
      // Research retention is never clean, outreach-ready inventory.
      assert.equal(r.netNewCleanInventory, 0); assert.equal(r.outreachReady, false); assert.equal(r.queuedForEnrichment, 0);
      assert.equal(r.scorecard.netNewCleanInventory, 0); assert.equal(r.scorecard.outreachReady, false);
      assert.equal(r.scorecard.newlyRetained, retained.length); assert.equal(r.scorecard.newlyQueued, 0);
      assert.equal(first.replenishment.newQualifiedInventory, 0);
      assert.notEqual(first.replenishment.status, 'replenished');
      assert.equal(first.replenishment.researchYield, 'retained_research_candidates');
    });

    await t.test('a retained research candidate rediscovered under another place id and URL is already known', async () => {
      const prior = (await pool.query('SELECT company,domain FROM scout_research_candidates ORDER BY id LIMIT 1')).rows[0];
      extras.push({ place_id: 'p007-retained-again', name: prior.company, website: `https://WWW.${prior.domain.toUpperCase()}/about` });
      const before = await retainedDomains();
      await require('./helpers/releaseResearchCycle').releaseNextResearchCycle(pool, h.actor, grant.grantId, 'p007-release-2');
      const second = await cycle('p007-2');
      assert.equal(second.researchOnly.status, 'completed', JSON.stringify(second.researchOnly));
      assert.deepEqual(await ticketReason('p007-retained-again'), { state: 'handled', reason: 'already_queued' });
      const after = await retainedDomains();
      assert.equal(new Set(after).size, after.length, 'no identity retained twice');
      assert.deepEqual(after.slice(0, before.length), before);
      assert.equal(second.researchOnly.netNewRetained, after.length - before.length, 'net-new equals the durable ledger delta');
    });

    await t.test('the receipt separates accepted (net-new) tickets from previously known ones', async () => {
      const receipt = await grants.inspectResearchOnlyGrant(pool, grant.grantId);
      const { ledger, retained } = receipt.candidates;
      assert.equal(ledger.acceptedTickets, retained.total, 'with one grant on this fingerprint, every accepted ticket was retained');
      assert.equal(ledger.acceptedNotRetained, 0);
      assert.ok(ledger.previouslyKnownTickets >= 2);
      assert.equal(ledger.ledgerSpansOtherPasses, true);
      assert.equal(ledger.grantCyclesConsumed, 2);
      assert.ok(receipt.candidates.perCycle.every(row => Number.isSafeInteger(row.previouslyKnownCandidates)));
    });

    await t.test('recovery holds remain enforced: a revision change halts before any provider call', async () => {
      const calls = places.calls.length;
      process.env.RAILWAY_GIT_COMMIT_SHA = 'c'.repeat(40);
      try {
        await require('./helpers/releaseResearchCycle').releaseNextResearchCycle(pool, h.actor, grant.grantId, 'p007-release-3')
          .catch(() => {}); // release itself is refused under a mismatched revision
        const held = await cycle('p007-3');
        assert.equal(held.halted, 'research_only_revision_mismatch');
        assert.equal(places.calls.length, calls, 'no Places request under a mismatched revision');
      } finally { process.env.RAILWAY_GIT_COMMIT_SHA = REVISION; }
      assert.equal(process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED, 'true', 'provider pause untouched');
      assert.equal(process.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE, 'true', 'email recovery maintenance untouched');
    });

    await t.test('boundaries: no enrichment, CRM write, queue write or real transport', async () => {
      assert.deepEqual(await crm(), crmBefore);
      assert.equal(deny.attempts(), 0);
    });
  });
