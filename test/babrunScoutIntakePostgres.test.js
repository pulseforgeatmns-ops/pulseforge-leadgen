'use strict';
// Non-skipping real PostgreSQL test. Only the disposable harness supplies a DB.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { loadScoutSave, runScoutWaterfall, verificationReceipt, leadFixture, ownerFixture } = require('./helpers/babrunScoutIntake');
const { governedContactReason } = require('../utils/governedContactEligibility');
const { loadCleanInventory } = require('../services/maxOutboundControlLoop');
const { GovernedOutboundStore } = require('../services/governedOutboundStore');
const { windowReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { evaluatePreparationRefill } = require('../services/governedOutboundRefill');
const companies = ['00000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-000000000002', '00000000-0000-4000-8000-000000000003'];
const policy = { tenantId: '13', allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'] };
const mission = { id: 'fixture-mission', payload: { structuredMission: {
  market: { segment: 'small_business_owners', industry: 'general', buyer: 'business_owner' },
  geography: { region: 'United States', scope: 'nationwide', cities: [] },
} } };

test('standard Scout INSERT/read-back and existing inventory guards on disposable PostgreSQL', async t => {
  const pg = await startDisposablePostgres('babrun-scout-intake-');
  const db = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 2 });
  t.after(() => closePostgresFixture(db, pg));
  await db.query(fs.readFileSync(path.join(__dirname, 'fixtures/babrunScoutIntakeSchema.sql'), 'utf8'));
  await db.query(`INSERT INTO companies VALUES
    ('${companies[0]}',13,'Fixture Painting','fixturepainting.example','https://fixturepainting.example','Denver, CO','painting'),
    ('${companies[1]}',13,'Other Fixture','otherfixture.example','https://otherfixture.example','Denver, CO','painting'),
    ('${companies[2]}',10,'Tenant Ten Fixture','tenantten.example','https://tenantten.example','Denver, CO','painting')`);
  const store = new GovernedOutboundStore(db, '13');
  const inventory = (source = mission) => loadCleanInventory(db, store, source, 13, policy);
  const read = async email => (await db.query('SELECT * FROM prospects WHERE email=$1', [email])).rows[0];
  const clear = () => db.query(`TRUNCATE prospects, touchpoints, ao_prospect_tasks, ao_leads, ao_contacts,
    acquisition_outbound_lifecycle, acquisition_outbound_items, acquisition_mission_outbound_executions`);
  const insert = async (selected = ownerFixture(), options = {}) => {
    const { lead } = await runScoutWaterfall(leadFixture(), { hunter: selected });
    if (options.modify) options.modify(lead);
    const intake = loadScoutSave({ db, companyId: companies[0], verify: async () => options.verification || verificationReceipt, ...options });
    assert.equal((await intake.save([lead])).saved, 1);
    return read(lead.email);
  };

  await t.test('supported owner becomes contact-eligible with address-bound source and exact verification receipt', async () => {
    const row = await insert();
    assert.equal(governedContactReason(row, policy), null);
    assert.equal(row.job_title, 'Owner'); assert.equal(row.first_name, 'Ada'); assert.equal(row.last_name, 'Lovelace');
    assert.equal(row.acquisition_metadata.contactResolution.bestEmail, row.email);
    assert.equal(row.acquisition_metadata.contactResolution.verification.verifiedAt, row.verified_at.toISOString());
    assert.deepEqual(row.acquisition_metadata.contactResolution.verification.raw, row.verifier_response);
    assert.equal(row.enrichment_provenance.email.source, 'hunter');
    assert.equal(row.enrichment_provenance.email.source_url, ownerFixture().sourceUrl);
    const inv = await inventory();
    assert.equal(inv.clean.length, 1, JSON.stringify(inv));
    assert.equal(inv.clean[0].contactClassification, 'VERIFIED_FOUNDER_EMAIL');
    // Clean inventory is only one prerequisite; it does not authorize or schedule mail.
    assert.equal(evaluatePreparationRefill({ cleanInventory: 1, grantActive: false }).shouldPrepare, false);
    assert.equal(windowReason({ expiresAt: '2026-10-13T00:00:00Z' }, new Date('2026-10-14T00:00:00Z'), true), 'authorization_expired');
    assert.equal((await db.query('SELECT count(*)::int AS n FROM acquisition_outbound_items')).rows[0].n, 0);
  });

  await t.test('role, unnamed, unsupported, pattern, risky, unknown and changed-address remain held after persistence', async () => {
    const variants = [
      { selected: { ...ownerFixture(), email: 'info@fixturepainting.example' } },
      { selected: { ...ownerFixture(), contact: '', title: '' } },
      { modify: lead => { lead.scoutContactEvidence.source = 'third_party'; } },
      { modify: lead => { lead.scoutContactEvidence.source = 'pattern_candidate'; } },
      { verification: { ...verificationReceipt, emailVerified: false, emailStatus: 'risky' } },
      { verification: { ...verificationReceipt, emailVerified: false, emailStatus: 'unknown' } },
      { verification: { ...verificationReceipt, reject: true, rejectReason: 'rejected_fixture' } },
      { modify: lead => { lead.email = 'grace@fixturepainting.example'; } },
    ];
    for (const variant of variants) {
      await clear(); const row = await insert(variant.selected, variant);
      assert.notEqual(governedContactReason(row, policy), null);
      assert.equal((await inventory()).clean.length, 0);
    }
  });

  await t.test('same-company duplicate, cross-company conflict and cross-tenant conflict cannot overwrite evidence', async () => {
    await clear(); const row = await insert();
    const baseline = JSON.stringify(await read(row.email));
    for (const input of [
      { lead: { ...leadFixture(), ...ownerFixture() }, clientId: 13, companyId: companies[0] },
      { lead: { ...leadFixture(), ...ownerFixture(), company: 'Other Fixture' }, clientId: 13, companyId: companies[1] },
      { lead: { ...leadFixture(), ...ownerFixture(), company: 'Tenant Ten Fixture' }, clientId: 10, companyId: companies[2] },
    ]) {
      const intake = loadScoutSave({ db, clientId: input.clientId, companyId: input.companyId });
      const result = await intake.save([input.lead]);
      assert.equal(result.saved, 0); assert.equal(result.skipped, 1);
      assert.equal(intake.writes.length, 0);
      assert.equal(JSON.stringify(await read(row.email)), baseline);
    }
  });

  await t.test('DNC, human ownership, prior contact, suppression, prior execution and mission checks still hold', async () => {
    const scenarios = [
      ["UPDATE prospects SET do_not_contact=true", null],
      ["UPDATE prospects SET assigned_ao_id=24", 'prior_contact_or_human_owned'],
      ["UPDATE prospects SET last_contacted_at=now()", 'prior_contact_or_human_owned'],
      ["INSERT INTO touchpoints SELECT 13,id,'email_sent' FROM prospects", 'prior_contact_or_human_owned'],
      ["INSERT INTO acquisition_outbound_lifecycle SELECT '13',true,email,company_id::text,'unsubscribed' FROM prospects", 'unsubscribed'],
      ["INSERT INTO acquisition_outbound_items SELECT 'prior','13',now(),email,company_id::text FROM prospects", 'already_attempted'],
      ["INSERT INTO acquisition_mission_outbound_executions SELECT 'prior','13','sent','other-mission',jsonb_build_object('email',email),id::text FROM prospects", 'prior_canonical_execution'],
    ];
    for (const [sql, reason] of scenarios) {
      await clear(); await insert(); await db.query(sql);
      const inv = await inventory(); assert.equal(inv.clean.length, 0, sql);
      if (reason) assert.deepEqual(inv.excluded.map(row => row.reason), [reason], JSON.stringify(inv));
    }
    await clear(); await insert();
    const wrongMission = { ...mission, payload: { structuredMission: {
      market: { segment: 'law_firm', industry: 'legal' },
      geography: { region: 'United States', scope: 'nationwide', cities: [] },
    } } };
    assert.equal((await inventory(wrongMission)).clean.length, 0);
    const localMission = { ...mission, payload: { structuredMission: {
      market: { segment: 'small_business_owners' },
      geography: { region: 'Manchester, NH', scope: 'local', cities: ['Manchester'] },
    } } };
    assert.equal((await inventory(localMission)).clean.length, 0);
  });
});
