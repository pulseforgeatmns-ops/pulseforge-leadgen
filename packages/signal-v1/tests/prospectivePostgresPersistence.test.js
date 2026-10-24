'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('../../../test/helpers/disposablePostgres');
const { ProspectivePostgresStore } = require('../storage/ProspectivePostgresStore');
const { ShadowModeService } = require('../prospective/ShadowModeService');
const { OperatorAlertOutbox } = require('../operator/alertOutbox');
const token='2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump';

test('prospective PostgreSQL persists real pipeline-shaped test evidence, delay jobs and isolated outbox across restart', async () => {
  // Isolated local test records only; never connect this test to DATABASE_URL.
  const instance = await startDisposablePostgres('sig-prospective-', {socketPrefix:'sigp-'});
  const pool = new Pool({connectionString:instance.connectionString});
  try {
    let now = new Date('2026-10-07T00:00:00Z');
    const store = await ProspectivePostgresStore.create(pool);
    const provider = { providerId:'local-test',
      getLiveTokenSnapshot:async()=>({ tokenAddress:token,priceUsd:1,
        occurredAt:now,observedTimestamp:now,providerTimestamp:null,
        intervalSeconds:0,provenance:{dataClass:'EMPIRICAL',freshness:'UNKNOWN'}}),
      getHistoricalPrices:async()=>[],
    };
    const service = new ShadowModeService(store,{collectors:[],marketProvider:provider,now:()=>now});
    await service.ensureProspectiveCohortStarted();
    await service.upsertSourceRegistryEntry({sourceId:'telegram-front-runners',displayName:'Local fixture',
      platform:'telegram',collectorId:'operator-json-feed',externalRef:'local-test',sourceRole:'CALLER',
      clusterRelationshipStatus:'UNKNOWN',provenance:{testEnvironment:true},active:true});
    now = new Date('2026-10-07T00:00:30Z');
    const raw={sourceId:'telegram-front-runners',externalMessageId:'local-fixture:1',
      messageTimestamp:'2026-10-07T00:00:20Z',ingestedAt:now,tokenCa:token,
      provenance:{dataClass:'EMPIRICAL',telegramChannelId:'local-test'}};
    await service.ingestRawCallerObservation(raw,{collectorId:'operator-json-feed'});
    assert.equal(store.events.length,1);
    assert.equal(store.researchObservations.length,1);
    assert.equal(store.prospectiveJobs.length,6);
    assert.equal(store.researchObservations[0].occurredAt.toISOString(),now.toISOString());
    // Emulate interruption after observation persistence but before all job writes.
    await pool.query('DELETE FROM signal_prospective_research_jobs WHERE target_delay_seconds=15');
    const interrupted = await ProspectivePostgresStore.create(pool);
    assert.equal(interrupted.prospectiveJobs.length,5);
    const recovery = new ShadowModeService(interrupted,{collectors:[],marketProvider:provider,now:()=>now});
    await recovery.ingestRawCallerObservation(raw,{collectorId:'operator-json-feed'});
    assert.equal(interrupted.prospectiveJobs.length,6);
    assert.equal(interrupted.events.length,1);
    assert.equal(interrupted.researchObservations.length,1);
    assert.equal((await pool.query('SELECT count(*) FROM signal_prospective_internal_alerts')).rows[0].count,'1');
    const quote=(await pool.query('SELECT provider_timestamp FROM signal_market_observations')).rows[0];
    assert.equal(quote.provider_timestamp,null);
    now = new Date('2026-10-07T00:05:31Z');
    await service.runDueJobs(50,{jobType:'DELAY_CAPTURE'});
    assert.equal(store.prospectiveJobs.filter(j=>j.status==='COMPLETE').length,5);
    assert.ok(store.researchObservationOutcomes.every(o=>o.metadata.captureWindow==='LATE'));
    const restarted = await ProspectivePostgresStore.create(pool);
    assert.equal(restarted.rawCallerEvidence.length,1);
    assert.equal(restarted.events.length,1);
    assert.equal(restarted.researchObservations.length,1);
    assert.equal(restarted.prospectiveJobs.filter(j=>j.status==='PENDING_OUTCOME').length,1);
    assert.equal(restarted.researchCohorts.values().next().value.metadata.startedAt,'2026-10-07T00:00:00.000Z');
    assert.equal(restarted.researchCohorts.values().next().value.metadata.blinded,true);
    const next = new ShadowModeService(restarted,{collectors:[],marketProvider:provider,now:()=>now});
    await next.ingestRawCallerObservation(raw,{collectorId:'operator-json-feed'});
    assert.equal(restarted.events.length,1);
    assert.equal(restarted.researchObservations.length,1);
    // Separate, explicit outbox migration; no hook from the live scheduler.
    await pool.query(fs.readFileSync(path.join(__dirname,'../../../migrations/2026-10-07-signal-v1-operator-outbox.sql'),'utf8'));
    const outbox = new OperatorAlertOutbox(pool,{channelId:'local-test'});
    const input={evidence:restarted.rawCallerEvidence[0],snapshot:restarted.marketObservations[0],researchState:'PENDING_OUTCOME_24H'};
    await outbox.enqueue(input); await outbox.enqueue(input);
    assert.equal((await outbox.listPending()).length,1);
    assert.equal((await new OperatorAlertOutbox(pool,{channelId:'local-test'}).listPending()).length,1);
    await assert.rejects(pool.query("UPDATE signal_operator_alert_outbox SET delivery_state='DELIVERED'"));
  } finally { await pool.end(); await instance.stop(); }
});
