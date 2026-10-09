'use strict';

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { createTelegramCallerFeedEngine } = require('../../../services/telegramCallerFeed/engine');
const { loadTelegramCredentials } = require('../../../services/telegramCallerFeed/credentials');
const { createOperatorJsonFeedCollector } = require('../collectors/operatorJsonFeedCollector');
const { InMemorySignalStore } = require('../storage/InMemorySignalStore');
const { ShadowModeService } = require('../prospective/ShadowModeService');
const { PROSPECTIVE_COHORT_001_ID, CLUSTER_RELATIONSHIP } = require('../prospective/constants');

function singleSourceEnv() {
  process.env.TELEGRAM_CALLER_SOURCES_JSON = JSON.stringify([
    {
      sourceId: 'telegram-front-runners',
      displayName: 'Front Runners',
      username: 'frontrunz',
      platform: 'telegram',
      sourceRole: 'CALLER',
      clusterRelationshipStatus: CLUSTER_RELATIONSHIP.UNKNOWN,
      collector: 'telegram-caller-feed',
    },
  ]);
}

describe('SIGNAL-V1-007 telegram empirical caller feed', () => {
  it('never invents an empirical timestamp for a message without its Telegram date', () => {
    const {mapGramMessage}=require('../../../services/telegramCallerFeed/telegramAdapter');
    assert.equal(mapGramMessage({id:1,message:'fixture'},{id:123}),null);
    assert.equal(mapGramMessage({id:1,message:'fixture',date:NaN},{id:123}),null);
  });
  it('fails closed on a corrupted durable cursor instead of silently resetting', () => {
    const dir=fs.mkdtempSync(path.join(os.tmpdir(),'tg-corrupt-'));
    const file=path.join(dir,'state.json');
    try {
      fs.writeFileSync(file,'{broken');
      assert.throws(()=>require('../../../services/telegramCallerFeed/stateStore').loadState(file),/caller_feed_state_unreadable/);
    } finally { fs.rmSync(dir,{recursive:true,force:true}); }
  });
  let statePath;
  let tmpDir;

  it('expired pilot disconnects and cannot start another Telegram read', async () => {
    let disconnects=0,reads=0;
    const engine=createTelegramCallerFeedEngine({statePath,client:{disconnect:async()=>{disconnects++;},getEntity:async()=>{reads++;}},pilotGate:()=>({ok:false})});
    const result=await engine.pollOnce();
    assert.equal(result.connected,false);assert.equal(reads,0);assert.equal(disconnects,1);
    await engine.pollOnce();assert.equal(disconnects,1);
  });

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tg-feed-'));
    statePath = path.join(tmpDir, 'state.json');
    delete process.env.TELEGRAM_API_ID;
    delete process.env.TELEGRAM_API_HASH;
    delete process.env.TELEGRAM_SESSION_STRING;
    delete process.env.TELEGRAM_CALLER_SOURCES_JSON;
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('credentials absent => fail closed', () => {
    const creds = loadTelegramCredentials();
    assert.equal(creds.ok, false);
    assert.ok(creds.missing.includes('TELEGRAM_API_ID'));
  });

  it('dedupes telegram messages and preserves occurredAt on restart', async () => {
    process.env.TELEGRAM_API_ID = '1';
    process.env.TELEGRAM_API_HASH = 'hash';
    process.env.TELEGRAM_SESSION_STRING = 'session';
    singleSourceEnv();

    const channelId = '-100123';
    const occurredAt = new Date('2026-10-06T00:00:00.000Z');
    const mockClient = {
      async getEntity() {
        return { id: channelId, username: 'frontrunz' };
      },
      async getMessages(_entity, opts) {
        if (opts.limit === 1) return [{ id: 10, message: 'seed', date: occurredAt.getTime() / 1000 }];
        if (opts.minId === 10) {
          return [
            {
              id: 11,
              message: 'buy 2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump',
              date: occurredAt.getTime() / 1000,
            },
          ];
        }
        return [];
      },
    };

    const engine = createTelegramCallerFeedEngine({
      statePath,
      client: mockClient,
      now: () => new Date('2026-10-06T00:00:05.000Z'),
    });

    const first = await engine.pollOnce();
    assert.equal(first.emitted.length, 1);
    assert.equal(first.emitted[0].occurredAt, occurredAt.toISOString());
    assert.equal(first.emitted[0].ingestedAt, '2026-10-06T00:00:05.000Z');

    const second = await engine.pollOnce();
    assert.equal(second.emitted.length, 0);

    const restarted = createTelegramCallerFeedEngine({
      statePath,
      client: mockClient,
      now: () => new Date('2026-10-06T00:00:10.000Z'),
    });
    const third = await restarted.pollOnce();
    assert.equal(third.emitted.length, 0);
    assert.deepEqual(restarted.getRecentCalls(), first.emitted,
      'a restart must preserve unread calls, their timestamps and provenance');
  });

  it('records edits without rewriting original occurredAt', async () => {
    process.env.TELEGRAM_API_ID = '1';
    process.env.TELEGRAM_API_HASH = 'hash';
    process.env.TELEGRAM_SESSION_STRING = 'session';
    singleSourceEnv();

    const channelId = '-100999';
    let pass = 0;
    const mockClient = {
      async getEntity() {
        return { id: channelId, username: 'frontrunz' };
      },
      async getMessages(_entity, opts) {
        if (opts.limit === 1) return [{ id: 5, message: 'seed', date: 1_700_000_000 }];
        if (pass === 0) {
          pass += 1;
          return [{ id: 6, message: 'alpha', date: 1_700_000_100 }];
        }
        return [{ id: 6, message: 'alpha edited', date: 1_700_000_100, editDate: 1_700_000_200 }];
      },
    };

    const engine = createTelegramCallerFeedEngine({
      statePath,
      client: mockClient,
      now: () => new Date('2026-10-06T01:00:00.000Z'),
    });
    await engine.pollOnce();
    const edited = await engine.pollOnce();
    assert.equal(edited.emitted.length, 1);
    assert.equal(edited.emitted[0].occurredAt, new Date(1_700_000_100 * 1000).toISOString());
    assert.ok(edited.emitted[0].provenance.messageEditAt);
  });

  it('preserves forwarding metadata', async () => {
    process.env.TELEGRAM_API_ID = '1';
    process.env.TELEGRAM_API_HASH = 'hash';
    process.env.TELEGRAM_SESSION_STRING = 'session';
    singleSourceEnv();
    const channelId = '-100555';
    const mockClient = {
      async getEntity() {
        return { id: channelId, username: 'frontrunz' };
      },
      async getMessages(_entity, opts) {
        if (opts.limit === 1) return [{ id: 1, message: 'seed', date: 1 }];
        return [
          {
            id: 2,
            message: 'fwd',
            date: 1_700_000_300,
            fwdFrom: { fromName: 'Origin Channel', channelPost: 99, date: 1_700_000_000 },
          },
        ];
      },
    };
    const engine = createTelegramCallerFeedEngine({
      statePath,
      client: mockClient,
      now: () => new Date('2026-10-06T02:00:00.000Z'),
    });
    const poll = await engine.pollOnce();
    assert.ok(poll.emitted[0].forwarding);
    assert.equal(poll.emitted[0].forwarding.fromName, 'Origin Channel');
  });

  it('marks unauthorized channel unavailable without synthetic observations', async () => {
    process.env.TELEGRAM_CALLER_SOURCES_JSON = JSON.stringify([
      { sourceId: 'test-available', username: 'frontrunz' },
      { sourceId: 'test-unavailable', username: 'unavailable_test_source' },
    ]);
    process.env.TELEGRAM_API_ID = '1';
    process.env.TELEGRAM_API_HASH = 'hash';
    process.env.TELEGRAM_SESSION_STRING = 'session';
    const mockClient = {
      async getEntity(username) {
        if (username === 'frontrunz') return { id: '-1001', username };
        throw new Error('CHANNEL_PRIVATE');
      },
      async getMessages() {
        return [];
      },
    };
    const engine = createTelegramCallerFeedEngine({ statePath, client: mockClient });
    const poll = await engine.pollOnce();
    const unavailable = poll.sources.filter(s => !s.available);
    assert.ok(unavailable.length >= 1);
    assert.equal(poll.emitted.length, 0);
    const health = engine.getHealth({ connected: poll.connected });
    assert.equal(health.sources.length, poll.sources.length);
    assert.ok(health.sources.some(s => !s.available && s.reason === 'CHANNEL_PRIVATE'));
  });

  it('never resolves guessed sources when no source list is configured', async () => {
    process.env.TELEGRAM_API_ID = '1';
    process.env.TELEGRAM_API_HASH = 'hash';
    process.env.TELEGRAM_SESSION_STRING = 'session';
    let resolutions = 0;
    const engine = createTelegramCallerFeedEngine({ statePath, client: {
      async getEntity() { resolutions += 1; throw new Error('unexpected'); },
    } });
    const poll = await engine.pollOnce();
    assert.equal(resolutions, 0);
    assert.equal(poll.connected, false);
    assert.deepEqual(poll.sources, []);
  });

  it('does not report connected when all configured sources fail', async () => {
    process.env.TELEGRAM_API_ID = '1';
    process.env.TELEGRAM_API_HASH = 'hash';
    process.env.TELEGRAM_SESSION_STRING = 'session';
    singleSourceEnv();
    const engine = createTelegramCallerFeedEngine({ statePath, client: {
      async getEntity() { throw new Error('CHANNEL_PRIVATE'); },
    } });
    const poll = await engine.pollOnce();
    assert.equal(poll.connected, false);
    assert.equal(engine.getHealth(poll).sources[0].available, false);
  });

  it('revokes availability when message read fails after entity resolution', async () => {
    process.env.TELEGRAM_API_ID = '1';
    process.env.TELEGRAM_API_HASH = 'hash';
    process.env.TELEGRAM_SESSION_STRING = 'session';
    singleSourceEnv();
    let fail = false;
    const engine = createTelegramCallerFeedEngine({ statePath, client: {
      async getEntity() { return { id: '123', username: 'frontrunz' }; },
      async getMessages() { if (fail) throw new Error('CHANNEL_PRIVATE'); return []; },
    } });
    assert.equal((await engine.pollOnce()).connected, true);
    fail = true;
    const poll = await engine.pollOnce();
    assert.equal(poll.connected, false);
    const health = engine.getHealth(poll);
    assert.deepEqual(health.activeChannels, []);
    assert.equal(health.sources[0].reason, 'CHANNEL_PRIVATE');
  });

  it('feed schema compatible with LiveCallerCollector and rejects procedural rows', async () => {
    const fetchFn = async (url, options = {}) => {
      if (url.includes('/health')) {
        return {
          ok: true,
          json: async () => ({ connected: true, credentialsConfigured: true, sources: [] }),
        };
      }
      assert.equal(options.headers?.authorization, 'Bearer OPERATIONAL-TEST-ONLY-AUTH-FIXTURE-000');
      return {
        ok: true,
        json: async () => ({
          calls: [
            {
              sourceId: 'telegram-front-runners',
              externalId: 'telegram:-100:42',
              occurredAt: '2026-10-06T03:00:00.000Z',
              ingestedAt: '2026-10-06T03:00:01.000Z',
              text: '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump',
            },
            {
              sourceId: 'bad',
              externalId: 'proc-1',
              occurredAt: '2026-10-06T03:00:00.000Z',
              text: 'x',
              provenance: { dataClass: 'PROCEDURAL' },
            },
          ],
        }),
      };
    };
    const collector = createOperatorJsonFeedCollector({
      feedUrl: 'http://feed.test/feed',
      feedToken: 'OPERATIONAL-TEST-ONLY-AUTH-FIXTURE-000',
      fetchFn,
    });
    const rows = await collector.poll();
    assert.equal(rows.length, 1);
    assert.equal(rows[0].externalMessageId, 'telegram:-100:42');
    assert.equal(rows[0].provenance.dataClass, 'EMPIRICAL');
  });

  it('UNKNOWN source relationship stays UNKNOWN in registry sync', async () => {
    const store = new InMemorySignalStore();
    const shadow = new ShadowModeService(store, { collectors: [] });
    await shadow.syncSourceRegistryFromFeedHealth({
      feedHealth: {
        sources: [
          {
            sourceId: 'telegram-front-runners',
            displayName: 'Front Runners',
            username: 'frontrunz',
            channelId: '-1001',
            available: true,
            active: true,
            relationshipStatus: CLUSTER_RELATIONSHIP.UNKNOWN,
            collector: 'telegram-caller-feed',
          },
        ],
      },
    });
    const row = shadow.registryBySourceId().get('telegram-front-runners');
    assert.equal(row.clusterRelationshipStatus, CLUSTER_RELATIONSHIP.UNKNOWN);
  });

  it('prospective cohort does not start until caller feed is connected', async () => {
    const store = new InMemorySignalStore();
    const collector = createOperatorJsonFeedCollector({
      feedUrl: 'http://feed.test/feed',
      fetchFn: async url => ({
        ok: true,
        json: async () =>
          url.includes('/health')
            ? { connected: false, credentialsConfigured: true }
            : { calls: [] },
      }),
    });
    const shadow = new ShadowModeService(store, { collectors: [collector] });
    await shadow.pollCollectorsOnce();
    assert.equal(store.researchCohorts.has(PROSPECTIVE_COHORT_001_ID), false);
  });

  it('startup smoke: telegram caller feed server module loads', () => {
    assert.doesNotThrow(() => {
      require('../../../services/telegramCallerFeed/server');
    });
  });

  it('declares and loads the MTProto production runtime', () => {
    const manifest = require('../../../package.json');
    assert.ok(manifest.dependencies.telegram);
    assert.equal(typeof require('telegram').TelegramClient, 'function');
    assert.equal(typeof require('telegram/sessions').StringSession, 'function');
    assert.equal(typeof require('telegram/extensions/Logger').Logger, 'function');
  });
});
