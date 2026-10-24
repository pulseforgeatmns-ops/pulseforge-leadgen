'use strict';

const { FRONT_RUNNERS_CLUSTER_ID, RESEARCH_CASES } = require('./frontRunnersCases');
const { seedResearchCohort } = require('./seedResearchCohort');

/**
 * Seed research metadata + a minimal DUPLICATE demo event stream for replay/tests.
 *
 * @param {import('../storage/InMemorySignalStore').InMemorySignalStore} store
 */
function seedFrontRunnersFixtures(store) {
  store.upsertCluster({
    id: FRONT_RUNNERS_CLUSTER_ID,
    name: 'Front Runners research network',
    clusterType: 'promotional_network',
    confidence: 0.55,
    metadata: {
      note: 'Seeded research hypothesis — not immutable truth',
      relatedNames: ['Front Runners', 'Solana Stackers', "Parker's Calls"],
    },
  });

  const sourceIds = ['src-front-runners', 'src-solana-stackers', 'src-parkers-calls'];
  for (const id of sourceIds) {
    store.upsertSource({
      id,
      name: id,
      sourceType: 'telegram',
      clusterId: FRONT_RUNNERS_CLUSTER_ID,
      active: true,
      metadata: { researchSeed: true },
    });
    store.addClusterMember(id, FRONT_RUNNERS_CLUSTER_ID);
  }

  store.upsertSource({
    id: 'src-independent-alpha',
    name: 'Independent Alpha (fixture)',
    sourceType: 'x',
    clusterId: 'cluster-independent-alpha',
    active: true,
  });
  store.upsertCluster({
    id: 'cluster-independent-alpha',
    name: 'Independent Alpha',
    clusterType: 'unknown',
    confidence: 0.4,
  });
  store.addClusterMember('src-independent-alpha', 'cluster-independent-alpha');

  store.upsertSourcePerformance({
    sourceId: 'src-independent-alpha',
    asOf: new Date('2026-09-01T00:00:00Z'),
    sampleSize: 40,
    passRate: 0.42,
    failRate: 0.38,
    medianMfe: 0.85,
    medianMae: -0.22,
    medianTimeTo2xSeconds: 3600,
    score: 0.72,
    version: 'source-performance-v1',
  });

  for (const c of RESEARCH_CASES) {
    if (!c.tokenAddress) continue;
    store.upsertToken({
      tokenAddress: c.tokenAddress,
      chain: 'solana',
      ticker: c.ticker,
      addressProvenance: c.addressProvenance,
      metadata: { telegramClaim: c.telegramClaim, slug: c.slug },
    });
  }

  seedDuplicateDemoTimeline(store);
  seedDoomTimeline(store);
  seedHallowTimeline(store);
  seedResearchCohort(store);
}

function seedDuplicateDemoTimeline(store) {
  const token = '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump';
  const t0 = new Date('2026-09-29T18:00:00Z');

  const ev = (minutes, event) => ({
    tokenAddress: token,
    chain: 'solana',
    occurredAt: new Date(t0.getTime() + minutes * 60000),
    observedAt: new Date(t0.getTime() + minutes * 60000),
    ingestedAt: new Date(t0.getTime() + minutes * 60000 + 5000),
    confidence: 0.9,
    ...event,
  });

  store.insertEvents([
    ev(0, {
      eventType: 'MARKET_SNAPSHOT',
      sourceType: 'market',
      sourceId: null,
      payload: { priceUsd: 0.00012, marketCapUsd: 180000, liquidityUsd: 22000, tokenAgeSeconds: 900 },
    }),
    ev(4, {
      eventType: 'CALL',
      sourceType: 'telegram',
      sourceId: 'src-front-runners',
      sourceClusterId: FRONT_RUNNERS_CLUSTER_ID,
      payload: { message: 'DUPLICATE early call' },
    }),
    ev(7, {
      eventType: 'WALLET_BUY',
      sourceType: 'wallet',
      sourceId: 'wallet-tracked-1',
      walletAddress: 'Wallet1111111111111111111111111111111111111111',
      payload: { profitableWallet: true, sizeUsd: 1200 },
    }),
    ev(9, {
      eventType: 'TOKEN_MENTION',
      sourceType: 'x',
      sourceId: 'src-independent-alpha',
      sourceClusterId: 'cluster-independent-alpha',
      payload: { message: 'Independent mention' },
    }),
    ev(11, {
      eventType: 'MARKET_SNAPSHOT',
      sourceType: 'market',
      payload: {
        priceUsd: 0.00016,
        marketCapUsd: 240000,
        liquidityUsd: 26000,
        uniqueBuyers5m: 42,
        volume5mUsd: 18000,
        priceAcceleration: 1.2,
      },
    }),
    ev(18, {
      eventType: 'DEX_RANK_CHANGE',
      sourceType: 'dex',
      payload: { rank: 88 },
    }),
    ev(25, {
      eventType: 'MARKET_SNAPSHOT',
      sourceType: 'market',
      payload: { priceUsd: 0.00028, marketCapUsd: 420000, liquidityUsd: 31000, uniqueBuyers5m: 65, volume5mUsd: 42000 },
    }),
    // Future relative to early evaluation — must not affect 18:09 snapshot
    ev(40, {
      eventType: 'WHALE_SELL',
      sourceType: 'wallet',
      walletAddress: 'WalletWhale9999999999999999999999999999999999999',
      payload: { profitableWallet: true, distributionType: 'whale', sizeUsd: 9000 },
    }),
    ev(45, {
      eventType: 'MARKET_SNAPSHOT',
      sourceType: 'market',
      payload: { priceUsd: 0.00008, marketCapUsd: 120000, liquidityUsd: 14000 },
    }),
  ]);
}

function seedDoomTimeline(store) {
  const token = 'Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump';
  const t0 = new Date('2026-09-28T14:00:00Z');

  const ev = (minutes, event) => ({
    tokenAddress: token,
    chain: 'solana',
    occurredAt: new Date(t0.getTime() + minutes * 60000),
    observedAt: new Date(t0.getTime() + minutes * 60000),
    ingestedAt: new Date(t0.getTime() + minutes * 60000 + 5000),
    confidence: 0.85,
    ...event,
  });

  store.insertEvents([
    ev(0, {
      eventType: 'MARKET_SNAPSHOT',
      sourceType: 'market',
      payload: { priceUsd: 0.00009, marketCapUsd: 90000, liquidityUsd: 18000, tokenAgeSeconds: 1200 },
    }),
    ev(3, {
      eventType: 'CALL',
      sourceType: 'telegram',
      sourceId: 'src-independent-alpha',
      sourceClusterId: 'cluster-independent-alpha',
      payload: { message: 'DOOM early independent call' },
    }),
    ev(20, {
      eventType: 'MARKET_SNAPSHOT',
      sourceType: 'market',
      payload: { priceUsd: 0.00015, marketCapUsd: 150000, liquidityUsd: 21000 },
    }),
  ]);
}

function seedHallowTimeline(store) {
  const token = '6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump';
  const t0 = new Date('2026-09-27T20:00:00Z');

  const ev = (minutes, event) => ({
    tokenAddress: token,
    chain: 'solana',
    occurredAt: new Date(t0.getTime() + minutes * 60000),
    observedAt: new Date(t0.getTime() + minutes * 60000),
    ingestedAt: new Date(t0.getTime() + minutes * 60000 + 5000),
    confidence: 0.85,
    ...event,
  });

  store.insertEvents([
    ev(0, {
      eventType: 'MARKET_SNAPSHOT',
      sourceType: 'market',
      payload: { priceUsd: 0.00004, marketCapUsd: 40000, liquidityUsd: 12000, tokenAgeSeconds: 800 },
    }),
    ev(6, {
      eventType: 'CALL',
      sourceType: 'telegram',
      sourceId: 'src-parkers-calls',
      sourceClusterId: FRONT_RUNNERS_CLUSTER_ID,
      payload: { message: 'Hallow Inu mention' },
    }),
  ]);
}

module.exports = {
  seedFrontRunnersFixtures,
  seedDuplicateDemoTimeline,
  seedDoomTimeline,
  seedHallowTimeline,
};
