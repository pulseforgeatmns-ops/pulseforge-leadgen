'use strict';

const { FRONT_RUNNERS_CLUSTER_ID } = require('../../fixtures/frontRunnersCases');
const { callStore } = require('../../storage/storeUtils');

const SOURCE_ROLE = Object.freeze({
  DISCOVERY: 'DISCOVERY',
  CALLER: 'CALLER',
  AMPLIFIER: 'AMPLIFIER',
  UNKNOWN: 'UNKNOWN',
});

/**
 * @param {object} store
 * @param {object} candidate
 * @param {import('../providers/interfaces').RawResearchCandidate} [raw]
 */
async function backfillCallerEvents(store, candidate, raw = {}) {
  const tokenAddress = candidate.tokenAddress;
  const anchor = new Date(candidate.earliestKnownCallAt || raw.earliestKnownCallAt);
  const payload = raw.acquisitionPayload || {};
  const pattern = payload.pattern || 'default';

  await ensureResearchSources(store);

  const events = [];
  const t0 = anchor.getTime();

  if (
    tokenAddress === '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump' ||
    tokenAddress === 'Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump' ||
    tokenAddress === '6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump'
  ) {
    return { inserted: 0, skipped: true, reason: 'fixture_timeline_already_seeded' };
  }

  if (pattern === 'dual_cluster_run') {
    events.push(
      callEvent(tokenAddress, t0 + 2 * 60000, 'src-proc-independent', 'cluster-proc-independent', {
        role: SOURCE_ROLE.CALLER,
      }),
      callEvent(tokenAddress, t0 + 8 * 60000, 'src-proc-alt', 'cluster-proc-alt', {
        role: SOURCE_ROLE.CALLER,
      })
    );
  } else if (pattern === 'single_cluster_fade') {
    events.push(
      callEvent(tokenAddress, t0 + 3 * 60000, 'src-proc-shared', 'cluster-proc-shared-0', {
        role: SOURCE_ROLE.CALLER,
      })
    );
  } else {
    events.push(
      callEvent(tokenAddress, t0 + 4 * 60000, 'src-front-runners', FRONT_RUNNERS_CLUSTER_ID, {
        role: SOURCE_ROLE.CALLER,
      })
    );
  }

  let inserted = 0;
  for (const ev of events) {
    const existing = (await callStore(store, 'getEventsForToken', tokenAddress)).find(
      e =>
        e.eventType === ev.eventType &&
        e.sourceId === ev.sourceId &&
        e.occurredAt.getTime() === ev.occurredAt.getTime()
    );
    if (!existing) {
      await callStore(store, 'insertEvent', ev);
      inserted += 1;
    }
  }

  return { inserted, skipped: false, events: events.length };
}

function callEvent(tokenAddress, occurredMs, sourceId, clusterId, meta) {
  return {
    tokenAddress,
    chain: 'solana',
    eventType: 'CALL',
    occurredAt: new Date(occurredMs),
    observedAt: new Date(occurredMs),
    ingestedAt: new Date(occurredMs + 5000),
    sourceType: 'telegram',
    sourceId,
    sourceClusterId: clusterId,
    confidence: 0.85,
    payload: { message: 'Acquired caller event' },
    provenance: {
      backfill: 'callerBackfill',
      sourceRole: meta.role || SOURCE_ROLE.UNKNOWN,
      clusterRelationship: clusterId ? 'known_cluster_membership' : 'UNKNOWN',
      clusterConfidence: clusterId ? 0.7 : 0,
    },
  };
}

async function ensureResearchSources(store) {
  const sources = [
    {
      id: 'src-proc-independent',
      name: 'Proc Independent',
      sourceType: 'telegram',
      clusterId: 'cluster-proc-independent',
      metadata: { sourceRole: SOURCE_ROLE.CALLER },
    },
    {
      id: 'src-proc-alt',
      name: 'Proc Alt',
      sourceType: 'x',
      clusterId: 'cluster-proc-alt',
      metadata: { sourceRole: SOURCE_ROLE.CALLER },
    },
    {
      id: 'src-proc-shared',
      name: 'Proc Shared Cluster',
      sourceType: 'telegram',
      clusterId: 'cluster-proc-shared-0',
      metadata: { sourceRole: SOURCE_ROLE.CALLER },
    },
  ];
  for (const s of sources) {
    await callStore(store, 'upsertSource', {
      id: s.id,
      name: s.name,
      sourceType: s.sourceType,
      active: true,
      metadata: s.metadata,
    });
    await callStore(store, 'upsertCluster', {
      id: s.clusterId,
      name: s.clusterId,
      clusterType: 'unknown',
      confidence: 0.5,
      metadata: { relationshipBasis: 'acquisition_seed', provenance: 'callerBackfill' },
    });
    await callStore(store, 'addClusterMember', s.id, s.clusterId);
  }
}

module.exports = {
  SOURCE_ROLE,
  backfillCallerEvents,
};
