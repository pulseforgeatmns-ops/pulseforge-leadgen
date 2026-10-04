'use strict';

const { randomUUID } = require('crypto');

/**
 * In-memory store for Signal V1 — used in tests and fixture-driven replay.
 */
class InMemorySignalStore {
  constructor() {
    this.tokens = new Map();
    this.sources = new Map();
    this.clusters = new Map();
    this.clusterMembers = new Map();
    this.events = [];
    this.snapshots = [];
    this.sourcePerformance = new Map();
    this.decisions = [];
    this.outcomes = [];
    this.paperPositions = [];
    this.paperTransactions = [];
    this.replayRuns = [];
    this.alerts = [];
  }

  upsertToken(token) {
    const key = token.tokenAddress;
    const existing = this.tokens.get(key) || {};
    this.tokens.set(key, { ...existing, ...token });
    return this.tokens.get(key);
  }

  upsertSource(source) {
    this.sources.set(source.id, { ...this.sources.get(source.id), ...source });
    return this.sources.get(source.id);
  }

  upsertCluster(cluster) {
    this.clusters.set(cluster.id, { ...this.clusters.get(cluster.id), ...cluster });
    return this.clusters.get(cluster.id);
  }

  addClusterMember(sourceId, clusterId) {
    this.clusterMembers.set(sourceId, clusterId);
  }

  getClusterIdForSource(sourceId) {
    return this.clusterMembers.get(sourceId) || null;
  }

  insertEvent(event) {
    const row = {
      id: event.id || randomUUID(),
      ...event,
      occurredAt: toDate(event.occurredAt),
      observedAt: toDate(event.observedAt || event.occurredAt),
      ingestedAt: toDate(event.ingestedAt || new Date()),
    };
    this.events.push(row);
    return row;
  }

  insertEvents(events) {
    return events.map(e => this.insertEvent(e));
  }

  getEventsForToken(tokenAddress, { startTime, endTime, maxOccurredAt } = {}) {
    let list = this.events.filter(e => e.tokenAddress === tokenAddress);
    if (startTime) {
      const start = toDate(startTime).getTime();
      list = list.filter(e => e.occurredAt.getTime() >= start);
    }
    if (endTime) {
      const end = toDate(endTime).getTime();
      list = list.filter(e => e.occurredAt.getTime() <= end);
    }
    if (maxOccurredAt) {
      const max = toDate(maxOccurredAt).getTime();
      list = list.filter(e => e.occurredAt.getTime() <= max);
    }
    return list.sort((a, b) => a.occurredAt - b.occurredAt || a.id.localeCompare(b.id));
  }

  getSourcePerformance(sourceId, asOf) {
    const asOfMs = toDate(asOf).getTime();
    const records = [...this.sourcePerformance.values()].filter(
      r => r.sourceId === sourceId && toDate(r.asOf).getTime() <= asOfMs
    );
    records.sort((a, b) => toDate(b.asOf) - toDate(a.asOf));
    return records[0] || null;
  }

  upsertSourcePerformance(record) {
    const key = `${record.sourceId}:${toDate(record.asOf).toISOString()}`;
    this.sourcePerformance.set(key, { ...record, asOf: toDate(record.asOf) });
    return this.sourcePerformance.get(key);
  }

  insertSnapshot(snapshot) {
    const row = {
      id: snapshot.id || randomUUID(),
      ...snapshot,
      evaluatedAt: toDate(snapshot.evaluatedAt),
    };
    this.snapshots.push(row);
    return row;
  }

  insertDecision(decision) {
    const row = {
      id: decision.id || randomUUID(),
      ...decision,
      decidedAt: toDate(decision.decidedAt || new Date()),
    };
    this.decisions.push(row);
    return row;
  }

  insertAlert(alert) {
    const row = {
      id: alert.id || randomUUID(),
      ...alert,
      createdAt: toDate(alert.createdAt || new Date()),
    };
    this.alerts.push(row);
    return row;
  }

  insertPaperPosition(position) {
    const row = {
      id: position.id || randomUUID(),
      ...position,
      openedAt: toDate(position.openedAt),
    };
    this.paperPositions.push(row);
    return row;
  }

  insertPaperTransaction(tx) {
    const row = {
      id: tx.id || randomUUID(),
      ...tx,
      executedAt: toDate(tx.executedAt),
    };
    this.paperTransactions.push(row);
    return row;
  }

  insertReplayRun(run) {
    const row = {
      id: run.id || randomUUID(),
      ...run,
      startedAt: toDate(run.startedAt || new Date()),
      completedAt: run.completedAt ? toDate(run.completedAt) : null,
    };
    this.replayRuns.push(row);
    return row;
  }

  insertOutcome(outcome) {
    const row = {
      id: outcome.id || randomUUID(),
      ...outcome,
      observedAt: toDate(outcome.observedAt),
    };
    this.outcomes.push(row);
    return row;
  }
}

function toDate(value) {
  if (value instanceof Date) return value;
  return new Date(value);
}

module.exports = {
  InMemorySignalStore,
};
