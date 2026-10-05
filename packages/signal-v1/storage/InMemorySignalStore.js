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
    this.clusterRelationships = new Map();
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
    this.researchObservations = [];
    this.researchObservationOutcomes = [];
    this.researchCohorts = new Map();
    this.researchCohortMembers = [];
    this.researchCandidates = new Map();
    this.walletPerformance = new Map();
    this.marketObservations = [];
    this.marketIngestionStats = [];
    this.rawCallerEvidence = [];
    this.prospectiveJobs = [];
    this.sourceRegistry = new Map();
    this.collectorHealth = {};
    this.tokenResearchEpisodes = new Map();
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

  insertMarketObservation(observation) {
    const occurredAt = toDate(observation.occurredAt);
    const key = `${observation.tokenAddress}|${observation.provider}|${occurredAt.toISOString()}|${observation.intervalSeconds}`;
    const existing = this.marketObservations.find(
      o =>
        `${o.tokenAddress}|${o.provider}|${o.occurredAt.toISOString()}|${o.intervalSeconds}` ===
        key
    );
    if (existing) return { row: existing, duplicate: true };

    const row = {
      id: observation.id || randomUUID(),
      tokenAddress: observation.tokenAddress,
      occurredAt,
      priceUsd: Number(observation.priceUsd),
      marketCapUsd: observation.marketCapUsd ?? null,
      liquidityUsd: observation.liquidityUsd ?? null,
      volumeIntervalUsd: observation.volumeIntervalUsd ?? null,
      intervalSeconds: observation.intervalSeconds,
      provider: observation.provider,
      externalId: observation.externalId ?? null,
      providerTimestamp: observation.providerTimestamp
        ? toDate(observation.providerTimestamp)
        : occurredAt,
      observedTimestamp: observation.observedTimestamp
        ? toDate(observation.observedTimestamp)
        : new Date(),
      ingestedAt: new Date(),
      provenance: observation.provenance || {},
    };
    this.marketObservations.push(row);
    return { row, duplicate: false };
  }

  getMarketObservationsForToken(tokenAddress, { startTime, endTime, maxOccurredAt } = {}) {
    let list = this.marketObservations.filter(o => o.tokenAddress === tokenAddress);
    if (startTime) {
      const start = toDate(startTime).getTime();
      list = list.filter(o => o.occurredAt.getTime() >= start);
    }
    if (endTime) {
      const end = toDate(endTime).getTime();
      list = list.filter(o => o.occurredAt.getTime() <= end);
    }
    if (maxOccurredAt) {
      const max = toDate(maxOccurredAt).getTime();
      list = list.filter(o => o.occurredAt.getTime() <= max);
    }
    return list.sort((a, b) => a.occurredAt - b.occurredAt || a.id.localeCompare(b.id));
  }

  insertMarketIngestionStats(stats) {
    this.marketIngestionStats.push({ ...stats });
    return stats;
  }

  getOutcomesForToken(tokenAddress) {
    return this.outcomes
      .filter(o => o.tokenAddress === tokenAddress)
      .sort((a, b) => a.observedAt - b.observedAt);
  }

  getLatestMarketIngestionStats(tokenAddress) {
    const rows = this.marketIngestionStats.filter(s => s.tokenAddress === tokenAddress);
    rows.sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0));
    return rows[0] || null;
  }

  getWalletPerformance(walletAddress, asOf) {
    const asOfMs = toDate(asOf).getTime();
    const records = [...this.walletPerformance.values()].filter(
      r => r.walletAddress === walletAddress && toDate(r.asOf).getTime() <= asOfMs
    );
    records.sort((a, b) => toDate(b.asOf) - toDate(a.asOf));
    return records[0] || null;
  }

  upsertWalletPerformance(record) {
    const key = `${record.walletAddress}:${toDate(record.asOf).toISOString()}`;
    this.walletPerformance.set(key, { ...record, asOf: toDate(record.asOf) });
    return this.walletPerformance.get(key);
  }

  getResearchObservations(tokenAddress, definitionVersion) {
    return this.researchObservations
      .filter(
        o =>
          o.tokenAddress === tokenAddress &&
          (!definitionVersion || o.definitionVersion === definitionVersion)
      )
      .sort((a, b) => a.occurredAt - b.occurredAt);
  }

  insertResearchObservation(observation) {
    const key = `${observation.tokenAddress}:${observation.observationType}:${observation.definitionVersion}`;
    const existing = this.researchObservations.find(
      o =>
        o.tokenAddress === observation.tokenAddress &&
        o.observationType === observation.observationType &&
        o.definitionVersion === observation.definitionVersion
    );
    if (existing) return existing;

    const row = {
      id: observation.id || randomUUID(),
      ...observation,
      occurredAt: toDate(observation.occurredAt),
    };
    this.researchObservations.push(row);
    return row;
  }

  getResearchObservationOutcomes(observationId) {
    return this.researchObservationOutcomes.filter(o => o.observationId === observationId);
  }

  insertResearchObservationOutcome(outcome) {
    const existing = this.researchObservationOutcomes.find(
      o =>
        o.observationId === outcome.observationId &&
        o.executionDelaySeconds === outcome.executionDelaySeconds
    );
    if (existing) return existing;

    const row = {
      id: outcome.id || randomUUID(),
      ...outcome,
    };
    this.researchObservationOutcomes.push(row);
    return row;
  }

  upsertResearchCohort(cohort) {
    this.researchCohorts.set(cohort.id, {
      ...this.researchCohorts.get(cohort.id),
      ...cohort,
      frozenAt: cohort.frozenAt ? toDate(cohort.frozenAt) : this.researchCohorts.get(cohort.id)?.frozenAt || null,
      selectionVersion: cohort.selectionVersion || this.researchCohorts.get(cohort.id)?.selectionVersion || null,
      createdAt: toDate(cohort.createdAt || new Date()),
    });
    return this.researchCohorts.get(cohort.id);
  }

  upsertResearchCandidate(candidate) {
    this.researchCandidates.set(candidate.id, {
      ...candidate,
      earliestKnownCallAt: candidate.earliestKnownCallAt
        ? toDate(candidate.earliestKnownCallAt)
        : null,
      updatedAt: new Date(),
      createdAt: this.researchCandidates.get(candidate.id)?.createdAt || new Date(),
    });
    return this.researchCandidates.get(candidate.id);
  }

  updateResearchCandidateStatus(id, status, exclusionReason = null) {
    const row = this.researchCandidates.get(id);
    if (!row) return null;
    row.status = status;
    if (exclusionReason != null) row.exclusionReason = exclusionReason;
    row.updatedAt = new Date();
    this.researchCandidates.set(id, row);
    return row;
  }

  listResearchCandidates({ status, tokenAddress } = {}) {
    let list = [...this.researchCandidates.values()];
    if (status) list = list.filter(c => c.status === status);
    if (tokenAddress) list = list.filter(c => c.tokenAddress === tokenAddress);
    return list.sort((a, b) => a.tokenAddress.localeCompare(b.tokenAddress));
  }

  addCohortMember(member) {
    const cohort = this.researchCohorts.get(member.cohortId);
    if (cohort?.frozenAt) {
      throw new Error(
        `Cohort ${member.cohortId} is frozen at ${cohort.frozenAt.toISOString()}; create a new cohort version to mutate membership`
      );
    }
    const existing = this.researchCohortMembers.find(
      m => m.cohortId === member.cohortId && m.tokenAddress === member.tokenAddress
    );
    if (existing) return existing;
    const row = {
      ...member,
      createdAt: toDate(member.createdAt || new Date()),
    };
    this.researchCohortMembers.push(row);
    return row;
  }

  getCohortMembers(cohortId) {
    return this.researchCohortMembers.filter(m => m.cohortId === cohortId);
  }

  listResearchCohorts() {
    return [...this.researchCohorts.values()].sort(
      (a, b) => toDate(b.createdAt) - toDate(a.createdAt)
    );
  }

  insertRawCallerEvidence(row) {
    const key = `${row.sourceId}|${row.externalMessageId}|${row.extractedCa || 'none'}`;
    const existing = this.rawCallerEvidence.find(
      r => `${r.sourceId}|${r.externalMessageId}|${r.extractedCa || 'none'}` === key
    );
    if (existing) return { row: existing, duplicate: true };
    const stored = {
      id: row.id || randomUUID(),
      ...row,
      occurredAt: toDate(row.occurredAt),
      ingestedAt: toDate(row.ingestedAt || new Date()),
    };
    this.rawCallerEvidence.push(stored);
    return { row: stored, duplicate: false };
  }

  insertProspectiveJob(job) {
    const existing = this.prospectiveJobs.find(j => j.id === job.id);
    if (existing) return existing;
    const row = {
      ...job,
      runAfter: toDate(job.runAfter),
      createdAt: toDate(job.createdAt || new Date()),
      updatedAt: toDate(job.updatedAt || new Date()),
      completedAt: job.completedAt ? toDate(job.completedAt) : null,
    };
    this.prospectiveJobs.push(row);
    return row;
  }

  loadProspectiveJobs() {
    return [...this.prospectiveJobs];
  }

  updateProspectiveJob(id, patch) {
    const job = this.prospectiveJobs.find(j => j.id === id);
    if (!job) return null;
    Object.assign(job, patch);
    job.updatedAt = new Date();
    return job;
  }

  upsertSourceRegistryEntry(entry) {
    this.sourceRegistry.set(entry.sourceId, { ...entry, updatedAt: toDate(entry.updatedAt || new Date()) });
    return this.sourceRegistry.get(entry.sourceId);
  }

  listSourceRegistryEntries() {
    return [...this.sourceRegistry.values()];
  }
}

function toDate(value) {
  if (value instanceof Date) return value;
  return new Date(value);
}

module.exports = {
  InMemorySignalStore,
};
