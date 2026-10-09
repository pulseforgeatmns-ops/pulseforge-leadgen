'use strict';

const { createHash } = require('crypto');
const { callStore } = require('../storage/storeUtils');
const { RESEARCH_DEFINITION_VERSION } = require('../types');
const {
  PROSPECTIVE_COHORT_001_ID,
  PROSPECTIVE_TOKEN_EPISODE_COOLDOWN_HOURS,
  PROSPECTIVE_RESEARCH_DEFINITION_VERSION,
  PROSPECTIVE_PARSER_VERSION,
  DATA_CLASS,
  JOB_STATUS,
} = require('./constants');
const {
  buildProspectiveCohortRecord,
  eventEligibleForProspectiveCohort,
  unblindCohort,
} = require('./cohort');
const { knowledgeAt, ingestionLatencyMs } = require('./knowledgeClock');
const {
  extractSolanaContractAddresses,
  deterministicCallEventId,
  deterministicEvidenceId,
} = require('./caExtraction');
const {
  evaluateProspectiveFirstCaller,
  evaluateProspectiveIndependentConvergence,
} = require('./prospectiveTriggers');
const { captureMarketSnapshot } = require('./marketCapture');
const {
  scheduleDelayCaptureJobs,
  scheduleOutcomeJob,
  buildPricePathFromObservations,
  processDelayCaptureJob,
  processOutcomeJob,
} = require('./outcomeWatch');
const { computeProspectiveDataQuality } = require('./dataQuality');
const { cohortProgress, redactEvaluationIfBlinded, isCohortBlinded } = require('./blinding');
const { assertEmpiricalCohort } = require('./empiricalGuard');
const { evaluateCohortLayers } = require('../research/cohortEvaluation');
const { createProductionCollectors } = require('../collectors/createCollectors');

class ShadowModeService {
  /**
   * @param {object} store
   * @param {{ marketProvider?: object, collectors?: object[], now?: () => Date }} [options]
   */
  constructor(store, options = {}) {
    this.store = store;
    this.marketProvider = options.marketProvider;
    this.collectors = options.collectors || createProductionCollectors(options);
    this.requiredCallerSource = options.requiredCallerSource || null;
    this.now = options.now || (() => new Date());
    this.inFlightJobs = new Set();
    this.ensureProspectiveStructures();
  }

  ensureProspectiveStructures() {
    if (!this.store.rawCallerEvidence) this.store.rawCallerEvidence = [];
    if (!this.store.prospectiveJobs) this.store.prospectiveJobs = [];
    if (!this.store.sourceRegistry) this.store.sourceRegistry = new Map();
    if (!this.store.collectorHealth) this.store.collectorHealth = {};
    if (!this.store.tokenResearchEpisodes) this.store.tokenResearchEpisodes = new Map();
  }

  async ensureProspectiveCohortStarted(providerVersions = {}) {
    let cohort = this.store.researchCohorts?.get?.(PROSPECTIVE_COHORT_001_ID);
    if (!cohort) {
      const { startedAt, ...versions } = providerVersions;
      cohort = buildProspectiveCohortRecord({
        startedAt: startedAt || this.now(),
        providerVersions: versions,
      });
      await callStore(this.store, 'upsertResearchCohort', cohort);
    }
    return cohort;
  }

  async getProductionCallerFeedHealth() {
    for (const collector of this.collectors) {
      if (collector.id !== 'operator-json-feed' || !collector.health) continue;
      const health = await collector.health();
      this.store.collectorHealth[collector.id] = health;
      if (health.available && health.connected) return health;
      return health;
    }
    return { available: false, connected: false, reason: 'no_production_caller_collector' };
  }

  async ensureProspectiveCohortWhenCallerFeedReady(providerVersions = {}) {
    const existing = this.store.researchCohorts?.get?.(PROSPECTIVE_COHORT_001_ID);
    if (existing) return existing;
    const feedHealth = await this.getProductionCallerFeedHealth();
    if (!feedHealth?.available || !feedHealth.connected) return null;
    if (this.requiredCallerSource) {
      const required = this.requiredCallerSource;
      const source = feedHealth.feedHealth?.sources?.find(s => s.sourceId === required.sourceId);
      if (!required.channelId || !source?.available || !source.active
        || String(source.channelId) !== String(required.channelId)) return null;
    }
    return this.ensureProspectiveCohortStarted({
      ...providerVersions,
      startedAt: this.now(),
      callerFeedConnectedAt: this.now().toISOString(),
    });
  }

  async resolveProspectiveCohortForIngest() {
    const existing = this.store.researchCohorts?.get?.(PROSPECTIVE_COHORT_001_ID);
    if (existing) return existing;
    return this.ensureProspectiveCohortWhenCallerFeedReady();
  }

  async syncSourceRegistryFromFeedHealth(feedHealth) {
    const sources = feedHealth?.feedHealth?.sources || feedHealth?.sources || [];
    for (const src of sources) {
      if (!src?.sourceId) continue;
      await this.upsertSourceRegistryEntry({
        sourceId: src.sourceId,
        displayName: src.displayName || src.sourceId,
        platform: src.platform || 'telegram',
        externalRef: src.username ? `@${src.username}` : src.channelId || null,
        collectorId: src.collector || 'telegram-caller-feed',
        sourceRole: src.role || 'CALLER',
        clusterRelationshipStatus: src.relationshipStatus || 'UNKNOWN',
        active: src.active !== false && src.available !== false,
        provenance: { channelId: src.channelId || null, unavailableReason: src.reason || null },
      });
    }
  }

  async upsertSourceRegistryEntry(entry) {
    this.ensureProspectiveStructures();
    await callStore(this.store, 'upsertSource', {
      id: entry.sourceId,
      name: entry.displayName,
      sourceType: entry.platform,
      externalHandle: entry.externalRef,
      clusterId: entry.clusterId || null,
      active: entry.active !== false,
      metadata: {
        sourceRole: entry.sourceRole || 'UNKNOWN',
        collectorId: entry.collectorId,
        clusterRelationshipStatus: entry.clusterRelationshipStatus || 'UNKNOWN',
        provenance: entry.provenance || {},
      },
    });
    if (entry.clusterId) {
      await callStore(this.store, 'addClusterMember', entry.sourceId, entry.clusterId);
    }
    const row = {
      ...entry,
      updatedAt: this.now(),
    };
    this.store.sourceRegistry.set(entry.sourceId, row);
    if (this.store.upsertSourceRegistryEntry) {
      return callStore(this.store, 'upsertSourceRegistryEntry', row);
    }
    return row;
  }

  listSourceRegistry() {
    return [...(this.store.sourceRegistry?.values() || [])];
  }

  registryBySourceId() {
    return new Map(this.listSourceRegistry().map(r => [r.sourceId, r]));
  }

  async ingestRawCallerObservation(raw, { collectorId, provider = collectorId }) {
    this.ensureProspectiveStructures();
    if (this.requiredCallerSource && (
      raw.sourceId !== this.requiredCallerSource.sourceId
      || !this.requiredCallerSource.channelId
      || String(raw.provenance?.telegramChannelId) !== String(this.requiredCallerSource.channelId)
      || raw.provenance?.dataClass !== DATA_CLASS.EMPIRICAL
      || raw.provenance?.synthetic || raw.provenance?.testOnly)) {
      return { accepted: false, reason: 'unapproved_or_non_empirical_source' };
    }
    const ingestedAt = raw.ingestedAt ? new Date(raw.ingestedAt) : this.now();
    const occurredAt = new Date(raw.messageTimestamp);
    const receivedAt = this.now();
    if (this.requiredCallerSource && (!Number.isFinite(ingestedAt.getTime())
      || !Number.isFinite(occurredAt.getTime()) || occurredAt > receivedAt || ingestedAt > receivedAt)) {
      return { accepted: false, reason: 'invalid_or_future_evidence_clock' };
    }
    const text = raw.rawText || '';
    const cas = raw.tokenCa
      ? [raw.tokenCa]
      : extractSolanaContractAddresses(text);
    if (!cas.length) {
      const evidenceId = deterministicEvidenceId(raw.sourceId, raw.externalMessageId, 'none');
      const row = {
        id: evidenceId,
        provider,
        collectorId,
        sourceId: raw.sourceId,
        externalMessageId: raw.externalMessageId,
        occurredAt: new Date(raw.messageTimestamp),
        ingestedAt,
        rawReference: raw.rawReferenceUrl || null,
        rawText: text || null,
        extractedCa: null,
        parserVersion: PROSPECTIVE_PARSER_VERSION,
        tokenAddress: null,
        provenance: raw.provenance || {},
      };
      await this.persistRawEvidence(row);
      return { accepted: false, reason: 'no_verified_ca', evidenceId: row.id };
    }

    const results = [];
    for (const ca of cas) {
      const evidenceId = deterministicEvidenceId(raw.sourceId, raw.externalMessageId, ca);
      const evidenceRow = {
        id: evidenceId,
        provider,
        collectorId,
        sourceId: raw.sourceId,
        externalMessageId: raw.externalMessageId,
        occurredAt: new Date(raw.messageTimestamp),
        ingestedAt,
        rawReference: raw.rawReferenceUrl || null,
        rawText: text || null,
        extractedCa: ca,
        parserVersion: PROSPECTIVE_PARSER_VERSION,
        tokenAddress: ca,
        provenance: {
          ...(raw.provenance || {}),
          forwarding: raw.forwarding || null,
        },
      };
      // Satisfy the evidence token FK before the durable evidence insert.
      await callStore(this.store, 'upsertToken', { tokenAddress: ca, chain: 'solana',
        addressProvenance: 'verified', metadata: { source: 'caller_extraction' } });
      let inserted = await this.persistRawEvidence(evidenceRow);
      if (inserted.duplicate) {
        const editStored = await this.maybePersistEditEvidence(inserted.row, evidenceRow, raw);
        if (editStored) {
          results.push({ accepted: false, reason: 'edit_evidence', tokenAddress: ca, evidenceId: editStored.id });
          continue;
        }
        // Recover an interrupted evidence -> CALL -> jobs sequence on replay.
        Object.assign(evidenceRow, inserted.row);

      }

      const registry = this.store.sourceRegistry.get(raw.sourceId);
      const eventId = deterministicCallEventId(raw.sourceId, raw.externalMessageId, ca);
      const event = {
        id: eventId,
        tokenAddress: ca,
        chain: 'solana',
        eventType: 'CALL',
        occurredAt: new Date(evidenceRow.occurredAt),
        observedAt: new Date(raw.providerTimestamp || raw.messageTimestamp),
        ingestedAt: new Date(evidenceRow.ingestedAt),
        sourceType: registry?.platform || 'other',
        sourceId: raw.sourceId,
        sourceClusterId: registry?.clusterId || null,
        payload: {
          externalMessageId: raw.externalMessageId,
          ingestionLatencyMs: ingestionLatencyMs(raw.messageTimestamp, ingestedAt),
          forwarding: raw.forwarding || null,
        },
        provenance: {
          ...(evidenceRow.provenance || {}),
          dataClass: evidenceRow.provenance?.dataClass || DATA_CLASS.EMPIRICAL,
          collectorId,
          evidenceId,
          rawReference: raw.rawReferenceUrl || null,
          forwardedFromSourceId: raw.forwarding?.sourceId || null,
          forwardedFromClusterId: raw.forwarding?.clusterId || null,
        },
        confidence: 1,
      };

      const existing = this.store.events.find(e => e.id === eventId);
      if (existing) {
        await this.processTokenAfterCall(existing);
        results.push({ accepted: false, reason: 'duplicate_event', tokenAddress: ca });
        continue;
      }

      await callStore(this.store, 'upsertToken', {
        tokenAddress: ca,
        chain: 'solana',
        addressProvenance: 'verified',
        metadata: { source: 'caller_extraction' },
      });
      await callStore(this.store, 'insertEvent', event);
      await this.processTokenAfterCall(event);
      results.push({ accepted: true, tokenAddress: ca, eventId });
    }
    return { results };
  }

  async persistRawEvidence(row) {
    const existing = this.store.rawCallerEvidence.find(r => r.id === row.id);
    if (existing) return { row: existing, duplicate: true };
    if (this.store.insertRawCallerEvidence) {
      return callStore(this.store, 'insertRawCallerEvidence', row);
    }
    this.store.rawCallerEvidence.push(row);
    return { row, duplicate: false };
  }

  async maybePersistEditEvidence(existingRow, proposedRow, raw) {
    const editAt = raw.provenance?.messageEditAt;
    if (!editAt || !existingRow) return null;
    if ((existingRow.rawText || '') === (proposedRow.rawText || '')) return null;
    const editId = `${proposedRow.id}:edit:${editAt}`;
    if (this.store.rawCallerEvidence.some(r => r.id === editId)) return null;
    const editRow = {
      ...proposedRow,
      id: editId,
      externalMessageId: `${proposedRow.externalMessageId}:edit:${editAt}`,
      ingestedAt: this.now(),
      occurredAt: existingRow.occurredAt,
      provenance: {
        ...(proposedRow.provenance || {}),
        evidenceKind: 'MESSAGE_EDIT',
        originalEvidenceId: existingRow.id,
        priorText: raw.provenance?.priorText || existingRow.rawText || null,
        messageEditAt: editAt,
      },
    };
    if (this.store.insertRawCallerEvidence) {
      const res = await callStore(this.store, 'insertRawCallerEvidence', editRow);
      return res.row;
    }
    this.store.rawCallerEvidence.push(editRow);
    return editRow;
  }

  async processTokenAfterCall(event) {
    const cohort = await this.resolveProspectiveCohortForIngest();
    if (!cohort) return;
    const startedAt = cohort.metadata?.startedAt;
    if (!eventEligibleForProspectiveCohort(event, startedAt)) return;

    if (event.provenance?.dataClass === DATA_CLASS.PROCEDURAL) return;

    const knowAt = knowledgeAt(event.occurredAt, event.ingestedAt);
    await this.maybeCreateResearchObservation('FIRST_CALLER', event.tokenAddress, knowAt);
    await this.maybeCreateResearchObservation('INDEPENDENT_CONVERGENCE', event.tokenAddress, knowAt);
  }

  inEpisodeCooldown(tokenAddress, kind) {
    const key = `${tokenAddress}|${kind}`;
    const last = this.store.tokenResearchEpisodes.get(key);
    if (!last) return false;
    const cooldownMs = PROSPECTIVE_TOKEN_EPISODE_COOLDOWN_HOURS * 60 * 60 * 1000;
    return this.now().getTime() - new Date(last.knowledgeAt).getTime() < cooldownMs;
  }

  async markEpisode(tokenAddress, kind, knowledgeAtIso, observationId) {
    const key = `${tokenAddress}|${kind}`;
    const row = {
      tokenAddress,
      episodeKind: kind,
      knowledgeAt: knowledgeAtIso,
      observationId,
    };
    if (this.store.persistEpisode) await this.store.persistEpisode(row);
    this.store.tokenResearchEpisodes.set(key, row);
  }

  async maybeCreateResearchObservation(type, tokenAddress, asOfKnowledge) {
    const defVersion = PROSPECTIVE_RESEARCH_DEFINITION_VERSION;
    const existing = (this.store.researchObservations || []).find(
      o =>
        o.tokenAddress === tokenAddress &&
        o.observationType === type &&
        o.definitionVersion === defVersion
    );
    if (existing) {
      await this.finalizeObservation(type, tokenAddress, existing);
      return existing;
    }

    if (type === 'FIRST_CALLER' && this.inEpisodeCooldown(tokenAddress, 'FIRST_CALLER')) {
      return null;
    }

    let payload = null;
    if (type === 'FIRST_CALLER') {
      payload = evaluateProspectiveFirstCaller(this.store, tokenAddress, asOfKnowledge);
    } else if (type === 'INDEPENDENT_CONVERGENCE') {
      payload = evaluateProspectiveIndependentConvergence(
        this.store,
        tokenAddress,
        asOfKnowledge,
        this.registryBySourceId()
      );
    }
    if (!payload) return null;

    const obsId = deterministicId(`${tokenAddress}|${type}|${defVersion}|${payload.occurredAt.toISOString()}`);
    let marketMeta = {};
    if (this.marketProvider) {
      const capture = await captureMarketSnapshot(this.marketProvider, tokenAddress, payload.occurredAt, { now: this.now });
      if (capture.ok && capture.snapshot?.priceUsd != null) {
        await callStore(this.store, 'insertMarketObservation', capture.snapshot);
        marketMeta.marketSnapshot = capture.snapshot;
      } else {
        marketMeta.marketCaptureFailed = true;
        marketMeta.marketCaptureError = capture.error || 'unknown';
      }
    }

    const observation = await callStore(this.store, 'insertResearchObservation', {
      id: obsId,
      tokenAddress,
      observationType: type,
      occurredAt: payload.occurredAt,
      featureSnapshotId: null,
      triggerEventIds: payload.triggerEventIds,
      evidenceEventIds: payload.evidenceEventIds,
      definitionVersion: defVersion,
      metadata: {
        ...payload.metadata,
        ...marketMeta,
        mode: 'PROSPECTIVE',
        knowledgeAt: payload.occurredAt.toISOString(),
      },
    });

    await this.finalizeObservation(type, tokenAddress, observation);

    return observation;
  }

  async finalizeObservation(type, tokenAddress, observation) {
    const knowledgeIso = observation.occurredAt.toISOString();
    const delayJobs = scheduleDelayCaptureJobs(observation, knowledgeIso);
    const outcomeJob = scheduleOutcomeJob(observation, knowledgeIso);
    for (const job of [...delayJobs, outcomeJob]) {
      await this.upsertJob(job);
    }

    if (type === 'FIRST_CALLER') {
      await this.markEpisode(tokenAddress, 'FIRST_CALLER', knowledgeIso, observation.id);
      await this.maybeAddCohortMember(tokenAddress, observation);
      await this.insertResearchAlert(type, tokenAddress, observation);
    }
    if (type === 'INDEPENDENT_CONVERGENCE') {
      await this.insertResearchAlert(type, tokenAddress, observation);
    }

  }

  async maybeAddCohortMember(tokenAddress, observation) {
    const cohort = await this.resolveProspectiveCohortForIngest();
    if (!cohort) return null;
    const members = await callStore(this.store, 'getCohortMembers', cohort.id);
    if (members.some(m => m.tokenAddress === tokenAddress)) return null;
    return callStore(this.store, 'addCohortMember', {
      cohortId: cohort.id,
      tokenAddress,
      inclusionReason: 'FIRST_CALLER',
      provenance: {
        observationId: observation.id,
        knowledgeAt: observation.occurredAt.toISOString(),
        naturalMembership: true,
      },
    });
  }

  async insertResearchAlert(type, tokenAddress, observation) {
    const id = deterministicId(`alert|${type}|${tokenAddress}|${observation.id}`);
    if (this.store.alerts?.some(alert => alert.id === id)) return;
    await callStore(this.store, 'insertAlert', {
      id: deterministicId(`alert|${type}|${tokenAddress}|${observation.id}`),
      tokenAddress,
      alertType: 'RESEARCH_SHADOW',
      severity: 'info',
      title: `RESEARCH / PAPER ONLY — ${type}`,
      message: `Prospective empirical observation ${type} for ${tokenAddress.slice(0, 8)}…`,
      metadata: {
        observationId: observation.id,
        paperOnly: true,
        notFinancialAdvice: true,
      },
    });
  }

  async upsertJob(job) {
    const existing = this.store.prospectiveJobs.find(j => j.id === job.id);
    if (existing) return existing;
    if (this.store.insertProspectiveJob) {
      return callStore(this.store, 'insertProspectiveJob', job);
    }
    this.store.prospectiveJobs.push({ ...job, attempts: job.attempts || 0 });
    return job;
  }

  async pollCollectorsOnce() {
    const summary = { ingested: 0, errors: [] };
    for (const collector of this.collectors) {
      try {
        const health = collector.health ? await collector.health() : { available: true };
        this.store.collectorHealth[collector.id] = health;
        if (collector.id === 'operator-json-feed' && health.feedHealth) {
          await this.syncSourceRegistryFromFeedHealth(health);
        }
        await this.ensureProspectiveCohortWhenCallerFeedReady();
        if (!health.available) continue;
        const rows = await collector.poll();
        for (const row of rows) {
          const result = await this.ingestRawCallerObservation(row, {
            collectorId: collector.id,
            provider: collector.id,
          });
          if (result.results) {
            summary.ingested += result.results.filter(r => r.accepted).length;
          } else if (result.accepted) {
            summary.ingested += 1;
          }
        }
      } catch (err) {
        summary.errors.push({ collectorId: collector.id, error: String(err.message || err) });
        this.store.collectorHealth[collector.id] = {
          available: false,
          error: String(err.message || err),
        };
      }
    }
    return summary;
  }

  async runDueJobs(limit = 50, { jobType } = {}) {
    const now = this.now();
    const due = (this.store.prospectiveJobs || [])
      .filter(j => (!jobType || j.jobType === jobType) && !this.inFlightJobs.has(j.id))
      .filter(j => j.status.startsWith('PENDING') && new Date(j.runAfter).getTime() <= now.getTime())
      .slice(0, limit);

    const processed = [];
    for (const job of due) {
      this.inFlightJobs.add(job.id);
      try {
        const result = await this.processJob(job);
        processed.push(result);
      } catch (err) {
        job.attempts = (job.attempts || 0) + 1;
        job.lastError = String(err.message || err);
        job.status = JOB_STATUS.PROVIDER_ERROR;
        await this.persistJobPatch(job);
      } finally {
        this.inFlightJobs.delete(job.id);
      }
    }
    return processed;
  }

  async processJob(job) {
    const observation = this.store.researchObservations.find(o => o.id === job.observationId);
    if (!observation) {
      job.status = JOB_STATUS.DATA_INSUFFICIENT;
      await this.persistJobPatch(job);
      return { jobId: job.id, status: job.status };
    }

    const marketObs = await callStore(this.store, 'getMarketObservationsForToken', job.tokenAddress);
    const pricePath = buildPricePathFromObservations(marketObs, this.now());

    if (job.jobType === 'DELAY_CAPTURE') {
      let path = pricePath;
      let outcomeRow = processDelayCaptureJob(job, observation, path);
      if (outcomeRow.entryPrice == null && (this.marketProvider?.getLiveTokenSnapshot || this.marketProvider?.getTokenSnapshot)) {
        const knowledgeIso = job.payload?.knowledgeAt || observation.occurredAt;
        const targetAt = new Date(
          new Date(knowledgeIso).getTime() + job.targetDelaySeconds * 1000
        );
        const capture = await captureMarketSnapshot(this.marketProvider, job.tokenAddress, targetAt,
          { now: this.now, captureKind: 'delay_poll' });
        if (capture.ok) {
          await callStore(this.store, 'insertMarketObservation', capture.snapshot);
          const refreshed = await callStore(this.store, 'getMarketObservationsForToken', job.tokenAddress);
          path = buildPricePathFromObservations(refreshed, this.now());
          outcomeRow = processDelayCaptureJob(job, observation, path);
        } else {
          outcomeRow.metadata.captureError = capture.error;
        }
      }
      await callStore(this.store, 'insertResearchObservationOutcome', {
        id: deterministicId(`${observation.id}|${outcomeRow.executionDelaySeconds}`),
        observationId: observation.id,
        ...outcomeRow,
      });
      job.status = outcomeRow.jobStatus;
      job.completedAt = this.now();
      await this.persistJobPatch(job);
      return { jobId: job.id, status: job.status };
    }

    if (job.jobType === 'OUTCOME_24H') {
      if (this.marketProvider) {
        const start = observation.occurredAt;
        const end = new Date(Math.min(this.now().getTime(), new Date(start).getTime() + 25 * 60 * 60 * 1000));
        try {
          const live = await this.marketProvider.getHistoricalPrices(job.tokenAddress, start, end, {
            resolutionSeconds: 60,
          });
          for (const point of live) {
            await callStore(this.store, 'insertMarketObservation', {
              ...point,
              provenance: { ...point.provenance, dataClass: point.provenance?.dataClass || DATA_CLASS.EMPIRICAL, backfilled: true },
            });
          }
        } catch {
          /* preserve observation; may backfill later */
        }
      }
      const refreshed = await callStore(this.store, 'getMarketObservationsForToken', job.tokenAddress);
      const refreshedPath = buildPricePathFromObservations(refreshed, this.now());
      const primary = this.store.researchObservationOutcomes.find(
        o =>
          o.observationId === observation.id &&
          o.executionDelaySeconds === 60
      );
      const outcomeResult = processOutcomeJob(observation, refreshedPath, primary);
      for (const row of outcomeResult.outcomes) {
        const existing = this.store.researchObservationOutcomes.find(
          o =>
            o.observationId === observation.id &&
            o.executionDelaySeconds === row.executionDelaySeconds
        );
        if (existing) {
          Object.assign(existing, row, {
            metadata: { ...(existing.metadata || {}), ...(row.metadata || {}) },
          });
          if (this.store.updateResearchObservationOutcome) await this.store.updateResearchObservationOutcome(existing);
        } else {
          await callStore(this.store, 'insertResearchObservationOutcome', {
            id: deterministicId(`${observation.id}|${row.executionDelaySeconds}`),
            observationId: observation.id,
            ...row,
          });
        }
      }
      job.status = outcomeResult.jobStatus;
      job.completedAt = this.now();
      await this.persistJobPatch(job);
      return { jobId: job.id, status: job.status, label: outcomeResult.outcomes[0]?.label };
    }

    return { jobId: job.id, status: job.status };
  }

  async persistJobPatch(job) {
    if (this.store.updateProspectiveJob) {
      await callStore(this.store, 'updateProspectiveJob', job.id, {
        status: job.status,
        attempts: job.attempts,
        lastError: job.lastError,
        completedAt: job.completedAt,
      });
    }
  }

  getShadowDashboard() {
    const cohort = this.store.researchCohorts?.get?.(PROSPECTIVE_COHORT_001_ID);
    const progress = cohort ? cohortProgress(this.store, cohort) : null;
    const quality = computeProspectiveDataQuality(this.store, {
      since: cohort?.metadata?.startedAt,
    });
    return {
      cohortId: PROSPECTIVE_COHORT_001_ID,
      dataClass: DATA_CLASS.EMPIRICAL,
      mode: 'PROSPECTIVE',
      blinded: cohort ? isCohortBlinded(cohort) : true,
      progress,
      dataQuality: quality,
      collectors: this.store.collectorHealth || {},
      primaryPerformanceHidden: true,
    };
  }

  async evaluateProspectiveCohort(delaySeconds = 60, options = {}) {
    const cohort = this.store.researchCohorts?.get?.(PROSPECTIVE_COHORT_001_ID);
    if (!cohort) throw new Error('prospective_cohort_not_started');
    if (!options.skipEmpiricalGuard) {
      assertEmpiricalCohort(this.store, cohort.id);
    }
    const evaluation = evaluateCohortLayers(this.store, cohort.id, delaySeconds, options);
    return redactEvaluationIfBlinded(evaluation, cohort);
  }

  async explicitUnblind({ unblindedBy, evaluationVersion }) {
    const cohort = this.store.researchCohorts?.get?.(PROSPECTIVE_COHORT_001_ID);
    if (!cohort) throw new Error('prospective_cohort_not_started');
    assertEmpiricalCohort(this.store, cohort.id);
    const updated = unblindCohort(cohort, { unblindedBy, evaluationVersion });
    await callStore(this.store, 'upsertResearchCohort', updated);
    return updated;
  }

  restoreJobsFromStore(jobs) {
    this.ensureProspectiveStructures();
    this.store.prospectiveJobs = jobs || [];
  }
}

function deterministicId(seed) {
  return createHash('sha256').update(seed).digest('hex').slice(0, 32);
}

module.exports = {
  ShadowModeService,
};
