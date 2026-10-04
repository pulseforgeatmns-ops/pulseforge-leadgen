'use strict';

const { randomUUID } = require('crypto');
const { observationDedupeId } = require('../ingestion/ingestHistoricalMarketData');

function toDate(value) {
  if (value instanceof Date) return value;
  return new Date(value);
}

class PostgresSignalStore {
  /**
   * @param {{ query: Function }} pool
   */
  constructor(pool) {
    if (!pool || typeof pool.query !== 'function') {
      throw new Error('PostgresSignalStore requires a pg pool');
    }
    this.pool = pool;
  }

  async upsertToken(token) {
    await this.pool.query(
      `INSERT INTO signal_tokens (token_address, chain, ticker, address_provenance, metadata, updated_at)
       VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
       ON CONFLICT (token_address) DO UPDATE SET
         ticker = EXCLUDED.ticker,
         address_provenance = EXCLUDED.address_provenance,
         metadata = signal_tokens.metadata || EXCLUDED.metadata,
         updated_at = NOW()`,
      [
        token.tokenAddress,
        token.chain || 'solana',
        token.ticker || null,
        token.addressProvenance || 'unknown',
        JSON.stringify(token.metadata || {}),
      ]
    );
    const res = await this.pool.query(`SELECT * FROM signal_tokens WHERE token_address = $1`, [
      token.tokenAddress,
    ]);
    return mapTokenRow(res.rows[0]);
  }

  async getToken(tokenAddress) {
    const res = await this.pool.query(`SELECT * FROM signal_tokens WHERE token_address = $1`, [
      tokenAddress,
    ]);
    return res.rows[0] ? mapTokenRow(res.rows[0]) : null;
  }

  async upsertSource(source) {
    await this.pool.query(
      `INSERT INTO signal_sources (id, name, source_type, external_handle, cluster_id, active, metadata)
       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
       ON CONFLICT (id) DO UPDATE SET
         name = EXCLUDED.name,
         source_type = EXCLUDED.source_type,
         cluster_id = EXCLUDED.cluster_id,
         active = EXCLUDED.active,
         metadata = signal_sources.metadata || EXCLUDED.metadata`,
      [
        source.id,
        source.name,
        source.sourceType,
        source.externalHandle || null,
        source.clusterId || null,
        source.active !== false,
        JSON.stringify(source.metadata || {}),
      ]
    );
    return source;
  }

  async upsertCluster(cluster) {
    await this.pool.query(
      `INSERT INTO signal_source_clusters (id, name, cluster_type, confidence, metadata)
       VALUES ($1, $2, $3, $4, $5::jsonb)
       ON CONFLICT (id) DO UPDATE SET
         name = EXCLUDED.name,
         cluster_type = EXCLUDED.cluster_type,
         confidence = EXCLUDED.confidence,
         metadata = signal_source_clusters.metadata || EXCLUDED.metadata`,
      [
        cluster.id,
        cluster.name,
        cluster.clusterType,
        cluster.confidence ?? 0.5,
        JSON.stringify(cluster.metadata || {}),
      ]
    );
    return cluster;
  }

  async addClusterMember(sourceId, clusterId) {
    await this.pool.query(
      `INSERT INTO signal_source_cluster_members (source_id, cluster_id)
       VALUES ($1, $2) ON CONFLICT DO NOTHING`,
      [sourceId, clusterId]
    );
  }

  async getClusterIdForSource(sourceId) {
    const res = await this.pool.query(
      `SELECT cluster_id FROM signal_source_cluster_members WHERE source_id = $1 LIMIT 1`,
      [sourceId]
    );
    return res.rows[0]?.cluster_id || null;
  }

  async insertEvent(event) {
    const row = {
      id: event.id || randomUUID(),
      tokenAddress: event.tokenAddress,
      chain: event.chain || 'solana',
      eventType: event.eventType,
      occurredAt: toDate(event.occurredAt),
      observedAt: toDate(event.observedAt || event.occurredAt),
      ingestedAt: toDate(event.ingestedAt || new Date()),
      sourceType: event.sourceType,
      sourceId: event.sourceId || null,
      sourceClusterId: event.sourceClusterId || null,
      actorId: event.actorId || null,
      walletAddress: event.walletAddress || null,
      payload: event.payload || {},
      provenance: event.provenance || {},
      confidence: event.confidence ?? 1,
    };
    await this.pool.query(
      `INSERT INTO signal_events (
        id, token_address, chain, event_type, occurred_at, observed_at, ingested_at,
        source_type, source_id, source_cluster_id, actor_id, wallet_address, payload, provenance, confidence
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15)
      ON CONFLICT (id) DO NOTHING`,
      [
        row.id,
        row.tokenAddress,
        row.chain,
        row.eventType,
        row.occurredAt,
        row.observedAt,
        row.ingestedAt,
        row.sourceType,
        row.sourceId,
        row.sourceClusterId,
        row.actorId,
        row.walletAddress,
        JSON.stringify(row.payload),
        JSON.stringify(row.provenance),
        row.confidence,
      ]
    );
    return row;
  }

  async insertEvents(events) {
    const rows = [];
    for (const e of events) rows.push(await this.insertEvent(e));
    return rows;
  }

  async getEventsForToken(tokenAddress, { startTime, endTime, maxOccurredAt } = {}) {
    const params = [tokenAddress];
    let sql = `SELECT * FROM signal_events WHERE token_address = $1`;
    if (startTime) {
      params.push(toDate(startTime));
      sql += ` AND occurred_at >= $${params.length}`;
    }
    if (endTime) {
      params.push(toDate(endTime));
      sql += ` AND occurred_at <= $${params.length}`;
    }
    if (maxOccurredAt) {
      params.push(toDate(maxOccurredAt));
      sql += ` AND occurred_at <= $${params.length}`;
    }
    sql += ` ORDER BY occurred_at ASC, id ASC`;
    const res = await this.pool.query(sql, params);
    return res.rows.map(mapEventRow);
  }

  async getSourcePerformance(sourceId, asOf) {
    const res = await this.pool.query(
      `SELECT * FROM signal_source_performance
       WHERE source_id = $1 AND as_of <= $2
       ORDER BY as_of DESC LIMIT 1`,
      [sourceId, toDate(asOf)]
    );
    const row = res.rows[0];
    if (!row) return null;
    return {
      sourceId: row.source_id,
      asOf: row.as_of,
      sampleSize: row.sample_size,
      passRate: row.pass_rate,
      failRate: row.fail_rate,
      medianMfe: row.median_mfe,
      medianMae: row.median_mae,
      medianTimeTo2xSeconds: row.median_time_to_2x_seconds,
      score: row.score,
      version: row.version,
      metadata: row.metadata,
    };
  }

  async upsertSourcePerformance(record) {
    await this.pool.query(
      `INSERT INTO signal_source_performance (
        id, source_id, as_of, sample_size, pass_rate, fail_rate, median_mfe, median_mae,
        median_time_to_2x_seconds, score, version, metadata
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
      ON CONFLICT (source_id, as_of, version) DO UPDATE SET
        sample_size = EXCLUDED.sample_size,
        pass_rate = EXCLUDED.pass_rate,
        fail_rate = EXCLUDED.fail_rate,
        score = EXCLUDED.score`,
      [
        record.id || randomUUID(),
        record.sourceId,
        toDate(record.asOf),
        record.sampleSize ?? 0,
        record.passRate ?? null,
        record.failRate ?? null,
        record.medianMfe ?? null,
        record.medianMae ?? null,
        record.medianTimeTo2xSeconds ?? null,
        record.score ?? null,
        record.version,
        JSON.stringify(record.metadata || {}),
      ]
    );
    return record;
  }

  async insertSnapshot(snapshot) {
    const row = {
      id: snapshot.id || randomUUID(),
      tokenAddress: snapshot.tokenAddress,
      evaluatedAt: toDate(snapshot.evaluatedAt),
      featureVersion: snapshot.featureVersion,
      features: snapshot.features,
      evidenceEventIds: snapshot.evidenceEventIds || [],
    };
    await this.pool.query(
      `INSERT INTO signal_feature_snapshots (id, token_address, evaluated_at, feature_version, features, evidence_event_ids)
       VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb)
       ON CONFLICT (id) DO NOTHING`,
      [
        row.id,
        row.tokenAddress,
        row.evaluatedAt,
        row.featureVersion,
        JSON.stringify(row.features),
        JSON.stringify(row.evidenceEventIds),
      ]
    );
    return row;
  }

  async insertDecision(decision) {
    const row = {
      id: decision.id || randomUUID(),
      tokenAddress: decision.tokenAddress,
      decidedAt: toDate(decision.decidedAt || new Date()),
      state: decision.state,
      previousState: decision.previousState ?? null,
      score: decision.score,
      featureSnapshotId: decision.featureSnapshotId || null,
      strategyVersion: decision.strategyVersion,
      explanation: decision.explanation || {},
    };
    await this.pool.query(
      `INSERT INTO signal_decisions (
        id, token_address, decided_at, state, previous_state, score, feature_snapshot_id, strategy_version, explanation
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
      ON CONFLICT (id) DO NOTHING`,
      [
        row.id,
        row.tokenAddress,
        row.decidedAt,
        row.state,
        row.previousState,
        row.score,
        row.featureSnapshotId,
        row.strategyVersion,
        JSON.stringify(row.explanation),
      ]
    );
    return row;
  }

  async insertAlert(alert) {
    const row = {
      id: alert.id || randomUUID(),
      tokenAddress: alert.tokenAddress,
      state: alert.state,
      previousState: alert.previousState ?? null,
      score: alert.score,
      reasons: alert.reasons || [],
      risks: alert.risks || [],
      createdAt: toDate(alert.createdAt || new Date()),
    };
    await this.pool.query(
      `INSERT INTO signal_alerts (id, token_address, state, previous_state, score, reasons, risks, created_at)
       VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8)
       ON CONFLICT (id) DO NOTHING`,
      [
        row.id,
        row.tokenAddress,
        row.state,
        row.previousState,
        row.score,
        JSON.stringify(row.reasons),
        JSON.stringify(row.risks),
        row.createdAt,
      ]
    );
    return row;
  }

  async insertPaperPosition(position) {
    const row = {
      id: position.id || randomUUID(),
      tokenAddress: position.tokenAddress,
      openedAt: toDate(position.openedAt),
      entryPrice: position.entryPrice,
      entryMarketCap: position.entryMarketCap ?? null,
      notionalUsd: position.notionalUsd,
      remainingPct: position.remainingPct ?? 100,
      realizedPnlUsd: position.realizedPnlUsd ?? 0,
      unrealizedPnlUsd: position.unrealizedPnlUsd ?? 0,
      status: position.status,
      entrySnapshotId: position.entrySnapshotId || null,
      closedAt: position.closedAt ? toDate(position.closedAt) : null,
      metadata: position.metadata || {},
    };
    await this.pool.query(
      `INSERT INTO signal_paper_positions (
        id, token_address, opened_at, entry_price, entry_market_cap, notional_usd, remaining_pct,
        realized_pnl_usd, unrealized_pnl_usd, status, entry_snapshot_id, closed_at, metadata
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb)
      ON CONFLICT (id) DO NOTHING`,
      [
        row.id,
        row.tokenAddress,
        row.openedAt,
        row.entryPrice,
        row.entryMarketCap,
        row.notionalUsd,
        row.remainingPct,
        row.realizedPnlUsd,
        row.unrealizedPnlUsd,
        row.status,
        row.entrySnapshotId,
        row.closedAt,
        JSON.stringify(row.metadata),
      ]
    );
    return row;
  }

  async insertPaperTransaction(tx) {
    const row = {
      id: tx.id || randomUUID(),
      positionId: tx.positionId,
      executedAt: toDate(tx.executedAt),
      side: tx.side,
      pct: tx.pct,
      price: tx.price,
      notionalUsd: tx.notionalUsd,
      feesUsd: tx.feesUsd ?? 0,
      slippageUsd: tx.slippageUsd ?? 0,
      metadata: tx.metadata || {},
    };
    await this.pool.query(
      `INSERT INTO signal_paper_transactions (
        id, position_id, executed_at, side, pct, price, notional_usd, fees_usd, slippage_usd, metadata
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
      ON CONFLICT (id) DO NOTHING`,
      [
        row.id,
        row.positionId,
        row.executedAt,
        row.side,
        row.pct,
        row.price,
        row.notionalUsd,
        row.feesUsd,
        row.slippageUsd,
        JSON.stringify(row.metadata),
      ]
    );
    return row;
  }

  async insertReplayRun(run) {
    const row = {
      id: run.id || randomUUID(),
      tokenAddress: run.tokenAddress,
      startTime: run.startTime ? toDate(run.startTime) : null,
      endTime: run.endTime ? toDate(run.endTime) : null,
      featureVersion: run.featureVersion,
      strategyVersion: run.strategyVersion,
      startedAt: toDate(run.startedAt || new Date()),
      completedAt: run.completedAt ? toDate(run.completedAt) : null,
      summary: run.summary || {},
    };
    await this.pool.query(
      `INSERT INTO signal_replay_runs (
        id, token_address, start_time, end_time, feature_version, strategy_version, started_at, completed_at, summary
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
      ON CONFLICT (id) DO UPDATE SET completed_at = EXCLUDED.completed_at, summary = EXCLUDED.summary`,
      [
        row.id,
        row.tokenAddress,
        row.startTime,
        row.endTime,
        row.featureVersion,
        row.strategyVersion,
        row.startedAt,
        row.completedAt,
        JSON.stringify(row.summary),
      ]
    );
    return row;
  }

  async insertOutcome(outcome) {
    const row = {
      id: outcome.id || randomUUID(),
      tokenAddress: outcome.tokenAddress,
      observedAt: toDate(outcome.observedAt),
      entryPrice: outcome.entryPrice,
      label: outcome.label,
      mfe: outcome.mfe ?? null,
      mae: outcome.mae ?? null,
      timeTo2xSeconds: outcome.timeTo2xSeconds ?? null,
      timeToMinus30Seconds: outcome.timeToMinus30Seconds ?? null,
      return15m: outcome.return15m ?? null,
      return1h: outcome.return1h ?? null,
      return6h: outcome.return6h ?? null,
      return24h: outcome.return24h ?? null,
      horizonHours: outcome.horizonHours ?? 24,
      metadata: outcome.metadata || {},
      decisionAt: outcome.decisionAt ? toDate(outcome.decisionAt) : null,
      executionDelaySeconds: outcome.executionDelaySeconds ?? null,
      effectiveExecutionAt: outcome.effectiveExecutionAt
        ? toDate(outcome.effectiveExecutionAt)
        : null,
      effectivePrice: outcome.effectivePrice ?? null,
      dataResolutionSeconds: outcome.dataResolutionSeconds ?? null,
      highestPrice: outcome.highestPrice ?? null,
      lowestPrice: outcome.lowestPrice ?? null,
      outcomeTimestamp: outcome.outcomeTimestamp ? toDate(outcome.outcomeTimestamp) : null,
    };

    if (row.decisionAt != null && row.executionDelaySeconds != null) {
      await this.pool.query(
        `INSERT INTO signal_market_outcomes (
          id, token_address, observed_at, entry_price, label, mfe, mae,
          time_to_2x_seconds, time_to_minus_30_seconds, return_15m, return_1h, return_6h, return_24h,
          horizon_hours, metadata, decision_at, execution_delay_seconds, effective_execution_at,
          effective_price, data_resolution_seconds, highest_price, lowest_price, outcome_timestamp
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16,$17,$18,$19,$20,$21,$22,$23
        )
        ON CONFLICT (token_address, decision_at, execution_delay_seconds)
        DO UPDATE SET
          label = EXCLUDED.label,
          mfe = EXCLUDED.mfe,
          mae = EXCLUDED.mae,
          metadata = EXCLUDED.metadata,
          effective_execution_at = EXCLUDED.effective_execution_at,
          effective_price = EXCLUDED.effective_price`,
        [
          row.id,
          row.tokenAddress,
          row.observedAt,
          row.entryPrice,
          row.label,
          row.mfe,
          row.mae,
          row.timeTo2xSeconds,
          row.timeToMinus30Seconds,
          row.return15m,
          row.return1h,
          row.return6h,
          row.return24h,
          row.horizonHours,
          JSON.stringify(row.metadata),
          row.decisionAt,
          row.executionDelaySeconds,
          row.effectiveExecutionAt,
          row.effectivePrice,
          row.dataResolutionSeconds,
          row.highestPrice,
          row.lowestPrice,
          row.outcomeTimestamp,
        ]
      );
    } else {
      await this.pool.query(
        `INSERT INTO signal_market_outcomes (
          id, token_address, observed_at, entry_price, label, mfe, mae,
          time_to_2x_seconds, time_to_minus_30_seconds, return_15m, return_1h, return_6h, return_24h,
          horizon_hours, metadata
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb)
        ON CONFLICT (id) DO NOTHING`,
        [
          row.id,
          row.tokenAddress,
          row.observedAt,
          row.entryPrice,
          row.label,
          row.mfe,
          row.mae,
          row.timeTo2xSeconds,
          row.timeToMinus30Seconds,
          row.return15m,
          row.return1h,
          row.return6h,
          row.return24h,
          row.horizonHours,
          JSON.stringify(row.metadata),
        ]
      );
    }
    return row;
  }

  async insertMarketObservation(observation) {
    const occurredAt = toDate(observation.occurredAt);
    const id = observation.id || observationDedupeId({ ...observation, occurredAt });
    const result = await this.pool.query(
      `INSERT INTO signal_market_observations (
        id, token_address, occurred_at, price_usd, market_cap_usd, liquidity_usd, volume_interval_usd,
        interval_seconds, provider, external_id, provider_timestamp, observed_timestamp, provenance
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb)
      ON CONFLICT (token_address, provider, occurred_at, interval_seconds) DO NOTHING
      RETURNING id`,
      [
        id,
        observation.tokenAddress,
        occurredAt,
        Number(observation.priceUsd),
        observation.marketCapUsd ?? null,
        observation.liquidityUsd ?? null,
        observation.volumeIntervalUsd ?? null,
        observation.intervalSeconds,
        observation.provider,
        observation.externalId ?? null,
        observation.providerTimestamp ? toDate(observation.providerTimestamp) : occurredAt,
        observation.observedTimestamp ? toDate(observation.observedTimestamp) : new Date(),
        JSON.stringify(observation.provenance || {}),
      ]
    );
    const duplicate = result.rowCount === 0;
    const row = {
      id,
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
      provenance: observation.provenance || {},
    };
    return { row, duplicate };
  }

  async getMarketObservationsForToken(tokenAddress, { startTime, endTime, maxOccurredAt } = {}) {
    const params = [tokenAddress];
    let sql = `SELECT * FROM signal_market_observations WHERE token_address = $1`;
    if (startTime) {
      params.push(toDate(startTime));
      sql += ` AND occurred_at >= $${params.length}`;
    }
    if (endTime) {
      params.push(toDate(endTime));
      sql += ` AND occurred_at <= $${params.length}`;
    }
    if (maxOccurredAt) {
      params.push(toDate(maxOccurredAt));
      sql += ` AND occurred_at <= $${params.length}`;
    }
    sql += ` ORDER BY occurred_at ASC, id ASC`;
    const res = await this.pool.query(sql, params);
    return res.rows.map(mapObservationRow);
  }

  async insertMarketIngestionStats(stats) {
    await this.pool.query(
      `INSERT INTO signal_market_ingestion_stats (
        id, token_address, provider, requested_start, requested_end, effective_resolution_seconds,
        received_count, inserted_count, duplicate_count, rejected_count, missing_intervals, metadata
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)`,
      [
        stats.id,
        stats.tokenAddress,
        stats.provider,
        toDate(stats.requestedStart),
        toDate(stats.requestedEnd),
        stats.effectiveResolutionSeconds ?? null,
        stats.receivedCount ?? 0,
        stats.insertedCount ?? 0,
        stats.duplicateCount ?? 0,
        stats.rejectedCount ?? 0,
        stats.missingIntervalCount ?? stats.missingIntervals ?? 0,
        JSON.stringify(stats.metadata || stats),
      ]
    );
    return stats;
  }

  async getLatestMarketIngestionStats(tokenAddress) {
    const res = await this.pool.query(
      `SELECT * FROM signal_market_ingestion_stats
       WHERE token_address = $1 ORDER BY created_at DESC LIMIT 1`,
      [tokenAddress]
    );
    const row = res.rows[0];
    if (!row) return null;
    return {
      tokenAddress: row.token_address,
      provider: row.provider,
      requestedStart: row.requested_start,
      requestedEnd: row.requested_end,
      effectiveResolutionSeconds: row.effective_resolution_seconds,
      receivedCount: row.received_count,
      insertedCount: row.inserted_count,
      duplicateCount: row.duplicate_count,
      rejectedCount: row.rejected_count,
      missingIntervals: row.missing_intervals,
      metadata: row.metadata,
      createdAt: row.created_at,
    };
  }

  async getOutcomesForToken(tokenAddress) {
    const res = await this.pool.query(
      `SELECT * FROM signal_market_outcomes WHERE token_address = $1
       ORDER BY decision_at NULLS LAST, execution_delay_seconds NULLS LAST, observed_at ASC`,
      [tokenAddress]
    );
    return res.rows.map(mapOutcomeRow);
  }

  async getDecisionsForToken(tokenAddress) {
    const res = await this.pool.query(
      `SELECT * FROM signal_decisions WHERE token_address = $1 ORDER BY decided_at ASC`,
      [tokenAddress]
    );
    return res.rows.map(mapDecisionRow);
  }

  async getSnapshotsForToken(tokenAddress) {
    const res = await this.pool.query(
      `SELECT * FROM signal_feature_snapshots WHERE token_address = $1 ORDER BY evaluated_at ASC`,
      [tokenAddress]
    );
    return res.rows.map(r => ({
      id: r.id,
      tokenAddress: r.token_address,
      evaluatedAt: r.evaluated_at,
      featureVersion: r.feature_version,
      features: r.features,
      evidenceEventIds: r.evidence_event_ids,
    }));
  }

  async getLatestDecision(tokenAddress) {
    const res = await this.pool.query(
      `SELECT * FROM signal_decisions WHERE token_address = $1 ORDER BY decided_at DESC LIMIT 1`,
      [tokenAddress]
    );
    return res.rows[0] ? mapDecisionRow(res.rows[0]) : null;
  }

  async getOpenPaperPosition(tokenAddress) {
    const res = await this.pool.query(
      `SELECT * FROM signal_paper_positions
       WHERE token_address = $1 AND status != 'CLOSED'
       ORDER BY opened_at DESC LIMIT 1`,
      [tokenAddress]
    );
    const row = res.rows[0];
    if (!row) return null;
    return mapPaperPositionRow(row);
  }

  async upsertResearchCandidate(candidate) {
    await this.pool.query(
      `INSERT INTO signal_research_candidates (
        id, token_address, chain, discovered_from, earliest_known_call_at,
        source_ids, source_cluster_ids, selection_category, selection_reason,
        provenance, status, exclusion_reason, updated_at
      ) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9,$10::jsonb,$11,$12,NOW())
      ON CONFLICT (token_address, discovered_from) DO UPDATE SET
        status = EXCLUDED.status,
        exclusion_reason = EXCLUDED.exclusion_reason,
        selection_category = EXCLUDED.selection_category,
        provenance = signal_research_candidates.provenance || EXCLUDED.provenance,
        updated_at = NOW()`,
      [
        candidate.id,
        candidate.tokenAddress,
        candidate.chain || 'solana',
        candidate.discoveredFrom,
        candidate.earliestKnownCallAt ? toDate(candidate.earliestKnownCallAt) : null,
        JSON.stringify(candidate.sourceIds || []),
        JSON.stringify(candidate.sourceClusterIds || []),
        candidate.selectionCategory || null,
        candidate.selectionReason || null,
        JSON.stringify({
          ...(candidate.provenance || {}),
          acquisitionPayload: candidate.acquisitionPayload || {},
        }),
        candidate.status,
        candidate.exclusionReason || null,
      ]
    );
    return candidate;
  }

  async updateResearchCandidateStatus(id, status, exclusionReason = null) {
    await this.pool.query(
      `UPDATE signal_research_candidates SET status = $2, exclusion_reason = COALESCE($3, exclusion_reason), updated_at = NOW()
       WHERE id = $1`,
      [id, status, exclusionReason]
    );
  }

  async listResearchCandidates({ status, tokenAddress } = {}) {
    const params = [];
    let sql = `SELECT * FROM signal_research_candidates WHERE 1=1`;
    if (status) {
      params.push(status);
      sql += ` AND status = $${params.length}`;
    }
    if (tokenAddress) {
      params.push(tokenAddress);
      sql += ` AND token_address = $${params.length}`;
    }
    sql += ` ORDER BY token_address ASC`;
    const res = await this.pool.query(sql, params);
    return res.rows.map(mapResearchCandidateRow);
  }

  async upsertResearchCohort(cohort) {
    await this.pool.query(
      `INSERT INTO signal_research_cohorts (id, name, definition_version, metadata, frozen_at, selection_version)
       VALUES ($1,$2,$3,$4::jsonb,$5,$6)
       ON CONFLICT (id) DO UPDATE SET
         name = EXCLUDED.name,
         definition_version = EXCLUDED.definition_version,
         metadata = signal_research_cohorts.metadata || EXCLUDED.metadata,
         frozen_at = COALESCE(EXCLUDED.frozen_at, signal_research_cohorts.frozen_at),
         selection_version = COALESCE(EXCLUDED.selection_version, signal_research_cohorts.selection_version)`,
      [
        cohort.id,
        cohort.name,
        cohort.definitionVersion,
        JSON.stringify(cohort.metadata || {}),
        cohort.frozenAt ? toDate(cohort.frozenAt) : null,
        cohort.selectionVersion || null,
      ]
    );
    return cohort;
  }

  async addCohortMember(member) {
    const frozen = await this.pool.query(
      `SELECT frozen_at FROM signal_research_cohorts WHERE id = $1`,
      [member.cohortId]
    );
    if (frozen.rows[0]?.frozen_at) {
      throw new Error(
        `Cohort ${member.cohortId} is frozen; create a new cohort version to mutate membership`
      );
    }
    await this.pool.query(
      `INSERT INTO signal_research_cohort_members (cohort_id, token_address, inclusion_reason, provenance)
       VALUES ($1,$2,$3,$4::jsonb)
       ON CONFLICT (cohort_id, token_address) DO NOTHING`,
      [
        member.cohortId,
        member.tokenAddress,
        member.inclusionReason,
        JSON.stringify(member.provenance || {}),
      ]
    );
    return member;
  }

  async getCohortMembers(cohortId) {
    const res = await this.pool.query(
      `SELECT * FROM signal_research_cohort_members WHERE cohort_id = $1 ORDER BY token_address ASC`,
      [cohortId]
    );
    return res.rows.map(r => ({
      cohortId: r.cohort_id,
      tokenAddress: r.token_address,
      inclusionReason: r.inclusion_reason,
      provenance: r.provenance,
      createdAt: r.created_at,
    }));
  }

  listResearchCohorts() {
    return this.pool
      .query(`SELECT * FROM signal_research_cohorts ORDER BY created_at DESC`)
      .then(res =>
        res.rows.map(r => ({
          id: r.id,
          name: r.name,
          definitionVersion: r.definition_version,
          metadata: r.metadata,
          frozenAt: r.frozen_at,
          selectionVersion: r.selection_version,
          createdAt: r.created_at,
        }))
      );
  }

  getResearchObservations(tokenAddress, definitionVersion) {
    return this.pool
      .query(
        `SELECT * FROM signal_research_observations
         WHERE token_address = $1 AND ($2::text IS NULL OR definition_version = $2)
         ORDER BY occurred_at ASC`,
        [tokenAddress, definitionVersion || null]
      )
      .then(res =>
        res.rows.map(r => ({
          id: r.id,
          tokenAddress: r.token_address,
          observationType: r.observation_type,
          occurredAt: r.occurred_at,
          featureSnapshotId: r.feature_snapshot_id,
          triggerEventIds: r.trigger_event_ids,
          evidenceEventIds: r.evidence_event_ids,
          definitionVersion: r.definition_version,
          metadata: r.metadata,
        }))
      );
  }

  async insertResearchObservation(observation) {
    await this.pool.query(
      `INSERT INTO signal_research_observations (
        id, token_address, observation_type, occurred_at, feature_snapshot_id,
        trigger_event_ids, evidence_event_ids, definition_version, metadata
      ) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9::jsonb)
      ON CONFLICT (token_address, observation_type, definition_version) DO NOTHING`,
      [
        observation.id || randomUUID(),
        observation.tokenAddress,
        observation.observationType,
        toDate(observation.occurredAt),
        observation.featureSnapshotId || null,
        JSON.stringify(observation.triggerEventIds || []),
        JSON.stringify(observation.evidenceEventIds || []),
        observation.definitionVersion,
        JSON.stringify(observation.metadata || {}),
      ]
    );
    const existing = await this.pool.query(
      `SELECT * FROM signal_research_observations
       WHERE token_address = $1 AND observation_type = $2 AND definition_version = $3`,
      [observation.tokenAddress, observation.observationType, observation.definitionVersion]
    );
    return mapResearchObservationRow(existing.rows[0]);
  }

  async getResearchObservationOutcomes(observationId) {
    const res = await this.pool.query(
      `SELECT * FROM signal_research_observation_outcomes WHERE observation_id = $1
       ORDER BY execution_delay_seconds ASC`,
      [observationId]
    );
    return res.rows.map(r => ({
      id: r.id,
      observationId: r.observation_id,
      executionDelaySeconds: r.execution_delay_seconds,
      dataAvailability: r.data_availability,
      entryPrice: r.entry_price,
      label: r.label,
      mfe: r.mfe,
      mae: r.mae,
      timeTo2xSeconds: r.time_to_2x_seconds,
      timeToMinus30Seconds: r.time_to_minus_30_seconds,
      return15m: r.return_15m,
      return1h: r.return_1h,
      return6h: r.return_6h,
      return24h: r.return_24h,
      horizonHours: r.horizon_hours,
      metadata: r.metadata,
    }));
  }

  async insertResearchObservationOutcome(outcome) {
    await this.pool.query(
      `INSERT INTO signal_research_observation_outcomes (
        id, observation_id, execution_delay_seconds, data_availability, entry_price, label,
        mfe, mae, time_to_2x_seconds, time_to_minus_30_seconds,
        return_15m, return_1h, return_6h, return_24h, horizon_hours, metadata
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb)
      ON CONFLICT (observation_id, execution_delay_seconds) DO NOTHING`,
      [
        outcome.id || randomUUID(),
        outcome.observationId,
        outcome.executionDelaySeconds,
        outcome.dataAvailability,
        outcome.entryPrice,
        outcome.label,
        outcome.mfe,
        outcome.mae,
        outcome.timeTo2xSeconds,
        outcome.timeToMinus30Seconds,
        outcome.return15m,
        outcome.return1h,
        outcome.return6h,
        outcome.return24h,
        outcome.horizonHours ?? 24,
        JSON.stringify(outcome.metadata || {}),
      ]
    );
    return outcome;
  }

  researchObservations = [];
  researchObservationOutcomes = [];
  researchCohorts = new Map();

  async clearReplayArtifacts(tokenAddress) {
    await this.pool.query(`DELETE FROM signal_paper_transactions WHERE position_id IN (
      SELECT id FROM signal_paper_positions WHERE token_address = $1
    )`, [tokenAddress]);
    await this.pool.query(`DELETE FROM signal_paper_positions WHERE token_address = $1`, [
      tokenAddress,
    ]);
    await this.pool.query(`DELETE FROM signal_decisions WHERE token_address = $1`, [tokenAddress]);
    await this.pool.query(`DELETE FROM signal_feature_snapshots WHERE token_address = $1`, [
      tokenAddress,
    ]);
    await this.pool.query(`DELETE FROM signal_alerts WHERE token_address = $1`, [tokenAddress]);
    await this.pool.query(`DELETE FROM signal_market_outcomes WHERE token_address = $1`, [
      tokenAddress,
    ]);
  }
}

function mapTokenRow(row) {
  return {
    tokenAddress: row.token_address,
    chain: row.chain,
    ticker: row.ticker,
    addressProvenance: row.address_provenance,
    metadata: row.metadata,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapEventRow(row) {
  return {
    id: row.id,
    tokenAddress: row.token_address,
    chain: row.chain,
    eventType: row.event_type,
    occurredAt: row.occurred_at,
    observedAt: row.observed_at,
    ingestedAt: row.ingested_at,
    sourceType: row.source_type,
    sourceId: row.source_id,
    sourceClusterId: row.source_cluster_id,
    actorId: row.actor_id,
    walletAddress: row.wallet_address,
    payload: row.payload,
    provenance: row.provenance,
    confidence: row.confidence,
  };
}

function mapObservationRow(row) {
  return {
    id: row.id,
    tokenAddress: row.token_address,
    occurredAt: row.occurred_at,
    priceUsd: row.price_usd,
    marketCapUsd: row.market_cap_usd,
    liquidityUsd: row.liquidity_usd,
    volumeIntervalUsd: row.volume_interval_usd,
    intervalSeconds: row.interval_seconds,
    provider: row.provider,
    externalId: row.external_id,
    providerTimestamp: row.provider_timestamp,
    observedTimestamp: row.observed_timestamp,
    ingestedAt: row.ingested_at,
    provenance: row.provenance,
  };
}

function mapOutcomeRow(row) {
  return {
    id: row.id,
    tokenAddress: row.token_address,
    observedAt: row.observed_at,
    entryPrice: row.entry_price,
    label: row.label,
    mfe: row.mfe,
    mae: row.mae,
    timeTo2xSeconds: row.time_to_2x_seconds,
    timeToMinus30Seconds: row.time_to_minus_30_seconds,
    return15m: row.return_15m,
    return1h: row.return_1h,
    return6h: row.return_6h,
    return24h: row.return_24h,
    horizonHours: row.horizon_hours,
    metadata: row.metadata,
    decisionAt: row.decision_at,
    executionDelaySeconds: row.execution_delay_seconds,
    effectiveExecutionAt: row.effective_execution_at,
    effectivePrice: row.effective_price,
    dataResolutionSeconds: row.data_resolution_seconds,
    highestPrice: row.highest_price,
    lowestPrice: row.lowest_price,
    outcomeTimestamp: row.outcome_timestamp,
  };
}

function mapDecisionRow(row) {
  return {
    id: row.id,
    tokenAddress: row.token_address,
    decidedAt: row.decided_at,
    state: row.state,
    previousState: row.previous_state,
    score: row.score,
    featureSnapshotId: row.feature_snapshot_id,
    strategyVersion: row.strategy_version,
    explanation: row.explanation,
  };
}

function mapPaperPositionRow(row) {
  return {
    id: row.id,
    tokenAddress: row.token_address,
    openedAt: row.opened_at,
    entryPrice: row.entry_price,
    entryMarketCap: row.entry_market_cap,
    notionalUsd: row.notional_usd,
    remainingPct: row.remaining_pct,
    realizedPnlUsd: row.realized_pnl_usd,
    unrealizedPnlUsd: row.unrealized_pnl_usd,
    status: row.status,
    entrySnapshotId: row.entry_snapshot_id,
    closedAt: row.closed_at,
    metadata: row.metadata,
  };
}

function mapResearchCandidateRow(row) {
  const provenance = row.provenance || {};
  return {
    id: row.id,
    tokenAddress: row.token_address,
    chain: row.chain,
    discoveredFrom: row.discovered_from,
    earliestKnownCallAt: row.earliest_known_call_at,
    sourceIds: row.source_ids,
    sourceClusterIds: row.source_cluster_ids,
    selectionCategory: row.selection_category,
    selectionReason: row.selection_reason,
    provenance,
    acquisitionPayload: provenance.acquisitionPayload || {},
    status: row.status,
    exclusionReason: row.exclusion_reason,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapResearchObservationRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    tokenAddress: row.token_address,
    observationType: row.observation_type,
    occurredAt: row.occurred_at,
    featureSnapshotId: row.feature_snapshot_id,
    triggerEventIds: row.trigger_event_ids,
    evidenceEventIds: row.evidence_event_ids,
    definitionVersion: row.definition_version,
    metadata: row.metadata,
  };
}

module.exports = {
  PostgresSignalStore,
};
