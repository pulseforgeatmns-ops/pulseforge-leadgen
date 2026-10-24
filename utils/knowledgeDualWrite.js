'use strict';

/**
 * Safe Knowledge dual-write helpers for CRM / Scout / webhook producers (SPEC-014).
 * Never throws into the primary write path — failures stay in the outbox.
 */

const { getKnowledgeBoot, dualWriteEnabled } = require('./knowledgeRuntime');
const {
  OPERATIONAL_EVENTS,
  operationalEventFromTouchpoint,
  FLIGHT_STAGES,
  recordFlightStage,
} = require('../packages/knowledge/dualWrite');

async function getWriter() {
  if (!dualWriteEnabled()) return null;
  try {
    const boot = await getKnowledgeBoot();
    return boot.writer || null;
  } catch (err) {
    console.error('[knowledgeDualWrite] boot failed:', err.message);
    return null;
  }
}

/**
 * Fire-and-forget safe wrapper — never rejects to callers.
 */
function safe(promiseFactory) {
  return Promise.resolve()
    .then(promiseFactory)
    .catch((err) => {
      console.error('[knowledgeDualWrite] suppressed:', err && err.message ? err.message : err);
      return { status: 'error', error: String(err && err.message ? err.message : err) };
    });
}

async function safeWriteCompany(row, options = {}) {
  return safe(async () => {
    const writer = await getWriter();
    if (!writer || !row || row.id == null) return { status: 'disabled' };
    return writer.writeCompany(
      {
        id: row.id,
        client_id: row.client_id,
        name: row.name,
        industry: row.industry,
        location: row.location,
        website: row.website,
        icp_score: row.icp_score,
        created_at: row.created_at,
        updated_at: row.updated_at,
      },
      {
        source: options.source || 'crm',
        operationalEventType:
          options.operationalEventType || OPERATIONAL_EVENTS.COMPANY_DISCOVERED,
        markDiscovered: options.markDiscovered !== false,
        flightId: options.flightId,
      }
    );
  });
}

async function safeWriteProspect(row, options = {}) {
  return safe(async () => {
    const writer = await getWriter();
    if (!writer || !row || row.id == null) return { status: 'disabled' };
    return writer.writeProspect(
      {
        id: row.id,
        client_id: row.client_id,
        company_id: row.company_id,
        company_name: row.company_name,
        first_name: row.first_name,
        last_name: row.last_name,
        email: row.email,
        phone: row.phone,
        job_title: row.job_title,
        icp_score: row.icp_score,
        vertical: row.vertical,
        source: row.source,
        created_at: row.created_at,
        updated_at: row.updated_at,
      },
      {
        source: options.source || 'crm',
        operationalEventType:
          options.operationalEventType || OPERATIONAL_EVENTS.CONTACT_DISCOVERED,
        markDiscovered: options.markDiscovered !== false,
        flightId: options.flightId,
      }
    );
  });
}

async function safeWriteTouchpoint(row, options = {}) {
  return safe(async () => {
    const writer = await getWriter();
    if (!writer || !row || row.id == null) return { status: 'disabled' };
    const operationalEventType =
      options.operationalEventType ||
      operationalEventFromTouchpoint(row.channel, row.action_type);
    return writer.writeTouchpoint(row, {
      source: options.source || 'crm',
      operationalEventType,
      flightId: options.flightId,
    });
  });
}

async function safeWriteOperational(event, options = {}) {
  return safe(async () => {
    const writer = await getWriter();
    if (!writer) return { status: 'disabled' };
    return writer.writeOperational(event, options);
  });
}

async function safeProcessOutbox(options = {}) {
  return safe(async () => {
    const writer = await getWriter();
    if (!writer) return { status: 'disabled', claimed: 0 };
    return writer.processOutbox(options);
  });
}

async function safeRecordFlightStage(input) {
  return safe(async () => {
    if (!dualWriteEnabled()) return { status: 'disabled' };
    const boot = await getKnowledgeBoot();
    if (!boot.pool) return { status: 'disabled' };
    await recordFlightStage(boot.pool, input);
    return { status: 'recorded', stage: input.stage };
  });
}

async function getDualWriteHealth(options = {}) {
  try {
    const writer = await getWriter();
    if (!writer) {
      return {
        enabled: false,
        knowledgeEventsToday: 0,
        knowledgeQueueDepth: 0,
        dualWriteFailures: 0,
        evidenceCreated: 0,
        recommendationsGenerated: 0,
        recommendationsExecuted: 0,
        outcomeRecords: 0,
        lastSuccessfulWrite: null,
      };
    }
    const health = await writer.health(options);
    // Pull recommendation / outcome counts from Max runtime when available
    let recommendationsGenerated = 0;
    let recommendationsExecuted = 0;
    let outcomeRecords = 0;
    try {
      const boot = await getKnowledgeBoot();
      if (boot.max && options.tenantId != null) {
        const review = boot.max.outcomeReview
          ? boot.max.outcomeReview(String(options.tenantId))
          : null;
        const success = review && review.sections && review.sections.recommendationSuccess;
        if (success) {
          recommendationsGenerated = success.generated || 0;
          recommendationsExecuted = success.executed || 0;
          outcomeRecords =
            (success.successful || 0) +
            (success.unsuccessful || 0) +
            (success.inconclusive || 0) +
            (success.observed || 0);
        }
      }
    } catch {
      // process-scoped stores may be empty
    }
    return {
      enabled: true,
      ...health,
      recommendationsGenerated,
      recommendationsExecuted,
      outcomeRecords,
    };
  } catch (err) {
    return {
      enabled: false,
      error: String(err.message || err),
      knowledgeEventsToday: 0,
      knowledgeQueueDepth: 0,
      dualWriteFailures: 0,
      evidenceCreated: 0,
      recommendationsGenerated: 0,
      recommendationsExecuted: 0,
      outcomeRecords: 0,
      lastSuccessfulWrite: null,
    };
  }
}

module.exports = {
  safeWriteCompany,
  safeWriteProspect,
  safeWriteTouchpoint,
  safeWriteOperational,
  safeProcessOutbox,
  safeRecordFlightStage,
  getDualWriteHealth,
  OPERATIONAL_EVENTS,
  FLIGHT_STAGES,
  dualWriteEnabled,
};
