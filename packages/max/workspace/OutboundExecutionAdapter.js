'use strict';

/**
 * Canonical outbound execution adapter — executes approved mission bundle via provider transport.
 * Emmett does not regenerate copy or select recipients during EXECUTE.
 */

const { sendEmail: brevoSendEmail } = require('../../providers/brevo/sendEmail');
const {
  buildExecutionBundle,
  buildExecutionRecord,
  findSuccessfulExecutionRecord,
  bindGovernedRefillSend,
  ensureExecutionIdentityFields,
  EXECUTION_RECORD_STATUS,
  summarizeExecutionRecords,
} = require('../../acquisition-mission/OutboundExecution');
const { nowIso } = require('../../acquisition-mission/types');
const { findValidExecutionApproval } = require('../../acquisition-mission/ExecutionApproval');
const {
  BLOCK_CODES,
  resolveCanonicalSenderIdentity,
  normalizeCanonicalSender,
  evaluateCanonicalSenderReadiness,
} = require('../../../utils/canonicalSenderIdentity');
const {
  createExternalActionLedger,
  requestFingerprint,
} = require('../../../services/maxReliability/externalActionLedger');
const { guardedDispatch } = require('../../../services/maxReliability/guardedDispatch');

const OUTBOUND_ACTION_KIND = 'amo_outbound_email';

/**
 * MAX-P0-003 — the durable external-action ledger for this execution.
 * Explicit injection wins (tests, alternative stores); otherwise any database
 * pool gets the PostgreSQL ledger, which fails closed when its schema is
 * missing. Without a pool there is no durable state to protect.
 */
function resolveExternalActionLedger(input = {}) {
  if (input.externalActionLedger !== undefined) return input.externalActionLedger;
  return input.pool ? createExternalActionLedger(input.pool) : null;
}

/**
 * MAX-P0-003 — a pre-ledger ATTEMPTED record with no later SENT/FAILED record
 * means the process stopped between dispatch and recording the result. The
 * message may have been delivered; it must not be sent again automatically.
 */
/**
 * R7 — a mission sends at most one outbound message per prospect. A revised
 * artifact (new prepared revision → new execution identity) must not bypass a
 * send that was, or may have been, dispatched under an earlier revision.
 * Legitimate follow-up outreach is a distinct, separately authorized logical
 * intent (a different mission); this adapter does not create one.
 */
function findPriorDispatchForProspect(records = [], prospectId, executionIdentity) {
  return records.find((row) => row
    && String(row.prospectId) === String(prospectId)
    && row.executionIdentity && row.executionIdentity !== executionIdentity
    && (row.status === EXECUTION_RECORD_STATUS.SENT || row.status === EXECUTION_RECORD_STATUS.ATTEMPTED)) || null;
}

function findUnresolvedLegacyAttempt(records = [], executionIdentity) {
  const forIdentity = records.filter((row) => row && row.executionIdentity === executionIdentity);
  if (forIdentity.some((row) => row.status === EXECUTION_RECORD_STATUS.SENT
    || row.status === EXECUTION_RECORD_STATUS.FAILED)) return null;
  return forIdentity.find((row) => row.status === EXECUTION_RECORD_STATUS.ATTEMPTED
    && !(row.payload && row.payload.externalActionLedger === true)) || null;
}

async function resolveExecuteSender(input = {}) {
  if (input.canonicalSender) {
    return normalizeCanonicalSender(input.canonicalSender);
  }
  if (input.senderIdentity && typeof input.senderIdentity === 'object') {
    return normalizeCanonicalSender(input.senderIdentity);
  }
  return resolveCanonicalSenderIdentity({
    tenantId: input.tenantId,
    clientId: input.clientId || input.tenantId,
    client: input.client,
    pool: input.pool,
    loadClient: input.loadClient,
  });
}

/**
 * Execute one canonical approved outbound bundle.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function executeOutboundBundle(input = {}) {
  let governedProgram = null;
  const governedTenantId = input.tenantId != null ? String(input.tenantId) : null;
  if (input.pool && governedTenantId && ['10', '13'].includes(governedTenantId)) {
    const installed = await input.pool.query("SELECT to_regclass('acquisition_outbound_programs') AS installed");
    if (installed.rows[0]?.installed) {
      const program = await input.pool.query('SELECT id FROM acquisition_outbound_programs WHERE tenant_id=$1 AND mode<>\'revoked\'', [governedTenantId]);
      governedProgram = program.rows[0] || null;
      if (program.rows.length && !input.governedEnvelopeId) {
        return { blocked: true, blockReason: 'Governed outbound is controlled by its daily program.',
          blockCode: 'governed_executor_required', records: [], summary: summarizeExecutionRecords([]) };
      }
    }
  }
  const {
    mission,
    contributions = [],
    approval,
    tenantId,
    transactionId,
    executionRequestId,
    sendEmail = brevoSendEmail,
    resolveProspectAttributes,
    existingRecords = [],
    persistExecutionRecord,
    executionRequest,
    maxSends: maxSendsInput,
  } = input;

  const resolvedSender = await resolveExecuteSender(input);
  if (!resolvedSender.ok) {
    return {
      blocked: true,
      blockReason: resolvedSender.blockReason,
      blockCode: resolvedSender.code,
      bundle: null,
      records: [],
      summary: summarizeExecutionRecords([]),
    };
  }

  if (input.senderReadiness && input.senderReadiness.ready === false) {
    return {
      blocked: true,
      blockReason: input.senderReadiness.blockReason || 'Canonical sender is not ready to send.',
      blockCode: input.senderReadiness.code || BLOCK_CODES.NOT_READY,
      bundle: null,
      records: [],
      summary: summarizeExecutionRecords([]),
    };
  }

  const mustEvaluateProvider = input.requireProviderReadiness === true
    || Boolean(input.brevoState)
    || (Boolean(input.pool) && input.senderReadiness == null && input.requireProviderReadiness !== false);
  if (mustEvaluateProvider && !(input.senderReadiness && input.senderReadiness.ready === true)) {
    const readiness = await evaluateCanonicalSenderReadiness({
      identity: resolvedSender.identity,
      client: input.client,
      brevoState: input.brevoState,
      brevoApiKey: input.brevoApiKey,
      http: input.http,
      pool: input.pool,
    });
    if (!readiness.ready) {
      return {
        blocked: true,
        blockReason: readiness.blockReason || 'Canonical sender failed sending readiness.',
        blockCode: readiness.code || BLOCK_CODES.NOT_READY,
        bundle: null,
        records: [],
        summary: summarizeExecutionRecords([]),
      };
    }
  }

  const bundleResult = buildExecutionBundle({
    mission,
    contributions,
    approval,
    tenantId,
    resolveProspectAttributes,
    canonicalSender: resolvedSender.identity,
  });

  if (!bundleResult.ok) {
    return {
      blocked: true,
      blockReason: bundleResult.blockReason,
      blockCode: bundleResult.blockCode || null,
      bundle: null,
      records: [],
      summary: summarizeExecutionRecords([]),
    };
  }

  const { bundle } = bundleResult;
  // A delegated daily approval cannot escape its durable scheduler via manual CER,
  // replay, a stale one-send runner, or a caller-supplied prospect filter.
  const governedApproval = (approval || findValidExecutionApproval(contributions, mission.id))?.payload?.dailyEnvelope;
  if (governedProgram && (!governedApproval || governedApproval.programId !== governedProgram.id)) {
    return { blocked: true, blockReason: 'Approval does not belong to the active governed outbound program.',
      blockCode: 'governed_executor_required', bundle, records: [], summary: summarizeExecutionRecords([]) };
  }
  if (governedApproval) {
    const envelope = governedApproval;
    if (!input.governedEnvelopeId || input.governedEnvelopeId !== envelope.id || typeof sendEmail.beforeAttempt !== 'function') {
      return { blocked: true, blockReason: 'Governed daily envelope requires its guarded executor.',
        blockCode: 'governed_executor_required', bundle, records: [], summary: summarizeExecutionRecords([]) };
    }
    const allowedIds = new Set((envelope.candidateIds || []).map((id) => String(id)));
    for (const id of input.governedManifestCandidateIds || []) {
      if (id) allowedIds.add(String(id));
    }
    bundle.sends = bundle.sends.filter(row => allowedIds.has(String(row.prospectId)));
    const refillItem = input.governedRefillItem;
    const refillProspectId = String(refillItem?.candidate_id || refillItem?.snapshot?.candidateId || '');
    if (refillItem && refillProspectId && !bundle.sends.some(row => String(row.prospectId) === refillProspectId)) {
      bundle.sends.push(bindGovernedRefillSend(bundle, refillItem, bundle.executionApproval));
    }
  }
  const records = [];
  const requestedMax = Number(
    maxSendsInput
    ?? (executionRequest && executionRequest.payload && executionRequest.payload.maxSends)
  );
  const maxSends = Number.isFinite(requestedMax) && requestedMax > 0 ? requestedMax : null;
  const prospectFilter = resolveProspectFilter(input, executionRequest);
  let sendableConsidered = 0;
  const approvalMeta = bundle.executionApproval;
  const explicitSender = {
    email: bundle.provider.senderIdentity,
    name: bundle.provider.senderName,
  };
  if (!explicitSender.email || !explicitSender.name) {
    return {
      blocked: true,
      blockReason: 'Canonical AMO execution requires an explicit provider sender object.',
      blockCode: BLOCK_CODES.REQUIRED,
      bundle,
      records: [],
      summary: summarizeExecutionRecords([]),
    };
  }

  for (const send of bundle.sends) {
    if (prospectFilter.size && !prospectFilter.has(String(send.prospectId || ''))) {
      continue;
    }

    if (send.status === EXECUTION_RECORD_STATUS.BLOCKED) {
      const blockedRecord = buildExecutionRecord({
        missionId: bundle.missionId,
        tenantId: bundle.tenantId,
        prospectId: send.prospectId,
        preparedArtifactRevision: approvalMeta.preparedArtifactRevision,
        executionApprovalContributionId: approvalMeta.contributionId,
        provider: bundle.provider.provider,
        status: EXECUTION_RECORD_STATUS.BLOCKED,
        providerErrorCode: 'blocked',
        providerErrorMessage: send.blockReason,
        executionRequestId,
        transactionId,
        executionIdentity: send.executionIdentity,
        idempotencyKey: send.idempotencyKey,
        payload: { blockReason: send.blockReason },
      });
      records.push(blockedRecord);
      if (typeof persistExecutionRecord === 'function') {
        await persistExecutionRecord(blockedRecord);
      }
      continue;
    }

    // Count sendable items toward the cap before send or dedup. Replay of
    // maxSends=1 must not advance to the next unsent queue item.
    if (maxSends != null && sendableConsidered >= maxSends) {
      continue;
    }
    sendableConsidered += 1;

    const identified = ensureExecutionIdentityFields({
      ...send,
      missionId: bundle.missionId,
      preparedArtifactRevision: approvalMeta.preparedArtifactRevision,
    });
    const priorSuccess = findSuccessfulExecutionRecord(
      [...existingRecords, ...records],
      identified.executionIdentity
    );
    if (priorSuccess) {
      records.push({ ...priorSuccess, deduplicated: true });
      continue;
    }

    const blockedFor = (code, message, extra = {}) => buildExecutionRecord({
      missionId: bundle.missionId,
      tenantId: bundle.tenantId,
      prospectId: send.prospectId,
      preparedArtifactRevision: approvalMeta.preparedArtifactRevision,
      executionApprovalContributionId: approvalMeta.contributionId,
      provider: bundle.provider.provider,
      status: EXECUTION_RECORD_STATUS.BLOCKED,
      providerErrorCode: code,
      providerErrorMessage: message,
      executionRequestId,
      transactionId,
      executionIdentity: identified.executionIdentity,
      idempotencyKey: identified.idempotencyKey,
      payload: { blockReason: message, ...extra },
    });

    if (findUnresolvedLegacyAttempt([...existingRecords, ...records], identified.executionIdentity)) {
      const blocked = blockedFor('external_action_outcome_unknown',
        'A previous send attempt has no recorded outcome; it is not retried automatically.');
      records.push(blocked);
      if (typeof persistExecutionRecord === 'function') await persistExecutionRecord(blocked);
      continue;
    }

    if (findPriorDispatchForProspect([...existingRecords, ...records], send.prospectId, identified.executionIdentity)) {
      const blocked = blockedFor('outreach_intent_already_dispatched',
        'This prospect already has a sent or attempted message in this mission; a revised message is not sent.');
      records.push(blocked);
      if (typeof persistExecutionRecord === 'function') await persistExecutionRecord(blocked);
      continue;
    }

    const command = {
      outreachAuthorization: { prospectId: send.prospectId, clientId: input.clientId || bundle.tenantId, pool: input.pool },
      toEmail: send.email, toName: send.toName, subject: send.message.subject, body: send.message.body,
      tags: [`mission:${bundle.missionId}`, `prospect:${send.prospectId}`, `revision:${approvalMeta.preparedArtifactRevision}`],
      idempotencyKey: identified.idempotencyKey, sender: explicitSender, requireExplicitSender: true,
    };

    // MAX-P0-003 — atomic claim before any provider invocation. Duplicate and
    // uncertain claims are rejected here, before the provider is reached; the
    // dispatch marker commits before the send (services/maxReliability/guardedDispatch).
    const ledger = resolveExternalActionLedger(input);
    const attemptedAt = nowIso();
    const pendingRecord = buildExecutionRecord({
      missionId: bundle.missionId,
      tenantId: bundle.tenantId,
      prospectId: send.prospectId,
      preparedArtifactRevision: approvalMeta.preparedArtifactRevision,
      executionApprovalContributionId: approvalMeta.contributionId,
      provider: bundle.provider.provider,
      status: EXECUTION_RECORD_STATUS.ATTEMPTED,
      executionRequestId,
      transactionId,
      executionIdentity: identified.executionIdentity,
      idempotencyKey: identified.idempotencyKey,
      attemptedAt,
      payload: {
        email: send.email,
        subject: send.message.subject,
        queuePosition: send.queuePosition,
        senderEmail: explicitSender.email,
        sendingDomain: bundle.provider.sendingDomain,
        ...(ledger ? { externalActionLedger: true } : {}),
      },
    });
    const persistPending = async () => {
      if (typeof persistExecutionRecord === 'function') await persistExecutionRecord(pendingRecord);
    };
    let providerResult;
    let externalActionState = null;
    if (ledger) {
      let guarded;
      try {
        // R7 — the durable ledger also sees sends from other processes/revisions.
        const priorClaim = await ledger.findBlockingForSubject({
          tenantId: String(bundle.tenantId), actionKind: OUTBOUND_ACTION_KIND, missionId: bundle.missionId,
          subjectRef: `prospect:${send.prospectId}`, excludeKey: `amo-outbound:${identified.executionIdentity}`,
        });
        if (priorClaim) {
          const blocked = blockedFor('outreach_intent_already_dispatched',
            'This prospect already has a dispatched or unresolved message in this mission; a revised message is not sent.',
            { externalActionState: priorClaim.state });
          records.push(blocked);
          if (typeof persistExecutionRecord === 'function') await persistExecutionRecord(blocked);
          continue;
        }
        guarded = await guardedDispatch({
          ledger,
          claim: {
            tenantId: String(bundle.tenantId),
            idempotencyKey: `amo-outbound:${identified.executionIdentity}`,
            actionKind: OUTBOUND_ACTION_KIND,
            requestFingerprint: requestFingerprint({
              tenantId: String(bundle.tenantId),
              missionId: bundle.missionId,
              prospectId: String(send.prospectId),
              revision: approvalMeta.preparedArtifactRevision,
              toEmail: String(send.email || '').toLowerCase(),
              subject: send.message.subject,
              body: send.message.body,
              sender: { email: explicitSender.email, name: explicitSender.name },
            }),
            missionId: bundle.missionId,
            subjectRef: `prospect:${send.prospectId}`,
            provider: bundle.provider.provider,
            providerIdempotencyKey: identified.idempotencyKey,
            correlationId: input.correlationId || null,
          },
          beforeDispatch: governedApproval ? () => sendEmail.beforeAttempt(command) : null,
          persistAttempt: persistPending,
          dispatch: () => sendEmail(command),
        });
      } catch (err) {
        if (!/^reliability_schema_|^external_action_ledger_/.test(String(err && err.code))) throw err;
        const blocked = blockedFor(err.code, 'Durable action claim unavailable; nothing was sent.');
        records.push(blocked);
        if (typeof persistExecutionRecord === 'function') await persistExecutionRecord(blocked);
        continue;
      }
      if (guarded.outcome === 'replay') {
        // Acknowledged earlier (the execution record was lost): restore it, never resend.
        const replayed = buildExecutionRecord({
          ...pendingRecord,
          status: EXECUTION_RECORD_STATUS.SENT,
          providerMessageId: guarded.receipt && guarded.receipt.providerRef,
          sentAt: (guarded.receipt && guarded.receipt.acknowledgedAt) || nowIso(),
          payload: { ...pendingRecord.payload, ledgerReplay: true, externalActionState: guarded.receipt && guarded.receipt.state },
        });
        records.push({ ...replayed, deduplicated: true });
        if (typeof persistExecutionRecord === 'function') await persistExecutionRecord(replayed);
        continue;
      }
      if (guarded.outcome === 'blocked') {
        const blocked = blockedFor(guarded.code || 'external_action_blocked',
          guarded.code === 'external_action_outcome_unknown'
            ? 'A previous send attempt has an unknown outcome; it requires reconciliation and is not retried.'
            : guarded.code === 'idempotency_key_reused'
              ? 'This send identity was already used for different content; nothing was sent.'
              : 'This send is already claimed by another execution; nothing was sent.',
          { externalActionState: guarded.receipt && guarded.receipt.state });
        records.push(blocked);
        if (typeof persistExecutionRecord === 'function') await persistExecutionRecord(blocked);
        continue;
      }
      providerResult = guarded.providerResult;
      externalActionState = guarded.ledgerState;
    } else {
      if (governedApproval) await sendEmail.beforeAttempt(command);
      await persistPending();
      providerResult = await sendEmail(command);
    }

    const sentAt = nowIso();
    const finalRecord = buildExecutionRecord({
      ...pendingRecord,
      status: providerResult.success ? EXECUTION_RECORD_STATUS.SENT : EXECUTION_RECORD_STATUS.FAILED,
      providerMessageId: providerResult.providerMessageId || providerResult.messageId || null,
      providerErrorCode: providerResult.providerErrorCode || null,
      providerErrorMessage: providerResult.providerErrorMessage || providerResult.error || null,
      sentAt: providerResult.success ? sentAt : null,
      attemptedAt,
      updatedAt: sentAt,
      payload: {
        ...pendingRecord.payload,
        providerResponse: providerResult.brevoResponse || null,
        ...(externalActionState ? { externalActionState } : {}),
      },
    });
    records.push(finalRecord);
    if (typeof persistExecutionRecord === 'function') {
      await persistExecutionRecord(finalRecord);
    }
  }

  return {
    blocked: false,
    bundle,
    records,
    summary: summarizeExecutionRecords(records),
  };
}

function resolveProspectFilter(input = {}, executionRequest = null) {
  const payload = (executionRequest && executionRequest.payload) || {};
  const values = []
    .concat(input.prospectId || [])
    .concat(input.prospectIds || [])
    .concat(payload.prospectId || [])
    .concat(payload.prospectIds || [])
    .map((value) => String(value || '').trim())
    .filter(Boolean);
  return new Set(values);
}

module.exports = {
  executeOutboundBundle,
  resolveExternalActionLedger,
  findUnresolvedLegacyAttempt,
  findPriorDispatchForProspect,
  OUTBOUND_ACTION_KIND,
  resolveExecuteSender,
  bindGovernedRefillSend,
};
