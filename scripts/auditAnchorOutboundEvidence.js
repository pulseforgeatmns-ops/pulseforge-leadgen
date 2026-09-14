#!/usr/bin/env node
'use strict';

/**
 * Read-only production audit: Brevo delivery/open evidence ingestion for Anchor outbound.
 * Never mutates state.
 *
 * Railway SSH:
 *   node scripts/auditAnchorOutboundEvidence.js --confirm-production
 *
 * Optional overrides:
 *   --mission-id <id>
 *   --execution-id <amo_send_...>
 *   --recipient <email>
 *   --message-id '<brevo-message-id>'
 */

require('dotenv').config();

const pool = require('../db');

const DEFAULTS = Object.freeze({
  CLIENT_ID: 10,
  TENANT_ID: '10',
  MISSION_ID: 'mission_ad7753b0-6def-441d-bb1a-3764656f5750',
  EXECUTION_ID: 'amo_send_37a03a00-2686-4804-8360-9cf93edb52ba',
  RECIPIENT_EMAIL: 'jmeyer@backusmeyer.com',
  BREVO_MESSAGE_ID: '<202609141225.88878070336@smtp-relay.mailin.fr>',
});

const PERSISTENCE_MODEL = Object.freeze({
  outbound_send: {
    table: 'acquisition_mission_outbound_executions',
    description: 'Canonical mission-bound outbound execution record (SPEC-071 EXECUTE path).',
  },
  brevo_webhook_events: {
    table: 'email_events',
    description: 'Primary Brevo webhook/history ingestion store (insertBrevoEvent in utils/brevoEvents.js).',
  },
  mission_provider_events: {
    table: 'acquisition_mission_provider_events',
    description: 'Mission-bound provider telemetry when webhook correlates to canonical execution.',
  },
  mission_observations: {
    table: 'acquisition_mission_observations',
    description: 'Mission workspace communication evidence (Max OBSERVE / evidence inspection).',
  },
  max_signals: {
    table: 'prospect_signal_events',
    description: 'Normalized Max orchestration signals (safeIngestBrevoSignal after webhook).',
  },
  touchpoints: {
    table: 'touchpoints',
    description: 'Riley side-effect touchpoints for opens/clicks/bounces (processBrevoEventSideEffects).',
  },
  webhook_audit_log: {
    table: 'agent_log',
    description: 'Riley brevo_event_received rows and emmett email_sent / brevo_correlation_failed audit.',
  },
});

const EVIDENCE_CHAIN = Object.freeze([
  { key: 'outbound_execution_record', label: 'Outbound execution record exists' },
  { key: 'provider_message_id_on_execution', label: 'Provider message ID stored on execution record' },
  { key: 'email_event_sent', label: 'email_events row for sent (or delivered proxy for send)' },
  { key: 'email_event_delivered', label: 'email_events row for delivered' },
  { key: 'email_event_opened', label: 'email_events row for opened' },
  { key: 'mission_provider_events', label: 'Mission-bound provider events persisted' },
  { key: 'mission_observations', label: 'Mission communication observations persisted' },
  { key: 'max_signal_events', label: 'prospect_signal_events ingested for lifecycle' },
  { key: 'prospect_linkage', label: 'Events linked to CRM prospect for recipient' },
  { key: 'mission_linkage', label: 'Events linked to target mission' },
  { key: 'execution_linkage', label: 'Events linked to target execution record' },
]);

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const readOpt = (flag) => {
    const idx = argv.indexOf(flag);
    if (idx < 0) return null;
    const value = argv[idx + 1];
    if (!value || value.startsWith('--')) {
      throw Object.assign(new Error(`${flag} requires a value.`), { code: 'arg_required' });
    }
    return value;
  };

  const unknown = argv.filter((arg, i) => {
    if (arg === '--confirm-production') return false;
    if (arg.startsWith('--')) {
      const valueIdx = i + 1;
      const value = argv[valueIdx];
      if (['--mission-id', '--execution-id', '--recipient', '--message-id'].includes(arg)) {
        return false;
      }
    }
    if (['--mission-id', '--execution-id', '--recipient', '--message-id'].includes(argv[i - 1])) {
      return false;
    }
    return true;
  });

  if (!confirmProduction) {
    throw Object.assign(new Error('Refusing without --confirm-production.'), { code: 'confirm_production_required' });
  }
  if (unknown.length) {
    throw Object.assign(new Error(`Unknown argument(s): ${unknown.join(', ')}.`), { code: 'unknown_args' });
  }
  if (!process.env.DATABASE_URL) {
    throw Object.assign(new Error('Missing DATABASE_URL.'), { code: 'runtime_env_missing' });
  }

  return {
    confirmProduction,
    missionId: readOpt('--mission-id') || DEFAULTS.MISSION_ID,
    executionId: readOpt('--execution-id') || DEFAULTS.EXECUTION_ID,
    recipientEmail: (readOpt('--recipient') || DEFAULTS.RECIPIENT_EMAIL).trim().toLowerCase(),
    brevoMessageId: readOpt('--message-id') || DEFAULTS.BREVO_MESSAGE_ID,
    clientId: DEFAULTS.CLIENT_ID,
    tenantId: DEFAULTS.TENANT_ID,
  };
}

function normalizeMessageId(value) {
  const raw = String(value || '').trim();
  if (!raw) return { raw: null, bare: null, bracketed: null, variants: [] };
  const bare = raw.replace(/^<|>$/g, '');
  const bracketed = raw.startsWith('<') ? raw : `<${bare}>`;
  const variants = [...new Set([raw, bare, bracketed].filter(Boolean))];
  return { raw, bare, bracketed, variants };
}

async function tableExists(db, tableName) {
  const result = await db.query('SELECT to_regclass($1) IS NOT NULL AS present', [`public.${tableName}`]);
  return result.rows[0]?.present === true;
}

function summarizeEmailEvent(row) {
  if (!row) return null;
  return {
    id: row.id,
    event_id: row.event_id,
    event_type: row.event_type,
    event_at: row.event_at,
    recipient_email: row.recipient_email,
    prospect_id: row.prospect_id,
    client_id: row.client_id,
    brevo_message_id: row.brevo_message_id,
    open_source: row.open_source || null,
    subject_line: row.subject_line || null,
  };
}

function summarizeProviderEvent(row) {
  if (!row) return null;
  return {
    id: row.id,
    event_type: row.event_type,
    event_category: row.event_category,
    occurred_at: row.occurred_at,
    mission_id: row.mission_id,
    prospect_id: row.prospect_id,
    execution_record_id: row.execution_record_id,
    provider_message_id: row.provider_message_id,
    raw_event_type: row.raw_event_type || null,
  };
}

function summarizeObservation(row) {
  if (!row) return null;
  const payload = row.payload && typeof row.payload === 'object' ? row.payload : {};
  const evidence = payload.evidence && typeof payload.evidence === 'object' ? payload.evidence : {};
  return {
    id: row.id,
    observation: row.observation,
    at: row.at,
    specialist: row.specialist,
    event_type: payload.eventType || null,
    execution_record_id: evidence.executionRecordId || null,
    provider_message_id: evidence.providerMessageId || null,
    prospect_id: payload.prospectId || null,
  };
}

function summarizeSignal(row) {
  if (!row) return null;
  const metadata = row.metadata && typeof row.metadata === 'object' ? row.metadata : {};
  return {
    id: row.id,
    event_type: row.event_type,
    event_timestamp: row.event_timestamp,
    source: row.source,
    source_record_id: row.source_record_id,
    prospect_id: row.prospect_id,
    mission_id: metadata.mission_id || null,
    execution_record_id: metadata.execution_record_id || null,
    brevo_event_type: metadata.brevo_event_type || null,
    open_source: metadata.open_source || null,
  };
}

function lifecycleFromEmailEvents(rows) {
  const pick = (types) => {
    const match = rows.find((row) => types.includes(String(row.event_type || '').toLowerCase()));
    return match
      ? { present: true, event_at: match.event_at, event_id: match.event_id, open_source: match.open_source || null }
      : { present: false, event_at: null, event_id: null, open_source: null };
  };
  return {
    sent: pick(['sent']),
    delivered: pick(['delivered']),
    opened: pick(['opened', 'open', 'opened_proxy']),
    clicked: pick(['clicked', 'click']),
    soft_bounce: pick(['soft_bounce']),
    hard_bounce: pick(['hard_bounce', 'bounce']),
    replied: pick(['replied', 'reply']),
    unsubscribed: pick(['unsubscribed']),
  };
}

function firstMissingLink(checks) {
  for (const step of EVIDENCE_CHAIN) {
    if (!checks[step.key]) return step.label;
  }
  return null;
}

async function queryEmailEvents(db, { messageIds, recipientEmail, clientId }) {
  if (!(await tableExists(db, 'email_events'))) return [];
  const result = await db.query(`
    SELECT id, event_id, prospect_id, client_id, recipient_email, event_type, subject_line,
           brevo_message_id, event_at, open_source::text AS open_source
    FROM email_events
    WHERE client_id = $1
      AND (
        LOWER(recipient_email) = LOWER($2)
        OR brevo_message_id = ANY($3::text[])
      )
    ORDER BY event_at ASC NULLS LAST, id ASC
  `, [clientId, recipientEmail, messageIds.variants]);
  return result.rows;
}

async function queryExecutionRecord(db, executionId) {
  if (!(await tableExists(db, 'acquisition_mission_outbound_executions'))) return null;
  const result = await db.query(
    'SELECT * FROM acquisition_mission_outbound_executions WHERE id = $1 LIMIT 1',
    [executionId]
  );
  return result.rows[0] || null;
}

async function queryProviderEvents(db, { executionId, missionId, messageIds }) {
  if (!(await tableExists(db, 'acquisition_mission_provider_events'))) return [];
  const result = await db.query(`
    SELECT id, mission_id, tenant_id, prospect_id, execution_record_id, provider_message_id,
           event_type, event_category, raw_event_type, occurred_at, created_at
    FROM acquisition_mission_provider_events
    WHERE execution_record_id = $1
       OR (mission_id = $2 AND provider_message_id = ANY($3::text[]))
    ORDER BY occurred_at ASC NULLS LAST, created_at ASC
  `, [executionId, missionId, messageIds.variants]);
  return result.rows;
}

async function queryObservations(db, { missionId, executionId, messageIds }) {
  if (!(await tableExists(db, 'acquisition_mission_observations'))) return [];
  const result = await db.query(`
    SELECT id, mission_id, tenant_id, specialist, observation, payload, at
    FROM acquisition_mission_observations
    WHERE mission_id = $1
      AND (
        payload->'evidence'->>'executionRecordId' = $2
        OR payload->'evidence'->>'providerMessageId' = ANY($3::text[])
      )
    ORDER BY at ASC NULLS LAST
  `, [missionId, executionId, messageIds.variants]);
  return result.rows;
}

async function querySignals(db, { recipientEmail, clientId, missionId, executionId, emailEventIds }) {
  if (!(await tableExists(db, 'prospect_signal_events'))) return [];
  const params = [clientId, missionId, executionId];
  let emailClause = '';
  if (emailEventIds.length) {
    params.push(emailEventIds);
    emailClause = `OR (source = 'brevo' AND source_record_id = ANY($${params.length}::text[]))`;
  }
  const result = await db.query(`
    SELECT id, client_id, prospect_id, event_type, event_timestamp, source, source_record_id, metadata
    FROM prospect_signal_events
    WHERE client_id = $1
      AND (
        metadata->>'mission_id' = $2
        OR metadata->>'execution_record_id' = $3
        ${emailClause}
      )
    ORDER BY event_timestamp ASC NULLS LAST, id ASC
  `, params);

  if (result.rows.length) return result.rows;

  const prospect = await queryProspect(db, recipientEmail, clientId);
  if (!prospect) return [];
  const byProspect = await db.query(`
    SELECT id, client_id, prospect_id, event_type, event_timestamp, source, source_record_id, metadata
    FROM prospect_signal_events
    WHERE client_id = $1
      AND prospect_id = $2
      AND source = 'brevo'
      AND event_timestamp >= NOW() - INTERVAL '7 days'
    ORDER BY event_timestamp ASC NULLS LAST, id ASC
  `, [clientId, prospect.id]);
  return byProspect.rows;
}

async function queryTouchpoints(db, { prospectId, clientId, messageIds }) {
  if (!prospectId || !(await tableExists(db, 'touchpoints'))) return [];
  const result = await db.query(`
    SELECT id, prospect_id, channel, action_type, content_summary, external_ref, created_at
    FROM touchpoints
    WHERE prospect_id = $1
      AND client_id = $2
      AND channel = 'email'
      AND (
        external_ref = ANY($3::text[])
        OR action_type IN ('email_opened', 'email_opened_human', 'email_opened_proxy', 'email_opened_unknown', 'email_clicked', 'email_bounced', 'email_reply')
      )
      AND created_at >= NOW() - INTERVAL '7 days'
    ORDER BY created_at ASC
  `, [prospectId, clientId, messageIds.variants]);
  return result.rows;
}

async function queryProspect(db, email, clientId) {
  const result = await db.query(`
    SELECT p.id, p.client_id, p.email, p.first_name, p.last_name, p.company_id,
           c.name AS company_name, c.id AS company_uuid
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.client_id = $1
      AND LOWER(p.email) = LOWER($2)
    ORDER BY p.created_at DESC
    LIMIT 1
  `, [clientId, email]);
  return result.rows[0] || null;
}

async function queryBackusCandidate(db, clientId, missionId, executionProspectId) {
  const companyByName = await db.query(`
    SELECT id, name, client_id
    FROM companies
    WHERE client_id = $1
      AND name ILIKE '%backus%'
    ORDER BY created_at DESC
    LIMIT 5
  `, [clientId]);

  let missionBound = null;
  if (executionProspectId) {
    const byKey = await db.query(`
      SELECT id, name, client_id
      FROM companies
      WHERE client_id = $1
        AND id::text = $2
      LIMIT 1
    `, [clientId, String(executionProspectId)]);
    missionBound = byKey.rows[0] || null;
  }

  return {
    executionProspectId: executionProspectId || null,
    missionBoundCompany: missionBound,
    backusCompanies: companyByName.rows,
  };
}

async function queryAgentLog(db, { recipientEmail, clientId, messageIds, executionId }) {
  if (!(await tableExists(db, 'agent_log'))) return { brevoEvents: [], emmettSends: [], correlationFailures: [] };
  const [brevoEvents, emmettSends, correlationFailures] = await Promise.all([
    db.query(`
      SELECT id, action, prospect_id, payload, status, ran_at
      FROM agent_log
      WHERE agent_name = 'riley'
        AND action = 'brevo_event_received'
        AND client_id = $1
        AND (
          LOWER(payload->>'recipient_email') = LOWER($2)
          OR payload->>'event_type' IS NOT NULL
        )
        AND ran_at >= NOW() - INTERVAL '7 days'
      ORDER BY ran_at ASC
      LIMIT 50
    `, [clientId, recipientEmail]),
    db.query(`
      SELECT id, action, prospect_id, payload, status, ran_at
      FROM agent_log
      WHERE agent_name = 'emmett'
        AND action = 'email_sent'
        AND client_id = $1
        AND (
          payload->>'message_id' = ANY($2::text[])
          OR payload->>'execution_record_id' = $3
        )
      ORDER BY ran_at DESC
      LIMIT 10
    `, [clientId, messageIds.variants, executionId]),
    db.query(`
      SELECT id, action, prospect_id, payload, status, ran_at
      FROM agent_log
      WHERE agent_name = 'riley'
        AND action = 'brevo_correlation_failed'
        AND client_id = $1
        AND ran_at >= NOW() - INTERVAL '7 days'
      ORDER BY ran_at DESC
      LIMIT 20
    `, [clientId]),
  ]);

  return {
    brevoEvents: brevoEvents.rows,
    emmettSends: emmettSends.rows,
    correlationFailures: correlationFailures.rows.filter((row) => {
      const payload = row.payload && typeof row.payload === 'object' ? row.payload : {};
      const msg = String(payload.provider_message_id || '');
      return messageIds.variants.some((variant) => msg.includes(variant.replace(/^<|>$/g, '')))
        || String(payload.recipient_email || '').toLowerCase() === recipientEmail;
    }),
  };
}

async function queryMission(db, missionId, tenantId) {
  if (!(await tableExists(db, 'acquisition_missions'))) return null;
  const result = await db.query(
    'SELECT id, tenant_id, client_id, stage, status, objective, updated_at FROM acquisition_missions WHERE id = $1 LIMIT 1',
    [missionId]
  );
  return result.rows[0] || null;
}

async function run(options = {}) {
  const args = options.missionId != null || options.confirmProduction
    ? {
      missionId: options.missionId || DEFAULTS.MISSION_ID,
      executionId: options.executionId || DEFAULTS.EXECUTION_ID,
      recipientEmail: (options.recipientEmail || DEFAULTS.RECIPIENT_EMAIL).trim().toLowerCase(),
      brevoMessageId: options.brevoMessageId || DEFAULTS.BREVO_MESSAGE_ID,
      clientId: options.clientId || DEFAULTS.CLIENT_ID,
      tenantId: options.tenantId || DEFAULTS.TENANT_ID,
    }
    : parseArgs();

  if (!options.confirmProduction && !options.missionId && !process.argv.includes('--confirm-production')) {
    throw Object.assign(new Error('Refusing without --confirm-production.'), { code: 'confirm_production_required' });
  }

  const db = options.pool || pool;
  const messageIds = normalizeMessageId(args.brevoMessageId);

  const tablePresence = {};
  for (const entry of Object.values(PERSISTENCE_MODEL)) {
    tablePresence[entry.table] = await tableExists(db, entry.table);
  }

  const executionRow = await queryExecutionRecord(db, args.executionId);
  const execution = executionRow
    ? {
      id: executionRow.id,
      mission_id: executionRow.mission_id,
      tenant_id: executionRow.tenant_id,
      prospect_id: executionRow.prospect_id,
      status: executionRow.status,
      provider: executionRow.provider,
      provider_message_id: executionRow.provider_message_id,
      prepared_artifact_revision: executionRow.prepared_artifact_revision,
      attempted_at: executionRow.attempted_at,
      sent_at: executionRow.sent_at,
      execution_identity: executionRow.execution_identity,
      payload: executionRow.payload,
    }
    : null;

  const emailEvents = await queryEmailEvents(db, {
    messageIds,
    recipientEmail: args.recipientEmail,
    clientId: args.clientId,
  });
  const lifecycle = lifecycleFromEmailEvents(emailEvents);

  const providerEvents = await queryProviderEvents(db, {
    executionId: args.executionId,
    missionId: args.missionId,
    messageIds,
  });
  const observations = await queryObservations(db, {
    missionId: args.missionId,
    executionId: args.executionId,
    messageIds,
  });
  const emailEventIds = emailEvents.map((row) => row.event_id).filter(Boolean);
  const signals = await querySignals(db, {
    recipientEmail: args.recipientEmail,
    clientId: args.clientId,
    missionId: args.missionId,
    executionId: args.executionId,
    emailEventIds,
  });

  const prospect = await queryProspect(db, args.recipientEmail, args.clientId);
  const touchpoints = await queryTouchpoints(db, {
    prospectId: prospect?.id || execution?.prospect_id || null,
    clientId: args.clientId,
    messageIds,
  });
  const candidate = await queryBackusCandidate(
    db,
    args.clientId,
    args.missionId,
    execution?.prospect_id || null
  );
  const agentLog = await queryAgentLog(db, {
    recipientEmail: args.recipientEmail,
    clientId: args.clientId,
    messageIds,
    executionId: args.executionId,
  });
  const mission = await queryMission(db, args.missionId, args.tenantId);

  const brevoWebhookIngestionExists = tablePresence.email_events === true
    && (emailEvents.length > 0 || agentLog.brevoEvents.length > 0);

  const missionLinkedEmailEvents = emailEvents.filter((row) => {
    // email_events has no mission_id column; linkage is via prospect + message id + execution correlation
    return row.brevo_message_id && messageIds.variants.includes(row.brevo_message_id);
  });
  const missionLinkedProviderEvents = providerEvents.filter((row) => row.mission_id === args.missionId);
  const missionLinkedSignals = signals.filter((row) => {
    const metadata = row.metadata && typeof row.metadata === 'object' ? row.metadata : {};
    return metadata.mission_id === args.missionId || metadata.execution_record_id === args.executionId;
  });

  const executionProspectId = execution?.prospect_id ? String(execution.prospect_id) : null;
  const prospectLinked = Boolean(
    prospect
    && emailEvents.some((row) => String(row.prospect_id) === String(prospect.id))
  );
  const candidateLinked = Boolean(
    candidate.missionBoundCompany
    || (prospect && candidate.backusCompanies.some((c) => String(c.id) === String(prospect.company_id)))
    || (executionProspectId && candidate.backusCompanies.some((c) => String(c.id) === executionProspectId))
  );

  const maxMissionEvidenceVisible = observations.length > 0 || missionLinkedProviderEvents.length > 0;
  const maxSignalsVisible = missionLinkedSignals.length > 0
    || signals.some((row) => ['email_sent', 'email_delivered', 'email_human_opened', 'email_proxy_opened', 'email_unknown_opened'].includes(row.event_type));

  const checks = {
    outbound_execution_record: Boolean(execution),
    provider_message_id_on_execution: Boolean(execution?.provider_message_id),
    email_event_sent: lifecycle.sent.present || lifecycle.delivered.present,
    email_event_delivered: lifecycle.delivered.present,
    email_event_opened: lifecycle.opened.present,
    mission_provider_events: missionLinkedProviderEvents.length > 0,
    mission_observations: observations.length > 0,
    max_signal_events: maxSignalsVisible,
    prospect_linkage: prospectLinked || Boolean(prospect),
    mission_linkage: execution?.mission_id === args.missionId || missionLinkedProviderEvents.length > 0,
    execution_linkage: missionLinkedProviderEvents.every((row) => row.execution_record_id === args.executionId)
      && (execution?.id === args.executionId),
  };

  if (execution && !checks.provider_message_id_on_execution) {
    checks.provider_message_id_on_execution = messageIds.variants.some(
      (variant) => variant === execution.provider_message_id
    );
  }

  const missingLink = firstMissingLink(checks);
  const chainStatus = missingLink ? (checks.outbound_execution_record ? 'partial' : 'missing') : 'complete';

  return {
    audit: 'anchor_outbound_evidence',
    readOnly: true,
    targets: {
      missionId: args.missionId,
      executionId: args.executionId,
      recipientEmail: args.recipientEmail,
      brevoMessageId: messageIds.raw,
      brevoMessageIdVariants: messageIds.variants,
      clientId: args.clientId,
      tenantId: args.tenantId,
    },
    persistenceModel: PERSISTENCE_MODEL,
    tablesPresent: tablePresence,
    brevoWebhookIngestion: {
      exists: brevoWebhookIngestionExists,
      primaryStore: PERSISTENCE_MODEL.brevo_webhook_events.table,
      ingestionPath: 'POST /webhooks/brevo → insertBrevoEvent → email_events (+ mission provider events when canonically correlated)',
      agentLogRowsForRecipient: agentLog.brevoEvents.length,
    },
    outboundSend: execution,
    providerMessageId: {
      expected: messageIds.raw,
      onExecutionRecord: execution?.provider_message_id || null,
      matchesExpected: messageIds.variants.includes(execution?.provider_message_id || ''),
      emailEventValues: [...new Set(emailEvents.map((row) => row.brevo_message_id).filter(Boolean))],
    },
    lifecycle: {
      sent: lifecycle.sent,
      delivered: lifecycle.delivered,
      opened: lifecycle.opened,
      clicked: lifecycle.clicked,
      soft_bounce: lifecycle.soft_bounce,
      hard_bounce: lifecycle.hard_bounce,
      replied: lifecycle.replied,
    },
    emailEvents: emailEvents.map(summarizeEmailEvent),
    missionProviderEvents: providerEvents.map(summarizeProviderEvent),
    missionObservations: observations.map(summarizeObservation),
    maxSignalEvents: signals.map(summarizeSignal),
    touchpoints: touchpoints.map((row) => ({
      id: row.id,
      action_type: row.action_type,
      external_ref: row.external_ref,
      created_at: row.created_at,
    })),
    mission: mission
      ? {
        id: mission.id,
        stage: mission.stage,
        status: mission.status,
        tenant_id: mission.tenant_id,
        updated_at: mission.updated_at,
      }
      : null,
    candidateCompanyLinkage: {
      crmProspect: prospect
        ? {
          id: prospect.id,
          email: prospect.email,
          name: [prospect.first_name, prospect.last_name].filter(Boolean).join(' ') || null,
          company_id: prospect.company_id,
          company_name: prospect.company_name,
        }
        : null,
      executionProspectId,
      missionBoundCompany: candidate.missionBoundCompany,
      backusCompanyMatches: candidate.backusCompanies,
      linkedToBackusCandidate: candidateLinked,
    },
    linkage: {
      mission: {
        executionRecordMissionId: execution?.mission_id || null,
        providerEventsOnMission: missionLinkedProviderEvents.length,
        observationsOnMission: observations.length,
        ok: checks.mission_linkage,
      },
      execution: {
        providerEventsOnExecution: providerEvents.filter((row) => row.execution_record_id === args.executionId).length,
        ok: checks.execution_linkage,
      },
      prospect: {
        emailEventsWithProspectId: emailEvents.filter((row) => row.prospect_id).length,
        ok: checks.prospect_linkage,
      },
    },
    maxVisibility: {
      missionEvidenceInspection: maxMissionEvidenceVisible,
      prospectSignalEvents: maxSignalsVisible,
      observationCount: observations.length,
      missionProviderEventCount: missionLinkedProviderEvents.length,
      signalCount: signals.length,
    },
    agentLog: {
      emmettEmailSent: agentLog.emmettSends.map((row) => ({
        id: row.id,
        prospect_id: row.prospect_id,
        message_id: row.payload?.message_id || null,
        ran_at: row.ran_at,
      })),
      brevoCorrelationFailures: agentLog.correlationFailures.map((row) => ({
        id: row.id,
        reason: row.payload?.reason || null,
        provider_message_id: row.payload?.provider_message_id || null,
        recipient_email: row.payload?.recipient_email || null,
        ran_at: row.ran_at,
      })),
    },
    evidenceChainChecks: checks,
    firstMissingLink: missingLink,
    chainStatus,
    railwayCommand: 'node scripts/auditAnchorOutboundEvidence.js --confirm-production',
    completedAt: new Date().toISOString(),
  };
}

module.exports = {
  run,
  parseArgs,
  DEFAULTS,
  PERSISTENCE_MODEL,
  normalizeMessageId,
};

if (require.main === module) {
  run()
    .then((report) => {
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = report.chainStatus === 'complete' ? 0 : 2;
    })
    .catch((err) => {
      console.log(JSON.stringify({ error: { code: err.code, message: err.message } }, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}
