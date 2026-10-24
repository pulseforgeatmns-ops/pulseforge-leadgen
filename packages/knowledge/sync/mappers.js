'use strict';

const { KNOWLEDGE_EVENTS } = require('../events/KnowledgeEventBus');
const {
  companyNodeId,
  personNodeId,
  interactionNodeId,
  stableEvidenceId,
  syncIdempotencyKey,
} = require('./stableIds');
const { SYNC_EVENTS, SYNC_ENTITY_KINDS } = require('./syncEvents');

/**
 * Map a companies row (or import company payload) to a sync envelope.
 * @param {object} row
 * @param {string|number} row.id
 * @param {string|number} row.client_id
 * @param {string} [row.name]
 */
function mapCompanyRow(row, options = {}) {
  const tenantId = String(row.client_id);
  const entityId = String(row.id);
  const revision = row.updated_at || row.created_at || options.revision || 'v1';
  const nodeId = companyNodeId(tenantId, entityId);
  const sourceType = options.sourceType || 'crm_company';
  const sourceId = `company:${entityId}`;

  return {
    type: SYNC_EVENTS.COMPANY_UPSERTED,
    tenantId,
    id: syncIdempotencyKey(tenantId, SYNC_ENTITY_KINDS.COMPANY, entityId, String(revision)),
    occurredAt: row.updated_at || row.created_at || new Date().toISOString(),
    payload: {
      entityKind: SYNC_ENTITY_KINDS.COMPANY,
      entityId,
      revision: String(revision),
      knowledgeEvent: {
        type: KNOWLEDGE_EVENTS.COMPANY_OBSERVED,
        tenantId,
        payload: {
          nodeId,
          name: row.name || null,
          sourceType,
          sourceId,
          evidenceId: stableEvidenceId(tenantId, sourceType, sourceId),
          confidence: options.confidence == null ? 0.85 : options.confidence,
          metadata: {
            crmCompanyId: entityId,
            industry: row.industry || null,
            location: row.location || null,
            website: row.website || null,
            icpScore: row.icp_score == null ? null : Number(row.icp_score),
            importBatchId: options.importBatchId || null,
          },
        },
      },
    },
  };
}

/**
 * Map a prospects row to a sync envelope.
 */
function mapProspectRow(row, options = {}) {
  const tenantId = String(row.client_id);
  const entityId = String(row.id);
  const revision = row.updated_at || row.created_at || options.revision || 'v1';
  const nodeId = personNodeId(tenantId, entityId);
  const sourceType = options.sourceType || 'crm_prospect';
  const sourceId = `prospect:${entityId}`;
  const name = [row.first_name, row.last_name].filter(Boolean).join(' ').trim() || null;
  const companyGraphId = row.company_id
    ? companyNodeId(tenantId, row.company_id)
    : null;

  return {
    type: SYNC_EVENTS.PROSPECT_UPSERTED,
    tenantId,
    id: syncIdempotencyKey(tenantId, SYNC_ENTITY_KINDS.PROSPECT, entityId, String(revision)),
    occurredAt: row.updated_at || row.created_at || new Date().toISOString(),
    payload: {
      entityKind: SYNC_ENTITY_KINDS.PROSPECT,
      entityId,
      revision: String(revision),
      ensureCompanyFirst: companyGraphId
        ? {
            id: row.company_id,
            client_id: tenantId,
            name: row.company_name || null,
          }
        : null,
      knowledgeEvent: {
        type: KNOWLEDGE_EVENTS.PERSON_OBSERVED,
        tenantId,
        payload: {
          nodeId,
          name,
          email: row.email || null,
          title: row.job_title || null,
          companyId: companyGraphId,
          sourceType,
          sourceId,
          evidenceId: stableEvidenceId(tenantId, sourceType, sourceId),
          confidence: options.confidence == null ? 0.8 : options.confidence,
          metadata: {
            crmProspectId: entityId,
            phone: row.phone || null,
            icpScore: row.icp_score == null ? null : Number(row.icp_score),
            vertical: row.vertical || null,
            source: row.source || null,
            importBatchId: options.importBatchId || null,
          },
        },
      },
    },
  };
}

/**
 * Map a touchpoints row to a sync envelope.
 */
function mapTouchpointRow(row, options = {}) {
  const tenantId = String(row.client_id);
  const entityId = String(row.id);
  const revision = row.created_at || options.revision || 'v1';
  const nodeId = interactionNodeId(tenantId, entityId);
  const participantId = row.prospect_id
    ? personNodeId(tenantId, row.prospect_id)
    : null;

  return {
    type: SYNC_EVENTS.TOUCHPOINT_RECORDED,
    tenantId,
    id: syncIdempotencyKey(tenantId, SYNC_ENTITY_KINDS.TOUCHPOINT, entityId, String(revision)),
    occurredAt: row.created_at || new Date().toISOString(),
    payload: {
      entityKind: SYNC_ENTITY_KINDS.TOUCHPOINT,
      entityId,
      revision: String(revision),
      knowledgeEvent: {
        type: KNOWLEDGE_EVENTS.INTERACTION_RECORDED,
        tenantId,
        payload: {
          nodeId,
          channel: row.channel || null,
          actionType: row.action_type || null,
          summary: row.content_summary || null,
          occurredAt: row.created_at || null,
          participantId,
          metadata: {
            crmTouchpointId: entityId,
            outcome: row.outcome || null,
            sentiment: row.sentiment || null,
            agentId: row.agent_id || null,
            externalRef: row.external_ref || null,
          },
        },
      },
    },
  };
}

/**
 * Import batch item — company or prospect shaped payload with import metadata.
 */
function mapImportBatchItem(item, options = {}) {
  const importBatchId = options.importBatchId || item.import_batch_id || 'unknown-batch';
  if (item.kind === 'company' || item.entity_kind === 'company') {
    const event = mapCompanyRow(item, {
      ...options,
      importBatchId,
      sourceType: options.sourceType || 'import_company',
    });
    event.type = SYNC_EVENTS.IMPORT_BATCH_ITEM;
    event.id = syncIdempotencyKey(
      event.tenantId,
      SYNC_ENTITY_KINDS.IMPORT_COMPANY,
      item.id,
      `${importBatchId}:${event.payload.revision}`
    );
    event.payload.importBatchId = importBatchId;
    return event;
  }

  const event = mapProspectRow(item, {
    ...options,
    importBatchId,
    sourceType: options.sourceType || 'import_prospect',
  });
  event.type = SYNC_EVENTS.IMPORT_BATCH_ITEM;
  event.id = syncIdempotencyKey(
    event.tenantId,
    SYNC_ENTITY_KINDS.IMPORT_PROSPECT,
    item.id,
    `${importBatchId}:${event.payload.revision}`
  );
  event.payload.importBatchId = importBatchId;
  return event;
}

/**
 * Generic future mutation envelope.
 */
function mapEntityMutation(input) {
  if (!input.tenantId || !input.entityKind || !input.entityId) {
    throw new Error('mapEntityMutation requires tenantId, entityKind, entityId');
  }
  if (!input.knowledgeEvent || !input.knowledgeEvent.type) {
    throw new Error('mapEntityMutation requires knowledgeEvent.type');
  }
  const revision = input.revision || 'v1';
  return {
    type: SYNC_EVENTS.ENTITY_MUTATED,
    tenantId: String(input.tenantId),
    id:
      input.id ||
      syncIdempotencyKey(input.tenantId, input.entityKind, input.entityId, revision),
    occurredAt: input.occurredAt || new Date().toISOString(),
    payload: {
      entityKind: input.entityKind,
      entityId: String(input.entityId),
      revision: String(revision),
      knowledgeEvent: {
        ...input.knowledgeEvent,
        tenantId: String(input.tenantId),
      },
    },
  };
}

module.exports = {
  mapCompanyRow,
  mapProspectRow,
  mapTouchpointRow,
  mapImportBatchItem,
  mapEntityMutation,
};
