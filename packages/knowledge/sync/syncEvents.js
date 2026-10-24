'use strict';

/**
 * Production → knowledge sync source event types.
 * Producers emit these; GraphSyncEngine translates them into KnowledgeService ops.
 */
const SYNC_EVENTS = Object.freeze({
  COMPANY_UPSERTED: 'sync.company_upserted',
  PROSPECT_UPSERTED: 'sync.prospect_upserted',
  TOUCHPOINT_RECORDED: 'sync.touchpoint_recorded',
  IMPORT_BATCH_ITEM: 'sync.import_batch_item',
  ENTITY_MUTATED: 'sync.entity_mutated',
});

const SYNC_ENTITY_KINDS = Object.freeze({
  COMPANY: 'company',
  PROSPECT: 'prospect',
  TOUCHPOINT: 'touchpoint',
  IMPORT_COMPANY: 'import_company',
  IMPORT_PROSPECT: 'import_prospect',
});

module.exports = {
  SYNC_EVENTS,
  SYNC_ENTITY_KINDS,
};
