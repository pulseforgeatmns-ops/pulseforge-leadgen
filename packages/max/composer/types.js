'use strict';

const { newComposerId } = require('./id');

/**
 * Transport contract for unified Max composer turns (MAX-INGEST-UX-001).
 * Not a semantic model — downstream MAX-UNDERSTANDING produces SituationModel.
 */

function createMaxAttachment(base = {}) {
  return {
    id: base.id || newComposerId('att'),
    type: base.type,
    filename: base.filename || null,
    mimeType: base.mimeType || null,
    storageRef: base.storageRef || null,
    extractedText: base.extractedText || null,
    structuredData: base.structuredData || null,
    transcription: base.transcription || null,
    extractionStatus: base.extractionStatus || 'pending',
    extractionEvidence: base.extractionEvidence || null,
  };
}

function createMaxIngestionEnvelope(base = {}) {
  const attachments = Array.isArray(base.attachments)
    ? base.attachments.map(a => (a.id ? a : createMaxAttachment(a)))
    : [];

  let sourceType = base.sourceType || base.source_type || null;
  if (!sourceType) {
    const hasText = Boolean(base.text && String(base.text).trim());
    const types = new Set(attachments.map(a => a.type));
    if (hasText && types.size) sourceType = 'mixed';
    else if (types.has('spreadsheet')) sourceType = 'spreadsheet';
    else if (types.has('voice')) sourceType = 'voice';
    else if (types.has('image')) sourceType = 'image';
    else if (types.has('document')) sourceType = 'file';
    else sourceType = 'text';
  }

  return {
    id: base.id || newComposerId('env'),
    tenantId: String(base.tenantId || base.tenant_id),
    conversationId: base.conversationId || base.conversation_id || null,
    actor: base.actor || {},
    text: base.text || null,
    attachments,
    sourceType,
    createdAt: base.createdAt || base.created_at || new Date().toISOString(),
    metadata: base.metadata || {},
  };
}

module.exports = {
  createMaxAttachment,
  createMaxIngestionEnvelope,
};
