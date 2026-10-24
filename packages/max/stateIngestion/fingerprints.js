'use strict';

const crypto = require('crypto');

function stableHash(parts) {
  return crypto.createHash('sha256').update(parts.filter(Boolean).join('|')).digest('hex');
}

function newIngestionId() {
  return `ing_${stableHash([Date.now(), Math.random(), process.hrtime?.()?.join(':')]).slice(0, 24)}`;
}

function claimFingerprint({ ingestionId, claimType, payload, sourceRecord = null }) {
  const recordKey = sourceRecord
    ? `${sourceRecord.sheet || ''}:${sourceRecord.row || ''}:${sourceRecord.record_id || ''}`
    : '';
  return stableHash([
    ingestionId,
    claimType,
    JSON.stringify(payload || {}),
    recordKey,
  ]);
}

function appliedClaimFingerprint({ sourceArtifact, sourceRecord, targetEntityType, targetEntityId, claimType, normalizedValue }) {
  const sourceKey = sourceRecord?.file_hash
    || sourceRecord?.semantic_hash
    || sourceArtifact
    || '';
  return stableHash([
    sourceKey,
    sourceRecord?.sheet || '',
    sourceRecord?.row ?? '',
    sourceRecord?.record_id || '',
    targetEntityType || '',
    targetEntityId || '',
    claimType || '',
    JSON.stringify(normalizedValue ?? null),
  ]);
}

module.exports = {
  stableHash,
  newIngestionId,
  claimFingerprint,
  appliedClaimFingerprint,
};
