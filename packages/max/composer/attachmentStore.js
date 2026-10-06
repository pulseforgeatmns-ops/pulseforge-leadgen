'use strict';

/** In-process attachment bytes keyed by storageRef (tenant-scoped). */
const blobs = new Map();

function storageRefFor(tenantId, attachmentId) {
  return `${tenantId}/${attachmentId}`;
}

function putAttachmentBuffer(tenantId, attachmentId, buffer, meta = {}) {
  const storageRef = storageRefFor(tenantId, attachmentId);
  blobs.set(storageRef, {
    tenantId: String(tenantId),
    buffer,
    meta,
    storedAt: new Date().toISOString(),
  });
  return storageRef;
}

function getAttachmentBuffer(storageRef, tenantId) {
  const entry = blobs.get(storageRef);
  if (!entry) return null;
  if (String(entry.tenantId) !== String(tenantId)) return null;
  return entry;
}

function clearAttachmentStore() {
  blobs.clear();
}

module.exports = {
  storageRefFor,
  putAttachmentBuffer,
  getAttachmentBuffer,
  clearAttachmentStore,
};
