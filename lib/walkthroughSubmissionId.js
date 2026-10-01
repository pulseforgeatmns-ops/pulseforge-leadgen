'use strict';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function normalizeSubmissionId(stored) {
  const raw = stored?.id ?? stored?.submission_id ?? stored?.submissionId;
  if (raw == null || raw === '') {
    throw new Error('walkthrough_submission_id_invalid');
  }
  const asString = String(raw).trim();
  if (UUID_RE.test(asString)) {
    return asString;
  }
  const submissionId = Number(asString);
  if (Number.isFinite(submissionId)) {
    return submissionId;
  }
  throw new Error('walkthrough_submission_id_invalid');
}

module.exports = {
  normalizeSubmissionId,
  UUID_RE,
};
