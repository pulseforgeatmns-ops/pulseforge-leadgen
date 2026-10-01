'use strict';

function normalizeSubmissionId(stored) {
  const submissionId = Number(stored?.id ?? stored?.submission_id ?? stored?.submissionId);
  if (!Number.isFinite(submissionId)) {
    throw new Error('walkthrough_submission_id_invalid');
  }
  return submissionId;
}

module.exports = {
  normalizeSubmissionId,
};
