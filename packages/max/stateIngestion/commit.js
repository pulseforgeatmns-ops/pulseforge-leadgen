'use strict';

const { SAFETY_CLASS } = require('./types');
const { appliedClaimFingerprint } = require('./fingerprints');
async function commitMutations({
  store,
  mutations,
  ingestionId,
  artifactId = null,
  clientId,
  aoId = null,
  sourceRecord = null,
  telemetry,
  bump,
}) {
  const results = [];
  for (const mutation of mutations) {
    if (mutation.safety_class === SAFETY_CLASS.C) {
      mutation.commit_status = 'blocked';
      results.push({ committed: false, mutation });
      continue;
    }

    const fingerprint = appliedClaimFingerprint({
      sourceArtifact: artifactId,
      sourceRecord,
      targetEntityType: mutation.entity_type,
      targetEntityId: mutation.entity_id || mutation.intended_value?.company_name || '',
      claimType: mutation.claim?.claim_type || mutation.field_name,
      normalizedValue: mutation.intended_value,
    });

    const existing = await store.findAppliedClaim(fingerprint);
    if (existing) {
      mutation.commit_status = 'skipped_duplicate';
      bump(telemetry, 'duplicate_claims_suppressed');
      results.push({ committed: false, duplicate: true, mutation });
      continue;
    }

    bump(telemetry, 'mutations_proposed');
    const applied = await store.applyMutation(mutation);
    if (applied.committed) {
      bump(telemetry, 'mutations_committed');
      if (applied.created?.prospect) bump(telemetry, 'entities_created');
      if (applied.created?.company && !applied.created?.prospect) bump(telemetry, 'entities_created');
      await store.recordAppliedClaim({
        claim_fingerprint: fingerprint,
        ingestion_id: ingestionId,
        target_entity_type: mutation.entity_type,
        target_entity_id: mutation.entity_id || applied.created?.prospect?.id || null,
      });
    }

    const verified = await store.verifyMutation(applied.mutation);
    await store.persistMutation(verified);
    if (verified.verification_status === 'VERIFIED') {
      bump(telemetry, 'mutations_verified');
    } else if (verified.verification_status === 'COMMIT_VERIFICATION_FAILED') {
      bump(telemetry, 'verification_failures');
    }
    results.push({ ...applied, verification: verified });

  }
  return results;
}

module.exports = {
  commitMutations,
};
