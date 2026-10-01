'use strict';

const ak = require('../packages/acquisition-knowledge');
const { asText } = require('../packages/acquisition-mission/types');
const { unwrapSpecialistPayload } = require('../packages/acquisition-mission/ContributionSupersession');
const {
  findPaigeVariants,
  computePreparedArtifactRevision,
} = require('../packages/acquisition-mission/ExecutionApproval');
const { resolvePaigeVariant } = require('../packages/acquisition-mission/OutboundExecution');
const { CADENCE_PROVENANCE } = require('../packages/acquisition-mission/PreparedOutreachSequence');

async function loadMissionContributions(pool, missionId) {
  const { rows } = await pool.query(
    `SELECT id, mission_id, specialist, kind, payload, at
       FROM acquisition_mission_contributions
      WHERE mission_id = $1
      ORDER BY at ASC`,
    [String(missionId)],
  );
  return rows.map((row) => ({
    id: row.id,
    missionId: row.mission_id,
    specialist: row.specialist,
    kind: row.kind,
    payload: row.payload && typeof row.payload === 'object' ? row.payload : {},
    at: row.at,
  }));
}

function buildVerifiedPaigeOutreachEvidence({
  missionId,
  preparedArtifactRevision,
  contributions = [],
  candidateId,
  subject,
  body,
}) {
  const revision = computePreparedArtifactRevision(missionId, contributions);
  if (asText(revision) !== asText(preparedArtifactRevision)) {
    throw ak.knowledgeError(
      'governed_paige_lineage_revision_mismatch',
      'Prepared artifact revision does not match mission contributions.',
    );
  }
  const paige = findPaigeVariants(contributions);
  if (!paige?.id) {
    throw ak.knowledgeError(
      'governed_paige_lineage_missing',
      'No Paige variants contribution found for mission.',
    );
  }
  const paigePayload = unwrapSpecialistPayload(paige);
  const message = resolvePaigeVariant(paigePayload, { candidateId: String(candidateId) });
  if (!message
    || asText(message.subject) !== asText(subject)
    || asText(message.body) !== asText(body)) {
    throw ak.knowledgeError(
      'governed_paige_lineage_copy_mismatch',
      'Frozen outreach copy does not match verified Paige variant.',
    );
  }
  return [{
    id: `evidence_paige_${paige.id}`,
    type: ak.EVIDENCE_TYPES.OBSERVED,
    statement: `Executable outreach copy matches Paige contribution ${paige.id} at prepared revision ${preparedArtifactRevision}.`,
    source: {
      kind: 'paige_contribution',
      type: 'paige_contribution',
      ref: paige.id,
    },
    payload: {
      missionId: String(missionId),
      preparedArtifactRevision: asText(preparedArtifactRevision),
      candidateId: String(candidateId),
      contributionKind: paige.kind,
      provenance: CADENCE_PROVENANCE.PAIGE_CONTRIBUTION,
    },
  }];
}

async function resolveGovernedOutreachAssetEvidence(pool, { envelope, item }) {
  const contributions = await loadMissionContributions(pool, envelope.mission_id);
  return buildVerifiedPaigeOutreachEvidence({
    missionId: envelope.mission_id,
    preparedArtifactRevision: envelope.revision,
    contributions,
    candidateId: item.candidate_id,
    subject: item.snapshot.message.subject,
    body: item.snapshot.message.body,
  });
}

module.exports = {
  loadMissionContributions,
  buildVerifiedPaigeOutreachEvidence,
  resolveGovernedOutreachAssetEvidence,
};
