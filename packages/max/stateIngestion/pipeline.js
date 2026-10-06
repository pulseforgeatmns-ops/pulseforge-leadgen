'use strict';

const { SOURCE_TYPES, RESOLUTION, SAFETY_CLASS } = require('./types');
const { newIngestionId, claimFingerprint } = require('./fingerprints');
const { extractClaims } = require('./claimParser');
const { resolveClaim } = require('./entityResolver');
const { buildMutationsFromClaims, reconcileOwnership } = require('./reconciliation');
const { commitMutations } = require('./commit');
const { emptyTelemetry, bump } = require('./telemetry');
const { formatIngestionReceipt } = require('./receipt');
const { buildDownstreamEffects } = require('./propagation');
const { expectationFromMutations } = require('./expectations');
const { MemoryStateStore } = require('./store/memoryStore');
const {
  interpretConversationalInput,
  interpretWithDurableConversationContext,
  isTrustedStructuredInput,
  conversationalText,
  mergeUnderstandingTelemetry,
  mergeConversationMemoryTelemetry,
} = require('../understanding');

function claimKey(claim) {
  return `${claim.claim_type}:${JSON.stringify(claim.payload)}:${JSON.stringify(claim.source_record || null)}`;
}

function classifySafety(claim) {
  if (claim.claim_type === 'OWNERSHIP') return SAFETY_CLASS.C;
  if (['PIPELINE_IMPLICATION', 'OPERATOR_CORRECTION'].includes(claim.claim_type)) return SAFETY_CLASS.B;
  return SAFETY_CLASS.A;
}

async function processClaimSet({
  claims,
  store,
  ingestionId,
  clientId,
  sourceType,
  sourceActor,
  artifactId,
  input,
  telemetry,
  operatorCorrection,
}) {
  bump(telemetry, 'claims_extracted', claims.length);

  const context = store.snapshotContext();
  const bindings = { ao: null, account: null, contact: null };
  const resolutions = {};

  for (const claim of claims) {
    claim._key = claimKey(claim);
    claim.safety_class = classifySafety(claim);
  }

  async function persistResolvedClaim(claim, resolution) {
    const fp = claimFingerprint({
      ingestionId,
      claimType: claim.claim_type,
      payload: claim.payload,
      sourceRecord: claim.source_record,
    });
    const applied = await store.findAppliedClaim(fp);
    if (applied) {
      resolution.status = RESOLUTION.ALREADY_APPLIED;
      bump(telemetry, 'duplicate_claims_suppressed');
    }
    const persistedClaim = {
      ingestion_id: ingestionId,
      claim_type: claim.claim_type,
      claim_fingerprint: fp,
      payload: claim.payload,
      resolution_status: resolution.status,
      resolution,
      safety_class: claim.safety_class,
      source_record: claim.source_record || null,
    };
    await store.persistClaim(persistedClaim);
    claim.id = persistedClaim.id;
    if (resolution.status === RESOLUTION.RESOLVED || resolution.status === RESOLUTION.PROVISIONALLY_RESOLVED) {
      bump(telemetry, 'claims_resolved');
    }
    if (resolution.status === RESOLUTION.AMBIGUOUS) bump(telemetry, 'claims_ambiguous');
    if (resolution.status === RESOLUTION.CONFLICT) bump(telemetry, 'claims_conflicted');
  }

  for (const claim of claims.filter(c => ['AO', 'ACCOUNT'].includes(c.claim_type))) {
    const resolution = resolveClaim(claim, context, bindings);
    resolutions[claim._key] = resolution;
    if (claim.claim_type === 'AO') bindings.ao = resolution;
    if (claim.claim_type === 'ACCOUNT') bindings.account = resolution;
    await persistResolvedClaim(claim, resolution);
  }

  for (const claim of claims.filter(c => !['AO', 'ACCOUNT'].includes(c.claim_type))) {
    const resolution = resolveClaim(claim, context, bindings);
    resolutions[claim._key] = resolution;
    if (claim.claim_type === 'CONTACT' && resolution.entity) bindings.contact = resolution;
    if (claim.claim_type === 'RELATIONSHIP' && resolution.entity) bindings.contact = resolution;
    await persistResolvedClaim(claim, resolution);
  }

  const accountEntity = bindings.account?.entity;
  let existingProspect = null;
  if (accountEntity) {
    if (accountEntity.kind === 'prospect' || accountEntity.company_name) {
      existingProspect = accountEntity;
    } else {
      existingProspect = context.prospects.find(p => p.company_id === accountEntity.id) || null;
      if (existingProspect && !existingProspect.company_name) {
        existingProspect.company_name = accountEntity.name;
      }
    }
  }

  if (bindings.account?.entity && existingProspect) bump(telemetry, 'entities_reconciled');

  const ownershipClaim = claims.find(c => c.claim_type === 'OWNERSHIP');
  if (ownershipClaim && bindings.ao?.entity && existingProspect) {
    const own = reconcileOwnership(existingProspect.assigned_ao_id, bindings.ao.entity);
    if (own.conflict) {
      bindings.ownershipConflict = { claim: ownershipClaim, conflict: own.conflict };
      await store.persistConflict({
        ingestion_id: ingestionId,
        conflict_type: 'ownership',
        existing_state: own.conflict.existing,
        incoming_state: own.conflict.incoming,
      });
      bump(telemetry, 'claims_conflicted');
    }
  }

  const { mutations, conflicts, unresolved } = buildMutationsFromClaims({
    claims,
    resolutions,
    bindings,
    sourceType,
    existingProspect,
    operatorCorrection: Boolean(operatorCorrection),
  });

  for (const mutation of mutations) {
    mutation.ingestion_id = ingestionId;
  }

  const sourceRecord = claims.find(c => c.source_record)?.source_record || input.artifact?.metadata || null;
  const commitResults = await commitMutations({
    store,
    mutations,
    ingestionId,
    artifactId,
    clientId,
    aoId: bindings.ao?.entity?.id || null,
    sourceRecord,
    telemetry,
    bump,
  });

  let prospect = existingProspect;
  const createdProspect = commitResults.find(r => r.created?.prospect)?.created?.prospect;
  if (createdProspect) prospect = createdProspect;

  const expectationMutations = mutations.filter(m =>
    m.entity_type === 'expectation' || m.field_name === 'next_expected_event'
  );
  if (prospect && expectationMutations.length) {
    await store.createExpectation(
      expectationFromMutations(mutations, {
        clientId,
        prospectId: prospect.id,
        aoId: bindings.ao?.entity?.id || null,
        ingestionId,
        sourceEvidence: {
          account_name: prospect.company_name,
          source_record: sourceRecord,
          artifact_id: artifactId,
        },
      })
    );
  }

  for (const mutation of mutations) {
    if (!prospect || !mutation.claim) continue;
    await store.persistEvidenceLink({
      client_id: clientId,
      entity_type: 'prospect',
      entity_id: prospect.id,
      field_name: mutation.field_name,
      ingestion_id: ingestionId,
      artifact_id: artifactId,
      source_record: mutation.claim.source_record || sourceRecord,
      derivation: mutation.field_name === 'relationship_active' ? 'derived_from_pipeline_implication' : null,
      confidence: resolutions[mutation.claim._key]?.confidence || null,
    });
  }

  const downstream_effects = buildDownstreamEffects({
    prospect,
    expectations: await store.listOpenExpectations({ clientId }),
    aoName: bindings.ao?.entity?.name || null,
  });

  const duplicateReplay = claims.length > 0 && claims.every(c => resolutions[c._key]?.status === RESOLUTION.ALREADY_APPLIED);

  return {
    claims,
    resolutions,
    mutations,
    conflicts,
    unresolved,
    downstream_effects,
    prospect,
    duplicateReplay,
    bindings,
    commitResults,
  };
}

async function ingestOperationalUpdate(input = {}) {
  const store = input.store || new MemoryStateStore({ clientId: input.clientId });
  const telemetry = emptyTelemetry();
  bump(telemetry, 'ingestions_received');

  const ingestionId = input.ingestionId || newIngestionId();
  const clientId = input.clientId;
  const sourceType = input.sourceType || SOURCE_TYPES.OPERATOR_REPORTED;
  const sourceActor = input.sourceActor || null;
  const rawSource = {
    text: input.text || input.message || null,
    structured: input.structured || null,
    artifact: input.artifact || null,
  };

  const ingestionRecord = {
    id: ingestionId,
    client_id: clientId,
    source_type: sourceType,
    source_actor: sourceActor,
    raw_source: rawSource,
    received_at: (input.now || new Date()).toISOString(),
    telemetry: {},
    pipeline_audit: {},
  };
  await store.persistIngestion(ingestionRecord);

  let artifactId = null;
  if (input.artifact) {
    const artifact = {
      id: input.artifact.id || undefined,
      ingestion_id: ingestionId,
      artifact_type: input.artifact.artifact_type || 'message',
      filename: input.artifact.filename || null,
      metadata: input.artifact.metadata || {},
      raw_content: input.artifact.raw_content || rawSource,
    };
    const saved = await store.persistArtifact(artifact);
    artifactId = saved.id;
    bump(telemetry, 'evidence_artifacts_created');
  }

  let situationModel = null;
  let understandingValidation = null;
  let understandingPreview = null;
  let conversationMemory = input.conversationMemory || null;
  const useUnderstanding = !isTrustedStructuredInput(input) && conversationalText(input);

  if (useUnderstanding) {
    const interpretInput = {
      text: input.text,
      message: input.message,
      inputId: ingestionId,
      conversationId: input.conversationId,
      actor: input.actor || { userId: sourceActor },
      now: input.now,
      memory: input.memory,
      conversationMemory,
      contextAccounts: input.contextAccounts,
      tenantId: clientId,
      clientId,
      memoryRepository: input.memoryRepository || null,
      telemetry,
    };
    const interpreted = input.memoryRepository && input.conversationId
      ? await interpretWithDurableConversationContext(interpretInput)
      : interpretConversationalInput(interpretInput);
    situationModel = interpreted.situationModel;
    understandingValidation = interpreted.validation;
    understandingPreview = interpreted.preview;
    conversationMemory = interpreted.memory;
    if (interpreted.understandingTelemetry) {
      mergeUnderstandingTelemetry(telemetry, interpreted.understandingTelemetry);
    }
    if (interpreted.conversationMemoryTelemetry) {
      mergeConversationMemoryTelemetry(telemetry, interpreted.conversationMemoryTelemetry);
    }

    if (understandingValidation?.blockCommit) {
      ingestionRecord.telemetry = telemetry;
      ingestionRecord.receipt_summary = formatIngestionReceipt({
        title: 'Understanding blocked pending clarification',
        held: 1,
        summaryLines: [understandingValidation.narrowestClarification || 'Clarification required before commit.'],
        unresolvedCount: 1,
      });
      ingestionRecord.pipeline_audit = {
        situation_model: situationModel,
        understanding_validation: understandingValidation,
        understanding_preview: understandingPreview,
        commit_blocked: true,
      };
      return {
        ingestion_id: ingestionId,
        receipt: ingestionRecord.receipt_summary,
        telemetry,
        claims: [],
        resolutions: {},
        mutations: [],
        conflicts: [],
        unresolved: understandingValidation.materialAmbiguities || [],
        downstream_effects: [],
        prospect: null,
        duplicateReplay: false,
        entities_created: 0,
        entities_reconciled: 0,
        verification_failures: 0,
        situation_model: situationModel,
        understanding_preview: understandingPreview,
        understanding_validation: understandingValidation,
        clarification_required: understandingValidation.narrowestClarification,
        commit_blocked: true,
        understanding_diagnostics: situationModel?.diagnostics || null,
        conversation_memory: conversationMemory,
      };
    }
  }

  let threadResults = [];
  if (situationModel?.threads?.length) {
    for (const thread of situationModel.threads) {
      const claims = thread.ingestionClaims?.length ? thread.ingestionClaims : extractClaims({ text: thread.text });
      if (!claims.length) continue;
      const result = await processClaimSet({
        claims,
        store,
        ingestionId,
        clientId,
        sourceType,
        sourceActor,
        artifactId,
        input,
        telemetry,
        operatorCorrection: Boolean(input.operatorCorrection),
      });
      threadResults.push({ threadId: thread.threadId, accountName: thread.accountName, ...result });
    }
  } else {
    const claims = situationModel?.threads?.[0]?.ingestionClaims?.length
      ? situationModel.threads[0].ingestionClaims
      : extractClaims(input);
    threadResults.push(await processClaimSet({
      claims,
      store,
      ingestionId,
      clientId,
      sourceType,
      sourceActor,
      artifactId,
      input,
      telemetry,
      operatorCorrection: Boolean(input.operatorCorrection),
    }));
  }

  const primary = threadResults[0] || {};
  const claims = threadResults.flatMap(r => r.claims || []);
  const resolutions = threadResults.reduce((acc, r) => ({ ...acc, ...(r.resolutions || {}) }), {});
  const mutations = threadResults.flatMap(r => r.mutations || []);
  const conflicts = threadResults.flatMap(r => r.conflicts || []);
  const unresolved = threadResults.flatMap(r => r.unresolved || []);
  const downstream_effects = primary.downstream_effects || [];
  const prospect = primary.prospect || null;
  const duplicateReplay = threadResults.every(r => r.duplicateReplay);
  const bindings = primary.bindings || {};
  const commitResults = threadResults.flatMap(r => r.commitResults || []);

  const receipt = formatIngestionReceipt({
    title: input.batchParent?.filename
      ? `${input.batchParent.filename} row ingested`
      : `${bindings.ao?.entity?.name || sourceActor || 'Update'} ingested`,
    recordsExamined: input.batchParent ? 1 : null,
    held: (conflicts.length + unresolved.length) > 0 ? (conflicts.length + unresolved.length) : 0,
    summaryLines: prospect
      ? [
        `${prospect.company_name || 'Account'} ${bindings.account?.status === RESOLUTION.RESOLVED ? 'matched successfully' : 'recorded'}.`,
        threadResults.length > 1 ? `${threadResults.length} account threads processed.` : null,
        unresolved.length === 0 && conflicts.length === 0
          ? 'Safe operational claims committed and verified where applicable.'
          : null,
      ].filter(Boolean)
      : [],
    unresolvedCount: unresolved.length + conflicts.length,
  });

  ingestionRecord.telemetry = telemetry;
  ingestionRecord.receipt_summary = receipt;
  ingestionRecord.pipeline_audit = {
    situation_model: situationModel,
    understanding_validation: understandingValidation,
    understanding_preview: understandingPreview,
    extracted_claims: claims,
    entity_candidates: bindings,
    resolution_results: resolutions,
    proposed_mutations: mutations,
    committed_mutations: commitResults.map(r => r.mutation),
    unresolved_claims: unresolved,
    conflicts,
    downstream_effects,
    thread_results: threadResults.map(r => ({
      threadId: r.threadId,
      accountName: r.accountName,
      prospect_id: r.prospect?.id || null,
    })),
  };

  return {
    ingestion_id: ingestionId,
    receipt,
    telemetry,
    claims,
    resolutions,
    mutations,
    conflicts,
    unresolved,
    downstream_effects,
    prospect,
    prospects: threadResults.map(r => r.prospect).filter(Boolean),
    duplicateReplay,
    entities_created: telemetry.entities_created,
    entities_reconciled: telemetry.entities_reconciled,
    verification_failures: telemetry.verification_failures,
    situation_model: situationModel,
    understanding_preview: understandingPreview,
    understanding_validation: understandingValidation,
    understanding_diagnostics: situationModel?.diagnostics || null,
    conversation_memory: conversationMemory,
  };
}

module.exports = {
  ingestOperationalUpdate,
  processClaimSet,
};
