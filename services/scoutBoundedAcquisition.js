'use strict';
// Standalone acquisition only: no scheduler, recovery/enrichment loop, sending,
// or backlog dispatcher. Reuse the actual provider, page, ticket and admission path.
const { createPlacesProvider, buildQueriesForEvidence } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { createCheckpointStore, createCheckpointSession, queryIdentity } = require('./scoutQueryCheckpoints');
const { createPageCheckpointStore } = require('./scoutQueryContinuation');
const { epochRootIdentity, validateCoverageEpochConfig } = require('./scoutCoverageEpoch');
const { createCandidateTicketStore } = require('./scoutCandidateTickets');
const { claimedCompanies, candidateTicketDependencyKey } = require('./scoutCandidateTicketDispatch');
const { withPlacesContext } = require('../utils/placesCostAttribution');
const { withResearchProviderScope } = require('../utils/anchorResearchProviderPause');
const { GovernedOutboundStore } = require('./governedOutboundStore');
const { scoutSearchFingerprint, sourceScope, resolveScoutRampAllowedCities, _test: { persistDiscoveredCompanies } } = require('./maxOutboundControlLoop');
const { createBoundedBudget, transaction, lockRecovery, assertGrant, hash, error } = require('./scoutBoundedAcquisitionBudget');
const PROGRAM_AUTHORITY_COLUMNS = 'id,tenant_id,source_mission_id,policy,policy_hash,scope_hash,authorized_by,authorized_at,mode';
const HEX = /^[a-f0-9]{64}(?![\s\S])/;
function validateManifest(input) {
  if (require('node:util').types.isProxy(input)) throw error('bounded_manifest_invalid');
  const version = input && Object.getOwnPropertyDescriptor(input, 'version');
  if (version && !Object.hasOwn(version, 'value')) throw error('bounded_manifest_invalid');
  const research = version?.value === 2;
  if (research) require('./scoutBoundedResearchAuthority').validateJsonDocument(input);
  const keys = ['version','approvalRef','tenantId','programId','missionId','programHash','sourceHash','fingerprint',
    'query','rootQueryKey','evidenceRequest','grant','maxModeledUsdMicros', ...(research ? ['researchAuthority'] : [])];
  if (!input || Object.keys(input).length !== keys.length || keys.some(k => !Object.hasOwn(input,k))
    || ![1,2].includes(input.version) || input.tenantId !== '10'
    || !/^[a-zA-Z0-9_.:-]{1,120}(?![\s\S])/.test(input.approvalRef)
    || !['programId','missionId','query'].every(k => typeof input[k] === 'string' && input[k].length > 0 && input[k].length < 512)
    || !['programHash','sourceHash','fingerprint','rootQueryKey'].every(k => HEX.test(input[k]))
    || !Number.isInteger(input.maxModeledUsdMicros) || input.maxModeledUsdMicros < 1 || input.maxModeledUsdMicros > 620000) throw error('bounded_manifest_invalid');
  const request = input.evidenceRequest;
  if (!request || request.tenantId !== input.tenantId || request.scope !== 'market' || request.evidenceType !== 'identity'
    || typeof request.segment !== 'string' || typeof request.discoveryConcept !== 'string'
    || !Array.isArray(request.geography?.cities) || request.geography.cities.length !== 1
    || typeof request.geography.cities[0] !== 'string' || !request.geography.cities[0].trim()
    || !(request.geography.state == null || typeof request.geography.state === 'string')
    || Object.keys(request).some(k => !['tenantId','scope','segment','evidenceType','discoveryConcept','geography'].includes(k))
    || Object.keys(request.geography).some(k => !['cities','state'].includes(k))) throw error('bounded_evidence_request_invalid');
  const queries = buildQueriesForEvidence({ ...request, cities:request.geography.cities, state:request.geography.state });
  if (queries.length !== 1 || queries[0] !== input.query || queryIdentity(input.query,false) !== input.rootQueryKey) throw error('bounded_root_mismatch');
  const grant = validateCoverageEpochConfig(input.grant, { tenantId:input.tenantId,fingerprint:input.fingerprint });
  if (grant?.version !== 2 || grant.maxNewPages !== 1 || grant.rootQueryKeys.length !== 1
    || grant.rootQueryKeys[0] !== input.rootQueryKey || grant.approvalRef !== input.approvalRef) throw error('bounded_grant_mismatch');
  if (research) require('./scoutBoundedResearchAuthority').validateResearchFields(input);
  const copy = JSON.parse(JSON.stringify(input));
  const manifest = { ...copy, grant, manifestHash:hash(copy), pageKey:epochRootIdentity(input.rootQueryKey) };
  // Persist/hash the approved document, never the normalized grant's derived fields.
  Object.defineProperty(manifest, 'approvalDocument', { value:copy });
  return manifest;
}
async function loadBoundSource(pool, manifest, lock = false) {
  const program = (await pool.query(`SELECT ${PROGRAM_AUTHORITY_COLUMNS} FROM acquisition_outbound_programs WHERE tenant_id=$1 AND id=$2${lock ? ' FOR SHARE' : ''}`, [manifest.tenantId,manifest.programId])).rows[0];
  const source = (await pool.query(`SELECT id,objective,target_segment,payload FROM acquisition_missions WHERE tenant_id=$1 AND id=$2${lock ? ' FOR SHARE' : ''}`, [manifest.tenantId,manifest.missionId])).rows[0];
  if (!program || !source || program.source_mission_id !== manifest.missionId || program.mode === 'revoked'
    || hash(program) !== manifest.programHash || hash(source) !== manifest.sourceHash
    || scoutSearchFingerprint(program,source) !== manifest.fingerprint) throw error('bounded_source_changed');
  const scope = sourceScope(source);
  if (!scope.cities.some(city => city.toLowerCase() === manifest.evidenceRequest.geography.cities[0].toLowerCase())) throw error('bounded_city_outside_mission');
  return { program, source, scope };
}
async function runBoundedAcquisition(args) {
  const manifest = validateManifest(args?.manifest);
  if (manifest.version === 2) return require('./scoutBoundedResearchAuthority').runWithResearchAuthority(args);
  return runBoundedAcquisitionInScope(args);
}
async function runBoundedAcquisitionInScope({ pool, manifest:input, apiKey, fetchImpl }) {
  const manifest = validateManifest(input);
  if (manifest.version === 2) require('./scoutBoundedResearchAuthority').assertResearchExecutionScope(manifest.manifestHash);
  if (!pool?.connect || !apiKey || typeof fetchImpl !== 'function' || pool.options?.max < 3
    || !(pool.options?.connectionTimeoutMillis > 0 && pool.options.connectionTimeoutMillis <= 2000)
    || !['statement_timeout','query_timeout'].every(key => pool.options?.[key] > 0 && pool.options[key] <= 30000)
    || !(pool.options?.lock_timeout > 0 && pool.options.lock_timeout <= 2000)) throw error('bounded_dependencies_required');
  return withResearchProviderScope(manifest.tenantId, async () => {
    const { program, source, scope } = await loadBoundSource(pool,manifest);
    const budget = createBoundedBudget({ pool,manifest,assertSource:db => loadBoundSource(db,manifest,true) });
    const tickets = createCandidateTicketStore({ pool,tenantId:manifest.tenantId,fingerprint:manifest.fingerprint,lockTimeoutMs:2000 });
    const pages = createPageCheckpointStore({ db:pool,tenantId:manifest.tenantId,fingerprint:manifest.fingerprint,coverageEpoch:manifest.grant });
    await budget.ready(); await tickets.assertReady(); await pages.assertReady();
    // Probe local admission dependencies before any provider call. This executes
    // unchanged read-only ownership checks, not schema-ensuring runtime helpers.
    const ownership = new GovernedOutboundStore(pool,manifest.tenantId);
    await ownership.candidateOwnership({ company:'', domain:'' });
    await pool.query('SELECT client_id,company,domain,website_url,vertical,location,source,enrichment_attempts,last_attempt_at,notes FROM scout_unenriched LIMIT 0');
    let claimed = false;
    try {
      // Plan under the legacy interlock or separately installed research authority. V2 allocation is
      // stable; a standalone attempt does not alter its cumulative root/page caps.
      const selected = await transaction(pool, async db => { await lockRecovery(db,manifest); return pages.plan(manifest.rootQueryKey); });
      if (selected.key !== manifest.pageKey || selected.index !== 0 || selected.pageToken || selected.row || selected.status
        || selected.coverageVersion !== 2) throw error('bounded_fresh_first_page_required');
      await budget.claim(); claimed = true;
      const scopedPages = { ...pages,
        plan:async root => { if (root !== manifest.rootQueryKey) throw error('bounded_unapproved_root'); return selected; },
        reserve:async plan => {
          if (plan.key !== manifest.pageKey || plan.index !== 0) throw error('bounded_page_mismatch');
          return transaction(pool, async db => { await lockRecovery(db,manifest); return pages.reserve(plan); });
        },
        // Even proven no-network denial cannot reopen this one-shot invocation.
        // Keep the page stop as well as the separate immutable run claim.
        fail:(plan,failure) => pages.fail(plan,{ ...failure,noNetwork:false }),
      };
      const session = createCheckpointSession({ store:createCheckpointStore({ db:pool,tenantId:manifest.tenantId,fingerprint:manifest.fingerprint }),
        ticketStore:tickets,pageStore:scopedPages });
      const provider = createPlacesProvider({ apiKey,tenantId:manifest.tenantId,queryCheckpointSession:session,useCache:false,
        fetchImpl:(url,options) => budget.transport(url,options,fetchImpl),
        placesApiDeps:{ db:pool,existingSchemaOnly:true,strictAttribution:true,reserveApiCall:budget.reserveApiCall,settleApiCall:budget.settleApiCall } });
      await withPlacesContext({ tenantId:manifest.tenantId,missionId:manifest.missionId,executionId:manifest.approvalRef,
        triggerMode:'operator',feature:'Discovery',caller:'ScoutBoundedAcquisition' }, () => provider.collectEvidence(manifest.evidenceRequest));
      if (!budget.healthy || provider.lastExecution?.errors.length || session.snapshot().fetchedQueries !== 1) throw error('bounded_acquisition_not_confirmed');
      const dependencyKey = candidateTicketDependencyKey({ tenantId:manifest.tenantId,program,source });
      // SQL restriction is applied BEFORE claim/attempt increments. Candidates
      // shared with any older query remain untouched by this admission pass.
      const claims = await transaction(pool, async db => {
        await lockRecovery(db,manifest);
        await assertGrant(db,manifest);
        await loadBoundSource(db,manifest,true);
        return tickets.claimDue({ dependencyKey,limit:20,exclusiveQueryKey:manifest.pageKey });
      });
      const pass = { renew:async refs => { await tickets.renew(refs); return true; },
        finish:(refs,outcome,write) => tickets.finish(refs,{ ...outcome,dependencyKey },write) };
      const admission = await transaction(pool, async db => {
        await lockRecovery(db,manifest);
        await assertGrant(db,manifest);
        await loadBoundSource(db,manifest,true);
        return persistDiscoveredCompanies(pool,ownership,{ tenantId:manifest.tenantId,
          companies:claimedCompanies(claims,manifest.tenantId),
          scoutContext:{ scope,source,allowedCities:resolveScoutRampAllowedCities(scope),serviceAreas:resolveScoutRampAllowedCities(scope),
            clientId:10,authorizedTenantId:'10',program,investigateBusinessEvidence:false,ticketPass:pass } });
      });
      const pageCounts=(await pool.query(`SELECT count(DISTINCT m.candidate_key)::int AS total,
        count(DISTINCT m.candidate_key) FILTER (WHERE EXISTS(SELECT 1 FROM scout_candidate_memberships old
          WHERE old.tenant_id=m.tenant_id AND old.search_fingerprint=m.search_fingerprint
          AND old.plan_version=m.plan_version AND old.ticket_version=m.ticket_version
          AND old.candidate_key=m.candidate_key AND old.query_key<>m.query_key))::int AS shared_old
        FROM scout_candidate_memberships m WHERE m.tenant_id=$1 AND m.search_fingerprint=$2 AND m.query_key=$3`,
      [manifest.tenantId,manifest.fingerprint,manifest.pageKey])).rows[0];
      const debits=(await pool.query(`SELECT operation,count(*)::int AS count,sum(reserved_usd_micros)::int AS micros
        FROM scout_bounded_acquisition_attempts WHERE approval_ref=$1 GROUP BY operation`,[manifest.approvalRef])).rows;
      await budget.finish(true);
      return { status:'completed', outcome:admission.inserted>0?'queued_new_candidates':'no_new_queue_admissions',
        rawSearchResults:provider.lastExecution.totals.results, distinctPageCandidates:pageCounts.total,
        sharedWithOlderQueries:pageCounts.shared_old, claimedNewPageOnlyCandidates:claims.length,
        unclaimedPageCandidates:pageCounts.total-claims.length,
        queuedForEnrichment:admission.inserted, cleanVerifiedContacts:0, otherProviderAttempts:0,
        admissionCounters:Object.fromEntries(Object.entries(admission.admission).filter(([,value])=>Number.isSafeInteger(value)&&value>=0)),
        reservedAttempts:Object.fromEntries(debits.map(row=>[row.operation,row.count])),
        modeledReservedUsdMicros:debits.reduce((sum,row)=>sum+row.micros,0) };

    } catch (failure) {
      if (claimed) await budget.finish(false).catch(() => {});
      // No implicit retry, page reset, raw-error/credential dump, or backlog drain.
      throw failure;
    }
  });
}
module.exports = { PROGRAM_AUTHORITY_COLUMNS, validateManifest, loadBoundSource, runBoundedAcquisition, runBoundedAcquisitionInScope };
