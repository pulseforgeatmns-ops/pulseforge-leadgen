'use strict';

const { createHash, randomUUID } = require('node:crypto');
const { PLAN_VERSION } = require('./scoutQueryCheckpoints');

const TICKET_VERSION = 'scout-candidate-ticket-v1';
const LEASE_MS = 15 * 60 * 1000;
const CRASH_RETRY_MS = 60 * 60 * 1000;
const MAX_CLAIM = 100;
const REASONS = Object.freeze({
  handled: ['queued', 'already_queued', 'already_usable', 'recovered', 'policy_excluded'],
  deferred: ['evidence_budget_deferred', 'unresolved_business_evidence', 'recovery_backoff', 'recovery_failed',
    'queue_retry_cooldown', 'queue_attempt_cooldown', 'contact_policy_hold', 'ownership_unresolved',
    'already_usable_observed', 'ownership_policy_observed', 'transient_business_evidence_failure'],
  blocked: ['contact_policy_hold', 'recovery_tenant_unsupported', 'provider_unavailable',
    'ownership_unresolved', 'identity_incomplete', 'queue_attempt_limit',
    'queue_unsupported_vertical', 'recovery_reservation_required', 'geography_unresolved',
    'unresolved_business_evidence', 'business_evidence_dependency', 'business_evidence_not_authorized'],
});
const SCOPE_SQL = 'tenant_id=$1 AND search_fingerprint=$2 AND plan_version=$3 AND ticket_version=$4';
const HEX = /^[a-f0-9]{64}$/;
const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[1-8][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i;

function fail(message, ticketCode) { return Object.assign(new Error(message), { ticketCode }); }
function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  const json = JSON.stringify(value);
  if (json === undefined || (typeof value === 'number' && !Number.isFinite(value))) {
    throw fail('Candidate payload must contain only JSON values', 'invalid_payload');
  }
  return json;
}
function digest(value) { return createHash('sha256').update(canonicalJson(value)).digest('hex'); }
// PostgreSQL JSONB can contain integers/decimals outside JavaScript's exact
// numeric range. Canonicalize its JSON text without rounding any number token.
function canonicalJsonText(source) {
  let at = 0;
  const skip = () => { while (/\s/.test(source[at] || '') && at < source.length) at += 1; };
  function parse() {
    skip();
    const char = source[at];
    if (char === '"') {
      const start = at++;
      while (at < source.length) {
        if (source[at] === '\\') { at += 2; continue; }
        if (source[at++] === '"') return JSON.stringify(JSON.parse(source.slice(start, at)));
      }
      throw fail('Invalid authoritative JSON text', 'invalid_payload');
    }
    if (char === '[' || char === '{') {
      const object = char === '{'; const end = object ? '}' : ']'; const values = [];
      at += 1; skip();
      if (source[at] === end) { at += 1; return object ? '{}' : '[]'; }
      while (at < source.length) {
        let key;
        if (object) {
          skip(); if (source[at] !== '"') break;
          key = parse(); skip(); if (source[at++] !== ':') break;
        }
        const value = parse(); values.push(object ? [key, value] : value); skip();
        if (source[at] === end) {
          at += 1;
          return object ? `{${values.sort((a, b) => { const x = JSON.parse(a[0]); const y = JSON.parse(b[0]);
            return x < y ? -1 : x > y ? 1 : 0; }).map(pair => pair.join(':')).join(',')}}` : `[${values.join(',')}]`;
        }
        if (source[at++] !== ',') break;
      }
      throw fail('Invalid authoritative JSON text', 'invalid_payload');
    }
    const token = /^(?:null|true|false|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)/.exec(source.slice(at));
    if (!token) throw fail('Invalid authoritative JSON text', 'invalid_payload');
    at += token[0].length; return token[0];
  }
  const result = parse(); skip();
  if (at !== source.length) throw fail('Invalid authoritative JSON text', 'invalid_payload');
  return result;
}
function hashText(value) { return createHash('sha256').update(value).digest('hex'); }
function candidateKey(raw, authoritativeText) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw fail('Candidate must be a raw JSON object', 'invalid_payload');
  // Do not merge by domain, name, geography or a guessed provider identifier.
  const placeId = typeof raw.placeId === 'string' && raw.placeId.length ? raw.placeId
    : typeof raw.place_id === 'string' && raw.place_id.length ? raw.place_id : null;
  return placeId === null && authoritativeText != null
    ? hashText(`["raw",${canonicalJsonText(authoritativeText)}]`)
    : digest(placeId === null ? ['raw', raw] : ['google_places', placeId]);
}
// Query labels/timestamps are provenance, not new business evidence. Keeping
// them out of this comparison avoids paid rework when another query rediscovers
// the exact same facts; all original values remain immutable memberships.
function businessFacts(raw) {
  const fields = ['name','companyName','company','website','url','domain','address','location','city','state','country',
    'description','snippet','businessType','placeTypes','types','classificationObservations','vertical','segment','category'];
  const facts = Object.fromEntries(fields.filter(key => raw[key] != null && raw[key] !== '').map(key => [key,raw[key]]));
  if (raw.source !== 'google_places' && raw.industry && ![raw.discoveryQuery,raw.discoveryConcept].includes(raw.industry)) facts.industry=raw.industry;
  return digest(facts);
}
function text(value, label, max = 256) {
  if (typeof value !== 'string' || !value || value.length > max || value.trim() !== value) {
    throw fail(`Invalid Scout ticket ${label}`, 'invalid_input');
  }
  return value;
}
function normalizeOutcome(outcome) {
  if (!outcome || !REASONS[outcome.state]?.includes(outcome.reason)) throw fail('Unsupported Scout ticket outcome', 'invalid_outcome');
  const dependencyKey = text(outcome.dependencyKey, 'dependency key', 1024);
  const evidence = outcome.evidence == null ? null : JSON.parse(canonicalJson(outcome.evidence));
  if (outcome.reason === 'policy_excluded' && (!evidence || typeof evidence !== 'object'
    || Array.isArray(evidence) || !Object.keys(evidence).length)) {
    throw fail('Terminal policy exclusion requires supporting evidence', 'invalid_outcome');
  }
  let retryAfterMs = outcome.retryAfterMs;
  if (retryAfterMs != null && (!Number.isSafeInteger(retryAfterMs) || retryAfterMs < 0)) {
    throw fail('Invalid Scout ticket retry delay', 'invalid_outcome');
  }
  if (outcome.state === 'deferred') {
    retryAfterMs = outcome.reason === 'evidence_budget_deferred' ? (retryAfterMs || 0)
      : Math.max(CRASH_RETRY_MS, retryAfterMs || 0);
  } else retryAfterMs = null;
  return { state: outcome.state, reason: outcome.reason, dependencyKey, retryAfterMs, evidence };
}

function createCandidateTicketStore({ pool, tenantId, fingerprint, planVersion = PLAN_VERSION,
  ticketVersion = TICKET_VERSION, lockTimeoutMs = 5000 }) {
  if (![2000,5000].includes(lockTimeoutMs)) throw fail('Invalid ticket lock timeout', 'invalid_input');
  if (!pool || typeof pool.query !== 'function' || typeof pool.connect !== 'function') {
    throw fail('Scout candidate tickets require a PostgreSQL pool', 'invalid_input');
  }
  if (!['string', 'number'].includes(typeof tenantId) || (typeof tenantId === 'number' && !Number.isSafeInteger(tenantId))) {
    throw fail('Scout candidate tickets require a tenant', 'invalid_input');
  }
  const tenant = text(String(tenantId), 'tenant');
  const mission = text(fingerprint, 'mission fingerprint');
  const plan = text(planVersion, 'plan version');
  const version = text(ticketVersion, 'ticket version');
  const scope = [tenant, mission, plan, version];
  let ready;
  async function assertReady() {
    // Read-only readiness probe. A missing/partial migration stops acquisition.
    if (!ready) ready = pool.query(`SELECT w.tenant_id,w.search_fingerprint,w.plan_version,w.ticket_version,
      w.candidate_key,w.state,w.reason,w.dependency_key,w.membership_revision,w.next_attempt_at,
      w.claim_token,w.lease_until,w.claim_revision,w.attempts,w.outcome_evidence,
      w.last_completion_token,w.last_completion_outcome,w.last_completion_revision,w.created_at,w.updated_at,
      m.query_key,m.payload_hash,m.ordinal,m.member_key,m.raw_candidate
      FROM scout_candidate_work w LEFT JOIN scout_candidate_memberships m
      USING (tenant_id,search_fingerprint,plan_version,ticket_version,candidate_key) LIMIT 0`).catch(error => {
      ready = null;
      throw Object.assign(fail('Scout candidate ticket schema is unavailable; apply the approved migration first', 'schema_not_ready'), { cause: error });
    });
    await ready;
  }
  async function transaction(fn) {
    await assertReady();
    const db = await pool.connect();
    let begun = false;
    let committing = false;
    let broken = false;
    let connectionError = null;
    // A checked-out pg client can emit an error between awaited statements.
    // Keep that backend failure scoped to this transaction, never the process.
    const onConnectionError = error => { connectionError = error; broken = true; };
    db.on?.('error', onConnectionError);
    try {
      await db.query('BEGIN'); begun = true;
      await db.query(lockTimeoutMs === 2000 ? "SET LOCAL lock_timeout = '2s'" : "SET LOCAL lock_timeout = '5s'");
      await db.query("SET LOCAL statement_timeout = '30s'");
      const result = await fn(db);
      if (connectionError) throw connectionError;
      committing = true;
      await db.query('COMMIT'); begun = false;
      return result;
    } catch (error) {
      if (committing) { error.ticketCode = 'commit_uncertain'; broken = true; }
      if (begun) { try { await db.query('ROLLBACK'); } catch { broken = true; } }
      throw error;
    } finally {
      db.release(broken);
      db.removeListener?.('error', onConnectionError);
    }
  }
  function memberRef(member, claim) {
    return { tenantId: tenant, fingerprint: mission, planVersion: plan, ticketVersion: version,
      queryKey: member.query_key, payloadHash: member.payload_hash, memberKey: member.member_key,
      candidateKey: member.candidate_key,
      ...(claim ? { claimToken: claim.claim_token, membershipRevision: String(claim.claim_revision) } : {}) };
  }
  function decorate(member, claim) {
    return { ...member.raw_candidate, _scoutTicketRefs: [memberRef(member, claim)] };
  }
  function validateRefs(refs) {
    if (!Array.isArray(refs) || !refs.length) throw fail('Scout ticket claim references are required', 'invalid_refs');
    const grouped = new Map();
    for (const ref of refs) {
      if (!ref || ref.tenantId !== tenant || ref.fingerprint !== mission || ref.planVersion !== plan
        || ref.ticketVersion !== version || !HEX.test(ref.candidateKey) || !HEX.test(ref.memberKey)
        || !HEX.test(ref.payloadHash) || !UUID.test(ref.claimToken)
        || !/^[1-9][0-9]*$/.test(String(ref.membershipRevision))) {
        throw fail('Scout ticket reference scope or claim mismatch', 'invalid_refs');
      }
      text(ref.queryKey, 'query key');
      const group = grouped.get(ref.candidateKey) || { refs: [], token: ref.claimToken, revision: String(ref.membershipRevision) };
      if (group.token !== ref.claimToken || group.revision !== String(ref.membershipRevision)) {
        throw fail('Conflicting Scout ticket claim references', 'invalid_refs');
      }
      if (!group.refs.some(existing => existing.memberKey === ref.memberKey)) group.refs.push({ ...ref });
      else if (group.refs.some(existing => existing.memberKey === ref.memberKey
        && (existing.queryKey !== ref.queryKey || existing.payloadHash !== ref.payloadHash))) {
        throw fail('Conflicting Scout ticket membership references', 'invalid_refs');
      }
      grouped.set(ref.candidateKey, group);
    }
    return grouped;
  }
  async function lockedClaims(db, grouped) {
    const keys = [...grouped.keys()].sort();
    const { rows } = await db.query(`SELECT *,lease_until > clock_timestamp() AS lease_active
      FROM scout_candidate_work WHERE ${SCOPE_SQL} AND candidate_key=ANY($5::text[])
      ORDER BY candidate_key FOR UPDATE`, [...scope, keys]);
    if (rows.length !== keys.length) throw fail('Scout ticket claim was not found', 'stale_claim');
    const members = (await db.query(`SELECT * FROM scout_candidate_memberships
      WHERE ${SCOPE_SQL} AND candidate_key=ANY($5::text[]) ORDER BY candidate_key,member_key`, [...scope, keys])).rows;
    for (const row of rows) {
      const group = grouped.get(row.candidate_key);
      const actual = members.filter(member => member.candidate_key === row.candidate_key);
      // Every supplied ref must resolve exactly. A revision change can introduce
      // extra members; report it explicitly rather than treating it as forgery.
      for (const ref of group.refs) {
        if (!actual.some(member => member.member_key === ref.memberKey && member.query_key === ref.queryKey
          && member.payload_hash === ref.payloadHash)) throw fail('Scout ticket membership was not found', 'invalid_refs');
      }
      if (String(row.membership_revision) === group.revision && actual.length !== group.refs.length) {
        throw fail('Complete Scout ticket membership references are required', 'invalid_refs');
      }
    }
    return rows;
  }
  // candidateKeys restricts the claim to an exact pre-approved set; db runs the
  // claim inside a caller-owned transaction so it commits with caller records.
  async function claimDue({ dependencyKey, limit = MAX_CLAIM, exclusiveQueryKey = null, candidateKeys = null, db = null } = {}) {
    if (exclusiveQueryKey != null && !HEX.test(exclusiveQueryKey)) throw fail('Invalid exclusive query key', 'invalid_input');
    text(dependencyKey, 'dependency key', 1024);
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_CLAIM) throw fail('Invalid Scout ticket claim limit', 'invalid_input');
    if (candidateKeys != null && (!Array.isArray(candidateKeys) || !candidateKeys.length || candidateKeys.length > limit
      || new Set(candidateKeys).size !== candidateKeys.length || !candidateKeys.every(key => HEX.test(key)))) {
      throw fail('Invalid Scout ticket candidate key restriction', 'invalid_input');
    }
    if (db != null && (typeof db.query !== 'function')) throw fail('Invalid Scout ticket claim transaction', 'invalid_input');
    const params = [...scope, dependencyKey, limit];
    const exclusiveParam = exclusiveQueryKey == null ? null : `$${params.push(exclusiveQueryKey)}`;
    const keysParam = candidateKeys == null ? null : `$${params.push(candidateKeys)}::text[]`;
    const claim = async db => {
      const due = await db.query(`SELECT candidate_key FROM scout_candidate_work w
        WHERE ${SCOPE_SQL} AND (claim_token IS NULL OR lease_until <= clock_timestamp())
          AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp())
          AND (state IN ('pending','deferred') OR (state='blocked' AND dependency_key IS DISTINCT FROM $5))
          ${exclusiveParam == null ? '' : `AND EXISTS (SELECT 1 FROM scout_candidate_memberships m WHERE m.tenant_id=w.tenant_id AND m.search_fingerprint=w.search_fingerprint AND m.plan_version=w.plan_version AND m.ticket_version=w.ticket_version AND m.candidate_key=w.candidate_key AND m.query_key=${exclusiveParam})
          AND NOT EXISTS (SELECT 1 FROM scout_candidate_memberships m WHERE m.tenant_id=w.tenant_id AND m.search_fingerprint=w.search_fingerprint AND m.plan_version=w.plan_version AND m.ticket_version=w.ticket_version AND m.candidate_key=w.candidate_key AND m.query_key<>${exclusiveParam})`}
          ${keysParam == null ? '' : `AND candidate_key=ANY(${keysParam})`}
        ORDER BY next_attempt_at NULLS FIRST,created_at,candidate_key
        LIMIT $6 FOR UPDATE SKIP LOCKED`, params);
      if (!due.rows.length) return [];
      const claims = [];
      for (const { candidate_key: key } of due.rows) {
        // Recheck at a fresh READ COMMITTED snapshot after acquiring the work
        // row lock. transferQuery takes that same lock before adding members.
        if (exclusiveQueryKey != null) {
          const mixed = await db.query(`SELECT 1 FROM scout_candidate_memberships WHERE ${SCOPE_SQL}
            AND candidate_key=$5 AND query_key<>$6 LIMIT 1`, [...scope,key,exclusiveQueryKey]);
          if (mixed.rows.length) continue;
        }
        const result = await db.query(`UPDATE scout_candidate_work SET claim_token=$6::uuid,
          lease_until=clock_timestamp()+($7::bigint * interval '1 millisecond'),claim_revision=membership_revision,
          next_attempt_at=clock_timestamp()+($8::bigint * interval '1 millisecond'),state='pending',
          dependency_key=$9,attempts=attempts+1,updated_at=clock_timestamp()
          WHERE ${SCOPE_SQL} AND candidate_key=$5 RETURNING *`,
        [...scope, key, randomUUID(), LEASE_MS, CRASH_RETRY_MS, dependencyKey]);
        claims.push(result.rows[0]);
      }
      const members = (await db.query(`SELECT * FROM scout_candidate_memberships
        WHERE ${SCOPE_SQL} AND candidate_key=ANY($5::text[]) ORDER BY candidate_key,query_key,payload_hash,ordinal`,
      [...scope, claims.map(row => row.candidate_key)])).rows;
      return claims.map(row => {
        const own = members.filter(member => member.candidate_key === row.candidate_key);
        if (!own.length) throw fail('Scout ticket has no raw membership', 'invalid_membership');
        return { candidateKey: row.candidate_key, membershipRevision: String(row.claim_revision),
          claimToken: row.claim_token, leaseUntil: row.lease_until, state: row.state, reason: row.reason,
          members: own.map(member => decorate(member, row)), refs: own.map(member => memberRef(member, row)) };
      });
    };
    if (db == null) return transaction(claim);
    await assertReady();
    return claim(db);
  }
  return {
    tenantId: tenant, fingerprint: mission, planVersion: plan, ticketVersion: version, assertReady,
    async transferQuery(queryKey) {
      text(queryKey, 'query key');
      return transaction(async db => {
        const checkpoint = (await db.query(`SELECT *,candidates::text AS candidates_text FROM scout_query_checkpoints
          WHERE tenant_id=$1 AND search_fingerprint=$2 AND query_key=$3 FOR UPDATE`,
        [tenant, mission, queryKey])).rows[0];
        if (!checkpoint || checkpoint.plan_version !== plan || !['fetched', 'handled'].includes(checkpoint.state)
          || !Array.isArray(checkpoint.candidates)) throw fail('Authoritative Scout query checkpoint is missing or incompatible', 'invalid_checkpoint');
        const payloadHash = hashText(canonicalJsonText(checkpoint.candidates_text));
        const originals = (await db.query(`SELECT value AS raw_candidate,value::text AS raw_text,(ordinality-1)::int AS ordinal
          FROM scout_query_checkpoints,jsonb_array_elements(candidates) WITH ORDINALITY
          WHERE tenant_id=$1 AND search_fingerprint=$2 AND query_key=$3 ORDER BY ordinality`,
        [tenant, mission, queryKey])).rows;
        for (const { raw_candidate: raw } of originals) {
          for (const field of ['tenantId','tenant_id','clientId','client_id']) {
            if (raw?.[field] != null && (!['string','number'].includes(typeof raw[field]) || String(raw[field]) !== tenant)) {
              throw fail('Raw Scout candidate tenant binding mismatch', 'invalid_payload');
            }
          }
        }
        const members = originals.map(({ raw_candidate: raw, raw_text: rawText, ordinal }) => ({ query_key: queryKey,
          payload_hash: payloadHash, ordinal, member_key: digest([version, queryKey, payloadHash, ordinal]),
          candidate_key: candidateKey(raw, rawText), raw_candidate: raw, raw_text: rawText }));
        // Stable candidate ordering prevents opposite-order overlapping queries
        // from deadlocking while each holds its own checkpoint lock.
        for (const key of [...new Set(members.map(member => member.candidate_key))].sort()) {
          await db.query(`INSERT INTO scout_candidate_work
            (tenant_id,search_fingerprint,plan_version,ticket_version,candidate_key)
            VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, [...scope, key]);
          await db.query(`SELECT candidate_key FROM scout_candidate_work
            WHERE ${SCOPE_SQL} AND candidate_key=$5 FOR UPDATE`, [...scope, key]);
          const priorFacts = new Set((await db.query(`SELECT raw_candidate FROM scout_candidate_memberships
            WHERE ${SCOPE_SQL} AND candidate_key=$5`, [...scope, key])).rows.map(row => businessFacts(row.raw_candidate)));
          let materialChange = false;
          let added = 0;
          for (const member of members.filter(value => value.candidate_key === key)) {
            const inserted = await db.query(`INSERT INTO scout_candidate_memberships
              (tenant_id,search_fingerprint,plan_version,ticket_version,query_key,payload_hash,ordinal,member_key,candidate_key,raw_candidate)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb) ON CONFLICT DO NOTHING RETURNING member_key`,
            [...scope, queryKey, payloadHash, member.ordinal, member.member_key, key, member.raw_text]);
            if (inserted.rows.length) {
              added += 1; const fact = businessFacts(member.raw_candidate);
              if (!priorFacts.has(fact)) materialChange = true; priorFacts.add(fact);
            }
            const saved = (await db.query(`SELECT *,raw_candidate::text AS raw_text FROM scout_candidate_memberships
              WHERE ${SCOPE_SQL} AND member_key=$5`, [...scope, member.member_key])).rows[0];
            if (!saved || saved.query_key !== queryKey || saved.payload_hash !== payloadHash
              || saved.ordinal !== member.ordinal || saved.candidate_key !== key
              || saved.raw_text !== member.raw_text) {
              throw fail('Scout ticket exact raw membership was not confirmed', 'invalid_membership');
            }
          }
          if (added) await db.query(`UPDATE scout_candidate_work SET membership_revision=membership_revision+$6::bigint,
            next_attempt_at=CASE WHEN state IN ('handled','blocked') AND $7::boolean THEN clock_timestamp() ELSE next_attempt_at END,
            reason=CASE WHEN state IN ('handled','blocked') AND $7::boolean THEN NULL ELSE reason END,
            state=CASE WHEN state IN ('handled','blocked') AND $7::boolean THEN 'pending' ELSE state END,updated_at=clock_timestamp()
            WHERE ${SCOPE_SQL} AND candidate_key=$5`, [...scope, key, added, materialChange]);
        }
        await db.query(`UPDATE scout_query_checkpoints SET state='handled',handled_at=COALESCE(handled_at,clock_timestamp())
          WHERE tenant_id=$1 AND search_fingerprint=$2 AND query_key=$3 AND plan_version=$4`,
        [tenant, mission, queryKey, plan]);
        return { queryKey, transferred: true, candidates: members.map(member => decorate(member)) };
      });
    },
    claimDue, loadDue: claimDue,
    async renew(refs) {
      const groups = validateRefs(refs);
      return transaction(async db => {
        const rows = await lockedClaims(db, groups);
        for (const row of rows) {
          const group = groups.get(row.candidate_key);
          if (row.claim_token !== group.token || row.lease_active !== true || String(row.claim_revision) !== group.revision) {
            throw fail('Scout ticket claim expired or was replaced', 'stale_claim');
          }
          if (String(row.membership_revision) !== group.revision) throw fail('Scout ticket membership changed', 'membership_changed');
        }
        await db.query(`UPDATE scout_candidate_work SET lease_until=clock_timestamp()+($6::bigint * interval '1 millisecond'),
          next_attempt_at=GREATEST(next_attempt_at,clock_timestamp()+($7::bigint * interval '1 millisecond')),
          updated_at=clock_timestamp() WHERE ${SCOPE_SQL} AND candidate_key=ANY($5::text[])`,
        [...scope, [...groups.keys()], LEASE_MS, CRASH_RETRY_MS]);
        return [...groups.values()].flatMap(group => group.refs.map(ref => ({ ...ref })));
      });
    },
    async finish(refs, outcome, writeCallback) {
      const groups = validateRefs(refs);
      const normalized = normalizeOutcome(outcome);
      const outcomeHash = digest(normalized);
      if (writeCallback != null && typeof writeCallback !== 'function') throw fail('Invalid Scout ticket write callback', 'invalid_input');
      return transaction(async db => {
        const rows = await lockedClaims(db, groups);
        const repeated = rows.filter(row => row.last_completion_token === groups.get(row.candidate_key).token);
        if (repeated.length) {
          if (repeated.length !== rows.length || repeated.some(row => row.last_completion_outcome !== outcomeHash
            || String(row.last_completion_revision) !== groups.get(row.candidate_key).revision)) {
            throw fail('Conflicting Scout ticket completion retry', 'completion_conflict');
          }
          return { applied: true, replayed: true, result: null };
        }
        for (const row of rows) {
          const group = groups.get(row.candidate_key);
          if (row.claim_token !== group.token || row.lease_active !== true || String(row.claim_revision) !== group.revision) {
            throw fail('Scout ticket claim expired or was replaced', 'stale_claim');
          }
        }
        if (rows.some(row => String(row.membership_revision) !== groups.get(row.candidate_key).revision)) {
          await db.query(`UPDATE scout_candidate_work SET state='pending',reason='membership_changed',
            claim_token=NULL,lease_until=NULL,claim_revision=NULL,updated_at=clock_timestamp()
            WHERE ${SCOPE_SQL} AND candidate_key=ANY($5::text[])`, [...scope, [...groups.keys()]]);
          return { applied: false, reason: 'membership_changed', result: null };
        }
        // Only DB work belongs here; network/provider work must precede finish.
        const result = writeCallback ? await writeCallback(db) : null;
        for (const row of rows) {
          await db.query(`UPDATE scout_candidate_work SET state=$6,reason=$7,dependency_key=$8,
            next_attempt_at=CASE WHEN $9::bigint IS NULL THEN NULL
              ELSE clock_timestamp()+($9::bigint * interval '1 millisecond') END,
            outcome_evidence=$10::jsonb,last_completion_token=claim_token,last_completion_outcome=$11,
            last_completion_revision=claim_revision,claim_token=NULL,lease_until=NULL,claim_revision=NULL,
            updated_at=clock_timestamp() WHERE ${SCOPE_SQL} AND candidate_key=$5`,
          [...scope, row.candidate_key, normalized.state, normalized.reason, normalized.dependencyKey,
            normalized.retryAfterMs, canonicalJson(normalized.evidence), outcomeHash]);
        }
        return { applied: true, result };
      });
    },
    async snapshot({ dependencyKey = null } = {}) {
      if (dependencyKey !== null) text(dependencyKey, 'dependency key', 1024);
      await assertReady();
      const { rows } = await pool.query(`SELECT state,reason,count(*)::int AS count,
        count(*) FILTER (WHERE claim_token IS NOT NULL AND lease_until > clock_timestamp())::int AS claimed,
        count(*) FILTER (WHERE (claim_token IS NULL OR lease_until <= clock_timestamp())
          AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp())
          AND (state IN ('pending','deferred') OR (state='blocked' AND $5::text IS NOT NULL AND dependency_key IS DISTINCT FROM $5)))::int AS due
        FROM scout_candidate_work WHERE ${SCOPE_SQL} GROUP BY state,reason ORDER BY state,reason`, [...scope, dependencyKey]);
      const byState = { pending: 0, deferred: 0, blocked: 0, handled: 0 };
      const byReason = {};
      let due = 0; let claimed = 0; let total = 0;
      for (const row of rows) {
        byState[row.state] += row.count; total += row.count; due += row.due; claimed += row.claimed;
        if (row.reason) byReason[row.reason] = (byReason[row.reason] || 0) + row.count;
      }
      return { total, byState, byReason, due, claimed,
        status: !total ? 'not_observed' : total === byState.handled ? 'observed_candidate_set_handled' : 'pending_candidate_work' };
    },
  };
}

module.exports = { TICKET_VERSION, LEASE_MS, CRASH_RETRY_MS, MAX_CLAIM, REASONS,
  canonicalJson, canonicalJsonText, candidateKey, createCandidateTicketStore };
