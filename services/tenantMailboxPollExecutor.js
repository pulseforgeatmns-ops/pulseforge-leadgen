'use strict';

/**
 * SPEC-253 — Multi-tenant inbound mailbox polling executor.
 *
 * Discovers active tenant mailbox integrations with IMAP configured and invokes
 * pollTenantMailbox() for each. Intended for recurring invocation via cron-job.org
 * at GET/POST /cron/tenant-mailbox-poll.
 *
 * Delivery/bounce observability (SMTP provider acceptance vs recipient delivery)
 * is out of scope — generic SMTP bounce/DSN ingestion remains a future capability.
 */

const crypto = require('crypto');
const defaultPool = require('../db');
const {
  pollTenantMailbox,
  publicIntegration,
  PostgresTenantMailboxStore,
  canResolveImapCredential,
  isPollableIntegration,
  EVENT_TYPES,
  sanitizeErrorMessage,
} = require('./tenantMailbox');

const POLL_EXECUTOR_LOCK_NAMESPACE = 701253;

function integrationLockKey(integrationId) {
  const hash = crypto.createHash('sha256').update(String(integrationId || '')).digest();
  return hash.readInt32BE(0);
}

function publicPollResult(result) {
  if (!result) return null;
  const safe = { ...result };
  if (safe.integration) safe.integration = publicIntegration(safe.integration);
  delete safe.results;
  return safe;
}

function countReplies(pollResult = {}) {
  return (pollResult.results || []).filter(
    (row) => row.event?.eventType === EVENT_TYPES.REPLY_RECEIVED && row.inserted
  ).length;
}

async function tryAcquireIntegrationLock(lockClient, integrationId, opts = {}) {
  if (typeof opts.tryAcquireLock === 'function') {
    return opts.tryAcquireLock(integrationId);
  }
  const lock = await lockClient.query(
    'SELECT pg_try_advisory_lock($1, $2) AS locked',
    [POLL_EXECUTOR_LOCK_NAMESPACE, integrationLockKey(integrationId)]
  );
  return lock.rows[0]?.locked === true;
}

async function releaseIntegrationLock(lockClient, integrationId, opts = {}) {
  if (typeof opts.releaseLock === 'function') {
    return opts.releaseLock(integrationId);
  }
  await lockClient.query(
    'SELECT pg_advisory_unlock($1, $2)',
    [POLL_EXECUTOR_LOCK_NAMESPACE, integrationLockKey(integrationId)]
  ).catch(() => {});
}

async function pollOneIntegration(integration, opts = {}) {
  const pool = opts.pool || defaultPool;
  const tenantId = integration.tenantId;
  const integrationId = integration.id;
  const base = {
    tenantId,
    integrationId,
    mailboxAddress: integration.mailboxAddress,
    success: false,
    skipped: false,
    fetched: 0,
    inserted: 0,
    duplicates: 0,
    unmatched: 0,
    replies: 0,
  };

  if (!isPollableIntegration(integration)) {
    return { ...base, skipped: true, reason: 'not_pollable' };
  }

  if (!canResolveImapCredential(integration, opts)) {
    return { ...base, skipped: true, reason: 'credential_unavailable' };
  }

  const usesInjectedLock = typeof opts.tryAcquireLock === 'function';
  const lockClient = opts.lockClient || (usesInjectedLock ? null : await pool.connect());
  const releaseClient = !opts.lockClient && !usesInjectedLock;
  let locked = false;
  try {
    locked = await tryAcquireIntegrationLock(lockClient, integrationId, opts);
    if (!locked) {
      return { ...base, success: true, skipped: true, reason: 'overlap' };
    }

    const pollResult = await pollTenantMailbox({ tenantId, integrationId }, {
      ...opts,
      store: opts.store || opts.mailboxStore,
    });
    const replies = countReplies(pollResult);
    return {
      ...base,
      success: true,
      integration: pollResult.integration,
      fetched: pollResult.fetched,
      inserted: pollResult.inserted,
      duplicates: pollResult.duplicates,
      unmatched: pollResult.unmatched,
      replies,
    };
  } catch (err) {
    return {
      ...base,
      success: false,
      code: err.code || 'poll_failed',
      error: sanitizeErrorMessage(err),
    };
  } finally {
    if (locked) await releaseIntegrationLock(lockClient, integrationId, opts);
    if (releaseClient) lockClient.release();
  }
}

async function executeTenantMailboxPolls(opts = {}) {
  const pool = opts.pool || defaultPool;
  const store = opts.mailboxStore || new PostgresTenantMailboxStore(pool);
  const integrations = opts.integrations ?? await store.listPollableIntegrations();

  const aggregate = {
    success: true,
    integrations: integrations.length,
    polled: 0,
    skipped: 0,
    fetched: 0,
    inserted: 0,
    duplicates: 0,
    unmatched: 0,
    replies: 0,
    failed: 0,
    empty: integrations.length === 0,
    results: [],
  };

  for (const integration of integrations) {
    const result = await pollOneIntegration(integration, { ...opts, mailboxStore: store, pool });
    aggregate.results.push(publicPollResult(result));

    if (result.skipped) {
      aggregate.skipped += 1;
      continue;
    }

    if (!result.success) {
      aggregate.failed += 1;
      continue;
    }

    aggregate.polled += 1;
    aggregate.fetched += result.fetched || 0;
    aggregate.inserted += result.inserted || 0;
    aggregate.duplicates += result.duplicates || 0;
    aggregate.unmatched += result.unmatched || 0;
    aggregate.replies += result.replies || 0;
  }

  aggregate.success = aggregate.failed === 0;
  aggregate.empty = aggregate.integrations === 0
    || (aggregate.polled > 0 && aggregate.fetched === 0 && aggregate.failed === 0);

  return aggregate;
}

module.exports = {
  POLL_EXECUTOR_LOCK_NAMESPACE,
  integrationLockKey,
  executeTenantMailboxPolls,
  pollOneIntegration,
  countReplies,
  publicPollResult,
};
