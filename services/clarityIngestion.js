'use strict';

const { parseClarityDigest, ClarityParseError } = require('../packages/max/clarity/parser');
const { assertClientId, assertIntegration, grantFingerprint, createClarityGmail, clarityError } = require('./clarityGmail');

const DAY_MS = 86400000;
const LIMITS = Object.freeze({ pageSize: 25, pagesPerRun: 2, attempts: 3, runtimeMs: 120000, maxBackfillDays: 90 });
const SAFE_ERROR = /^clarity_[a-z0-9_]{1,70}$/;

function safeErrorCode(error) { return SAFE_ERROR.test(error?.code || '') ? error.code : 'clarity_ingestion_failed'; }

function createClarityIngestion({ store, gmail, env = process.env, now = () => Date.now(), sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)), onSnapshot } = {}) {
  const client = gmail || createClarityGmail({ env, now });
  async function retry(fn, deadline) {
    for (let attempt = 0; attempt < LIMITS.attempts; attempt += 1) {
      if (now() >= deadline) throw clarityError('clarity_run_deadline', { retryable: true });
      try { return await fn(); }
      catch (error) {
        if (!error.retryable || attempt + 1 >= LIMITS.attempts) throw error;
        await sleep(250 * (2 ** attempt));
      }
    }
    throw clarityError('clarity_retry_exhausted');
  }

  async function run({ clientId, backfillDays = 21, force = false } = {}) {
    assertClientId(clientId); // Explicit tenant required even while dark.
    if (env.CLARITY_ENABLED !== 'true' || env.CLARITY_INGESTION_ENABLED !== 'true') return { skipped: true, reason: 'disabled' };
    if (!Number.isInteger(backfillDays) || backfillDays < 7 || backfillDays > LIMITS.maxBackfillDays || typeof force !== 'boolean') throw clarityError('clarity_backfill_bounds_invalid');
    return store.withIngestionLock({ clientId }, async (lockedStore) => {
      const started = now();
      const deadline = started + LIMITS.runtimeMs;
      const integration = await lockedStore.getIntegration({ clientId });
      if (!integration || integration.enabled !== true) return { skipped: true, reason: 'integration_disabled' };
      if (!force && integration.next_run_at && new Date(integration.next_run_at).getTime() > started) return { skipped: true, reason: 'not_due' };
      const counts = { listed: 0, parsed: 0, inserted: 0, duplicate: 0, rejected: 0, pages: 0 };
      const rejections = [];
      let cursor = integration.ingestion_cursor || { after: new Date(started - backfillDays * DAY_MS).toISOString(), before: new Date(started).toISOString(), page_token: null };
      const seen = new Set();
      try {
        assertIntegration({ clientId, integration, env });
        const originalGrant = grantFingerprint(integration);
        async function authorizeCurrentGrant() {
          const current = await lockedStore.getIntegration({ clientId });
          assertIntegration({ clientId, integration: current, env });
          if (grantFingerprint(current) !== originalGrant) throw clarityError('clarity_grant_changed');
          if (now() >= deadline) throw clarityError('clarity_run_deadline', { retryable: true });
        }
        async function guardedRead(fn) {
          return retry(async () => { await authorizeCurrentGrant(); return fn(); }, deadline);
        }
        // Validate persisted continuation too: it must not expand mailbox reads.
        const afterMs = Date.parse(cursor.after), beforeMs = Date.parse(cursor.before);
        if (!Number.isFinite(afterMs) || !Number.isFinite(beforeMs) || beforeMs <= afterMs || beforeMs - afterMs > LIMITS.maxBackfillDays * DAY_MS + 1000 || beforeMs > started + 60000 || (cursor.page_token !== null && (typeof cursor.page_token !== 'string' || cursor.page_token.length > 2048))) throw clarityError('clarity_cursor_invalid');
        for (let page = 0; page < LIMITS.pagesPerRun; page += 1) {
          const response = await guardedRead(() => client.listMessages({ clientId, integration, after: cursor.after, before: cursor.before, maxResults: LIMITS.pageSize, pageToken: cursor.page_token || undefined }));
          if (!Array.isArray(response.messages) || response.messages.length > LIMITS.pageSize) throw clarityError('clarity_message_list_invalid');
          counts.pages += 1;
          for (const item of response.messages) {
            counts.listed += 1;
            if (seen.has(item.id)) { counts.duplicate += 1; continue; }
            seen.add(item.id);
            const message = await guardedRead(() => client.getMessage({ clientId, integration, messageId: item.id }));
            let snapshot;
            try {
              if (message.id !== item.id) throw new ClarityParseError('message_id_mismatch', { gmail_message_id: item.id });
              snapshot = parseClarityDigest(message, { expectedSender: integration.digest_sender, expectedProjectId: integration.project_id, requireAuthentication: true });
            } catch (error) {
              if (!(error instanceof ClarityParseError)) throw error;
              counts.rejected += 1;
              rejections.push({ code: error.code, provenance: error.provenance });
              continue;
            }
            counts.parsed += 1;
            await authorizeCurrentGrant();
            const saved = await lockedStore.saveSnapshot({ clientId, snapshot, expectedConsentAt: integration.consented_at });
            counts[saved.inserted ? 'inserted' : 'duplicate'] += 1;
            // Re-run idempotent recommendation creation on duplicates as well:
            // a prior crash can have persisted the snapshot but not its recommendations.
            if (onSnapshot) {
              await authorizeCurrentGrant();
              await onSnapshot({ clientId, snapshot: saved.snapshot, store: lockedStore, expectedConsentAt: integration.consented_at, remainingMs: Math.max(1, deadline - now()) });
              await authorizeCurrentGrant();
            }
          }
          if (!response.nextPageToken) { cursor = null; break; }
          if (typeof response.nextPageToken !== 'string' || response.nextPageToken.length > 2048 || response.nextPageToken === cursor.page_token) throw clarityError('clarity_page_token_invalid');
          cursor = { ...cursor, page_token: response.nextPageToken };
        }
        const status = cursor ? 'partial' : counts.parsed ? 'completed' : 'no_digest';
        const delay = cursor ? 3600000 : counts.parsed ? 7 * DAY_MS : DAY_MS;
        const nextRunAt = new Date(now() + delay).toISOString();
        await lockedStore.recordIngestionRun({ clientId, status, counts, rejections, cursor, nextRunAt, expectedConsentAt: integration.consented_at });
        return { status, counts, next_run_at: nextRunAt, has_more: Boolean(cursor) };
      } catch (error) {
        const errorCode = safeErrorCode(error);
        if (cursor && ['clarity_page_token_rejected', 'clarity_page_token_invalid'].includes(errorCode)) cursor = { ...cursor, page_token: null };
        const failureCount = Math.max(0, Number(integration.consecutive_failures) || 0);
        const nextRunAt = new Date(now() + Math.min(DAY_MS, 3600000 * (2 ** Math.min(failureCount, 5)))).toISOString();
        await lockedStore.recordIngestionRun({ clientId, status: 'failed', counts, rejections, cursor, errorCode, nextRunAt, expectedConsentAt: integration.consented_at });
        return { status: 'failed', counts, error_code: errorCode, next_run_at: nextRunAt };
      }
    });
  }

  async function runDue({ limit = 10 } = {}) {
    if (env.CLARITY_ENABLED !== 'true' || env.CLARITY_INGESTION_ENABLED !== 'true') return { skipped: true, reason: 'disabled', runs: [] };
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 10) throw clarityError('clarity_scheduler_limit_invalid');
    const integrations = await store.listDueIntegrations({ now: new Date(now()).toISOString(), limit });
    const runs = [];
    for (const integration of integrations.slice(0, limit)) {
      // Defense in depth if a store returns an unapproved tenant.
      assertClientId(integration.client_id);
      runs.push({ client_id: integration.client_id, ...await run({ clientId: integration.client_id }) });
    }
    return { runs };
  }

  return { run, runDue };
}

module.exports = { LIMITS, createClarityIngestion, safeErrorCode };
