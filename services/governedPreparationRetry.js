'use strict';
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');

// A reservation is serialized by the governed send/preparation lock. Unchanged
// work backs off; newly eligible identities can resume after a short cooldown.
function preparationRetryReason(progress, priorFingerprint, fingerprint, now = new Date()) {
  if (!progress?.last_attempt_at) return null;
  const elapsed = +now - +new Date(progress.last_attempt_at);
  const changed = Boolean(fingerprint && fingerprint !== priorFingerprint);
  // The caller holds the exclusive preparation lock. A reservation without a
  // terminal error on an unfinished mission is an interrupted run, not a live
  // concurrent attempt; resume its idempotent stages after the short cooldown.
  const interrupted = Number(progress.attempts) > 0 && progress.last_error == null;
  return elapsed < (changed || interrupted ? 5 : 60) * 60000 ? 'preparation_backoff' : null;
}
async function preparationInventoryState(pool, store, program, source) {
  const { loadCleanInventory } = require('./maxOutboundControlLoop');
  const inventory = await loadCleanInventory(pool, store, source.mission || source, store.clientId, program.policy);
  return { inventory, fingerprint: hash({ preparationVersion: 'canonical-contact-v2', policy: program.policy_hash, scope: program.scope_hash,
    clean: inventory.clean.map(row => [row.prospectId, row.companyId, row.email]).sort() }) };
}
module.exports = { preparationRetryReason, preparationInventoryState };
