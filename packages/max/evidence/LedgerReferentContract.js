'use strict';

/**
 * Contract between a conversational memory (e.g. the ConversationLedger of
 * P0-MAX-REASONING-001 PR 3) and the EvidenceRegistry.
 *
 * This module depends on no ledger code. It accepts the plain data shape the
 * ledger's resolveFocus() returns —
 *   { referent: { kind, memberIds?, missionId? }, requiresRevalidation, verified: false }
 * — or a bare referent / array of referents, and turns it into registry
 * referents. Everything a memory remembers is a hint: the registry revalidates
 * each exact id against the tenant's current records and reports deleted,
 * transferred or inaccessible ids explicitly, never substituting another.
 */

const ID_RE = /^[A-Za-z0-9_.:-]{1,160}$/;

function referentsFromMemory(input) {
  const list = Array.isArray(input) ? input : [input && input.referent ? input.referent : input];
  const out = [];
  for (const r of list) {
    if (!r || typeof r !== 'object') continue;
    if (r.kind === 'prospect_set' && Array.isArray(r.memberIds)) {
      const memberIds = r.memberIds.map(String).filter((id) => ID_RE.test(id));
      if (memberIds.length) out.push({ kind: 'prospect_set', memberIds });
    } else if (r.kind === 'mission' && ID_RE.test(String(r.missionId || ''))) {
      out.push({ kind: 'mission', missionId: String(r.missionId) });
    } else if (r.kind === 'prospect' && ID_RE.test(String(r.id || ''))) {
      out.push({ kind: 'prospect', id: String(r.id) });
    }
  }
  return out;
}

/** Revalidate a remembered focus and return fresh evidence for exactly those ids. */
async function revalidateRemembered(registry, { tenantId, memory, domains = ['crm_pipeline'] }) {
  const referents = referentsFromMemory(memory);
  if (!referents.length) return { referents: [], report: null, reason: 'nothing_remembered_to_revalidate' };
  const needed = new Set(domains);
  if (referents.some((r) => r.kind === 'mission')) needed.add('mission_lifecycle');
  const report = await registry.retrieve({ tenantId, domains: [...needed], referents });
  return { referents, report };
}

module.exports = { referentsFromMemory, revalidateRemembered };
