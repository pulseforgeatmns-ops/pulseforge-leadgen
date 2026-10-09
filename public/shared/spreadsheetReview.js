/* Spreadsheet previews are server-owned. This module retains display data only in this
 * page; commit requests contain immutable identifiers and exact selected operation IDs. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.PulseforgeSpreadsheetReview = api;
}(typeof globalThis === 'undefined' ? this : globalThis, function () {
  'use strict';
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const display = value => escape(value === undefined ? 'Unspecified' : JSON.stringify(value, null, 2));
  function intent(text) {
    const value = String(text || '').trim();
    if (/\b(do\s+not|don't|dont|never|not\s+yet|wait|cancel|hold|stop|without|preview|compare|review\s+only)\b/i.test(value)) return 'hold';
    if (/^(?:please\s+)?(?:save|confirm|approve)(?:\s+(?:the\s+)?(?:selected\s+)?(?:changes|updates|operations|proposal))?[.!]?$/i.test(value)) return 'approve';
    return 'other';
  }
  function candidateLabel(candidate, fallback) {
    if (!candidate) return fallback;
    return [candidate.name || fallback, candidate.title, candidate.email, candidate.phone, candidate.address, candidate.website, `ID ${candidate.id}`].filter(Boolean).join(' · ');
  }
  function unresolvedContacts(row) {
    const contacts = [...(row.contacts || []), ...(row.contactAssertions || [])].filter(contact => !['matched', 'reviewed_match', 'reviewed_new_contact'].includes(contact.status));
    return [...new Map(contacts.map(contact => [contact.name, contact])).values()];
  }
  function resolutionHtml(row, index, enabled) {
    const contacts = unresolvedContacts(row);
    if (!enabled || (!row.conflicts?.length && !contacts.length && row.provider?.status !== 'unresolved')) return '';
    const match = row.accountResolution || {};
    const candidates = match.candidates || [];
    return `<fieldset data-spreadsheet-resolution-row="${index}"><legend>Resolve ${escape(row.sheet)} row ${escape(row.rowNumber)}: ${escape(row.company)}</legend>
      <label>Account identity <select data-resolution-account><option value="">Leave account identity unchanged / unresolved</option>${candidates.map(id => `<option value="${escape(id)}">${escape(candidateLabel(row.candidateSummaries?.find(item => String(item.id) === String(id)), `Existing account ${id}`))}</option>`).join('')}${match.status === 'new_candidate' ? '<option value="__create__">Confirm this is a new account</option>' : ''}</select></label>
      <label>Evidence supporting these decisions (required)<textarea data-resolution-evidence placeholder="Explain the verified identity and why these choices are correct"></textarea></label>
      ${(row.conflicts || []).some(item => item.code === 'IDENTITY_CONFLICT') ? '<label><input type="checkbox" data-resolution-identity> I reviewed the conflicting organization evidence and explicitly confirm the selected identity.</label>' : ''}
      ${(row.comparisons || []).filter(item => item.result === 'conflict' || (row.conflicts || []).some(c => c.field === item.field && c.code === 'MULTIPLE_PHONES')).map(item => `<label>${escape(item.field)}: existing ${display(item.existing)}; source ${display(item.incoming)} <select data-resolution-field="${escape(item.field)}"><option value="">Leave unresolved</option><option value="keep_existing">Keep existing value</option><option value="use_source">Use source value as an explicit amendment</option></select></label>${item.field === 'phone' && row.phoneNumbers?.length > 1 ? `<label>Choose the source phone for the primary phone field <select data-resolution-phone><option value="">Choose a source phone</option>${row.phoneNumbers.map(phone => `<option value="${escape(phone.normalized)}">${escape(phone.raw || phone.value || phone.normalized)}</option>`).join('')}</select></label>` : ''}`).join('')}
      ${contacts.map(contact => `<label>Contact ${escape(contact.name)} <select data-resolution-contact="${escape(contact.name)}"><option value="">Leave identity unresolved</option>${(contact.candidates || []).map(id => `<option value="${escape(id)}">${escape(candidateLabel(contact.candidateSummaries?.find(item => String(item.id) === String(id)), `Existing contact ${id}`))}</option>`).join('')}<option value="__create__">Confirm a distinct new contact with this source name</option></select></label>`).join('')}
      ${row.provider?.status === 'unresolved' ? `<label>Provider / building management: ${escape(row.provider.raw)} ${row.provider.candidateSummaries?.length ? `<select data-resolution-provider><option value="">Leave relationship unresolved</option>${row.provider.candidateSummaries.filter(item => String(item.id) !== String(match.account?.id)).map(item => `<option value="${escape(item.id)}">${escape(candidateLabel(item, `Existing provider ${item.id}`))}</option>`).join('')}</select>` : '<span>No verified provider candidate available. Keep the source assertion for research.</span>'}</label>` : ''}
      <button type="button" data-resolution-submit="${index}">Build a fresh proposal from these decisions</button>
      <p>This changes the proposal only. Saving CRM changes requires a separate approval.</p></fieldset>`;
  }
  function proposalHtml(proposal, selected, { busy = false, locked = false, saved = new Set(), status = '' } = {}) {
    const operations = proposal.plan?.operations || [];
    const rows = proposal.plan?.rows || [];
    return `<section class="pf-spreadsheet-review" aria-label="Spreadsheet proposed changes">
      <h3>Review proposed CRM changes</h3>
      <p>Nothing is saved until Jake approves the selected changes. Source notes never authorize sending messages.</p>
      <p>Tenant ${escape(proposal.tenantId)} · AO ${escape(proposal.aoId)} · proposal ${escape(proposal.id)}</p>
      <details><summary>Source and review identity</summary><pre>${display({ sourceHash: proposal.sourceHash, digest: proposal.digest, actorId: proposal.actorId, conversationId: proposal.conversationId })}</pre></details>
      <details><summary>Workbook row classification and summary</summary><pre>${display({ summary: proposal.plan?.summary, sourceRows: proposal.plan?.sourceRows })}</pre></details>
      <p>${operations.length} proposed operations. Blanks in the source do not clear existing CRM values.</p>
      ${operations.map(op => `<article class="pf-spreadsheet-operation">
        <label><input type="checkbox" data-spreadsheet-operation="${escape(op.id)}" ${selected.has(op.id) ? 'checked' : ''} ${op.blocked || saved.has(op.id) || busy || locked || !proposal.can_approve ? 'disabled' : ''}> ${escape(op.type)} · ${escape(op.id)}${saved.has(op.id) ? ' — saved' : op.blocked ? ' — held' : ''}</label>
        <p>Target: ${display(op.target)}</p>
        ${op.outreachReviewRequired === true || op.after?.outreachReviewRequired === true ? '<p class="pf-spreadsheet-outreach-hold"><strong>Outreach held for separate authorization</strong>. Saving this operation does not authorize automated calls, emails or messages. This hold is not an opt-out.</p>' : ''}
        ${rows.flatMap(row => row.admissionHolds || []).filter(hold => hold.operationId === op.id).map(hold => `<p>Automated outreach hold — before: ${display(hold.before)}; proposed: ${display(hold.after)}. ${escape(hold.reason || '')}</p>`).join('')}
        <div class="pf-spreadsheet-values"><div><strong>Before</strong><pre>${display(op.before)}</pre></div><div><strong>Proposed</strong><pre>${display(op.after)}</pre></div></div>
        <details open><summary>Source evidence and dependencies</summary><pre>${display({ evidence: op.evidence, dependsOn: op.dependsOn, blocked: op.blocked })}</pre></details>
      </article>`).join('')}
      <details open><summary>Every source row, match and unresolved question (${rows.length})</summary>${rows.map((row, index) => `<pre>${display(row)}</pre>${resolutionHtml(row, index, proposal.can_approve && !proposal.review_closed && !busy && !locked)}`).join('')}</details>
      ${proposal.receipt ? `<details open><summary>Verified save receipt</summary><pre>${display(proposal.receipt)}</pre></details>` : ''}
      <p role="status">${escape(status)}</p>
      <button type="button" data-spreadsheet-close ${busy || locked ? 'disabled' : ''}>Close review and return to chat</button>
      ${proposal.can_approve ? `<button type="button" data-spreadsheet-save ${busy || !selected.size ? 'disabled' : ''}>${busy ? 'Saving selected changes…' : locked ? 'Retry the same approval' : 'Approve and save selected changes'}</button>` : (proposal.review_closed ? '<p>This review is closed. Upload again for a fresh comparison before further changes.</p>' : '<p>Jake approval required. This preview cannot be saved by your account.</p>')}
    </section>`;
  }
  function create({ fetch: request, host, scopeHost, onMessage = () => {}, getTenantId = () => null, makeId = async binding => [...new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', new TextEncoder().encode(binding)))].map(byte => byte.toString(16).padStart(2, '0')).join('') }) {
    let scope = null, selectedAo = null, resumable = [];
    let proposal = null, selected = new Set(), saved = new Set(), pendingRequest = null, busy = false, epoch = 0, status = '';
    const getHost = () => typeof host === 'function' ? host() : host;
    function clear() {
      epoch += 1; proposal = null; selected = new Set(); saved = new Set(); pendingRequest = null; busy = false; status = '';
      const el = getHost(); if (el) el.innerHTML = '';
    }
    async function loadScope() {
      const scopeEpoch = epoch;
      const res = await request('/api/v1/max/spreadsheet/scope', { credentials: 'same-origin' });
      if (!res.ok) { clear(); scope = null; throw new Error('Could not verify the spreadsheet review scope. Sign in and try again.'); }
      const next = await res.json();
      if (scopeEpoch !== epoch) throw new Error('Review scope changed. Please retry in the current workspace.');
      if (!next.tenant_id || !next.actor_id || !Array.isArray(next.aos)) throw new Error('Spreadsheet review scope is unavailable.');
      if (scope && (String(scope.tenant_id) !== String(next.tenant_id) || String(scope.actor_id) !== String(next.actor_id))) { clear(); selectedAo = null; }
      scope = next;
      if (!next.aos.some(ao => String(ao.id) === String(selectedAo))) selectedAo = next.ao_id || null;
      const el = typeof scopeHost === 'function' ? scopeHost() : scopeHost;
      if (el) {
        el.innerHTML = `<label>Review accounts assigned to <select data-spreadsheet-ao><option value="">Choose an acquisition operator</option>${next.aos.map(ao => `<option value="${escape(ao.id)}" ${String(ao.id) === String(selectedAo) ? 'selected' : ''}>${escape(ao.name)}</option>`).join('')}</select></label><button type="button" data-spreadsheet-refresh>Find saved previews</button><div data-spreadsheet-resume></div>`;
        el.querySelector('[data-spreadsheet-refresh]')?.addEventListener('click', () => listProposals().catch(err => onMessage(err.message)));
        el.querySelector('[data-spreadsheet-ao]')?.addEventListener('change', event => { selectedAo = event.target.value || null; clear(); resumable = []; const resumeHost = el.querySelector('[data-spreadsheet-resume]'); if (resumeHost) resumeHost.innerHTML = ''; });
      }
      return next;
    }
    async function listProposals() {
      if (!scope || !selectedAo) throw new Error('Choose the acquisition operator first.');
      const requestEpoch = epoch;
      const res = await request(`/api/v1/max/spreadsheet/proposals?ao_id=${encodeURIComponent(selectedAo)}`, { credentials: 'same-origin' });
      const data = await res.json();
      if (requestEpoch !== epoch) return;
      if (!res.ok) throw new Error(data.message || data.error || 'Could not load saved previews.');
      resumable = data.proposals || [];
      const parent = typeof scopeHost === 'function' ? scopeHost() : scopeHost;
      const el = parent?.querySelector('[data-spreadsheet-resume]');
      if (!el) return;
      el.innerHTML = resumable.length ? `<label>Saved preview <select data-resume-choice><option value="">Choose a preview</option>${resumable.map((item, index) => `<option value="${index}">${escape(item.filename || item.id)} · creator ${escape(item.actorId)} · ${escape(item.status)} · ${escape(item.createdAt || '')}</option>`).join('')}</select></label><button type="button" data-resume-load>Open preview / saved receipt</button>` : '<p>No saved previews for this AO.</p>';
      el.querySelector('[data-resume-load]')?.addEventListener('click', () => {
        const value = el.querySelector('[data-resume-choice]').value;
        if (value === '') return;
        resume(resumable[Number(value)]).catch(err => onMessage(err.message));
      });
    }
    async function resume(item) {
      if (busy || pendingRequest) throw new Error('Recover the pending save result before opening another proposal.');
      await loadScope();
      const requestEpoch = epoch;
      if (!scope || !selectedAo || !item?.id || !item.conversationId) throw new Error('Select a saved preview within this AO scope.');
      const res = await request(`/api/v1/max/spreadsheet/proposals/${encodeURIComponent(item.id)}?ao_id=${encodeURIComponent(selectedAo)}&conversation_id=${encodeURIComponent(item.conversationId)}`, { credentials: 'same-origin' });
      const data = await res.json();
      if (requestEpoch !== epoch) return null;
      if (!res.ok) throw new Error(data.message || data.error || 'Could not open this preview.');
      if (!accept(data, requestEpoch)) throw new Error('The saved preview does not match the current review scope.');
      return data;
    }
    async function appendScope(form) {
      await loadScope();
      if (!selectedAo) throw new Error('Choose the acquisition operator whose accounts should be compared.');
      clear();
      form.append('ao_id', selectedAo);
    }
    function checkScope() {
      const tenant = getTenantId() ?? scope?.tenant_id;
      if (proposal && tenant != null && String(tenant) !== String(proposal.tenantId)) { clear(); return false; }
      if (proposal && scope && ((!scope.can_approve && String(proposal.actorId) !== String(scope.actor_id)) || String(proposal.aoId) !== String(selectedAo))) { clear(); return false; }
      if (proposal && scope && !scope.can_approve) proposal.can_approve = false;
      return Boolean(proposal);
    }
    function render() {
      const el = getHost(); if (!el || !checkScope()) return;
      el.innerHTML = proposalHtml(proposal, selected, { busy, locked: Boolean(pendingRequest), saved, status });
      el.querySelectorAll('[data-spreadsheet-operation]').forEach(input => input.addEventListener('change', () => {
        if (busy || pendingRequest) return;
        const id = input.getAttribute('data-spreadsheet-operation');
        if (input.checked) selected.add(id); else selected.delete(id);
        render();
      }));
      el.querySelectorAll('[data-resolution-submit]').forEach(button => button.addEventListener('click', () => {
        const index = Number(button.getAttribute('data-resolution-submit'));
        const row = proposal.plan.rows[index];
        const fieldset = button.closest('[data-spreadsheet-resolution-row]');
        const account = fieldset.querySelector('[data-resolution-account]').value;
        const decision = { sourceHash: proposal.sourceHash, sheet: row.sheet, rowNumber: row.rowNumber, identityEvidence: fieldset.querySelector('[data-resolution-evidence]').value.trim() };
        const provider = fieldset.querySelector('[data-resolution-provider]')?.value;
        if (provider) decision.providerId = provider;
        if (account === '__create__') decision.createAccount = true;
        else if (account) decision.accountId = account;
        if (fieldset.querySelector('[data-resolution-identity]')?.checked) decision.acknowledgedIdentityConflict = true;
        decision.fields = [...fieldset.querySelectorAll('[data-resolution-field]')].filter(input => input.value).map(input => {
          const item = { field: input.getAttribute('data-resolution-field'), decision: input.value };
          const phone = fieldset.querySelector('[data-resolution-phone]')?.value;
          if (item.field === 'phone' && item.decision === 'use_source' && phone) item.value = phone;
          return item;
        });
        decision.contacts = [...fieldset.querySelectorAll('[data-resolution-contact]')].filter(input => input.value).map(input => ({ sourceName: input.getAttribute('data-resolution-contact'), ...(input.value === '__create__' ? { create: true } : { contactId: input.value }) }));
        resolve([decision]).catch(err => onMessage(err.message));
      }));
      el.querySelector('[data-spreadsheet-close]')?.addEventListener('click', () => { if (!busy && !pendingRequest) clear(); });
      el.querySelector('[data-spreadsheet-save]')?.addEventListener('click', () => commit('Save selected changes').catch(err => onMessage(err.message)));
    }
    function accept(data, expectedEpoch = epoch) {
      if (expectedEpoch !== epoch || !data?.spreadsheet_proposal) return false;
      const incoming = data.spreadsheet_proposal;
      if (!incoming.id || !incoming.digest || !incoming.sourceHash || !incoming.tenantId || !incoming.actorId || !incoming.aoId || !incoming.conversationId || !Array.isArray(incoming.plan?.operations)) return false;
      proposal = { ...incoming, can_approve: incoming.can_approve === true || data.can_approve === true };
      selected = new Set(proposal.plan.operations.filter(op => !op.blocked).map(op => op.id));
      saved = new Set(); pendingRequest = null; status = 'Review each proposed change and any held rows before approval.';
      if (incoming.status === 'committed' && incoming.receipt?.committed === true) {
        saved = new Set(incoming.receipt.selectedOperationIds || []); selected = new Set();
        proposal.can_approve = false; proposal.review_closed = true;
        status = 'Saved result recovered from the server. Held and unselected rows remain available below.';
      }
      render(); return checkScope();
    }
    async function resolve(resolutions) {
      await loadScope();
      if (!checkScope() || !proposal.can_approve || proposal.review_closed) throw new Error('Jake must review a current proposal before resolving identities.');
      if (busy || pendingRequest) throw new Error('Recover the pending save result before changing the proposal.');
      if (!Array.isArray(resolutions) || !resolutions.length || resolutions.some(item => String(item.identityEvidence || '').trim().length < 12)) throw new Error('Explain the verified identity evidence before resolving this row.');
      const requestEpoch = epoch;
      busy = true; status = 'Building a new proposal. No CRM changes are being saved.'; render();
      try {
        const res = await request(`/api/v1/max/spreadsheet/proposals/${encodeURIComponent(proposal.id)}/resolve`, { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ao_id: proposal.aoId, conversation_id: proposal.conversationId, source_hash: proposal.sourceHash, proposal_digest: proposal.digest, resolutions }) });
        const data = await res.json();
        if (requestEpoch !== epoch) return null;
        if (!res.ok) throw new Error(data.message || data.error || 'Resolution was not accepted.');
        if (!accept(data, requestEpoch)) throw new Error('A fresh proposal was not returned. Nothing has been saved.');
        onMessage(data.operational_response || 'Fresh proposal ready. Review every selected operation before saving.');
        return data;
      } finally { if (requestEpoch === epoch) { busy = false; render(); } }
    }
    async function commit(text) {
      if (intent(text) !== 'approve') throw new Error('Nothing saved. Review the proposal and explicitly approve selected changes.');
      await loadScope();
      if (!checkScope()) throw new Error('No current proposal. Upload the workbook again for a fresh comparison.');
      if (!proposal.can_approve) throw new Error('Jake approval is required before saving.');
      if (busy) return null;
      if (!selected.size) throw new Error('Select at least one eligible operation.');
      const ops = new Map(proposal.plan.operations.map(op => [op.id, op]));
      for (const id of selected) {
        const op = ops.get(id);
        if (!op || op.blocked || saved.has(id)) throw new Error('The selection includes an unavailable operation.');
        for (const dependency of op.dependsOn || []) {
          if (!selected.has(dependency) && !saved.has(dependency)) throw new Error('Select the required dependent operations together.');
        }
      }
      const requestEpoch = epoch;
      const current = proposal;
      busy = true;
      try {
      pendingRequest ||= { conversation_id: current.conversationId, ao_id: current.aoId, source_hash: current.sourceHash, proposal_digest: current.digest, operation_ids: [...selected].sort(), idempotency_key: await makeId(JSON.stringify({ tenant: scope?.tenant_id, actor: scope?.actor_id, ao: current.aoId, proposal: current.id, digest: current.digest, operations: [...selected].sort() })), text: 'approve selected operations' };
      if (requestEpoch !== epoch) return null;
      status = 'Saving the exact selected operations…'; render();
        const res = await request(`/api/v1/max/spreadsheet/proposals/${encodeURIComponent(current.id)}/commit`, { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(pendingRequest) });
        const data = await res.json();
        if (requestEpoch !== epoch) return null;
        if (!res.ok) {
          if ([401, 403, 404, 409, 410, 422].includes(res.status)) {
            proposal.can_approve = false; proposal.review_closed = true; pendingRequest = null;
            status = 'Approval is no longer valid. Upload the workbook again for a fresh comparison and approval.';
          }
          throw new Error(data.message || data.error || 'Save could not be verified.');
        }
        if (!data.ok || !data.committed) throw new Error('Save has not been confirmed. Retry the same approval to recover its result.');
        pendingRequest.operation_ids.forEach(id => saved.add(id));
        selected = new Set(); pendingRequest = null;
        // Retain all held and unselected rows. Any later change needs fresh baseline approval.
        proposal.can_approve = false; proposal.review_closed = true;
        proposal.receipt = data.spreadsheet_commit || data.spreadsheet_proposal?.receipt;
        status = data.operational_response || 'Selected changes saved. Held and unselected rows remain below; upload again to review further changes.';
        onMessage(status); return data;
      } catch (err) {
        if (requestEpoch === epoch && pendingRequest) status = 'Outcome unconfirmed. Selection is locked; retry uses the same approval key.';
        throw err;
      } finally { if (requestEpoch === epoch) { busy = false; render(); } }
    }
    async function handleText(text) {
      if (!checkScope()) return false;
      if (intent(text) === 'approve') await commit(text);
      else onMessage('Nothing saved. The proposal remains available for review. To revise it, resolve a row or upload the workbook for a fresh comparison. Close the review to return to chat.');
      return true;
    }
    return { accept, commit, resolve, handleText, clear, render, loadScope, listProposals, resume, appendScope, hasPending: checkScope, token: () => epoch };
  }
  return { create, intent, proposalHtml };
}));
