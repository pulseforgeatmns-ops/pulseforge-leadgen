'use strict';
(() => {
  const $ = id => document.getElementById(id);
  const API = '/api/v1/tenant-outreach/anchor-program/preparation-approval/';
  const STORAGE_KEY = 'pulseforge.anchorApproval.pending.v1';
  const PREPARATION_STORAGE_KEY = 'pulseforge.anchorPreparation.attempt.v1';
  const hex = (v, n = 64) => typeof v === 'string' && new RegExp(`^[a-f0-9]{${n}}$`).test(v);
  const state = { context: null, preview: null, receipt: null, busy: false, generation: 0,
    marker: null, prepareAttempt: null, preparedResponse: null, receiptRevision: null,
    storageProblem: false, recoveryOnly: false, dialogAction: null };
  function status(message, tone = '') { $('approvalStatus').textContent = message; $('approvalStatus').dataset.tone = tone; }
  function node(tag, text, className) { const n = document.createElement(tag); if (text != null) n.textContent = String(text); if (className) n.className = className; return n; }
  function facts(parent, rows) { const dl = node('dl', null, 'facts'); for (const [label, value] of rows) { dl.append(node('dt', label), node('dd', value == null ? 'Not asserted' : value)); } parent.append(dl); }
  function resetConsents() { for (const id of ['grantConfirm', 'reviewConfirm', 'recoverySaved', 'prepareConfirm']) $(id).checked = false; }
  function clearReceipt() { state.receipt = null; state.receiptRevision = null; $('receiptResult').replaceChildren();
    $('prepareConfirm').checked = false; $('preparationSection').hidden = true;
    $('preparationIntent').replaceChildren(); $('preparationResult').replaceChildren(); }
  function invalidate(message) {
    state.generation++; state.preview = null; resetConsents();
    const badge = $('previewResult').querySelector('.badge');
    if (badge) badge.textContent = 'Stale preview · not reusable';
    if ($('confirmDialog').open) $('confirmDialog').close(); state.dialogAction = null;
    if (!$('previewSection').hidden && message) status(message, 'error'); controls();
  }
  function validMarker(m) {
    return m && m.version === 1 && ['install', 'revoke'].includes(m.operation)
      && typeof m.manifestId === 'string' && m.manifestId.length > 0 && m.manifestId.length <= 512
      && hex(m.manifestHash) && (m.expectedReviewInitializationHash === null || hex(m.expectedReviewInitializationHash))
      && Object.keys(m).sort().join(',') === 'expectedReviewInitializationHash,manifestHash,manifestId,operation,version';
  }
  function loadMarker() {
    let raw;
    try { raw = sessionStorage.getItem(STORAGE_KEY); }
    catch { state.storageProblem = true; status('Recovery storage is unavailable. Installation and revocation are locked. Read-only previews and exact receipt lookup remain available.', 'error'); return; }
    try { state.marker = raw === null ? null : JSON.parse(raw);
      if (raw !== null && !validMarker(state.marker)) throw Error('invalid_marker');
      if (state.marker) {
        $('receiptManifestId').value = state.marker.manifestId; $('receiptManifestHash').value = state.marker.manifestHash;
        status('Outstanding grant attempt restored. Recovery only: read its exact receipt. No install or replacement will be submitted.', 'error');
      }
    } catch { state.marker = null; state.storageProblem = true; state.recoveryOnly = true; status('Saved recovery locator is invalid. Recovery only: use exact receipt lookup and operator review. No replacement approval can be submitted.', 'error'); }
  }
  function persistMarker(marker) {
    if (state.storageProblem) throw Error('Recovery storage is unavailable. No mutation was sent.');
    try { sessionStorage.setItem(STORAGE_KEY, JSON.stringify(marker));
      if (sessionStorage.getItem(STORAGE_KEY) !== JSON.stringify(marker)) throw Error('storage_not_verified');
      state.marker = marker;
    } catch { state.storageProblem = true; throw Error('Could not save the recovery locator. No mutation was sent.'); }
  }
  function validPreparationMarker(m) {
    return m && m.version === 1 && typeof m.manifestId === 'string' && m.manifestId.trim() === m.manifestId
      && m.manifestId.length > 0 && m.manifestId.length <= 512 && hex(m.manifestHash) && hex(m.revision, 40)
      && hex(m.sourceMissionHash) && hex(m.operationId) && m.missionId === `mission_crm_${m.operationId.slice(0, 32)}`
      && hex(m.expectedReviewInitializationHash)
      && Object.keys(m).sort().join(',') === 'expectedReviewInitializationHash,manifestHash,manifestId,missionId,operationId,revision,sourceMissionHash,version';
  }
  function loadPreparationMarker() {
    let raw;
    try { raw = sessionStorage.getItem(PREPARATION_STORAGE_KEY); }
    catch { state.storageProblem = true; status('Preparation recovery storage is unavailable. Mutations are locked; use read-only preview or receipt lookup.', 'error'); return; }
    try {
      const marker = raw === null ? null : JSON.parse(raw);
      if (raw !== null && !validPreparationMarker(marker)) throw Error('invalid_preparation_marker');
      state.prepareAttempt = marker;
      if (marker) {
        $('receiptManifestId').value = marker.manifestId; $('receiptManifestHash').value = marker.manifestHash;
        status('A preparation attempt is recorded for this tab. Read its exact receipt. No retry, replacement, or new installation is enabled, even after revocation.', 'error');
      }
    } catch { state.storageProblem = true; state.recoveryOnly = true; status('Saved preparation locator is invalid. Recovery only: use exact receipt lookup and operator review.', 'error'); }
  }
  function persistPreparationMarker(marker) {
    if (state.storageProblem || state.prepareAttempt || state.recoveryOnly) throw Error('Preparation is recovery-only. No request was sent.');
    try { sessionStorage.setItem(PREPARATION_STORAGE_KEY, JSON.stringify(marker));
      if (sessionStorage.getItem(PREPARATION_STORAGE_KEY) !== JSON.stringify(marker)) throw Error('storage_not_verified');
      state.prepareAttempt = marker;
    } catch { state.storageProblem = true; throw Error('Could not save the preparation recovery locator. No request was sent.'); }
  }
  function windowCurrent() {
    const p = state.preview?.data.proposal;
    return p && Date.parse(p.validFrom) <= Date.now() && Date.now() < Date.parse(p.expiresAt);
  }
  function canInstall() {
    return state.context && !state.busy && !state.marker && !state.prepareAttempt && !state.storageProblem && state.preview
      && state.preview.data.installable === true && state.preview.data.status === 'previewed'
      && state.preview.data.installed === false && windowCurrent()
      && $('grantConfirm').checked && $('recoverySaved').checked
      && (!state.preview.input.review || $('reviewConfirm').checked);
  }
  function canRevoke() {
    const r = state.receipt;
    return state.context && !state.busy && !state.storageProblem && r?.recorded === true && r.revoked === false
      && r.currentControlMatches === true && ['installed', 'consumed'].includes(r.status)
      && state.marker?.operation !== 'revoke' && $('revokeReference').value.trim().length > 0;
  }
  function preparationAvailable(r = state.receipt) {
    const i = r?.preparationIntent;
    return r?.recorded === true && r.status === 'installed' && r.installed === true && r.revoked === false
      && r.expired === false && r.currentControlMatches === true && r.maintenanceArmed === true
      && r.consumedOperations === 0 && r.consumedItems === 0 && r.preparationOutcome?.status === 'not_recorded'
      && i?.originalSessionMatches === true && Date.parse(i.proposal.validFrom) <= Date.now()
      && Date.now() < Date.parse(i.proposal.expiresAt);
  }
  function canPrepare() {
    return state.context && state.receiptRevision === state.context.revision && !state.busy && !state.prepareAttempt
      && !state.storageProblem && !state.recoveryOnly && state.marker?.operation !== 'revoke'
      && preparationAvailable() && $('prepareConfirm').checked;
  }
  function canConfirm(kind) { return kind === 'install' ? canInstall() : kind === 'revoke' ? canRevoke() : kind === 'prepare' ? canPrepare() : false; }
  function controls() {
    $('previewButton').disabled = !state.context || state.busy || Boolean(state.marker) || Boolean(state.prepareAttempt) || state.recoveryOnly || !$('newApprovalAcknowledgment').checked;
    $('receiptButton').disabled = !state.context || state.busy;
    $('reloadContextButton').disabled = state.busy;
    $('installButton').disabled = !canInstall(); $('revokeButton').disabled = !canRevoke(); $('prepareButton').disabled = !canPrepare();
    $('prepareConfirm').disabled = state.busy || Boolean(state.prepareAttempt) || !preparationAvailable();
    $('confirmSubmit').disabled = state.busy || !canConfirm(state.dialogAction);
    $('confirmCancel').disabled = state.busy;
    for (const id of ['grantConfirm', 'reviewConfirm', 'recoverySaved']) $(id).disabled = state.busy || Boolean(state.marker);
    $('addAdmission').disabled = state.busy || $('admissions').children.length >= 5;
    $('newApprovalAcknowledgment').disabled = state.busy || Boolean(state.marker) || Boolean(state.prepareAttempt) || state.recoveryOnly;
    for (const fieldset of $('intentForm').querySelectorAll('fieldset')) fieldset.disabled = state.busy || Boolean(state.marker) || Boolean(state.prepareAttempt) || state.recoveryOnly;
    for (const id of ['receiptManifestId', 'receiptManifestHash', 'revokeReference']) $(id).disabled = state.busy;
    const reviewed = [...$('admissions').querySelectorAll('[name=reviewSelected]')].some(c => c.checked);
    $('reviewApprovalReference').disabled = !reviewed || state.busy || Boolean(state.marker) || Boolean(state.prepareAttempt) || state.recoveryOnly;
    $('reviewApprovalReference').required = reviewed;
    for (const b of $('admissions').querySelectorAll('[name=removeAdmission]')) b.disabled = state.busy || $('admissions').children.length <= 1 || Boolean(state.marker);
  }
  async function request(path, body, preparation = false) {
    const controller = new AbortController(); const timer = setTimeout(() => controller.abort(), 60000);
    try {
      const response = await fetch(preparation ? '/api/v1/tenant-outreach/anchor-program/prepare-drafts' : API + path, { method: body === undefined ? 'GET' : 'POST', credentials: 'same-origin',
        cache: 'no-store', redirect: 'error', headers: body === undefined ? {} : { 'Content-Type': 'application/json' },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }), signal: controller.signal });
      if (!response.headers.get('content-type')?.includes('application/json')) throw Error('Unexpected server response.');
      const data = await response.json();
      if (!response.ok) throw Object.assign(Error(response.status === 401 || response.status === 403
        ? 'Current administrator session, selected tenant, or origin was rejected. Verify identity; do not repeat a mutation.'
        : 'The server rejected this request. No automatic retry is permitted.'), { response: data, httpStatus: response.status });
      if (!data || typeof data !== 'object' || Array.isArray(data)) throw Error('Unexpected server response.');
      return data;
    } finally { clearTimeout(timer); }
  }
  async function context() {
    const c = await request('context');
    if (!hex(c.revision, 40) || typeof c.actorId !== 'string' || !/^[1-9][0-9]*$/.test(c.actorId)
      || c.tenantId !== '10' || c.role !== 'admin' || Object.keys(c).sort().join(',') !== 'actorId,revision,role,tenantId') throw Error('Current authority could not be verified.');
    return c;
  }
  async function unchangedContext() {
    let c;
    try { c = await context(); }
    catch (e) {
      state.context = null; clearReceipt(); invalidate();
      $('approvalContext').textContent = 'Current authority is no longer verified. Use native sign-in/selection if needed, then verify identity and read any outstanding receipt.';
      throw e;
    }
    if (!state.context || JSON.stringify(c) !== JSON.stringify(state.context)) {
      state.context = null; clearReceipt(); invalidate(); $('approvalContext').textContent = 'Identity, tenant, or deployment changed. Verify identity again; a previous preview cannot be reused.';
      throw Error('Identity or deployment changed. No mutation was sent by this check.');
    }
    return c;
  }
  async function verifyIdentity() {
    if (state.busy) return; state.busy = true; state.context = null; clearReceipt(); invalidate(); controls();
    try { state.context = await context(); $('approvalContext').textContent = `Verified current admin ${state.context.actorId} · Anchor tenant 10 · deployed revision ${state.context.revision}`;
      if (!state.marker && !state.prepareAttempt && !state.storageProblem) status('Identity verified. Enter an exact intent, or look up a saved receipt. This identity check recorded no approval.');
    } catch (e) { state.context = null; $('approvalContext').textContent = 'Blocked. Use native sign-in and Anchor selection, then explicitly verify identity again.'; status(e.message, 'error'); }
    finally { state.busy = false; controls(); }
  }
  function field(id) { return $(id).value.trim(); }
  function utc(id) { const v = field(id); if (!/^\d{4}-\d\d-\d\dT\d\d:\d\d(?::\d\d)?$/.test(v)) throw Error('Enter explicit UTC date and time.'); return new Date(`${v}Z`).toISOString(); }
  function input() {
    const admissions = [...$('admissions').children].map(row => ({ prospectId: row.querySelector('[name=prospectId]').value.trim(), approvalReference: row.querySelector('[name=admissionReference]').value.trim() }));
    const intent = { id: field('manifestId'), programId: field('programId'), sourceMissionId: field('sourceMissionId'),
      validFrom: utc('validFrom'), expiresAt: utc('expiresAt'), approvalReference: field('approvalReference'),
      preparationApprovalReference: field('preparationApprovalReference'), dailyOperationCap: Number(field('dailyOperationCap')),
      dailyItemCap: Number(field('dailyItemCap')), admissions };
    if (new Set(admissions.map(a => a.prospectId)).size !== admissions.length) throw Error('Each admitted prospect ID must be unique.');
    const duration = Date.parse(intent.expiresAt) - Date.parse(intent.validFrom);
    if (!(duration > 0 && duration <= 86400000) || Date.parse(intent.validFrom) > Date.now() || Date.parse(intent.expiresAt) <= Date.now()) throw Error('Use an already-started, unexpired fixed UTC window of no more than 24 hours.');
    const ids = [...$('admissions').children].filter(row => row.querySelector('[name=reviewSelected]').checked).map(row => row.querySelector('[name=prospectId]').value.trim());
    return { revision: state.context.revision, intent, ...(ids.length ? { review: { action: 'initialize_null_review_false', approvalReference: field('reviewApprovalReference'), prospectIds: ids } } : {}) };
  }
  function addAdmission() {
    if ($('admissions').children.length >= 5) return;
    $('admissions').append($('admissionTemplate').content.cloneNode(true));
    for (const [i, row] of [...$('admissions').children].entries()) {
      row.querySelector('[name=prospectId]').setAttribute('aria-label', `Prospect ${i + 1} ID`);
      row.querySelector('[name=admissionReference]').setAttribute('aria-label', `Prospect ${i + 1} admission approval reference`);
      row.querySelector('[name=reviewSelected]').setAttribute('aria-label', `Explicit NULL to false review for prospect ${i + 1}`);
    }
    invalidate('Prospect selection changed. Preview again; no earlier confirmation is reusable.');
  }
  function renderPreview(data, submitted) {
    const out = $('previewResult'); out.replaceChildren(); const p = data.proposal;
    out.append(node('span', data.status === 'already_installed' ? 'Previously installed · read exact receipt'
      : data.installable ? 'Preview ready · this request made no changes' : 'Preview blocked · this request made no changes', 'badge'));
    out.append(node('p', 'Buying intent and canonical eligibility are not asserted. A preview is not preparation approval beyond the exact grant shown.'));
    if (data.blockers.length) { out.append(node('h3', 'Blocking conditions')); const list = node('ul', null, 'blocked'); for (const b of data.blockers) list.append(node('li', b)); out.append(list); }
    facts(out, [['Manifest ID', p.id], ['Manifest hash', data.manifestHash], ['Current approver', data.approverId], ['Deployment revision', data.revision],
      ['Program', p.programId], ['Source mission', p.sourceMissionId], ['UTC window', `${p.validFrom} → ${p.expiresAt}`],
      ['Daily operations / items', `${p.scopes.preparation.dailyCap} / ${p.scopes.preparation.dailyItemCap}`],
      ['Total operations / items', `${p.scopes.preparation.totalCap} / ${p.scopes.preparation.totalItemCap}`],
      ['Consumed baseline operations / items', `${data.consumedOperations} / ${data.consumedItems}`],
      ['Grant approval reference', p.approvalReference], ['Preparation approval reference', p.scopes.preparation.approvalReference],
      ['Sender / inbox', `${p.senderName} · ${p.senderEmail} · ${p.inboxIntegrationId}`], ['Program binding hash', data.programBindingHash],
      ['Source hash', data.sourceMissionHash], ['Identity batch hash', data.identityBatchHash], ['Policy hash', p.policyHash], ['Scope hash', p.scopeHash], ['Quarantine decision hash', p.quarantineDecisionHash]]);
    out.append(node('h3', 'Exact admitted identities'));
    for (const a of p.admissions) {
      const card = node('div', null, 'admission');
      facts(card, [['Prospect', a.prospectId], ['Company', a.companyId], ['Company identity', `${a.companyNameKey} · ${a.companyDomain}`],
        ['Recipient', a.recipientEmail], ['Fingerprint', a.identityFingerprint], ['Admission reference', a.approvalReference],
        ['Proposed review transition', submitted.review?.prospectIds.includes(a.prospectId) ? 'NULL → false · explicitly selected' : 'None · no review initialization requested']]); out.append(card);
    }
    if (submitted.review) facts(out, [['Separate review reference', submitted.review.approvalReference], ['Review confirmation hash', data.reviewConfirmationHash], ['Review audit hash', data.reviewInitializationHash]]);
    else out.append(node('p', 'No prospect review initialization is requested. Ordinary grant installation does not release an unreviewed prospect.', 'boundary'));
    const detail = node('details'); detail.append(node('summary', 'Full exact proposal (read-only)'), node('pre', JSON.stringify(data, null, 2))); out.append(detail);
    $('reviewConfirmLabel').hidden = !submitted.review;
    $('installButton').textContent = submitted.review ? `Confirm grant and ${submitted.review.prospectIds.length} exact review(s)…` : 'Confirm grant installation only…';
    $('previewSection').hidden = false;
    $('receiptManifestId').value = p.id; $('receiptManifestHash').value = data.manifestHash;
    $('recoveryReference').value = `RECEIPT LOCATOR ONLY — not authority\nManifest ID: ${p.id}\nManifest hash: ${data.manifestHash}\nReview audit hash: ${data.reviewInitializationHash || 'No review initialization requested'}\nRevision at preview: ${data.revision}`;
  }
  function validatePreview(d, i) {
    const p = d.proposal;
    if (d.revision !== i.revision || p?.id !== i.intent.id || p.programId !== i.intent.programId || p.sourceMissionId !== i.intent.sourceMissionId
      || p.validFrom !== i.intent.validFrom || p.expiresAt !== i.intent.expiresAt || p.tenantId !== '10' || !hex(d.manifestHash)
      || p.approvalReference !== i.intent.approvalReference || p.scopes?.preparation?.approvalReference !== i.intent.preparationApprovalReference
      || p.scopes?.preparation?.dailyCap !== i.intent.dailyOperationCap || p.scopes?.preparation?.dailyItemCap !== i.intent.dailyItemCap
      || p.scopes?.preparation?.enabled !== true || p.scopes?.email?.enabled !== false || p.scopes?.research?.enabled !== false
      || !Array.isArray(d.blockers) || !Array.isArray(p.admissions) || p.admissions.length !== i.intent.admissions.length
      || p.admissions.some(a => !i.intent.admissions.some(b => a.prospectId === b.prospectId && a.approvalReference === b.approvalReference))
      || !['previewed', 'blocked', 'already_installed'].includes(d.status) || typeof d.installable !== 'boolean'
      || (i.review && (!hex(d.reviewConfirmationHash) || !hex(d.reviewInitializationHash) || !d.reviewInitialization
        || d.reviewInitialization.action !== i.review.action || d.reviewInitialization.approvalReference !== i.review.approvalReference
        || !Array.isArray(d.reviewInitialization.identities)
        || JSON.stringify([...d.reviewInitialization.identities.map(a => a.prospectId)].sort()) !== JSON.stringify([...i.review.prospectIds].sort())))
      || (!i.review && d.reviewInitialization !== undefined)) throw Error('Preview did not match the exact request. Nothing can be installed.');
  }
  async function preview(event) {
    event.preventDefault(); if ($('previewButton').disabled || !$('intentForm').reportValidity()) return;
    invalidate(); clearReceipt(); state.busy = true; controls(); const generation = state.generation;
    try { await unchangedContext(); const submitted = input(); const signature = JSON.stringify(submitted); const data = await request('preview', submitted);
      if (generation !== state.generation || signature !== JSON.stringify(input())) throw Error('Inputs changed while previewing. Discarded stale response.');
      validatePreview(data, submitted); state.preview = { data, input: submitted, signature }; renderPreview(data, submitted);
      status(data.status === 'already_installed' ? 'This exact grant was already installed. This preview made no changes. Read its exact receipt; do not install again.'
        : data.installable ? 'Preview ready for inspection. This request recorded no grant or review.' : 'Preview is blocked. This request applied nothing; installation state may be unknown. Review the conditions below.', data.installable ? '' : 'error');
    } catch (e) { invalidate(); status(e.message, 'error'); }
    finally { state.busy = false; controls(); }
  }
  function pair() { return { manifestId: field('receiptManifestId'), manifestHash: field('receiptManifestHash') }; }
  function matches(a, b) { return a?.manifestId === b?.manifestId && a?.manifestHash === b?.manifestHash; }
  function validateReceipt(r, target) {
    if (!matches(r, target) || !['not_installed', 'installed', 'consumed', 'revoked'].includes(r.status)
      || typeof r.recorded !== 'boolean' || !Array.isArray(r.reviewInitializations)
      || !Number.isSafeInteger(r.reviewInitializationCount) || r.reviewInitializationCount < 0 || r.reviewInitializationCount > 5 || r.reviewInitializationCount !== r.reviewInitializations.length
      || r.reviewInitializations.some(a => a.beforeReview !== null || a.afterReview !== false || typeof a.prospectId !== 'string' || !hex(a.identityFingerprint))
      || (r.recorded && (!hex(r.reviewInitializationHash) || typeof r.revoked !== 'boolean' || r.status === 'not_installed'))
      || (r.recorded && (r.installed !== !r.revoked || (r.revoked !== (r.status === 'revoked'))))
      || (!r.recorded && r.status !== 'not_installed') || (r.status === 'revoked' && r.revoked !== true)
      || (state.marker?.expectedReviewInitializationHash && r.recorded && r.reviewInitializationHash !== state.marker.expectedReviewInitializationHash)) throw Error('Receipt identity or review audit did not match. Recovery remains locked.');
    if (r.recorded && state.marker?.operation === 'install' && state.marker.expectedReviewInitializationHash === null && r.reviewInitializationCount !== 0)
      throw Error('Unexpected review audit for an installation that requested no review. Recovery remains locked.');
    if (state.prepareAttempt && r.recorded && r.reviewInitializationHash !== state.prepareAttempt.expectedReviewInitializationHash)
      throw Error('Preparation review audit did not match the saved locator. Recovery remains locked.');
    if (r.recorded) validatePreparationProjection(r);
  }
  function validatePreparationProjection(r) {
    const i = r.preparationIntent, p = i?.proposal, o = r.preparationOutcome;
    if (!i || !p || p.id !== r.manifestId || p.tenantId !== '10' || p.kind !== 'scoped_resume' || p.version !== 2
      || p.scopes?.preparation?.enabled !== true || p.scopes?.email?.enabled !== false || p.scopes?.research?.enabled !== false
      || !hex(i.sourceMissionHash) || i.sourceMissionHash !== p.preparationAuthority?.sourceMissionHash
      || typeof i.originalSessionMatches !== 'boolean' || !hex(i.expectedOperationId)
      || i.expectedMissionId !== `mission_crm_${i.expectedOperationId.slice(0, 32)}`
      || !Number.isFinite(Date.parse(p.validFrom)) || !Number.isFinite(Date.parse(p.expiresAt))
      || !Array.isArray(p.admissions) || p.admissions.length < 1 || p.admissions.length > 5
      || new Set(p.admissions.map(a => a.prospectId)).size !== p.admissions.length
      || p.admissions.some(a => typeof a.prospectId !== 'string' || typeof a.companyId !== 'string' || typeof a.recipientEmail !== 'string' || !hex(a.identityFingerprint))
      || !o || !['not_recorded', 'consumed_outcome_unknown', 'ready_for_review'].includes(o.status)
      || o.evidenceScope !== 'current_persisted_snapshot' || o.retryAllowed !== false
      || o.providerReadiness !== 'not_checked' || o.sendOutcome !== 'not_checked' || !Array.isArray(o.drafts))
      throw Error('Preparation receipt projection is malformed. No preparation is enabled.');
    if (state.prepareAttempt && (!matches(r, state.prepareAttempt) || i.sourceMissionHash !== state.prepareAttempt.sourceMissionHash
      || i.expectedOperationId !== state.prepareAttempt.operationId || i.expectedMissionId !== state.prepareAttempt.missionId))
      throw Error('Preparation pins did not match the saved attempt. Do not substitute another batch or source.');
    if (o.status !== 'ready_for_review') {
      if (o.drafts.length || (o.operationId !== undefined && o.operationId !== null && o.operationId !== i.expectedOperationId)
        || (o.missionId !== undefined && o.missionId !== null && o.missionId !== i.expectedMissionId))
        throw Error('Unverified preparation outcome must not contain draft content or different operation IDs.');
      return;
    }
    if (o.operationId !== i.expectedOperationId || o.missionId !== i.expectedMissionId || !hex(o.preparedArtifactRevision, 16)
      || !hex(o.draftContentHash) || o.candidateCount !== p.admissions.length || o.executionApproved !== false || o.envelopeAuthorized !== false
      || o.drafts.length !== p.admissions.length || new Set(o.drafts.map(d => d.prospectId)).size !== o.drafts.length
      || o.drafts.some(d => !p.admissions.some(a => a.prospectId === d.prospectId && a.companyId === d.companyId && a.recipientEmail === d.recipientEmail)
        || typeof d.subject !== 'string' || !d.subject.trim() || typeof d.body !== 'string' || !d.body.trim()))
      throw Error('Recorded draft identity or no-send evidence did not match. No draft result is asserted.');
  }
  function renderPreparation(r) {
    if (!r.recorded) return;
    const i = r.preparationIntent, p = i.proposal, o = r.preparationOutcome;
    $('preparationSection').hidden = false;
    const intent = $('preparationIntent'); intent.replaceChildren();
    facts(intent, [['Stored manifest ID', r.manifestId], ['Stored manifest hash', r.manifestHash], ['Pinned source hash', i.sourceMissionHash],
      ['Current deployment revision', state.context.revision], ['Original native session matches', String(i.originalSessionMatches)],
      ['Fixed UTC window', `${p.validFrom} → ${p.expiresAt}`], ['Exact batch size', p.admissions.length],
      ['Daily / total operation caps', `${p.scopes.preparation.dailyCap} / ${p.scopes.preparation.totalCap}`],
      ['Daily / total item caps', `${p.scopes.preparation.dailyItemCap} / ${p.scopes.preparation.totalItemCap}`],
      ['Sender', `${p.senderName} · ${p.senderEmail}`], ['Expected operation ID (not proof of execution)', i.expectedOperationId],
      ['Expected mission ID (not proof of execution)', i.expectedMissionId]]);
    for (const a of p.admissions) facts(intent, [['Admitted prospect / company', `${a.prospectId} / ${a.companyId}`], ['Exact recipient', a.recipientEmail], ['Identity fingerprint', a.identityFingerprint]]);
    const result = $('preparationResult'); result.replaceChildren();
    if (state.prepareAttempt) result.append(node('p', 'This tab has recorded a preparation attempt. Preparation and new installation stay disabled, including after revocation. Readback is not permission to retry.', 'boundary'));
    if (!i.originalSessionMatches) result.append(node('p', 'This is not the original native grant session. You may inspect or revoke, but this session cannot prepare this grant.', 'blocked'));
    if (!preparationAvailable(r) && !state.prepareAttempt && o.status === 'not_recorded') result.append(node('p', 'Preparation is unavailable: the grant must be installed, current, unexpired, unconsumed and bound to this original session. Existing server guards remain decisive.', 'blocked'));
    if (o.status === 'not_recorded') {
      result.append(node('h3', 'No recorded preparation outcome'));
      result.append(node('p', state.prepareAttempt ? 'No matching operation was observed in this snapshot. An outstanding request may still commit; do not retry or replace it.' : 'No preparation consumption is recorded for this grant. This is not a complete eligibility or provider-readiness check.'));
      return;
    }
    if (o.status === 'consumed_outcome_unknown') {
      result.append(node('h3', 'Consumption recorded · draft outcome unknown'));
      result.append(node('p', 'The operation remains charged, but a complete validated draft result is not established. Partial effects may exist. Do not repeat, refund, or substitute a new batch.', 'blocked'));
      return;
    }
    const response = state.preparedResponse;
    if (response && (response.operationId !== o.operationId || response.missionId !== o.missionId
      || response.preparedArtifactRevision !== o.preparedArtifactRevision || response.candidateCount !== o.candidateCount)) {
      result.append(node('p', 'The current artifact snapshot differs from the preparation response. No matching completed result is asserted here; operator review is required. The grant may still be closed.', 'blocked'));
      return;
    }
    result.append(node('h3', 'Drafts ready for human review — current persisted snapshot'));
    result.append(node('p', 'These are the currently recorded draft bytes, not a guarantee of the original lost response. Buyer intent, current capacity, provider readiness and send history are not established. No execution approval or envelope authorization is present in this snapshot.', 'preparation-note'));
    facts(result, [['Operation ID', o.operationId], ['Mission ID', o.missionId], ['Prepared artifact revision', o.preparedArtifactRevision], ['Current draft content hash', o.draftContentHash], ['Evidence scope', o.evidenceScope]]);
    for (const d of o.drafts) {
      const card = node('article', null, 'admission');
      facts(card, [['Prospect / company', `${d.prospectId} / ${d.companyId}`], ['Recipient', d.recipientEmail], ['Sender', `${p.senderName} · ${p.senderEmail}`]]);
      card.append(node('h3', d.subject, 'draft-subject'), node('pre', d.body, 'draft-body')); result.append(card);
    }
  }
  function renderReceipt(r) {
    const out = $('receiptResult'); out.replaceChildren(node('h3', `Durable receipt: ${r.status}`));
    facts(out, [['Manifest ID', r.manifestId], ['Manifest hash', r.manifestHash], ['Recorded grant', String(r.recorded)], ['Revoked', r.revoked == null ? 'Not recorded' : String(r.revoked)],
      ['Expired', r.expired == null ? 'Not recorded' : String(r.expired)], ['Current control matches', r.currentControlMatches == null ? 'Not recorded' : String(r.currentControlMatches)],
      ['Consumed operations / items', `${r.consumedOperations ?? 'Not recorded'} / ${r.consumedItems ?? 'Not recorded'}`], ['Historical initialized reviews', r.reviewInitializationCount], ['Historical review audit hash', r.reviewInitializationHash]]);
    for (const a of r.reviewInitializations) facts(out, [['Historically initialized prospect', a.prospectId], ['Company / recipient', `${a.companyId} · ${a.recipientEmail}`], ['Recorded transition', 'NULL → false; current state not checked'], ['Recorded review reference', a.approvalReference], ['Recorded fingerprint', a.identityFingerprint]]);
    if (!r.recorded) out.append(node('p', 'No committed grant was observed. An outstanding request may still commit. This does not unlock retry or replacement.', 'blocked'));
    else if (!r.revoked && !r.currentControlMatches) out.append(node('p', 'The current control no longer matches this grant. Revocation is blocked by the existing guarded transaction; operator review is required.', 'blocked'));
    if (state.marker?.operation === 'revoke' && r.status !== 'revoked') out.append(node('p', 'An earlier revocation is still unresolved. This snapshot cannot prove that request finished; do not submit it again.', 'blocked'));
  }
  async function receipt(event) {
    event.preventDefault(); if (state.busy || !state.context || !$('receiptForm').reportValidity()) return;
    const target = pair(); clearReceipt(); state.busy = true; controls();
    try {
      if (state.marker && !matches(target, state.marker)) throw Error('Read the outstanding locator exactly; a different grant cannot resolve this attempt.');
      if (state.prepareAttempt && !matches(target, state.prepareAttempt)) throw Error('Read the exact preparation-attempt locator; another grant cannot resolve its outcome.');
      await unchangedContext(); const r = await request('preview', { revision: state.context.revision, receipt: target });
      if (!matches(pair(), target)) throw Error('Receipt fields changed. Discarded stale response.'); validateReceipt(r, target);
      state.receipt = r; state.receiptRevision = state.context.revision; renderReceipt(r); renderPreparation(r);
      if (r.status === 'revoked' && state.marker) { sessionStorage.removeItem(STORAGE_KEY); if (sessionStorage.getItem(STORAGE_KEY) !== null) throw Error('Could not clear the resolved receipt locator.'); state.marker = null; $('newApprovalAcknowledgment').checked = false; }
      status(r.status === 'revoked' ? 'Revocation is durably recorded. Reviews and consumed history are retained.' : r.recorded ? 'Matching grant found. This historical receipt is not permission to prepare or send.' : 'No committed grant observed. Do not retry or replace an uncertain attempt.', r.recorded ? 'success' : 'error');
    } catch (e) { clearReceipt(); status(e.message, 'error'); }
    finally { state.busy = false; controls(); }
  }
  function openConfirmation(kind) {
    if (!canConfirm(kind)) return;
    state.dialogAction = kind; const target = kind === 'install' ? { manifestId: state.preview.data.proposal.id, manifestHash: state.preview.data.manifestHash } : state.receipt;
    $('confirmTitle').textContent = kind === 'install' ? 'Install exactly this grant?' : kind === 'prepare' ? 'Prepare this exact batch once?' : 'Revoke exactly this grant?';
    $('confirmDescription').textContent = kind === 'install'
      ? `This records the displayed preparation-only grant${state.preview.input.review ? ` and ${state.preview.input.review.prospectIds.length} explicitly selected NULL → false reviews` : ', with no review initialization'}. No draft, activation or send will run.`
      : kind === 'prepare' ? `This separately authorizes one guarded preparation attempt for exactly ${target.preparationIntent.proposal.admissions.length} admitted prospect(s). Consumption may remain charged even if preparation fails or the response is lost. No research, provider verification, execution approval or send will run.`
      : 'This closes the exact grant. It will not undo prospect reviews, remove consumption, restore settings or authorize a retry.';
    $('confirmIdentity').textContent = `Manifest ID: ${target.manifestId}\nManifest hash: ${target.manifestHash}`
      + (kind === 'prepare' ? `\nSource hash: ${target.preparationIntent.sourceMissionHash}\nRevision: ${state.receiptRevision}\nExpected operation: ${target.preparationIntent.expectedOperationId}` : '');
    $('confirmSubmit').textContent = kind === 'install' ? 'Install this exact grant' : kind === 'prepare' ? 'Prepare this exact batch once' : 'Revoke this exact grant';
    controls(); $('confirmDialog').showModal(); $('confirmCancel').focus();
  }
  async function prepare() {
    if (!canPrepare() || state.dialogAction !== 'prepare') return;
    const approved = state.receipt, intent = approved.preparationIntent, revision = state.receiptRevision;
    const target = { manifestId: approved.manifestId, manifestHash: approved.manifestHash };
    const generation = state.generation, signature = JSON.stringify(intent);
    state.busy = true; controls(); let dispatched = false;
    $('confirmDescription').textContent = 'Checking current identity and the exact unchanged receipt for your separate preparation decision. Do not repeat this request.';
    try {
      await unchangedContext();
      const fresh = await request('preview', { revision, receipt: target });
      validateReceipt(fresh, target);
      if (generation !== state.generation || !$('confirmDialog').open || state.dialogAction !== 'prepare'
        || !$('prepareConfirm').checked || state.receipt !== approved || !matches(pair(), target)
        || state.context.revision !== revision || state.receiptRevision !== revision
        || signature !== JSON.stringify(fresh.preparationIntent) || !preparationAvailable(fresh))
        throw Error('The exact preparation receipt, session, window or confirmation changed. No preparation request was sent. Read the receipt again.');
      const marker = { version: 1, ...target, revision, sourceMissionHash: intent.sourceMissionHash,
        operationId: intent.expectedOperationId, missionId: intent.expectedMissionId,
        expectedReviewInitializationHash: approved.reviewInitializationHash };
      persistPreparationMarker(marker);
      dispatched = true; state.preparedResponse = null; clearReceipt(); $('confirmDialog').close(); state.dialogAction = null;
      const r = await request('', { revision, manifestHash: target.manifestHash, sourceMissionHash: intent.sourceMissionHash }, true);
      if (r.status !== 'ready_for_review' || r.operationId !== marker.operationId || r.missionId !== marker.missionId
        || !hex(r.preparedArtifactRevision, 16) || r.candidateCount !== intent.proposal.admissions.length
        || r.readyForReview !== true || r.executionApproved !== false || r.envelopeAuthorized !== false
        || r.sent !== 0 || r.providerCalls !== 0 || r.providerReadiness !== 'not_checked' || r.retryAllowed !== false)
        throw Error('Preparation response did not match the exact operation.');
      state.preparedResponse = r;
      status('Preparation returned a matching review-ready result. Read the exact receipt to verify the persisted outcome and view drafts. No retry is enabled.', 'success');
    } catch (e) {
      if (dispatched) status(e.response?.writesMayHaveOccurred === false
        ? 'Preparation was rejected before canonical invocation. No result is asserted. Read the exact receipt; this page will not retry or replace the attempt.'
        : 'Preparation outcome is uncertain or incomplete. Consumption may already be charged. Read the exact receipt; do not retry, refund, or create a replacement grant.', 'error');
      else { clearReceipt(); if ($('confirmDialog').open) $('confirmDialog').close(); state.dialogAction = null; status(e.message, 'error'); }
    } finally {
      if (dispatched) { invalidate(); $('newApprovalAcknowledgment').checked = false;
        $('receiptManifestId').value = target.manifestId; $('receiptManifestHash').value = target.manifestHash; }
      state.busy = false; controls();
    }
  }
  async function mutate() {
    if (state.dialogAction === 'prepare') return prepare();
    const kind = state.dialogAction; if (kind === 'install' ? !canInstall() : kind !== 'revoke' || !canRevoke()) return;
    const approved = state.preview; const target = kind === 'install' ? { manifestId: approved.data.proposal.id, manifestHash: approved.data.manifestHash } : { manifestId: state.receipt.manifestId, manifestHash: state.receipt.manifestHash };
    const generation = state.generation, revokeReference = field('revokeReference'), approvedReceipt = state.receipt;
    const audit = kind === 'install' ? approved.data.reviewInitializationHash || null : state.receipt.reviewInitializationHash;
    state.busy = true; controls(); $('confirmDescription').textContent = 'Checking current authority for your explicit submission. Do not repeat this request. If a response is lost, use the exact receipt locator.'; let dispatched = false;
    try {
      await unchangedContext();
      if (generation !== state.generation || !$('confirmDialog').open || state.dialogAction !== kind) throw Error('Confirmation changed. No mutation was sent.');
      if (kind === 'install' && (!state.preview || approved.signature !== JSON.stringify(input()) || !windowCurrent())) throw Error('The preview changed or expired. Nothing was sent.');
      if (kind === 'install' && (!$('grantConfirm').checked || !$('recoverySaved').checked || (approved.input.review && !$('reviewConfirm').checked))) throw Error('Explicit acknowledgments changed. No mutation was sent.');
      if (kind === 'revoke' && (!matches(pair(), target) || state.receipt !== approvedReceipt || !revokeReference || field('revokeReference') !== revokeReference)) throw Error('Revocation inputs changed. Nothing was sent.');
      const body = kind === 'install' ? { revision: state.context.revision, proposal: approved.data.proposal, manifestHash: target.manifestHash,
        ...(approved.input.review ? { reviewInitialization: approved.data.reviewInitialization, reviewConfirmationHash: approved.data.reviewConfirmationHash } : {}) }
        : { revision: state.context.revision, ...target, approvalReference: revokeReference };
      persistMarker({ version: 1, operation: kind, ...target, expectedReviewInitializationHash: audit });
      dispatched = true; clearReceipt(); $('confirmDialog').close(); state.dialogAction = null;
      const r = await request(kind, body);
      if (!matches(r, target) || (kind === 'install' ? !['installed', 'already_installed'].includes(r.status) || r.installed !== true
        || r.reviewInitializationCount !== (approved.input.review?.prospectIds.length || 0)
        || (audit && r.reviewInitializationHash !== audit) : !['revoked', 'already_revoked'].includes(r.status) || r.revoked !== true)) throw Error('Unexpected mutation response.');
      status(`${kind === 'install' ? 'Installation' : 'Revocation'} response received. Read the exact durable receipt now. This request did not invoke preparation or sending.`, 'success');
    } catch (e) {
      status(dispatched ? 'Outcome uncertain. Do not repeat or replace this request. Read the exact receipt; a missing grant does not prove rollback.' : e.message, 'error');
    } finally {
      if (dispatched) { invalidate(); $('newApprovalAcknowledgment').checked = false; $('receiptManifestId').value = target.manifestId; $('receiptManifestHash').value = target.manifestHash; }
      state.busy = false; controls();
    }
  }
  $('intentForm').addEventListener('submit', preview);
  $('intentForm').addEventListener('input', () => { invalidate('Intent changed. The previous preview and confirmations are no longer usable.'); controls(); });
  $('admissions').addEventListener('click', e => { if (e.target.name === 'removeAdmission' && $('admissions').children.length > 1) { e.target.closest('[data-admission]').remove(); invalidate('Prospect removed. Preview again.'); controls(); } });
  $('addAdmission').addEventListener('click', addAdmission);
  for (const id of ['newApprovalAcknowledgment', 'grantConfirm', 'reviewConfirm', 'recoverySaved', 'revokeReference', 'prepareConfirm']) $(id).addEventListener('input', controls);
  for (const id of ['receiptManifestId', 'receiptManifestHash']) $(id).addEventListener('input', () => { clearReceipt(); controls(); });
  $('receiptForm').addEventListener('submit', receipt);
  $('installButton').addEventListener('click', () => openConfirmation('install'));
  $('revokeButton').addEventListener('click', () => openConfirmation('revoke'));
  $('prepareButton').addEventListener('click', () => openConfirmation('prepare'));
  $('confirmCancel').addEventListener('click', () => $('confirmDialog').close());
  $('confirmDialog').addEventListener('close', () => { state.dialogAction = null; controls(); });
  $('confirmDialog').addEventListener('cancel', event => { if (state.busy) event.preventDefault(); });
  $('confirmSubmit').addEventListener('click', mutate);
  $('reloadContextButton').addEventListener('click', verifyIdentity);
  $('downloadRecovery').addEventListener('click', () => { if (!$('recoveryReference').value) return;
    const url = URL.createObjectURL(new Blob([$('recoveryReference').value + '\n'], { type: 'text/plain' }));
    const a = node('a'); a.href = url; a.download = 'anchor-approval-receipt-locator.txt'; a.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  });
  window.addEventListener('beforeunload', event => { if (state.marker || state.prepareAttempt || state.busy) { event.preventDefault(); event.returnValue = ''; } });
  window.addEventListener('pageshow', event => { $('newApprovalAcknowledgment').checked = false; $('prepareConfirm').checked = false; resetConsents();
    if (event.persisted) { invalidate('Restored page: previous confirmations were discarded. Read any outstanding receipt.'); loadMarker(); loadPreparationMarker(); void verifyIdentity(); } controls(); });
  addAdmission(); $('newApprovalAcknowledgment').checked = false; $('prepareConfirm').checked = false; resetConsents(); loadMarker(); loadPreparationMarker(); controls(); void verifyIdentity();
  setInterval(controls, 1000); // Local expiry only. No background request or mutation.
})();
