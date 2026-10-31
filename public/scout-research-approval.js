'use strict';
// Display-only controller. Every decision is made by the existing server
// endpoints; this page previews, asks for explicit confirmation, and shows
// exactly what the server returned. It stores nothing in localStorage or
// sessionStorage and never handles session tokens.
(function () {
  const API = '/api/v1/tenant-outreach/anchor-program/';
  const CONTEXT = '/api/v1/admin/scout-research-approval/context';
  const CONFIRM = '/api/v1/auth/fresh-confirmation';
  const PURPOSE = 'scout_research_approval';
  const REF = /^[A-Za-z0-9_.:-]{1,120}$/;
  const HEX64 = /^[a-f0-9]{64}$/;
  const SEGMENT_LABELS = { managed_facilities: 'Managed facilities', restaurant_foh: 'Restaurant front of house' };
  const MESSAGES = {
    research_only_grant_forbidden: 'This needs a verified administrator session with Anchor (tenant 10) selected, signed in directly on this site. Nothing was recorded.',
    fresh_confirmation_required: 'Your password confirmation was missing or expired. Nothing was recorded. Try again and enter your password.',
    fresh_confirmation_rejected: 'That password was not accepted. Nothing was recorded.',
    fresh_confirmation_locked: 'Too many incorrect passwords. Password confirmation is paused for 15 minutes. Nothing was recorded.',
    fresh_confirmation_forbidden: 'Password confirmation must come from this page on the Pulseforge site.',
    fresh_confirmation_invalid: 'Enter your password to confirm.',
    research_mission_operator_invalid: 'Your administrator session is not accepted as the approver. Sign in again directly as the Anchor administrator.',
    research_mission_schema_unavailable: 'The research mission tables are not installed. The operator migration has not been applied.',
    research_mission_schema_guards_missing: 'The research mission safety triggers are missing. Do not approve until the migration is verified.',
    research_mission_program_missing: 'There is no active Anchor outbound program to copy the approved targeting from.',
    research_mission_predecessor_missing: 'The approved facility-buyer mission could not be found.',
    research_mission_changed: 'The approved targeting changed since your preview. Preview again before approving.',
    research_mission_conflict: 'This approval reference or mission is already recorded differently. Use a new reference only after checking the existing one.',
    research_mission_preview_invalid: 'Enter a valid mission approval reference.',
    research_mission_explicit_approval_required: 'The approval was not the exact previewed mission. Preview again.',
    research_mission_exact_confirmation_required: 'The mission hash did not match the previewed mission. Preview again.',
    research_only_disabled: 'Research-only grants are switched off on this deployment (SCOUT_RESEARCH_ONLY_GRANTS_ENABLED).',
    research_only_closed_environment_required: 'Recovery maintenance, the provider pause and the outbound restriction must all be in place. They are not, so no grant can be installed.',
    research_only_discovery_memory_required: 'Durable discovery memory is not enabled, so research cannot run safely.',
    research_only_revision_mismatch: 'The running revision differs from the one this grant would be pinned to.',
    research_only_schema_unavailable: 'The research-only grant tables are not installed. The operator migration has not been applied.',
    research_only_schema_guards_missing: 'The grant safety triggers are missing. Do not approve until the migration is verified.',
    research_only_operator_invalid: 'Your administrator session is not accepted as the approver. Sign in again directly as the Anchor administrator.',
    research_only_program_changed: 'The Anchor outbound program changed since your preview. Preview again.',
    research_only_mission_changed: 'The outbound mission changed since your preview. Preview again.',
    research_only_research_mission_changed: 'The research mission is missing or changed. Approve the research mission first, then preview again.',
    research_only_quarantine_ambiguous: 'The recovery quarantine is not in the single expected state.',
    research_only_closed_control_required: 'The recovery control is not in the closed state this grant requires.',
    research_only_other_authority_open: 'Another recovery or resume authority is open. Research cannot be granted alongside it.',
    research_only_other_grant_open: 'Another research-only grant is still open. Revoke it before approving a new one.',
    research_only_grant_window_inactive: 'The grant window is not active (it has not started or has already ended).',
    research_only_grant_conflict: 'This approval reference is already recorded for a different grant.',
    research_only_grant_revoked: 'This grant was revoked and cannot be reinstalled.',
    research_only_grant_not_found: 'No grant was found with that ID.',
    research_only_awaiting_cycle_release: 'The released allowance is used. A separate administrator approval is required for the next research cycle.',
    research_only_release_initial_missing: 'This older grant has no initial cycle release. Revoke it and approve a new grant; no release is backfilled.',
    research_only_release_not_due: 'An approved cycle is still unused or running. Review the next release after it finishes.',
    research_only_release_allowance_changed: 'The allowance changed after this preview. Refresh status and review again.',
    research_only_release_invalid: 'This must be an exact one-cycle release within the original grant ceiling.',
    research_only_release_exact_confirmation_required: 'The release did not match the preview. Review again.',
    research_only_release_conflict: 'This release reference is already recorded differently. Refresh status before trying again.',
    research_only_cycles_exhausted: 'All eight authorized cycles are consumed. A release cannot extend the grant ceiling.',
    research_only_retrospective_grant_required: 'Look up the exact grant before downloading its research records.',
    research_only_retrospective_mission_unavailable: 'The recorded research mission is unavailable. No export was produced.',
    research_only_retrospective_limit_exceeded: 'This cohort exceeds the bounded download size. An operator must review its size before export.',
    research_only_retrospective_snapshot_incomplete: 'The complete cohort could not be read. No partial export was produced.',
    research_only_preview_invalid: 'Check the grant fields: reference, a UTC expiry and a statement are all required.',
    research_only_manifest_invalid: 'The grant terms are outside the allowed limits (for example an expiry more than 7 days away).',
    research_only_explicit_approval_required: 'The approval was not the exact previewed grant. Preview again.',
    research_only_exact_confirmation_required: 'The grant hash did not match the previewed grant. Preview again.',
    research_only_explicit_revocation_required: 'Enter a valid revocation reference.',
    research_only_commit_uncertain: 'The server could not confirm whether the change was saved.',
    research_mission_commit_uncertain: 'The server could not confirm whether the change was saved.',
  };

  const $ = id => document.getElementById(id);
  const state = { context: null, mission: null, grantPreview: null, grant: null, busy: false };

  function el(tag, attrs, ...children) {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(attrs || {})) {
      if (key === 'class') node.className = value; else node.setAttribute(key, value);
    }
    for (const child of children.flat()) if (child != null) node.append(child instanceof Node ? child : String(child));
    return node;
  }
  function facts(pairs) {
    const dl = el('dl', { class: 'facts' });
    for (const [label, value, mono] of pairs) dl.append(el('dt', null, label), el('dd', mono ? { class: 'mono' } : null, value ?? '—'));
    return dl;
  }
  // Exact from integer micros: binary floats turn 1.845 into 1.84 under toFixed(2).
  // At least two decimals, never rounded, so $1.845 stays $1.845 and $5 is $5.00.
  function dollars(micros) {
    const value = Math.max(0, Math.trunc(Number(micros) || 0));
    const fraction = String(value % 1e6).padStart(6, '0').replace(/0+$/, '').padEnd(2, '0');
    return `$${Math.floor(value / 1e6)}.${fraction}`;
  }
  function utc(value) {
    if (!value) return '—';
    const date = new Date(value);
    return Number.isFinite(+date) ? `${date.toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, '')} UTC` : String(value);
  }
  function say(text, tone) {
    const status = $('status');
    status.textContent = text || '';
    if (tone) status.dataset.tone = tone; else delete status.dataset.tone;
    if (text) status.focus({ preventScroll: false });
  }
  function describeCode(code) { return MESSAGES[code] || (code ? `The server refused this request (${code}).` : 'The server refused this request.'); }

  class ApiError extends Error {
    constructor(status, data) { super('api_error'); this.status = status; this.data = data || {}; }
  }
  async function call(path, body, method = 'POST') {
    let response;
    try {
      response = await fetch(path, { method, credentials: 'same-origin', cache: 'no-store',
        headers: body ? { 'Content-Type': 'application/json', Accept: 'application/json' } : { Accept: 'application/json' },
        body: body ? JSON.stringify(body) : undefined });
    } catch { throw new ApiError(0, { error: 'network_unavailable' }); }
    let data = null;
    try { data = await response.json(); } catch { data = null; }
    if (!response.ok) throw new ApiError(response.status, data);
    return data;
  }
  function errorBox(error, { mutation } = {}) {
    const data = error.data || {};
    const code = data.code || data.error;
    const known = Boolean(code && MESSAGES[code]);
    let text;
    if (known) text = MESSAGES[code];
    else if (error.status === 0) text = mutation
      ? 'The connection dropped before a reply arrived. The change may or may not have been saved. Refresh the status before trying again.'
      : 'Could not reach the server. Check your connection and try again.';
    else if (error.status === 401) text = 'You are signed out. Sign in again, then reopen this page.';
    else if (error.status === 403 && !data.code && data.error === 'Forbidden') text = 'Only administrators can use this page.';
    else text = describeCode(code);
    const box = el('div', { class: 'error-box', role: 'alert' }, el('strong', null, 'Not completed. '), text);
    if (code) box.append(el('p', { class: 'help mono' }, `HTTP ${error.status || '—'} · ${code}`));
    if (data.writesMayHaveOccurred || (mutation && error.status === 0)) {
      box.append(el('p', null, el('strong', null, 'Outcome uncertain: '),
        'do not resubmit. Refresh the grant status or preview the mission again to see what was recorded.'));
    }
    if (error.status === 401 && !known) box.append(el('p', null, el('a', { href: '/login' }, 'Sign in')));
    return box;
  }
  function blockerBox(blockers) {
    if (!blockers || !blockers.length) return null;
    return el('div', { class: 'blockers', role: 'alert' }, el('h3', null, 'Blocked — approval is disabled'),
      el('ul', { class: 'list' }, blockers.map(code => el('li', null, describeCode(code), ' ', el('span', { class: 'mono help' }, `(${code})`)))));
  }
  function ready() { return Boolean(state.context?.approvalReady) && !state.busy; }

  // ---------- authority ----------
  async function loadAuthority() {
    const box = $('authority');
    box.textContent = 'Checking your administrator session…';
    try {
      const ctx = await call(CONTEXT, null, 'GET');
      state.context = ctx;
      const tenant = ctx.anchorSelected ? 'Anchor (tenant 10)' : (ctx.activeClientId ? `tenant ${ctx.activeClientId} — not Anchor` : 'no tenant selected');
      box.replaceChildren(el('span', { class: ctx.approvalReady ? 'ok' : 'blocked' }, ctx.approvalReady ? 'Ready to approve. ' : 'Approvals unavailable. '),
        `Signed in as ${ctx.name || 'administrator'} (${ctx.role}). Active workspace: ${tenant}.`,
        ctx.impersonating ? ' You are acting as another user.' : '',
        ctx.rememberedSession ? ' This sign-in is remembered on this device.' : '');
      $('authorityHelp').hidden = Boolean(ctx.approvalReady);
      // Lets password managers match the saved login in the confirm dialog.
      $('confirmUsername').value = ctx.email || '';
    } catch (error) {
      state.context = null;
      box.replaceChildren(errorBox(error));
      $('authorityHelp').hidden = false;
    }
    sync();
  }

  // ---------- grant status ----------
  function renderGrant(grant) {
    const area = $('grantStatus');
    state.grant = grant;
    if (!grant || grant.status === 'not_installed') {
      area.replaceChildren(el('p', null, el('span', { class: 'badge' }, 'No open grant'),
        'No research-only grant is open. Research cycles cannot run.'), el('p', { class: 'help' }, `Checked ${utc(grant?.observedAt)}.`));
      $('revokeArea').hidden = true;
      $('releaseArea').hidden = true;
      return sync();
    }
    const tone = { installed: 'good', awaiting_cycle_release: 'warn', expired: 'warn', exhausted: 'warn', revoked: 'bad' }[grant.status] || '';
    const label = { installed: 'Active', awaiting_cycle_release: 'Awaiting cycle release', expired: 'Expired', exhausted: 'All cycles used', revoked: 'Revoked' }[grant.status] || grant.status;
    const max = Number(grant.maxCycles || 0), used = Number(grant.cyclesConsumed || 0);
    const fill = el('span');
    // CSSOM, not a style attribute: the page CSP forbids inline styles.
    fill.style.width = `${max ? Math.min(100, Math.round(used / max * 100)) : 0}%`;
    const meter = el('div', { class: 'meter', role: 'img', 'aria-label': `${used} of ${max} cycles used` }, fill);
    const today = new Date().toISOString().slice(0, 10);
    const todaySpend = (grant.spend || []).filter(row => row.day === today).reduce((sum, row) => sum + Number(row.micros || 0), 0);
    const notes = {
      expired: 'The grant has expired. No further cycles can be claimed. Revoke it to close it before approving a new grant.',
      exhausted: 'All 8 cycles are used. No further cycles can be claimed. Revoke it to close it before approving a new grant.',
      revoked: `Revoked ${utc(grant.revokedAt)}${grant.revokedBy ? ` by ${grant.revokedBy}` : ''}. Cycles and spend stay on record.`,
      installed: 'Cycles run only from the Max control loop, within these limits.',
      awaiting_cycle_release: grant.releasedCycles ? 'No additional cycle can be claimed without a separate administrator release.'
        : 'This older grant has no initial release. Revoke it and approve a new grant.',
    };
    area.replaceChildren(
      el('p', null, el('span', { class: `badge ${tone}` }, label), notes[grant.status] || ''),
      el('p', { class: 'help' }, `Cycles used: ${used} of ${max}`), meter,
      facts([
        ['Grant ID', grant.grantId, true],
        ['Approval reference', grant.approvalRef, true],
        ['Reserved Google spend', `${dollars(grant.reservedUsdMicros)} total · ${dollars(todaySpend)} today (UTC) — upper-bound reservations, not Google’s invoice`],
        ['Google limit', `${dollars(grant.dailyGoogleUsdMicros)} per UTC day`],
        ['Cycles remaining', String(grant.cyclesRemaining ?? Math.max(0, max - used))],
        ['Released cycles', String(grant.releasedCycles ?? 0)],
        ['Released cycles remaining', String(grant.releasedCyclesRemaining ?? 0)],
        ['Window', `${utc(grant.notBefore)} → ${utc(grant.expiresAt)}`],
        ['Revision pin', grant.expectedRevision, true],
        ['Research mission', grant.researchMissionId, true],
        ['Installed', utc(grant.installedAt)],
        ['Checked', utc(grant.observedAt)],
      ]),
      ...renderCandidates(grant.candidates),
      el('h3', null, 'Cycle release history'),
      el('ul', { class: 'list' }, (grant.releases || []).map(r => el('li', null,
        `${r.previous_allowance} → ${r.released_cycles} · administrator ${r.administrator_id} · ${utc(r.created_at)} · ${r.approval_reference}`))));
    $('revokeArea').hidden = grant.status === 'revoked';
    $('releaseArea').hidden = grant.status !== 'awaiting_cycle_release' || !grant.releasedCycles || grant.cycleInProgress;
    sync();
  }

  // ---------- candidates ----------
  const OUTCOMES = {
    queued: 'Accepted by admission',
    policy_excluded: 'Excluded by mission policy',
    business_evidence_not_authorized: 'Awaiting website evidence — parked (investigation not authorised)',
    evidence_budget_deferred: 'Awaiting website evidence — re-checked every cycle (earlier behaviour)',
    unresolved_business_evidence: 'Website gave no business-type evidence',
    business_evidence_dependency: 'Website evidence unavailable',
    transient_business_evidence_failure: 'Website fetch failed; retry later',
    ownership_policy_observed: 'Already owned, contacted or assigned in Anchor',
    ownership_unresolved: 'Ownership unresolved',
    geography_unresolved: 'Outside the approved cities',
    identity_incomplete: 'Missing business name or website',
  };
  const STATES = { handled: 'Done', deferred: 'Waiting', blocked: 'Parked', pending: 'In progress' };
  const count = value => (Number.isSafeInteger(value) ? String(value) : '—');
  function table(headings, rows) {
    return el('div', { class: 'table-wrap' }, el('table', headings.length > 3 ? { class: 'wide-table' } : null,
      el('thead', null, el('tr', null, headings.map(h => el('th', { scope: 'col' }, h)))),
      el('tbody', null, rows.map(cells => el('tr', null, cells.map(cell => el('td', null, cell ?? '—')))))));
  }
  function tally(items) {
    return el('div', { class: 'tally' }, items.map(([label, value, help]) =>
      el('div', null, el('strong', null, value), el('span', null, label), help ? el('span', { class: 'help' }, help) : null)));
  }
  function renderCandidates(candidates) {
    if (!candidates) return [];
    const retained = candidates.retained || { total: 0, rows: [] };
    const ledger = candidates.ledger;
    const nodes = [el('h3', null, 'Candidates')];
    nodes.push(tally([
      ['Retained in the research ledger', count(retained.total), 'Records kept for later review. Not in enrichment.'],
      ['Accepted by admission', ledger ? count(ledger.acceptedTickets) : '—', 'Passed the business-type, segment and city checks.'],
      ['Accepted, not retained', ledger ? count(ledger.acceptedNotRetained) : '—', 'Already retained or already in Anchor’s queue.'],
      ['Awaiting website evidence', ledger ? count(ledger.awaitingBusinessEvidence?.total) : '—', 'Business type not yet established. Kept, not rejected.'],
    ]));
    nodes.push(el('p', { class: 'help' }, 'Nothing on this list has been enriched, prepared or contacted. Moving a retained candidate into enrichment is a separate, explicit operator step.'));
    const perCycle = candidates.perCycle || [];
    if (perCycle.length) {
      nodes.push(el('h3', null, 'By cycle'), table(['Cycle', 'Result', 'Evaluated', 'Accepted', 'Retained'],
        perCycle.map(row => [String(row.cycleNumber), row.state, count(row.evaluatedCandidates), count(row.acceptedCandidates), count(row.retainedCandidates)])));
      if (perCycle.some(row => row.acceptedCandidates == null))
        nodes.push(el('p', { class: 'help' }, '“—” means that cycle ran before accepted and evaluated counts were recorded. The totals above come from the durable records.'));
    }
    if (ledger) {
      const rows = Object.entries(ledger.byReason || {}).sort((a, b) => b[1] - a[1]);
      nodes.push(el('h3', null, 'Outcome of every discovered business'),
        el('p', { class: 'help' }, `${ledger.total} unique businesses for this search plan (fingerprint ${String(ledger.fingerprint || '').slice(0, 12)}…). Any other Scout pass on the same plan shares these counts.`),
        table(['Outcome', 'Businesses'], rows.map(([reason, n]) => [OUTCOMES[reason] || reason, String(n)])));
    }
    if (retained.rows?.length) {
      nodes.push(el('details', null, el('summary', null, `Retained businesses (${retained.total})`),
        table(['Cycle', 'Business', 'Website', 'Type', 'Location'],
          retained.rows.map(row => [String(row.cycleNumber), row.company, row.domain, row.vertical, row.location])),
        retained.listed < retained.total ? el('p', { class: 'help' }, `Showing ${retained.listed} of ${retained.total}.`) : null));
    }
    const awaiting = ledger?.awaitingBusinessEvidence;
    if (awaiting?.rows?.length) {
      nodes.push(el('details', null, el('summary', null, `Businesses awaiting website evidence (${awaiting.total})`),
        table(['Business', 'Location', 'Google place types', 'Found by query', 'Status'],
          awaiting.rows.map(row => [row.name, row.location, (row.placeTypes || []).join(', ') || '—', row.discoveryQuery,
            row.reason === 'business_evidence_not_authorized' ? 'Parked' : 'Re-checked each cycle'])),
        awaiting.listed < awaiting.total ? el('p', { class: 'help' }, `Showing ${awaiting.listed} of ${awaiting.total}.`) : null));
    }
    return nodes;
  }
  async function refreshGrant(grantId) {
    const area = $('grantStatus');
    area.replaceChildren(el('p', { class: 'help' }, 'Loading…'));
    try { renderGrant(await call(API + 'research-only-grant/inspect', grantId ? { grantId } : {})); }
    catch (error) { state.grant = null; area.replaceChildren(errorBox(error)); $('revokeArea').hidden = true; sync(); }
  }

  // ---------- mission ----------
  function renderMission(preview) {
    state.mission = preview;
    const area = $('missionResult');
    const segments = preview.eligibleSubsegments || [];
    const cities = preview.cities || [];
    const status = { previewed: ['Ready to approve', 'good'], already_installed: ['Already approved', 'good'], blocked: ['Blocked', 'bad'] }[preview.status]
      || [preview.status, ''];
    const nodes = [
      el('p', null, el('span', { class: `badge ${status[1]}` }, status[0]),
        preview.outboundEligible === false ? el('span', { class: 'badge' }, 'Not outbound-eligible') : el('span', { class: 'badge bad' }, 'Outbound-eligible — do not approve')),
      el('h3', null, 'Segments'),
      el('div', null, segments.map(s => el('span', { class: 'badge' }, SEGMENT_LABELS[s] || s))),
      el('h3', null, `Cities (${cities.length})`),
      el('ul', { class: 'list' }, cities.map(city => el('li', null, city))),
      facts([
        ['Mission ID', preview.mission?.id, true],
        ['Mission hash', preview.missionHash, true],
        ['Targeting hash', preview.targetingHash, true],
        ['Copied from mission', preview.predecessorMissionId, true],
        ['Outbound program', preview.programId, true],
        ...(preview.installedMissionHash ? [['Approved hash on record', preview.installedMissionHash, true]] : []),
      ]),
    ];
    if (cities.length !== 8) nodes.push(el('p', { class: 'blocked' }, `Expected eight cities, the server returned ${cities.length}. Do not approve.`));
    const blockers = blockerBox(preview.blockers);
    if (blockers) nodes.push(blockers);
    area.replaceChildren(...nodes);
    area.hidden = false;
    $('missionApprove').hidden = preview.status === 'already_installed';
    $('missionConfirm').checked = false;
    if (preview.mission?.id && (!$('grantMissionId').value || preview.status === 'already_installed')) $('grantMissionId').value = preview.mission.id;
    sync();
  }
  function missionApprovable() {
    const m = state.mission;
    return Boolean(m && m.status === 'previewed' && !(m.blockers || []).length && m.outboundEligible === false
      && (m.cities || []).length === 8 && HEX64.test(m.missionHash || ''));
  }

  // ---------- grant preview ----------
  function renderGrantPreview(preview) {
    state.grantPreview = preview;
    const area = $('grantResult');
    const m = preview.manifest || {};
    const perms = preview.permissions || m.permissions || {};
    const missionHashMatches = !state.mission || !state.mission.missionHash || state.mission.mission?.id !== m.researchMissionId
      || state.mission.missionHash === m.researchMissionHash || state.mission.installedMissionHash === m.researchMissionHash;
    const nodes = [
      el('p', null, el('span', { class: `badge ${preview.installable ? 'good' : 'bad'}` }, preview.installable ? 'Ready to install' : 'Blocked')),
      el('h3', null, 'Limits'),
      facts([
        ['Maximum cycles', `${preview.maxCycles} discovery cycles`],
        ['Initially released cycles', String(preview.initiallyReleasedCycles ?? 0)],
        ['Google limit', `${dollars(preview.dailyGoogleUsdMicros)} per UTC day (Google Places only)`],
        ['Starts', utc(preview.notBefore)],
        ['Expires', utc(preview.expiresAt)],
        ['Revision pin', preview.expectedRevision, true],
      ]),
      el('h3', null, 'Recovery safeguards'),
      el('ul', { class: 'list' },
        el('li', null, 'Email recovery stays in maintenance; the grant requires it and never lowers it.'),
        el('li', null, 'Research providers stay paused; only metered Google Places opens inside a claimed cycle, debited before each call.'),
        el('li', null, 'Outbound stays off. Discoveries are kept apart and never queued for enrichment or email without a separate reconciliation.'),
        el('li', null, `Permitted: ${Object.entries(perms).filter(([, v]) => v).map(([k]) => k).join(', ') || 'none'}. ` +
          `Not permitted: ${Object.entries(perms).filter(([, v]) => !v).map(([k]) => k).join(', ') || 'none'}.`),
        el('li', null, 'Only the first cycle is released. Each later cycle needs separate administrator approval. The grant also stops at its eight-cycle ceiling, at expiry, on a revision change, or if pinned state changes. You can revoke it at any time.')),
      facts([
        ['Grant ID', preview.grantId, true],
        ['Research mission', m.researchMissionId, true],
        ['Research mission hash', m.researchMissionHash, true],
        ['Quarantine decision hash', m.quarantineDecisionHash, true],
        ['Recovery control hash', m.recoveryControlHash, true],
        ['Statement', m.authorizationEvidence?.statement],
      ]),
    ];
    if (!missionHashMatches) nodes.push(el('p', { class: 'blocked' }, 'The research mission hash differs from the mission you previewed above. Do not install.'));
    const blockers = blockerBox(preview.blockers);
    if (blockers) nodes.push(blockers);
    if (/^0{40}$/.test(preview.expectedRevision || '')) nodes.push(el('p', { class: 'blocked' }, 'No deployed revision is known, so this grant cannot be pinned.'));
    area.replaceChildren(...nodes);
    area.hidden = false;
    state.grantPreview.missionHashMatches = missionHashMatches;
    $('grantApprove').hidden = false;
    $('grantConfirm').checked = false;
    sync();
  }
  function grantInstallable() {
    const g = state.grantPreview;
    return Boolean(g && g.installable === true && g.status === 'previewed' && !(g.blockers || []).length
      && g.missionHashMatches !== false && HEX64.test(g.manifestHash || ''));
  }

  // ---------- enablement ----------
  function sync() {
    const ok = ready();
    $('previewMission').disabled = !ok || !REF.test($('missionRef').value.trim());
    $('approveMission').disabled = !ok || !missionApprovable() || !$('missionConfirm').checked;
    $('previewGrant').disabled = !ok || !REF.test($('grantRef').value.trim()) || !$('grantMissionId').value.trim()
      || !$('grantExpires').value || !$('grantStatement').value.trim();
    $('installGrant').disabled = !ok || !grantInstallable() || !$('grantConfirm').checked;
    $('revokeButton').disabled = !ok || !state.grant?.grantId || state.grant.status === 'revoked' || !REF.test($('revokeReference').value.trim());
    $('refreshGrant').disabled = state.busy;
    $('exportCohort').disabled = !ok || !HEX64.test(state.grant?.grantId || '');
    $('previewRelease').disabled = !ok || state.grant?.status !== 'awaiting_cycle_release'
      || !REF.test($('releaseReference').value.trim()) || !$('releaseStatement').value.trim();
  }
  async function busy(work) {
    if (state.busy) return;
    state.busy = true; sync();
    try { return await work(); } finally { state.busy = false; sync(); }
  }

  // ---------- confirmation dialog ----------
  function confirmDialog({ title, description, pairs, password, action }) {
    const dialog = $('confirmDialog');
    $('confirmTitle').textContent = title;
    $('confirmDescription').textContent = description;
    $('confirmFacts').replaceChildren(...facts(pairs).childNodes);
    $('confirmPasswordArea').hidden = !password;
    $('confirmPassword').value = '';
    $('confirmError').textContent = '';
    $('confirmSubmit').disabled = false;
    $('confirmSubmit').textContent = action;
    return new Promise(resolve => {
      const finish = value => { cleanup(); $('confirmPassword').value = ''; if (dialog.open) dialog.close(); resolve(value); };
      const onSubmit = event => {
        event.preventDefault();
        if (password && !$('confirmPassword').value) { $('confirmError').textContent = 'Enter your password to continue.'; return; }
        finish({ password: password ? $('confirmPassword').value : null });
      };
      const onCancel = () => finish(null);
      const onClose = () => finish(null);
      function cleanup() {
        $('confirmForm').removeEventListener('submit', onSubmit);
        $('confirmCancel').removeEventListener('click', onCancel);
        dialog.removeEventListener('cancel', onClose);
      }
      $('confirmForm').addEventListener('submit', onSubmit);
      $('confirmCancel').addEventListener('click', onCancel);
      dialog.addEventListener('cancel', onClose);
      dialog.showModal();
      (password ? $('confirmPassword') : $('confirmSubmit')).focus();
    });
  }
  async function stepUp(password, purpose = PURPOSE) {
    await call(CONFIRM, { purpose, password });
  }

  // ---------- actions ----------
  async function previewMission(event) {
    event.preventDefault();
    await busy(async () => {
      say('Previewing the research mission — nothing is being changed…');
      $('missionResult').hidden = false;
      try { renderMission(await call(API + 'research-mission/preview', { approvalRef: $('missionRef').value.trim() })); say(''); }
      catch (error) { state.mission = null; $('missionApprove').hidden = true; $('missionResult').replaceChildren(errorBox(error)); say('Mission preview failed.', 'error'); }
    });
  }
  async function approveMission() {
    const m = state.mission;
    if (!missionApprovable() || !$('missionConfirm').checked) return;
    const answer = await confirmDialog({ title: 'Approve research mission', action: 'Approve mission',
      description: 'This records the research mission. It does not start research, grant spending or change outbound.',
      password: true,
      pairs: [['Segments', (m.eligibleSubsegments || []).map(s => SEGMENT_LABELS[s] || s).join(', ')],
        ['Cities', (m.cities || []).join(', ')], ['Mission hash', m.missionHash, true], ['Approval reference', $('missionRef').value.trim(), true]] });
    if (!answer) return say('Mission approval cancelled. Nothing was recorded.');
    await busy(async () => {
      try {
        say('Confirming your password…');
        await stepUp(answer.password);
        say('Recording the research mission…');
        const result = await call(API + 'research-mission/install', { approvalRef: $('missionRef').value.trim(), mission: m.mission, missionHash: m.missionHash });
        $('grantMissionId').value = result.missionId;
        say(result.idempotent ? 'This research mission was already approved with this reference.' : 'Research mission approved. Next, preview the grant.', 'success');
        try { renderMission(await call(API + 'research-mission/preview', { approvalRef: $('missionRef').value.trim() })); } catch { /* status shown above */ }
      } catch (error) {
        $('missionResult').prepend(errorBox(error, { mutation: true }));
        say('Mission approval did not complete.', 'error');
      }
    });
  }
  function expiryIso() {
    const raw = $('grantExpires').value;
    if (!raw) return null;
    const date = new Date(`${raw.length === 16 ? `${raw}:00` : raw}Z`);
    return Number.isFinite(+date) ? date.toISOString() : null;
  }
  async function previewGrant(event) {
    event.preventDefault();
    const expiresAt = expiryIso();
    if (!expiresAt) return say('Enter a valid UTC expiry.', 'error');
    await busy(async () => {
      say('Previewing the grant — nothing is being changed…');
      $('grantResult').hidden = false;
      try {
        renderGrantPreview(await call(API + 'research-only-grant/preview', { approvalRef: $('grantRef').value.trim(), expiresAt,
          statement: $('grantStatement').value.trim(), researchMissionId: $('grantMissionId').value.trim() }));
        say('');
      } catch (error) { state.grantPreview = null; $('grantApprove').hidden = true; $('grantResult').replaceChildren(errorBox(error)); say('Grant preview failed.', 'error'); }
    });
  }
  async function installGrant() {
    const g = state.grantPreview;
    if (!grantInstallable() || !$('grantConfirm').checked) return;
    const answer = await confirmDialog({ title: 'Install research-only grant', action: 'Install grant',
      description: 'This releases one Scout discovery cycle. Every later cycle needs separate approval. The ceiling, budget, expiry and revision stay fixed.',
      password: true,
      pairs: [['Maximum cycles', String(g.maxCycles)], ['Initially released cycles', String(g.initiallyReleasedCycles)], ['Google limit', `${dollars(g.dailyGoogleUsdMicros)} per UTC day`],
        ['Expires', utc(g.expiresAt)], ['Revision pin', g.expectedRevision, true], ['Grant ID', g.grantId, true]] });
    if (!answer) return say('Grant installation cancelled. Nothing was recorded.');
    await busy(async () => {
      try {
        say('Confirming your password…');
        await stepUp(answer.password);
        say('Installing the grant…');
        const result = await call(API + 'research-only-grant/install', { manifest: g.manifest, manifestHash: g.manifestHash });
        say(result.idempotent ? 'This exact grant was already installed.' : 'Grant installed. Research runs only from the Max control loop, within these limits.', 'success');
        state.grantPreview = null; $('grantApprove').hidden = true; $('grantConfirm').checked = false;
        await refreshGrant();
      } catch (error) {
        $('grantResult').prepend(errorBox(error, { mutation: true }));
        say('Grant installation did not complete.', 'error');
      }
    });
  }
  async function revokeGrant() {
    const grant = state.grant, reference = $('revokeReference').value.trim();
    if (!grant?.grantId || !REF.test(reference)) return;
    const answer = await confirmDialog({ title: 'Revoke research-only grant', action: 'Revoke grant', password: false,
      description: 'Revocation is immediate and permanent. Recorded cycles, spend and candidates are kept.',
      pairs: [['Grant ID', grant.grantId, true], ['Cycles used', `${grant.cyclesConsumed} of ${grant.maxCycles}`], ['Reference', reference, true]] });
    if (!answer) return say('Revocation cancelled.');
    await busy(async () => {
      try {
        say('Revoking…');
        const result = await call(API + 'research-only-grant/revoke', { grantId: grant.grantId, approvalReference: reference });
        say(result.idempotent ? 'This grant was already revoked.' : 'Grant revoked.', 'success');
        $('revokeReference').value = '';
        await refreshGrant(grant.grantId);
      } catch (error) {
        $('grantStatus').prepend(errorBox(error, { mutation: true }));
        say('Revocation did not complete.', 'error');
      }
    });
  }

  async function reviewCycleRelease(event) {
    event.preventDefault();
    const grantId = state.grant?.grantId;
    if (!ready() || !HEX64.test(grantId || '')) return;
    let preview;
    await busy(async () => {
      try {
        preview = await call(API + 'research-cycle-release/preview', { grantId,
          approvalReference: $('releaseReference').value.trim(), statement: $('releaseStatement').value.trim() });
      } catch (error) { $('grantStatus').prepend(errorBox(error)); say('Cycle release preview failed.', 'error'); }
    });
    if (!preview?.releasable) return;
    const r = preview.release;
    const answer = await confirmDialog({ title: `Release research cycle ${r.newAllowance}`, action: 'Approve one cycle', password: true,
      description: 'This permits one additional research-only cycle. The scheduler may claim it after approval. Every later cycle stays held.',
      pairs: [['Grant ID', grantId, true], ['Released allowance', `${r.previousAllowance} → ${r.newAllowance}`],
        ['Maximum cycles', String(preview.maxCycles)], ['Google limit', `${dollars(preview.dailyGoogleUsdMicros)} per UTC day`],
        ['Expires', utc(preview.expiresAt)], ['Revision pin', r.expectedRevision, true],
        ['Reference', r.approvalReference], ['Statement', r.statement]] });
    if (!answer) return say('Cycle release cancelled. Nothing was recorded.');
    await busy(async () => {
      try {
        await stepUp(answer.password, 'scout_research_cycle_release');
        await call(API + 'research-cycle-release/release', { release: r, releaseHash: preview.releaseHash });
        $('releaseReference').value = ''; $('releaseStatement').value = '';
        await refreshGrant(grantId);
        say(`Cycle ${r.newAllowance} released. Every later cycle still requires approval.`, 'success');
      } catch (error) { $('grantStatus').prepend(errorBox(error, { mutation: true })); say('Cycle release did not complete.', 'error'); }
    });
  }

  function defaultExpiry() {
    const date = new Date(Date.now() + 72 * 3600 * 1000);
    date.setUTCMinutes(0, 0, 0);
    return date.toISOString().slice(0, 19);
  }

  document.addEventListener('DOMContentLoaded', () => {
    $('grantExpires').value = defaultExpiry();
    $('missionForm').addEventListener('submit', previewMission);
    $('grantForm').addEventListener('submit', previewGrant);
    $('releaseForm').addEventListener('submit', reviewCycleRelease);
    $('approveMission').addEventListener('click', approveMission);
    $('installGrant').addEventListener('click', installGrant);
    $('revokeButton').addEventListener('click', revokeGrant);
    $('refreshGrant').addEventListener('click', () => refreshGrant());
    $('exportCohort').addEventListener('click', () => busy(async () => {
      const grantId = state.grant?.grantId;
      if (!state.context?.approvalReady || !HEX64.test(grantId || '')) return;
      // Read-only export is valid even after exhaustion or revocation. The
      // server still applies the direct Anchor admin and same-origin gates.
      try {
        const cohort = await call(API + 'research-only-grant/export', { grantId });
        if (cohort.grantId !== grantId || cohort.tenantId !== '10' || !Array.isArray(cohort.records))
          throw new ApiError(409, { code: 'research_only_retrospective_snapshot_incomplete' });
        const url = URL.createObjectURL(new Blob([JSON.stringify(cohort)], { type: 'application/json' }));
        const link = document.createElement('a');
        link.href = url; link.download = `scout-research-cohort-${grantId.slice(0, 12)}.json`;
        document.body.append(link); link.click(); link.remove();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
        say(`Downloaded ${cohort.records.length} existing research records. No research was started.`);
      } catch (error) { $('grantStatus').append(errorBox(error)); }
    }));
    $('reloadAuthority').addEventListener('click', loadAuthority);
    $('lookupForm').addEventListener('submit', event => {
      event.preventDefault();
      const id = $('lookupGrantId').value.trim();
      if (!HEX64.test(id)) return say('A grant ID is 64 lowercase hex characters.', 'error');
      refreshGrant(id);
    });
    // Any edit invalidates a preview: approval needs a fresh preview.
    for (const id of ['grantRef', 'grantExpires', 'grantStatement', 'grantMissionId']) {
      $(id).addEventListener('input', () => { if (state.grantPreview) { state.grantPreview = null; $('grantApprove').hidden = true; $('grantResult').hidden = true; } sync(); });
    }
    $('missionRef').addEventListener('input', () => { if (state.mission) { state.mission = null; $('missionApprove').hidden = true; $('missionResult').hidden = true; } sync(); });
    for (const id of ['missionConfirm', 'grantConfirm', 'revokeReference', 'releaseReference', 'releaseStatement']) $(id).addEventListener('input', sync);
    for (const id of ['missionConfirm', 'grantConfirm']) $(id).addEventListener('change', sync);
    sync();
    loadAuthority().then(() => refreshGrant());
  });
})();
