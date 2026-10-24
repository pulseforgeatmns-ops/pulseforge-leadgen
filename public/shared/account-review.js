'use strict';
(function () {
  const el = (tag, text, cls) => { const item = document.createElement(tag); if (text != null) item.textContent = String(text); if (cls) item.className = cls; return item; };
  const label = text => String(text || 'Not recorded').replace(/_/g, ' ').replace(/^./, c => c.toUpperCase());
  const activeTenant = () => window.PulseforgeTenantCoordinator?.getActiveClientId?.() ?? window.PulseforgeShell?.activeClientId;
  const date = value => { if (!value) return 'Not recorded'; const d = new Date(/^\d{4}-\d{2}-\d{2}$/.test(value) ? `${value}T12:00:00` : value); return Number.isFinite(d.getTime()) ? d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' }) : 'Not recorded'; };
  let owner = null, model = null, selected = null, generation = 0, previousFocus = null, previousOverflow = '';
  const dialog = el('section', null, 'cr-overlay ar-overlay'); dialog.id = 'accountReview'; dialog.hidden = true;
  dialog.setAttribute('role', 'dialog'); dialog.setAttribute('aria-modal', 'true'); dialog.setAttribute('aria-labelledby', 'arTitle');
  dialog.innerHTML = '<div class="cr-backdrop" data-ar-close></div><div class="cr-panel ar-panel"><header class="cr-header"><div><p class="cr-eyebrow">YOUR WORKSPACE · ACCOUNTS</p><h2 id="arTitle">Account review</h2><p id="arSubtitle" class="cr-muted"></p></div><button type="button" class="cr-button" data-ar-close aria-label="Close account review">Close</button></header><p id="arNotice" class="cr-notice" role="status" aria-live="polite"></p><div class="cr-layout"><aside class="cr-sidebar ar-sidebar"><input id="arSearch" type="search" placeholder="Search accounts" aria-label="Search accounts"><select id="arFilter" aria-label="Filter accounts"><option value="all">All accounts</option><option value="overdue">Overdue follow-ups</option><option value="help">Needs your help</option></select><div id="arList" class="cr-list" aria-label="Assigned accounts"></div></aside><article id="arDetail" class="cr-detail" aria-label="Selected account review"></article></div></div>';
  document.body.append(dialog);
  const $ = id => document.getElementById(id);
  function notice(text) { $('arNotice').textContent = text; }
  function current(token) { return !dialog.hidden && token === generation && (activeTenant() == null || owner?.tenant == null || Number(activeTenant()) === Number(owner.tenant)); }
  async function request(path) {
    const response = await fetch(path, { credentials: 'same-origin', headers: { Accept: 'application/json' } });
    const body = await response.json();
    if (!response.ok) throw Object.assign(new Error('Account review could not be loaded.'), { status: response.status });
    return body;
  }
  function scoped(data) { return Number(data.ao_id) === Number(owner.aoId) && (activeTenant() == null || Number(data.tenant_id) === Number(activeTenant())) && (owner.tenant == null || Number(data.tenant_id) === Number(owner.tenant)); }
  function visibleAccounts() {
    const search = $('arSearch').value.trim().toLowerCase(); const filter = $('arFilter').value;
    return (model?.accounts || []).filter(a => (!search || `${a.company_name} ${label(a.current_status)}`.toLowerCase().includes(search)) && (filter === 'all' || (filter === 'overdue' && a.overdue) || (filter === 'help' && a.help_requested)));
  }
  function renderList() {
    $('arList').replaceChildren();
    for (const account of visibleAccounts()) {
      const button = el('button', null, 'cr-draft ar-account'); button.type = 'button'; button.dataset.accountId = account.prospect_id;
      button.setAttribute('aria-pressed', String(selected === account.prospect_id));
      const copy = el('span', null, 'cr-draft-copy'); copy.append(el('strong', account.company_name), el('span', label(account.current_status)));
      if (account.help_requested || account.overdue) copy.append(el('small', account.help_requested ? 'Needs your help' : 'Follow-up overdue', 'ar-attention'));
      button.append(copy); button.addEventListener('click', () => select(account.prospect_id)); $('arList').append(button);
    }
    if (!visibleAccounts().length) $('arList').append(el('p', 'No accounts match these filters.', 'cr-muted'));
  }
  function field(parent, title, text) { const item = el('div', null, 'ar-field'); item.append(el('dt', title), el('dd', text || 'Not recorded')); parent.append(item); }
  async function select(id) {
    selected = id; renderList();
    const account = model?.accounts.find(a => a.prospect_id === id); if (!account) return;
    const token = ++generation, detail = $('arDetail'); detail.replaceChildren();
    const head = el('div', null, 'cr-preview-head'); head.append(el('span', owner.aoName || 'Assigned operator', 'cr-channel'), el('span', label(account.current_status), 'cr-status'));
    detail.append(head, el('h3', account.company_name));
    const facts = el('dl', null, 'ar-facts');
    field(facts, 'Next action', label(account.next_action)); field(facts, 'Follow-up date', date(account.next_action_date || account.next_action_due_at));
    field(facts, 'Last contact', date(account.last_touch_at)); field(facts, 'Priority', label(account.priority)); detail.append(facts);
    if (account.help_requested) detail.append(el('p', account.help_reason || 'This account has been flagged for your help.', 'ar-help'));
    if (account.latest_note_preview) detail.append(el('h4', 'Latest note'), el('p', account.latest_note_preview, 'ar-note'));
    const more = el('div', null, 'ar-more'); more.append(el('p', 'Loading contact and history…', 'cr-muted')); detail.append(more);
    try {
      const data = await request(`/api/v1/max/account-review/${encodeURIComponent(id)}?ao_id=${encodeURIComponent(owner.aoId)}`);
      if (!current(token) || selected !== id) return;
      if (!scoped(data) || String(data.account?.summary?.assigned_ao_id) !== String(owner.aoId)) throw new Error('scope_changed');
      const item = data.account; more.replaceChildren();
      const contact = el('dl', null, 'ar-facts'); field(contact, 'Contact', item.contact?.name); field(contact, 'Role', item.contact?.role);
      field(contact, 'Email', item.contact?.email); field(contact, 'Phone', item.contact?.phone);
      more.append(el('h4', 'Contact'), contact);
      if (item.summary?.address) more.append(el('p', item.summary.address, 'cr-muted'));
      if (item.summary?.why_account_matters) more.append(el('h4', 'Account context'), el('p', item.summary.why_account_matters, 'ar-note'));
      const history = item.history || [];
      more.append(el('h4', 'Recent activity'));
      if (!history.length) more.append(el('p', 'No activity has been recorded for this account.', 'cr-muted'));
      for (const event of history) {
        const row = el('section', null, 'ar-event'); row.append(el('p', `${date(event.occurredOn || event.occurredAt || event.created_at)} · ${label(event.activity_type || event.kind)}`, 'cr-muted'));
        if (event.outcome) row.append(el('strong', label(event.outcome)));
        row.append(el('p', event.notes || 'No note recorded.', 'ar-note')); more.append(row);
      }
    } catch (error) { if (current(token)) more.replaceChildren(el('p', error.status === 404 ? 'This account is no longer available in this operator’s scope. Refresh the review.' : 'Contact and history could not be loaded. Reopen this account to retry.', 'cr-blocker')); }
  }
  async function load() {
    const token = ++generation; model = null; selected = null; notice('');
    $('arList').replaceChildren(el('p', 'Loading assigned accounts…', 'cr-muted')); $('arDetail').replaceChildren();
    try {
      const data = await request(`/api/v1/max/account-review?ao_id=${encodeURIComponent(owner.aoId)}`);
      if (!current(token)) return;
      if (!scoped(data)) throw new Error('scope_changed');
      model = data; owner.tenant = data.tenant_id; owner.aoName = data.ao_name || owner.aoName;
      $('arTitle').textContent = owner.aoName ? `${owner.aoName}’s accounts` : 'Account review';
      const summary = data.summary || {};
      $('arSubtitle').textContent = `${window.PulseforgeShell?.tenantName || 'Current workspace'} · ${data.accounts.length} assigned · ${summary.overdue || 0} overdue · ${summary.help_requested || 0} need help`;
      renderList();
      const first = visibleAccounts()[0];
      if (first) await select(first.prospect_id);
      else $('arDetail').append(el('h3', 'No accounts to review.'), el('p', 'No accounts are currently assigned to this operator in this workspace.', 'cr-muted'));
    } catch (error) {
      if (!current(token)) return;
      $('arList').replaceChildren(); $('arDetail').replaceChildren(el('h3', 'Account review is unavailable.'));
      notice(error.status === 403 ? 'You cannot review accounts for this operator in this workspace.' : 'Assigned accounts could not be loaded. Please try again.');
      const retry = el('button', 'Try again', 'cr-button'); retry.type = 'button'; retry.addEventListener('click', load); $('arDetail').append(retry);
    }
  }
  function open(input) {
    if (!input?.aoId || !(input.tenantId ?? activeTenant())) return false;
    if (dialog.hidden) { previousFocus = document.activeElement; previousOverflow = document.body.style.overflow; }
    owner = { ...input, tenant: input.tenantId ?? activeTenant() }; dialog.hidden = false; document.body.style.overflow = 'hidden';
    $('arTitle').textContent = input.aoName ? `${input.aoName}’s accounts` : 'Account review'; $('arSubtitle').textContent = '';
    $('arSearch').value = ''; $('arFilter').value = 'all'; dialog.querySelector('button[data-ar-close]').focus(); load(); return true;
  }
  function close() { document.dispatchEvent(new CustomEvent('pulseforge:review-dismissed')); generation++; dialog.hidden = true; model = null; owner = null; selected = null; $('arList').replaceChildren(); $('arDetail').replaceChildren(); $('arSubtitle').textContent = ''; document.body.style.overflow = previousOverflow; if (previousFocus?.isConnected) previousFocus.focus(); }
  function filterChanged() { if (!model) return; renderList(); const first = visibleAccounts()[0]; if (first) select(first.prospect_id); else { generation++; selected = null; $('arDetail').replaceChildren(el('p', 'No accounts match these filters.', 'cr-muted')); } }
  $('arSearch').addEventListener('input', filterChanged); $('arFilter').addEventListener('change', filterChanged);
  dialog.querySelectorAll('[data-ar-close]').forEach(button => button.addEventListener('click', close));
  dialog.addEventListener('keydown', event => {
    if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); close(); }
    if (event.key === 'Tab') { const controls = [...dialog.querySelectorAll('button:not(:disabled),input,select')].filter(control => control.offsetParent !== null); const first = controls[0], last = controls.at(-1); if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); } else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); } }
  });
  document.addEventListener('pulseforge:tenant-changing', () => { if (!dialog.hidden) close(); });
  document.addEventListener('pulseforge:tenant-changed', () => { if (!dialog.hidden) close(); });
  window.PulseforgeAccountReview = { open, close };
})();
