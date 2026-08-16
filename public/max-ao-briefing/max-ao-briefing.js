'use strict';

(function () {
  const els = {
    status: document.getElementById('mabStatus'),
    error: document.getElementById('mabError'),
    timestamp: document.getElementById('mabTimestamp'),
    digest: document.getElementById('mabDigestText'),
    today: document.getElementById('mabToday'),
    campaign: document.getElementById('mabCampaign'),
    escalations: document.getElementById('mabEscalations'),
    escFilter: document.getElementById('mabEscFilter'),
    warm: document.getElementById('mabWarm'),
    intel: document.getElementById('mabIntel'),
    jake: document.getElementById('mabJakeActions'),
    mike: document.getElementById('mabMikeActions'),
    promo: document.getElementById('mabPromo'),
    clientSelect: document.getElementById('mabClientSelect'),
    refresh: document.getElementById('mabRefresh'),
    askForm: document.getElementById('mabAskForm'),
    askInput: document.getElementById('mabAskInput'),
    askHistory: document.getElementById('mabAskHistory'),
  };

  let clientId = 10;
  let briefing = null;
  let escalations = [];

  function esc(text) {
    return String(text == null ? '' : text)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function setStatus(msg) {
    if (els.status) els.status.textContent = msg || '';
  }

  function setError(msg) {
    if (!els.error) return;
    if (msg) {
      els.error.hidden = false;
      els.error.textContent = msg;
    } else {
      els.error.hidden = true;
      els.error.textContent = '';
    }
  }

  function api(path, options) {
    const sep = path.includes('?') ? '&' : '?';
    const url = `${path}${sep}client_id=${encodeURIComponent(clientId)}`;
    return fetch(url, {
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      ...options,
    }).then(async (res) => {
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || data.message || res.statusText);
      return data;
    });
  }

  function stat(label, value) {
    return `<div class="mab-stat"><div class="mab-stat-val">${esc(value)}</div><div class="mab-stat-label">${esc(label)}</div></div>`;
  }

  function renderToday(data) {
    const t = data.today || {};
    els.today.innerHTML = [
      stat('Visits today', t.visits_today || 0),
      stat('Calls today', t.calls_today || 0),
      stat('Open escalations', t.open_escalations || 0),
      stat('Overdue follow-ups', t.overdue_follow_ups || 0),
      stat('Due today', t.due_today || 0),
    ].join('');
  }

  function renderCampaign(c) {
    if (!c) {
      els.campaign.innerHTML = '<p class="mab-empty">No Campaign 001 data</p>';
      return;
    }
    els.campaign.innerHTML = [
      stat('Touched', `${c.visited}/${c.target_total}`),
      stat('Not yet touched', c.not_yet_touched || 0),
      stat('Meaningful convos', c.meaningful_conversations || 0),
      stat('Decision-makers', c.decision_makers_reached || 0),
      stat('Walkthroughs', c.walkthrough_requests || 0),
      stat('Queue remaining', c.remaining_route_queue || 0),
    ].join('');
  }

  function urgencyBadge(u) {
    const cls = u === 'high' ? 'mab-badge-high' : 'mab-badge-seen';
    return `<span class="mab-badge ${cls}">${esc(u || 'medium')}</span>`;
  }

  function statusBadge(s) {
    const cls = s === 'new' ? 'mab-badge-new' : 'mab-badge-seen';
    return `<span class="mab-badge ${cls}">${esc(s)}</span>`;
  }

  function renderEscalations(items) {
    if (!items.length) {
      els.escalations.innerHTML = '<p class="mab-empty">No escalations in this view.</p>';
      return;
    }
    const rows = items.map((e) => `
      <tr data-id="${esc(e.id)}">
        <td>${statusBadge(e.status)} ${urgencyBadge(e.urgency)}</td>
        <td>
          <strong>${esc(e.business_name)}</strong>
          ${e.contact_name ? `<br>${esc(e.contact_name)}${e.contact_title ? ` · ${esc(e.contact_title)}` : ''}` : ''}
          ${e.phone ? `<br><a href="tel:${esc(e.phone)}">${esc(e.phone)}</a>` : ''}
        </td>
        <td>${esc(e.reason)}<br><span class="mab-card-meta">${esc(e.visit_summary)}</span></td>
        <td>${esc(e.recommended_action)}</td>
        <td>${esc(e.ao_owner)}<br><span class="mab-card-meta">${esc(e.campaign || e.source || '')}</span></td>
        <td>
          <div class="mab-actions">
            ${e.status === 'new' ? `<button type="button" class="mab-action-btn" data-act="seen">Mark Seen</button>` : ''}
            <button type="button" class="mab-action-btn" data-act="resolve">Resolve</button>
            ${e.phone ? `<a class="mab-action-btn" href="tel:${esc(e.phone)}">Call</a>` : ''}
            <a class="mab-action-btn" href="/admin/field-visits/?lead=${esc(e.lead_id)}">View Visit</a>
            <button type="button" class="mab-action-btn" data-act="promote-lead" data-lead="${esc(e.lead_id)}">Promote To CRM</button>
            <button type="button" class="mab-action-btn" data-act="assign">Assign Follow-Up</button>
          </div>
        </td>
      </tr>
    `).join('');

    els.escalations.innerHTML = `
      <table class="mab-table">
        <thead>
          <tr>
            <th>Status</th>
            <th>Business / Contact</th>
            <th>Reason / Summary</th>
            <th>Recommended</th>
            <th>AO / Source</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    `;
  }

  function renderList(container, items, formatter) {
    if (!items || !items.length) {
      container.innerHTML = '<p class="mab-empty">Nothing here yet.</p>';
      return;
    }
    container.innerHTML = items.map(formatter).join('');
  }

  function renderBriefing(data) {
    briefing = data;
    if (els.timestamp) {
      els.timestamp.textContent = `Updated ${new Date(data.generated_at).toLocaleString()}`;
    }
    if (els.digest) els.digest.textContent = data.daily_digest?.text || '';

    renderToday(data);
    renderCampaign(data.campaign_001);

    escalations = data.needs_jake || [];
    renderEscalations(filterEscalations());

    renderList(els.warm, data.warm_opportunities, (w) => `
      <div class="mab-card">
        <p class="mab-card-title">${esc(w.business_name)}${w.contact_name ? ` — ${esc(w.contact_name)}` : ''}</p>
        <p class="mab-card-meta">${esc(w.warm_reason)}<br>Next: ${esc(w.next_step)}</p>
      </div>
    `);

    const intel = data.field_intelligence || {};
    const intelItems = [];
    for (const o of (intel.objections || []).slice(0, 4)) {
      intelItems.push({ title: `Objection: ${o.text}`, meta: `${o.count} mention${o.count === 1 ? '' : 's'}` });
    }
    for (const v of (intel.vendor_complaints || []).slice(0, 3)) {
      intelItems.push({ title: 'Vendor complaint', meta: v.text });
    }
    for (const p of (intel.pain_points || []).slice(0, 3)) {
      intelItems.push({ title: 'Pain point', meta: p.text });
    }
    renderList(els.intel, intelItems, (i) => `
      <div class="mab-card">
        <p class="mab-card-title">${esc(i.title)}</p>
        <p class="mab-card-meta">${esc(i.meta)}</p>
      </div>
    `);

    renderList(els.jake, data.recommended_actions?.jake, (a) => `
      <div class="mab-card">
        <p class="mab-card-title">${esc(a.action)}</p>
        <p class="mab-card-meta">${a.business ? esc(a.business) : ''}${a.contact ? ` · ${esc(a.contact)}` : ''}</p>
      </div>
    `);

    renderList(els.mike, data.recommended_actions?.mike, (a) => `
      <div class="mab-card">
        <p class="mab-card-title">${esc(a.action)}</p>
        <p class="mab-card-meta">${esc(a.detail || '')}</p>
      </div>
    `);

    renderList(els.promo, data.promotion_candidates, (p) => `
      <div class="mab-card" data-lead="${esc(p.lead_id)}">
        <p class="mab-card-title">${esc(p.business_name)}${p.contact_name ? ` — ${esc(p.contact_name)}` : ''}</p>
        <p class="mab-card-meta">${esc(p.reasons.join(', '))}</p>
        <button type="button" class="mab-action-btn" data-promote="${esc(p.lead_id)}">Approve Promotion</button>
      </div>
    `);
  }

  function filterEscalations() {
    const mode = els.escFilter?.value || 'open';
    if (mode === 'new') return escalations.filter((e) => e.status === 'new');
    return escalations;
  }

  async function loadAllEscalations() {
    const data = await api('/api/v1/max/ao-escalations?include_resolved=1');
    escalations = data.escalations || [];
    renderEscalations(escalations);
    return escalations;
  }

  async function loadBriefing() {
    setError('');
    setStatus('Loading briefing…');
    try {
      const data = await api('/api/v1/max/ao-briefing');
      const escData = await api('/api/v1/max/ao-escalations');
      escalations = escData.escalations || [];
      data.needs_jake = escalations;
      renderBriefing(data);
      setStatus('');
    } catch (err) {
      setError(err.message);
      setStatus('');
    }
  }

  async function patchEscalation(id, status) {
    await fetch(`/api/v1/max/ao-escalations/${id}?client_id=${clientId}`, {
      method: 'PATCH',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status }),
    });
    await loadBriefing();
  }

  async function promoteLead(leadId) {
    if (!window.confirm('Promote this AO lead to CRM? This requires manual approval.')) return;
    setStatus('Promoting to CRM…');
    try {
      const res = await fetch(`/api/v1/max/ao-leads/${leadId}/promote?client_id=${clientId}`, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Promotion failed');
      setStatus(`Promoted — prospect #${data.prospect_id}`);
      await loadBriefing();
    } catch (err) {
      setError(err.message);
      setStatus('');
    }
  }

  async function askMax(question) {
    const q = String(question || '').trim();
    if (!q) return;
    appendAskBubble('q', q);
    els.askInput.value = '';
    try {
      const data = await api('/api/v1/max/ao-briefing/ask', {
        method: 'POST',
        body: JSON.stringify({ question: q }),
      });
      appendAskBubble('a', data.answer || 'No answer.');
    } catch (err) {
      appendAskBubble('a', `Error: ${err.message}`);
    }
  }

  function appendAskBubble(kind, text) {
    const div = document.createElement('div');
    div.className = `mab-ask-bubble mab-ask-${kind}`;
    div.textContent = text;
    els.askHistory.appendChild(div);
    div.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }

  async function loadClients() {
    try {
      const res = await fetch('/api/clients', { credentials: 'same-origin' });
      if (!res.ok) return;
      const clients = await res.json();
      if (!els.clientSelect || !Array.isArray(clients)) return;
      els.clientSelect.innerHTML = clients
        .map((c) => `<option value="${c.id}">${esc(c.name || c.slug || c.id)}</option>`)
        .join('');
      const anchor = clients.find((c) => c.id === 10);
      clientId = anchor ? 10 : (clients[0]?.id || 10);
      els.clientSelect.value = String(clientId);
    } catch {
      /* optional */
    }
  }

  els.refresh?.addEventListener('click', loadBriefing);

  els.clientSelect?.addEventListener('change', () => {
    clientId = Number(els.clientSelect.value) || 10;
    loadBriefing();
  });

  els.escFilter?.addEventListener('change', async () => {
    if (els.escFilter.value === 'all') await loadAllEscalations();
    else renderEscalations(filterEscalations());
  });

  els.escalations?.addEventListener('click', async (ev) => {
    const btn = ev.target.closest('[data-act]');
    if (!btn) return;
    const row = btn.closest('tr');
    const id = row?.dataset?.id;
    if (!id) return;
    const act = btn.dataset.act;
    if (act === 'seen') await patchEscalation(id, 'seen');
    if (act === 'resolve') await patchEscalation(id, 'resolved');
    if (act === 'assign') {
      await fetch(`/api/v1/max/ao-escalations/${id}/assign-follow-up?client_id=${clientId}`, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nextAction: 'Jake follow-up assigned from escalation inbox' }),
      });
      await loadBriefing();
    }
    if (act === 'promote-lead' && btn.dataset.lead) await promoteLead(btn.dataset.lead);
  });

  els.promo?.addEventListener('click', (ev) => {
    const btn = ev.target.closest('[data-promote]');
    if (btn) promoteLead(btn.dataset.promote);
  });

  els.askForm?.addEventListener('submit', (ev) => {
    ev.preventDefault();
    askMax(els.askInput.value);
  });

  document.querySelectorAll('.mab-chip[data-q]').forEach((chip) => {
    chip.addEventListener('click', () => askMax(chip.dataset.q));
  });

  if (window.PulseforgeShell && typeof window.PulseforgeShell.init === 'function') {
    window.PulseforgeShell.init({ surface: 'max-briefing' });
  }

  loadClients().then(loadBriefing);
})();
