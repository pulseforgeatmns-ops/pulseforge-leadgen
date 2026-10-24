'use strict';

// Pulseforge canonical Prospect Workspace UI (Phase B).
// The primary call workflow for both Pipeline (dashboard.html) and Calls
// (setter-dashboard.html). Reads exclusively from the canonical server read
// model (/api/prospects/:id/workspace) and the deterministic call preparation
// endpoint. All lifecycle writes flow through the canonical services.
//
// Layout:
//   Desktop — three panes: prospect context | call preparation/script |
//             outcome and next-step controls.
//   Mobile  — persistent prospect header with visible phone + Call button,
//             Script / Context / History / Outcome tabs, sticky action bar.
//
// Accessibility: role=dialog + aria-modal, focus trap, Escape close, focus
// restoration, keyboard tab navigation, labeled inputs, 44px touch targets,
// live-region announcements on saves, errors, and outcome form changes.

(function () {
  const state = {
    open: false,
    prospectId: null,
    workspace: null,
    preparation: null,
    releaseFocus: null,
    activeTab: 'prep',
    outcomeFlow: null,
    outcomeValues: {},
  };

  function esc(value) {
    return String(value ?? '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }

  function fmtDate(value) {
    if (!value) return '—';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '—';
    return date.toLocaleString([], { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });
  }

  function localDateTimeValue(date) {
    const d = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
    return d.toISOString().slice(0, 16);
  }

  function defaultCallback(days, hour = 10) {
    const date = new Date();
    date.setDate(date.getDate() + days);
    date.setHours(hour, 0, 0, 0);
    return localDateTimeValue(date);
  }

  const TABS = [
    { id: 'prep', label: 'Script' },
    { id: 'context', label: 'Context' },
    { id: 'history', label: 'History' },
    { id: 'outcome', label: 'Outcome' },
  ];

  // ── Dynamic outcome flows ─────────────────────────────────────────────
  // The ten product flows (Phase B §5) plus full-parity extra outcomes.
  // Each flow declares its fields; the form renders only what applies and
  // announces the consequence when the flow changes.
  const OUTCOME_FLOWS = [
    {
      id: 'no_answer', server: 'no_answer', label: 'No answer', group: 'reach',
      consequence: ['ok', 'Keep in calling queue'],
      saveLabel: 'Save — keep in calling queue',
      fields: { notes: true, callback: 'optional' },
    },
    {
      id: 'voicemail', server: 'voicemail', label: 'Voicemail left', group: 'reach',
      consequence: ['ok', 'Keep in calling queue'],
      saveLabel: 'Save — keep in calling queue',
      fields: { notes: true, callback: 'optional' },
    },
    {
      id: 'decision_maker_not_reached', server: 'gatekeeper_relayed', label: 'Decision-maker not reached', group: 'reach',
      consequence: ['ok', 'Moves to Follow-up. If the message was relayed, a next-day callback is suggested.'],
      saveLabel: 'Save — move to Follow-up',
      fields: { gatekeeper: true, notes: true, callback: 'suggested-1' },
    },
    {
      id: 'callback_requested', server: 'answered_callback', label: 'Callback requested', group: 'progress',
      consequence: ['ok', 'Callback scheduled — moves to Follow-up'],
      saveLabel: 'Save — schedule callback',
      fields: { summary: 'required', nextStep: 'required', callback: 'required-1' },
    },
    {
      id: 'interested', server: 'answered_interested', label: 'Interested', group: 'progress',
      consequence: ['ok', 'Warm prospect; set next step'],
      saveLabel: 'Save — mark warm and set next step',
      fields: { summary: 'required', nextStep: 'required', callback: 'optional' },
    },
    {
      id: 'meeting_booked', server: 'meeting_booked', label: 'Meeting booked', group: 'progress',
      consequence: ['ok', 'Moves to Booked and creates exactly one closer handoff'],
      saveLabel: 'Save — book meeting and hand off',
      fields: { summary: 'required', nextStep: 'required', nextStepLabel: 'Meeting details — required' },
    },
    {
      id: 'answered_not_interested', server: 'answered_not_interested', label: 'Not interested', group: 'pause',
      consequence: ['warn', 'Pause outreach; revisit in about 90 days (Nurture — not Dead)'],
      saveLabel: 'Save — nurture for ~90 days',
      fields: { summary: 'required', reason: 'required', reasonLabel: 'Why not interested — required', callback: 'suggested-90' },
    },
    {
      id: 'wrong_number', server: 'wrong_number', label: 'Wrong number', group: 'pause',
      consequence: ['warn', 'Remove invalid number and find a new contact method (Data remediation — not Dead)'],
      saveLabel: 'Save — mark data remediation and remove this number',
      fields: { notes: true },
    },
    {
      id: 'disconnected', server: 'disconnected', label: 'Disconnected', group: 'pause',
      consequence: ['warn', 'Remove invalid number and find a new contact method (Data remediation — not Dead)'],
      saveLabel: 'Save — mark data remediation and remove this number',
      fields: { notes: true },
    },
    {
      id: 'do_not_call', server: 'do_not_call', label: 'Do not call', group: 'terminal',
      consequence: ['danger', 'Permanently suppress outreach — confirmation required'],
      saveLabel: 'Save — permanently suppress outreach',
      fields: {
        summary: 'required',
        reason: 'required', reasonLabel: 'Their request, verbatim — required',
        confirm: 'They asked not to be contacted. Suppress this prospect permanently.',
      },
    },
  ];

  const OUTCOME_GROUPS = [
    { id: 'reach', label: 'Did not connect' },
    { id: 'progress', label: 'Connected — progress' },
    { id: 'pause', label: 'Pause or repair' },
    { id: 'terminal', label: 'Terminal' },
  ];

  // Full-parity secondary outcomes (kept reachable, not in the primary grid).
  const MORE_OUTCOMES = [
    {
      id: 'gatekeeper_blocked', server: 'gatekeeper_blocked', label: 'Gatekeeper blocked',
      consequence: ['warn', 'Moves to Follow-up — retry with a different approach or disqualify.'],
      fields: { notes: true, callback: 'optional' },
    },
    {
      id: 'incumbent_all_set', server: 'incumbent_all_set', label: 'All set with current vendor',
      consequence: ['warn', 'Nurture — stays alive in Follow-up with a 90-day re-check.'],
      fields: { notes: true, callback: 'suggested-90' },
    },
    {
      id: 'qualified', server: 'qualified', label: 'Qualified',
      consequence: ['ok', 'Marks warm and hot — document the meeting or next step.'],
      fields: { summary: 'required', nextStep: 'required', callback: 'optional' },
    },
    {
      id: 'disqualified', server: 'disqualified', label: 'Disqualified',
      consequence: ['danger', 'Permanent Dead — the prospect is out. (Use "Do not call" only when they ask not to be contacted.)'],
      fields: { summary: 'required', reason: 'required', reasonLabel: 'Disqualification reason — required' },
    },
  ];

  const ALL_FLOWS = [...OUTCOME_FLOWS, ...MORE_OUTCOMES];

  function flowById(id) {
    return ALL_FLOWS.find(flow => flow.id === id) || null;
  }

  // ── Rendering ─────────────────────────────────────────────────────────
  function reasonChip(ws) {
    const reason = ws.lifecycle?.lifecycleReason;
    if (!reason) return '';
    const label = window.PulseforgeLifecycle.lifecycleReasonLabel?.(reason) || reason;
    return `<span class="pf-reason-chip pf-reason-${esc(reason)}">${esc(label)}</span>`;
  }

  function headerHtml(ws) {
    const p = ws.prospect;
    const phone = p.phone || {};
    const telLink = phone.callable && window.PulseforgePhone
      ? window.PulseforgePhone.telHref(phone.normalized || phone.raw)
      : null;
    // Phone uses data-pf-ws="tel" so the click router runs beginDialHandoff
    // (persist workspace → tel:), never FaceTime / bare navigation.
    const phoneHtml = phone.display
      ? (telLink
        ? `<span class="pf-ws-phone"><a href="${esc(telLink)}" data-pf-ws="tel" aria-label="Call ${esc(phone.display)}">📞 ${esc(phone.display)}</a></span>`
        : `<span class="pf-ws-phone">📞 ${esc(phone.display)}</span>`)
      : '<span class="pf-ws-phone">📞 No phone on file</span>';
    return `
      <div class="pf-workspace-title">
        <h2 id="pf-ws-heading">${esc(p.companyName)}</h2>
        <div class="pf-ws-contact">${esc(p.contactName || 'No contact name')}${p.contactRole ? ` · ${esc(p.contactRole)}` : ''}</div>
        <div class="pf-ws-meta">
          ${phoneHtml}
          ${p.email ? `<a href="mailto:${esc(p.email)}">✉ ${esc(p.email)}</a>` : '<span>✉ No email</span>'}
          <span>${window.PulseforgeLifecycle.stageChip(ws.lifecycle.canonicalStage)}</span>
          ${reasonChip(ws)}
          <span title="${esc(p.priority?.reason || '')}">◆ ${esc(p.priority?.label || 'Unranked')}</span>
          <span>Last: ${ws.lastInteraction ? `${esc(ws.lastInteraction.summary).slice(0, 60)} · ${fmtDate(ws.lastInteraction.occurredAt)}` : 'No interactions yet'}</span>
          <span${ws.nextAction?.overdue ? ' style="color:var(--pf-red)"' : ''}>Next: ${esc(ws.nextAction?.label || '—')}${ws.nextAction?.dueAt ? ` · ${fmtDate(ws.nextAction.dueAt)}` : ''}</span>
        </div>
      </div>
      <button type="button" class="pf-workspace-close" data-pf-ws="close" aria-label="Back to queue">← Queue</button>
    `;
  }

  function actionsHtml(ws) {
    const p = ws.prospect;
    const canCall = ws.permissions?.canCall && p.phone?.callable;
    return `
      <button type="button" class="pf-primary" data-pf-ws="call" ${canCall ? '' : 'aria-disabled="true" disabled'}>Call</button>
      <button type="button" data-pf-ws="copy-phone" ${p.phone?.display ? '' : 'aria-disabled="true" disabled'}>Copy phone</button>
      <button type="button" data-pf-ws="tab-outcome" ${ws.permissions?.canLogCall ? '' : 'aria-disabled="true" disabled'}>Log outcome</button>
      <button type="button" data-pf-ws="tab-callback">Schedule callback</button>
      <button type="button" data-pf-ws="tab-notes">Add note</button>
      <button type="button" data-pf-ws="tab-opportunity">${ws.opportunity?.exists ? 'Open opportunity' : 'Opportunity'}</button>
    `;
  }

  function stickyActionsHtml(ws) {
    const canCall = ws?.permissions?.canCall && ws?.prospect?.phone?.callable;
    const phone = ws?.prospect?.phone || {};
    const flow = flowById(state.outcomeFlow);
    const saveLabel = flow?.saveLabel || 'Save outcome';
    const onOutcome = state.activeTab === 'outcome';
    return `
      ${phone.display ? `<span class="pf-ws-sticky-phone" aria-hidden="false">${esc(phone.display)}</span>` : ''}
      <button type="button" class="pf-primary" data-pf-ws="call" ${canCall ? '' : 'aria-disabled="true" disabled'}>Call</button>
      ${onOutcome
        ? `<button type="button" class="pf-primary" data-pf-ws="save-outcome"${flow ? '' : ' disabled'}>${esc(saveLabel)}</button>`
        : `<button type="button" data-pf-ws="tab-outcome">Outcome</button>`}
      <button type="button" data-pf-ws="close">← Queue</button>
    `;
  }

  function contextHtml(ws) {
    const cb = ws.callback || {};
    const calling = ws.calling || {};
    const structured = calling.lastStructuredNotes || {};
    const conflict = cb.conflict
      ? `<div class="pf-ws-conflict" role="alert">Callback conflict: canonical store says ${fmtDate(cb.dueAt)} but the legacy field says ${fmtDate(cb.legacyDueAt)}. The canonical value is shown; review before rescheduling.</div>`
      : '';
    const operatorNotes = ws.notes?.operatorNotes || [];
    const opp = ws.opportunity || {};
    return `
      ${conflict}
      <h3>Prior outcome &amp; next step</h3>
      <div class="pf-ws-grid">
        <div class="pf-ws-fact"><div class="pf-ws-fact-label">Last outcome</div><div class="pf-ws-fact-value">${calling.lastDispositionLabel ? esc(calling.lastDispositionLabel) : 'No calls logged yet'}${calling.lastAttemptAt ? ` · ${fmtDate(calling.lastAttemptAt)}` : ''}</div></div>
        <div class="pf-ws-fact"><div class="pf-ws-fact-label">Promised next step</div><div class="pf-ws-fact-value">${structured.next_step ? esc(structured.next_step) : 'None recorded'}</div></div>
        <div class="pf-ws-fact"><div class="pf-ws-fact-label">Objection / reason</div><div class="pf-ws-fact-value">${structured.reason ? esc(structured.reason) : (calling.lastDispositionNotes ? esc(calling.lastDispositionNotes).slice(0, 160) : 'None recorded')}</div></div>
        <div class="pf-ws-fact"><div class="pf-ws-fact-label">Callback</div><div class="pf-ws-fact-value">${cb.dueAt ? `${fmtDate(cb.dueAt)}${cb.overdue || (new Date(cb.dueAt) < new Date()) ? ' · overdue' : ''}` : 'None scheduled'}</div></div>
      </div>
      <h3>Snapshot</h3>
      <div class="pf-ws-grid">
        <div class="pf-ws-fact"><div class="pf-ws-fact-label">Stage</div><div class="pf-ws-fact-value">${window.PulseforgeLifecycle.stageChip(ws.lifecycle.canonicalStage)} ${reasonChip(ws)}</div></div>
        <div class="pf-ws-fact"><div class="pf-ws-fact-label">Next action</div><div class="pf-ws-fact-value">${esc(ws.nextAction?.label || '—')}${ws.nextAction?.dueAt ? ` · ${fmtDate(ws.nextAction.dueAt)}` : ''}</div></div>
        <div class="pf-ws-fact"><div class="pf-ws-fact-label">Call attempts</div><div class="pf-ws-fact-value">${calling.attempts ?? 0}</div></div>
        <div class="pf-ws-fact"><div class="pf-ws-fact-label">Score</div><div class="pf-ws-fact-value">${ws.prospect.score ?? '—'}</div></div>
      </div>
      <h3>Known facts</h3>
      ${ws.knownFacts?.length ? `<div class="pf-ws-grid">
        ${ws.knownFacts.map(fact => `
          <div class="pf-ws-fact">
            <div class="pf-ws-fact-label">${esc(fact.label)}</div>
            <div class="pf-ws-fact-value">${esc(fact.value)}</div>
            <div class="pf-ws-fact-source">source: ${esc(fact.sourceType)}${fact.verified ? ' · verified' : ''}</div>
          </div>
        `).join('')}
      </div>` : '<p class="pf-ws-empty">No verified facts recorded yet — score, vertical, location, and contact role appear here once available.</p>'}
      <h3>Add note</h3>
      <label for="pf-ws-note-input">New note</label>
      <textarea id="pf-ws-note-input" rows="3" placeholder="What did you learn? Objections, decision maker, timing…"></textarea>
      <div style="margin-top:8px">
        <button type="button" class="pf-btn pf-primary" data-pf-ws="save-note">Save note</button>
      </div>
      <h3>Operator notes</h3>
      ${operatorNotes.length ? operatorNotes.map(note => `
        <div class="pf-ws-note">
          <div class="pf-ws-note-meta">${esc(note.noteType)} · ${fmtDate(note.createdAt)}${note.author ? ` · ${esc(note.author)}` : ''} · ${esc(note.source)}</div>
          ${esc(note.text)}
        </div>
      `).join('') : '<p class="pf-ws-empty">No operator notes yet. Notes you save appear here with author and date.</p>'}
      ${ws.notes?.legacyNotes ? `
        <div class="pf-ws-note pf-ws-note-legacy">
          <div class="pf-ws-note-meta">Legacy scratchpad — migrated read-only</div>
          ${esc(ws.notes.legacyNotes).replace(/\n/g, '<br>')}
        </div>
      ` : ''}
      ${ws.notes?.legacyBaseNotes ? `
        <div class="pf-ws-note pf-ws-note-legacy">
          <div class="pf-ws-note-meta">Scout base record</div>
          ${esc(ws.notes.legacyBaseNotes)}
        </div>
      ` : ''}
      <h3 id="pf-ws-opportunity-anchor">Opportunity</h3>
      ${opp.exists ? `
        <div class="pf-ws-grid">
          <div class="pf-ws-fact"><div class="pf-ws-fact-label">Stage</div><div class="pf-ws-fact-value">${esc(opp.stage || '—')}</div></div>
          <div class="pf-ws-fact"><div class="pf-ws-fact-label">Estimated value</div><div class="pf-ws-fact-value">${opp.estimatedValueCents != null ? `$${(opp.estimatedValueCents / 100).toLocaleString()}` : '—'}</div></div>
          <div class="pf-ws-fact"><div class="pf-ws-fact-label">Reference</div><div class="pf-ws-fact-value">${esc(opp.id || 'booked handoff')}</div></div>
        </div>
      ` : '<p class="pf-ws-empty">No opportunity yet. Booking a meeting creates the booked handoff; revenue opportunities are created through the revenue workflow where enabled.</p>'}
    `;
  }

  function prepHtml(prep) {
    if (!prep) return '<p class="pf-ws-empty">Call preparation is unavailable for this prospect.</p>';
    return `
      <h3>Why now</h3>
      <p class="pf-ws-why-now">${esc(prep.whyNow || prep.reasonSelected || 'In the calling queue')}</p>
      <h3>Objective</h3>
      <p>${esc(prep.objective)}</p>
      <div class="pf-ws-generated"><span class="pf-ws-generated-tag">Generated guidance</span>Deterministic from verified facts · ${fmtDate(prep.generatedAt)}</div>
      <h3>Recommended opener</h3>
      <p>${esc(prep.opener)}</p>
      <h3>Known facts</h3>
      ${prep.verifiedFacts.length ? `<div class="pf-ws-grid">${prep.verifiedFacts.map(fact => `
        <div class="pf-ws-fact"><div class="pf-ws-fact-value">${esc(fact.text)}</div><div class="pf-ws-fact-source">source: ${esc(fact.sourceType)}</div></div>
      `).join('')}</div>` : '<p class="pf-ws-empty">No verified facts yet — vertical, location, role, score, website, and email appear here when available.</p>'}
      <h3>Suggested discovery questions</h3>
      ${prep.discoveryQuestions?.length
        ? `<ul>${prep.discoveryQuestions.map(q => `<li>${esc(q)}</li>`).join('')}</ul>`
        : '<p class="pf-ws-empty">No discovery questions configured for this vertical.</p>'}
      <h3>Possible pain points — unverified</h3>
      ${(prep.painPointHypotheses || []).map(h => `
        <div class="pf-ws-hypothesis"><span class="pf-ws-hypothesis-tag">Hypothesis — not verified</span>${esc(h.text)}</div>
      `).join('') || '<p class="pf-ws-empty">No hypotheses for this vertical.</p>'}
      <h3>Proof points</h3>
      <ul>${(prep.proofPoints || []).map(point => `<li>${esc(point)}</li>`).join('')}</ul>
      <h3>Objection responses</h3>
      ${(prep.objections || []).map(o => `
        <div class="pf-ws-note"><div class="pf-ws-note-meta">“${esc(o.objection)}”</div>${esc(o.response)}</div>
      `).join('') || '<p class="pf-ws-empty">No objection prompts configured.</p>'}
      <h3>Outcomes to aim for</h3>
      <div class="pf-ws-grid">
        <div class="pf-ws-fact"><div class="pf-ws-fact-label">Desired</div><div class="pf-ws-fact-value">${esc(prep.desiredOutcome)}</div></div>
        <div class="pf-ws-fact"><div class="pf-ws-fact-label">Fallback</div><div class="pf-ws-fact-value">${esc(prep.fallbackOutcome)}</div></div>
      </div>
    `;
  }

  function historyBucket(item) {
    const type = String(item.type || '').toLowerCase();
    const source = String(item.source || '').toLowerCase();
    if (type === 'lifecycle_transition' || source.includes('lifecycle')) return 'lifecycle';
    if (type === 'email' || source.includes('email') || /emmett|brevo|mail/.test(source)) return 'email';
    if (type === 'call' || source === 'activity_log' || source === 'call_dispositions') return 'call';
    if (source === 'touchpoints' && (type.includes('sms') || type.includes('text'))) return 'call';
    if (/system|cron|agent_log|scout/.test(source) || type === 'system') return 'system';
    return 'other';
  }

  function historyHtml(ws) {
    const notes = ws.notes?.operatorNotes || [];
    const buckets = {
      notes: notes.map(note => ({
        occurredAt: note.createdAt,
        summary: note.text,
        meta: `${note.noteType} · ${note.author || 'operator'} · ${note.source}`,
      })),
      call: [],
      email: [],
      lifecycle: [],
      system: [],
      other: [],
    };
    for (const item of ws.history || []) {
      const key = historyBucket(item);
      buckets[key === 'other' ? 'other' : key].push({
        occurredAt: item.occurredAt,
        summary: item.summary,
        meta: `${item.type} · ${item.source}${item.actorName ? ` · ${item.actorName}` : ''}`,
      });
    }

    function section(title, empty, items) {
      if (!items.length) {
        return `<h3>${title}</h3><p class="pf-ws-empty">${empty}</p>`;
      }
      return `
        <h3>${title}</h3>
        ${items.map(item => `
          <div class="pf-ws-history-item">
            <div class="pf-ws-history-time">${fmtDate(item.occurredAt)}</div>
            <div>
              <div>${esc(item.summary)}</div>
              <div class="pf-ws-fact-source">${esc(item.meta)}</div>
            </div>
          </div>
        `).join('')}
      `;
    }

    if (!ws.history?.length && !notes.length) {
      return `
        <h3>Interaction history</h3>
        <p class="pf-ws-empty">No calls, emails, notes, or lifecycle changes yet. After the first logged call, prior outcome and promised next step will appear here.</p>
      `;
    }

    return [
      section('Notes (newest first)', 'No operator notes yet.', buckets.notes),
      section('Call notes & activity', 'No call activity logged yet.', buckets.call),
      section('Emails', 'No email touchpoints recorded for this prospect.', buckets.email),
      section('Lifecycle changes', 'No stage transitions recorded yet.', buckets.lifecycle),
      section('System & other', 'No system events for this prospect.', [...buckets.system, ...buckets.other]),
    ].join('');
  }

  // Dynamic outcome form: only the fields the selected flow declares.
  function outcomeFieldsHtml(flow) {
    if (!flow) return '<p class="pf-ws-empty">Pick the call outcome above.</p>';
    const f = flow.fields || {};
    const parts = [];
    const [severity, text] = flow.consequence || ['ok', ''];
    parts.push(`<div class="pf-outcome-consequence pf-consequence-${esc(severity)}" id="pf-ws-outcome-consequence">${esc(text)}</div>`);
    if (f.gatekeeper) {
      parts.push(`
        <label for="pf-ws-gatekeeper">What happened with the gatekeeper?</label>
        <select id="pf-ws-gatekeeper">
          <option value="gatekeeper_relayed">Message relayed to the decision-maker</option>
          <option value="gatekeeper_blocked">Blocked — could not get through</option>
        </select>
      `);
    }
    if (f.summary) {
      parts.push(`
        <label for="pf-ws-outcome-summary">Outcome summary — required</label>
        <textarea id="pf-ws-outcome-summary" rows="2" placeholder="What happened on the call?"></textarea>
      `);
    }
    if (f.nextStep) {
      parts.push(`
        <label for="pf-ws-outcome-next-step">${esc(f.nextStepLabel || 'Next step — required')}</label>
        <textarea id="pf-ws-outcome-next-step" rows="2" placeholder="What happens next, and when?"></textarea>
      `);
    }
    if (f.reason) {
      parts.push(`
        <label for="pf-ws-outcome-reason">${esc(f.reasonLabel || 'Reason — required')}</label>
        <textarea id="pf-ws-outcome-reason" rows="2" placeholder="Their words, not yours"></textarea>
      `);
    }
    if (f.notes) {
      parts.push(`
        <label for="pf-ws-outcome-notes">Notes</label>
        <textarea id="pf-ws-outcome-notes" rows="4" placeholder="What happened? Who did you speak with? Any objections or timing cues worth remembering."></textarea>
      `);
    }
    let suggestedCallback = '';
    if (f.callback) {
      const required = String(f.callback).startsWith('required');
      const suggestedDays = /-(\d+)$/.exec(String(f.callback))?.[1];
      suggestedCallback = suggestedDays ? defaultCallback(Number(suggestedDays)) : '';
      const whenLabel = suggestedCallback
        ? new Date(suggestedCallback).toLocaleString([], { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })
        : null;
      parts.push(`
        <label for="pf-ws-outcome-callback">Callback ${required ? '— required' : '(optional)'}</label>
        <input type="datetime-local" id="pf-ws-outcome-callback" value="${esc(suggestedCallback)}">
        ${whenLabel ? `<p class="pf-ws-fact-source">Suggested: ${esc(whenLabel)}</p>` : ''}
      `);
    }
    if (f.confirm) {
      parts.push(`
        <label style="display:flex;align-items:center;gap:10px;text-transform:none;letter-spacing:0;font-family:var(--pf-font-body);font-size:0.82rem;color:var(--pf-red);min-height:var(--pf-touch-target);">
          <input type="checkbox" id="pf-ws-outcome-confirm" style="width:22px;height:22px;min-height:22px;flex-shrink:0;">
          <span>${esc(f.confirm)}</span>
        </label>
      `);
    }
    const saveLabel = dynamicSaveLabel(flow, suggestedCallback);
    parts.push(`
      <div style="margin-top:10px">
        <button type="button" class="pf-btn pf-primary" data-pf-ws="save-outcome">${esc(saveLabel)}</button>
      </div>
    `);
    return parts.join('');
  }

  function dynamicSaveLabel(flow, suggestedCallback = '') {
    if (!flow) return 'Save outcome';
    const cbVal = document.getElementById('pf-ws-outcome-callback')?.value || suggestedCallback;
    if ((flow.id === 'callback_requested' || flow.id === 'answered_not_interested') && cbVal) {
      const when = new Date(cbVal);
      if (!Number.isNaN(when.getTime())) {
        const stamp = when.toLocaleString([], { month: 'short', day: 'numeric' });
        if (flow.id === 'answered_not_interested') return `Save — nurture; revisit ~${stamp}`;
        return `Save — schedule callback for ${stamp}`;
      }
    }
    return flow.saveLabel || 'Save outcome';
  }

  // Persistent completion footer for the desktop outcome pane: the selected
  // outcome's Save action (with its consequence) is always visible without
  // scrolling. Hidden on mobile, where the sticky action bar owns Save.
  function outcomeFooterHtml(flow) {
    if (!flow) {
      return `
        <span class="pf-outcome-footer-hint">Select an outcome to finish logging this call.</span>
        <button type="button" class="pf-btn pf-primary" data-pf-ws="save-outcome" disabled>Save outcome</button>
      `;
    }
    return `
      <span class="pf-outcome-footer-hint">${esc(flow.label)}${flow.consequence?.[1] ? ` — ${esc(flow.consequence[1])}` : ''}</span>
      <button type="button" class="pf-btn pf-primary" data-pf-ws="save-outcome">${esc(dynamicSaveLabel(flow))}</button>
    `;
  }

  function outcomeHtml(ws) {
    const phone = ws.prospect.phone || {};
    const selected = state.outcomeFlow;
    return `
      <div class="pf-outcome-scroll">
      <h3>Log call outcome</h3>
      <p class="pf-ws-fact-source" style="font-family:var(--pf-font-mono);font-size:0.68rem;">
        ${phone.display ? `Number dialed: ${esc(phone.display)}` : 'No phone on file'}
      </p>
      ${OUTCOME_GROUPS.map(group => {
        const flows = OUTCOME_FLOWS.filter(flow => flow.group === group.id);
        if (!flows.length) return '';
        return `
          <div class="pf-outcome-group">
            <div class="pf-outcome-group-label">${esc(group.label)}</div>
            <div class="pf-outcome-options" role="group" aria-label="${esc(group.label)}">
              ${flows.map(flow => `
                <button type="button" class="pf-outcome-option" data-pf-outcome="${flow.id}" aria-pressed="${selected === flow.id ? 'true' : 'false'}">${esc(flow.label)}</button>
              `).join('')}
            </div>
          </div>
        `;
      }).join('')}
      <label for="pf-ws-more-outcomes">More outcomes</label>
      <select id="pf-ws-more-outcomes">
        <option value="">—</option>
        ${MORE_OUTCOMES.map(flow => `<option value="${flow.id}" ${selected === flow.id ? 'selected' : ''}>${esc(flow.label)}</option>`).join('')}
      </select>
      <div id="pf-ws-outcome-fields" aria-live="off">${outcomeFieldsHtml(flowById(selected))}</div>
      <h3 id="pf-ws-callback-anchor">Next-step controls</h3>
      <label for="pf-ws-callback-input">Schedule callback (without logging a call)</label>
      <input type="datetime-local" id="pf-ws-callback-input" value="">
      <div style="margin-top:8px;display:flex;gap:8px;flex-wrap:wrap;">
        <button type="button" class="pf-btn pf-primary" data-pf-ws="save-callback">Save callback</button>
        <button type="button" class="pf-btn" data-pf-ws="clear-callback">Clear callback</button>
      </div>
      <h3>Change stage (confirmed action)</h3>
      <label for="pf-ws-stage-select">Canonical stage</label>
      <select id="pf-ws-stage-select">
        ${window.PulseforgeLifecycle.STAGES.map(stage => `<option value="${stage}" ${stage === ws.lifecycle.canonicalStage ? 'selected' : ''}>${esc(window.PulseforgeLifecycle.stageLabel(stage))}</option>`).join('')}
      </select>
      <label for="pf-ws-stage-reason">Reason / handoff note (required for Booked and Dead)</label>
      <textarea id="pf-ws-stage-reason" rows="2" placeholder="Why is this prospect moving?"></textarea>
      <div style="margin-top:8px">
        <button type="button" class="pf-btn pf-primary" data-pf-ws="save-stage">Save stage</button>
      </div>
      </div>
      <div class="pf-outcome-footer" id="pf-ws-outcome-footer">${outcomeFooterHtml(flowById(selected))}</div>
    `;
  }

  const PANES = {
    context: contextHtml,
    prep: () => prepHtml(state.preparation),
    history: historyHtml,
    outcome: outcomeHtml,
  };

  function render() {
    const root = document.getElementById('pf-workspace-root');
    if (!root || !state.workspace) return;
    root.querySelector('.pf-workspace-header').innerHTML = headerHtml(state.workspace);
    root.querySelector('.pf-workspace-actions').innerHTML = actionsHtml(state.workspace);
    root.querySelector('.pf-workspace-sticky-actions').innerHTML = stickyActionsHtml(state.workspace);
    root.querySelectorAll('.pf-workspace-tabs [role="tab"]').forEach(tabEl => {
      const selected = tabEl.dataset.tab === state.activeTab;
      tabEl.setAttribute('aria-selected', selected ? 'true' : 'false');
      tabEl.tabIndex = selected ? 0 : -1;
    });
    root.querySelectorAll('.pf-workspace-pane').forEach(pane => {
      const renderer = PANES[pane.dataset.pane];
      pane.innerHTML = renderer ? renderer(state.workspace) : '';
      pane.classList.toggle('pf-active', pane.dataset.pane === state.activeTab);
    });
  }

  function setTab(tab) {
    state.activeTab = tab;
    const root = document.getElementById('pf-workspace-root');
    if (!root) return;
    root.querySelectorAll('.pf-workspace-tabs [role="tab"]').forEach(tabEl => {
      const selected = tabEl.dataset.tab === tab;
      tabEl.setAttribute('aria-selected', selected ? 'true' : 'false');
      tabEl.tabIndex = selected ? 0 : -1;
    });
    root.querySelectorAll('.pf-workspace-pane').forEach(pane => {
      pane.classList.toggle('pf-active', pane.dataset.pane === tab);
    });
    // Sticky bar swaps Outcome ↔ Save depending on the active tab.
    if (state.workspace) {
      root.querySelector('.pf-workspace-sticky-actions').innerHTML = stickyActionsHtml(state.workspace);
    }
  }

  function setOutcomeFlow(flowId) {
    state.outcomeFlow = flowId || null;
    const root = document.getElementById('pf-workspace-root');
    if (!root) return;
    root.querySelectorAll('[data-pf-outcome]').forEach(btn => {
      btn.setAttribute('aria-pressed', btn.dataset.pfOutcome === flowId ? 'true' : 'false');
    });
    const more = root.querySelector('#pf-ws-more-outcomes');
    if (more && !MORE_OUTCOMES.some(flow => flow.id === flowId)) more.value = '';
    const fields = root.querySelector('#pf-ws-outcome-fields');
    const flow = flowById(flowId);
    if (fields) fields.innerHTML = outcomeFieldsHtml(flow);
    const footer = root.querySelector('#pf-ws-outcome-footer');
    if (footer) footer.innerHTML = outcomeFooterHtml(flow);
    if (state.workspace) {
      root.querySelector('.pf-workspace-sticky-actions').innerHTML = stickyActionsHtml(state.workspace);
    }
    if (flow) {
      // Screen readers hear which fields appeared and what the outcome does.
      window.PulseforgeA11y?.announce(`${flow.label}. ${flow.consequence?.[1] || ''}`);
    }
  }

  // ── Data ──────────────────────────────────────────────────────────────
  async function load(prospectId) {
    state.prospectId = prospectId;
    state.workspace = null;
    state.preparation = null;
    const [workspace, preparation] = await Promise.all([
      window.PulseforgeApi.getWorkspace(prospectId),
      window.PulseforgeApi.getCallPreparation(prospectId).catch(() => null),
    ]);
    state.workspace = workspace;
    state.preparation = preparation;
  }

  async function refresh() {
    try {
      await load(state.prospectId);
      render();
      if (state.outcomeFlow) setOutcomeFlow(null);
    } catch (err) {
      window.PulseforgeA11y?.announce(`Could not refresh workspace: ${err.message}`, { assertive: true });
    }
  }

  // ── Actions ───────────────────────────────────────────────────────────
  async function saveStage() {
    const stage = document.getElementById('pf-ws-stage-select')?.value;
    const reason = document.getElementById('pf-ws-stage-reason')?.value?.trim();
    if (['booked', 'dead'].includes(stage) && !reason) {
      window.PulseforgeA11y?.announce('A reason or handoff note is required for Booked and Dead', { assertive: true });
      return;
    }
    try {
      await window.PulseforgeApi.transitionLifecycle(state.prospectId, { target_stage: stage, reason: reason || undefined });
      window.PulseforgeA11y?.announce(`Stage saved: ${window.PulseforgeLifecycle.stageLabel(stage)}`);
      document.dispatchEvent(new CustomEvent('pulseforge:lifecycle-changed', { detail: { prospectId: state.prospectId, stage } }));
      await refresh();
    } catch (err) {
      window.PulseforgeA11y?.announce(`Stage change failed: ${err.message}`, { assertive: true });
    }
  }

  async function saveCallback(clear = false) {
    const input = document.getElementById('pf-ws-callback-input');
    const value = clear ? null : (input?.value ? new Date(input.value).toISOString() : null);
    if (!clear && !value) {
      window.PulseforgeA11y?.announce('Pick a callback date and time first', { assertive: true });
      return;
    }
    try {
      const stage = state.workspace.lifecycle.canonicalStage;
      await window.PulseforgeApi.transitionLifecycle(state.prospectId, {
        target_stage: stage,
        reason: ['dead', 'booked'].includes(stage) ? 'Callback update' : undefined,
        callback_at: value,
      });
      window.PulseforgeA11y?.announce(clear ? 'Callback cleared' : 'Callback saved');
      document.dispatchEvent(new CustomEvent('pulseforge:lifecycle-changed', { detail: { prospectId: state.prospectId } }));
      await refresh();
    } catch (err) {
      window.PulseforgeA11y?.announce(`Callback save failed: ${err.message}`, { assertive: true });
    }
  }

  async function saveNote() {
    const input = document.getElementById('pf-ws-note-input');
    const text = input?.value?.trim();
    if (!text) {
      window.PulseforgeA11y?.announce('Write the note first', { assertive: true });
      return;
    }
    try {
      await window.PulseforgeApi.addNote(state.prospectId, text);
      window.PulseforgeA11y?.announce('Note saved');
      await refresh();
      setTab('context');
    } catch (err) {
      window.PulseforgeA11y?.announce(`Note save failed: ${err.message}`, { assertive: true });
    }
  }

  async function saveOutcome() {
    const flow = flowById(state.outcomeFlow);
    if (!flow) {
      window.PulseforgeA11y?.announce('Pick the call outcome first', { assertive: true });
      return;
    }
    const f = flow.fields || {};
    const read = id => document.getElementById(id)?.value?.trim() || '';
    const summary = read('pf-ws-outcome-summary');
    const nextStep = read('pf-ws-outcome-next-step');
    const reason = read('pf-ws-outcome-reason');
    const notes = read('pf-ws-outcome-notes');
    const callbackValue = document.getElementById('pf-ws-outcome-callback')?.value || '';

    if (f.summary === 'required' && !summary) {
      window.PulseforgeA11y?.announce('This outcome requires a summary', { assertive: true });
      return;
    }
    if (f.nextStep === 'required' && !nextStep) {
      window.PulseforgeA11y?.announce('This outcome requires a documented next step', { assertive: true });
      return;
    }
    if (f.reason === 'required' && !reason) {
      window.PulseforgeA11y?.announce('This outcome requires a reason', { assertive: true });
      return;
    }
    if (f.callback && String(f.callback).startsWith('required') && !callbackValue) {
      window.PulseforgeA11y?.announce('This outcome requires a scheduled callback', { assertive: true });
      return;
    }
    if (f.confirm && !document.getElementById('pf-ws-outcome-confirm')?.checked) {
      window.PulseforgeA11y?.announce('Confirm the suppression checkbox first', { assertive: true });
      return;
    }

    let disposition = flow.server;
    if (f.gatekeeper) {
      disposition = document.getElementById('pf-ws-gatekeeper')?.value || flow.server;
    }
    const usesStructuredNotes = Boolean(f.summary || f.reason);
    const body = {
      disposition,
      notes: notes || summary || '',
      callback_at: callbackValue ? new Date(callbackValue).toISOString() : undefined,
    };
    if (usesStructuredNotes) {
      body.structured_notes = {
        summary: summary || undefined,
        next_step: nextStep || undefined,
        reason: reason || undefined,
      };
    }
    try {
      await window.PulseforgeApi.logCallDisposition(state.prospectId, body);
      window.PulseforgePhone?.clearActiveCall();
      window.PulseforgeA11y?.announce(`Call outcome saved: ${flow.label}`);
      document.dispatchEvent(new CustomEvent('pulseforge:lifecycle-changed', { detail: { prospectId: state.prospectId, disposition } }));
      await refresh();
      setTab('context');
    } catch (err) {
      window.PulseforgeA11y?.announce(`Outcome save failed: ${err.message}`, { assertive: true });
    }
  }

  function beginCall() {
    const ws = state.workspace;
    if (!ws?.prospect?.phone?.callable) return;
    window.PulseforgePhone.beginDialHandoff({
      prospectId: ws.prospect.id,
      phone: ws.prospect.phone.normalized || ws.prospect.phone.raw,
      companyName: ws.prospect.companyName,
      clientId: ws.prospect.clientId,
      workspaceRoute: window.location.pathname,
    });
  }

  // ── Dialog lifecycle ──────────────────────────────────────────────────
  function close() {
    const root = document.getElementById('pf-workspace-root');
    if (root) root.remove();
    if (state.releaseFocus) {
      state.releaseFocus();
      state.releaseFocus = null;
    }
    state.open = false;
    state.prospectId = null;
    state.outcomeFlow = null;
  }

  function mount() {
    const backdrop = document.createElement('div');
    backdrop.id = 'pf-workspace-root';
    backdrop.className = 'pf-workspace-backdrop';
    backdrop.innerHTML = `
      <div class="pf-workspace" role="dialog" aria-modal="true" aria-labelledby="pf-ws-heading">
        <div class="pf-workspace-header"><div class="pf-workspace-title"><h2 id="pf-ws-heading">Loading…</h2></div></div>
        <div class="pf-workspace-actions"></div>
        <div class="pf-workspace-tabs" role="tablist" aria-label="Prospect workspace sections">
          ${TABS.map(tab => `<button type="button" role="tab" data-tab="${tab.id}" aria-selected="${tab.id === state.activeTab ? 'true' : 'false'}" tabindex="${tab.id === state.activeTab ? '0' : '-1'}">${tab.label}</button>`).join('')}
        </div>
        <div class="pf-workspace-panes">
          <div class="pf-ws-col-left">
            <section class="pf-workspace-pane" data-pane="context" aria-label="Prospect context"><p class="pf-ws-empty">Loading…</p></section>
            <section class="pf-workspace-pane" data-pane="history" aria-label="Interaction history"></section>
          </div>
          <section class="pf-workspace-pane" data-pane="prep" aria-label="Call preparation"><p class="pf-ws-empty">Loading…</p></section>
          <section class="pf-workspace-pane" data-pane="outcome" aria-label="Outcome and next-step controls"></section>
        </div>
        <div class="pf-workspace-sticky-actions"></div>
      </div>
    `;
    backdrop.addEventListener('mousedown', event => {
      if (event.target === backdrop) close();
    });
    backdrop.addEventListener('change', event => {
      if (event.target.id === 'pf-ws-more-outcomes' && event.target.value) {
        setOutcomeFlow(event.target.value);
      }
    });
    // Save copy states the consequence: keep every Save button in sync with
    // the chosen callback date (e.g. "Save — schedule callback for Aug 4").
    backdrop.addEventListener('input', event => {
      if (event.target.id !== 'pf-ws-outcome-callback') return;
      const flow = flowById(state.outcomeFlow);
      if (!flow) return;
      const label = dynamicSaveLabel(flow);
      backdrop.querySelectorAll('[data-pf-ws="save-outcome"]:not(:disabled)').forEach(btn => { btn.textContent = label; });
    });
    backdrop.addEventListener('click', event => {
      const outcomeBtn = event.target.closest('[data-pf-outcome]');
      if (outcomeBtn) {
        setOutcomeFlow(outcomeBtn.dataset.pfOutcome);
        return;
      }
      const tabButton = event.target.closest('[role="tab"]');
      if (tabButton) {
        setTab(tabButton.dataset.tab);
        return;
      }
      const action = event.target.closest('[data-pf-ws]')?.dataset.pfWs;
      if (!action) return;
      if (action === 'close') close();
      else if (action === 'call' || action === 'tel') {
        event.preventDefault();
        beginCall();
      }
      else if (action === 'copy-phone') window.PulseforgePhone.copyPhone(state.workspace?.prospect?.phone?.raw);
      else if (action === 'tab-outcome') setTab('outcome');
      else if (action === 'tab-callback') { setTab('outcome'); document.getElementById('pf-ws-callback-anchor')?.scrollIntoView(); }
      else if (action === 'tab-notes') setTab('context');
      else if (action === 'tab-opportunity') { setTab('context'); document.getElementById('pf-ws-opportunity-anchor')?.scrollIntoView(); }
      else if (action === 'save-stage') saveStage();
      else if (action === 'save-callback') saveCallback(false);
      else if (action === 'clear-callback') saveCallback(true);
      else if (action === 'save-note') saveNote();
      else if (action === 'save-outcome') saveOutcome();
    });
    document.body.appendChild(backdrop);
    const dialog = backdrop.querySelector('.pf-workspace');
    state.releaseFocus = window.PulseforgeA11y.trapFocus(dialog, { onEscape: close });
    window.PulseforgeA11y.enableTablistKeyboard(
      backdrop.querySelector('.pf-workspace-tabs'),
      tabEl => setTab(tabEl.dataset.tab)
    );
    return backdrop;
  }

  async function open(prospectId, { intent } = {}) {
    if (state.open) close();
    state.open = true;
    state.outcomeFlow = null;
    state.activeTab = intent === 'log_outcome' ? 'outcome' : 'prep';
    mount();
    try {
      await load(prospectId);
      render();
    } catch (err) {
      const pane = document.querySelector('#pf-workspace-root .pf-workspace-pane');
      if (pane) pane.innerHTML = `<div class="pf-ws-conflict" role="alert">Could not load workspace: ${esc(err.message)}</div>`;
      window.PulseforgeA11y?.announce(`Workspace failed to load: ${err.message}`, { assertive: true });
    }
  }

  // Restore-from-call events (dial handoff continuity).
  document.addEventListener('pulseforge:restore-call', event => {
    const { prospectId, intent } = event.detail || {};
    if (!prospectId) return;
    open(prospectId, { intent: intent === 'log_outcome' ? 'log_outcome' : undefined });
  });

  window.PulseforgeWorkspace = { close, open, refresh, OUTCOME_FLOWS, MORE_OUTCOMES };
})();
