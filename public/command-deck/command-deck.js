'use strict';

/**
 * Command Deck UI — SPEC-008.
 * Render-only consumer of GET /api/v1/command-deck → CommandDeckModel.
 * Never calculate, rank, filter, sort, merge, or infer intelligence.
 */

(function () {
  const CACHE_KEY = 'pulseforge.commandDeck.lastSuccessful';
  const ACTION_TYPES = Object.freeze({
    REVIEW_RECOMMENDATION: 'review_recommendation',
    ASK_MAX: 'ask_max',
    OPEN_COMPANY: 'open_company',
    DISMISS: 'dismiss',
    SNOOZE: 'snooze',
  });

  const CARD_TYPE_LABELS = Object.freeze({
    morning_brief: 'Morning Brief',
    highest_leverage: 'Highest-Leverage Action',
    watch_alert: 'Watch Alert',
    market_trend: 'Trend',
    priority_item: 'Priority',
    empty: 'Empty',
  });

  /** @type {object|null} */
  let currentModel = null;
  /** @type {string|null} */
  let askContext = 'command_deck';

  const els = {
    status: document.getElementById('cdStatus'),
    error: document.getElementById('cdError'),
    timestamp: document.getElementById('cdTimestamp'),
    morning: document.getElementById('cdMorningBrief'),
    hla: document.getElementById('cdHighestLeverage'),
    secondary: document.getElementById('cdSecondary'),
    queue: document.getElementById('cdPriorityQueue'),
    askForm: document.getElementById('cdAskForm'),
    askInput: document.getElementById('cdAskInput'),
    askNote: document.getElementById('cdAskNote'),
  };

  function prefersReducedMotion() {
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  }

  function announce(message) {
    if (window.PulseforgeA11y && typeof window.PulseforgeA11y.announce === 'function') {
      window.PulseforgeA11y.announce(message);
      return;
    }
    if (els.status) els.status.textContent = message;
  }

  function setStatus(text) {
    if (els.status) els.status.textContent = text || '';
  }

  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function formatDisplayTime(iso) {
    if (!iso) return '';
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) return String(iso);
    return new Intl.DateTimeFormat(undefined, {
      weekday: 'long',
      hour: '2-digit',
      minute: '2-digit',
    }).format(date);
  }

  function cardTypeLabel(type) {
    return CARD_TYPE_LABELS[type] || String(type || 'Intelligence');
  }

  function readCache() {
    try {
      const raw = sessionStorage.getItem(CACHE_KEY);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch (_err) {
      return null;
    }
  }

  function writeCache(model) {
    try {
      sessionStorage.setItem(
        CACHE_KEY,
        JSON.stringify({
          savedAt: new Date().toISOString(),
          model,
        })
      );
    } catch (_err) {
      /* quota / private mode — ignore */
    }
  }

  async function fetchCommandDeck() {
    if (window.PulseforgeApi && typeof window.PulseforgeApi.request === 'function') {
      return window.PulseforgeApi.request('/api/v1/command-deck');
    }
    const response = await fetch('/api/v1/command-deck', {
      credentials: 'same-origin',
      headers: { Accept: 'application/json' },
    });
    if (response.status === 401) {
      window.location.href = '/login';
      throw new Error('Session expired');
    }
    const payload = await response.json().catch(() => null);
    if (!response.ok) {
      const err = new Error('command_deck_compose_failed');
      err.status = response.status;
      err.payload = payload;
      throw err;
    }
    return payload;
  }

  function clearSections() {
    for (const key of ['morning', 'hla', 'secondary', 'queue']) {
      const el = els[key];
      if (!el) continue;
      el.hidden = true;
      el.classList.remove('is-revealed');
      el.innerHTML = '';
    }
  }

  function showError(options = {}) {
    const hasCache = Boolean(readCache() && readCache().model);
    els.error.hidden = false;
    els.error.innerHTML = `
      <h2>Today's briefing is unavailable.</h2>
      <div class="cd-error-actions">
        <button type="button" class="cd-btn cd-btn-primary" data-cd-action="retry">Retry</button>
        ${
          hasCache
            ? '<button type="button" class="cd-btn cd-btn-ghost" data-cd-action="view-last">View last successful briefing</button>'
            : ''
        }
      </div>
    `;
    els.error.querySelector('[data-cd-action="retry"]')?.addEventListener('click', () => {
      loadDeck({ force: true });
    });
    els.error.querySelector('[data-cd-action="view-last"]')?.addEventListener('click', () => {
      const cached = readCache();
      if (!cached || !cached.model) return;
      els.error.hidden = true;
      renderModel(cached.model, { fromCache: true });
    });
    if (options.announce !== false) {
      announce("Today's briefing is unavailable.");
    }
  }

  function hideError() {
    els.error.hidden = true;
    els.error.innerHTML = '';
  }

  function renderMorningBrief(model) {
    const brief = model.morningBrief || {};
    const card =
      (model.cards || []).find((c) => c.type === 'morning_brief') || null;
    const headline = brief.headline || (card && card.title) || '';
    const summary = brief.summary || (card && card.summary) || '';
    const generatedAt = brief.generatedAt || (model.meta && model.meta.generatedAt);

    els.timestamp.textContent = formatDisplayTime(generatedAt);

    els.morning.innerHTML = `
      <p class="cd-kicker" id="cdMorningHeading">Morning Brief</p>
      <p class="cd-morning-greeting">Good morning.</p>
      <h1 class="cd-morning-headline">${escapeHtml(headline)}</h1>
      <p class="cd-morning-summary">${escapeHtml(summary)}</p>
    `;
    els.morning.hidden = false;
  }

  function renderHighestLeverage(model) {
    const hla = model.highestLeverageAction;
    const empty = model.emptyStates && model.emptyStates.highestLeverage;
    const card =
      (model.cards || []).find((c) => c.type === 'highest_leverage') ||
      empty ||
      null;

    if (!hla && !card) {
      els.hla.hidden = true;
      return;
    }

    if (!hla && card) {
      els.hla.innerHTML = `
        <p class="cd-kicker" id="cdHlaHeading">Highest-Leverage Action</p>
        ${renderIntelligenceCardHtml(card, { elevated: true })}
      `;
      els.hla.hidden = false;
      bindCardActions(els.hla);
      return;
    }

    const title = card && card.title
      ? card.title
      : [
          hla.recommendation && hla.recommendation.recommendedAction,
          hla.recommendation && hla.recommendation.companyName,
        ]
          .filter(Boolean)
          .join(' ');
    const summary = (card && card.summary) || '';
    const signals = Array.isArray(hla.supportingSignals) ? hla.supportingSignals : [];
    const actions = (card && Array.isArray(card.actions) ? card.actions : []).slice(0, 2);
    const chips = [];
    if (hla.opportunity != null) chips.push(`Opportunity ${hla.opportunity}`);
    if (hla.confidence != null) chips.push(`Confidence ${hla.confidence}`);
    if (hla.trend && hla.trend !== 'insufficient') chips.push(`Trend ${hla.trend}`);
    if (Array.isArray(card && card.sources) && card.sources.length) {
      chips.push(`Evidence: ${card.sources.length} sources`);
    }

    els.hla.innerHTML = `
      <p class="cd-kicker" id="cdHlaHeading">Highest-Leverage Action</p>
      <article class="cd-hla-card" aria-labelledby="cdHlaTitle">
        <h2 class="cd-hla-title" id="cdHlaTitle">${escapeHtml(title)}</h2>
        ${summary ? `<p class="cd-hla-summary">${escapeHtml(summary)}</p>` : ''}
        ${
          chips.length
            ? `<div class="cd-meta-row">${chips
                .map((c) => `<span class="cd-chip">${escapeHtml(c)}</span>`)
                .join('')}</div>`
            : ''
        }
        ${
          signals.length
            ? `<ul class="cd-signals" aria-label="Supporting signals">${signals
                .map(
                  (s) =>
                    `<li>${escapeHtml(s.summary || s.kind || '')}</li>`
                )
                .join('')}</ul>`
            : ''
        }
        ${
          actions.length
            ? `<div class="cd-actions">${actions
                .map((action, index) => {
                  const primary = index === 0;
                  return `<button type="button" class="cd-btn ${
                    primary ? 'cd-btn-primary' : 'cd-btn-ghost'
                  }" data-cd-action-type="${escapeHtml(action.type)}" data-cd-action-payload="${escapeHtml(
                    JSON.stringify(action.payload || {})
                  )}">${escapeHtml(action.label)}</button>`;
                })
                .join('')}</div>`
            : ''
        }
      </article>
    `;
    els.hla.hidden = false;
    bindCardActions(els.hla);
  }

  /**
   * Universal IntelligenceCard renderer — every secondary card uses this.
   * @param {object} card
   * @param {{ elevated?: boolean }} [opts]
   */
  function renderIntelligenceCardHtml(card, opts = {}) {
    if (!card) return '';
    const typeLabel = cardTypeLabel(card.type);
    const actions = Array.isArray(card.actions) ? card.actions : [];
    const primaryAction = actions[0] || null;
    const interactive = Boolean(primaryAction);
    const conf =
      card.confidence != null && Number.isFinite(Number(card.confidence))
        ? `<span class="cd-chip">Confidence ${escapeHtml(card.confidence)}</span>`
        : '';

    return `
      <article
        class="cd-card${opts.elevated ? ' cd-hla-card' : ''}${interactive ? ' is-interactive' : ''}"
        ${interactive ? 'tabindex="0" role="button"' : ''}
        data-card-id="${escapeHtml(card.id)}"
        data-card-type="${escapeHtml(card.type)}"
        ${
          primaryAction
            ? `data-cd-action-type="${escapeHtml(primaryAction.type)}" data-cd-action-payload="${escapeHtml(
                JSON.stringify(primaryAction.payload || {})
              )}"`
            : ''
        }
        aria-label="${escapeHtml(typeLabel + ': ' + (card.title || ''))}"
      >
        <p class="cd-card-type">${escapeHtml(typeLabel)}</p>
        <h3 class="cd-card-title">${escapeHtml(card.title || '')}</h3>
        <p class="cd-card-summary">${escapeHtml(card.summary || '')}</p>
        <div class="cd-card-footer">
          ${conf}
          ${actions
            .slice(0, 2)
            .map(
              (action) =>
                `<button type="button" class="cd-card-link" data-cd-action-type="${escapeHtml(
                  action.type
                )}" data-cd-action-payload="${escapeHtml(
                  JSON.stringify(action.payload || {})
                )}">${escapeHtml(action.label)}</button>`
            )
            .join('')}
        </div>
      </article>
    `;
  }

  function renderSecondary(model) {
    const watchAlerts = Array.isArray(model.watchAlerts) ? model.watchAlerts : [];
    const marketTrends = Array.isArray(model.marketTrends) ? model.marketTrends : [];
    const empty = model.emptyStates || {};

    // Preserve composer order within each section; section order matches layout.
    /** @type {object[]} */
    const cards = [];
    if (watchAlerts.length) {
      for (const c of watchAlerts) cards.push(c);
    } else if (empty.watchAlerts) {
      cards.push(empty.watchAlerts);
    }
    if (marketTrends.length) {
      for (const c of marketTrends) cards.push(c);
    } else if (empty.marketTrends) {
      cards.push(empty.marketTrends);
    }

    if (!cards.length) {
      els.secondary.hidden = true;
      return;
    }

    els.secondary.innerHTML = `
      <p class="cd-kicker">Supporting intelligence</p>
      <div class="cd-secondary-grid">
        ${cards.map((c) => renderIntelligenceCardHtml(c)).join('')}
      </div>
    `;
    els.secondary.hidden = false;
    bindCardActions(els.secondary);
  }

  function renderPriorityQueue(model) {
    const items = Array.isArray(model.priorityQueue) ? model.priorityQueue : [];
    const empty = model.emptyStates && model.emptyStates.priorities;

    if (!items.length) {
      els.queue.innerHTML = `
        <p class="cd-kicker" id="cdQueueHeading">Priority Queue</p>
        <div class="cd-empty-block">
          ${
            empty
              ? `<h3 class="cd-card-title">${escapeHtml(empty.title || '')}</h3>
                 <p class="cd-card-summary">${escapeHtml(empty.summary || '')}</p>`
              : ''
          }
        </div>
      `;
      els.queue.hidden = false;
      return;
    }

    els.queue.innerHTML = `
      <p class="cd-kicker" id="cdQueueHeading">Priority Queue</p>
      <ol class="cd-queue-list">
        ${items
          .map((item) => {
            const movement = item.movement != null ? String(item.movement) : '—';
            const moveClass =
              movement.indexOf('↑') === 0
                ? 'is-up'
                : movement.indexOf('↓') === 0
                  ? 'is-down'
                  : '';
            const rank = String(item.rank != null ? item.rank : '').padStart(2, '0');
            const lineSummary = item.summary != null ? String(item.summary) : '';
            const payload = {
              recommendationId: item.recommendationId || null,
              companyId: item.companyId || null,
              context: 'priority_queue',
            };
            return `
              <li>
                <a
                  class="cd-queue-row"
                  href="#ask-max"
                  data-cd-action-type="${ACTION_TYPES.ASK_MAX}"
                  data-cd-action-payload="${escapeHtml(JSON.stringify(payload))}"
                >
                  <span class="cd-queue-rank">${escapeHtml(rank)}</span>
                  <span class="cd-queue-company">${escapeHtml(item.company || item.companyId || '')}</span>
                  <span class="cd-queue-move ${moveClass}" aria-label="Movement ${escapeHtml(
                    movement
                  )}">${escapeHtml(movement)}</span>
                  <span class="cd-queue-summary">${escapeHtml(lineSummary)}</span>
                </a>
              </li>
            `;
          })
          .join('')}
      </ol>
    `;
    els.queue.hidden = false;
    bindCardActions(els.queue);
  }

  function bindCardActions(root) {
    root.querySelectorAll('[data-cd-action-type]').forEach((node) => {
      const handler = (event) => {
        if (event.type === 'keydown' && event.key !== 'Enter' && event.key !== ' ') return;
        if (event.type === 'keydown') event.preventDefault();
        // Prefer the innermost action target (button/link), not a parent card.
        const target = event.currentTarget;
        if (!target || !target.getAttribute) return;
        const type = target.getAttribute('data-cd-action-type');
        let payload = {};
        try {
          payload = JSON.parse(target.getAttribute('data-cd-action-payload') || '{}');
        } catch (_err) {
          payload = {};
        }
        handleAction(type, payload);
        if (target.tagName === 'A') event.preventDefault();
      };
      node.addEventListener('click', handler);
      if (node.getAttribute('role') === 'button') {
        node.addEventListener('keydown', handler);
      }
    });
  }

  function handleAction(type, payload) {
    if (type === ACTION_TYPES.ASK_MAX) {
      inviteAskMax(payload);
      return;
    }
    if (type === ACTION_TYPES.REVIEW_RECOMMENDATION) {
      inviteAskMax({
        ...payload,
        prompt: payload.recommendationId
          ? `Review recommendation ${payload.recommendationId}`
          : 'Review this recommendation',
      });
      return;
    }
    if (type === ACTION_TYPES.OPEN_COMPANY) {
      inviteAskMax({
        ...payload,
        prompt: payload.companyId
          ? `Tell me about company ${payload.companyId}`
          : 'Open this company',
      });
      return;
    }
    // dismiss / snooze — presentation acknowledgement only until SPEC-006 actions ship
    announce(type === ACTION_TYPES.SNOOZE ? 'Snooze noted.' : 'Dismiss noted.');
  }

  function inviteAskMax(payload) {
    askContext =
      (payload && (payload.context || payload.recommendationId || payload.companyId)) ||
      'command_deck';
    const prompt =
      (payload && payload.prompt) ||
      (payload && payload.recommendationId
        ? `Why is recommendation ${payload.recommendationId} ranked here?`
        : '');
    if (prompt) els.askInput.value = prompt;
    els.askInput.focus();
    els.askNote.hidden = false;
    announce('Ask Max is ready. Enter a question when you want to investigate.');
  }

  function stagedReveal() {
    const stages = [
      els.morning,
      els.hla,
      els.secondary,
      els.queue,
    ].filter((el) => el && !el.hidden);

    if (prefersReducedMotion()) {
      stages.forEach((el) => el.classList.add('is-revealed'));
      return;
    }

    stages.forEach((el, index) => {
      window.setTimeout(() => {
        el.classList.add('is-revealed');
      }, 90 + index * 160);
    });
  }

  function renderModel(model, options = {}) {
    currentModel = model;
    clearSections();
    hideError();

    renderMorningBrief(model);
    renderHighestLeverage(model);
    renderSecondary(model);
    renderPriorityQueue(model);
    stagedReveal();

    const fromCache = Boolean(options.fromCache);
    setStatus(
      fromCache
        ? 'Showing last successful briefing'
        : model.meta && model.meta.withinTarget === false
          ? 'Briefing ready'
          : 'Briefing assembled'
    );
    announce(
      fromCache
        ? 'Showing last successful briefing.'
        : (model.morningBrief && model.morningBrief.headline) || 'Command Deck ready.'
    );
  }

  async function loadDeck(options = {}) {
    setStatus(options.force ? 'Retrying…' : 'Assembling today’s briefing…');
    hideError();
    clearSections();
    try {
      const model = await fetchCommandDeck();
      writeCache(model);
      renderModel(model);
    } catch (err) {
      console.error('[command-deck]', err);
      setStatus('');
      showError();
    }
  }

  els.askForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const question = (els.askInput.value || '').trim();
    if (!question) {
      els.askInput.focus();
      return;
    }
    // SPEC-008: launcher only — never auto-open a workspace.
    els.askNote.hidden = false;
    els.askNote.textContent =
      'Ask Max conversation opens in a later release. Your question is held for context.';
    announce('Question held. Ask Max conversation is not open yet.');
    try {
      sessionStorage.setItem(
        'pulseforge.commandDeck.askDraft',
        JSON.stringify({
          question,
          context: askContext,
          briefingId: currentModel && currentModel.meta && currentModel.meta.briefingId,
          at: new Date().toISOString(),
        })
      );
    } catch (_err) {
      /* ignore */
    }
  });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => loadDeck());
  } else {
    loadDeck();
  }
})();
