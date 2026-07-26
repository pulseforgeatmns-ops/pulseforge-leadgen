'use strict';

/**
 * Command Deck UI — SPEC-008 + SPEC-011 Live Intelligence + SPEC-012 Operator Intelligence.
 * Render-only consumer of GET /api/v1/command-deck → CommandDeckModel.
 * Soft-poll evolves the deck gently — never calculate, rank, filter, sort, or invent.
 * Operator events personalize presentation only.
 */

(function () {
  const CACHE_KEY = 'pulseforge.commandDeck.lastSuccessful';
  const LIVE_POLL_MS = 45000;
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
  /** @type {string|null} */
  let workspaceSessionId = null;
  /** @type {object|null} */
  let workspaceContext = null;
  /** @type {Element|null} */
  let workspaceLastFocus = null;
  /** @type {object|null} */
  let investigationFocus = null;
  /** @type {ReturnType<typeof window.PulseforgeInvestigation.createInvestigation>|null} */
  let investigation = null;
  /** @type {string} */
  let liveCursor = '';
  /** @type {number|null} */
  let livePollTimer = null;
  /** @type {boolean} */
  let livePollInFlight = false;

  const els = {
    status: document.getElementById('cdStatus'),
    liveNotify: document.getElementById('cdLiveNotify'),
    error: document.getElementById('cdError'),
    timestamp: document.getElementById('cdTimestamp'),
    morning: document.getElementById('cdMorningBrief'),
    hla: document.getElementById('cdHighestLeverage'),
    secondary: document.getElementById('cdSecondary'),
    queue: document.getElementById('cdPriorityQueue'),
    askForm: document.getElementById('cdAskForm'),
    askInput: document.getElementById('cdAskInput'),
    workspace: document.getElementById('maxWorkspace'),
    mxThread: document.getElementById('mxThread'),
    mxSuggestions: document.getElementById('mxSuggestions'),
    mxAskForm: document.getElementById('mxAskForm'),
    mxAskInput: document.getElementById('mxAskInput'),
    mxAskSend: document.getElementById('mxAskSend'),
    mxContextLabel: document.getElementById('mxContextLabel'),
    mxSwitch: document.getElementById('mxSwitch'),
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
   * @param {{ elevated?: boolean, dominance?: string }} [opts]
   */
  function renderIntelligenceCardHtml(card, opts = {}) {
    if (!card) return '';
    const typeLabel = cardTypeLabel(card.type);
    const actions = Array.isArray(card.actions) ? card.actions : [];
    const primaryAction = actions[0] || null;
    const interactive = Boolean(primaryAction);
    const dominance =
      opts.dominance === 'high' || opts.dominance === 'quiet'
        ? opts.dominance
        : 'normal';
    const conf =
      card.confidence != null && Number.isFinite(Number(card.confidence))
        ? `<span class="cd-chip">Confidence ${escapeHtml(card.confidence)}</span>`
        : '';

    return `
      <article
        class="cd-card${opts.elevated ? ' cd-hla-card' : ''}${interactive ? ' is-interactive' : ''} cd-dominance-${dominance}"
        ${interactive ? 'tabindex="0" role="button"' : ''}
        data-card-id="${escapeHtml(card.id)}"
        data-card-type="${escapeHtml(card.type)}"
        data-cd-dominance="${dominance}"
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
    const presentation = model.presentation || {};
    const dominance = presentation.sectionDominance || {};
    const sectionOrder = Array.isArray(presentation.sectionOrder)
      ? presentation.sectionOrder
      : [
          'morning_brief',
          'highest_leverage',
          'watch_alerts',
          'market_trends',
          'priority_queue',
        ];

    const watchCards = watchAlerts.length
      ? watchAlerts
      : empty.watchAlerts
        ? [empty.watchAlerts]
        : [];
    const marketCards = marketTrends.length
      ? marketTrends
      : empty.marketTrends
        ? [empty.marketTrends]
        : [];

    // Adaptive order for secondary blocks — never hide sections that have cards.
    const secondaryIds = sectionOrder.filter(
      (id) => id === 'watch_alerts' || id === 'market_trends'
    );
    if (!secondaryIds.includes('watch_alerts')) secondaryIds.push('watch_alerts');
    if (!secondaryIds.includes('market_trends')) secondaryIds.push('market_trends');

    /** @type {{ card: object, section: string }[]} */
    const cards = [];
    for (const id of secondaryIds) {
      const list = id === 'watch_alerts' ? watchCards : marketCards;
      for (const c of list) cards.push({ card: c, section: id });
    }

    if (!cards.length) {
      els.secondary.hidden = true;
      return;
    }

    const watchDom = dominance.watch_alerts || 'normal';
    const marketDom = dominance.market_trends || 'normal';

    els.secondary.innerHTML = `
      <p class="cd-kicker">Supporting intelligence</p>
      <div class="cd-secondary-grid">
        ${cards
          .map(({ card, section }) => {
            const dom =
              section === 'watch_alerts'
                ? watchDom
                : section === 'market_trends'
                  ? marketDom
                  : 'normal';
            return renderIntelligenceCardHtml(card, { dominance: dom });
          })
          .join('')}
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
      trackOperator({
        type: 'AskedMax',
        recommendationId: payload && payload.recommendationId,
        companyId: payload && payload.companyId,
        payload: { context: (payload && payload.context) || 'command_deck' },
      });
      openWorkspaceFromAction(payload);
      return;
    }
    if (type === ACTION_TYPES.REVIEW_RECOMMENDATION) {
      const recommendationId = payload && payload.recommendationId;
      if (recommendationId && investigation) {
        const entryLabel =
          (payload && payload.entryLabel) ||
          findQueueCompanyName(
            currentModel,
            recommendationId,
            payload.companyId
          ) ||
          'Highest Leverage Action';
        investigation.seedFromDeck(
          payload.context === 'priority_queue'
            ? 'Priority Queue'
            : entryLabel === 'Highest Leverage Action' ||
                payload.context === 'highest_leverage'
              ? 'Highest Leverage Action'
              : entryLabel
        );
        investigation.openRecommendation(
          recommendationId,
          entryLabel || 'Recommendation'
        );
        return;
      }
      openWorkspaceFromAction({
        ...payload,
        page: 'recommendation',
        prompt: payload.recommendationId
          ? 'Explain this recommendation.'
          : 'Review this recommendation',
      });
      return;
    }
    if (type === ACTION_TYPES.OPEN_COMPANY) {
      const companyId = payload && payload.companyId;
      trackOperator({
        type: 'OpenedSection',
        companyId,
        recommendationId: payload && payload.recommendationId,
        section:
          payload && payload.context === 'watch_alert'
            ? 'watch_alerts'
            : 'priority_queue',
      });
      if (companyId && investigation) {
        const label =
          findQueueCompanyName(
            currentModel,
            payload.recommendationId,
            companyId
          ) ||
          companyId;
        investigation.seedFromDeck(
          payload.context === 'watch_alert' ? 'Watch Alert' : 'Company'
        );
        investigation.openCompany(companyId, label);
        return;
      }
      openWorkspaceFromAction({
        ...payload,
        page: 'company',
        prompt: payload.companyId
          ? 'Explain supporting signals.'
          : 'Open this company',
      });
      return;
    }
    // dismiss / snooze — presentation acknowledgement + operator learning
    trackOperator({
      type:
        type === ACTION_TYPES.SNOOZE
          ? 'SnoozedRecommendation'
          : 'DismissedCard',
      recommendationId: payload && payload.recommendationId,
      companyId: payload && payload.companyId,
    });
    announce(type === ACTION_TYPES.SNOOZE ? 'Snooze noted.' : 'Dismiss noted.');
  }

  /**
   * Fire-and-forget operator interaction event (SPEC-012).
   * Never blocks the UI; never invents intelligence.
   * @param {object} event
   */
  function trackOperator(event) {
    if (!event || !event.type) return;
    const body = {
      type: event.type,
      recommendationId: event.recommendationId || null,
      companyId: event.companyId || null,
      section: event.section || null,
      depth: event.depth != null ? event.depth : null,
      payload: event.payload || null,
    };
    apiRequest('/api/v1/operator/events', {
      method: 'POST',
      body,
    }).catch(() => {
      /* learning is best-effort */
    });
  }

  function tenantIdFromModel(model) {
    return (
      (model && model.meta && model.meta.tenantId) ||
      (model && model.meta && model.meta.clientId) ||
      null
    );
  }

  function buildVisibleCards(model) {
    if (!model) return [];
    if (Array.isArray(model.cards) && model.cards.length) return model.cards;
    const cards = [];
    if (model.highestLeverageAction) {
      const hlaCard = (model.cards || []).find((c) => c.type === 'highest_leverage');
      if (hlaCard) cards.push(hlaCard);
    }
    for (const c of model.watchAlerts || []) cards.push(c);
    for (const c of model.marketTrends || []) cards.push(c);
    return cards;
  }

  function buildMaxContext(payload) {
    const model = currentModel || {};
    const focus = investigationFocus || {};
    const page =
      (payload && payload.page) ||
      focus.page ||
      (payload && payload.recommendationId
        ? 'recommendation'
        : payload && payload.companyId
          ? 'company'
          : 'command-deck');

    const briefing = model.morningBrief
      ? { ...model.morningBrief }
      : null;

    let selectedEntity =
      (payload && payload.selectedEntity) || focus.selectedEntity || null;
    const companyId =
      (payload && payload.companyId) || focus.companyId || null;
    const recommendationId =
      (payload && payload.recommendationId) || focus.recommendationId || null;

    if (!selectedEntity) {
      if (page === 'recommendation' || recommendationId) {
        const hla = model.highestLeverageAction;
        const name =
          (hla && hla.recommendation && hla.recommendation.companyName) ||
          findQueueCompanyName(model, recommendationId, companyId) ||
          recommendationId ||
          'recommendation';
        selectedEntity = {
          id: recommendationId || (hla && hla.recommendation && hla.recommendation.id) || '',
          type: 'recommendation',
          name: String(name),
          companyName: (hla && hla.recommendation && hla.recommendation.companyName) || null,
        };
      } else if (page === 'company' || companyId) {
        const name =
          findQueueCompanyName(model, recommendationId, companyId) ||
          companyId ||
          'company';
        selectedEntity = {
          id: String(companyId || name),
          type: 'company',
          name: String(name),
        };
      } else if (payload && payload.context === 'watch_alert') {
        selectedEntity = {
          id: String((payload && payload.alertId) || 'watch'),
          type: 'watch_alert',
          name: String((payload && payload.title) || 'Watch alert'),
        };
      }
    }

    return {
      page,
      tenantId: String(tenantIdFromModel(model) || 'unknown'),
      companyId: companyId ? String(companyId) : null,
      recommendationId: recommendationId ? String(recommendationId) : null,
      visibleCards: buildVisibleCards(model),
      briefing,
      selectedEntity,
      deck: model,
      asOf: (briefing && briefing.generatedAt) || (model.meta && model.meta.generatedAt) || null,
      trail: (focus.trail || []).slice(),
    };
  }

  function findQueueCompanyName(model, recommendationId, companyId) {
    const items = Array.isArray(model.priorityQueue) ? model.priorityQueue : [];
    for (const item of items) {
      if (
        (recommendationId && String(item.recommendationId) === String(recommendationId)) ||
        (companyId && String(item.companyId) === String(companyId))
      ) {
        return item.company || item.companyName || null;
      }
    }
    const hla = model.highestLeverageAction;
    if (hla && hla.recommendation) {
      if (
        (recommendationId &&
          String(hla.recommendation.id) === String(recommendationId)) ||
        (companyId && String(hla.recommendation.companyId) === String(companyId))
      ) {
        return hla.recommendation.companyName || null;
      }
    }
    return null;
  }

  function apiRequest(path, options) {
    if (window.PulseforgeApi && typeof window.PulseforgeApi.request === 'function') {
      return window.PulseforgeApi.request(path, options);
    }
    const opts = options || {};
    const body =
      opts.body && typeof opts.body !== 'string'
        ? JSON.stringify(opts.body)
        : opts.body;
    return fetch(path, {
      credentials: 'same-origin',
      headers: {
        Accept: 'application/json',
        ...(body ? { 'Content-Type': 'application/json' } : {}),
        ...(opts.headers || {}),
      },
      method: opts.method || 'GET',
      body,
    }).then(async (response) => {
      if (response.status === 401) {
        window.location.href = '/login';
        throw new Error('Session expired');
      }
      const payload = await response.json().catch(() => null);
      if (!response.ok) {
        const err = new Error((payload && payload.message) || 'request_failed');
        err.status = response.status;
        err.payload = payload;
        throw err;
      }
      return payload;
    });
  }

  async function openWorkspaceFromAction(payload) {
    askContext =
      (payload && (payload.context || payload.recommendationId || payload.companyId)) ||
      'command_deck';
    const prompt = (payload && payload.prompt) || '';
    if (prompt && els.askInput) els.askInput.value = prompt;

    // Enrich watch alert opens from card type
    const enriched = { ...(payload || {}) };
    if (
      !enriched.page &&
      enriched.context &&
      String(enriched.context).indexOf('watch') !== -1
    ) {
      enriched.context = 'watch_alert';
    }

    await openWorkspace(buildMaxContext(enriched), {
      initialQuestion: prompt || null,
    });
  }

  async function openWorkspace(context, options = {}) {
    if (!els.workspace) return;
    workspaceLastFocus = document.activeElement;
    workspaceContext = context;

    els.workspace.hidden = false;
    document.body.style.overflow = 'hidden';
    if (els.mxThread) els.mxThread.innerHTML = '';
    if (els.mxSuggestions) els.mxSuggestions.innerHTML = '';
    if (els.mxSwitch) {
      els.mxSwitch.hidden = true;
      els.mxSwitch.textContent = '';
    }
    setContextLabel(context);
    announce('Max intelligence workspace opened.');

    try {
      const opened = await apiRequest('/api/v1/max/workspace/open', {
        method: 'POST',
        body: context,
      });
      workspaceSessionId = opened.sessionId;
      workspaceContext = opened.context || context;
      setContextLabel(workspaceContext);
      if (opened.awareness && opened.awareness.headline) {
        if (els.mxSwitch) {
          els.mxSwitch.hidden = false;
          els.mxSwitch.textContent = opened.awareness.headline;
        }
      }
      appendMaxOpening(opened.opening && opened.opening.fullText);
      renderSuggestions(opened.suggestions || []);
      if (options.initialQuestion) {
        await askWorkspace(options.initialQuestion);
      } else {
        els.mxAskInput?.focus();
      }
    } catch (err) {
      console.error('[max-workspace]', err);
      appendSystemMessage(
        'Max could not open with the current context. ' +
          (err.message || 'Please try again.')
      );
      announce('Max workspace failed to open.');
    }
  }

  function setContextLabel(context) {
    if (!els.mxContextLabel) return;
    const entity =
      context &&
      context.selectedEntity &&
      context.selectedEntity.name
        ? context.selectedEntity.name
        : null;
    const page = (context && context.page) || 'command-deck';
    els.mxContextLabel.textContent = entity
      ? `${page.replace(/-/g, ' ')} · ${entity}`
      : page.replace(/-/g, ' ');
  }

  function closeWorkspace() {
    if (!els.workspace || els.workspace.hidden) return;
    els.workspace.hidden = true;
    document.body.style.overflow = '';
    announce('Max workspace closed.');
    if (workspaceLastFocus && typeof workspaceLastFocus.focus === 'function') {
      workspaceLastFocus.focus();
    }
  }

  function appendSystemMessage(text) {
    if (!els.mxThread) return;
    const div = document.createElement('div');
    div.className = 'mx-msg';
    div.innerHTML = `
      <p class="mx-msg-role">Max</p>
      <p class="mx-msg-body">${escapeHtml(text)}</p>
    `;
    els.mxThread.appendChild(div);
    els.mxThread.scrollTop = els.mxThread.scrollHeight;
  }

  function appendMaxOpening(text) {
    appendSystemMessage(text || 'What would you like to investigate?');
  }

  function appendOperatorMessage(text) {
    if (!els.mxThread) return;
    const div = document.createElement('div');
    div.className = 'mx-msg is-operator';
    div.innerHTML = `
      <p class="mx-msg-role">You</p>
      <p class="mx-msg-body">${escapeHtml(text)}</p>
    `;
    els.mxThread.appendChild(div);
    els.mxThread.scrollTop = els.mxThread.scrollHeight;
  }

  function appendMaxResponse(result) {
    if (!els.mxThread) return;
    const structured = result.structured || {};
    const metadata = result.metadata || structured.metadata || {};
    const div = document.createElement('div');
    div.className = 'mx-msg';

    const evidenceHtml = renderEvidencePanel(structured);
    const metaHtml = renderMetadataStrip(metadata);
    const actionsHtml = renderRecommendedActions(
      result.recommendedActions || structured.recommendedActions || []
    );

    div.innerHTML = `
      <p class="mx-msg-role">Max</p>
      <p class="mx-msg-body">${escapeHtml(result.prose || '')}</p>
      ${evidenceHtml}
      ${metaHtml}
      ${actionsHtml}
    `;
    els.mxThread.appendChild(div);
    bindWorkspaceActions(div);
    els.mxThread.scrollTop = els.mxThread.scrollHeight;
  }

  function renderEvidencePanel(structured) {
    const supporting = structured.supportingEvidence || [];
    const contradicting = structured.contradictingEvidence || [];
    const contributors = structured.confidenceContributors || [];
    const timeline = structured.timelineReferences || [];
    const related = structured.relatedEntities || [];
    const total =
      supporting.length +
      contradicting.length +
      contributors.length +
      timeline.length +
      related.length;
    if (!total) return '';

    return `
      <details class="mx-evidence">
        <summary>Evidence · ${total} references</summary>
        <div class="mx-evidence-body">
          ${evidenceGroup('Supporting evidence', supporting)}
          ${evidenceGroup('Contradicting evidence', contradicting)}
          ${
            contributors.length
              ? `<div class="mx-evidence-group"><h4>Confidence contributors</h4><ul>${contributors
                  .map((c) => `<li>${escapeHtml(c)}</li>`)
                  .join('')}</ul></div>`
              : ''
          }
          ${evidenceGroup('Timeline', timeline)}
          ${evidenceGroup('Related entities', related)}
        </div>
      </details>
    `;
  }

  function evidenceGroup(title, items) {
    if (!items || !items.length) return '';
    return `
      <div class="mx-evidence-group">
        <h4>${escapeHtml(title)}</h4>
        <ul>
          ${items
            .map((item) => {
              const id = item.id || item.nodeId || '';
              const type =
                item.type ||
                item.kind ||
                (title && String(title).toLowerCase().indexOf('related') !== -1
                  ? item.name
                    ? 'company'
                    : 'evidence'
                  : 'evidence');
              const display =
                item.summary || item.statement || item.name || item.id || '';
              if (id && investigation) {
                return `<li><button type="button" class="cd-nav-link" data-mx-nav-type="${escapeHtml(
                  type
                )}" data-mx-nav-id="${escapeHtml(id)}" data-mx-nav-label="${escapeHtml(
                  display
                )}">${escapeHtml(display)}</button></li>`;
              }
              return `<li>${escapeHtml(display)}</li>`;
            })
            .join('')}
        </ul>
      </div>
    `;
  }

  function renderMetadataStrip(metadata) {
    const sources = (metadata && metadata.sourcesUsed) || {};
    const labels = [
      ['briefing', 'Briefing'],
      ['reasoning', 'Reasoning'],
      ['memory', 'Memory'],
      ['policy', 'Policy'],
      ['knowledge', 'Knowledge'],
    ];
    const evidenceCount = metadata.evidenceCount != null ? metadata.evidenceCount : 0;
    const asOf = metadata.asOf ? formatRelativeTime(metadata.asOf) : null;

    return `
      <details class="mx-meta">
        <summary>Generated from · ${evidenceCount} evidence sources</summary>
        <div class="mx-meta-body">
          <div class="mx-meta-sources">
            ${labels
              .map(([key, label]) => {
                const on = Boolean(sources[key]);
                return `<span class="mx-meta-source ${on ? 'is-on' : 'is-off'}">${
                  on ? '✓' : '·'
                } ${escapeHtml(label)}</span>`;
              })
              .join('')}
          </div>
          <div>${evidenceCount} evidence sources</div>
          ${asOf ? `<div>Updated ${escapeHtml(asOf)}</div>` : ''}
        </div>
      </details>
    `;
  }

  function formatRelativeTime(iso) {
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) return String(iso);
    const diffMs = Date.now() - date.getTime();
    const mins = Math.round(diffMs / 60000);
    if (mins < 1) return 'just now';
    if (mins === 1) return '1 minute ago';
    if (mins < 60) return `${mins} minutes ago`;
    const hours = Math.round(mins / 60);
    if (hours === 1) return '1 hour ago';
    if (hours < 48) return `${hours} hours ago`;
    return formatDisplayTime(iso);
  }

  function renderRecommendedActions(actions) {
    if (!actions || !actions.length) return '';
    return `
      <div class="mx-actions">
        ${actions
          .map(
            (action) =>
              `<button type="button" class="mx-action-btn" data-mx-action-type="${escapeHtml(
                action.type
              )}" data-mx-action-payload="${escapeHtml(
                JSON.stringify(action.payload || {})
              )}">${escapeHtml(action.label)}</button>`
          )
          .join('')}
      </div>
    `;
  }

  function bindWorkspaceActions(root) {
    root.querySelectorAll('[data-mx-action-type]').forEach((btn) => {
      btn.addEventListener('click', () => {
        let payload = {};
        try {
          payload = JSON.parse(btn.getAttribute('data-mx-action-payload') || '{}');
        } catch (_err) {
          payload = {};
        }
        const type = btn.getAttribute('data-mx-action-type');
        if (type === ACTION_TYPES.ASK_MAX && payload.prompt) {
          askWorkspace(payload.prompt);
          return;
        }
        handleAction(type, payload);
      });
    });
    root.querySelectorAll('[data-mx-nav-type]').forEach((btn) => {
      btn.addEventListener('click', () => {
        if (!investigation) return;
        const type = btn.getAttribute('data-mx-nav-type');
        const id = btn.getAttribute('data-mx-nav-id');
        const label = btn.getAttribute('data-mx-nav-label') || id;
        closeWorkspace();
        investigation.navigateTo({ type, id, label });
      });
    });
  }

  function renderSuggestions(suggestions) {
    if (!els.mxSuggestions) return;
    els.mxSuggestions.innerHTML = (suggestions || [])
      .map(
        (s) =>
          `<button type="button" class="mx-chip" data-mx-suggestion="${escapeHtml(
            s
          )}">${escapeHtml(s)}</button>`
      )
      .join('');
    els.mxSuggestions.querySelectorAll('[data-mx-suggestion]').forEach((chip) => {
      chip.addEventListener('click', () => {
        askWorkspace(chip.getAttribute('data-mx-suggestion') || '');
      });
    });
  }

  async function askWorkspace(question) {
    const q = String(question || '').trim();
    if (!q) return;
    if (els.mxAskSend) els.mxAskSend.disabled = true;
    appendOperatorMessage(q);
    if (els.mxAskInput) els.mxAskInput.value = '';

    try {
      const result = await apiRequest('/api/v1/max/workspace/ask', {
        method: 'POST',
        body: {
          sessionId: workspaceSessionId,
          question: q,
          context: workspaceContext,
        },
      });
      workspaceSessionId = result.sessionId || workspaceSessionId;
      if (result.context) {
        workspaceContext = result.context;
        setContextLabel(workspaceContext);
      }
      if (result.contextSwitch && els.mxSwitch) {
        els.mxSwitch.hidden = false;
        els.mxSwitch.textContent = result.contextSwitch;
      }
      appendMaxResponse(result);
      renderSuggestions(result.suggestions || []);
    } catch (err) {
      console.error('[max-workspace] ask', err);
      appendSystemMessage(
        'I could not complete that investigation. ' + (err.message || 'Try again.')
      );
    } finally {
      if (els.mxAskSend) els.mxAskSend.disabled = false;
      els.mxAskInput?.focus();
    }
  }

  // Legacy name used by older call sites — routes into workspace
  function inviteAskMax(payload) {
    openWorkspaceFromAction(payload);
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
    if (model && model.live && model.live.cursor) {
      liveCursor = model.live.cursor;
    }
    clearSections();
    hideError();

    applyPresentationLayout(model);
    renderMorningBrief(model);
    renderHighestLeverage(model);
    renderSecondary(model);
    renderPriorityQueue(model);
    stagedReveal();
    appendEvolutionFootnotes(model);

    const fromCache = Boolean(options.fromCache);
    const evolved = Boolean(options.evolved);
    setStatus(
      fromCache
        ? 'Showing last successful briefing'
        : evolved
          ? 'Intelligence evolved'
          : model.meta && model.meta.withinTarget === false
            ? 'Briefing ready'
            : 'Briefing assembled'
    );
    announce(
      fromCache
        ? 'Showing last successful briefing.'
        : evolved
          ? 'Intelligence updated quietly.'
          : (model.morningBrief && model.morningBrief.headline) || 'Command Deck ready.'
    );
    ensureLivePoll();
  }

  /**
   * Apply adaptive section dominance from Operator Intelligence.
   * Never hides sections — only visual priority.
   * @param {object} model
   */
  function applyPresentationLayout(model) {
    const dominance =
      (model && model.presentation && model.presentation.sectionDominance) ||
      {};
    const map = [
      [els.morning, 'morning_brief'],
      [els.hla, 'highest_leverage'],
      [els.secondary, 'watch_alerts'],
      [els.queue, 'priority_queue'],
    ];
    for (const [el, key] of map) {
      if (!el) continue;
      el.classList.remove(
        'cd-dominance-high',
        'cd-dominance-normal',
        'cd-dominance-quiet'
      );
      const level = dominance[key] || 'normal';
      el.classList.add(
        level === 'high'
          ? 'cd-dominance-high'
          : level === 'quiet'
            ? 'cd-dominance-quiet'
            : 'cd-dominance-normal'
      );
    }
    // Market trends share secondary; quiet market is handled per-card.
    if (els.secondary && dominance.market_trends === 'quiet') {
      els.secondary.classList.add('cd-has-quiet-market');
    } else if (els.secondary) {
      els.secondary.classList.remove('cd-has-quiet-market');
    }
  }

  function appendEvolutionFootnotes(model) {
    const evolution =
      (model && model.live && model.live.evolution) || [];
    if (!evolution.length || !els.morning || els.morning.hidden) return;
    const existing = els.morning.querySelector('.cd-evolution');
    if (existing) existing.remove();
    const recent = evolution.slice(-4);
    if (!recent.length) return;
    const aside = document.createElement('aside');
    aside.className = 'cd-evolution';
    aside.setAttribute('aria-label', 'Briefing evolution');
    aside.innerHTML = `
      <p class="cd-evolution-label">Evolution</p>
      <ol class="cd-evolution-list">
        ${recent
          .map(
            (e) =>
              `<li><time>${escapeHtml(formatClock(e.at))}</time> ${escapeHtml(
                e.summary || ''
              )}</li>`
          )
          .join('')}
      </ol>
    `;
    els.morning.appendChild(aside);
  }

  function formatClock(iso) {
    if (!iso) return '';
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso);
    return new Intl.DateTimeFormat(undefined, {
      hour: 'numeric',
      minute: '2-digit',
    }).format(d);
  }

  function showLiveNotify(notifications) {
    if (!els.liveNotify) return;
    const list = Array.isArray(notifications) ? notifications : [];
    if (!list.length) {
      els.liveNotify.hidden = true;
      els.liveNotify.textContent = '';
      return;
    }
    const latest = list[list.length - 1];
    els.liveNotify.hidden = false;
    els.liveNotify.textContent = latest.summary || 'Intelligence updated';
    els.liveNotify.classList.remove('is-animating');
    // Restart fade once
    void els.liveNotify.offsetWidth;
    els.liveNotify.classList.add('is-animating');
    window.setTimeout(() => {
      if (els.liveNotify) els.liveNotify.classList.remove('is-animating');
    }, 2400);
  }

  function ensureLivePoll() {
    if (livePollTimer != null) return;
    livePollTimer = window.setInterval(() => {
      softEvolve();
    }, LIVE_POLL_MS);
  }

  async function softEvolve() {
    if (livePollInFlight) return;
    if (document.hidden) return;
    livePollInFlight = true;
    try {
      const qs = new URLSearchParams();
      if (liveCursor) qs.set('since', liveCursor);
      qs.set('refresh', '1');
      const payload = await apiRequest(
        `/api/v1/intelligence/live?${qs.toString()}`
      );
      if (payload.cursor) liveCursor = payload.cursor;

      if (payload.notifications && payload.notifications.length) {
        showLiveNotify(payload.notifications);
      }

      if (
        investigation &&
        investigation.isOpen() &&
        typeof investigation.noteLiveEvents === 'function'
      ) {
        investigation.noteLiveEvents(payload.events || [], payload.affectedEntityIds || []);
      }

      if (payload.deck) {
        const next = payload.deck;
        if (next.live && next.live.cursor) liveCursor = next.live.cursor;
        writeCache(next);
        const shouldEvolve =
          payload.hasUpdates ||
          (payload.notifications && payload.notifications.length > 0) ||
          (next.live && next.live.eventCount > 0);
        if (shouldEvolve) {
          if (investigation && investigation.isOpen()) {
            currentModel = next;
            // Keep investigation stable — deck updates wait for Review / close
            setStatus('New intelligence available');
          } else {
            evolveRender(next);
          }
        } else {
          currentModel = next;
          appendEvolutionFootnotes(next);
        }
      } else if (payload.evolution && payload.evolution.length && currentModel) {
        currentModel = {
          ...currentModel,
          live: {
            ...(currentModel.live || {}),
            cursor: payload.cursor || liveCursor,
            evolution: payload.evolution,
            notifications: payload.notifications || [],
          },
        };
        appendEvolutionFootnotes(currentModel);
      }
    } catch (err) {
      console.warn('[command-deck] live poll', err);
    } finally {
      livePollInFlight = false;
    }
  }

  function evolveRender(model) {
    currentModel = model;
    // Mark previous cards, then re-render with evolve class (fade-in new)
    const wasHla = els.hla ? els.hla.innerHTML : '';
    clearSections();
    hideError();
    renderMorningBrief(model);
    renderHighestLeverage(model);
    renderSecondary(model);
    renderPriorityQueue(model);
    appendEvolutionFootnotes(model);

    [els.morning, els.hla, els.secondary, els.queue].forEach((el) => {
      if (!el || el.hidden) return;
      el.classList.add('is-revealed', 'cd-evolved');
      if (!prefersReducedMotion()) {
        window.setTimeout(() => el.classList.remove('cd-evolved'), 900);
      }
    });

    // One-shot movement indicator on HLA when content changed
    if (els.hla && !els.hla.hidden && els.hla.innerHTML !== wasHla) {
      els.hla.classList.add('cd-moved');
      window.setTimeout(() => els.hla.classList.remove('cd-moved'), 1200);
    }

    setStatus('Intelligence evolved');
    announce('Intelligence updated quietly.');
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
    const focus =
      (investigation && investigation.isOpen() && investigation.focusPayload()) ||
      {};
    openWorkspaceFromAction({
      page: focus.page || 'command-deck',
      companyId: focus.companyId || null,
      recommendationId: focus.recommendationId || null,
      selectedEntity: focus.selectedEntity || null,
      context: askContext || 'command_deck',
      prompt: question || null,
    });
  });

  els.mxAskForm?.addEventListener('submit', (event) => {
    event.preventDefault();
    askWorkspace(els.mxAskInput?.value || '');
  });

  els.workspace?.querySelectorAll('[data-mx-close]').forEach((node) => {
    node.addEventListener('click', () => closeWorkspace());
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && els.workspace && !els.workspace.hidden) {
      closeWorkspace();
    }
  });

  // Make morning brief clickable invite to open Max
  els.morning?.addEventListener('click', (event) => {
    if (event.target.closest('button, a')) return;
    openWorkspaceFromAction({ page: 'command-deck', context: 'morning_brief' });
  });

  if (
    window.PulseforgeInvestigation &&
    typeof window.PulseforgeInvestigation.createInvestigation === 'function'
  ) {
    investigation = window.PulseforgeInvestigation.createInvestigation({
      apiRequest,
      escapeHtml,
      announce,
      onAskMax: (payload) => openWorkspaceFromAction(payload),
      onFocusChange: (focus) => {
        investigationFocus = focus;
      },
      onClose: () => {
        investigationFocus = null;
        trackOperator({ type: 'ReturnedToDeck' });
        // Apply any queued deck evolution after investigation closes
        if (currentModel) evolveRender(currentModel);
      },
      onReviewLive: () => {
        if (currentModel) evolveRender(currentModel);
      },
      onOperatorEvent: (event) => trackOperator(event),
    });
    investigation.init();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => loadDeck());
  } else {
    loadDeck();
  }
})();
