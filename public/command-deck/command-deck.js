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
    OPEN_MISSION: 'open_mission',
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
    operations: document.getElementById('cdOperations'),
    secondary: document.getElementById('cdSecondary'),
    queue: document.getElementById('cdPriorityQueue'),
    askForm: document.getElementById('cdAskForm'),
    askInput: document.getElementById('cdAskInput'),
    workspace: document.getElementById('maxWorkspace'),
    missionWorkspace: document.getElementById('missionWorkspace'),
    msnBody: document.getElementById('msnBody'),
    msnActions: document.getElementById('msnActions'),
    msnTitle: document.getElementById('msnTitle'),
    msnStatus: document.getElementById('msnStatus'),
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

  /** SPEC-045 — auto-grow composer (~1–10 lines). */
  const MX_ASK_LINE_PX = 22;
  const MX_ASK_MAX_LINES = 10;

  function autoGrowAskInput() {
    const el = els.mxAskInput;
    if (!el) return;
    el.style.height = 'auto';
    const max = MX_ASK_LINE_PX * MX_ASK_MAX_LINES + 24;
    const next = Math.min(Math.max(el.scrollHeight, MX_ASK_LINE_PX + 20), max);
    el.style.height = `${next}px`;
  }

  function resetAskInput() {
    if (!els.mxAskInput) return;
    els.mxAskInput.value = '';
    els.mxAskInput.style.height = '';
    autoGrowAskInput();
  }

  /**
   * SPEC-045 — detect a ProspectList-shaped block for display cards only.
   * @param {string} text
   */
  function detectProspectListDisplay(text) {
    const raw = String(text || '').replace(/^\uFEFF/, '');
    if (!raw.trim()) return null;
    const lines = raw.split(/\r?\n/);
    let start = -1;
    let hasHeader = false;
    const headerRe =
      /^(company\s*name|company|business(\s*name)?|name)\b/i;
    for (let i = 0; i < lines.length; i += 1) {
      const line = String(lines[i] || '').trim();
      if (!line) continue;
      if (/^(build|create|launch)\s+(a\s+)?campaign\b/i.test(line) && !/,|\t/.test(line)) {
        continue;
      }
      const cells = line.split(/,|\t/).map((c) => c.trim());
      if (cells.some((c) => headerRe.test(c) || /^company_?name$/i.test(c.replace(/\s+/g, '_')))) {
        start = i;
        hasHeader = true;
        break;
      }
      if ((/,|\t/.test(line) && cells.length >= 2) || (!/,|\t/.test(line) && i > 0)) {
        // fall through — may be name list after blank
      }
    }
    if (start < 0) {
      const blankIdx = lines.findIndex((l, idx) => idx > 0 && !String(l).trim());
      if (blankIdx >= 0) {
        const names = [];
        for (let i = blankIdx + 1; i < lines.length; i += 1) {
          const line = String(lines[i] || '').trim();
          if (!line) break;
          if (/^(build|create|launch)\s+(a\s+)?campaign\b/i.test(line)) continue;
          names.push(i);
        }
        if (names.length >= 2) start = names[0];
      }
    }
    if (start < 0) return null;
    let end = start;
    for (let i = start; i < lines.length; i += 1) {
      if (!String(lines[i] || '').trim() && i > start) break;
      end = i;
    }
    const blockLines = lines.slice(start, end + 1).filter((l) => String(l).trim());
    if (blockLines.length < (hasHeader ? 2 : 2)) return null;
    const dataLines = hasHeader ? blockLines.slice(1) : blockLines;
    const count = dataLines.filter((l) => String(l).trim()).length;
    if (count < 1) return null;
    const objective = lines
      .slice(0, start)
      .join('\n')
      .trim();
    return {
      block: blockLines.join('\n'),
      count,
      hasHeader,
      objective,
    };
  }

  /**
   * SPEC-045 — reusable attachment card.
   * @param {object} opts
   */
  function renderAttachmentCard(opts) {
    const id = `attach_${Math.random().toString(36).slice(2, 9)}`;
    const status = opts.status || 'Detected';
    const meta = opts.meta || '';
    const body = opts.body || '';
    return `<div class="mx-attach-card" data-attach-type="${escapeHtml(
      opts.type || 'attachment'
    )}">
      <div class="mx-attach-head">
        <p class="mx-attach-title">${escapeHtml(opts.title || 'Attachment')}</p>
        <span class="cd-chip">${escapeHtml(status)}</span>
      </div>
      ${meta ? `<p class="mx-attach-meta">${escapeHtml(meta)}</p>` : ''}
      <div class="mx-attach-actions">
        <button type="button" class="cd-btn cd-btn-ghost" data-attach-toggle="${id}">View</button>
      </div>
      <div class="mx-attach-body" id="${id}" hidden>
        <pre class="mx-attach-raw">${escapeHtml(body)}</pre>
      </div>
    </div>`;
  }

  function bindAttachmentCards(root) {
    if (!root) return;
    root.querySelectorAll('[data-attach-toggle]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const id = btn.getAttribute('data-attach-toggle');
        const body = id ? document.getElementById(id) : null;
        if (!body) return;
        const open = body.hasAttribute('hidden');
        if (open) body.removeAttribute('hidden');
        else body.setAttribute('hidden', '');
        btn.textContent = open ? 'Hide' : 'View';
      });
    });
  }

  function industryBuckets(prospects) {
    const list = Array.isArray(prospects) ? prospects : [];
    const map = Object.create(null);
    list.forEach((p) => {
      const key = String(p.industry || p.vertical || '').trim();
      if (!key) return;
      map[key] = (map[key] || 0) + 1;
    });
    return Object.entries(map).sort((a, b) => b[1] - a[1]);
  }

  function businessArtifactHeadline(art) {
    const type = String(art.artifactType || art.type || '');
    const p = art.payload || {};
    if (type === 'ProspectList') {
      const n =
        p.prospectCount != null
          ? Number(p.prospectCount)
          : Array.isArray(p.prospects)
            ? p.prospects.length
            : 0;
      return {
        title: 'Prospect List',
        summary: `${n} ${n === 1 ? 'Company' : 'Companies'}`,
      };
    }
    if (type === 'CompanyIntelligence') {
      const n =
        p.enrichedCount != null
          ? Number(p.enrichedCount)
          : Array.isArray(p.prospects)
            ? p.prospects.length
            : 0;
      return {
        title: 'Company Intelligence',
        summary: `${n} packages`,
      };
    }
    if (type === 'OpportunityRanking') {
      const n =
        p.rankedCount != null
          ? Number(p.rankedCount)
          : Array.isArray(p.prospects)
            ? p.prospects.length
            : 0;
      return { title: 'Opportunity Ranking', summary: `${n} ranked` };
    }
    if (type === 'Campaign') {
      const c = p.campaign || {};
      const n =
        c.prospectCount != null
          ? Number(c.prospectCount)
          : Array.isArray(c.prospects)
            ? c.prospects.length
            : Array.isArray(c.mailMerge)
              ? c.mailMerge.length
              : 0;
      const mail = Array.isArray(c.mailMerge) ? c.mailMerge.length : n;
      return {
        title: 'Campaign',
        summary: `${n} prospects · ${mail} personalized`,
      };
    }
    if (type === 'MailPackage') {
      return { title: 'Mail Package', summary: art.summary || 'Ready for review' };
    }
    return {
      title: type || 'Artifact',
      summary: art.summary || 'Published',
    };
  }

  function stageProgressPct(step) {
    const status = String(step.status || 'queued');
    if (status === 'completed' || status === 'stale') return 100;
    if (status === 'running') return 55;
    if (status === 'blocked' || status === 'failed') return 100;
    return 8;
  }

  function reviewDashboardModel(mission, artifacts) {
    const campaignArt = (artifacts || []).find(
      (a) => a.artifactType === 'Campaign' || a.type === 'Campaign'
    );
    const campaign =
      (campaignArt && campaignArt.payload && campaignArt.payload.campaign) ||
      (mission.deliverables && mission.deliverables.campaign) ||
      null;
    const prospects =
      (campaign && Array.isArray(campaign.prospects) && campaign.prospects.length) ||
      (campaign && campaign.prospectCount) ||
      (mission.deliverables &&
        Array.isArray(mission.deliverables.prospects) &&
        mission.deliverables.prospects.length) ||
      0;
    const personalized =
      (campaign && Array.isArray(campaign.mailMerge) && campaign.mailMerge.length) ||
      prospects;
    const warnings = [];
    (mission.plan && mission.plan.steps ? mission.plan.steps : []).forEach((s) => {
      (s.warnings || []).forEach((w) => warnings.push(w));
    });
    if (mission.stageReview && Array.isArray(mission.stageReview.warnings)) {
      mission.stageReview.warnings.forEach((w) => warnings.push(w));
    }
    const warningCount = warnings.length;
    const needsReview = warningCount;
    const ready = Math.max(0, Number(prospects) - needsReview);
    return {
      prospects: Number(prospects) || 0,
      personalized: Number(personalized) || 0,
      warnings: warningCount,
      needsReview,
      ready,
    };
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
    for (const key of ['morning', 'hla', 'operations', 'secondary', 'queue']) {
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

  function renderOperations(model) {
    if (!els.operations) return;
    const ops = model.operations;
    if (!ops) {
      els.operations.hidden = true;
      return;
    }

    const missions = Array.isArray(ops.missions) ? ops.missions : [];
    const summary = ops.summary || {};
    const summaryBits = [];
    if (summary.active) summaryBits.push(`${summary.active} active`);
    if (summary.needsAttention) summaryBits.push(`${summary.needsAttention} need attention`);
    if (summary.finished) summaryBits.push(`${summary.finished} finished`);
    if (summary.blocked) summaryBits.push(`${summary.blocked} blocked`);

    const cardsHtml = missions.length
      ? missions
          .map((m) => {
            const stage = (m.progress && m.progress.currentStage) || m.statusLabel || '';
            const progressLabel =
              (m.progress && m.progress.label) ||
              (m.progress
                ? `${m.progress.completedSteps || 0} / ${m.progress.totalSteps || 0}`
                : '');
            const started = m.startedAt
              ? formatDisplayTime(m.startedAt)
              : m.createdAt
                ? formatDisplayTime(m.createdAt)
                : '';
            const eta = m.estimatedCompletion
              ? formatDisplayTime(m.estimatedCompletion)
              : '';
            return `
          <article class="cd-ops-card" data-mission-id="${escapeHtml(m.id)}">
            <div class="cd-ops-card-head">
              <span class="cd-ops-status" data-status="${escapeHtml(m.status)}">${escapeHtml(
                m.statusLabel || m.status
              )}</span>
              <h3 class="cd-ops-title">${escapeHtml(m.title || 'Mission')}</h3>
            </div>
            <p class="cd-ops-stage">${escapeHtml(stage)}</p>
            ${
              progressLabel
                ? `<p class="cd-ops-progress">${escapeHtml(progressLabel)}</p>`
                : ''
            }
            <div class="cd-ops-meta">
              ${started ? `<span>Started ${escapeHtml(started)}</span>` : ''}
              ${eta ? `<span>ETA ${escapeHtml(eta)}</span>` : ''}
            </div>
            <div class="cd-actions">
              <button type="button" class="cd-btn cd-btn-ghost" data-cd-action-type="${
                ACTION_TYPES.OPEN_MISSION
              }" data-cd-action-payload="${escapeHtml(
                JSON.stringify({ missionId: m.id })
              )}">Expand</button>
            </div>
          </article>`;
          })
          .join('')
      : `<p class="cd-ops-empty">${escapeHtml(
          ops.emptyMessage || 'No active missions.'
        )}</p>`;

    els.operations.innerHTML = `
      <p class="cd-kicker" id="cdOpsHeading">Operations</p>
      ${
        summaryBits.length
          ? `<p class="cd-ops-summary">${escapeHtml(summaryBits.join(' · '))}</p>`
          : ''
      }
      <div class="cd-ops-list">${cardsHtml}</div>
    `;
    els.operations.hidden = false;
    bindCardActions(els.operations);

    if ((window.location.hash || '') === '#operations') {
      els.operations.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
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
    if (type === ACTION_TYPES.OPEN_MISSION || type === 'review_mission') {
      const missionId = payload && payload.missionId;
      if (missionId) openMissionWorkspace(missionId);
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

  async function openMissionWorkspace(missionId) {
    if (!els.missionWorkspace || !missionId) return;
    try {
      const data = await apiRequest(`/api/v1/missions/${encodeURIComponent(missionId)}`);
      const mission = data.mission || {};
      if (els.msnTitle) els.msnTitle.textContent = mission.title || 'Mission';
      if (els.msnStatus) {
        els.msnStatus.textContent = `${mission.status || ''} · ${
          (mission.progress && mission.progress.currentStage) || ''
        }`.trim();
      }

      const steps = (mission.plan && mission.plan.steps) || [];
      const evidence = (data.evidence || [])
        .map((e) => `<li>${escapeHtml(e.summary || '')}</li>`)
        .join('');
      const audit = (data.audit || [])
        .slice(-12)
        .map(
          (a) =>
            `<li><span class="cd-chip">${escapeHtml(a.kind)}</span> ${escapeHtml(
              a.at || ''
            )}</li>`
        )
        .join('');

      const artifacts = Array.isArray(data.artifacts) ? data.artifacts : [];
      const prospectListArt = artifacts.find(
        (a) => a.artifactType === 'ProspectList' || a.type === 'ProspectList'
      );
      const prospectPayload = (prospectListArt && prospectListArt.payload) || {};
      const prospectRows = Array.isArray(prospectPayload.prospects)
        ? prospectPayload.prospects
        : [];
      const prospectCount =
        prospectPayload.prospectCount != null
          ? Number(prospectPayload.prospectCount)
          : prospectRows.length;
      const opMeta = mission.operatorProspectList || null;
      const attachedCount =
        (opMeta && opMeta.prospectCount) ||
        prospectCount ||
        0;
      const buckets = industryBuckets(prospectRows);
      const reviewModel = reviewDashboardModel(mission, artifacts);

      const objectiveRaw = String(mission.objectiveText || '');
      const objectiveFirstLine =
        objectiveRaw.split(/\r?\n/).find((l) => String(l).trim()) || 'Mission objective';
      const objectiveHtml = `<section class="msn-block" id="msnObjectiveBlock">
          <h3>Objective</h3>
          <div data-msn-objective-collapsed>
            <p class="msn-objective-collapsed">${escapeHtml(
              objectiveFirstLine.length > 90
                ? `${objectiveFirstLine.slice(0, 87)}…`
                : objectiveFirstLine
            )}</p>
            ${
              attachedCount
                ? `<p class="msn-objective-meta">${escapeHtml(
                    String(attachedCount)
                  )} prospects attached</p>`
                : ''
            }
            <button type="button" class="msn-link-btn" data-msn-objective-expand>Expand</button>
          </div>
          <div data-msn-objective-expanded hidden>
            <p class="msn-objective">${escapeHtml(objectiveRaw)}</p>
            <button type="button" class="msn-link-btn" data-msn-objective-collapse>Collapse</button>
          </div>
        </section>`;

      const inputsHtml = `<section class="msn-block">
          <h3>Inputs</h3>
          ${
            attachedCount
              ? `<p class="msn-artifact-title">Prospect List</p>
            <div class="msn-metric-grid">
              <div class="msn-metric"><span class="msn-metric-label">Companies</span><span class="msn-metric-value">${escapeHtml(
                String(attachedCount)
              )}</span></div>
              ${
                prospectListArt && prospectListArt.metadata && prospectListArt.metadata.operatorSupplied
                  ? `<div class="msn-metric"><span class="msn-metric-label">Source</span><span class="msn-metric-value" style="font-size:0.85rem">Operator</span></div>`
                  : ''
              }
            </div>
            ${
              buckets.length
                ? `<ul class="msn-bucket-list">${buckets
                    .map(
                      ([name, n]) =>
                        `<li><span>${escapeHtml(name)}</span><strong>${escapeHtml(
                          String(n)
                        )}</strong></li>`
                    )
                    .join('')}</ul>`
                : `<p class="msn-objective-meta">Industry breakdown unavailable for these rows.</p>`
            }`
              : `<p class="msn-objective-meta">No prospect list attached yet.</p>`
          }
        </section>`;

      const stageRows = steps
        .map((s) => {
          const pct = stageProgressPct(s);
          const running = String(s.status) === 'running';
          const rs = s.reviewSummary || {};
          const metrics = [];
          if (rs.publishedCount != null) {
            metrics.push(`Accepted ${rs.publishedCount}`);
          }
          if (Array.isArray(s.warnings) && s.warnings.length) {
            metrics.push(`Warnings ${s.warnings.length}`);
          }
          if (s.blockingIssues && s.blockingIssues.length) {
            metrics.push(`Blocked ${s.blockingIssues.length}`);
          }
          return `<li class="msn-stage">
            <div class="msn-stage-head">
              <p class="msn-stage-name">${escapeHtml(s.name || s.capabilityId || 'Stage')}</p>
              <span class="msn-stage-status">${escapeHtml(
                s.outcomeLabel || s.status || 'queued'
              )}</span>
            </div>
            <div class="msn-stage-bar${running ? ' is-running' : ''}" aria-hidden="true"><span style="width:${pct}%"></span></div>
            ${
              metrics.length
                ? `<div class="msn-stage-metrics">${metrics
                    .map((m) => `<span>${escapeHtml(m)}</span>`)
                    .join('')}</div>`
                : ''
            }
          </li>`;
        })
        .join('');

      const artifactRows = artifacts
        .map((art) => {
          const headline = businessArtifactHeadline(art);
          const status = art.validationStatus || 'unknown';
          const rev = art.revision != null ? `v${art.revision}` : '';
          return `<li class="msn-artifact" data-artifact-id="${escapeHtml(
            art.id || ''
          )}">
            <div class="msn-artifact-head">
              <p class="msn-artifact-title">${escapeHtml(headline.title)}</p>
              <span class="cd-chip">${escapeHtml(rev)}</span>
              <span class="cd-chip">${escapeHtml(status)}</span>
              ${
                art.metadata && art.metadata.operatorSupplied
                  ? '<span class="cd-chip">Operator supplied</span>'
                  : ''
              }
            </div>
            <p class="msn-artifact-summary">${escapeHtml(headline.summary)}</p>
            <details class="msn-dev-details">
              <summary>Developer Details</summary>
              <pre class="msn-pre">${escapeHtml(
                JSON.stringify(
                  {
                    id: art.id,
                    artifactType: art.artifactType,
                    producer: art.producer,
                    stageId: art.stageId,
                    validationStatus: art.validationStatus,
                    dependencies: art.dependencies,
                    metadata: art.metadata,
                    payload: art.payload,
                  },
                  null,
                  2
                )
              )}</pre>
            </details>
          </li>`;
        })
        .join('');

      const reviewHtml = `<section class="msn-block msn-review-dash">
          <h3>Campaign Summary</h3>
          <div class="msn-metric-grid">
            <div class="msn-metric"><span class="msn-metric-label">Prospects</span><span class="msn-metric-value">${escapeHtml(
              String(reviewModel.prospects)
            )}</span></div>
            <div class="msn-metric"><span class="msn-metric-label">Personalized</span><span class="msn-metric-value">${escapeHtml(
              String(reviewModel.personalized)
            )}</span></div>
            <div class="msn-metric"><span class="msn-metric-label">Warnings</span><span class="msn-metric-value">${escapeHtml(
              String(reviewModel.warnings)
            )}</span></div>
            <div class="msn-metric"><span class="msn-metric-label">Needs Review</span><span class="msn-metric-value">${escapeHtml(
              String(reviewModel.needsReview)
            )}</span></div>
            <div class="msn-metric"><span class="msn-metric-label">Ready</span><span class="msn-metric-value">${escapeHtml(
              String(reviewModel.ready)
            )}</span></div>
          </div>
        </section>`;

      const recoveryActions = Array.isArray(data.recoveryActions)
        ? data.recoveryActions
        : [];
      const pendingImport =
        data.pendingOperatorImport ||
        (mission.deliverables && mission.deliverables.pendingOperatorImport) ||
        null;
      const recoveryTitle = pendingImport && !isDiscoveryBlockedUi(mission)
        ? 'Prospect list detected'
        : 'Discovery failed';
      const recoveryCopy =
        pendingImport && !isDiscoveryBlockedUi(mission)
          ? pendingImport.errors && pendingImport.errors.length
            ? `A prospect list was detected in the Mission prompt but needs fixes before import: ${pendingImport.errors[0]}`
            : 'A prospect list was detected in the Mission prompt. Import it to skip Discovery and continue at Company Intelligence.'
          : (Array.isArray(mission.blockingIssues) &&
              mission.blockingIssues[0]) ||
            (mission.stageReview &&
              Array.isArray(mission.stageReview.blockingIssues) &&
              mission.stageReview.blockingIssues[0]) ||
            'Discovery could not produce a ProspectList.';
      const showImportOpen =
        Boolean(pendingImport && pendingImport.paste) ||
        recoveryActions.some((a) => a.prefill);
      const discoveryFailedHtml = recoveryActions.length
        ? `<section class="msn-block msn-recovery">
            <h3>${escapeHtml(recoveryTitle)}</h3>
            <p>${escapeHtml(recoveryCopy)}</p>
            <div class="msn-recovery-actions">
              ${recoveryActions
                .map(
                  (a) =>
                    `<button type="button" class="cd-btn ${
                      a.id === 'import_prospect_list'
                        ? 'cd-btn-primary'
                        : 'cd-btn-ghost'
                    }" data-msn-recovery="${escapeHtml(a.id)}">${escapeHtml(
                      a.label || a.id
                    )}</button>`
                )
                .join('')}
            </div>
            <div class="msn-import" id="msnImportPanel"${
              showImportOpen ? '' : ' hidden'
            }>
              <label class="msn-import-label" for="msnImportPaste">Import Prospect List</label>
              <p class="msn-import-hint">Paste CSV or rows with Company Name (required). Website and Address recommended.</p>
              <textarea id="msnImportPaste" class="msn-import-input" rows="8" placeholder="Company Name, Website, Address&#10;Acme Law, https://acme.example, 1 Main St">${
                pendingImport && pendingImport.paste
                  ? escapeHtml(pendingImport.paste)
                  : ''
              }</textarea>
              <div class="msn-import-actions">
                <button type="button" class="cd-btn cd-btn-primary" data-msn-import-submit>Validate &amp; resume</button>
                <button type="button" class="cd-btn cd-btn-ghost" data-msn-import-cancel>Cancel</button>
              </div>
              <p class="msn-import-error" id="msnImportError" hidden></p>
            </div>
          </section>`
        : '';

      els.msnBody.innerHTML = `
        ${discoveryFailedHtml}
        ${reviewHtml}
        ${objectiveHtml}
        ${inputsHtml}
        <section class="msn-block">
          <h3>Progress</h3>
          <ul class="msn-stage-list">${
            stageRows || '<li class="msn-objective-meta">No stages yet</li>'
          }</ul>
        </section>
        <section class="msn-block">
          <h3>Deliverables</h3>
          <ul class="msn-artifacts">${
            artifactRows || '<li class="msn-objective-meta">No deliverables published yet</li>'
          }</ul>
        </section>
        <details class="msn-dev-details msn-block">
          <summary>Developer Details</summary>
          <h3 style="margin-top:0.75rem">Evidence</h3>
          <ul>${evidence || '<li>No evidence yet</li>'}</ul>
          <h3>Audit</h3>
          <ul>${audit || '<li>No events</li>'}</ul>
          <h3>Raw deliverables</h3>
          <pre class="msn-pre">${escapeHtml(
            JSON.stringify(mission.deliverables || {}, null, 2)
          )}</pre>
        </details>
        <p class="msn-note">No outbound actions occur automatically. Approve records review only.</p>
      `;

      els.msnBody
        .querySelectorAll('[data-msn-objective-expand]')
        .forEach((btn) => {
          btn.addEventListener('click', () => {
            const root = els.msnBody.querySelector('#msnObjectiveBlock');
            if (!root) return;
            root.querySelector('[data-msn-objective-collapsed]')?.setAttribute('hidden', '');
            root.querySelector('[data-msn-objective-expanded]')?.removeAttribute('hidden');
          });
        });
      els.msnBody
        .querySelectorAll('[data-msn-objective-collapse]')
        .forEach((btn) => {
          btn.addEventListener('click', () => {
            const root = els.msnBody.querySelector('#msnObjectiveBlock');
            if (!root) return;
            root.querySelector('[data-msn-objective-expanded]')?.setAttribute('hidden', '');
            root.querySelector('[data-msn-objective-collapsed]')?.removeAttribute('hidden');
          });
        });

      const actionLabels = {
        approve: 'Approve',
        reject: 'Reject',
        edit: 'Edit',
        run_again: 'Run again',
      };
      const actions = (data.actions || ['approve', 'reject', 'edit', 'run_again']).filter(
        (action) => action !== 'import_prospect_list'
      );
      els.msnActions.innerHTML = actions
        .map(
          (action) =>
            `<button type="button" class="cd-btn ${
              action === 'approve' ? 'cd-btn-primary' : 'cd-btn-ghost'
            }" data-msn-review="${escapeHtml(action)}">${escapeHtml(
              actionLabels[action] || action.replace(/_/g, ' ')
            )}</button>`
        )
        .join('');

      els.msnActions.querySelectorAll('[data-msn-review]').forEach((btn) => {
        btn.addEventListener('click', async () => {
          const action = btn.getAttribute('data-msn-review');
          try {
            await apiRequest(
              `/api/v1/missions/${encodeURIComponent(missionId)}/review`,
              { method: 'POST', body: { action } }
            );
            announce(`Mission ${action.replace(/_/g, ' ')} recorded.`);
            await openMissionWorkspace(missionId);
            loadDeck({ force: true });
          } catch (err) {
            announce(err.message || 'Review failed');
          }
        });
      });

      bindMissionRecovery(missionId);

      els.missionWorkspace.hidden = false;
      document.body.style.overflow = 'hidden';
      announce('Mission workspace opened.');
    } catch (err) {
      announce(err.message || 'Could not open mission');
    }
  }

  function isDiscoveryBlockedUi(mission) {
    if (!mission || mission.status !== 'waiting') return false;
    const steps = (mission.plan && mission.plan.steps) || [];
    const discovery = steps.find(
      (s) =>
        s.stageId === 'prospect_discovery' ||
        s.capabilityId === 'prospect_discovery'
    );
    if (
      discovery &&
      (discovery.status === 'blocked' || discovery.status === 'failed')
    ) {
      return true;
    }
    const stage = mission.stageReview;
    if (
      stage &&
      (stage.capabilityId === 'prospect_discovery' ||
        stage.capabilityId === 'prospect_discovery') &&
      (stage.outcome === 'blocked' || stage.outcome === 'failed')
    ) {
      return true;
    }
    return Boolean(
      Array.isArray(mission.blockingIssues) && mission.blockingIssues.length
    );
  }

  function bindMissionRecovery(missionId) {
    if (!els.msnBody) return;
    const importPanel = els.msnBody.querySelector('#msnImportPanel');
    const importError = els.msnBody.querySelector('#msnImportError');
    const pasteEl = els.msnBody.querySelector('#msnImportPaste');

    els.msnBody.querySelectorAll('[data-msn-recovery]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        const id = btn.getAttribute('data-msn-recovery');
        if (id === 'import_prospect_list') {
          if (importPanel) importPanel.hidden = false;
          if (pasteEl) pasteEl.focus();
          return;
        }
        if (id === 'retry_discovery') {
          try {
            await apiRequest(
              `/api/v1/missions/${encodeURIComponent(missionId)}/review`,
              { method: 'POST', body: { action: 'run_again' } }
            );
            announce('Retrying Discovery.');
            await openMissionWorkspace(missionId);
            loadDeck({ force: true });
          } catch (err) {
            announce(err.message || 'Retry failed');
          }
          return;
        }
        if (id === 'cancel_mission') {
          try {
            await apiRequest(
              `/api/v1/missions/${encodeURIComponent(missionId)}/review`,
              { method: 'POST', body: { action: 'reject' } }
            );
            announce('Mission cancelled.');
            await openMissionWorkspace(missionId);
            loadDeck({ force: true });
          } catch (err) {
            announce(err.message || 'Cancel failed');
          }
        }
      });
    });

    els.msnBody
      .querySelectorAll('[data-msn-import-cancel]')
      .forEach((btn) => {
        btn.addEventListener('click', () => {
          if (importPanel) importPanel.hidden = true;
          if (importError) {
            importError.hidden = true;
            importError.textContent = '';
          }
        });
      });

    els.msnBody
      .querySelectorAll('[data-msn-import-submit]')
      .forEach((btn) => {
        btn.addEventListener('click', async () => {
          const paste = pasteEl ? pasteEl.value : '';
          if (!String(paste || '').trim()) {
            if (importError) {
              importError.hidden = false;
              importError.textContent = 'Paste at least one company name.';
            }
            return;
          }
          try {
            btn.disabled = true;
            await apiRequest(
              `/api/v1/missions/${encodeURIComponent(missionId)}/artifacts/inject`,
              {
                method: 'POST',
                body: { paste, source: 'spreadsheet_paste' },
              }
            );
            announce('Prospect list imported. Mission resumed.');
            await openMissionWorkspace(missionId);
            loadDeck({ force: true });
          } catch (err) {
            if (importError) {
              importError.hidden = false;
              const details =
                (err.payload &&
                  Array.isArray(err.payload.errors) &&
                  err.payload.errors.join('; ')) ||
                '';
              importError.textContent =
                details ||
                err.message ||
                'Import failed';
            }
            announce(err.message || 'Import failed');
          } finally {
            btn.disabled = false;
          }
        });
      });
  }

  function closeMissionWorkspace() {
    if (!els.missionWorkspace || els.missionWorkspace.hidden) return;
    els.missionWorkspace.hidden = true;
    document.body.style.overflow = '';
    announce('Mission workspace closed.');
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
    const detected = detectProspectListDisplay(text);
    let bodyHtml = '';
    if (detected) {
      const prose = detected.objective
        ? `<p class="mx-msg-body">${escapeHtml(detected.objective)}</p>`
        : '';
      const card = renderAttachmentCard({
        type: 'prospect_list',
        title: 'Prospect List',
        status: 'Detected',
        meta: `${detected.count} ${detected.count === 1 ? 'Company' : 'Companies'}`,
        body: detected.block,
      });
      bodyHtml = `${prose}${card}`;
    } else {
      bodyHtml = `<p class="mx-msg-body">${escapeHtml(text)}</p>`;
    }
    const div = document.createElement('div');
    div.className = 'mx-msg is-operator';
    div.innerHTML = `
      <p class="mx-msg-role">You</p>
      ${bodyHtml}
    `;
    els.mxThread.appendChild(div);
    bindAttachmentCards(div);
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
    const sourceCount = labels.filter(([key]) => Boolean(sources[key])).length;
    const asOf = metadata.asOf ? formatRelativeTime(metadata.asOf) : null;

    return `
      <details class="mx-meta">
        <summary>Generated from · ${sourceCount} context source${sourceCount === 1 ? '' : 's'}</summary>
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
          <div>${evidenceCount} evidence item${evidenceCount === 1 ? '' : 's'}</div>
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
    const thread = els.mxThread;
    const prevScroll = thread ? thread.scrollTop : 0;
    const nearBottom =
      thread &&
      thread.scrollHeight - thread.scrollTop - thread.clientHeight < 48;
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
    if (thread) {
      if (nearBottom) thread.scrollTop = thread.scrollHeight;
      else thread.scrollTop = prevScroll;
    }
  }

  async function askWorkspace(question) {
    const q = String(question || '').trim();
    if (!q) return;
    console.info('[mission-objective-len]', {
      stage: 'frontend',
      chars: q.length,
      newlines: (q.match(/\n/g) || []).length,
    });
    if (els.mxAskSend) els.mxAskSend.disabled = true;
    appendOperatorMessage(q);
    resetAskInput();

    try {
      const result = await apiRequest('/api/v1/max/workspace/ask', {
        method: 'POST',
        body: {
          sessionId: workspaceSessionId,
          question: q,
          context: workspaceContext,
        },
      });
      console.info('[mission-objective-len]', {
        stage: 'frontend_request_payload',
        chars: q.length,
        payloadQuestionChars: String(q).length,
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
      if (
        result.mission &&
        result.mission.operatorProspectList &&
        result.mission.operatorProspectList.injected
      ) {
        const count = result.mission.operatorProspectList.prospectCount || 0;
        const note = document.createElement('div');
        note.className = 'mx-msg';
        note.innerHTML = `
          <p class="mx-msg-role">Mission</p>
          ${renderAttachmentCard({
            type: 'prospect_list',
            title: 'Prospect List',
            status: 'Imported',
            meta: `${count} ${count === 1 ? 'Company' : 'Companies'}`,
            body:
              (result.mission.operatorProspectList.paste) ||
              'Prospect list imported onto the Mission Artifact Bus.',
          })}
        `;
        els.mxThread?.appendChild(note);
        bindAttachmentCards(note);
        if (els.mxThread) els.mxThread.scrollTop = els.mxThread.scrollHeight;
      }
      renderSuggestions(result.suggestions || []);
      if (result.route === 'mission' || (result.mission && result.mission.id)) {
        loadDeck({ force: true });
      }
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
      els.operations,
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
    renderOperations(model);
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

  // Enter sends; Shift+Enter keeps a newline so multi-row ProspectList pastes survive.
  els.mxAskInput?.addEventListener('keydown', (event) => {
    if (event.key !== 'Enter' || event.shiftKey || event.isComposing) return;
    event.preventDefault();
    els.mxAskForm?.requestSubmit();
  });
  els.mxAskInput?.addEventListener('input', () => autoGrowAskInput());
  autoGrowAskInput();

  els.workspace?.querySelectorAll('[data-mx-close]').forEach((node) => {
    node.addEventListener('click', () => closeWorkspace());
  });

  els.missionWorkspace?.querySelectorAll('[data-msn-close]').forEach((node) => {
    node.addEventListener('click', () => closeMissionWorkspace());
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      if (els.missionWorkspace && !els.missionWorkspace.hidden) {
        closeMissionWorkspace();
        return;
      }
      if (els.workspace && !els.workspace.hidden) {
        closeWorkspace();
      }
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
