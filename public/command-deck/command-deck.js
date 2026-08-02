'use strict';

/**
 * Command Deck UI — SPEC-008 + SPEC-011 Live Intelligence + SPEC-012 Operator Intelligence
 * + SPEC-045 operator workspace polish + SPEC-047 evidence-first Review interaction.
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
  /** SPEC-047 session-local review interaction state (presentation only). */
  /** @type {object|null} */
  let msnReviewSession = null;

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

  /** Auto-grow Max composer (~1 line → ~220px, then internal scroll). */
  const MX_ASK_MIN_PX = 42;
  const MX_ASK_MAX_PX = 220;
  const CD_ASK_MIN_PX = 40;
  const CD_ASK_MAX_PX = 220;

  function measureAskMaxHeight(el, fallbackMax) {
    const raw = window.getComputedStyle(el).maxHeight;
    const n = Number.parseFloat(raw);
    return Number.isFinite(n) && n > 0 ? Math.min(fallbackMax, n) : fallbackMax;
  }

  function syncCdAskBarHeight() {
    const bar = document.querySelector('.cd-ask-bar');
    if (!bar) return;
    const h = Math.ceil(bar.getBoundingClientRect().height);
    if (h > 0) {
      document.documentElement.style.setProperty('--cd-ask-height', `${h}px`);
    }
  }

  function autoGrowAskInput() {
    const el = els.mxAskInput;
    if (!el) return;
    el.style.height = '0px';
    const measured = el.scrollHeight;
    const max = measureAskMaxHeight(el, MX_ASK_MAX_PX);
    const next = Math.min(Math.max(measured, MX_ASK_MIN_PX), max);
    el.style.height = `${next}px`;
    el.style.overflowY = measured > max ? 'auto' : 'hidden';
  }

  function resetAskInput() {
    if (!els.mxAskInput) return;
    els.mxAskInput.value = '';
    els.mxAskInput.style.height = '';
    els.mxAskInput.style.overflowY = '';
    autoGrowAskInput();
  }

  /** Page-level Command Deck Ask Max composer (bottom bar). */
  function autoGrowCdAskInput() {
    const el = els.askInput;
    if (!el) return;
    el.style.height = '0px';
    const measured = el.scrollHeight;
    const max = measureAskMaxHeight(el, CD_ASK_MAX_PX);
    const next = Math.min(Math.max(measured, CD_ASK_MIN_PX), max);
    el.style.height = `${next}px`;
    el.style.overflowY = measured > max ? 'auto' : 'hidden';
    syncCdAskBarHeight();
  }

  function resetCdAskInput() {
    if (!els.askInput) return;
    els.askInput.value = '';
    els.askInput.style.height = '';
    els.askInput.style.overflowY = '';
    autoGrowCdAskInput();
  }

  /**
   * Long operator prompts: several lines by default, Expand/Collapse for full text.
   * @param {string} text
   * @returns {string}
   */
  function renderExpandableOperatorBody(text) {
    const full = String(text == null ? '' : text);
    if (!full) return `<p class="mx-msg-body"></p>`;
    const lines = full.split(/\r?\n/);
    const needsExpand = lines.length > 6 || full.length > 480;
    if (!needsExpand) {
      return `<p class="mx-msg-body">${escapeHtml(full)}</p>`;
    }
    let preview;
    if (lines.length > 6) {
      preview = `${lines.slice(0, 6).join('\n')}\n…`;
    } else {
      preview = `${full.slice(0, 477)}…`;
    }
    return `<div class="mx-expandable" data-mx-expandable>
      <p class="mx-msg-body" data-mx-expand-preview>${escapeHtml(preview)}</p>
      <p class="mx-msg-body" data-mx-expand-full hidden>${escapeHtml(full)}</p>
      <button type="button" class="mx-expand-btn" data-mx-expand-toggle aria-expanded="false">Expand</button>
    </div>`;
  }

  /**
   * @param {ParentNode | null | undefined} root
   */
  function bindExpandableOperatorBodies(root) {
    if (!root) return;
    root.querySelectorAll('[data-mx-expand-toggle]').forEach((btn) => {
      if (btn.dataset.boundExpand === '1') return;
      btn.dataset.boundExpand = '1';
      btn.addEventListener('click', () => {
        const wrap = btn.closest('[data-mx-expandable]');
        if (!wrap) return;
        const open = !wrap.classList.contains('is-expanded');
        wrap.classList.toggle('is-expanded', open);
        const preview = wrap.querySelector('[data-mx-expand-preview]');
        const fullEl = wrap.querySelector('[data-mx-expand-full]');
        if (open) {
          preview?.setAttribute('hidden', '');
          fullEl?.removeAttribute('hidden');
        } else {
          fullEl?.setAttribute('hidden', '');
          preview?.removeAttribute('hidden');
        }
        btn.setAttribute('aria-expanded', open ? 'true' : 'false');
        btn.textContent = open ? 'Collapse' : 'Expand';
      });
    });
  }

  /**
   * Mission Workspace Operator Request — summary by default, full raw on Expand.
   * @param {string} text
   * @returns {string}
   */
  function renderOperatorRequestDd(text) {
    const raw = String(text == null ? '' : text).trim();
    if (!raw) return `<dd>—</dd>`;
    const firstLine =
      raw.split(/\r?\n/).find((line) => String(line).trim()) || raw;
    const summary =
      firstLine.length > 120 ? `${firstLine.slice(0, 117)}…` : firstLine;
    const needsExpand = raw.includes('\n') || raw.length > 120 || raw !== summary;
    if (!needsExpand) {
      return `<dd class="msn-operator-request">${escapeHtml(raw)}</dd>`;
    }
    return `<dd class="msn-operator-request" data-msn-opreq>
      <div data-msn-opreq-collapsed>
        <p class="msn-objective-collapsed">${escapeHtml(summary)}</p>
        <button type="button" class="msn-link-btn msn-interactive" data-msn-opreq-expand>Expand</button>
      </div>
      <div data-msn-opreq-expanded hidden>
        <p class="msn-objective">${escapeHtml(raw)}</p>
        <button type="button" class="msn-link-btn msn-interactive" data-msn-opreq-collapse>Collapse</button>
      </div>
    </dd>`;
  }

  /**
   * SPEC-045 — detect a ProspectList-shaped block for display cards only.
   * Fillable verification table field mutations must not show as Prospect List Detected.
   * @param {string} text
   */
  function detectProspectListDisplay(text) {
    const raw = String(text || '').replace(/^\uFEFF/, '');
    if (!raw.trim()) return null;

    // Suppress false positives from fillable verification table pastes,
    // field mutations, and readiness reassessment.
    if (looksLikeFillableTableMutationDisplay(raw)) return null;
    if (looksLikeFillableVerificationTablePasteDisplay(raw)) return null;

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
      if (isFillableTableMutationDisplayLine(line)) continue;
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
          if (isFillableTableMutationDisplayLine(line)) continue;
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
    const blockLines = lines
      .slice(start, end + 1)
      .filter((l) => String(l).trim())
      .filter((l) => !isFillableTableMutationDisplayLine(l));
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

  const FILLABLE_TABLE_DISPLAY_FIELDS = [
    'prospect_id',
    'company_name',
    'contact_name',
    'contact_role_status',
    'website_status',
    'website_value',
    'mailing_address_status',
    'mailing_address_value',
    'phone_status',
    'phone_value',
    'source_to_check_first',
    'verification_status',
    'mail_readiness',
    'draft_readiness',
    'execution_readiness',
    'operator_next_action',
    'notes',
  ];

  function isFillableTableMutationDisplayLine(line) {
    const text = String(line || '').trim();
    if (!text) return false;
    if (
      FILLABLE_TABLE_DISPLAY_FIELDS.some((field) =>
        new RegExp(`^(?:[-*•]\\s*)?${field}\\s*[=:]\\s*\\S`, 'i').test(text)
      )
    ) {
      return true;
    }
    if (/^for\s+[A-Za-z0-9_-]+\s+only\b/i.test(text)) return true;
    if (/^set\s*:?\s*$/i.test(text)) return true;
    if (/^leave\b[\s\S]{0,80}\bunchanged\b/i.test(text)) return true;
    if (/^update\s+(?:the\s+)?(?:fillable\s+)?(?:verification\s+)?table\b/i.test(text)) {
      return true;
    }
    if (/^edit\s+(?:the\s+)?(?:fillable\s+)?(?:verification\s+)?table\b/i.test(text)) {
      return true;
    }
    if (/^reassess\b/i.test(text) && /\breadiness\b/i.test(text)) return true;
    if (
      /^reassess\b/i.test(text) &&
      /\b(?:canary\s+table|(?:fillable\s+)?(?:verification\s+)?table)\b/i.test(text)
    ) {
      return true;
    }
    if (/^return\s+only\b/i.test(text)) return true;
    if (/^keep\s+this\s+preparation/i.test(text)) return true;
    if (/^do\s+not\b/i.test(text)) return true;
    return false;
  }

  function looksLikeFillableTableMutationDisplay(text) {
    const raw = String(text || '');
    if (!raw.trim()) return false;
    if (looksLikeFillableVerificationTablePasteDisplay(raw)) return true;
    const lower = raw.toLowerCase();
    const updateCue =
      /\bupdate\s+(?:the\s+)?(?:fillable\s+)?(?:verification\s+)?table\b/.test(
        lower
      ) ||
      /\bedit\s+(?:the\s+)?(?:fillable\s+)?(?:verification\s+)?table\b/.test(
        lower
      );
    const fieldAssignment = FILLABLE_TABLE_DISPLAY_FIELDS.some((field) =>
      new RegExp(`\\b${field}\\s*[=:]\\s*\\S`, 'i').test(raw)
    );
    const forOnlySet = /\bfor\s+[A-Za-z0-9_-]+\s+only\b/i.test(raw);
    const reassess =
      /\breassess\b[\s\S]{0,120}\breadiness\b/i.test(raw) ||
      /\busing\s+(?:the\s+)?table\s+gates\b/i.test(raw) ||
      /\breassess\b[\s\S]{0,160}\b(?:the\s+)?(?:campaign\s+\d+\s+)?(?:preparation[-\s]*only\s+)?canary\s+table\b/i.test(
        raw
      ) ||
      /\breassess\b[\s\S]{0,120}\b(?:the\s+)?(?:fillable\s+)?verification\s+table\b/i.test(
        raw
      ) ||
      /\breassess\b[\s\S]{0,80}\b(?:the\s+)?fillable\s+table\b/i.test(raw) ||
      (/\breassess\b[\s\S]{0,60}\bthe\s+table\b/i.test(raw) &&
        !/\breadiness\b/i.test(raw) &&
        !/\busing\s+(?:the\s+)?table\s+gates\b/i.test(raw));
    if (updateCue && (fieldAssignment || forOnlySet || reassess)) return true;
    if (fieldAssignment && forOnlySet) return true;
    if (reassess && (forOnlySet || fieldAssignment || updateCue)) return true;
    if (reassess) return true;
    return false;
  }

  function looksLikeFillableVerificationTablePasteDisplay(text) {
    const lines = String(text || '').split(/\r?\n/);
    for (const line of lines) {
      const trimmed = String(line || '').trim();
      if (!trimmed.includes('|')) continue;
      let body = trimmed;
      if (body.startsWith('|')) body = body.slice(1);
      if (body.endsWith('|')) body = body.slice(0, -1);
      const keys = body
        .split('|')
        .map((c) =>
          String(c || '')
            .trim()
            .toLowerCase()
            .replace(/[\s-]+/g, '_')
        )
        .filter(Boolean);
      if (!keys.length) continue;
      const set = new Set(keys);
      if (!set.has('prospect_id') || !set.has('company_name')) continue;
      const hits = FILLABLE_TABLE_DISPLAY_FIELDS.filter((f) => set.has(f)).length;
      if (hits >= 5) return true;
    }
    return false;
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
        title: 'Company Enrichment',
        summary: `${n} packages`,
      };
    }
    if (type === 'BusinessIntelligenceProfile') {
      const n =
        p.profileCount != null
          ? Number(p.profileCount)
          : Array.isArray(p.profiles)
            ? p.profiles.length
            : 0;
      return {
        title: 'Business Intelligence',
        summary: `${n} ${n === 1 ? 'profile' : 'profiles'}`,
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

  function confidenceLabel(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 'Unknown';
    if (n >= 0.8 || n >= 80) return 'High';
    if (n >= 0.65 || n >= 65) return 'Medium';
    if (n > 0) return 'Low';
    return 'Unknown';
  }

  function warningText(w) {
    if (w == null) return '';
    if (typeof w === 'string') return w;
    if (typeof w === 'object') {
      return String(w.message || w.summary || w.reason || w.text || JSON.stringify(w));
    }
    return String(w);
  }

  function companyFromWarning(text) {
    const t = String(text || '');
    const m =
      t.match(/^([^:—\-]+?)\s*[:—\-]\s*/) ||
      t.match(/\bfor\s+([A-Z][^.,;]{2,60})/) ||
      t.match(/^([A-Z][A-Za-z0-9&.'\-\s]{2,60})\s+(?:has|is|missing|needs)/i);
    return m ? String(m[1]).trim() : '';
  }

  function normalizeMailPackage(raw, index) {
    const pkg = raw || {};
    const letter = pkg.letter || {};
    const envelope = pkg.envelope || {};
    const summary = pkg.personalizationSummary || {};
    const company =
      letter.companyName ||
      envelope.companyName ||
      pkg.companyName ||
      `Package ${index + 1}`;
    const recipient =
      letter.recipientName ||
      envelope.recipientName ||
      pkg.recipientName ||
      '';
    const confidence =
      pkg.confidence != null
        ? Number(pkg.confidence)
        : summary.letterConfidence != null
          ? Number(summary.letterConfidence)
          : null;
    const personalization =
      summary.whySelected ||
      (Array.isArray(summary.personalizationFacts) &&
        summary.personalizationFacts[0]) ||
      pkg.personalizationSentence ||
      letter.personalizedOpening ||
      pkg.openingHook ||
      '';
    const letterBody =
      letter.body ||
      pkg.letterBody ||
      pkg.letterPreview ||
      [letter.personalizedOpening, letter.valueProposition, letter.cta, letter.signature]
        .filter(Boolean)
        .join('\n\n') ||
      personalization ||
      '';
    const warnings = [
      ...(Array.isArray(pkg.warnings) ? pkg.warnings : []),
      ...(Array.isArray(summary.missingDataWarnings)
        ? summary.missingDataWarnings
        : []),
    ].map(warningText).filter(Boolean);
    const needsReview =
      Boolean(pkg.needsReview) ||
      pkg.status === 'needs_review' ||
      pkg.approved !== true ||
      warnings.length > 0 ||
      !letterBody;
    const ready =
      Boolean(pkg.approved) ||
      pkg.status === 'ready_to_print' ||
      pkg.status === 'ready' ||
      (!needsReview && Boolean(letterBody));
    const id =
      pkg.id ||
      pkg.prospectId ||
      `pkg-${index}-${String(company).toLowerCase().replace(/\s+/g, '-')}`;
    const sales = pkg.salesIntelligence || pkg.salesIntelligenceProfile || null;
    const bi =
      pkg.businessIntelligence ||
      pkg.businessIntelligenceProfile ||
      (sales && sales.businessIntelligence) ||
      null;
    const messaging =
      pkg.messagingStrategy ||
      (sales && sales.messaging_strategy) ||
      null;
    const operatorConfidence =
      pkg.operatorConfidence ||
      (sales && sales.operatorConfidence) ||
      null;
    return {
      id: String(id),
      companyName: String(company),
      recipientName: String(recipient),
      confidence,
      confidenceLabel: confidenceLabel(confidence),
      personalization: String(personalization),
      letterBody: String(letterBody),
      envelopeAddress: String(envelope.mailingAddress || pkg.mailingAddress || ''),
      warnings,
      needsReview,
      ready,
      approved: Boolean(pkg.approved),
      businessIntelligence: bi,
      salesIntelligence: sales,
      messagingStrategy: messaging,
      operatorConfidence,
      source: pkg,
    };
  }

  function mailPackagesFromWorkspace(mission, artifacts) {
    const list = [];
    const seen = new Set();
    const pushPkg = (raw, i) => {
      const n = normalizeMailPackage(raw, i);
      const key = n.id || n.companyName;
      if (seen.has(key)) return;
      seen.add(key);
      list.push(n);
    };

    (artifacts || []).forEach((art) => {
      const type = art.artifactType || art.type;
      const payload = art.payload || {};
      if (type === 'MailPackage') {
        if (Array.isArray(payload.packages)) {
          payload.packages.forEach((p, i) => pushPkg(p, list.length + i));
        } else if (payload.letter || payload.companyName || payload.id) {
          pushPkg(payload, list.length);
        } else if (Array.isArray(payload)) {
          payload.forEach((p, i) => pushPkg(p, list.length + i));
        }
      }
    });

    if (list.length) return list;

    const campaignArt = (artifacts || []).find(
      (a) => a.artifactType === 'Campaign' || a.type === 'Campaign'
    );
    const campaign =
      (campaignArt && campaignArt.payload && campaignArt.payload.campaign) ||
      (mission.deliverables && mission.deliverables.campaign) ||
      null;
    const mailMerge =
      (campaign && Array.isArray(campaign.mailMerge) && campaign.mailMerge) ||
      [];
    mailMerge.forEach((row, i) => {
      pushPkg(
        {
          id: row.prospectId || `mail-merge-${i}`,
          companyName: row.companyName,
          recipientName: row.recipientName || row.contactName || '',
          personalizationSentence: row.personalizationSentence,
          openingHook: row.openingHook,
          letter: {
            companyName: row.companyName,
            recipientName: row.recipientName || row.contactName || '',
            personalizedOpening: row.personalizationSentence || row.openingHook || '',
            body: [row.personalizationSentence, row.openingHook, row.recommendedOffer]
              .filter(Boolean)
              .join('\n\n'),
          },
          confidence: row.confidence,
          warnings: row.warnings || [],
          businessIntelligence: row.businessIntelligence || null,
          salesIntelligence: row.salesIntelligence || null,
          messagingStrategy:
            (row.salesIntelligence && row.salesIntelligence.messaging_strategy) ||
            null,
          operatorConfidence:
            row.operatorConfidence != null
              ? typeof row.operatorConfidence === 'object'
                ? row.operatorConfidence
                : { overall: row.operatorConfidence }
              : (row.salesIntelligence &&
                  row.salesIntelligence.operatorConfidence) ||
                null,
        },
        i
      );
    });

    if (list.length) return list;

    const prospects =
      (campaign && Array.isArray(campaign.prospects) && campaign.prospects) ||
      (mission.deliverables &&
        Array.isArray(mission.deliverables.prospects) &&
        mission.deliverables.prospects) ||
      [];
    prospects.forEach((p, i) => {
      pushPkg(
        {
          id: p.id || `prospect-${i}`,
          companyName: p.companyName || p.name,
          recipientName: p.contactName || p.name || '',
          personalizationSentence: p.personalizationSentence,
          letter: {
            companyName: p.companyName || p.name,
            recipientName: p.contactName || '',
            body: p.personalizationSentence || '',
          },
          confidence: p.icp_score != null ? Number(p.icp_score) / 100 : null,
          warnings: p.warnings || [],
        },
        i
      );
    });

    return list;
  }

  function collectReviewWarnings(mission, packages, evidence) {
    /** @type {Array<{id:string,company:string,message:string,packageId:string|null}>} */
    const out = [];
    const push = (message, company, packageId) => {
      const msg = warningText(message);
      if (!msg) return;
      const co = company || companyFromWarning(msg) || '';
      out.push({
        id: `warn-${out.length}`,
        company: co,
        message: msg,
        packageId: packageId || null,
      });
    };

    (mission.plan && mission.plan.steps ? mission.plan.steps : []).forEach((s) => {
      (s.warnings || []).forEach((w) => push(w, '', null));
      (s.blockingIssues || []).forEach((w) => push(w, '', null));
    });
    if (mission.stageReview) {
      (mission.stageReview.warnings || []).forEach((w) => push(w, '', null));
      (mission.stageReview.blockingIssues || []).forEach((w) => push(w, '', null));
    }
    (packages || []).forEach((pkg) => {
      (pkg.warnings || []).forEach((w) => push(w, pkg.companyName, pkg.id));
      if (!pkg.recipientName) {
        push('Missing contact name', pkg.companyName, pkg.id);
      }
      if (!pkg.letterBody) {
        push('Letter preview unavailable', pkg.companyName, pkg.id);
      }
    });
    (evidence || []).forEach((e) => {
      if (e && e.kind === 'warning') push(e.summary || e, '', null);
    });

    const dedup = [];
    const seen = new Set();
    out.forEach((w) => {
      const key = `${w.company}|${w.message}`;
      if (seen.has(key)) return;
      seen.add(key);
      if (!w.packageId && w.company) {
        const match = (packages || []).find(
          (p) =>
            String(p.companyName).toLowerCase() === String(w.company).toLowerCase()
        );
        if (match) w.packageId = match.id;
      }
      dedup.push(w);
    });
    return dedup;
  }

  function reviewDashboardModel(mission, artifacts, packages, warnings) {
    const pkgs = packages || mailPackagesFromWorkspace(mission, artifacts);
    const warns = warnings || collectReviewWarnings(mission, pkgs, []);
    const campaignArt = (artifacts || []).find(
      (a) => a.artifactType === 'Campaign' || a.type === 'Campaign'
    );
    const campaign =
      (campaignArt && campaignArt.payload && campaignArt.payload.campaign) ||
      (mission.deliverables && mission.deliverables.campaign) ||
      null;
    const prospectListArt = (artifacts || []).find(
      (a) => a.artifactType === 'ProspectList' || a.type === 'ProspectList'
    );
    const prospectCount =
      (prospectListArt &&
        prospectListArt.payload &&
        (prospectListArt.payload.prospectCount != null
          ? Number(prospectListArt.payload.prospectCount)
          : Array.isArray(prospectListArt.payload.prospects)
            ? prospectListArt.payload.prospects.length
            : 0)) ||
      (mission.operatorProspectList && mission.operatorProspectList.prospectCount) ||
      (campaign && campaign.prospectCount) ||
      (campaign && Array.isArray(campaign.prospects) && campaign.prospects.length) ||
      pkgs.length ||
      0;
    const personalized = pkgs.length || 0;
    const needsReview = pkgs.filter((p) => p.needsReview && !p.approved).length;
    const ready = pkgs.filter((p) => p.ready || p.approved).length;
    return {
      prospects: Number(prospectCount) || 0,
      personalized: Number(personalized) || 0,
      warnings: warns.length,
      needsReview,
      ready,
      packages: pkgs,
    };
  }

  /**
   * SPEC-055 — Operator Request → Understood Intent → Execution Plan.
   * Surfaces how Max interpreted the request before execution.
   */
  function renderMissionIntentHtml(missionIntent, summary) {
    const intent = missionIntent || null;
    const sum = summary || null;
    if (!intent && !sum) return '';

    const operatorRequest =
      (sum && sum.operatorRequest) ||
      (intent && intent.sourceText) ||
      (intent && intent.goal) ||
      '';
    const understood =
      (sum && sum.understoodIntent) ||
      (intent && intent.label) ||
      (intent && intent.matchedIntent) ||
      '—';
    const confidencePct =
      (sum && sum.confidencePercent != null
        ? sum.confidencePercent
        : intent && intent.confidence != null
          ? Math.round(Number(intent.confidence) * 100)
          : null);
    const goal = (sum && sum.goal) || (intent && intent.goal) || '';
    const alts =
      (sum && sum.alternateIntents) ||
      (intent && intent.alternateIntents) ||
      [];
    const needsClarification =
      (sum && sum.needsClarification) ||
      (intent && intent.needsClarification) ||
      false;
    const clarificationPrompt =
      (sum && sum.clarificationPrompt) ||
      (intent && intent.clarificationPrompt) ||
      null;
    const campaign =
      (sum && sum.target && sum.target.campaign) ||
      (intent && intent.target && intent.target.campaign) ||
      null;

    const altHtml =
      Array.isArray(alts) && alts.length
        ? `<div class="msn-si-row"><dt>Alternatives</dt><dd>${escapeHtml(
            alts
              .map((a) => {
                const label = a.label || a.intent || '';
                const c =
                  a.confidence != null
                    ? ` (${Math.round(Number(a.confidence) * 100)}%)`
                    : '';
                return `${label}${c}`;
              })
              .join(' · ')
          )}</dd></div>`
        : '';

    return `<section class="msn-block msn-mission-intent" id="msnMissionIntent">
      <h3>Understood Intent</h3>
      <p class="msn-objective-meta">How Max interpreted the request before selecting capabilities.</p>
      <dl class="msn-si-dl">
        <div class="msn-si-row"><dt>Operator Request</dt>${renderOperatorRequestDd(
          operatorRequest
        )}</div>
        <div class="msn-si-row"><dt>Understood Intent</dt><dd><strong>${escapeHtml(
          understood
        )}</strong>${
          confidencePct != null
            ? ` <span class="msn-objective-meta">(Confidence: ${escapeHtml(
                String(confidencePct)
              )}%)</span>`
            : ''
        }</dd></div>
        ${
          goal && goal !== understood
            ? `<div class="msn-si-row"><dt>Goal</dt><dd>${escapeHtml(
                goal
              )}</dd></div>`
            : ''
        }
        ${
          campaign
            ? `<div class="msn-si-row"><dt>Target</dt><dd>Campaign ${escapeHtml(
                String(campaign)
              )}</dd></div>`
            : ''
        }
        ${altHtml}
      </dl>
      ${
        needsClarification
          ? `<p class="msn-objective-meta" role="status">${escapeHtml(
              clarificationPrompt ||
                'Ambiguous request — choose a suggested interpretation.'
            )}</p>`
          : ''
      }
    </section>`;
  }

  /**
   * SPEC-056 — Evidence Requirements: available / scheduled / blocked.
   */
  function renderEvidenceRequirementsHtml(evidencePlan, summary) {
    const plan = evidencePlan || null;
    const sum = summary || null;
    if (!plan && !sum) return '';

    const items =
      (sum && Array.isArray(sum.items) && sum.items.length
        ? sum.items
        : null) ||
      buildEvidenceItemsFromPlan(plan);
    if (!items.length && !(plan && plan.required && plan.required.length)) {
      return '';
    }

    const unable = (sum && sum.unableToAnswer) || (plan && plan.unableToAnswer);
    const reason = (sum && sum.reason) || (plan && plan.reason) || null;
    const satisfied =
      (sum && sum.satisfiedCount != null
        ? sum.satisfiedCount
        : plan && plan.satisfiedCount) || 0;
    const missing =
      (sum && sum.missingCount != null
        ? sum.missingCount
        : plan && plan.missingCount) || 0;

    const rows = items
      .map((item) => {
        const status = String(item.status || 'required').toLowerCase();
        let mark = '·';
        let statusLabel = status;
        if (status === 'available') {
          mark = '✓';
          statusLabel = 'Available';
        } else if (status === 'acquired') {
          mark = '✓';
          statusLabel = 'Acquired';
        } else if (status === 'scheduled') {
          mark = '→';
          statusLabel = 'Scheduled';
        } else if (status === 'blocked') {
          mark = '✗';
          statusLabel = 'Blocked';
        }
        const detail = item.reason
          ? ` — ${escapeHtml(String(item.reason))}`
          : '';
        return `<div class="msn-si-row"><dt>${escapeHtml(
          mark
        )} ${escapeHtml(item.label || item.evidenceType || '')}</dt><dd>${escapeHtml(
          statusLabel
        )}${detail}</dd></div>`;
      })
      .join('');

    return `<section class="msn-block msn-evidence-requirements" id="msnEvidenceRequirements">
      <h3>Evidence Requirements</h3>
      <p class="msn-objective-meta">Information required to answer the operator before capabilities run (${escapeHtml(
        String(satisfied)
      )} satisfied · ${escapeHtml(String(missing))} missing).</p>
      <dl class="msn-si-dl">${rows}</dl>
      ${
        unable
          ? `<p class="msn-objective-meta" role="alert">${escapeHtml(
              reason || 'Unable to answer — missing evidence with no registered producer.'
            )}</p>`
          : ''
      }
    </section>`;
  }

  function buildEvidenceItemsFromPlan(plan) {
    if (!plan || !Array.isArray(plan.required)) return [];
    const available = new Set(plan.available || []);
    const scheduled = new Set(
      (plan.acquisitions || []).map((a) => a.evidenceType)
    );
    const blockedMap = new Map(
      (plan.blocked || []).map((b) => [b.evidenceType, b.reason])
    );
    return plan.required.map((t) => {
      if (blockedMap.has(t)) {
        return {
          evidenceType: t,
          label: t,
          status: 'blocked',
          reason: blockedMap.get(t) || 'No producer registered',
        };
      }
      if (available.has(t)) {
        return { evidenceType: t, label: t, status: 'available', reason: null };
      }
      if (scheduled.has(t)) {
        return { evidenceType: t, label: t, status: 'scheduled', reason: null };
      }
      return { evidenceType: t, label: t, status: 'required', reason: null };
    });
  }

  /**
   * SPEC-050 — show parsed Mission Plan before / alongside execution artifacts.
   * Operators verify intent was interpreted correctly; Notes never look like stages.
   * SPEC-055 — Execution Plan follows Understood Intent.
   * SPEC-056 — Evidence Requirements sit between Intent and Execution Plan.
   */
  function renderMissionPlanHtml(missionPlan, summary) {
    const plan = missionPlan || null;
    const sum = summary || null;
    if (!plan && !sum) return '';

    const objective =
      (sum && sum.objective) || (plan && plan.objective) || '';
    const subject = (sum && sum.subject) || (plan && plan.subject) || null;
    const execution = (sum && sum.execution) || [];
    const parameters =
      (sum && sum.parameters) || (plan && plan.parameters) || {};
    const notes =
      (sum && sum.notes) ||
      (plan && Array.isArray(plan.notes) ? plan.notes : []) ||
      [];
    const reviewEnabled =
      (sum && sum.reviewEnabled) ||
      (plan && plan.options && plan.options.review) ||
      false;

    const paramRows = Object.entries(parameters)
      .filter(([, v]) => v != null && String(v).trim())
      .map(
        ([k, v]) =>
          `<div class="msn-si-row"><dt>${escapeHtml(
            formatMissionPlanLabel(k)
          )}</dt><dd>${escapeHtml(String(v))}</dd></div>`
      )
      .join('');

    const execLabel = Array.isArray(execution)
      ? execution.filter(Boolean).join(' → ')
      : String(execution || '');

    return `<section class="msn-block msn-mission-plan" id="msnMissionPlan">
      <h3>Execution Plan</h3>
      <p class="msn-objective-meta">Deterministic capabilities selected from Understood Intent.</p>
      <dl class="msn-si-dl">
        <div class="msn-si-row"><dt>Objective</dt><dd>${escapeHtml(
          objective || '—'
        )}</dd></div>
        ${
          subject
            ? `<div class="msn-si-row"><dt>Subject</dt><dd>${escapeHtml(
                subject
              )}</dd></div>`
            : ''
        }
        <div class="msn-si-row"><dt>Execution</dt><dd>${escapeHtml(
          execLabel || '—'
        )}</dd></div>
        ${paramRows}
        <div class="msn-si-row"><dt>Review</dt><dd>${escapeHtml(
          reviewEnabled ? 'Enabled' : 'Off'
        )}</dd></div>
      </dl>
      ${
        notes.length
          ? `<h4 class="msn-subhead">Notes</h4>
        <ul class="msn-bucket-list">${notes
          .map((n) => `<li><span>${escapeHtml(String(n))}</span></li>`)
          .join('')}</ul>
        <p class="msn-objective-meta">Notes are operator guidance only — they never become executable stages.</p>`
          : ''
      }
    </section>`;
  }

  /** SPEC-051 — resolved artifacts + acquisition decisions before execute */
  function renderArtifactResolutionHtml(resolution) {
    const res = resolution || null;
    if (!res) return '';
    const resolved = Array.isArray(res.resolved) ? res.resolved : [];
    const acquisitions = Array.isArray(res.acquisitions)
      ? res.acquisitions.filter((a) => a && a.strategy !== 'use_existing')
      : [];
    const skipped = res.skippedStages || {};
    const missing = Array.isArray(res.missingWithOptions)
      ? res.missingWithOptions
      : (res.missing || []).map((t) => ({ artifactType: t, options: [] }));

    if (
      !resolved.length &&
      !acquisitions.length &&
      !Object.keys(skipped).length &&
      !missing.length
    ) {
      return '';
    }

    const resolvedRows = resolved
      .map((r) => {
        const decision = skipped.prospect_discovery
          ? 'Discovery skipped'
          : 'Use existing';
        return `<div class="msn-si-row"><dt>${escapeHtml(
          r.type || 'Artifact'
        )}</dt><dd>${escapeHtml(r.sourceLabel || r.source || '—')} · ${escapeHtml(
          r.confidence || 'High'
        )} · ${escapeHtml(r.freshness || '—')}${
          r.pending ? ' · pending supply' : ''
        }<br/><span class="msn-objective-meta">${escapeHtml(
          decision
        )}: compatible artifact already exists.</span></dd></div>`;
      })
      .join('');

    const skipRows = Object.entries(skipped)
      .map(
        ([stageId, reason]) =>
          `<li><span>${escapeHtml(formatStageLabel(stageId))}: ${escapeHtml(
            String(reason)
          )}</span></li>`
      )
      .join('');

    const acquireRows = acquisitions
      .map((a) => {
        if (a.strategy === 'unavailable') {
          return `<div class="msn-si-row"><dt>${escapeHtml(
            a.artifactType || 'Artifact'
          )}</dt><dd><strong>Blocked</strong> — no registered producer<br/><span class="msn-objective-meta">Expected: ${escapeHtml(
            a.expectedProducer || '—'
          )} · ${escapeHtml(
            a.recommendedAction ||
              `Register a capability that produces ${a.artifactType}`
          )}</span></dd></div>`;
        }
        const opts =
          (missing.find((m) => m.artifactType === a.artifactType) || {})
            .options || [];
        const optLabel = opts.length
          ? opts.map((o) => o.label || o.id).join(' · ')
          : a.stageName || a.strategy;
        return `<div class="msn-si-row"><dt>${escapeHtml(
          a.artifactType || 'Artifact'
        )}</dt><dd>Acquire via ${escapeHtml(optLabel || '—')}<br/><span class="msn-objective-meta">${escapeHtml(
          a.reason || ''
        )}</span></dd></div>`;
      })
      .join('');

    const missingRows = missing
      .filter((m) => !(resolved || []).some((r) => r.type === m.artifactType))
      .filter(
        (m) =>
          !acquisitions.some(
            (a) =>
              a.artifactType === m.artifactType &&
              a.strategy === 'unavailable'
          )
      )
      .map((m) => {
        const opts = (m.options || [])
          .map((o) => o.label || o.id)
          .filter(Boolean);
        return `<div class="msn-si-row"><dt>${escapeHtml(
          m.artifactType
        )}</dt><dd>No compatible artifact found.${
          opts.length
            ? `<br/><span class="msn-objective-meta">Acquire via: ${escapeHtml(
                opts.join(' · ')
              )}</span>`
            : `<br/><span class="msn-objective-meta">Register a capability that produces ${escapeHtml(
                m.artifactType
              )}.</span>`
        }</dd></div>`;
      })
      .join('');

    return `<section class="msn-block msn-artifact-resolution" id="msnArtifactResolution">
      <h3>Artifact Resolution</h3>
      <p class="msn-objective-meta">Required state before capability selection (SPEC-051).</p>
      ${
        resolvedRows
          ? `<h4 class="msn-subhead">Resolved Artifacts</h4><dl class="msn-si-dl">${resolvedRows}</dl>`
          : ''
      }
      ${
        acquireRows || missingRows
          ? `<h4 class="msn-subhead">Acquisition Decisions</h4><dl class="msn-si-dl">${
              acquireRows || missingRows
            }</dl>`
          : ''
      }
      ${
        skipRows
          ? `<h4 class="msn-subhead">Skipped Capabilities</h4><ul class="msn-bucket-list">${skipRows}</ul>`
          : ''
      }
    </section>`;
  }

  /** SPEC-054 — why capabilities were selected, rejected, or unavailable */
  function renderPlanningDiagnosticsHtml(diagnostics) {
    const diag = diagnostics || null;
    if (!diag) return '';
    const decisions = Array.isArray(diag.decisions) ? diag.decisions : [];
    const blocked = Array.isArray(diag.blocked) ? diag.blocked : [];
    const segments = Array.isArray(diag.missionSegments)
      ? diag.missionSegments
      : [];
    if (!decisions.length && !blocked.length && !segments.length) return '';

    const decisionRows = decisions
      .map((d) => {
        const mark = d.selected ? '✓' : '✗';
        return `<li><span>${mark} ${escapeHtml(
          d.name || d.capabilityId || 'Capability'
        )}${
          d.reason
            ? ` <span class="msn-objective-meta">— ${escapeHtml(d.reason)}</span>`
            : ''
        }</span></li>`;
      })
      .join('');

    const blockedRows = blocked
      .map((b) => {
        if (b.kind === 'missing_producer' || b.artifact) {
          return `<div class="msn-si-row"><dt>✗ ${escapeHtml(
            b.expectedProducer || b.artifact || 'Producer'
          )} missing</dt><dd>Reason: ${escapeHtml(
            (b.possibleCauses && b.possibleCauses[0]) || b.reason || 'Blocked'
          )}<br/><span class="msn-objective-meta">Suggested Fix: ${escapeHtml(
            b.recommendedAction || 'Register a matching capability.'
          )}</span></dd></div>`;
        }
        return `<div class="msn-si-row"><dt>✗ ${escapeHtml(
          b.name || b.capabilityId || 'Capability'
        )}</dt><dd>Reason: ${escapeHtml(
          b.reason || 'Blocked'
        )}<br/><span class="msn-objective-meta">Suggested Fix: ${escapeHtml(
          b.recommendedAction || 'Inspect Capability Registry.'
        )}</span></dd></div>`;
      })
      .join('');

    const segmentRows = segments
      .map((s) => {
        const matches = (s.suggestedMatches || []).join(', ');
        return `<div class="msn-si-row"><dt>Mission Segment</dt><dd>Input: ${escapeHtml(
          s.input || '—'
        )}<br/>Status: ${escapeHtml(
          s.status || 'No matching mission alias'
        )}${
          matches
            ? `<br/><span class="msn-objective-meta">Suggested Matches: ${escapeHtml(
                matches
              )}</span>`
            : ''
        }${
          s.recommendedAction
            ? `<br/><span class="msn-objective-meta">${escapeHtml(
                s.recommendedAction
              )}</span>`
            : ''
        }</dd></div>`;
      })
      .join('');

    return `<section class="msn-block msn-planning-diagnostics" id="msnPlanningDiagnostics">
      <h3>Planning Diagnostics</h3>
      <p class="msn-objective-meta">Why the planner selected or blocked capabilities (SPEC-054 / ADR-038).</p>
      ${
        decisionRows
          ? `<h4 class="msn-subhead">Capability Decisions</h4><ul class="msn-bucket-list">${decisionRows}</ul>`
          : ''
      }
      ${
        blockedRows
          ? `<h4 class="msn-subhead">Blocked</h4><dl class="msn-si-dl">${blockedRows}</dl>`
          : ''
      }
      ${
        segmentRows
          ? `<h4 class="msn-subhead">Unknown Mission Text</h4><dl class="msn-si-dl">${segmentRows}</dl>`
          : ''
      }
    </section>`;
  }

  /** SPEC-058 — Blocked Preconditions panel */
  function renderPreconditionDiagnosticsHtml(diagnostics) {
    const d = diagnostics || null;
    if (!d || typeof d !== 'object') return '';
    const failed =
      d.failedPrecondition ||
      d.message ||
      (d.diagnosis && d.diagnosis.failedPrecondition) ||
      null;
    if (!failed && !d.expectedArtifact && !d.actualState) return '';
    const expected =
      d.expectedArtifact ||
      (d.diagnosis && d.diagnosis.expectedArtifact) ||
      '—';
    const actual =
      d.actualState || (d.diagnosis && d.diagnosis.actualState) || '—';
    const producer =
      d.expectedProducer ||
      d.producer ||
      (d.diagnosis &&
        (d.diagnosis.expectedProducer || d.diagnosis.producer)) ||
      '—';
    const next =
      d.recommendedNextAction ||
      (d.diagnosis && d.diagnosis.recommendedNextAction) ||
      '—';
    const capabilityName =
      d.capabilityName ||
      (d.capabilityId === 'campaign_review' ? 'Campaign Review' : null) ||
      d.capabilityId ||
      'Capability';
    return `<section class="msn-block msn-precondition-diagnostics" id="msnPreconditionDiagnostics">
      <h3>Blocked Preconditions</h3>
      <p class="msn-objective-meta">${escapeHtml(
        String(capabilityName)
      )} · Status: Blocked</p>
      <dl class="msn-si-dl">
        <div class="msn-si-row"><dt>Failed Precondition</dt><dd>${escapeHtml(
          String(failed || 'Blocked')
        )}</dd></div>
        <div class="msn-si-row"><dt>Expected Artifact</dt><dd>${escapeHtml(
          String(expected)
        )}</dd></div>
        <div class="msn-si-row"><dt>Actual State</dt><dd>${escapeHtml(
          String(actual)
        )}</dd></div>
        <div class="msn-si-row"><dt>Producer</dt><dd>${escapeHtml(
          String(producer)
        )}</dd></div>
        <div class="msn-si-row"><dt>Recommended Next Action</dt><dd>${escapeHtml(
          String(next)
        )}</dd></div>
      </dl>
    </section>`;
  }

  function formatStageLabel(stageId) {
    const labels = {
      prospect_discovery: 'Discovery',
      company_enrichment: 'Company Enrichment',
      opportunity_ranking: 'Opportunity Ranking',
      business_intelligence: 'Business Intelligence',
      sales_intelligence: 'Sales Intelligence',
      campaign_builder: 'Campaign Builder',
    };
    return labels[stageId] || String(stageId || '').replace(/_/g, ' ');
  }

  function formatMissionPlanLabel(key) {
    const labels = {
      prospectList: 'ProspectList',
      client: 'Client',
      campaign: 'Campaign',
      market: 'Market',
      budget: 'Budget',
      tenant: 'Tenant',
      targetCount: 'Target count',
    };
    return labels[key] || String(key);
  }

  function filteredQueuePackages(session) {
    if (!session) return [];
    const all = session.packages || [];
    const filter = session.queueFilter || 'all';
    if (filter === 'warnings') {
      return all.filter(
        (p) =>
          (p.warnings && p.warnings.length) ||
          session.warningItems.some((w) => w.packageId === p.id)
      );
    }
    if (filter === 'needs_review') {
      return all.filter((p) => p.needsReview && !session.approvals[p.id]);
    }
    if (filter === 'ready') {
      return all.filter((p) => p.ready || session.approvals[p.id]);
    }
    return all.slice();
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
    resetAskInput();
    setContextLabel(context);
    announce('Max intelligence workspace opened.');
    window.requestAnimationFrame(() => autoGrowAskInput());

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

  function renderBusinessIntelligenceHtml(pkg) {
    const bi = pkg && pkg.businessIntelligence;
    if (!bi) {
      return `<section class="msn-si-block">
        <h4 class="msn-subhead">Business Intelligence</h4>
        <p class="msn-objective-meta">No Business Intelligence Profile attached to this package.</p>
      </section>`;
    }
    const rows = [
      ['Company', bi.company],
      ['Industry', bi.industry],
      ['Business model', bi.business_model],
      ['Revenue model', bi.revenue_model],
      ['Primary customers', bi.primary_customers],
      ['Growth strategy', bi.growth_strategy],
      ['Competitive position', bi.competitive_position],
      ['Vendor landscape', bi.vendor_landscape],
      ['Seasonality', bi.seasonality],
      ['Service angle', bi.service_angle],
      ['Confidence', bi.confidence],
    ];
    const answers = bi.qualityAnswers || {};
    const answerRows = [
      ['How they make money', answers.howTheyMakeMoney],
      ['Growth constraints', answers.growthConstraints],
      ['Operational pressures', answers.operationalPressures],
      ['Problem owner', answers.problemOwner],
      ['Why buy now', answers.whyBuyNow],
    ];
    const constraints = Array.isArray(bi.operational_constraints)
      ? bi.operational_constraints
      : [];
    const kpis = Array.isArray(bi.likely_kpis) ? bi.likely_kpis : [];
    const triggers = Array.isArray(bi.buying_triggers) ? bi.buying_triggers : [];
    const uncertainty = Array.isArray(bi.uncertainty) ? bi.uncertainty : [];
    return `<section class="msn-si-block">
      <h4 class="msn-subhead">Business Intelligence</h4>
      <dl class="msn-si-dl">
        ${rows
          .filter(([, v]) => v)
          .map(
            ([k, v]) =>
              `<div class="msn-si-row"><dt>${escapeHtml(k)}</dt><dd>${escapeHtml(
                String(v)
              )}</dd></div>`
          )
          .join('')}
      </dl>
      <h5 class="msn-subhead">Required reasoning</h5>
      <dl class="msn-si-dl">
        ${answerRows
          .filter(([, v]) => v)
          .map(
            ([k, v]) =>
              `<div class="msn-si-row"><dt>${escapeHtml(k)}</dt><dd>${escapeHtml(
                String(v)
              )}</dd></div>`
          )
          .join('')}
      </dl>
      ${
        constraints.length
          ? `<p class="msn-objective-meta"><strong>Constraints:</strong> ${escapeHtml(
              constraints.join(' · ')
            )}</p>`
          : ''
      }
      ${
        kpis.length
          ? `<p class="msn-objective-meta"><strong>Likely KPIs:</strong> ${escapeHtml(
              kpis.join(' · ')
            )}</p>`
          : ''
      }
      ${
        triggers.length
          ? `<p class="msn-objective-meta"><strong>Buying triggers:</strong> ${escapeHtml(
              triggers.join(' · ')
            )}</p>`
          : ''
      }
      ${
        uncertainty.length
          ? `<p class="msn-objective-meta"><strong>Uncertainty:</strong> ${escapeHtml(
              uncertainty.slice(0, 5).join(' · ')
            )}</p>`
          : ''
      }
    </section>`;
  }

  function renderSalesIntelligenceHtml(pkg) {
    const sales = pkg && pkg.salesIntelligence;
    if (!sales) {
      return `<section class="msn-si-block">
        <h4 class="msn-subhead">Sales Intelligence</h4>
        <p class="msn-objective-meta">No Sales Intelligence Profile attached to this package.</p>
      </section>`;
    }
    const rows = [
      ['Company', sales.company],
      ['Industry', sales.industry],
      ['Decision maker', sales.decision_maker],
      ['Buyer type', sales.buyer_type],
      ['Primary pain', sales.primary_pain],
      ['Secondary pain', sales.secondary_pain],
      ['Business goal', sales.business_goal],
      ['Risk if unchanged', sales.risk_if_unchanged],
      ['Recommended angle', sales.recommended_angle],
      ['Call to action', sales.call_to_action],
      ['Confidence', sales.confidence],
    ];
    const advantages = Array.isArray(sales.anchor_advantage)
      ? sales.anchor_advantage
      : [];
    const claims = Array.isArray(sales.personalization_claims)
      ? sales.personalization_claims.filter((c) => c && c.verified)
      : [];
    const signals = Array.isArray(sales.buying_signals)
      ? sales.buying_signals
      : [];
    return `<section class="msn-si-block">
      <h4 class="msn-subhead">Sales Intelligence</h4>
      <dl class="msn-si-dl">
        ${rows
          .filter(([, v]) => v)
          .map(
            ([k, v]) =>
              `<div class="msn-si-row"><dt>${escapeHtml(k)}</dt><dd>${escapeHtml(
                String(v)
              )}</dd></div>`
          )
          .join('')}
      </dl>
      ${
        advantages.length
          ? `<p class="msn-objective-meta"><strong>Anchor advantages:</strong> ${escapeHtml(
              advantages.join(' · ')
            )}</p>`
          : ''
      }
      ${
        claims.length
          ? `<ul class="msn-si-claims">${claims
              .slice(0, 4)
              .map(
                (c) =>
                  `<li>${escapeHtml(c.claim)}${
                    c.evidenceRef
                      ? ` <span class="msn-objective-meta">(${escapeHtml(
                          c.evidenceRef
                        )})</span>`
                      : ''
                  }</li>`
              )
              .join('')}</ul>`
          : ''
      }
      ${
        signals.length
          ? `<p class="msn-objective-meta"><strong>Buying signals:</strong> ${escapeHtml(
              signals
                .slice(0, 3)
                .map((s) => s.signal)
                .join(' · ')
            )}</p>`
          : ''
      }
    </section>`;
  }

  function renderMessagingStrategyHtml(pkg) {
    const ms =
      (pkg && pkg.messagingStrategy) ||
      (pkg && pkg.salesIntelligence && pkg.salesIntelligence.messaging_strategy);
    if (!ms) {
      return `<section class="msn-si-block">
        <h4 class="msn-subhead">Messaging Strategy</h4>
        <p class="msn-objective-meta">No messaging strategy on this package.</p>
      </section>`;
    }
    return `<section class="msn-si-block">
      <h4 class="msn-subhead">Messaging Strategy</h4>
      <dl class="msn-si-dl">
        ${
          ms.opening_focus
            ? `<div class="msn-si-row"><dt>Opening focus</dt><dd>${escapeHtml(
                ms.opening_focus
              )}</dd></div>`
            : ''
        }
        ${
          ms.positioning
            ? `<div class="msn-si-row"><dt>Positioning</dt><dd>${escapeHtml(
                ms.positioning
              )}</dd></div>`
            : ''
        }
        ${
          ms.cta
            ? `<div class="msn-si-row"><dt>CTA</dt><dd>${escapeHtml(
                ms.cta
              )}</dd></div>`
            : ''
        }
        ${
          Array.isArray(ms.tone) && ms.tone.length
            ? `<div class="msn-si-row"><dt>Tone</dt><dd>${escapeHtml(
                ms.tone.join(' · ')
              )}</dd></div>`
            : ''
        }
        ${
          Array.isArray(ms.avoid) && ms.avoid.length
            ? `<div class="msn-si-row"><dt>Avoid</dt><dd>${escapeHtml(
                ms.avoid.join(' · ')
              )}</dd></div>`
            : ''
        }
        ${
          Array.isArray(ms.social_proof) && ms.social_proof.length
            ? `<div class="msn-si-row"><dt>Social proof</dt><dd>${escapeHtml(
                ms.social_proof.join(' · ')
              )}</dd></div>`
            : ''
        }
      </dl>
    </section>`;
  }

  function renderOperatorConfidenceHtml(pkg) {
    const oc = pkg && pkg.operatorConfidence;
    if (!oc) {
      return `<section class="msn-si-block">
        <h4 class="msn-subhead">Operator Confidence Score</h4>
        <p class="msn-objective-meta">Advisory score unavailable.</p>
      </section>`;
    }
    const dims = [
      ['Industry Accuracy', oc.industryAccuracy],
      ['Buyer Relevance', oc.buyerRelevance],
      ['Evidence Use', oc.evidenceUse],
      ['Specificity', oc.specificity],
      ['Naturalness', oc.naturalness],
      ['Sales Judgment', oc.salesJudgment],
    ];
    return `<section class="msn-si-block msn-si-confidence">
      <h4 class="msn-subhead">Operator Confidence Score</h4>
      <p class="msn-pkg-meta-value"><strong>${escapeHtml(
        String(oc.overall != null ? oc.overall : '—')
      )}</strong>${
        oc.passed === false || oc.editInstinct
          ? ' · <span class="msn-queue-filter-label">edit instinct</span>'
          : ' · advisory'
      }</p>
      <ul class="msn-si-score-list">
        ${dims
          .map(
            ([label, val]) =>
              `<li>${escapeHtml(label)}: ${escapeHtml(
                val != null ? `${val}/10` : '—'
              )}</li>`
          )
          .join('')}
      </ul>
    </section>`;
  }

  function renderPackagePreviewHtml(pkg, opts = {}) {
    if (!pkg) {
      return '<p class="msn-objective-meta">No package selected.</p>';
    }
    const editing = Boolean(opts.editing);
    const letter = editing
      ? `<label class="msn-import-label" for="msnPkgEditBody">Letter</label>
         <textarea id="msnPkgEditBody" class="msn-import-input msn-letter-edit" rows="10">${escapeHtml(
           pkg.letterBody || ''
         )}</textarea>`
      : `<pre class="msn-letter-preview">${escapeHtml(pkg.letterBody || 'No letter body available.')}</pre>`;
    return `<article class="msn-pkg-preview" data-pkg-id="${escapeHtml(pkg.id)}">
      <div class="msn-pkg-meta-grid">
        <div><span class="msn-metric-label">Company</span><p class="msn-pkg-meta-value">${escapeHtml(
          pkg.companyName
        )}</p></div>
        <div><span class="msn-metric-label">Recipient</span><p class="msn-pkg-meta-value">${escapeHtml(
          pkg.recipientName || '—'
        )}</p></div>
        <div><span class="msn-metric-label">Confidence</span><p class="msn-pkg-meta-value">${escapeHtml(
          pkg.confidenceLabel
        )}</p></div>
      </div>
      ${renderBusinessIntelligenceHtml(pkg)}
      ${renderSalesIntelligenceHtml(pkg)}
      ${renderMessagingStrategyHtml(pkg)}
      ${renderOperatorConfidenceHtml(pkg)}
      ${
        pkg.personalization
          ? `<p class="msn-pkg-personalization">${escapeHtml(pkg.personalization)}</p>`
          : ''
      }
      <h4 class="msn-subhead">Generated Letter</h4>
      ${letter}
      ${
        pkg.envelopeAddress
          ? `<h4 class="msn-subhead">Envelope</h4><pre class="msn-letter-preview msn-envelope-preview">${escapeHtml(
              pkg.envelopeAddress
            )}</pre>`
          : `<p class="msn-objective-meta">Envelope preview unavailable for this package.</p>`
      }
    </article>`;
  }

  function renderReviewQueueHtml(session) {
    const queue = filteredQueuePackages(session);
    if (!queue.length) {
      return `<section class="msn-block" id="msnReviewQueue">
        <h3>Review Queue</h3>
        <p class="msn-objective-meta">No packages in this filter.</p>
      </section>`;
    }
    let index = Number(session.queueIndex) || 0;
    if (index < 0) index = 0;
    if (index >= queue.length) index = queue.length - 1;
    session.queueIndex = index;
    const pkg = queue[index];
    const approved = Boolean(session.approvals[pkg.id]);
    return `<section class="msn-block msn-review-queue" id="msnReviewQueue">
      <h3>Review Queue</h3>
      <p class="msn-queue-progress">Package <strong>${escapeHtml(
        String(index + 1)
      )}</strong> / ${escapeHtml(String(queue.length))}${
      session.queueFilter && session.queueFilter !== 'all'
        ? ` · <span class="msn-queue-filter-label">${escapeHtml(
            session.queueFilter.replace(/_/g, ' ')
          )}</span>`
        : ''
    }</p>
      ${renderPackagePreviewHtml(pkg, { editing: session.editingId === pkg.id })}
      <div class="msn-queue-actions">
        <button type="button" class="cd-btn cd-btn-primary" data-msn-queue="approve"${
          approved ? ' disabled' : ''
        }>${approved ? 'Approved' : 'Approve'}</button>
        <button type="button" class="cd-btn cd-btn-ghost" data-msn-queue="edit">Edit</button>
        <button type="button" class="cd-btn cd-btn-ghost" data-msn-queue="prev"${
          index <= 0 ? ' disabled' : ''
        }>Previous</button>
        <button type="button" class="cd-btn cd-btn-ghost" data-msn-queue="next"${
          index >= queue.length - 1 ? ' disabled' : ''
        }>Next</button>
      </div>
    </section>`;
  }

  function renderWarningInspectorHtml(warningItems) {
    const items = warningItems || [];
    if (!items.length) {
      return `<section class="msn-block" id="msnWarnings">
        <h3>Warnings (0)</h3>
        <p class="msn-objective-meta">No warnings.</p>
      </section>`;
    }
    return `<section class="msn-block" id="msnWarnings">
      <h3>Warnings (${escapeHtml(String(items.length))})</h3>
      <ul class="msn-warning-list">
        ${items
          .map(
            (w) => `<li>
            <button type="button" class="msn-warning-item msn-interactive" data-msn-warning-pkg="${escapeHtml(
              w.packageId || ''
            )}" ${w.packageId ? '' : 'disabled'}>
              <span class="msn-warning-mark" aria-hidden="true">⚠</span>
              <span class="msn-warning-copy">
                <strong>${escapeHtml(w.company || 'Mission')}</strong>
                <span>${escapeHtml(w.message)}</span>
              </span>
            </button>
          </li>`
          )
          .join('')}
      </ul>
    </section>`;
  }

  function renderPackageListHtml(packages) {
    const pkgs = packages || [];
    if (!pkgs.length) {
      return '<p class="msn-objective-meta">No mail packages generated yet.</p>';
    }
    return `<ul class="msn-package-list">
      ${pkgs
        .map(
          (pkg, i) => `<li class="msn-package-row">
          <button type="button" class="msn-package-open msn-interactive" data-msn-open-pkg="${escapeHtml(
            pkg.id
          )}">
            <span class="msn-package-name">${escapeHtml(pkg.companyName)}</span>
            <span class="msn-package-conf">Confidence: ${escapeHtml(
              pkg.confidenceLabel
            )}</span>
            <span class="msn-package-preview-label">Preview</span>
          </button>
          ${i < pkgs.length - 1 ? '<hr class="msn-package-rule" />' : ''}
        </li>`
        )
        .join('')}
    </ul>`;
  }

  function scrollMsnSection(id) {
    const node = els.msnBody && els.msnBody.querySelector(`#${id}`);
    if (node && typeof node.scrollIntoView === 'function') {
      node.scrollIntoView({ behavior: prefersReducedMotion() ? 'auto' : 'smooth', block: 'start' });
    }
  }

  function refreshReviewQueueUi() {
    if (!els.msnBody || !msnReviewSession) return;
    const host = els.msnBody.querySelector('#msnReviewQueue');
    if (!host) return;
    const wrap = document.createElement('div');
    wrap.innerHTML = renderReviewQueueHtml(msnReviewSession);
    const next = wrap.firstElementChild;
    if (next) host.replaceWith(next);
    bindReviewQueueControls();
  }

  function openPackageInQueue(packageId, filter) {
    if (!msnReviewSession) return;
    if (filter) msnReviewSession.queueFilter = filter;
    const queue = filteredQueuePackages(msnReviewSession);
    const idx = queue.findIndex((p) => p.id === packageId);
    msnReviewSession.queueIndex = idx >= 0 ? idx : 0;
    msnReviewSession.editingId = null;
    refreshReviewQueueUi();
    scrollMsnSection('msnReviewQueue');
  }

  function bindReviewQueueControls() {
    if (!els.msnBody || !msnReviewSession) return;
    els.msnBody.querySelectorAll('[data-msn-queue]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const action = btn.getAttribute('data-msn-queue');
        const queue = filteredQueuePackages(msnReviewSession);
        const pkg = queue[msnReviewSession.queueIndex];
        if (!pkg && action !== 'next' && action !== 'prev') return;
        if (action === 'approve' && pkg) {
          msnReviewSession.approvals[pkg.id] = true;
          pkg.approved = true;
          pkg.ready = true;
          pkg.needsReview = false;
          announce(`Approved ${pkg.companyName} (session).`);
          if (msnReviewSession.queueIndex < queue.length - 1) {
            msnReviewSession.queueIndex += 1;
          }
          msnReviewSession.editingId = null;
          refreshReviewQueueUi();
          return;
        }
        if (action === 'edit' && pkg) {
          msnReviewSession.editingId =
            msnReviewSession.editingId === pkg.id ? null : pkg.id;
          refreshReviewQueueUi();
          const ta = els.msnBody.querySelector('#msnPkgEditBody');
          if (ta) {
            ta.focus();
            ta.addEventListener(
              'change',
              () => {
                pkg.letterBody = ta.value;
              },
              { once: true }
            );
          }
          return;
        }
        if (action === 'prev') {
          msnReviewSession.queueIndex = Math.max(0, msnReviewSession.queueIndex - 1);
          msnReviewSession.editingId = null;
          refreshReviewQueueUi();
          return;
        }
        if (action === 'next') {
          msnReviewSession.queueIndex = Math.min(
            queue.length - 1,
            msnReviewSession.queueIndex + 1
          );
          msnReviewSession.editingId = null;
          refreshReviewQueueUi();
        }
      });
    });
  }

  function bindMissionReviewInteractions() {
    if (!els.msnBody || !msnReviewSession) return;

    els.msnBody.querySelectorAll('[data-msn-toggle]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const id = btn.getAttribute('data-msn-toggle');
        const panel = els.msnBody.querySelector(`[data-msn-panel="${id}"]`);
        if (!panel) return;
        const open = panel.hasAttribute('hidden');
        if (open) panel.removeAttribute('hidden');
        else panel.setAttribute('hidden', '');
        btn.setAttribute('aria-expanded', open ? 'true' : 'false');
        const label = btn.querySelector('[data-msn-toggle-label]');
        if (label) {
          const openLabel = btn.getAttribute('data-label-open') || 'Hide';
          const closedLabel =
            btn.getAttribute('data-label-closed') ||
            (btn.hasAttribute('data-msn-toggle-details')
              ? 'Details'
              : 'View packages');
          label.textContent = open ? openLabel : closedLabel;
        }
      });
    });

    els.msnBody.querySelectorAll('[data-msn-nav]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const nav = btn.getAttribute('data-msn-nav');
        if (nav === 'prospects') {
          const inputs = els.msnBody.querySelector('#msnInputs');
          const list = els.msnBody.querySelector('[data-msn-panel="prospect-rows"]');
          if (list) list.removeAttribute('hidden');
          scrollMsnSection('msnInputs');
          announce('Prospect list');
          return;
        }
        if (nav === 'personalized') {
          const panel =
            els.msnBody.querySelector('#msnCampaignCard [data-msn-panel]') ||
            els.msnBody.querySelector('.msn-artifact-expand[data-msn-panel]');
          if (panel) panel.removeAttribute('hidden');
          const toggle =
            els.msnBody.querySelector('#msnCampaignCard [data-msn-toggle]') ||
            els.msnBody.querySelector('.msn-artifact-expandable [data-msn-toggle]');
          if (toggle) {
            toggle.setAttribute('aria-expanded', 'true');
            const label = toggle.querySelector('[data-msn-toggle-label]');
            if (label) {
              label.textContent =
                toggle.getAttribute('data-label-open') || 'Hide packages';
            }
          }
          scrollMsnSection('msnDeliverables');
          msnReviewSession.queueFilter = 'all';
          refreshReviewQueueUi();
          scrollMsnSection('msnReviewQueue');
          return;
        }
        if (nav === 'warnings') {
          scrollMsnSection('msnWarnings');
          return;
        }
        if (nav === 'needs_review') {
          msnReviewSession.queueFilter = 'needs_review';
          msnReviewSession.queueIndex = 0;
          refreshReviewQueueUi();
          scrollMsnSection('msnReviewQueue');
          return;
        }
        if (nav === 'ready') {
          msnReviewSession.queueFilter = 'ready';
          msnReviewSession.queueIndex = 0;
          refreshReviewQueueUi();
          scrollMsnSection('msnReviewQueue');
        }
      });
    });

    els.msnBody.querySelectorAll('[data-msn-open-pkg]').forEach((btn) => {
      btn.addEventListener('click', () => {
        openPackageInQueue(btn.getAttribute('data-msn-open-pkg'), 'all');
      });
    });

    els.msnBody.querySelectorAll('[data-msn-warning-pkg]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const id = btn.getAttribute('data-msn-warning-pkg');
        if (!id) {
          announce('No related package for this warning.');
          return;
        }
        openPackageInQueue(id, 'warnings');
      });
    });

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

    els.msnBody.querySelectorAll('[data-msn-opreq-expand]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const root = btn.closest('[data-msn-opreq]');
        if (!root) return;
        root.querySelector('[data-msn-opreq-collapsed]')?.setAttribute('hidden', '');
        root.querySelector('[data-msn-opreq-expanded]')?.removeAttribute('hidden');
      });
    });
    els.msnBody.querySelectorAll('[data-msn-opreq-collapse]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const root = btn.closest('[data-msn-opreq]');
        if (!root) return;
        root.querySelector('[data-msn-opreq-expanded]')?.setAttribute('hidden', '');
        root.querySelector('[data-msn-opreq-collapsed]')?.removeAttribute('hidden');
      });
    });

    bindReviewQueueControls();
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
      const evidenceList = Array.isArray(data.evidence) ? data.evidence : [];
      const evidence = evidenceList
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
      const packages = mailPackagesFromWorkspace(mission, artifacts);
      const warningItems = collectReviewWarnings(mission, packages, evidenceList);
      const reviewModel = reviewDashboardModel(
        mission,
        artifacts,
        packages,
        warningItems
      );

      const prevApprovals =
        msnReviewSession && msnReviewSession.missionId === missionId
          ? msnReviewSession.approvals
          : {};
      packages.forEach((p) => {
        if (prevApprovals[p.id]) {
          p.approved = true;
          p.ready = true;
          p.needsReview = false;
        }
      });
      msnReviewSession = {
        missionId,
        packages,
        warningItems,
        approvals: { ...prevApprovals },
        queueFilter: 'all',
        queueIndex: 0,
        editingId: null,
      };

      const objectiveRaw = String(mission.objectiveText || '');
      const objectiveFirstLine =
        objectiveRaw.split(/\r?\n/).find((l) => String(l).trim()) || 'Mission objective';
      const missionPlan =
        (mission.plan && mission.plan.missionPlan) || mission.missionPlan || null;
      const missionPlanSummary =
        (mission.plan && mission.plan.missionPlanSummary) || null;
      const missionIntent =
        (mission.plan && mission.plan.missionIntent) ||
        mission.missionIntent ||
        null;
      const missionIntentSummary =
        (mission.plan && mission.plan.missionIntentSummary) ||
        mission.missionIntentSummary ||
        null;
      const missionIntentHtml = renderMissionIntentHtml(
        missionIntent,
        missionIntentSummary
      );
      const evidencePlan =
        (mission.plan && mission.plan.evidencePlan) ||
        mission.evidencePlan ||
        null;
      const evidencePlanSummary =
        (mission.plan && mission.plan.evidencePlanSummary) ||
        mission.evidencePlanSummary ||
        null;
      const evidenceRequirementsHtml = renderEvidenceRequirementsHtml(
        evidencePlan,
        evidencePlanSummary
      );
      const missionPlanHtml = renderMissionPlanHtml(missionPlan, missionPlanSummary);
      const artifactResolutionHtml = renderArtifactResolutionHtml(
        (mission.plan && mission.plan.artifactResolution) ||
          (mission.plan &&
            mission.plan.executionGraph &&
            mission.plan.executionGraph.artifactResolution) ||
          null
      );
      const planningDiagnosticsHtml = renderPlanningDiagnosticsHtml(
        (mission.plan && mission.plan.planningDiagnostics) || null
      );
      const preconditionDiagnosticsHtml = renderPreconditionDiagnosticsHtml(
        data.preconditionDiagnostics ||
          (mission.stageReview &&
            mission.stageReview.preconditionDiagnostics) ||
          (mission.deliverables &&
            mission.deliverables.preconditionDiagnostics) ||
          null
      );
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
            <button type="button" class="msn-link-btn msn-interactive" data-msn-objective-expand>Expand</button>
          </div>
          <div data-msn-objective-expanded hidden>
            <p class="msn-objective">${escapeHtml(objectiveRaw)}</p>
            <button type="button" class="msn-link-btn msn-interactive" data-msn-objective-collapse>Collapse</button>
          </div>
        </section>`;

      const prospectRowsHtml = prospectRows.length
        ? `<ul class="msn-prospect-rows" data-msn-panel="prospect-rows" hidden>
            ${prospectRows
              .slice(0, 200)
              .map(
                (p) =>
                  `<li><strong>${escapeHtml(
                    p.companyName || p.name || 'Company'
                  )}</strong>${
                    p.website
                      ? ` · <span class="msn-objective-meta">${escapeHtml(
                          p.website
                        )}</span>`
                      : ''
                  }</li>`
              )
              .join('')}
          </ul>`
        : '';

      const inputsHtml = `<section class="msn-block" id="msnInputs">
          <h3>Inputs</h3>
          ${
            attachedCount
              ? `<p class="msn-artifact-title">Prospect List</p>
            <div class="msn-metric-grid">
              <div class="msn-metric msn-metric-static"><span class="msn-metric-label">Companies</span><span class="msn-metric-value">${escapeHtml(
                String(attachedCount)
              )}</span></div>
              ${
                prospectListArt && prospectListArt.metadata && prospectListArt.metadata.operatorSupplied
                  ? `<div class="msn-metric msn-metric-static"><span class="msn-metric-label">Source</span><span class="msn-metric-value" style="font-size:0.85rem">Operator</span></div>`
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
            }
            ${
              prospectRows.length
                ? `<button type="button" class="msn-link-btn msn-interactive" data-msn-toggle="prospect-rows" data-label-open="Hide companies" data-label-closed="View companies" aria-expanded="false"><span data-msn-toggle-label>View companies</span></button>${prospectRowsHtml}`
                : ''
            }`
              : `<p class="msn-objective-meta">No prospect list attached yet.</p>`
          }
        </section>`;

      const stageRows = steps
        .map((s, idx) => {
          const pct = stageProgressPct(s);
          const running = String(s.status) === 'running';
          const rs = s.reviewSummary || {};
          const stageId = s.stageId || s.capabilityId || `stage-${idx}`;
          const stageArts = artifacts.filter(
            (a) =>
              a.stageId === stageId ||
              a.stageId === s.capabilityId ||
              (a.producer &&
                (a.producer === s.capabilityId || a.producer === stageId))
          );
          const stageEvidence = evidenceList.filter(
            (e) =>
              e.capabilityId === s.capabilityId ||
              e.capabilityId === stageId ||
              e.name === s.name
          );
          const metrics = [];
          if (rs.publishedCount != null) {
            metrics.push(`${rs.publishedCount} processed`);
          }
          if (Array.isArray(s.warnings) && s.warnings.length) {
            metrics.push(`Warnings ${s.warnings.length}`);
          }
          if (s.blockingIssues && s.blockingIssues.length) {
            metrics.push(`Blocked ${s.blockingIssues.length}`);
          }
          const panelId = `stage-${idx}`;
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
            <button type="button" class="msn-link-btn msn-interactive" data-msn-toggle="${escapeHtml(
              panelId
            )}" data-msn-toggle-details data-label-open="Hide details" data-label-closed="Details" aria-expanded="false"><span data-msn-toggle-label>Details</span></button>
            <div class="msn-stage-details" data-msn-panel="${escapeHtml(
              panelId
            )}" hidden>
              <h4 class="msn-subhead">Artifacts</h4>
              ${
                stageArts.length
                  ? `<ul class="msn-stage-art-list">${stageArts
                      .map((a) => {
                        const h = businessArtifactHeadline(a);
                        return `<li><strong>${escapeHtml(
                          h.title
                        )}</strong> · ${escapeHtml(h.summary)} · ${escapeHtml(
                          a.validationStatus || ''
                        )}</li>`;
                      })
                      .join('')}</ul>`
                  : `<p class="msn-objective-meta">No artifacts for this stage yet.</p>`
              }
              <h4 class="msn-subhead">Evidence</h4>
              ${
                stageEvidence.length
                  ? `<ul>${stageEvidence
                      .map((e) => `<li>${escapeHtml(e.summary || '')}</li>`)
                      .join('')}</ul>`
                  : `<p class="msn-objective-meta">No evidence summary.</p>`
              }
              <h4 class="msn-subhead">Warnings</h4>
              ${
                Array.isArray(s.warnings) && s.warnings.length
                  ? `<ul>${s.warnings
                      .map((w) => `<li>${escapeHtml(warningText(w))}</li>`)
                      .join('')}</ul>`
                  : `<p class="msn-objective-meta">None</p>`
              }
              <h4 class="msn-subhead">Blocking issues</h4>
              ${
                Array.isArray(s.blockingIssues) && s.blockingIssues.length
                  ? `<ul>${s.blockingIssues
                      .map((w) => `<li>${escapeHtml(warningText(w))}</li>`)
                      .join('')}</ul>`
                  : `<p class="msn-objective-meta">None</p>`
              }
            </div>
          </li>`;
        })
        .join('');

      const artifactRows = artifacts
        .map((art, artIdx) => {
          const headline = businessArtifactHeadline(art);
          const status = art.validationStatus || 'unknown';
          const rev = art.revision != null ? `v${art.revision}` : '';
          const type = art.artifactType || art.type;
          const expandable =
            type === 'Campaign' || type === 'MailPackage';
          const panelId = `artifact-pkgs-${artIdx}`;
          let packagePanel = '';
          if (type === 'Campaign' || type === 'MailPackage') {
            packagePanel = `<button type="button" class="msn-link-btn msn-interactive" data-msn-toggle="${escapeHtml(
              panelId
            )}" data-label-open="Hide packages" data-label-closed="View packages" aria-expanded="false"><span data-msn-toggle-label>View packages</span></button>
              <div class="msn-artifact-expand" data-msn-panel="${escapeHtml(
                panelId
              )}" hidden>
                ${renderPackageListHtml(packages)}
              </div>`;
          }
          return `<li class="msn-artifact${expandable ? ' msn-artifact-expandable' : ''}" data-artifact-id="${escapeHtml(
            art.id || ''
          )}"${type === 'Campaign' ? ' id="msnCampaignCard"' : ''}>
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
            ${packagePanel}
          </li>`;
        })
        .join('');

      const validationFailures = Array.isArray(data.artifactValidationFailures)
        ? data.artifactValidationFailures
        : Array.isArray(
              mission.deliverables &&
                mission.deliverables.artifactValidationFailures
            )
          ? mission.deliverables.artifactValidationFailures
          : [];
      const validationFailureHtml = validationFailures.length
        ? `<section class="msn-block msn-artifact-validation" id="msnArtifactValidation">
            <h3>Artifact Validation</h3>
            <ul class="msn-validation-failures">
              ${validationFailures
                .map((f) => {
                  const reasons = Array.isArray(f.reasons)
                    ? f.reasons
                    : f.reason
                      ? [f.reason]
                      : ['Validation failed.'];
                  return `<li class="msn-validation-failure">
                    <div class="msn-artifact-head">
                      <p class="msn-artifact-title">${escapeHtml(
                        f.artifactType || 'Unknown'
                      )}</p>
                      <span class="cd-chip">${escapeHtml(
                        f.status || 'FAILED'
                      )}</span>
                      ${
                        f.remainsPlainText
                          ? '<span class="cd-chip">Plain text</span>'
                          : ''
                      }
                    </div>
                    <p class="msn-subhead">Reason</p>
                    <ul>${reasons
                      .map((r) => `<li>${escapeHtml(String(r))}</li>`)
                      .join('')}</ul>
                  </li>`;
                })
                .join('')}
            </ul>
          </section>`
        : '';

      const metricBtn = (nav, label, value) =>
        `<button type="button" class="msn-metric msn-metric-nav msn-interactive" data-msn-nav="${escapeHtml(
          nav
        )}">
          <span class="msn-metric-label">${escapeHtml(label)}</span>
          <span class="msn-metric-value">${escapeHtml(String(value))}</span>
        </button>`;

      const reviewHtml = `<section class="msn-block msn-review-dash" id="msnCampaignSummary">
          <h3>Campaign Summary</h3>
          <div class="msn-metric-grid">
            ${metricBtn('prospects', 'Prospects', reviewModel.prospects)}
            ${metricBtn('personalized', 'Personalized', reviewModel.personalized)}
            ${metricBtn('warnings', 'Warnings', reviewModel.warnings)}
            ${metricBtn('needs_review', 'Needs Review', reviewModel.needsReview)}
            ${metricBtn('ready', 'Ready', reviewModel.ready)}
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
            : 'A prospect list was detected in the Mission prompt. Import it to skip Discovery and continue at Business Intelligence.'
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

      const artifactDevDetails = artifacts
        .map(
          (art) => `<details class="msn-dev-nested">
            <summary>${escapeHtml(
              (art.artifactType || art.type || 'Artifact') +
                (art.revision != null ? ` v${art.revision}` : '')
            )}</summary>
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
          </details>`
        )
        .join('');

      els.msnBody.innerHTML = `
        ${discoveryFailedHtml}
        ${preconditionDiagnosticsHtml}
        ${reviewHtml}
        ${renderWarningInspectorHtml(warningItems)}
        ${renderReviewQueueHtml(msnReviewSession)}
        ${missionIntentHtml}
        ${evidenceRequirementsHtml}
        ${missionPlanHtml}
        ${artifactResolutionHtml}
        ${planningDiagnosticsHtml}
        ${validationFailureHtml}
        ${objectiveHtml}
        ${inputsHtml}
        <section class="msn-block">
          <h3>Progress</h3>
          <ul class="msn-stage-list">${
            stageRows || '<li class="msn-objective-meta">No stages yet</li>'
          }</ul>
        </section>
        <section class="msn-block" id="msnDeliverables">
          <h3>Deliverables</h3>
          <ul class="msn-artifacts">${
            artifactRows || '<li class="msn-objective-meta">No deliverables published yet</li>'
          }</ul>
        </section>
        <details class="msn-dev-details msn-block">
          <summary class="msn-interactive">Developer Details</summary>
          <h3 style="margin-top:0.75rem">Artifact revisions</h3>
          ${artifactDevDetails || '<p class="msn-objective-meta">No artifacts</p>'}
          <h3>Evidence</h3>
          <ul>${evidence || '<li>No evidence yet</li>'}</ul>
          <h3>Audit</h3>
          <ul>${audit || '<li>No events</li>'}</ul>
          <h3>Raw deliverables</h3>
          <pre class="msn-pre">${escapeHtml(
            JSON.stringify(mission.deliverables || {}, null, 2)
          )}</pre>
        </details>
        <p class="msn-note">No outbound actions occur automatically. Package Approve is session-local; mission Approve records review only.</p>
      `;

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

      bindMissionReviewInteractions();
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
    msnReviewSession = null;
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
    // Always keep the full operator prompt inspectable (Expand shows late instructions).
    let bodyHtml = renderExpandableOperatorBody(text);
    if (detected) {
      const card = renderAttachmentCard({
        type: 'prospect_list',
        title: 'Prospect List',
        status: 'Detected',
        meta: `${detected.count} ${detected.count === 1 ? 'Company' : 'Companies'}`,
        body: detected.block,
      });
      bodyHtml = `${bodyHtml}${card}`;
    }
    const div = document.createElement('div');
    div.className = 'mx-msg is-operator';
    div.innerHTML = `
      <p class="mx-msg-role">You</p>
      ${bodyHtml}
    `;
    els.mxThread.appendChild(div);
    bindAttachmentCards(div);
    bindExpandableOperatorBodies(div);
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
    resetCdAskInput();
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
  els.mxAskInput?.addEventListener('paste', () => {
    window.requestAnimationFrame(() => autoGrowAskInput());
  });

  // Page-level Ask Max bar: same Enter / Shift+Enter contract as the modal composer.
  els.askInput?.addEventListener('keydown', (event) => {
    if (event.key !== 'Enter' || event.shiftKey || event.isComposing) return;
    event.preventDefault();
    els.askForm?.requestSubmit();
  });
  els.askInput?.addEventListener('input', () => autoGrowCdAskInput());
  els.askInput?.addEventListener('paste', () => {
    window.requestAnimationFrame(() => autoGrowCdAskInput());
  });
  window.addEventListener('resize', () => {
    autoGrowAskInput();
    autoGrowCdAskInput();
  });
  autoGrowAskInput();
  autoGrowCdAskInput();

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
