'use strict';

/**
 * SPEC-102 — Max natural interaction cadence & progressive response reveal.
 * Presentation-only helpers shared by CIE and Max Workspace.
 * Does not call LLMs, mutate business state, or delay real computation.
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.MaxInteractionCadence = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  const WEIGHTS = Object.freeze({
    IMMEDIATE: 'immediate',
    STANDARD: 'standard',
    REASONING: 'reasoning',
    SYNTHESIS: 'synthesis',
    MAJOR: 'major',
  });

  /** Hard ceiling for artificial post-computation presentation delay (ms). */
  const MAX_ARTIFICIAL_DELAY_MS = 4000;

  const PROFILE_BASE = Object.freeze({
    immediate: Object.freeze({
      weight: WEIGHTS.IMMEDIATE,
      minimumPerceivedMs: 450,
      jitterMs: 180,
      thinkingAppearMs: 0,
      revealMode: 'chunk',
      wordsPerTick: 5,
      tickMs: 28,
    }),
    standard: Object.freeze({
      weight: WEIGHTS.STANDARD,
      minimumPerceivedMs: 1500,
      jitterMs: 400,
      thinkingAppearMs: 320,
      revealMode: 'chunk',
      wordsPerTick: 4,
      tickMs: 36,
    }),
    reasoning: Object.freeze({
      weight: WEIGHTS.REASONING,
      minimumPerceivedMs: 2800,
      jitterMs: 550,
      thinkingAppearMs: 350,
      revealMode: 'chunk',
      wordsPerTick: 4,
      tickMs: 40,
    }),
    synthesis: Object.freeze({
      weight: WEIGHTS.SYNTHESIS,
      minimumPerceivedMs: 4000,
      jitterMs: 700,
      thinkingAppearMs: 400,
      revealMode: 'chunk',
      wordsPerTick: 5,
      tickMs: 32,
    }),
    major: Object.freeze({
      weight: WEIGHTS.MAJOR,
      minimumPerceivedMs: 0,
      jitterMs: 0,
      thinkingAppearMs: 0,
      revealMode: 'staged',
      wordsPerTick: 0,
      tickMs: 0,
    }),
  });

  const THINKING_LABELS = Object.freeze({
    immediate: Object.freeze(['Considering your note…']),
    standard: Object.freeze([
      'Considering what you\'ve told me…',
      'Connecting this to what I know about your business…',
      'Working through this with you…',
    ]),
    reasoning: Object.freeze([
      'Thinking through the tradeoffs…',
      'Thinking through this with you…',
      'Connecting this to what you\'ve told me…',
      'Updating my understanding…',
    ]),
    synthesis: Object.freeze([
      'Reviewing the evidence…',
      'Looking across what we\'ve learned…',
      'Reviewing what we know…',
    ]),
    major: Object.freeze(['Understanding your business…']),
    evidence: Object.freeze(['Reviewing the evidence…', 'Looking across what we\'ve learned…']),
    neutral: Object.freeze(['Considering your question…']),
  });

  const IMMEDIATE_OPERATOR_RE =
    /^(?:yes|yep|yeah|ok|okay|sure|no|nope|hold(?:\s+this)?|continue|show\s+me\s+the\s+blueprint|open\s+the\s+pipeline|go\s+ahead|proceed|lgtm|thanks|thank\s+you)[.!]?$/i;

  const REASONING_OPERATOR_RE =
    /\b(?:don'?t\s+know|do\s+not\s+know|not\s+sure|unsure|help\s+me\s+think|think\s+through|what\s+do\s+you\s+think|biggest\s+opportunity|focus\s+on\s+first|who\s+should\s+we\s+target|ideal\s+customer|trade.?offs?)\b/i;

  const SYNTHESIS_OPERATOR_RE =
    /\b(?:recommend|compare|review\s+what|getting\s+wrong|publish\s+next|what\s+should\s+we\s+(?:do|publish|focus)|across\s+what\s+we'?ve\s+learned|executive\s+business\s+brief|campaign\s+review)\b/i;

  const NAV_OPERATOR_RE =
    /\b(?:show\s+me\s+(?:the\s+)?blueprint|open\s+(?:the\s+)?pipeline|status|where\s+are\s+we)\b/i;

  function clamp(n, min, max) {
    return Math.max(min, Math.min(max, n));
  }

  function prefersReducedMotion(matchMediaFn) {
    try {
      const mm = matchMediaFn || (typeof window !== 'undefined' && window.matchMedia);
      if (typeof mm !== 'function') return false;
      return Boolean(mm('(prefers-reduced-motion: reduce)').matches);
    } catch (_err) {
      return false;
    }
  }

  /**
   * Lightweight weight classification from UI-available signals only.
   * @param {object} ctx
   * @returns {string}
   */
  function classifyInteractionWeight(ctx) {
    const c = ctx || {};
    if (c.loadingMode === 'premium_checklist' || c.majorSynthesis === true) {
      return WEIGHTS.MAJOR;
    }
    if (
      c.nextAction === 'GENERATE_BLUEPRINT' ||
      c.nextAction === 'COMPLETE' ||
      c.hasExecutiveBrief === true
    ) {
      return WEIGHTS.MAJOR;
    }

    const disposition = String(c.answerDisposition || '');
    if (
      disposition === 'ANSWER_UNCERTAIN' ||
      disposition === 'ANSWER_CONTRADICTORY' ||
      disposition === 'ANSWER_NEEDS_SPECIFICITY'
    ) {
      return WEIGHTS.REASONING;
    }
    if (
      disposition === 'ANSWER_ACCEPTED' ||
      disposition === 'ANSWER_PARTIAL' ||
      disposition === 'ANSWER_DEFERRED'
    ) {
      return WEIGHTS.IMMEDIATE;
    }

    if (c.synthesis === true || c.recommendation === true || c.hasEvidencePath === true) {
      const proseLen = String(c.prose || c.responseText || '').length;
      if (proseLen > 900) return WEIGHTS.SYNTHESIS;
      return WEIGHTS.SYNTHESIS;
    }

    const operatorText = String(c.operatorText || c.message || '').trim();
    if (operatorText) {
      if (IMMEDIATE_OPERATOR_RE.test(operatorText) || NAV_OPERATOR_RE.test(operatorText)) {
        return WEIGHTS.IMMEDIATE;
      }
      if (SYNTHESIS_OPERATOR_RE.test(operatorText)) return WEIGHTS.SYNTHESIS;
      if (REASONING_OPERATOR_RE.test(operatorText)) return WEIGHTS.REASONING;
      if (operatorText.length <= 40 && !/[?]/.test(operatorText)) {
        return WEIGHTS.IMMEDIATE;
      }
    }

    if (c.surface === 'cie' && c.phase === 'discovery' && !disposition) {
      // Pre-response CIE capture turns default to standard; disposition refines after.
      if (operatorText && operatorText.length < 80 && !REASONING_OPERATOR_RE.test(operatorText)) {
        return WEIGHTS.IMMEDIATE;
      }
    }

    const prose = String(c.prose || c.responseText || '');
    if (prose.length > 1200) return WEIGHTS.SYNTHESIS;
    if (prose.length > 500) return WEIGHTS.REASONING;

    if (c.route === 'mission' && prose.length > 400) return WEIGHTS.SYNTHESIS;

    return WEIGHTS.STANDARD;
  }

  /**
   * @param {string} weight
   * @param {object} [opts]
   * @returns {object}
   */
  function buildInteractionProfile(weight, opts) {
    const key = PROFILE_BASE[weight] ? weight : WEIGHTS.STANDARD;
    const base = PROFILE_BASE[key];
    const options = opts || {};
    const reduced = options.reducedMotion === true;
    const jitterSeed =
      typeof options.jitterSeed === 'number'
        ? options.jitterSeed
        : Math.random();
    const jitter = reduced
      ? 0
      : Math.round((jitterSeed - 0.5) * 2 * base.jitterMs);
    const minimumPerceivedMs = reduced
      ? 0
      : Math.max(0, base.minimumPerceivedMs + jitter);
    return {
      weight: base.weight,
      thinkingLabel: options.thinkingLabel || selectThinkingLabel(base.weight, options),
      minimumPerceivedMs,
      jitterAppliedMs: jitter,
      thinkingAppearMs: reduced ? 0 : base.thinkingAppearMs,
      revealMode: reduced ? 'immediate' : base.revealMode,
      wordsPerTick: reduced ? 9999 : base.wordsPerTick,
      tickMs: reduced ? 0 : base.tickMs,
      reducedMotion: reduced,
    };
  }

  /**
   * Deterministic thinking-state language — no LLM.
   * @param {string} weight
   * @param {object} [ctx]
   */
  function selectThinkingLabel(weight, ctx) {
    const c = ctx || {};
    if (c.thinkingLabel) return String(c.thinkingLabel);
    if (c.clientName) {
      const name = String(c.clientName).trim();
      if (name && (weight === WEIGHTS.REASONING || weight === WEIGHTS.SYNTHESIS)) {
        return 'Thinking through this with what I know about ' + name + '…';
      }
      if (name && weight === WEIGHTS.STANDARD) {
        return 'Connecting this to what I know about ' + name + '…';
      }
    }
    if (c.hasEvidencePath === true || c.synthesis === true) {
      return pickLabel(THINKING_LABELS.evidence, c);
    }
    const bucket = THINKING_LABELS[weight] || THINKING_LABELS.neutral;
    return pickLabel(bucket, c);
  }

  function pickLabel(list, ctx) {
    const arr = list || THINKING_LABELS.neutral;
    if (!arr.length) return 'Considering your question…';
    const seed =
      typeof ctx.labelSeed === 'number'
        ? ctx.labelSeed
        : Math.floor(Math.random() * arr.length);
    return arr[Math.abs(seed) % arr.length];
  }

  /**
   * Real work time counts toward the minimum cadence.
   * Returns remaining artificial presentation delay only (never delays the request).
   */
  function computeRemainingDelayMs(startedAtMs, profile, nowMs) {
    const started = Number(startedAtMs) || 0;
    const now = nowMs == null ? Date.now() : Number(nowMs);
    const minimum = Number(profile && profile.minimumPerceivedMs) || 0;
    const elapsed = Math.max(0, now - started);
    const remaining = Math.max(0, minimum - elapsed);
    return clamp(remaining, 0, MAX_ARTIFICIAL_DELAY_MS);
  }

  function sleep(ms, signal) {
    const wait = Math.max(0, Number(ms) || 0);
    if (wait === 0) return Promise.resolve();
    return new Promise((resolve, reject) => {
      if (signal && signal.aborted) {
        reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
        return;
      }
      const timer = setTimeout(() => {
        if (signal) signal.removeEventListener('abort', onAbort);
        resolve();
      }, wait);
      function onAbort() {
        clearTimeout(timer);
        reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
      }
      if (signal) signal.addEventListener('abort', onAbort, { once: true });
    });
  }

  /**
   * Split prose into small word-group chunks (not character typing).
   */
  function chunkTextForReveal(text, wordsPerTick) {
    const full = String(text == null ? '' : text);
    if (!full) return [''];
    const n = Math.max(1, Number(wordsPerTick) || 4);
    // Preserve whitespace/newlines by splitting on spaces while keeping delimiters.
    const tokens = full.split(/(\s+)/);
    const chunks = [];
    let buf = '';
    let wordCount = 0;
    for (let i = 0; i < tokens.length; i += 1) {
      const tok = tokens[i];
      buf += tok;
      if (!/^\s+$/.test(tok) && tok !== '') {
        wordCount += 1;
        if (wordCount >= n) {
          chunks.push(buf);
          buf = '';
          wordCount = 0;
        }
      }
    }
    if (buf) chunks.push(buf);
    return chunks.length ? chunks : [full];
  }

  /**
   * Progressively reveal completed text into a target via setText(partial).
   * CLIENT-SIDE PROGRESSIVE REVEAL — backend already returned the full string.
   */
  async function revealTextProgressively(options) {
    const opts = options || {};
    const text = String(opts.text == null ? '' : opts.text);
    const setText = opts.setText;
    const profile = opts.profile || buildInteractionProfile(WEIGHTS.STANDARD);
    const signal = opts.signal;
    const onTick = opts.onTick;
    const reduced =
      opts.reducedMotion === true ||
      profile.reducedMotion === true ||
      profile.revealMode === 'immediate';

    if (typeof setText !== 'function') {
      throw new Error('revealTextProgressively requires setText');
    }

    if (reduced || profile.revealMode === 'staged' || !text) {
      setText(text);
      if (typeof onTick === 'function') onTick(text, true);
      return { text, chunks: 1, mode: 'immediate' };
    }

    // Long answers accelerate so the UI does not pretend to type for minutes.
    let wordsPerTick = profile.wordsPerTick || 4;
    let tickMs = profile.tickMs || 36;
    if (text.length > 1800) {
      wordsPerTick = Math.max(wordsPerTick, 10);
      tickMs = Math.max(12, Math.floor(tickMs * 0.55));
    } else if (text.length > 900) {
      wordsPerTick = Math.max(wordsPerTick, 7);
      tickMs = Math.max(16, Math.floor(tickMs * 0.7));
    } else if (text.length < 160) {
      wordsPerTick = Math.max(wordsPerTick, 6);
      tickMs = Math.max(16, Math.floor(tickMs * 0.75));
    }

    const chunks = chunkTextForReveal(text, wordsPerTick);
    let shown = '';
    for (let i = 0; i < chunks.length; i += 1) {
      if (signal && signal.aborted) {
        setText(text);
        throw Object.assign(new Error('aborted'), { name: 'AbortError' });
      }
      shown += chunks[i];
      setText(shown);
      if (typeof onTick === 'function') onTick(shown, i === chunks.length - 1);
      if (i < chunks.length - 1 && tickMs > 0) {
        await sleep(tickMs, signal);
      }
    }
    // Guarantee exact backend text (no loss/reorder).
    setText(text);
    return { text, chunks: chunks.length, mode: 'progressive' };
  }

  function createThinkingElement(documentRef, label, className) {
    const doc = documentRef || (typeof document !== 'undefined' ? document : null);
    if (!doc) return null;
    const el = doc.createElement('div');
    el.className = className || 'max-thinking';
    el.setAttribute('role', 'status');
    el.setAttribute('aria-live', 'polite');
    el.setAttribute('data-max-thinking', '1');
    el.textContent = label || 'Considering your question…';
    return el;
  }

  function clearThinkingElement(el) {
    if (!el) return;
    if (el.parentNode) el.parentNode.removeChild(el);
  }

  /**
   * Stick-to-bottom scroll helper. Stops forcing when user scrolls up.
   */
  function createScrollFollow(container, options) {
    const opts = options || {};
    const threshold = opts.threshold == null ? 80 : opts.threshold;
    let stick = true;
    function nearBottom() {
      if (!container) return true;
      return (
        container.scrollHeight - container.scrollTop - container.clientHeight <=
        threshold
      );
    }
    function onScroll() {
      stick = nearBottom();
    }
    function follow() {
      if (container && stick) {
        container.scrollTop = container.scrollHeight;
      }
    }
    function forceStick() {
      stick = true;
      follow();
    }
    function detach() {
      if (container) container.removeEventListener('scroll', onScroll);
    }
    if (container) container.addEventListener('scroll', onScroll);
    return {
      follow,
      forceStick,
      isSticking: () => stick,
      setStick: (v) => {
        stick = Boolean(v);
      },
      nearBottom,
      detach,
    };
  }

  /**
   * High-level turn presenter: thinking → wait remaining cadence → reveal → complete.
   * Request must already be in flight (or completed) when this is called with startedAtMs.
   */
  async function presentCompletedResponse(options) {
    const opts = options || {};
    const startedAtMs = opts.startedAtMs != null ? opts.startedAtMs : Date.now();
    const response = opts.response;
    const classifyCtx = Object.assign({}, opts.classifyContext || {}, {
      prose: opts.prose,
      responseText: opts.prose,
      answerDisposition: opts.answerDisposition,
      nextAction: opts.nextAction,
      synthesis: opts.synthesis,
      recommendation: opts.recommendation,
      hasEvidencePath: opts.hasEvidencePath,
      majorSynthesis: opts.majorSynthesis,
      loadingMode: opts.loadingMode,
      route: opts.route,
      operatorText: opts.operatorText,
      surface: opts.surface,
      phase: opts.phase,
      clientName: opts.clientName,
    });

    let weight = classifyInteractionWeight(classifyCtx);
    // Pre-response provisional weight can be overridden by response signals,
    // but never upgrade a CIE accepted capture turn into reasoning theater.
    if (opts.provisionalWeight === WEIGHTS.IMMEDIATE && !classifyCtx.answerDisposition) {
      weight = WEIGHTS.IMMEDIATE;
    }
    if (
      opts.provisionalWeight === WEIGHTS.REASONING &&
      !classifyCtx.answerDisposition &&
      weight === WEIGHTS.STANDARD
    ) {
      weight = WEIGHTS.REASONING;
    }

    const profile = buildInteractionProfile(weight, {
      reducedMotion: opts.reducedMotion,
      clientName: opts.clientName,
      hasEvidencePath: opts.hasEvidencePath,
      synthesis: opts.synthesis,
      thinkingLabel: opts.thinkingLabel,
      jitterSeed: opts.jitterSeed,
      labelSeed: opts.labelSeed,
    });

    if (profile.weight === WEIGHTS.MAJOR && opts.skipMajorReveal) {
      return {
        profile,
        remainingDelayMs: 0,
        reveal: null,
        skipped: true,
        response,
      };
    }

    const remainingDelayMs = computeRemainingDelayMs(
      startedAtMs,
      profile,
      opts.nowMs != null ? opts.nowMs : Date.now()
    );

    if (typeof opts.onProfile === 'function') opts.onProfile(profile);

    if (remainingDelayMs > 0) {
      await sleep(remainingDelayMs, opts.signal);
    }

    if (typeof opts.clearThinking === 'function') opts.clearThinking();

    let reveal = null;
    if (typeof opts.setText === 'function') {
      reveal = await revealTextProgressively({
        text: opts.prose || '',
        setText: opts.setText,
        profile,
        signal: opts.signal,
        onTick: opts.onRevealTick,
        reducedMotion: opts.reducedMotion,
      });
    }

    if (typeof opts.onComplete === 'function') {
      await opts.onComplete({ profile, remainingDelayMs, reveal, response });
    }

    return { profile, remainingDelayMs, reveal, response };
  }

  /**
   * Schedule thinking UI after a short delay unless aborted / already cleared.
   */
  function scheduleThinking(options) {
    const opts = options || {};
    const delay = Math.max(0, Number(opts.delayMs) || 0);
    let cleared = false;
    let timer = null;
    let el = null;

    function clear() {
      cleared = true;
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      if (typeof opts.remove === 'function' && el) opts.remove(el);
      else clearThinkingElement(el);
      el = null;
    }

    function show() {
      if (cleared) return;
      if (typeof opts.mount === 'function') {
        el = opts.mount(opts.label);
      } else if (typeof opts.create === 'function') {
        el = opts.create(opts.label);
      }
    }

    if (delay === 0) show();
    else {
      timer = setTimeout(show, delay);
    }

    return { clear, getElement: () => el };
  }

  return {
    WEIGHTS,
    MAX_ARTIFICIAL_DELAY_MS,
    PROFILE_BASE,
    THINKING_LABELS,
    classifyInteractionWeight,
    buildInteractionProfile,
    selectThinkingLabel,
    computeRemainingDelayMs,
    sleep,
    chunkTextForReveal,
    revealTextProgressively,
    createThinkingElement,
    clearThinkingElement,
    createScrollFollow,
    presentCompletedResponse,
    scheduleThinking,
    prefersReducedMotion,
  };
});
