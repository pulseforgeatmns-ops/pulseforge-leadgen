'use strict';

/**
 * SPEC-102 — Max natural interaction cadence & progressive response reveal.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const Cadence = require('../public/shared/maxInteractionCadence');

const {
  WEIGHTS,
  MAX_ARTIFICIAL_DELAY_MS,
  classifyInteractionWeight,
  buildInteractionProfile,
  selectThinkingLabel,
  computeRemainingDelayMs,
  chunkTextForReveal,
  revealTextProgressively,
  presentCompletedResponse,
  scheduleThinking,
  createScrollFollow,
  prefersReducedMotion,
} = Cadence;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('SPEC-102 Max interaction cadence', () => {
  describe('TEST A — Fast computation / reasoning turn', () => {
    it('applies bounded minimum cadence and does not materialize instantly', async () => {
      const startedAtMs = Date.now();
      const frames = [];
      let thinkingVisible = true;

      const result = await presentCompletedResponse({
        startedAtMs,
        prose:
          "That's okay. We don't need to force an answer yet. Let's reason from what we know.",
        operatorText: "I don't know who we should target.",
        provisionalWeight: WEIGHTS.REASONING,
        answerDisposition: 'ANSWER_UNCERTAIN',
        reducedMotion: false,
        jitterSeed: 0.5,
        labelSeed: 0,
        clearThinking: () => {
          thinkingVisible = false;
        },
        setText: (partial) => {
          frames.push(partial);
        },
      });

      assert.equal(result.profile.weight, WEIGHTS.REASONING);
      assert.ok(result.remainingDelayMs > 500, 'fast backend still waits for cadence');
      assert.ok(
        result.remainingDelayMs <= MAX_ARTIFICIAL_DELAY_MS,
        'artificial delay within ceiling'
      );
      assert.equal(thinkingVisible, false);
      assert.ok(frames.length > 1, 'progressive reveal begins across multiple frames');
      assert.equal(frames[frames.length - 1], result.reveal.text);
      assert.notEqual(frames[0], frames[frames.length - 1]);
    });
  });

  describe('TEST B — Slow real computation', () => {
    it('adds no additional minimum delay when work already exceeded cadence', () => {
      const profile = buildInteractionProfile(WEIGHTS.REASONING, {
        reducedMotion: true,
        jitterSeed: 0.5,
      });
      const startedAtMs = Date.now() - 5000;
      const remaining = computeRemainingDelayMs(startedAtMs, profile, Date.now());
      assert.equal(remaining, 0);
    });

    it('presentCompletedResponse begins reveal immediately after slow work', async () => {
      const startedAtMs = Date.now() - 8000;
      const t0 = Date.now();
      const result = await presentCompletedResponse({
        startedAtMs,
        prose: 'Here is the synthesis.',
        operatorText: 'What should we focus on first?',
        provisionalWeight: WEIGHTS.REASONING,
        reducedMotion: true,
        jitterSeed: 0.5,
        setText: () => {},
        clearThinking: () => {},
      });
      const waited = Date.now() - t0;
      assert.equal(result.remainingDelayMs, 0);
      assert.ok(waited < 200, 'no theatrical post-computation wait');
    });
  });

  describe('TEST C — Simple turn', () => {
    it('classifies accepted CIE answers as immediate / short profile', () => {
      const weight = classifyInteractionWeight({
        surface: 'cie',
        phase: 'discovery',
        operatorText: 'Greater Toronto Area.',
        answerDisposition: 'ANSWER_ACCEPTED',
      });
      assert.equal(weight, WEIGHTS.IMMEDIATE);
      const profile = buildInteractionProfile(weight, {
        reducedMotion: false,
        jitterSeed: 0.5,
      });
      assert.ok(profile.minimumPerceivedMs < 1200);
      assert.ok(profile.minimumPerceivedMs >= 200);
    });

    it('classifies short navigation commands as immediate', () => {
      assert.equal(
        classifyInteractionWeight({ operatorText: 'Show me the Blueprint.' }),
        WEIGHTS.IMMEDIATE
      );
      assert.equal(
        classifyInteractionWeight({ operatorText: 'Open the pipeline.' }),
        WEIGHTS.IMMEDIATE
      );
      assert.equal(
        classifyInteractionWeight({ operatorText: 'Yes.' }),
        WEIGHTS.IMMEDIATE
      );
    });
  });

  describe('TEST D — SPEC-100 uncertainty', () => {
    it('uses reasoning profile and contextual thinking for ANSWER_UNCERTAIN', () => {
      const weight = classifyInteractionWeight({
        answerDisposition: 'ANSWER_UNCERTAIN',
        operatorText: "I don't know yet.",
        surface: 'cie',
      });
      assert.equal(weight, WEIGHTS.REASONING);
      const label = selectThinkingLabel(weight, {
        clientName: 'AS Cleaning',
        labelSeed: 0,
      });
      assert.match(label, /AS Cleaning/);
      assert.doesNotMatch(label, /Maybe property managers/i);
      assert.doesNotMatch(label, /Step 1/i);
      assert.doesNotMatch(label, /Analyzing token/i);
    });
  });

  describe('TEST E — Progressive reveal', () => {
    it('does not insert complete prose on first render and preserves exact text', async () => {
      const full =
        'First sentence about customers.\n\nSecond paragraph with a recommendation.';
      const frames = [];
      const result = await revealTextProgressively({
        text: full,
        profile: buildInteractionProfile(WEIGHTS.STANDARD, {
          reducedMotion: false,
          jitterSeed: 0.5,
        }),
        setText: (partial) => frames.push(partial),
      });
      assert.ok(frames.length >= 2);
      assert.notEqual(frames[0], full);
      assert.equal(frames[frames.length - 1], full);
      assert.equal(result.text, full);
      assert.ok(frames.every((f) => full.startsWith(f)));
    });

    it('chunkTextForReveal keeps word groups without character typing', () => {
      const chunks = chunkTextForReveal('one two three four five six', 3);
      assert.ok(chunks.length >= 2);
      assert.equal(chunks.join(''), 'one two three four five six');
      assert.ok(chunks[0].includes(' '));
    });
  });

  describe('TEST F — Structured actions after reveal', () => {
    it('defers onComplete (actions) until reveal finishes', async () => {
      const events = [];
      await presentCompletedResponse({
        startedAtMs: Date.now() - 10000,
        prose: 'Recommendation ready for review.',
        reducedMotion: false,
        jitterSeed: 0.5,
        provisionalWeight: WEIGHTS.IMMEDIATE,
        setText: (partial) => events.push(['text', partial]),
        clearThinking: () => events.push(['thinking_cleared']),
        onComplete: async () => {
          events.push(['actions_enabled']);
        },
      });
      const actionIdx = events.findIndex((e) => e[0] === 'actions_enabled');
      const lastTextIdx = events.map((e) => e[0]).lastIndexOf('text');
      assert.ok(actionIdx > lastTextIdx);
      assert.ok(events.some((e) => e[0] === 'thinking_cleared'));
    });
  });

  describe('TEST G — Error clears thinking and unlocks', () => {
    it('scheduleThinking clear removes the thinking state', async () => {
      const mounted = [];
      const removed = [];
      const handle = scheduleThinking({
        delayMs: 10,
        label: 'Considering your question…',
        mount: (label) => {
          const el = { label };
          mounted.push(el);
          return el;
        },
        remove: (el) => removed.push(el),
      });
      await sleep(30);
      assert.equal(mounted.length, 1);
      handle.clear();
      assert.equal(removed.length, 1);
      assert.equal(handle.getElement(), null);
    });

    it('aborted reveal restores full text path without hanging', async () => {
      const controller = new AbortController();
      const frames = [];
      const p = revealTextProgressively({
        text: 'A fairly long response that would normally take several ticks to reveal completely for the operator.',
        profile: buildInteractionProfile(WEIGHTS.REASONING, { jitterSeed: 0.5 }),
        signal: controller.signal,
        setText: (partial) => frames.push(partial),
      });
      controller.abort();
      await assert.rejects(p, (err) => err && err.name === 'AbortError');
      assert.equal(frames[frames.length - 1].length > 0, true);
    });
  });

  describe('TEST H — Scroll follow', () => {
    it('follows while near bottom and stops after scroll-up', () => {
      const container = {
        scrollHeight: 1000,
        scrollTop: 920,
        clientHeight: 80,
        listeners: {},
        addEventListener(type, fn) {
          this.listeners[type] = fn;
        },
        removeEventListener(type) {
          delete this.listeners[type];
        },
      };
      const follow = createScrollFollow(container, { threshold: 80 });
      assert.equal(follow.isSticking(), true);
      follow.follow();
      assert.equal(container.scrollTop, 1000);

      container.scrollTop = 100;
      container.listeners.scroll();
      assert.equal(follow.isSticking(), false);
      const before = container.scrollTop;
      follow.follow();
      assert.equal(container.scrollTop, before);
      follow.detach();
    });
  });

  describe('TEST I — Reduced motion', () => {
    it('reveals immediately and keeps full text accessible', async () => {
      const frames = [];
      const full = 'Accessible complete answer.';
      const result = await revealTextProgressively({
        text: full,
        reducedMotion: true,
        setText: (partial) => frames.push(partial),
      });
      assert.equal(frames.length, 1);
      assert.equal(frames[0], full);
      assert.equal(result.mode, 'immediate');
    });

    it('prefersReducedMotion reads matchMedia safely', () => {
      assert.equal(
        prefersReducedMotion(() => ({ matches: true })),
        true
      );
      assert.equal(
        prefersReducedMotion(() => ({ matches: false })),
        false
      );
    });
  });

  describe('TEST J — State integrity', () => {
    it('presentation timing does not mutate business payloads', async () => {
      const interviewState = Object.freeze({
        status: 'IN_PROGRESS',
        currentStage: 'ideal_customers',
      });
      const blueprint = Object.freeze({ version: 2, status: 'draft' });
      const evidence = Object.freeze([{ id: 'e1' }]);
      const approval = Object.freeze({ pending: true });
      const maxContext = Object.freeze({ clientId: 99 });

      const payload = {
        interviewState,
        blueprint,
        evidence,
        approval,
        maxContext,
        message: 'Reasoned reply.',
        answerDisposition: 'ANSWER_UNCERTAIN',
      };

      await presentCompletedResponse({
        startedAtMs: Date.now() - 10000,
        prose: payload.message,
        answerDisposition: payload.answerDisposition,
        reducedMotion: true,
        jitterSeed: 0.5,
        setText: () => {},
        clearThinking: () => {},
        response: payload,
      });

      assert.equal(payload.interviewState.status, 'IN_PROGRESS');
      assert.equal(payload.blueprint.version, 2);
      assert.equal(payload.evidence[0].id, 'e1');
      assert.equal(payload.approval.pending, true);
      assert.equal(payload.maxContext.clientId, 99);
      assert.equal(payload.answerDisposition, 'ANSWER_UNCERTAIN');
    });

    it('thinking labels never invent chain-of-thought', () => {
      for (const weight of Object.values(WEIGHTS)) {
        for (let i = 0; i < 5; i += 1) {
          const label = selectThinkingLabel(weight, { labelSeed: i });
          assert.doesNotMatch(label, /Maybe |I think perhaps|Step \d|Analyzing token|Crunching|Working my magic|Hang tight/i);
        }
      }
    });

    it('artificial delay is hard-capped', () => {
      const profile = { minimumPerceivedMs: 999999 };
      const remaining = computeRemainingDelayMs(Date.now(), profile, Date.now());
      assert.equal(remaining, MAX_ARTIFICIAL_DELAY_MS);
    });
  });

  describe('Surface wiring (static)', () => {
    it('CIE and Command Deck load the shared cadence module', () => {
      const cie = fs.readFileSync(
        path.join(__dirname, '..', 'public', 'client-intel.html'),
        'utf8'
      );
      const deckHtml = fs.readFileSync(
        path.join(__dirname, '..', 'public', 'command-deck.html'),
        'utf8'
      );
      const deckJs = fs.readFileSync(
        path.join(__dirname, '..', 'public', 'command-deck', 'command-deck.js'),
        'utf8'
      );
      assert.match(cie, /maxInteractionCadence\.js/);
      assert.match(cie, /presentCieAssistantMessage|MaxCadence|runCieTurn/);
      assert.match(deckHtml, /maxInteractionCadence\.js/);
      assert.match(deckJs, /presentCompletedResponse|workspaceAskInFlight|appendMaxResponse/);
      assert.match(deckJs, /mx-thinking|clearThinking/);
    });

    it('Executive Brief premium checklist path remains intact', () => {
      const cie = fs.readFileSync(
        path.join(__dirname, '..', 'public', 'client-intel.html'),
        'utf8'
      );
      assert.match(cie, /runChecklist\(LOAD_STEPS,\s*PREMIUM_LOAD_MS/);
      assert.match(cie, /enterUnderstandingFlow/);
      assert.match(cie, /GENERATE_BLUEPRINT/);
    });
  });
});
