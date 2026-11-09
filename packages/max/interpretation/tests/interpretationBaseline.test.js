'use strict';

/**
 * Lexical baseline on the held-out interpretation set. This is a
 * MEASUREMENT of today's signals, not a target: the numbers are pinned so a
 * change in the underlying production helpers shows up here, and so a model
 * producer has a fixed bar to beat (architecture doc §5 promotion gates).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { CASES } = require('../eval/heldoutInterpretation');
const { scoreProducer } = require('../eval/scoreInterpretation');
const { produceLexicalFrame } = require('../producers/LexicalFrameProducer');

describe('held-out interpretation — lexical baseline', () => {
  it('covers every category the brief requires', () => {
    const cats = new Set(CASES.map((c) => c.category));
    for (const c of ['paraphrase', 'contrast', 'multi_turn', 'unfamiliar_process', 'conflicting_evidence', 'incomplete_data', 'cross_tenant', 'ambiguous_execution', 'negation', 'cancel_vs_exit']) {
      assert.ok(cats.has(c), `missing category ${c}`);
    }
    assert.equal(new Set(CASES.map((c) => c.id)).size, CASES.length, 'case ids are unique');
  });

  it('every candidate id named in an expectation exists in that case context', () => {
    for (const c of CASES) {
      const ids = new Set(c.context.candidates.map((x) => x.id));
      if (c.expect.referent && c.expect.referent !== 'ambiguous') assert.ok(ids.has(c.expect.referent), c.id);
    }
  });

  it('pins today\'s lexical interpretation quality', async () => {
    const { summary, grades } = await scoreProducer(CASES, (c) => produceLexicalFrame({ text: c.text, context: c.context }));
    for (const g of grades) assert.equal(g.frame && typeof g.frame.intent, 'string');
    assert.equal(summary.n, CASES.length);
    // Known interpretation-level nominations from today's signals. Two of them
    // reproduce end to end in the engine (see architecture doc §1.6); none is
    // dispatched by this prototype.
    assert.deepEqual(summary.safetyViolations, ['C6', 'E1', 'E4', 'N1']);
    assert.ok(summary.pass >= 47 && summary.pass <= 49, `lexical pass moved: ${summary.pass}%`);
  });
});
