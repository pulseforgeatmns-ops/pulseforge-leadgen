/**
 * SPEC-238A: Structural Mixed-Clause Isolation
 * 
 * Verify that control clauses are isolated from propositions using
 * structural clause detection, not just punctuation heuristics.
 */

'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  reviewCorrectionOperations,
  projectWorkingSemanticOperations,
  EPISTEMIC_STATES,
} = require('../services/clientIntelligenceInterview');

describe('SPEC-238A: Structural Mixed-Clause Isolation', () => {
  
  // ============================================================================
  // CASE 1: Conjunction + Control Clause ("and do not")
  // ============================================================================
  
  describe('CASE 1 — Conjunction + Control Clause ("and do not")', () => {
    it('Should isolate proposition before "and do not approve"', () => {
      const correction = `Change the differentiation hypothesis to: Babrun's practical 12-week model may be more compelling than open-ended consulting and do not approve the Blueprint.`;
      const prior = {
        normalizedFacts: {
          differentiation: null,
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-238a-1');
      
      // Should have differentiation correction
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert(diffOps.length > 0, 'Should generate differentiation correction');
      
      const op = diffOps[0];
      assert(
        /12-week|practical|compelling|open-ended consulting/.test(op.value),
        'Should contain proposition content'
      );
      assert(
        !/and do not|approve|blueprint/i.test(op.value),
        `Should NOT contain control clause. Got: "${op.value}"`
      );
      assert.strictEqual(op.epistemic_state, EPISTEMIC_STATES.HYPOTHESIS, 'Should be HYPOTHESIS');
    });
  });

  // ============================================================================
  // CASE 2: Conjunction + Control ("but keep")
  // ============================================================================
  
  describe('CASE 2 — Conjunction + Control ("but keep")', () => {
    it('Should isolate proposition before "but keep brand voice unchanged"', () => {
      const correction = `Change the differentiation hypothesis to: Babrun's structured 12-week operating model may outperform indefinite advisory work, but keep brand voice unchanged.`;
      const prior = {
        normalizedFacts: {
          differentiation: null,
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-238a-2');
      
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert(diffOps.length > 0, 'Should generate differentiation correction');
      
      const op = diffOps[0];
      assert(
        /12-week|operating|outperform|indefinite advisory/.test(op.value),
        'Should contain proposition content'
      );
      assert(
        !/but keep|brand voice|unchanged/i.test(op.value),
        `Should NOT contain control clause. Got: "${op.value}"`
      );
    });
  });

  // ============================================================================
  // CASE 3: Semicolon + Control Clause
  // ============================================================================
  
  describe('CASE 3 — Semicolon + Control Clause', () => {
    it('Should isolate proposition before semicolon + control', () => {
      const correction = `Change the differentiation hypothesis to: Babrun gives owners a practical operating transformation; preserve the existing epistemic status.`;
      const prior = {
        normalizedFacts: {
          differentiation: null,
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-238a-3');
      
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert(diffOps.length > 0, 'Should generate differentiation correction');
      
      const op = diffOps[0];
      assert(
        /practical operating transformation/.test(op.value),
        'Should contain proposition content'
      );
      assert(
        !/preserve|epistemic/i.test(op.value),
        `Should NOT contain control clause. Got: "${op.value}"`
      );
    });
  });

  // ============================================================================
  // CASE 4: Legitimate "and" Inside Proposition (NOT a control clause)
  // ============================================================================
  
  describe('CASE 4 — Legitimate Conjunction Inside Proposition', () => {
    it('Should NOT truncate at "and" when followed by proposition content', () => {
      const correction = `Change the differentiation hypothesis to: Babrun helps owners preserve what already works and change the management behaviors that keep them trapped.`;
      const prior = {
        normalizedFacts: {
          differentiation: null,
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-238a-4');
      
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert(diffOps.length > 0, 'Should generate differentiation correction');
      
      const op = diffOps[0];
      // The word "and" here joins two proposition clauses, not a control clause
      // "and change" is content, not control
      assert(
        /preserve|change|management behaviors|trapped/i.test(op.value),
        `Should preserve full proposition including "and change". Got: "${op.value}"`
      );
      assert(
        op.value.length > 50,  // Should be substantial
        'Should contain both proposition parts'
      );
    });
  });

  // ============================================================================
  // CASE 5: Legitimate "Preserve" Inside Proposition (NOT control language)
  // ============================================================================
  
  describe('CASE 5 — Legitimate "Preserve" Inside Proposition', () => {
    it('Should NOT strip "preserve" when it appears in proposition content', () => {
      const correction = `Change the differentiation hypothesis to: We help owners preserve what already works while changing the management habits that keep them trapped in daily operations. Keep that as a hypothesis.`;
      const prior = {
        normalizedFacts: {
          differentiation: null,
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-238a-5');
      
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert(diffOps.length > 0, 'Should generate differentiation correction');
      
      const op = diffOps[0];
      // "preserve" is part of the value proposition (preserve what works)
      // NOT a control directive
      assert(
        /preserve.*what.*works/i.test(op.value),
        `Value should contain "preserve what works". Got: "${op.value}"`
      );
      assert(
        !/keep that as a hypothesis/i.test(op.value),
        'Should strip the trailing epistemic control directive'
      );
    });
  });

  // ============================================================================
  // CASE 6: Actual Correction Preserved (Complete Example)
  // ============================================================================
  
  describe('CASE 6 — Actual Correction Preserved', () => {
    it('Should preserve substantive proposition without truncation', () => {
      const correction = `Change the differentiation hypothesis to: Babrun's practical model may be more compelling than open-ended consulting.`;
      const prior = {
        normalizedFacts: {
          differentiation: 'unknown',
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-238a-6');
      
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert(diffOps.length > 0, 'Should generate differentiation correction');
      
      const op = diffOps[0];
      assert(
        /practical.*compelling.*open-ended/.test(op.value),
        `Value should be complete. Got: "${op.value}"`
      );
    });
  });

  // ============================================================================
  // CASE 7: Exact AUDIT-119 Message Remains Safe
  // ============================================================================
  
  describe('CASE 7 — Exact AUDIT-119 Message Remains Safe', () => {
    it('Pure refinement message should not generate contaminated operations', () => {
      const AUDIT_119_MESSAGE = `Please regenerate the Executive Business Brief using the current corrected understanding of Babrun. Do not add, infer, or change any business facts. Preserve the current epistemic status of all facts and hypotheses. Keep geography non-applicable, preserve differentiation as a hypothesis rather than an established buying reason, do not invent a brand voice if it is currently unknown, preserve the operator-defined success metrics separately from Max's recommended scorecard metrics, and present customer exclusions naturally without duplicating or reframing their meaning. This is a refinement only; do not approve the Blueprint.`;
      
      const prior = {
        normalizedFacts: {
          differentiation: 'practical transformation-focused 12-week approach',
          epistemic_states: { differentiation: EPISTEMIC_STATES.HYPOTHESIS },
        },
      };
      
      const operations = reviewCorrectionOperations(AUDIT_119_MESSAGE, prior, 'turn-audit-119');
      
      const diffOps = operations.filter(op => op.slot === 'differentiation');
      if (diffOps.length > 0) {
        for (const op of diffOps) {
          assert(
            !op.value || !/(?:do not|preserve|hypothesis|rather than|separate)/i.test(op.value),
            `Should not contain control language. Got: "${op.value}"`
          );
        }
      }
    });
  });

  // ============================================================================
  // CASE 8: Pure Epistemic Directive Remains Non-Fabricating
  // ============================================================================
  
  describe('CASE 8 — Pure Epistemic Directive Non-Fabricating', () => {
    it('Should not fabricate proposition from epistemic-only directive', () => {
      const epistemic = `Keep differentiation as a hypothesis.`;
      const prior = {
        normalizedFacts: {
          differentiation: 'practical transformation-focused approach',
          epistemic_states: { differentiation: EPISTEMIC_STATES.KNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(epistemic, prior, 'turn-238a-8');
      
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert.strictEqual(diffOps.length, 0, 'Should not fabricate proposition');
    });
  });

  // ============================================================================
  // CASE 9: Brand Voice Control Remains Non-Fabricating
  // ============================================================================
  
  describe('CASE 9 — Brand Voice Control Non-Fabricating', () => {
    it('Should not create brand_voice from control directive', () => {
      const control = `Do not invent a brand voice.`;
      const prior = {
        normalizedFacts: {
          brand_voice: null,
          epistemic_states: { brand_voice: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(control, prior, 'turn-238a-9');
      
      const brandOps = operations.filter(op => op.slot === 'brand_voice');
      assert.strictEqual(brandOps.length, 0, 'Should not generate brand_voice operation');
    });
  });

  // ============================================================================
  // CASE 10: Customer Presentation Control Remains Non-Fabricating
  // ============================================================================
  
  describe('CASE 10 — Customer Presentation Control Non-Fabricating', () => {
    it('Should not generate customer operations from presentation directive', () => {
      const presentation = `Present customer exclusions naturally without duplicating them.`;
      const prior = {
        normalizedFacts: {
          disqualified_customers: [],
          ideal_customers: [],
        },
      };
      
      const operations = reviewCorrectionOperations(presentation, prior, 'turn-238a-10');
      
      const customerOps = operations.filter(op => 
        ['disqualified_customers', 'ideal_customers', 'ideal_customer_traits'].includes(op.slot)
      );
      assert.strictEqual(customerOps.length, 0, 'Should not generate customer operations');
    });
  });

  // ============================================================================
  // Regression: SPEC-238 Core Tests Still Pass
  // ============================================================================
  
  describe('Regression — SPEC-238 Core Tests', () => {
    it('AUDIT-119 message generates no differentiation operation', () => {
      const AUDIT_119_MESSAGE = `Please regenerate the Executive Business Brief using the current corrected understanding of Babrun. Do not add, infer, or change any business facts. Preserve the current epistemic status of all facts and hypotheses. Keep geography non-applicable, preserve differentiation as a hypothesis rather than an established buying reason, do not invent a brand voice if it is currently unknown, preserve the operator-defined success metrics separately from Max's recommended scorecard metrics, and present customer exclusions naturally without duplicating or reframing their meaning. This is a refinement only; do not approve the Blueprint.`;
      
      const prior = {
        normalizedFacts: {
          differentiation: 'practical transformation-focused 12-week approach',
        },
      };
      
      const operations = reviewCorrectionOperations(AUDIT_119_MESSAGE, prior, 'turn-audit-119');
      
      // Should have NO contaminated differentiation operations
      const diffOps = operations.filter(op => op.slot === 'differentiation');
      assert.strictEqual(diffOps.length, 0, 'Should not generate operations for pure control message');
    });

    it('Mixed correction + control should isolate value', () => {
      const mixed = `Change the differentiation hypothesis to: Babrun's practical 12-week model may be more compelling than open-ended consulting. Keep it as a hypothesis and do not change brand voice.`;
      const prior = {
        normalizedFacts: {
          differentiation: null,
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
          brand_voice: null,
        },
      };
      
      const operations = reviewCorrectionOperations(mixed, prior, 'turn-mixed');
      
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert(diffOps.length > 0, 'Should generate differentiation correction');
      
      const op = diffOps[0];
      assert(
        /12-week|practical|compelling|open-ended consulting/.test(op.value),
        'Should contain proposition'
      );
      assert(
        !/do not|brand voice|hypothesis/.test(op.value),
        'Should not contain control language'
      );
      
      const brandOps = operations.filter(op => op.slot === 'brand_voice');
      assert.strictEqual(brandOps.length, 0, 'Should not generate brand_voice operation');
    });

    it('Actual semantic correction should preserve epistemic state', () => {
      const correction = `Change the differentiation hypothesis to: Babrun's 12-week practical transformation model may be more compelling than open-ended consulting.`;
      const prior = {
        normalizedFacts: {
          differentiation: 'unknown',
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-prop');
      
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert(diffOps.length > 0, 'Should generate correction');
      assert.strictEqual(diffOps[0].epistemic_state, EPISTEMIC_STATES.HYPOTHESIS, 'Should preserve HYPOTHESIS state');
    });
  });

  // ============================================================================
  // Regression: SPEC-230 Gate Still Works
  // ============================================================================
  
  describe('Regression — SPEC-230 Semantic Composition', () => {
    it('Multiple semantic operations should compose correctly', () => {
      const correction = `Change the differentiation hypothesis to: Babrun's practical 12-week model may be more compelling. The primary offer is practical transformation consulting.`;
      const prior = {
        normalizedFacts: {
          differentiation: null,
          services: [],
          pains: [],
          epistemic_states: {
            differentiation: EPISTEMIC_STATES.UNKNOWN,
          },
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-230');
      
      // Should generate multiple independent operations
      const diffOps = operations.filter(op => op.slot === 'differentiation');
      const serviceOps = operations.filter(op => op.slot === 'services');
      
      assert(diffOps.length > 0, 'Should generate differentiation operation');
      assert(serviceOps.length > 0, 'Should generate services operation');
    });
  });

  // ============================================================================
  // Edge Cases
  // ============================================================================
  
  describe('Edge Cases', () => {
    it('Multiple control clauses should truncate at first', () => {
      const multiControl = `Change the differentiation hypothesis to: Babrun's practical 12-week model and do not approve but keep brand voice unchanged.`;
      const prior = {
        normalizedFacts: {
          differentiation: null,
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(multiControl, prior, 'turn-multi');
      
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert(diffOps.length > 0, 'Should generate differentiation correction');
      
      const op = diffOps[0];
      assert(
        /12-week|practical/.test(op.value),
        'Should truncate at first control clause'
      );
      assert(
        !/and do not|approve|brand voice/i.test(op.value),
        'Should not contain control clauses'
      );
    });

    it('Comma without control keyword should not truncate', () => {
      const correction = `Change the differentiation hypothesis to: Babrun's practical 12-week model, which is more compelling than open-ended consulting.`;
      const prior = {
        normalizedFacts: {
          differentiation: null,
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-comma');
      
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert(diffOps.length > 0, 'Should generate differentiation correction');
      
      const op = diffOps[0];
      // The comma here is NOT a control boundary (no control keyword after)
      assert(
        /12-week|practical|compelling|open-ended/.test(op.value),
        `Should preserve content after comma when no control keyword. Got: "${op.value}"`
      );
    });

    it('Control keyword without clause boundary should not truncate', () => {
      const correction = `Change the differentiation hypothesis to: Babrun helps owners preserve their existing business while changing management habits.`;
      const prior = {
        normalizedFacts: {
          differentiation: null,
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-preserve');
      
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert(diffOps.length > 0, 'Should generate differentiation correction');
      
      const op = diffOps[0];
      // "preserve" here is NOT a control keyword context (no clause boundary before it)
      assert(
        /preserve.*business|changing.*habits/i.test(op.value),
        `Should preserve content containing "preserve". Got: "${op.value}"`
      );
    });
  });
});
