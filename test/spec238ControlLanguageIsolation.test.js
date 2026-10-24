/**
 * SPEC-238: Control-Language Isolation
 * 
 * Prevent refinement/control instructions from being extracted as business-semantic
 * replacement values by reviewCorrectionOperations().
 */

'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  reviewCorrectionOperations,
  projectWorkingSemanticOperations,
  EPISTEMIC_STATES,
} = require('../services/clientIntelligenceInterview');

describe('SPEC-238: Control-Language Isolation', () => {
  
  // ============================================================================
  // CASE 1: Pure Refinement Control Message (AUDIT-119)
  // ============================================================================
  
  describe('CASE 1 — Pure Refinement Control Message (AUDIT-119)', () => {
    const AUDIT_119_MESSAGE = `Please regenerate the Executive Business Brief using the current corrected understanding of Babrun. Do not add, infer, or change any business facts. Preserve the current epistemic status of all facts and hypotheses. Keep geography non-applicable, preserve differentiation as a hypothesis rather than an established buying reason, do not invent a brand voice if it is currently unknown, preserve the operator-defined success metrics separately from Max's recommended scorecard metrics, and present customer exclusions naturally without duplicating or reframing their meaning. This is a refinement only; do not approve the Blueprint.`;
    
    it('Should NOT generate differentiation correction containing control-language', () => {
      const prior = {
        normalizedFacts: {
          differentiation: 'practical transformation-focused 12-week approach',
          epistemic_states: { differentiation: EPISTEMIC_STATES.HYPOTHESIS },
          hypotheses: { differentiation: 'practical transformation-focused 12-week approach' },
        },
      };
      
      const operations = reviewCorrectionOperations(AUDIT_119_MESSAGE, prior, 'turn-audit-119');
      
      // Should not have a CORRECT operation for differentiation with contaminated control language
      const diffOps = operations.filter(op => op.slot === 'differentiation');
      
      // SPEC-238 requirement: control message must NOT generate contaminated operation
      if (diffOps.length > 0) {
        // If there are differentiation operations, they must not contain control language
        for (const op of diffOps) {
          assert(
            !op.value || !/(?:do not|do not invent|preserve|rather than|separate|separately from)/i.test(op.value),
            `Contaminated control language in differentiation value: "${op.value}"`
          );
        }
      }
    });
    
    it('Should NOT generate brand_voice operation from control directive', () => {
      const prior = {
        normalizedFacts: {
          brand_voice: null,
          epistemic_states: { brand_voice: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(AUDIT_119_MESSAGE, prior, 'turn-audit-119');
      
      // Should not have brand_voice operation
      const brandOps = operations.filter(op => op.slot === 'brand_voice');
      assert.strictEqual(brandOps.length, 0, 'Pure control directive should not generate brand_voice operation');
    });
    
    it('Should NOT generate customer exclusion operations from presentation control', () => {
      const prior = {
        normalizedFacts: {
          disqualified_customers: [],
          ideal_customers: [],
        },
      };
      
      const operations = reviewCorrectionOperations(AUDIT_119_MESSAGE, prior, 'turn-audit-119');
      
      // Should not have customer operations
      const customerOps = operations.filter(op => 
        ['disqualified_customers', 'ideal_customers', 'ideal_customer_traits'].includes(op.slot)
      );
      assert.strictEqual(customerOps.length, 0, 'Presentation control should not generate customer operations');
    });
    
    it('Persisted state should remain unchanged after pure refinement message', () => {
      const prior = {
        normalizedFacts: {
          differentiation: 'practical transformation-focused 12-week approach',
          brand_voice: null,
          disqualified_customers: [],
          ideal_customers: [],
          epistemic_states: {
            differentiation: EPISTEMIC_STATES.HYPOTHESIS,
            brand_voice: EPISTEMIC_STATES.UNKNOWN,
          },
          hypotheses: {
            differentiation: 'practical transformation-focused 12-week approach',
          },
          evidence_statements: {},
          superseded_slots: [],
        },
      };
      
      const operations = reviewCorrectionOperations(AUDIT_119_MESSAGE, prior, 'turn-audit-119');
      const next = projectWorkingSemanticOperations(prior.normalizedFacts, operations);
      
      // Verify state unchanged
      assert.strictEqual(
        next.differentiation,
        prior.normalizedFacts.differentiation,
        'differentiation should remain unchanged'
      );
      assert.strictEqual(
        next.brand_voice,
        prior.normalizedFacts.brand_voice,
        'brand_voice should remain unchanged'
      );
    });
  });

  // ============================================================================
  // CASE 2: Actual Semantic Correction
  // ============================================================================
  
  describe('CASE 2 — Actual Semantic Correction', () => {
    it('Should generate valid semantic correction to differentiation', () => {
      const correction = `Change the differentiation hypothesis to: Babrun's 12-week practical transformation model may be more compelling than open-ended consulting.`;
      const prior = {
        normalizedFacts: {
          differentiation: 'unknown',
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-test');
      
      // Should have a valid differentiation correction
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert(diffOps.length > 0, 'Should generate differentiation correction');
      
      const op = diffOps[0];
      assert(op.value, 'Should have a value');
      assert(
        /12-week|practical transformation|compelling|open-ended consulting/.test(op.value),
        `Value should contain proposition content, got: "${op.value}"`
      );
    });
  });

  // ============================================================================
  // CASE 3: Mixed Correction + Control
  // ============================================================================
  
  describe('CASE 3 — Mixed Correction + Control', () => {
    it('Should isolate proposition value from control language', () => {
      const mixed = `Change the differentiation hypothesis to: Babrun's practical 12-week model may be more compelling than open-ended consulting. Keep it as a hypothesis and do not change brand voice.`;
      const prior = {
        normalizedFacts: {
          differentiation: null,
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
          brand_voice: null,
        },
      };
      
      const operations = reviewCorrectionOperations(mixed, prior, 'turn-test');
      
      // Should have differentiation correction
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert(diffOps.length > 0, 'Should generate differentiation correction');
      
      const diffOp = diffOps[0];
      assert(
        /12-week|practical|compelling|open-ended consulting/.test(diffOp.value),
        'Should contain actual proposition'
      );
      assert(
        !/do not change|brand voice/.test(diffOp.value),
        'Should not contain control language'
      );
      assert.strictEqual(diffOp.epistemic_state, EPISTEMIC_STATES.HYPOTHESIS, 'Should be HYPOTHESIS');
      
      // Should NOT have brand_voice operation
      const brandOps = operations.filter(op => op.slot === 'brand_voice');
      assert.strictEqual(brandOps.length, 0, 'Should not generate brand_voice operation from control language');
    });
  });

  // ============================================================================
  // CASE 4: Epistemic-Only Directive
  // ============================================================================
  
  describe('CASE 4 — Epistemic-Only Directive', () => {
    it('Should NOT fabricate replacement proposition from epistemic directive', () => {
      const epistemic = `Keep differentiation as a hypothesis.`;
      const prior = {
        normalizedFacts: {
          differentiation: 'practical transformation-focused approach',
          epistemic_states: { differentiation: EPISTEMIC_STATES.KNOWN },
          hypotheses: {},
        },
      };
      
      const operations = reviewCorrectionOperations(epistemic, prior, 'turn-test');
      
      // Should NOT generate a CORRECT operation with fabricated value
      const diffOps = operations.filter(op => op.slot === 'differentiation' && op.operation === 'CORRECT');
      assert.strictEqual(diffOps.length, 0, 'Should not fabricate proposition from epistemic directive');
    });
    
    it('If no proposition exists, should not invent one', () => {
      const epistemic = `Preserve differentiation as a hypothesis.`;
      const prior = {
        normalizedFacts: {
          differentiation: null,
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(epistemic, prior, 'turn-test');
      
      // Should not have fabricated differentiation
      const diffOps = operations.filter(op => op.slot === 'differentiation');
      assert.strictEqual(diffOps.length, 0, 'Should not fabricate proposition when none exists');
    });
  });

  // ============================================================================
  // CASE 5: Negative Control
  // ============================================================================
  
  describe('CASE 5 — Negative Control', () => {
    it('Should NOT create brand_voice operation from "do not invent" directive', () => {
      const negative = `Do not invent a brand voice.`;
      const prior = {
        normalizedFacts: {
          brand_voice: null,
          epistemic_states: { brand_voice: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(negative, prior, 'turn-test');
      
      // Must not become: brand_voice = "do not invent a brand voice"
      const brandOps = operations.filter(op => op.slot === 'brand_voice');
      assert.strictEqual(brandOps.length, 0, 'Should not create brand_voice from control directive');
    });
  });

  // ============================================================================
  // CASE 6: Presentation Control
  // ============================================================================
  
  describe('CASE 6 — Presentation Control', () => {
    it('Should NOT generate customer operations from presentation-only directive', () => {
      const presentation = `Present customer exclusions naturally without duplicating them.`;
      const prior = {
        normalizedFacts: {
          disqualified_customers: [],
          ideal_customers: [],
          ideal_customer_traits: [],
        },
      };
      
      const operations = reviewCorrectionOperations(presentation, prior, 'turn-test');
      
      // Must not generate operations unless message has actual customer content
      const customerOps = operations.filter(op => 
        ['disqualified_customers', 'ideal_customers', 'ideal_customer_traits'].includes(op.slot)
      );
      assert.strictEqual(customerOps.length, 0, 'Should not generate customer operations from presentation control');
    });
  });

  // ============================================================================
  // Regression Tests
  // ============================================================================
  
  describe('Regression Tests', () => {
    it('SPEC-230: Semantic composition gate unchanged', () => {
      // Verify that mixed semantic operations still compose correctly
      const correction = `Change the differentiation hypothesis to: Babrun's practical 12-week model may be more compelling. Preserve it as a hypothesis.`;
      const prior = {
        normalizedFacts: {
          differentiation: null,
          epistemic_states: { differentiation: EPISTEMIC_STATES.UNKNOWN },
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-test');
      
      // Should still generate valid operations that compose
      const diffOps = operations.filter(op => op.slot === 'differentiation');
      assert(diffOps.length > 0, 'Should generate differentiation operations');
      assert.strictEqual(
        diffOps[0].epistemic_state,
        EPISTEMIC_STATES.HYPOTHESIS,
        'Should preserve hypothesis state'
      );
    });
    
    it('SPEC-226: Existing correction patterns still work', () => {
      // Verify that non-control-language corrections still work
      const correction = `Premium positioning never established validated.`;
      const prior = {
        normalizedFacts: {
          differentiation: 'premium positioning',
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-test');
      
      // Should still detect and retract premium positioning
      const retracts = operations.filter(op => op.operation === 'RETRACT' && op.slot === 'differentiation');
      assert(retracts.length > 0, 'Should detect retraction patterns');
    });
    
    it('Actual proposition corrections should still work', () => {
      // Verify that real business corrections are not affected
      const correction = `The primary offer is affordable, scalable lead generation and prospecting for small business owners.`;
      const prior = {
        normalizedFacts: {
          services: ['unknown'],
        },
      };
      
      const operations = reviewCorrectionOperations(correction, prior, 'turn-test');
      
      // Should generate services correction
      const serviceOps = operations.filter(op => op.slot === 'services' && op.operation === 'CORRECT');
      assert(serviceOps.length > 0, 'Should generate services correction');
      assert(serviceOps[0].value, 'Should have value');
    });
  });

  // ============================================================================
  // Validation: Core Invariants
  // ============================================================================
  
  describe('Core Invariants', () => {
    it('Control language is never business value', () => {
      const controlKeywords = [
        'do not add', 'do not infer', 'do not change',
        'preserve', 'keep', 'do not invent', 'do not approve',
        'do not duplicate', 'do not reframe', 'regenerate', 'separate'
      ];
      
      const testMessages = [
        'Do not add any facts.',
        'Preserve geography non-applicable.',
        'Keep it as a hypothesis.',
        'Do not invent a brand voice.',
        'Do not approve the Blueprint.',
      ];
      
      for (const message of testMessages) {
        const operations = reviewCorrectionOperations(message, { normalizedFacts: {} }, 'turn-test');
        
        for (const op of operations) {
          if (op.value && typeof op.value === 'string') {
            for (const keyword of controlKeywords) {
              assert(
                !op.value.toLowerCase().includes(keyword),
                `Control keyword "${keyword}" leaked into operation value for: "${message}"`
              );
            }
          }
        }
      }
    });
    
    it('Proposition distinction is between proposition-bearing and control clauses', () => {
      // Test that we distinguish between:
      // - "differentiation is X" with epistemic keywords (proposition)
      // - "Do not change the differentiation" (control)
      
      const proposition = `The differentiation is a hypothesis: Babrun delivers practical 12-week transformations.`;
      const control = `Do not change the differentiation.`;
      
      const propOps = reviewCorrectionOperations(proposition, { normalizedFacts: {} }, 'turn-prop');
      const ctrlOps = reviewCorrectionOperations(control, { normalizedFacts: {} }, 'turn-ctrl');
      
      const propDiff = propOps.filter(op => op.slot === 'differentiation' && op.value && op.operation === 'CORRECT');
      const ctrlDiff = ctrlOps.filter(op => op.slot === 'differentiation' && op.value && op.operation === 'CORRECT');
      
      assert(propDiff.length > 0, 'Proposition with epistemic keywords should generate value');
      assert.strictEqual(ctrlDiff.length, 0, 'Pure control should not generate value');
    });
  });
});
