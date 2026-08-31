'use strict';

/**
 * SPEC-212 — Prospect-Bound Messaging Integrity
 *
 * Regression test for cross-prospect message contamination (AUDIT-089).
 *
 * Scenario: Two targets with different intelligence
 * - A-Z Properties (Priority 1, has specific rationale)
 * - Peloquin Property Management (Priority 2, has specific rationale)
 *
 * Requirements:
 * 1. Paige generates separate variants for each prospect
 * 2. Each variant contains only that prospect's intelligence
 * 3. Variants are bound to candidateId
 * 4. Emmett assigns correct variant to each candidate
 * 5. No cross-prospect contamination in final messages
 * 6. Binding validation passes when correct
 * 7. Binding validation fails when mismatched (fail closed)
 */

const test = require('ava');
const {
  MESSAGE_BINDING_SCOPES,
  BINDING_VALIDATION_RESULTS,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
} = require('../types');
const {
  validateProspectMessageBindings,
} = require('../ExecutionApproval');
const {
  buildPerProspectVariants,
} = require('../../max/workspace/PaigeVariantsExecutor');
const {
  findBoundVariant,
} = require('../../max/workspace/EmmettMissionCandidates');

// Regression test scenario setup
function buildRegressionScenario() {
  return {
    max: {
      rankedTargets: [
        {
          id: 'az-props-1',
          companyId: 'az-props-1',
          name: 'A-Z Properties',
          rank: 1,
          fit: 0.85,
          timing: 0.8,
          rationale: 'A-Z Properties — Priority 1, strong office management signals',
          reason: 'A-Z Properties — Priority 1, strong office management signals',
        },
        {
          id: 'peloquin-1',
          companyId: 'peloquin-1',
          name: 'Peloquin Property Management',
          rank: 2,
          fit: 0.78,
          timing: 0.7,
          rationale: 'Peloquin expansion phase, high receptivity window',
          reason: 'Peloquin expansion phase, high receptivity window',
        },
      ],
      objectives: [
        {
          text: 'Establish commercial cleaning partnership for office operations',
        },
      ],
      objectiveReason: 'Multi-location office opportunities with pending budget cycles',
    },
    scout: {
      companies: [
        { id: 'az-props-1', name: 'A-Z Properties' },
        { id: 'peloquin-1', name: 'Peloquin Property Management' },
      ],
      buyingSignals: ['Recent office lease signing'],
    },
    plan: {
      market: {
        label: 'local offices',
        segment: 'professional_services',
      },
      objective: 'Establish commercial cleaning partnership for office operations',
    },
  };
}

test('SPEC-212.1: Paige generates per-prospect variants with candidateId binding', (t) => {
  const scenario = buildRegressionScenario();

  // Generate per-prospect variants
  const variants = buildPerProspectVariants(scenario);

  // Assert: Multiple variants generated (one per prospect)
  t.is(variants.length, 2, 'Should generate 2 variants for 2 prospects');

  // Assert: Each variant has candidateId binding
  t.truthy(variants[0].candidateId, 'First variant must have candidateId');
  t.truthy(variants[1].candidateId, 'Second variant must have candidateId');

  // Assert: Variant bindings are distinct
  t.not(
    variants[0].candidateId,
    variants[1].candidateId,
    'Each variant should bind to different candidateId'
  );
});

test('SPEC-212.2: Peloquin message does NOT contain A-Z-specific intelligence', (t) => {
  const scenario = buildRegressionScenario();
  const variants = buildPerProspectVariants(scenario);

  // Find Peloquin variant
  const peloquinVariant = variants.find(
    (v) => v.candidateId === 'peloquin-1' || String(v.companyName).includes('Peloquin')
  );
  t.truthy(peloquinVariant, 'Should find Peloquin variant');

  // Assert: Peloquin message contains Peloquin-specific info
  t.true(
    peloquinVariant.body.includes('Peloquin') || peloquinVariant.subject.includes('Peloquin'),
    'Peloquin message should contain "Peloquin"'
  );

  // Assert: Peloquin message does NOT contain A-Z Properties
  t.false(
    peloquinVariant.body.includes('A-Z Properties'),
    'Peloquin message MUST NOT contain "A-Z Properties"'
  );

  // Assert: Peloquin message does NOT contain A-Z-specific reasoning
  t.false(
    peloquinVariant.body.includes('Priority 1'),
    'Peloquin message MUST NOT contain A-Z Priority ranking'
  );
});

test('SPEC-212.3: A-Z message contains A-Z-specific intelligence', (t) => {
  const scenario = buildRegressionScenario();
  const variants = buildPerProspectVariants(scenario);

  // Find A-Z variant
  const azVariant = variants.find(
    (v) => v.candidateId === 'az-props-1' || String(v.companyName).includes('A-Z')
  );
  t.truthy(azVariant, 'Should find A-Z variant');

  // Assert: A-Z message contains A-Z company name
  t.true(
    azVariant.subject.includes('A-Z Properties') || azVariant.body.includes('A-Z'),
    'A-Z message should contain "A-Z Properties"'
  );
});

test('SPEC-212.4: Each variant has explicit bindingScope marking', (t) => {
  const scenario = buildRegressionScenario();
  const variants = buildPerProspectVariants(scenario);

  for (const variant of variants) {
    t.is(
      variant.bindingScope,
      MESSAGE_BINDING_SCOPES.PROSPECT,
      `Variant ${variant.candidateId} must have bindingScope='prospect'`
    );
  }
});

test('SPEC-212.5: Each variant has attributable intelligence attached', (t) => {
  const scenario = buildRegressionScenario();
  const variants = buildPerProspectVariants(scenario);

  for (const variant of variants) {
    t.truthy(
      variant.attributableIntelligence,
      `Variant ${variant.candidateId} must have attributableIntelligence`
    );
    t.truthy(
      variant.attributableIntelligence.rationale,
      `Variant ${variant.candidateId} intelligence must include rationale`
    );
    t.truthy(
      variant.attributableIntelligence.companyName,
      `Variant ${variant.candidateId} intelligence must include companyName`
    );
  }
});

test('SPEC-212.6: findBoundVariant retrieves correct variant by candidateId', (t) => {
  const scenario = buildRegressionScenario();
  const variants = buildPerProspectVariants(scenario);

  // Simulate Emmett candidate matching
  const azVariant = findBoundVariant(variants, 'az-props-1');
  const peloquinVariant = findBoundVariant(variants, 'peloquin-1');

  t.is(azVariant.candidateId, 'az-props-1', 'Should retrieve A-Z variant');
  t.is(peloquinVariant.candidateId, 'peloquin-1', 'Should retrieve Peloquin variant');
  t.not(azVariant, peloquinVariant, 'Should retrieve different variants');
});

test('SPEC-212.7: Message binding validation PASSES with correct candidateId matching', (t) => {
  // Simulate Emmett queue with correct bindings
  const emmettPayload = {
    queue: {
      items: [
        {
          id: 'az-props-1',
          prospectId: 'prospect-az-1',
          company: 'A-Z Properties',
          paige: {
            candidateId: 'az-props-1',
            variantId: 'paige_v_az_props_1',
            bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
            subject: 'Commercial cleaning walkthrough for A-Z Properties',
            body: 'A-Z Properties specific message',
            attributableIntelligence: {
              rationale: 'A-Z Properties — Priority 1',
              companyName: 'A-Z Properties',
            },
          },
        },
        {
          id: 'peloquin-1',
          prospectId: 'prospect-peloquin-1',
          company: 'Peloquin Property Management',
          paige: {
            candidateId: 'peloquin-1',
            variantId: 'paige_v_peloquin_1',
            bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
            subject: 'Commercial cleaning walkthrough for Peloquin Property Management',
            body: 'Peloquin specific message',
            attributableIntelligence: {
              rationale: 'Peloquin expansion phase',
              companyName: 'Peloquin Property Management',
            },
          },
        },
      ],
    },
  };

  const validation = validateProspectMessageBindings(emmettPayload);

  t.true(validation.valid, 'Validation should pass with correct bindings');
  t.is(
    validation.result,
    BINDING_VALIDATION_RESULTS.VALID,
    'Result should be VALID'
  );
  t.is(validation.violations.length, 0, 'Should have no violations');
});

test('SPEC-212.8: Message binding validation FAILS on candidateId mismatch (fail closed)', (t) => {
  // Simulate Emmett queue with CONTAMINATED bindings (mismatch)
  const emmettPayload = {
    queue: {
      items: [
        {
          id: 'peloquin-1', // This is Peloquin
          prospectId: 'prospect-peloquin-1',
          company: 'Peloquin Property Management',
          paige: {
            // But message is bound to A-Z!
            candidateId: 'az-props-1',
            variantId: 'paige_v_az_props_1',
            subject: 'Commercial cleaning walkthrough for A-Z Properties',
            body: 'A-Z specific message (CONTAMINATION)',
          },
        },
      ],
    },
  };

  const validation = validateProspectMessageBindings(emmettPayload);

  t.false(validation.valid, 'Validation should FAIL with mismatched bindings');
  t.is(
    validation.result,
    BINDING_VALIDATION_RESULTS.CONTAMINATED,
    'Result should be CONTAMINATED'
  );
  t.true(
    validation.violations.length > 0,
    'Should report violations'
  );
  t.is(
    validation.violations[0].reason,
    'candidate_id_mismatch',
    'Should identify mismatch as reason'
  );
});

test('SPEC-212.9: Message binding validation FAILS when message lacks binding', (t) => {
  const emmettPayload = {
    queue: {
      items: [
        {
          id: 'az-props-1',
          prospectId: 'prospect-az-1',
          company: 'A-Z Properties',
          // Missing paige binding
          paige: null,
        },
      ],
    },
  };

  const validation = validateProspectMessageBindings(emmettPayload);

  t.false(validation.valid, 'Validation should fail when binding is missing');
  t.true(
    validation.violations.length > 0,
    'Should report missing binding violation'
  );
  t.is(
    validation.violations[0].reason,
    'missing_message_binding',
    'Should identify missing binding'
  );
});

test('SPEC-212.10: Full scenario validation', (t) => {
  // This test simulates the complete Paige → Emmett → Approval flow
  const scenario = buildRegressionScenario();

  // Step 1: Paige generates per-prospect variants
  const variants = buildPerProspectVariants(scenario);
  t.is(variants.length, 2, 'Paige should generate 2 variants');

  // Step 2: Emmett builds queue candidates with correct variant assignments
  const emmettQueue = {
    items: variants.map((variant, idx) => ({
      id: variant.candidateId,
      company: variant.companyName,
      prospectId: `prospect-${idx}`,
      paige: {
        candidateId: variant.candidateId,
        variantId: variant.variantId,
        bindingScope: variant.bindingScope,
        subject: variant.subject,
        body: variant.body,
        attributableIntelligence: variant.attributableIntelligence,
      },
    })),
  };

  // Step 3: ExecutionApproval validates bindings
  const validation = validateProspectMessageBindings({ queue: emmettQueue });

  t.true(validation.valid, 'Complete flow should pass validation');
  t.is(validation.result, BINDING_VALIDATION_RESULTS.VALID);

  // Step 4: Verify no cross-prospect contamination
  const peloquinItem = emmettQueue.items.find((i) => i.id === 'peloquin-1');
  t.false(
    peloquinItem.paige.body.includes('A-Z'),
    'Peloquin message should not contain A-Z reference'
  );
});
