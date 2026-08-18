'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  COGNITIVE_MODES,
  classifyCognitiveMode,
  forbidsSpecialistDelegation,
} = require('../packages/max/specialistDelegation/CognitiveMode');
const {
  UNKNOWN_ANSWER,
  mayCreateDelegation,
  mayEnterSpecialistPath,
  shouldInvokeSpecialist,
} = require('../packages/max/specialistDelegation/RetrievalGate');
const {
  looksLikeAcquisitionQuestion,
  assessScoutNeed,
} = require('../packages/max/scoutAcquisition/NeedAssessment');
const { shouldDelegateToPaige } = require('../services/maxPaigeCampaignDelegation');

const NEVER_DELEGATE = [
  'What do you understand about our service area?',
  'What do you currently understand about our service area?',
  'What do you know about Anchor?',
  "Why didn't you elevate Acquisition?",
  'What did Scout investigate?',
  'What are you uncertain about?',
];

const SHOULD_INVESTIGATE = [
  'Find commercial cleaning opportunities.',
  'Investigate property managers.',
  'Research competitors.',
  'Look for expansion signals.',
];

describe('SPEC-102 cognitive mode classification', () => {
  it('classifies retrieval, explanation, and reflection acceptance questions', () => {
    assert.equal(
      classifyCognitiveMode('What do you understand about our service area?').kind,
      COGNITIVE_MODES.RETRIEVAL
    );
    assert.equal(
      classifyCognitiveMode('What do you know about Anchor?').kind,
      COGNITIVE_MODES.RETRIEVAL
    );
    assert.equal(
      classifyCognitiveMode("Why didn't you elevate Acquisition?").kind,
      COGNITIVE_MODES.EXPLANATION
    );
    assert.equal(
      classifyCognitiveMode('What did Scout investigate?').kind,
      COGNITIVE_MODES.RETRIEVAL
    );
    assert.equal(
      classifyCognitiveMode('What are you uncertain about?').kind,
      COGNITIVE_MODES.UNKNOWN_ANALYSIS
    );
  });

  it('classifies investigation acceptance questions', () => {
    for (const question of SHOULD_INVESTIGATE) {
      const mode = classifyCognitiveMode(question);
      assert.equal(mode.kind, COGNITIVE_MODES.INVESTIGATION, question);
      assert.equal(mode.explicitInvestigation, true, question);
    }
  });

  it('does not treat a topic word as an investigation', () => {
    const mode = classifyCognitiveMode('Cleaning', {
      context: { acquisitionLoop: true, lastScoutEvaluation: { id: 'ev-1' } },
    });
    assert.equal(mode.kind, COGNITIVE_MODES.RETRIEVAL);
    assert.equal(mode.via, 'conversation_continuity');
    assert.equal(forbidsSpecialistDelegation(mode), true);
  });

  it('classifies recommendation without treating it as investigation', () => {
    const mode = classifyCognitiveMode('Should we target property managers?');
    assert.equal(mode.kind, COGNITIVE_MODES.RECOMMENDATION);
    assert.equal(mayCreateDelegation(mode, { question: 'Should we target property managers?' }), false);
  });
});

describe('SPEC-102 retrieval-before-delegation gate', () => {
  it('never creates a specialist delegation for retrieval/explanation/reflection', () => {
    for (const question of NEVER_DELEGATE) {
      assert.equal(shouldInvokeSpecialist(question), false, question);
      assert.equal(
        mayEnterSpecialistPath(classifyCognitiveMode(question), { question }),
        false,
        question
      );
    }
  });

  it('allows a specialist for explicit investigation', () => {
    for (const question of SHOULD_INVESTIGATE) {
      assert.equal(shouldInvokeSpecialist(question), true, question);
    }
  });

  it('keeps the unknown phrase specialist-free', () => {
    assert.equal(UNKNOWN_ANSWER, "I don't currently know.");
    assert.doesNotMatch(UNKNOWN_ANSWER, /scout/i);
  });
});

describe('SPEC-102 Scout need assessment', () => {
  const sticky = {
    acquisitionLoop: true,
    lastScoutEvaluation: { id: 'ev-1', materialChange: false },
    domainId: 'acquisition',
  };

  it('does not treat service-area recall as Scout work even after a Scout turn', () => {
    assert.equal(
      looksLikeAcquisitionQuestion(
        'What do you currently understand about our service area?',
        sticky
      ),
      false
    );
    const need = assessScoutNeed({
      question: 'What do you currently understand about our service area?',
      context: sticky,
      existingIntelligence: { sufficient: false },
    });
    assert.equal(need.needed, false);
    assert.notEqual(need.kind, 'investigate');
  });

  it('does not treat Anchor recall or uncertainty as Scout work', () => {
    assert.equal(looksLikeAcquisitionQuestion('What do you know about Anchor?', sticky), false);
    assert.equal(looksLikeAcquisitionQuestion('What are you uncertain about?', sticky), false);
    assert.equal(
      assessScoutNeed({
        question: 'What do you know about Anchor?',
        context: sticky,
      }).needed,
      false
    );
  });

  it('still recognizes explicit commercial-cleaning investigation', () => {
    assert.equal(
      looksLikeAcquisitionQuestion('Find commercial cleaning opportunities.'),
      true
    );
    const need = assessScoutNeed({
      question: 'Find commercial cleaning opportunities.',
      context: { domainId: 'acquisition' },
      existingIntelligence: { sufficient: false },
    });
    assert.equal(need.needed, true);
  });
});

describe('SPEC-102 Paige gate', () => {
  it('does not delegate retrieval questions to Paige', () => {
    assert.equal(
      shouldDelegateToPaige('What do you know about Anchor?', {
        resolvedObjective: { title: 'Public Max Launch' },
        campaignId: 'campaign-1',
      }),
      false
    );
  });
});
