'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  REASONING_MARKER,
  looksLikeReasoningRequest,
  buildReasoningEvidence,
  formatReasoningDisclosure,
  buildMissionCommunication,
  formatMissionProse,
  buildUnknownsMissionCommunication,
  buildEngineMissionCommunication,
} = require('../packages/max/workspace/MissionCommunication');

const { formatUnknownsAnswer } = require('../packages/max/workspace/ClientIntelligenceContext');
const { PresentationEngine, formatDeterministicProse } = require('../packages/max/workspace/PresentationEngine');
const { composeMissionResponse } = require('../packages/max/workspace/MissionResponse');

const SAMPLE_SUMMARY = {
  approved: true,
  identity: 'AS Cleaning Co.',
  idealCustomers: 'property and facility managers',
  geography: 'Greater Toronto Area',
  campaignGoals: 'adding new clients',
  successMetrics: 'walkthroughs and conversion rate',
  unknowns: ['Which commercial segment will respond first'],
};

describe('SPEC-121 MissionCommunication', () => {
  it('detects explicit reasoning requests', () => {
    assert.equal(looksLikeReasoningRequest('Show your reasoning'), true);
    assert.equal(looksLikeReasoningRequest('Why are we doing this?'), true);
    assert.equal(looksLikeReasoningRequest('Where are we?'), false);
  });

  it('formats mission prose with mission-first hierarchy', () => {
    const comm = buildMissionCommunication({
      headline: 'Mission Created',
      mission: 'Commercial STR Acquisition',
      status: 'Planning',
      stage: 'Discovery',
      progress: 10,
      confidence: 0.84,
      currentUnderstanding: [
        { label: 'Target customer defined', done: true },
        { label: 'Geography defined', done: true },
      ],
      nextStep: 'Scout will identify high-probability operators.',
      operatorDecision: 'Approve discovery?',
      reasoningEvidence: buildReasoningEvidence({
        known: ['Target beachhead approved in Blueprint.'],
        inference: ['STR operators are the strongest next experiment.'],
        unknown: ['Expected walkthrough rate'],
        evidenceNeeded: ['Live campaign performance data'],
        confidence: 0.84,
      }),
    });

    const prose = formatMissionProse(comm);
    assert.match(prose, /Mission Created/);
    assert.match(prose, /Commercial STR Acquisition/);
    assert.match(prose, /Status/);
    assert.match(prose, /Planning/);
    assert.match(prose, /Stage/);
    assert.match(prose, /Discovery/);
    assert.match(prose, /Progress/);
    assert.match(prose, /10%/);
    assert.match(prose, /Next Step/);
    assert.match(prose, /Operator Decision/);
    assert.match(prose, /Approve discovery/);
    assert.match(prose, new RegExp(REASONING_MARKER));
    assert.doesNotMatch(prose, /^Known/m);
    assert.doesNotMatch(prose, /INFERENCE:/);
  });

  it('expands reasoning when explicitly requested', () => {
    const comm = buildUnknownsMissionCommunication(SAMPLE_SUMMARY);
    const prose = formatMissionProse(comm, { explicitReasoningRequest: true });
    assert.match(prose, /Known/);
    assert.match(prose, /Inference/);
    assert.match(prose, /Unknown/);
    assert.match(prose, /Evidence Needed/);
    assert.doesNotMatch(prose, /KNOWN from your approved Blueprint/i);
  });

  it('buildUnknownsMissionCommunication avoids default cognitive labels in primary prose', () => {
    const comm = buildUnknownsMissionCommunication(SAMPLE_SUMMARY);
    const prose = formatMissionProse(comm);
    assert.match(prose, /Mission Updated/);
    assert.match(prose, /Current Understanding/);
    assert.match(prose, /Next Step/);
    assert.match(prose, /Operator Decision/);
    assert.doesNotMatch(prose, /KNOWN from your approved Blueprint/i);
    assert.doesNotMatch(prose, /INFERENCE \(Max reasoning/i);
    assert.doesNotMatch(prose, /^UNKNOWN:/m);
    assert.doesNotMatch(prose, /^EVIDENCE NEEDED:/m);
  });

  it('formatUnknownsAnswer uses mission-oriented communication', () => {
    const prose = formatUnknownsAnswer(SAMPLE_SUMMARY);
    assert.match(prose, /Mission Updated/);
    assert.match(prose, /Discovery/);
    assert.doesNotMatch(prose, /KNOWN from your approved Blueprint/i);
  });

  it('formatReasoningDisclosure preserves internal reasoning tiers', () => {
    const text = formatReasoningDisclosure(
      buildReasoningEvidence({
        known: ['Blueprint defines beachhead.'],
        inference: ['Commercial motion is reasonable.'],
        unknown: ['Segment response rate'],
        evidenceNeeded: ['Campaign evidence'],
        confidence: 0.84,
      })
    );
    assert.match(text, /Known/);
    assert.match(text, /Inference/);
    assert.match(text, /Unknown/);
    assert.match(text, /Evidence Needed/);
    assert.match(text, /0\.84/);
  });

  it('PresentationEngine passes through mission communication without appending Reasoning bullets', () => {
    const comm = buildUnknownsMissionCommunication(SAMPLE_SUMMARY);
    const structured = {
      answer: formatMissionProse(comm),
      reasoning: ['should not appear'],
      metadata: {
        missionCommunication: true,
        strictOutputShape: true,
        showReasoningDisclosure: true,
        reasoningEvidence: comm.reasoningEvidence,
      },
    };
    const engine = new PresentationEngine({ disableLlm: true });
    return engine.present(structured).then((result) => {
      assert.equal(result.presentation, 'mission_communication');
      assert.doesNotMatch(result.prose, /^Reasoning:/m);
      assert.match(result.prose, /Mission Updated/);
    });
  });

  it('composeMissionResponse leads with Mission Created structure', () => {
    const structured = composeMissionResponse({
      mission: {
        id: 'm1',
        title: 'Commercial STR Acquisition',
        type: 'acquisition',
        status: 'planning',
        progress: { currentStage: 'Discovery', percent: 10, totalSteps: 5, completedSteps: 0 },
        confidence: 0.84,
      },
      question: 'Acquire one recurring commercial cleaning client',
    });
    assert.equal(structured.metadata.missionCommunication, true);
    assert.match(structured.answer, /Mission Created/);
    assert.match(structured.answer, /Commercial STR Acquisition/);
    assert.match(structured.answer, /Next Step/);
    assert.doesNotMatch(structured.answer, /Mission created:/i);
  });

  it('buildEngineMissionCommunication maps blocked missions', () => {
    const comm = buildEngineMissionCommunication(
      {
        title: 'Test Mission',
        status: 'waiting',
        type: 'acquisition',
        progress: { currentStage: 'Discovery', percent: 22 },
        blockingIssues: ['Missing prospect list'],
      },
      { created: false }
    );
    assert.match(formatMissionProse(comm), /Mission Waiting/);
    assert.match(formatMissionProse(comm), /22%/);
  });
});

describe('SPEC-121 formatDeterministicProse mission mode', () => {
  it('does not append Reasoning section for missionCommunication metadata', () => {
    const prose = formatDeterministicProse({
      answer: 'Mission Updated\n\nStatus\n\nPlanning',
      reasoning: ['internal only'],
      metadata: { missionCommunication: true, strictOutputShape: true },
    });
    assert.equal(prose, 'Mission Updated\n\nStatus\n\nPlanning');
  });
});
