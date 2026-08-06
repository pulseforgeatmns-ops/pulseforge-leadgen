'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
} = require('../services/clientIntelligenceInterview');
const {
  SECTION_PROVENANCE,
  createPlaybookFromApprovedBlueprint,
} = require('../services/clientIntelligencePlaybookHandoff');
const {
  ClientPlaybookStore,
} = require('../packages/capabilities/playbook/ClientPlaybookStore');
const { PLAYBOOK_STATUS } = require('../packages/capabilities/playbook/types');

const ANSWERS = [
  'Aji Home Services',
  'Residential cleaning',
  'Homeowners',
  'Warehouses',
  'Myrtle Beach',
  'Reliable crews',
  'Friendly',
  'More appointments',
  'Booked jobs',
];

describe('clientIntelligenceHandoff', () => {
  it('maps understanding only and keeps strategy fields empty', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const started = await startClientInterview({ clientId: 100 }, opts);
    for (const a of ANSWERS) {
      await postInterviewMessage(started.interviewId, a, opts);
    }
    const detail = await require('../services/clientIntelligenceInterview').getInterview(
      started.interviewId,
      opts
    );
    const handoff = await createPlaybookFromApprovedBlueprint(detail.blueprint, {
      playbookStore: new ClientPlaybookStore({ seed: false }),
    });
    assert.equal(handoff.playbook.status, PLAYBOOK_STATUS.PENDING_REVIEW);
    assert.deepEqual(handoff.playbook.preferredChannels, []);
    assert.deepEqual(handoff.playbook.offers, []);
    assert.deepEqual(handoff.playbook.outreachSequence, []);
    assert.ok(handoff.playbook.valuePropositions.length || handoff.playbook.notes);
    assert.deepEqual(handoff.sectionProvenance, SECTION_PROVENANCE);
  });

  it('every generated understanding field is traceable to blueprint sections', () => {
    for (const [field, sources] of Object.entries(SECTION_PROVENANCE)) {
      if (['preferredChannels', 'outreachSequence', 'offers', 'constraints'].includes(field)) {
        assert.deepEqual(sources, []);
        continue;
      }
      assert.ok(
        Array.isArray(sources) && sources.length >= 1,
        `${field} must be traceable to one or more Business Blueprint sections`
      );
    }
  });

  it('approve path does not activate playbook', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const started = await startClientInterview({ clientId: 101 }, opts);
    let turn = started;
    for (const a of ANSWERS) {
      turn = await postInterviewMessage(started.interviewId, a, opts);
    }
    const result = await approveBlueprint(turn.blueprint.id, opts);
    assert.equal(result.playbook.status, 'pending_review');
    assert.notEqual(result.playbook.status, 'active');
  });
});
