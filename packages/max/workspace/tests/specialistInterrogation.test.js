'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  maybeHandleSpecialistInterrogationTurn,
} = require('../SpecialistInterrogationContext');
const {
  createMemoryStore,
  createSpecialistDelegationService,
} = require('../../../../services/specialistDelegation');
const {
  runAcquisitionIntelligenceLoop,
  createMemoryAcquisitionState,
} = require('../../../../services/scoutAcquisitionIntelligence');

const GEO_QUESTION =
  "Why couldn't Scout resolve the geography? What geographic information did you give him for this investigation?";

describe('SPEC-101 Max workspace specialist interrogation', () => {
  let store;
  let service;
  let aoStore;
  let session;

  beforeEach(() => {
    store = createMemoryStore();
    service = createSpecialistDelegationService({ store });
    aoStore = createMemoryAcquisitionState();
    session = {
      id: 'sess-101',
      context: {
        tenantId: '10',
        page: 'command-deck',
        domainId: 'acquisition',
        clientIntelligence: {
          businessName: 'Anchor Cleaning',
          geography: 'Manchester, NH',
        },
      },
    };
  });

  async function seedScout() {
    return runAcquisitionIntelligenceLoop(
      {
        authorizedTenantId: '10',
        question: 'Max, where should we be looking for commercial cleaning opportunities right now?',
        objective: 'Find commercial cleaning opportunities.',
        reason: 'Need current market intelligence.',
        businessContext: {
          serviceGeography: 'Manchester, NH',
          commercialCapability: 'commercial_cleaning',
          preferredSegments: ['property_management'],
        },
        targetContext: {
          geography: 'Manchester, NH',
          segments: ['property_management'],
          businessType: 'commercial_cleaning',
        },
        context: session.context,
      },
      {
        delegationService: service,
        aoStore,
        companies: [],
      }
    );
  }

  it('answers a geography interrogation from the trace without rerunning Scout', async () => {
    await seedScout();
    const before = (await store.listDelegations({ tenantId: '10' })).length;
    const turn = await maybeHandleSpecialistInterrogationTurn({
      question: GEO_QUESTION,
      session,
      context: session.context,
      delegationService: service,
      delegationOpts: { store },
    });
    assert.ok(turn);
    assert.equal(turn.interrogation.rerun, false);
    assert.match(turn.prose, /Manchester|did not include|supplied|can't tell|don't have enough/i);
    assert.doesNotMatch(
      turn.prose,
      /couldn't construct a candidate universe because geography couldn't be resolved/i
    );
    const after = (await store.listDelegations({ tenantId: '10' })).length;
    assert.equal(after, before);
  });

  it('states the limitation when Paige has no equivalent trace', async () => {
    const turn = await maybeHandleSpecialistInterrogationTurn({
      question: 'Why did Paige recommend this direction?',
      session,
      context: { tenantId: '10' },
      delegationService: service,
      delegationOpts: { store },
    });
    assert.ok(turn);
    assert.match(turn.prose, /don't have an inspectable cognitive trace for paige/i);
    assert.equal(turn.interrogation.rerun, false);
  });

  it('does not claim a new-investigation question as interrogation', async () => {
    const turn = await maybeHandleSpecialistInterrogationTurn({
      question: 'Find commercial cleaning opportunities.',
      session,
      context: session.context,
      delegationService: service,
      delegationOpts: { store },
    });
    assert.equal(turn, null);
  });

  it('WorkspaceEngine includes the interrogation seam before Scout routing', () => {
    const engineSrc = fs.readFileSync(
      path.join(__dirname, '..', 'WorkspaceEngine.js'),
      'utf8'
    );
    const interrogateAt = engineSrc.indexOf(
      'const interrogationTurn = await maybeHandleSpecialistInterrogationTurn'
    );
    const retrieveAt = engineSrc.indexOf(
      'const retrievalTurn = await maybeHandleRetrievalBeforeDelegationTurn'
    );
    const scoutAt = engineSrc.indexOf('await maybeHandleScoutAcquisitionTurn');
    assert.ok(interrogateAt > 0);
    assert.ok(retrieveAt > interrogateAt);
    assert.ok(scoutAt > retrieveAt);
  });
});
