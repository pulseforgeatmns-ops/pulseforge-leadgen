'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  assessScoutNeed,
  isOutboundInventoryReplenishment,
  runAcquisitionIntelligenceLoop,
  createMemoryAcquisitionState,
  ANCHOR_TENANT_ID,
} = require('../services/scoutAcquisitionIntelligence');
const {
  createMemoryStore,
  createSpecialistDelegationService,
} = require('../services/specialistDelegation');

function replenishmentInput(overrides = {}) {
  return {
    authorizedTenantId: ANCHOR_TENANT_ID,
    tenantId: ANCHOR_TENANT_ID,
    workflow: 'outbound_inventory_replenishment',
    inventoryDeficit: 15,
    question:
      'Max needs Scout to replenish verified outbound inventory for short_term_rental in Greater Manchester.',
    objective:
      'Find enough net-new, in-scope prospects to close an outbound inventory deficit of 15 while preserving ownership, prior-contact, DNC and suppression boundaries.',
    reason: 'Reusable inventory is insufficient for the outbound buffer deficit.',
    authority: 'observe',
    force: true,
    businessContext: {
      serviceGeography: 'Greater Manchester',
      commercialCapability: 'commercial_cleaning',
      preferredSegments: ['short_term_rental'],
    },
    targetContext: {
      geography: 'Greater Manchester',
      segments: ['short_term_rental'],
      businessType: 'commercial_cleaning',
    },
    operatorDirection:
      'Maintain verified inventory ahead of governed outbound demand. Do not contact prospects.',
    ...overrides,
  };
}

function loopOpts(store, aoStore, extras = {}) {
  const service = createSpecialistDelegationService({ store });
  return {
    delegationService: service,
    aoStore,
    companies: extras.companies || [],
    people: extras.people || [],
    discover: extras.discover || (async () => []),
    loadCompanies: extras.loadCompanies,
    persistCompanies: extras.persistCompanies,
    enablePlaces: extras.enablePlaces !== false,
    placesProvider: extras.placesProvider,
  };
}

describe('Max-directed outbound inventory replenishment', () => {
  /** @type {ReturnType<typeof createMemoryStore>} */
  let store;
  /** @type {ReturnType<typeof createMemoryAcquisitionState>} */
  let aoStore;

  beforeEach(() => {
    store = createMemoryStore();
    aoStore = createMemoryAcquisitionState();
  });

  it('detects replenishment workflow from structured fields', () => {
    assert.equal(
      isOutboundInventoryReplenishment({
        workflow: 'outbound_inventory_replenishment',
        inventoryDeficit: 15,
      }),
      true
    );
    assert.equal(
      isOutboundInventoryReplenishment({
        workflow: 'outbound_inventory_replenishment',
        inventoryDeficit: 0,
      }),
      false
    );
  });

  it('forces fresh discovery when reusable inventory is insufficient', async () => {
    const need = assessScoutNeed({
      ...replenishmentInput(),
      existingIntelligence: {
        sufficient: false,
        counts: { considered: 87, matched: 0, rejected: 87 },
      },
    });
    assert.equal(need.needed, true);
    assert.equal(need.kind, 'investigate');
    assert.notEqual(need.kind, 'unrelated');

    let discoveryRan = false;
    const result = await runAcquisitionIntelligenceLoop(
      replenishmentInput(),
      loopOpts(store, aoStore, {
        loadCompanies: async () =>
          Array.from({ length: 87 }, (_, i) => ({
            id: `co-${i}`,
            tenantId: ANCHOR_TENANT_ID,
            name: `Company ${i}`,
            industry: 'short_term_rental',
            location: null,
          })),
        discover: async () => {
          discoveryRan = true;
          return [
            {
              name: 'Fresh STR Manager',
              website: 'https://fresh-str.example',
              industry: 'str_manager',
              location: 'Manchester, NH',
              source: 'places',
            },
          ];
        },
        persistCompanies: async () => ({ inserted: 1 }),
      })
    );

    assert.equal(discoveryRan, true);
    assert.equal(result.delegated, true);
    assert.notEqual(result.kind, 'unrelated');
    assert.equal(result.outboundInvoked.length, 0);
  });

  it('does not force discovery for unrelated non-replenishment requests with zero reuse', () => {
    const need = assessScoutNeed({
      question: 'Draft a LinkedIn post about reviews.',
      existingIntelligence: {
        sufficient: false,
        counts: { considered: 87, matched: 0, rejected: 87 },
      },
    });
    assert.equal(need.needed, false);
    assert.equal(need.kind, 'unrelated');
  });

  it('does not let matching intelligence or a completed search satisfy a clean-inventory deficit', () => {
    const input = replenishmentInput();
    const evidence = {
      existingIntelligence: { sufficient: true, counts: { matched: 27 } },
      recentResults: [{
        status: 'completed',
        completedAt: new Date().toISOString(),
        objective: input.objective,
        evidenceRefs: [{ id: 'recent-search' }],
      }],
    };
    const need = assessScoutNeed({ ...input, ...evidence });
    assert.equal(need.needed, true);
    assert.equal(need.kind, 'investigate');
    const satisfied = assessScoutNeed({ ...input, ...evidence, inventoryDeficit: 0 });
    assert.equal(satisfied.needed, false);
  });

  it('reaches discovery and admission on consecutive deficit cycles despite reusable company stock', async () => {
    let discoveryCalls = 0;
    let admissionCalls = 0;
    const options = loopOpts(store, aoStore, {
      loadCompanies: async () => Array.from({ length: 27 }, (_, i) => ({
        id: `existing-${i}`, tenantId: ANCHOR_TENANT_ID,
        name: `Existing Property Manager ${i}`, industry: 'property_management',
        location: 'Manchester, NH', website: `https://existing-${i}.example`,
        updatedAt: new Date().toISOString(),
      })),
      discover: async () => {
        discoveryCalls += 1;
        return [{ name: 'New STR Manager', website: 'https://new-str.example',
          industry: 'str_manager', location: 'Bedford, NH', source: 'places' }];
      },
      persistCompanies: async () => { admissionCalls += 1; return { inserted: 1 }; },
    });
    for (let cycle = 0; cycle < 2; cycle += 1) {
      const before = discoveryCalls;
      const result = await runAcquisitionIntelligenceLoop(replenishmentInput(), options);
      assert.equal(result.delegated, true);
      assert.ok(discoveryCalls > before);
      assert.equal(result.delegation.authority, 'observe');
      assert.deepEqual(result.outboundInvoked, []);
    }
    assert.ok(admissionCalls >= 2);
  });

  it('preserves governance: observe authority and no outbound contact from Scout', async () => {
    const result = await runAcquisitionIntelligenceLoop(
      replenishmentInput({ authority: 'observe' }),
      loopOpts(store, aoStore, {
        loadCompanies: async () => [],
        discover: async () => [
          {
            name: 'Governed STR Co',
            website: 'https://governed-str.example',
            industry: 'str_manager',
            location: 'Bedford, NH',
          },
        ],
      })
    );

    assert.equal(result.delegated, true);
    assert.equal(result.delegation.authority, 'observe');
    assert.equal(result.outboundInvoked.length, 0);
  });
});
