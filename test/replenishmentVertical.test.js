'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  ENRICHABLE_SCOUT_VERTICALS,
  resolveReplenishmentVertical,
  evaluateReplenishmentAdmission,
  buildDiscoveryProvenance,
  formatProvenanceNotes,
} = require('../utils/replenishmentVertical');
const { _test: { persistDiscoveredCompanies, runEnrichmentBatches } } = require('../services/maxOutboundControlLoop');

const STR_MISSION = {
  missionSegment: 'short_term_rental',
  service_area: ['Manchester', 'Bedford', 'Goffstown', 'Hooksett', 'Londonderry', 'Auburn'],
};

test('A. canonical STR mapping from business evidence, not search phrase', () => {
  const candidate = {
    name: 'Vacation Rental Management Co',
    description: 'Full-service Airbnb and vacation rental management',
    discoveryConcept: 'short term rental management Manchester NH',
    location: 'Manchester, NH',
    website: 'https://example.com',
    domain: 'example.com',
  };
  assert.equal(resolveReplenishmentVertical(candidate, STR_MISSION), 'str_manager');
});

test('B. generic property manager maps to property_manager', () => {
  const candidate = {
    name: 'Granite State Property Management',
    description: 'Residential and commercial property management services',
    location: 'Bedford, NH',
    website: 'https://granitepm.com',
    domain: 'granitepm.com',
  };
  assert.equal(resolveReplenishmentVertical(candidate, STR_MISSION), 'property_manager');
});

test('C. search phrase does not classify unrelated equipment rental company', () => {
  const candidate = {
    name: 'NorthPoint Equipment Rentals',
    businessType: 'equipment rental',
    discoveryConcept: 'short term rental management Hooksett NH',
    location: 'Hooksett, NH',
    website: 'https://northpoint.example',
    domain: 'northpoint.example',
  };
  assert.equal(resolveReplenishmentVertical(candidate, STR_MISSION), null);

  const admission = evaluateReplenishmentAdmission(candidate, STR_MISSION);
  assert.equal(admission.admitted, false);
  assert.equal(admission.reason, 'contradictory_business_type');
});

test('D. waste-hauling contradiction rejects with contradictory_business_type', () => {
  const candidate = {
    name: 'CC Hauls Junk Removal',
    description: 'Residential junk removal and waste hauling services',
    discoveryConcept: 'Airbnb property management Manchester NH',
    location: 'Manchester, NH',
    website: 'https://cchauls.example',
    domain: 'cchauls.example',
  };
  const admission = evaluateReplenishmentAdmission(candidate, STR_MISSION);
  assert.equal(admission.admitted, false);
  assert.equal(admission.reason, 'contradictory_business_type');
});

test('E. search metadata preserved separately from canonical vertical', () => {
  const candidate = {
    name: 'Vacation Rental Management Co',
    description: 'Full-service Airbnb and vacation rental management',
    discoveryConcept: 'short term rental management Manchester NH',
    discoveryCity: 'Manchester NH',
    discoverySource: 'google_maps',
    location: 'Manchester, NH',
    website: 'https://example.com',
    domain: 'example.com',
  };
  const provenance = buildDiscoveryProvenance(candidate, STR_MISSION);
  assert.equal(provenance.discoveryConcept, 'short term rental management Manchester NH');
  assert.equal(provenance.discoveryCity, 'Manchester NH');
  assert.equal(provenance.discoverySource, 'google_maps');

  const admission = evaluateReplenishmentAdmission(candidate, STR_MISSION);
  assert.equal(admission.admitted, true);
  assert.equal(admission.vertical, 'str_manager');
  assert.notEqual(admission.vertical, 'short_term_rental_management_manchester_nh');

  const notes = formatProvenanceNotes('Base note.', provenance);
  assert.match(notes, /discoveryConcept/);
  assert.doesNotMatch(notes, /"vertical"/);
});

test('F. producer/consumer compatibility — persisted row is visible to enrichment selector', async () => {
  const insertedRows = [];
  const pool = {
    query: async (sql, params) => {
      if (/INSERT INTO scout_unenriched/i.test(sql)) {
        insertedRows.push({
          id: `row-${insertedRows.length + 1}`,
          client_id: 10,
          company: params[0],
          website_url: params[1],
          domain: params[2],
          vertical: params[3],
          location: params[4],
          source: 'max_buffer_replenishment',
          enrichment_attempts: 0,
          last_attempt_at: null,
          notes: params[5],
        });
        return { rowCount: 1, rows: [{ id: insertedRows.at(-1).id }] };
      }
      if (/FROM scout_unenriched/i.test(sql)) {
        const verticals = params[4];
        const rows = insertedRows.filter(row =>
          !verticals || verticals.includes(row.vertical)
        );
        return { rows: rows.slice(0, params[3]) };
      }
      return { rows: [], rowCount: 0 };
    },
  };
  const store = {
    candidateOwnership: async () => null,
  };

  const candidate = {
    name: 'Vacation Rental Management Co',
    description: 'Full-service Airbnb and vacation rental management',
    discoveryConcept: 'short term rental management Manchester NH',
    location: 'Manchester, NH',
    website: 'https://example.com',
    domain: 'example.com',
  };

  const persisted = await persistDiscoveredCompanies(pool, store, {
    companies: [candidate],
    scoutContext: {
      scope: { segment: 'short_term_rental' },
      serviceAreas: STR_MISSION.service_area,
    },
  });

  assert.equal(persisted.inserted, 1);
  assert.equal(insertedRows[0].vertical, 'str_manager');
  assert.notEqual(insertedRows[0].vertical, 'short_term_rental_management_manchester_nh');

  const enrichment = {
    run: async params => {
      const rows = await pool.query(`
        SELECT *
        FROM scout_unenriched
        WHERE client_id = $1
          AND COALESCE(enrichment_attempts, 0) < $2
          AND COALESCE(last_attempt_at, NOW() - INTERVAL '100 years') <= NOW() - ($3::numeric * INTERVAL '1 hour')
          AND ($5::text[] IS NULL OR vertical = ANY($5::text[]))
        ORDER BY last_attempt_at ASC, id ASC
        LIMIT $4
      `, [10, 3, 1, params.limit, params.verticals]);

      return {
        client_id: 10,
        considered: rows.rows.length,
        promoted: 0,
        unresolved: 0,
        failed: 0,
        limit: params.limit,
        retry_hours: params.retryHours,
        verticals: params.verticals,
      };
    },
  };

  const enrichmentResult = await runEnrichmentBatches(enrichment, pool, 1);
  assert.ok(enrichmentResult.summaries[0].considered >= 1);
  assert.deepEqual(enrichmentResult.summaries[0].verticals, ENRICHABLE_SCOUT_VERTICALS);
});

test('G. unknown buying readiness still admits fit candidate to enrichment', () => {
  const candidate = {
    name: 'Manchester Property Management LLC',
    description: 'Full-service property management for residential rentals',
    location: 'Manchester, NH',
    website: 'https://mpm.example',
    domain: 'mpm.example',
    signals: [],
  };
  const admission = evaluateReplenishmentAdmission(candidate, STR_MISSION);
  assert.equal(admission.admitted, true);
  assert.equal(admission.vertical, 'property_manager');
});

test('query-slug industry field is ignored for classification', () => {
  const candidate = {
    name: 'Vacation Rental Management Co',
    industry: 'short_term_rental_management_manchester_nh',
    description: 'Full-service Airbnb and vacation rental management',
    discoveryConcept: 'short term rental management Manchester NH',
    location: 'Manchester, NH',
  };
  assert.equal(resolveReplenishmentVertical(candidate, {
    discoveryConcept: 'short term rental management Manchester NH',
  }), 'str_manager');
});

test('persistDiscoveredCompanies rejects query-slug-only candidates', async () => {
  const pool = {
    query: async () => ({ rowCount: 0, rows: [] }),
  };
  const store = { candidateOwnership: async () => null };

  const result = await persistDiscoveredCompanies(pool, store, {
    companies: [{
      name: 'Mystery Business',
      industry: 'short_term_rental_management_hooksett_nh',
      discoveryConcept: 'short term rental management Hooksett NH',
      location: 'Hooksett, NH',
      website: 'https://mystery.example',
      domain: 'mystery.example',
    }],
    scoutContext: {
      scope: { segment: 'short_term_rental' },
      serviceAreas: STR_MISSION.service_area,
    },
  });

  assert.equal(result.inserted, 0);
  assert.equal(result.admission.rejected.unclassifiable_vertical, 1);
});

test('admission counters track discovered/evaluated/fit/admitted/rejected', async () => {
  const pool = {
    query: async (sql) => {
      if (/INSERT INTO scout_unenriched/i.test(sql)) {
        return { rowCount: 1, rows: [{ id: 'row-1' }] };
      }
      return { rowCount: 0, rows: [] };
    },
  };
  const store = { candidateOwnership: async () => null };

  const result = await persistDiscoveredCompanies(pool, store, {
    companies: [
      {
        name: 'Vacation Rental Management Co',
        description: 'Airbnb and vacation rental management',
        location: 'Manchester, NH',
        website: 'https://good.example',
        domain: 'good.example',
      },
      {
        name: 'NorthPoint Equipment Rentals',
        businessType: 'equipment rental',
        location: 'Hooksett, NH',
        website: 'https://bad.example',
        domain: 'bad.example',
      },
    ],
    scoutContext: {
      scope: { segment: 'short_term_rental' },
      serviceAreas: STR_MISSION.service_area,
    },
  });

  assert.equal(result.admission.discovered, 2);
  assert.equal(result.admission.evaluated, 2);
  assert.equal(result.admission.fit, 1);
  assert.equal(result.admission.admittedToEnrichment, 1);
  assert.equal(result.admission.rejected.contradictory_business_type, 1);
});
