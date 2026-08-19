'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {
  CANONICAL_BUSINESS_VERTICALS,
  listCanonicalBusinessVerticals,
  isCanonicalBusinessVertical,
  labelForBusinessVertical,
  unsupportedVerticalMessage,
  assertCanonicalBusinessVertical,
  mapVerticalConstraintError,
} = require('../utils/canonicalVerticals');
const { validateCreateClientInput } = require('../services/tenantWorkspace');

describe('PEC-116 canonical business verticals', () => {
  it('exposes a stable registry with labels', () => {
    assert.ok(CANONICAL_BUSINESS_VERTICALS.length >= 10);
    assert.ok(CANONICAL_BUSINESS_VERTICALS.some((v) => v.value === 'business_coaching'));
    assert.equal(labelForBusinessVertical('business_coaching'), 'Business Coaching');
    assert.deepEqual(listCanonicalBusinessVerticals()[0], CANONICAL_BUSINESS_VERTICALS[0]);
  });

  it('accepts canonical snake_case values and rejects free text', () => {
    assert.equal(assertCanonicalBusinessVertical('business_coaching'), 'business_coaching');
    assert.equal(assertCanonicalBusinessVertical('Commercial Cleaning'), 'commercial_cleaning');
    assert.equal(isCanonicalBusinessVertical('legal'), true);
    assert.equal(isCanonicalBusinessVertical('Founder-led agencies'), false);
    assert.throws(
      () => assertCanonicalBusinessVertical('Founder Led Businesses'),
      (err) =>
        err.code === 'tenant_validation' &&
        err.message === unsupportedVerticalMessage('Founder Led Businesses')
    );
  });

  it('tenant create validation requires a canonical vertical', () => {
    assert.throws(
      () =>
        validateCreateClientInput({
          companyName: 'Fedir',
          primaryContact: 'Fedir',
          email: 'hello@fedir.example',
          vertical: 'Founder-led agencies',
          country: 'United States',
          timezone: 'America/New_York',
        }),
      (err) => err.code === 'tenant_validation' && /not currently a supported business vertical/.test(err.message)
    );

    const input = validateCreateClientInput({
      companyName: 'Fedir',
      primaryContact: 'Fedir',
      email: 'hello@fedir.example',
      vertical: 'business_coaching',
      country: 'United States',
      timezone: 'America/New_York',
    });
    assert.equal(input.vertical, 'business_coaching');
  });

  it('maps database constraint violations to operator-safe messages', () => {
    const mapped = mapVerticalConstraintError({
      code: '23514',
      constraint: 'clients_vertical_canonical_chk',
    });
    assert.equal(mapped.code, 'tenant_validation');
    assert.equal(mapped.status, 400);
    assert.match(mapped.message, /not currently supported/i);
    assert.doesNotMatch(mapped.message, /clients_vertical_canonical_chk/);
  });

  it('migration and registry stay aligned', () => {
    const migration = fs.readFileSync(
      path.join(__dirname, '../migrations/2026-08-19-canonical-business-verticals.sql'),
      'utf8'
    );
    for (const entry of CANONICAL_BUSINESS_VERTICALS) {
      assert.match(migration, new RegExp(`'${entry.value}'`));
    }
  });

  it('operator UI loads verticals from the API instead of hardcoding options', () => {
    const admin = fs.readFileSync(path.join(__dirname, '../public/admin-clients.html'), 'utf8');
    const signup = fs.readFileSync(path.join(__dirname, '../public/signup.html'), 'utf8');
    assert.match(admin, /\/api\/canonical-verticals/);
    assert.match(admin, /Business Vertical/);
    assert.doesNotMatch(admin, /id="industry"/);
    assert.match(signup, /\/api\/canonical-verticals/);
    assert.doesNotMatch(signup, /id="industry"/);
  });
});
