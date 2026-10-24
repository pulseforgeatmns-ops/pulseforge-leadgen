'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  resolveActiveTenantId,
  resolveMaxPromptContext,
  buildTenantGreeting,
  NO_ACTIVE_CLIENT,
} = require('../TenantContextResolver');
const { buildOpeningState } = require('../OpeningStateBuilder');

describe('SPEC-114 TenantContextResolver', () => {
  it('fails closed without a tenant', () => {
    const result = resolveMaxPromptContext({ publishedAim: { id: 'aim-fedir' } });
    assert.equal(result.ok, false);
    assert.equal(result.message, NO_ACTIVE_CLIENT);
    assert.equal(result.publishedAim, null);
  });

  it('does not default admin sessions to Pulseforge', () => {
    assert.equal(resolveActiveTenantId({ user: { role: 'admin' }, session: {} }), null);
  });

  it('resolves session.active_client_id for operators', () => {
    assert.equal(
      resolveActiveTenantId({
        user: { role: 'manager' },
        session: { active_client_id: 80 },
      }),
      80
    );
  });

  it('builds the Fedir onboarding greeting', () => {
    const greeting = buildTenantGreeting('Fedir');
    assert.match(greeting.fullText, /Welcome, Fedir/);
    const opening = buildOpeningState({
      tenantId: '80',
      tenantName: 'Fedir',
      tenantWorkspace: { needsOnboarding: true },
    });
    assert.match(opening.fullText, /Client Intelligence/);
  });
});
