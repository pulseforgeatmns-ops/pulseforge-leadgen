'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  ALLOWED_GOVERNED_OUTBOUND_TENANTS,
  assertGovernedOutboundTenantId,
  createGovernedOutboundTenantContext,
  parseGovernedOutboundTenantIds,
  governedOutboundEnabledForTenant,
  governedOutboundSendingDisabledForTenant,
} = require('../services/governedOutboundTenant');
const { readOutboundHistory } = require('../services/governedOutboundAdapters');
const { policy } = require('../packages/acquisition-mission/DailyOutboundPolicy');

test('mailbox grant binds identity and the reviewed Emmett window', () => {
  const now=new Date('2026-09-28T15:00:00Z');
  const input={tenantId:'13',sourceMissionId:'mission',senderEmail:'hello@babrun.com',inboxIntegrationId:'mailbox',sendingIdentityId:'identity',dailyCap:1,totalCap:1,spacingMinutes:240,startHour:9,endHour:16,expiresAt:'2026-09-30T15:00:00Z'};
  const grant=policy(input,now);
  assert.equal(grant.sendingIdentityId,'identity');assert.equal(grant.endHour,16);assert.equal(grant.spacingMinutes,240);
  assert.throws(()=>policy({...input,sendingIdentityId:''},now),{code:'sending_identity_required'});
  assert.throws(()=>policy({...input,endHour:24},now),{code:'invalid_business_window'});
});

test('only tenants 10 and 13 are allowed for governed outbound', () => {
  assert.deepEqual(ALLOWED_GOVERNED_OUTBOUND_TENANTS, ['10', '13']);
  assert.equal(assertGovernedOutboundTenantId('10'), '10');
  assert.equal(assertGovernedOutboundTenantId('13'), '13');
  assert.throws(() => assertGovernedOutboundTenantId('1'), { code: 'unsupported_governed_outbound_tenant' });
  assert.throws(() => assertGovernedOutboundTenantId('99'), { code: 'unsupported_governed_outbound_tenant' });
  assert.throws(() => assertGovernedOutboundTenantId(''), { code: 'unsupported_governed_outbound_tenant' });
});

test('tenant enablement is explicit per tenant and fails closed by default', () => {
  const savedAnchor = process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
  const savedBabrun = process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
  try {
    delete process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
    delete process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
    assert.equal(governedOutboundEnabledForTenant('10'), false);
    assert.equal(governedOutboundEnabledForTenant('13'), false);

    process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = 'true';
    assert.equal(governedOutboundEnabledForTenant('10'), true);
    assert.equal(governedOutboundEnabledForTenant('13'), false);

    process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = 'true';
    assert.equal(governedOutboundEnabledForTenant('13'), true);

    process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = 'false';
    assert.equal(governedOutboundSendingDisabledForTenant('10'), true);
    process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = 'false';
    assert.equal(governedOutboundSendingDisabledForTenant('13'), true);
  } finally {
    if (savedAnchor === undefined) delete process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
    else process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = savedAnchor;
    if (savedBabrun === undefined) delete process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
    else process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = savedBabrun;
  }
});

test('tenant context binds transport and Anchor-only policy without cross-tenant leakage', () => {
  const anchor = createGovernedOutboundTenantContext('10');
  const babrun = createGovernedOutboundTenantContext('13');
  assert.equal(anchor.tenantId, '10');
  assert.equal(anchor.clientId, 10);
  assert.equal(babrun.tenantId, '13');
  assert.equal(babrun.clientId, 13);
  assert.equal(anchor.usesBrevoTransport, true);
  assert.equal(babrun.usesBrevoTransport, false);
  assert.equal(anchor.usesTenantMailboxTransport, false);
  assert.equal(babrun.usesTenantMailboxTransport, true);
  assert.equal(anchor.requiresAoOwners, true);
  assert.equal(babrun.requiresAoOwners, false);
  assert.notEqual(anchor.advisoryLockKey, babrun.advisoryLockKey);
});

test('parseGovernedOutboundTenantIds rejects unknown ids in configuration', () => {
  const saved = process.env.GOVERNED_OUTBOUND_TENANT_IDS;
  try {
    process.env.GOVERNED_OUTBOUND_TENANT_IDS = '10,13';
    assert.deepEqual(parseGovernedOutboundTenantIds(), ['10', '13']);
    assert.throws(() => parseGovernedOutboundTenantIds('10,14'), { code: 'unsupported_governed_outbound_tenant' });
  } finally {
    if (saved === undefined) delete process.env.GOVERNED_OUTBOUND_TENANT_IDS;
    else process.env.GOVERNED_OUTBOUND_TENANT_IDS = saved;
  }
});

test('readOutboundHistory scopes send evidence to the requested tenant and client', async () => {
  const queries = [];
  const pool = {
    query(sql, args) {
      queries.push({ sql, args });
      return { rows: [{ today: 2, last_attempt: new Date() }] };
    },
  };
  const history = await readOutboundHistory(pool, '10', 10);
  assert.equal(history.today, 2);
  assert.equal(queries.length, 1);
  assert.match(queries[0].sql, /tenant_id=\$3/);
  assert.match(queries[0].sql, /client_id=\$4/);
  assert.deepEqual(queries[0].args.slice(2), ['10', 10]);
});

test('cron run iterates configured tenants without merging tick results', async () => {
  const saved = process.env.GOVERNED_OUTBOUND_TENANT_IDS;
  const ticks = [];
  const governedOutbound = require('../services/governedOutbound');
  const original = governedOutbound.productionService;
  governedOutbound.productionService = (pool, { tenantId }) => ({
    tick: async () => {
      ticks.push(tenantId);
      return { tenantId, sent: tenantId === '10' ? 1 : 0 };
    },
  });
  try {
    process.env.GOVERNED_OUTBOUND_TENANT_IDS = '10,13';
    const result = await require('../anchorDailyOutboundCron').run({ pool: {}, allTenants: true });
    assert.deepEqual(ticks, ['10', '13']);
    assert.equal(result.tenants['10'].sent, 1);
    assert.equal(result.tenants['13'].sent, 0);
    assert.notEqual(result.tenants['10'], result.tenants['13']);
  } finally {
    governedOutbound.productionService = original;
    if (saved === undefined) delete process.env.GOVERNED_OUTBOUND_TENANT_IDS;
    else process.env.GOVERNED_OUTBOUND_TENANT_IDS = saved;
  }
});
