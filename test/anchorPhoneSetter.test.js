'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { _test } = require('../leadgen');
const phoneSetter = require('../utils/anchorPhoneSetter');
const { preflight } = require('../scripts/preflightAnchorPhoneSetter');

const root = path.join(__dirname, '..');
const forward = fs.readFileSync(path.join(root, 'migrations', '2026-07-18-anchor-phone-setter-immediate-cash-v1.sql'), 'utf8');
const rollback = fs.readFileSync(path.join(root, 'migrations', '2026-07-18-anchor-phone-setter-immediate-cash-v1.rollback.sql'), 'utf8');
const setterRoute = fs.readFileSync(path.join(root, 'routes', 'setter.js'), 'utf8');

test('Anchor Phone Setter has exactly six Tier-A manual Scout categories', () => {
  const expected = [
    'cleaning_company_overflow', 'str_manager', 'property_manager', 'realtor',
    'restoration_remodeling_partner', 'commercial_office',
  ];
  assert.deepEqual(phoneSetter.ANCHOR_PHONE_SETTER_CATEGORIES, expected);
  assert.deepEqual(Object.keys(_test.CLIENT_SCOUT_PLANS[10].verticals), expected);
  assert.equal(phoneSetter.categoryPriority('cleaning_company_overflow'), 1);
  assert.equal(phoneSetter.categoryPriority('commercial_office'), 6);
  assert.equal(phoneSetter.categoryPriority('unknown'), 99);
});

test('Anchor structured call details are strict and category-bound', () => {
  const valid = phoneSetter.validateStructuredDetails({
    category: 'property_manager', contact_role: 'Office manager', decision_maker_reached: true,
    interest_level: 'Interested', next_step: 'Call next Tuesday', follow_up_channel: 'phone',
  }, 'property_manager');
  assert.equal(valid.category, 'property_manager');
  assert.throws(() => phoneSetter.validateStructuredDetails({
    category: 'realtor', contact_role: 'Office manager', decision_maker_reached: true,
    interest_level: 'Interested', next_step: 'Call next Tuesday',
  }, 'property_manager'), /must match/);
  assert.throws(() => phoneSetter.validateStructuredDetails({ category: 'property_manager' }, 'property_manager'), /require contact_role/);
});

test('forward and rollback migrations preserve no-send and structured-history safety', () => {
  assert.match(forward, /CREATE TABLE IF NOT EXISTS campaigns/);
  assert.match(forward, /id UUID PRIMARY KEY DEFAULT gen_random_uuid\(\)/);
  assert.match(forward, /ADD COLUMN IF NOT EXISTS details JSONB NOT NULL DEFAULT '\{\}'::jsonb/);
  assert.match(forward, /CREATE TABLE IF NOT EXISTS setter_follow_up_drafts/);
  assert.match(forward, /'anchor_phone_setter_immediate_cash_v1', 'paused'/);
  assert.match(forward, /'external_sends_enabled',false/);
  assert.match(forward, /'revenue_writes_enabled',false/);
  for (const category of phoneSetter.ANCHOR_PHONE_SETTER_CATEGORIES) assert.match(forward, new RegExp(`\\"vertical\\":\\"${category}\\"`));
  assert.match(rollback, /Rollback blocked: Anchor structured call history exists/);
  assert.match(rollback, /details <> '\{\}'::jsonb/);
  assert.match(rollback, /target_verticals = b.target_verticals/);
});

test('Anchor route explicitly blocks provider-adjacent actions and revenue writes remain flag-gated', () => {
  assert.match(setterRoute, /if \(isAnchorPhoneSetter\(clientId\)\) \{\s*return res\.status\(404\)/);
  assert.match(setterRoute, /const anchorPhoneSetter = isAnchorPhoneSetter\(clientId\);/);
  assert.match(setterRoute, /const closer = anchorPhoneSetter \? null : await getLeviCloser\(clientId\);/);
  assert.match(setterRoute, /assertRevenueFlag\(flags, 'revenue_operator_writes_enabled'\)/);
  assert.match(setterRoute, /status='manual_sent'/);
  assert.match(setterRoute, /provider_action: false/);
});

test('Anchor preflight reports a missing campaign table as not ready', async () => {
  const db = {
    async query(sql) {
      if (sql.includes('FROM clients')) {
        return { rows: [{ id: 10, active: true, enabled_agents: ['scout'], autosend_enabled: false }] };
      }
      if (sql.includes('FROM revenue_feature_flags')) {
        return { rows: [{ revenue_schema_enabled: false, revenue_operator_reads_enabled: false, revenue_operator_writes_enabled: false, revenue_max_reads_enabled: false, revenue_followup_recommendations_enabled: false }] };
      }
      if (sql.includes('FROM campaigns')) {
        const error = new Error('relation "campaigns" does not exist');
        error.code = '42P01';
        throw error;
      }
      throw new Error(`Unexpected query: ${sql}`);
    },
  };

  const report = await preflight(db);
  assert.equal(report.ok, false);
  assert.equal(report.checks.anchor_campaign_paused, false);
  assert.equal(report.checks.campaign_external_sends_disabled, false);
  assert.equal(report.checks.campaign_revenue_writes_disabled, false);
});
