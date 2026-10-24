'use strict';

const assert = require('node:assert/strict');
const { describe, test, before, after } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const {
  buildAoSignature,
  canAoOriginateSend,
  formatPhoneDisplay,
  resolveOutboundSenderForAssignment,
  toPublicIdentity,
  mapRowToIdentity,
  stripSecretsFromRecord,
} = require('../utils/aoCommunicationIdentity');

describe('SPEC-AO-MAILBOX-001 ao communication identity', () => {
  test('Tony phone formats to (978) 505-1501', () => {
    assert.equal(formatPhoneDisplay('+1 978 505 1501'), '(978) 505-1501');
  });

  test('canonical signature is assembled from identity fields', () => {
    const sig = buildAoSignature({
      userName: 'Tony Jackson',
      identity: mapRowToIdentity({
        ao_id: 24,
        tenant_id: 10,
        email_address: 'tony@goanchorcleaning.com',
        phone_number: '+1 978 505 1501',
        reply_to_address: 'tony@goanchorcleaning.com',
        sender_enabled: false,
        mailbox_status: 'configured',
      }, { name: 'Tony Jackson' }),
    });
    assert.match(sig, /Tony Jackson/);
    assert.match(sig, /Acquisition Operator/);
    assert.match(sig, /Anchor Cleaning/);
    assert.match(sig, /\(978\) 505-1501/);
    assert.match(sig, /tony@goanchorcleaning\.com/);
    assert.match(sig, /goanchorcleaning\.com/);
  });

  test('unauthenticated mailbox cannot send — falls back to Jake', () => {
    const fallback = {
      senderEmail: 'jacob@goanchorcleaning.com',
      senderName: 'Jacob Maynard',
      sendingDomain: 'goanchorcleaning.com',
    };
    const identity = mapRowToIdentity({
      ao_id: 24,
      tenant_id: 10,
      email_address: 'tony@goanchorcleaning.com',
      phone_number: '+1 978 505 1501',
      reply_to_address: 'tony@goanchorcleaning.com',
      sender_enabled: false,
      mailbox_status: 'configured',
    }, { name: 'Tony Jackson' });
    const resolution = resolveOutboundSenderForAssignment({ identity, fallbackSender: fallback });
    assert.equal(resolution.usedAoSender, false);
    assert.equal(resolution.sender.senderEmail, fallback.senderEmail);
  });

  test('senderEnabled=false blocks AO send even when mailbox ready', () => {
    const fallback = {
      senderEmail: 'jacob@goanchorcleaning.com',
      senderName: 'Jacob Maynard',
      sendingDomain: 'goanchorcleaning.com',
    };
    const identity = mapRowToIdentity({
      ao_id: 24,
      tenant_id: 10,
      email_address: 'tony@goanchorcleaning.com',
      reply_to_address: 'tony@goanchorcleaning.com',
      sender_enabled: false,
      mailbox_status: 'ready',
    }, { name: 'Tony Jackson' });
    assert.equal(canAoOriginateSend(identity), false);
    const resolution = resolveOutboundSenderForAssignment({ identity, fallbackSender: fallback });
    assert.equal(resolution.reason, 'ao_sender_disabled');
    assert.equal(resolution.sender.senderEmail, 'jacob@goanchorcleaning.com');
  });

  test('ready + enabled selects AO sender', () => {
    const fallback = {
      senderEmail: 'jacob@goanchorcleaning.com',
      senderName: 'Jacob Maynard',
      sendingDomain: 'goanchorcleaning.com',
      tenantId: '10',
      clientId: 10,
    };
    const identity = mapRowToIdentity({
      ao_id: 24,
      tenant_id: 10,
      email_address: 'tony@goanchorcleaning.com',
      reply_to_address: 'tony@goanchorcleaning.com',
      sender_enabled: true,
      mailbox_status: 'ready',
    }, { name: 'Tony Jackson' });
    const resolution = resolveOutboundSenderForAssignment({ identity, fallbackSender: fallback });
    assert.equal(resolution.usedAoSender, true);
    assert.equal(resolution.sender.senderEmail, 'tony@goanchorcleaning.com');
    assert.equal(resolution.sender.replyToAddress, 'tony@goanchorcleaning.com');
  });

  test('public API shape omits credential fields', () => {
    const pub = toPublicIdentity(mapRowToIdentity({
      ao_id: 24,
      tenant_id: 10,
      email_address: 'tony@goanchorcleaning.com',
      phone_number: '+1 978 505 1501',
      reply_to_address: 'tony@goanchorcleaning.com',
      sender_enabled: false,
      mailbox_status: 'configured',
      mailbox_integration_id: 'tmi_secret',
      oauth_refresh_secret_ref: 'SECRET',
    }, { name: 'Tony Jackson' }));
    assert.equal(pub.email, 'tony@goanchorcleaning.com');
    assert.equal(JSON.stringify(pub).includes('SECRET'), false);
    assert.equal(pub.oauth_refresh_secret_ref, undefined);
    assert.equal(stripSecretsFromRecord({ imap_secret_ref: 'x', email_address: 'a@b.com' }).imap_secret_ref, undefined);
  });
});

describe('SPEC-AO-MAILBOX-001 postgres migration seed', () => {
  let pool;
  let stopPostgres;
  let postgresAvailable = true;

  before(async () => {
    try {
      ({ pool, stop: stopPostgres } = await startDisposablePostgres());
    } catch (err) {
      postgresAvailable = false;
      return;
    }
    await pool.query(`
      CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT,
        email TEXT,
        role TEXT,
        client_id INT,
        active BOOLEAN DEFAULT true
      );
      CREATE TABLE tenant_mailbox_integrations (
        id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        provider_type TEXT,
        mailbox_address TEXT,
        status TEXT DEFAULT 'unverified'
      );
    `);
    await pool.query(`INSERT INTO users (id, name, email, role, client_id, active)
      VALUES (24, 'Tony Jackson', 'tony@example.com', 'ao', 10, true)`);
    const sql = fs.readFileSync(
      path.join(__dirname, '..', 'migrations', '2026-10-06-ao-mailbox-001.sql'),
      'utf8'
    );
    await pool.query(sql);
  });

  after(async () => {
    if (stopPostgres) await stopPostgres();
  });

  test('Tony row resolves to tony@goanchorcleaning.com', async (t) => {
    if (!postgresAvailable) {
      t.skip('PostgreSQL not available in this environment');
      return;
    }
    const { rows } = await pool.query(
      'SELECT email_address, phone_number, mailbox_status, sender_enabled FROM ao_communication_identities WHERE ao_id = 24'
    );
    assert.equal(rows[0].email_address, 'tony@goanchorcleaning.com');
    assert.equal(rows[0].phone_number, '+1 978 505 1501');
    assert.equal(rows[0].mailbox_status, 'configured');
    assert.equal(rows[0].sender_enabled, false);
  });
});
