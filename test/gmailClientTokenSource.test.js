'use strict';

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  resolveMarketIntelTokenSource,
  selectTokenMaterial,
  normalizeTokenSource,
} = require('../utils/gmailClient');

describe('gmailClient tokenSource selection', () => {
  const original = {};

  beforeEach(() => {
    for (const key of [
      'GMAIL_TOKEN',
      'RILEY_ACCESS_TOKEN',
      'RILEY_REFRESH_TOKEN',
      'RILEY_TOKEN_EXPIRY',
    ]) {
      original[key] = process.env[key];
      delete process.env[key];
    }
  });

  afterEach(() => {
    for (const [key, value] of Object.entries(original)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });

  it('normalizes known sources and rejects invalid ones', () => {
    assert.equal(normalizeTokenSource('Gmail'), 'gmail');
    assert.equal(normalizeTokenSource(null), 'auto');
    assert.throws(() => normalizeTokenSource('personal'), /Invalid tokenSource/);
  });

  it('auto prefers Riley over GMAIL_TOKEN (legacy shared behavior)', () => {
    const selected = selectTokenMaterial({
      tokenSource: 'auto',
      env: {
        RILEY_ACCESS_TOKEN: 'riley-access',
        RILEY_REFRESH_TOKEN: 'riley-refresh',
        GMAIL_TOKEN: JSON.stringify({ access_token: 'gmail-access' }),
      },
    });
    assert.equal(selected.source, 'riley');
    assert.equal(selected.kind, 'riley_env');
  });

  it('gmail source ignores Riley tokens', () => {
    const selected = selectTokenMaterial({
      tokenSource: 'gmail',
      env: {
        RILEY_ACCESS_TOKEN: 'riley-access',
        GMAIL_TOKEN: JSON.stringify({ access_token: 'gmail-access' }),
      },
    });
    assert.equal(selected.source, 'gmail');
    assert.equal(selected.kind, 'gmail_json');
    assert.match(String(selected.value), /gmail-access/);
  });

  it('riley source ignores GMAIL_TOKEN', () => {
    const selected = selectTokenMaterial({
      tokenSource: 'riley',
      env: {
        RILEY_ACCESS_TOKEN: 'riley-access',
        GMAIL_TOKEN: JSON.stringify({ access_token: 'gmail-access' }),
      },
    });
    assert.equal(selected.source, 'riley');
  });

  it('market-intel default uses gmail when GMAIL_TOKEN exists', () => {
    assert.equal(
      resolveMarketIntelTokenSource(null, {
        GMAIL_TOKEN: '{}',
        RILEY_ACCESS_TOKEN: 'riley',
      }),
      'gmail'
    );
    assert.equal(
      resolveMarketIntelTokenSource(undefined, {
        RILEY_ACCESS_TOKEN: 'riley',
      }),
      'auto'
    );
    assert.equal(resolveMarketIntelTokenSource('riley', { GMAIL_TOKEN: '{}' }), 'riley');
  });

  it('gmail source fails closed without GMAIL_TOKEN', () => {
    assert.throws(
      () => selectTokenMaterial({
        tokenSource: 'gmail',
        env: { RILEY_ACCESS_TOKEN: 'riley' },
      }),
      /tokenSource=gmail/
    );
  });
});
