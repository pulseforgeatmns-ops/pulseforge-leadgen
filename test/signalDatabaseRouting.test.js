'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { resolveSignalConnection, getSignalPool } = require('../signalDb');

test('migration Signal SQL connection stays on legacy path before opt-in', () => {
  assert.equal(resolveSignalConnection({ DATABASE_URL:'postgresql://legacy.example/core' }), null);
});
test('opted-in separation refuses missing Signal database', () => {
  assert.throws(() => resolveSignalConnection({
    DATABASE_URL:'postgresql://legacy.example/core', SIGNAL_DB_SEPARATION_REQUIRED:'1',
  }), /signal_database_url_required/);
});
test('Signal database must not equal Pulseforge database', () => {
  assert.throws(() => resolveSignalConnection({
    DATABASE_URL:'postgresql://same.example/core',
    SIGNAL_DATABASE_URL:'postgresql://same.example/core',
    SIGNAL_DB_SEPARATION_REQUIRED:'1',
  }), /signal_database_must_be_separate/);
});
test('configured Signal URL without explicit activation is refused', () => {
  assert.throws(() => resolveSignalConnection({
    SIGNAL_DATABASE_URL:'postgresql://signal.example/signal',
  }), /signal_database_separation_requires_explicit_opt_in/);
});
test('explicit split accepts a distinct PostgreSQL URL without connecting', () => {
  assert.equal(resolveSignalConnection({
    DATABASE_URL:'postgresql://core.example/core',
    SIGNAL_DATABASE_URL:'postgresql://signal.example/signal',
    SIGNAL_DB_SEPARATION_REQUIRED:'1',
  }), 'postgresql://signal.example/signal');
});
test('malformed Signal database URL is rejected', () => {
  assert.throws(() => resolveSignalConnection({
    SIGNAL_DATABASE_URL:'not-a-url', SIGNAL_DB_SEPARATION_REQUIRED:'1',
  }), /invalid_signal_database_url/);
});
test('strict split refuses shared database before even constructing a pool', () => {
  assert.throws(() => getSignalPool({
    SIGNAL_DB_SEPARATION_REQUIRED:'1', DATABASE_URL:'postgresql://legacy.example/core',
  }), /signal_database_url_required/);
});
