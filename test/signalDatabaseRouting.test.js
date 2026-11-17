'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { resolveSignalConnection } = require('../signalDb');

test('migration Signal SQL connection stays on legacy path only before explicit opt-in', () => {
  assert.equal(resolveSignalConnection({ DATABASE_URL:'postgresql://legacy.example/core' }), null);
});
test('opted-in separation refuses missing Signal DB URL rather than silently using core', () => {
  assert.throws(() => resolveSignalConnection({ DATABASE_URL:'postgresql://legacy.example/core', SIGNAL_DB_SEPARATION_REQUIRED:'1' }), /signal_database_url_required/);
});
test('Signal database cannot equal Pulseforge database', () => {
  assert.throws(() => resolveSignalConnection({ DATABASE_URL:'postgresql://same.example/core', SIGNAL_DATABASE_URL:'postgresql://same.example/core' }), /signal_database_must_be_separate/);
});
test('separate valid URL passes without connecting or exposing credentials', () => {
  assert.equal(resolveSignalConnection({
    DATABASE_URL:'postgresql://core.example/core',
    SIGNAL_DATABASE_URL:'postgresql://signal.example/signal',
    SIGNAL_DB_SEPARATION_REQUIRED:'1',
  }), 'postgresql://signal.example/signal');
});
test('invalid connection strings are rejected', () => {
  assert.throws(() => resolveSignalConnection({ SIGNAL_DATABASE_URL:'\\$\\{\\{Postgres.DATABASE_URL\\}\\}' }), /invalid_signal_database_url/);
});
