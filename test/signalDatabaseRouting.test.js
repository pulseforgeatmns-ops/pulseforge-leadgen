'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { resolveSignalConnection, getSignalPool } = require('../signalDb');

test('migration retains legacy pool while split is not activated', () => {
  assert.equal(resolveSignalConnection({ DATABASE_URL:'postgresql://old.example/core' }), null);
  assert.equal(resolveSignalConnection({
    DATABASE_URL:'postgresql://old.example/core',
    SIGNAL_DATABASE_URL:'postgresql://new.example/signal',
  }), null);
});
test('enabled split refuses missing Signal URL', () => {
  assert.throws(() => resolveSignalConnection({
    DATABASE_URL:'postgresql://old.example/core', SIGNAL_DB_SEPARATION_REQUIRED:'1',
  }), /signal_database_url_required/);
});
test('same database is rejected even with distinct credentials', () => {
  assert.throws(() => resolveSignalConnection({
    DATABASE_URL:'postgresql://alice:one@same.example:5432/core',
    SIGNAL_DATABASE_URL:'postgresql://bob:two@same.example/core',
    SIGNAL_DB_SEPARATION_REQUIRED:'1',
  }), /signal_database_must_be_separate/);
});
test('same database is rejected when URL spelling differs', () => {
  assert.throws(() => resolveSignalConnection({
    DATABASE_URL:'postgres://alice@SAME.example/core',
    SIGNAL_DATABASE_URL:'postgresql://bob@same.example:5432/core',
    SIGNAL_DB_SEPARATION_REQUIRED:'1',
  }), /signal_database_must_be_separate/);
});
test('different database is accepted when explicitly enabled', () => {
  assert.equal(resolveSignalConnection({
    DATABASE_URL:'postgresql://core.example/core',
    SIGNAL_DATABASE_URL:'postgresql://signal.example/signal',
    SIGNAL_DB_SEPARATION_REQUIRED:'1',
  }), 'postgresql://signal.example/signal');
});
test('invalid URL is rejected only when isolation enabled', () => {
  assert.equal(resolveSignalConnection({ SIGNAL_DATABASE_URL:'not-a-url' }), null);
  assert.throws(() => resolveSignalConnection({
    SIGNAL_DATABASE_URL:'not-a-url', SIGNAL_DB_SEPARATION_REQUIRED:'1',
  }), /invalid_signal_database_url/);
});
test('strict split refuses shared DB before making pool', () => {
  assert.throws(() => getSignalPool({
    SIGNAL_DB_SEPARATION_REQUIRED:'1', DATABASE_URL:'postgresql://legacy.example/core',
  }), /signal_database_url_required/);
});
