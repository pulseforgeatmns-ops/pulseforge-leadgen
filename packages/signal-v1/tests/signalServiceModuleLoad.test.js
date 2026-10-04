'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

describe('Signal V1 production module load', () => {
  it('requires SignalService via the production path without ReferenceError', () => {
    const { SignalService } = require('../SignalService');
    assert.equal(typeof SignalService, 'function');
    const service = new SignalService(undefined, { seedFixtures: false });
    assert.ok(service.store);
  });

  it('requires package index entry (server.js path) without ReferenceError', () => {
    const pkg = require('../index');
    assert.equal(typeof pkg.SignalService, 'function');
    assert.equal(typeof pkg.replayToken, 'function');
    assert.equal(typeof pkg.validateHistoricalCoverage, 'function');
  });
});
