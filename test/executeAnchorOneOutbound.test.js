'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const {
  TENANT_ID,
  MISSION_ID,
  EXPECTED_CAPACITY_ID,
  parseArgs,
  run,
  sendableQueueItems,
} = require('../scripts/executeAnchorOneOutbound');

const SCRIPT = path.join(__dirname, '..', 'scripts/executeAnchorOneOutbound.js');
const source = fs.readFileSync(SCRIPT, 'utf8');

describe('executeAnchorOneOutbound — one controlled production send', () => {
  it('is tenant 10 / expected mission+capacity and refuses without --confirm-production', async () => {
    assert.equal(TENANT_ID, '10');
    assert.equal(MISSION_ID, 'mission_ad7753b0-6def-441d-bb1a-3764656f5750');
    assert.equal(EXPECTED_CAPACITY_ID, 'contrib_55e11312-3837-4485-b95a-58134dcd7601');
    assert.equal(parseArgs([]).confirmProduction, false);
    assert.equal(parseArgs(['--confirm-production']).confirmProduction, true);
    await assert.rejects(
      () => run({ confirmProduction: false }),
      (err) => err.code === 'confirm_production_required'
    );
  });

  it('uses the canonical approve then execute path and never mutates activation', () => {
    assert.match(source, /APPROVE_EXECUTION/);
    assert.match(source, /EXECUTE_OUTBOUND/);
    assert.match(source, /maxSends:\s*1/);
    assert.match(source, /prospectId:\s*selectedProspectId/);
    assert.match(source, /probe\.run/);
    assert.match(source, /runtime\.persistOpts\(\{\s*persist:\s*true\s*\}\)/);
    assert.doesNotMatch(source, /UPDATE\s+clients/i);
    assert.doesNotMatch(source, /SET\s+autosend_enabled/i);
    assert.doesNotMatch(source, /array_append\(\s*enabled_agents/i);
    assert.match(source, /ALLOW_FIXTURE_FALLBACK === 'true'/);
    assert.match(source, /allowFixtureFallback:\s*false/);
    assert.match(source, /createProviderCounter/);
  });

  it('classifies sendable queue items without inventing recipients', () => {
    const sendable = sendableQueueItems({
      queue: {
        items: [
          { prospectId: 'a', email: 'a@example.com', sendable: true, paige: { candidateId: 'a' } },
          { prospectId: 'b', email: 'b@example.com', sendable: false },
          { prospectId: 'c', dnc: true, email: 'c@example.com' },
          { prospectId: 'd' },
        ],
      },
    });
    assert.deepEqual(sendable.map((row) => row.prospectId), ['a']);
  });
});
