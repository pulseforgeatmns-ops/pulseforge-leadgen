'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('child_process');
const path = require('path');

const {
  createMemoryStore,
  createContentPublication,
} = require('../services/contentOutcomeIntelligence');
const { parseCommand, main } = require('../scripts/contentOutcome');

describe('contentOutcome CLI', () => {
  it('parses commands', () => {
    const parsed = parseCommand([
      'publish',
      '--client-id=1',
      '--artifact-id=99',
      '--json',
    ]);
    assert.equal(parsed.command, 'publish');
    assert.equal(parsed.parsed.values.get('--artifact-id'), '99');
    assert.equal(parsed.parsed.flags.has('--json'), true);
  });

  it('prints help without error', () => {
    const script = path.join(__dirname, '..', 'scripts', 'contentOutcome.js');
    const result = spawnSync(process.execPath, [script, '--help'], {
      encoding: 'utf8',
    });
    assert.equal(result.status, 0);
    assert.match(result.stdout, /Content Outcome Intelligence/);
    assert.match(result.stdout, /publish/);
    assert.match(result.stdout, /performance/);
    assert.match(result.stdout, /add-outcome/);
  });

  it('exports main for programmatic use', async () => {
    // Smoke: unknown command fails cleanly
    await assert.rejects(() => main(['nope']), /Unknown command/);
  });

  it('service used by CLI can create publications (shared logic)', async () => {
    const store = createMemoryStore();
    const pub = await createContentPublication(
      {
        tenant_id: 1,
        content_artifact_id: 'cli-1',
        published_at: '2026-08-12T00:00:00.000Z',
        objective: 'awareness',
      },
      { store }
    );
    assert.ok(pub.id);
  });
});
