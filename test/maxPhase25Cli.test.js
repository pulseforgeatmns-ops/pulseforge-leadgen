'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { tokenizeArgs, optionalPositiveInteger, optionalUuid } = require('../utils/maxCli');
const { parseArgs: parseRecalculate } = require('../scripts/recalculateMaxOrchestration');
const { parseArgs: parseDecay } = require('../maxDecayAgent');

const UUID = '550e8400-e29b-41d4-a716-446655440000';

test('Max CLI accepts both supported value syntaxes with canonical ID types', () => {
  assert.equal(parseRecalculate(['--client-id=10','--prospect-id',UUID]).clientId, 10);
  assert.equal(parseDecay(['--client-id','1','--after-id='+UUID,'--dry-run']).after_id, UUID);
  assert.equal(parseDecay(['--client-id','1']).dry_run, true);
  assert.equal(parseDecay(['--client-id','1','--apply']).dry_run, false);
});

test('Max CLI rejects malformed and silently coercible IDs', () => {
  for (const value of ['abc','1.5','-1','0','1junk']) assert.throws(() => optionalPositiveInteger(value, '--client-id'));
  for (const value of ['abc','1','550e8400-e29b-41d4-a716']) assert.throws(() => optionalUuid(value, '--prospect-id'));
  assert.throws(() => parseRecalculate(['--client-id','1junk']));
  assert.throws(() => parseDecay(['--unknown','1']));
  assert.throws(() => tokenizeArgs(['--limit','10','--limit=20']));
});
