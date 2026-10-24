'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {
  SCOPES,
  TOKEN_PATH,
  parseArgs,
} = require('../getGmailToken');

describe('getGmailToken helper', () => {
  it('uses readonly Gmail scope only', () => {
    assert.deepEqual(SCOPES, ['https://www.googleapis.com/auth/gmail.readonly']);
  });

  it('targets gitignored gmail_token.json in repo root', () => {
    assert.equal(path.basename(TOKEN_PATH), 'gmail_token.json');
    const gitignore = fs.readFileSync(path.join(__dirname, '..', '.gitignore'), 'utf8');
    assert.match(gitignore, /^gmail_token\.json$/m);
  });

  it('parses --refresh and rejects unknown flags', () => {
    assert.deepEqual(parseArgs([]), { refresh: false, help: false });
    assert.equal(parseArgs(['--refresh']).refresh, true);
    assert.equal(parseArgs(['--help']).help, true);
    assert.throws(() => parseArgs(['--force']), /Unknown argument/);
  });
});
