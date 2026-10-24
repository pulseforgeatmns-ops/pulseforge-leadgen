'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.join(__dirname, '..');
const maxHarnessFiles = [
  'test/maxDecayCronPostgres.test.js',
  'test/maxSmokeProductionSchema.test.js',
  'test/maxTransactionClient.test.js',
];

test('Max disposable PostgreSQL harnesses pin socket directories under the data directory', () => {
  for (const relativePath of maxHarnessFiles) {
    const source = fs.readFileSync(path.join(root, relativePath), 'utf8');
    assert.match(
      source,
      /-k \$\{directory\}/,
      `${relativePath} must pass -k \${directory} so GitHub Actions can start PostgreSQL 18`,
    );
    assert.match(
      source,
      /postgres\.log/,
      `${relativePath} must capture a postgres.log for startup diagnostics`,
    );
  }
});
