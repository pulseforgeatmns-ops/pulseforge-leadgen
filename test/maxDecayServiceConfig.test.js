'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.join(__dirname, '..');

test('shared Railway config lets each Dockerfile own its process command', () => {
  const config = JSON.parse(fs.readFileSync(path.join(root, 'railway.json'), 'utf8'));
  assert.equal(config.build.builder, 'DOCKERFILE');
  assert.equal(config.deploy?.startCommand, undefined);
  assert.match(fs.readFileSync(path.join(root, 'Dockerfile'), 'utf8'), /CMD \["node", "server\.js"\]/);
});

test('dedicated decay image runs only bounded resumable Anchor shadow decay', () => {
  const dockerfile = fs.readFileSync(path.join(root, 'Dockerfile.max-decay'), 'utf8');
  assert.match(dockerfile, /CMD \["npm", "run", "max:decay", "--", "--apply", "--resume", "--client-id=10", "--limit=250"\]/);
  assert.doesNotMatch(dockerfile, /server\.js|EXPOSE/);
});
