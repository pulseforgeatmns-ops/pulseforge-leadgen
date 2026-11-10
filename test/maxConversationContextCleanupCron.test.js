
'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const express = require('express');

async function withFixture(options, callback) {
  const calls = [];
  const db = {
    async query(sql) {
      calls.push(sql);
      if (options.dbError) throw options.dbError;
      return { rowCount: options.deleted ?? 0 };
    },
  };

  const filename = path.join(__dirname, '..', 'routes/cron.js');
  const dependencies = {
    express,
    'node:crypto': require('node:crypto'),
    '../utils/meteredAnthropic': { forAgent: () => class {} },
    '../db': db,
    '../utils/clientContext': {},
    '../utils/agentDispatchPolicy': {},
    '../scoutExpansion': {},
    '../utils/maxDecayCron': {
      createMaxDecayCronHandler: () => (_req, res) => res.sendStatus(404),
    },
    '../services/scoutPlacesDiagnostic': {},
    '../utils/anchorResearchProviderPause':
      require('../utils/anchorResearchProviderPause'),
    '../utils/researchProviderBlockLog':
      require('../utils/researchProviderBlockLog'),
  };

  const module = { exports: {} };
  const logs = [];

  vm.compileFunction(
    fs.readFileSync(filename, 'utf8'),
    ['module', 'require', 'process', 'console'],
    { filename }
  )(
    module,
    name => {
      if (Object.hasOwn(dependencies, name)) return dependencies[name];
      throw new Error(`Unexpected dependency: ${name}`);
    },
    {
      env: {
        CRON_SECRET: options.secret === undefined
          ? 'test-cleanup-secret'
          : options.secret,
      },
    },
    {
      info: (...args) => logs.push(args),
      error: (...args) => logs.push(args),
    }
  );

  const app = express();
  app.use(express.json());
  app.use(module.exports);

  const server = app.listen(0, '127.0.0.1');
  await new Promise((resolve, reject) => {
    server.once('listening', resolve);
    server.once('error', reject);
  });

  async function request({
    method = 'POST',
    token = 'test-cleanup-secret',
  } = {}) {
    const headers = {};
    if (token !== null) headers.Authorization = `Bearer ${token}`;

    const response = await fetch(
      `http://127.0.0.1:${server.address().port}/cron/max-conversation-context-cleanup`,
      { method, headers }
    );

    return {
      status: response.status,
      body: await response.json(),
      cache: response.headers.get('cache-control'),
    };
  }

  try {
    await callback({ request, calls, logs });
  } finally {
    await new Promise((resolve, reject) => {
      server.close(error => error ? reject(error) : resolve());
    });
  }
}

test('valid bearer token performs bounded expiry cleanup', async () => {
  await withFixture({ deleted: 12 }, async ({ request, calls }) => {
    const response = await request();

    assert.equal(response.status, 200);
    assert.deepEqual(response.body, { ok: true, deleted: 12 });
    assert.equal(response.cache, 'no-store');
    assert.equal(calls.length, 1);

    const sql = calls[0];
    assert.match(sql, /expires_at\s*<=\s*NOW\(\)/i);
    assert.match(sql, /LIMIT\s+500/i);
    assert.match(sql, /FOR UPDATE SKIP LOCKED/i);
    assert.match(sql, /DELETE FROM max_conversation_context/i);
  });
});

test('invalid and missing credentials never access database', async () => {
  await withFixture({}, async ({ request, calls }) => {
    for (const token of [null, '', 'wrong-secret']) {
      const response = await request({ token });
      assert.equal(response.status, 401);
      assert.deepEqual(response.body, { error: 'Unauthorized' });
    }
    assert.equal(calls.length, 0);
  });
});

test('missing server secret fails closed', async () => {
  await withFixture({ secret: '' }, async ({ request, calls }) => {
    const response = await request({ token: '' });
    assert.equal(response.status, 401);
    assert.equal(calls.length, 0);
  });
});

test('GET cannot perform cleanup', async () => {
  await withFixture({}, async ({ request, calls }) => {
    const response = await request({ method: 'GET' });
    assert.notEqual(response.status, 200);
    assert.equal(calls.length, 0);
  });
});

test('database failures return sanitized errors', async () => {
  const sensitive = 'postgres://user:password@private-host/database';

  await withFixture(
    { dbError: new Error(sensitive) },
    async ({ request, calls }) => {
      const response = await request();

      assert.equal(response.status, 500);
      assert.deepEqual(response.body, {
        ok: false,
        error: 'max_context_cleanup_failed',
      });
      assert.equal(calls.length, 1);
      assert.doesNotMatch(JSON.stringify(response.body), /password|private-host/);
    }
  );
});
