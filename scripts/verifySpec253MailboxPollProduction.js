'use strict';

/**
 * SPEC-253 production acceptance — invoke canonical tenant mailbox poll cron path.
 * Read-only: does not send email or fabricate inbound messages.
 */

require('dotenv').config();

const https = require('node:https');
const http = require('node:http');

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw Object.assign(new Error(`Missing required env: ${name}`), { code: 'runtime_env_missing' });
  }
  return value;
}

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    lib.get(url, (res) => {
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(body) });
        } catch (err) {
          reject(Object.assign(new Error(`Non-JSON response (${res.statusCode}): ${body.slice(0, 500)}`), { cause: err }));
        }
      });
    }).on('error', reject);
  });
}

async function main() {
  const appUrl = (process.env.APP_URL || 'https://pulseforge-leadgen-production.up.railway.app').replace(/\/$/, '');
  const secret = requireEnv('CRON_SECRET');
  const url = `${appUrl}/cron/tenant-mailbox-poll?secret=${encodeURIComponent(secret)}`;
  const { status, body } = await fetchJson(url);
  process.stdout.write(`${JSON.stringify({ status, body }, null, 2)}\n`);

  if (status !== 200) {
    throw Object.assign(new Error(`Expected HTTP 200, got ${status}`), { code: 'spec253_http_status', body });
  }
  if (!body.success) {
    throw Object.assign(new Error('Tenant mailbox poll reported success=false'), { code: 'spec253_poll_failed', body });
  }
  if (body.failed > 0) {
    throw Object.assign(new Error('Tenant mailbox poll reported failed integrations'), { code: 'spec253_poll_failed', body });
  }
}

main().catch((err) => {
  process.stderr.write(`${err.message}\n`);
  if (err.body) process.stderr.write(`${JSON.stringify(err.body, null, 2)}\n`);
  process.exit(1);
});
