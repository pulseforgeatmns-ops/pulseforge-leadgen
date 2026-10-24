#!/usr/bin/env node
'use strict';

/** Explicit operator enrollment. No automatic production access or activation. */
const http = require('node:http');
const { createOAuthAuthorization, completeOAuthAuthorization, PILOT_CLIENT_ID, PILOT_MAILBOX } = require('../services/clarityGmail');

async function main(argv = process.argv.slice(2), env = process.env) {
  if (argv.length === 1 && argv[0] === '--help') {
    console.log('Usage: node scripts/clarityOAuth.js --confirm=authorize-clarity-client-10 --sender=<verified exact Microsoft sender> --project-id=<verified Clarity project ID> [--port=8787]');
    console.log('Requires explicit owner approval and dedicated secret-manager configuration. Saves a disabled integration only.');
    return;
  }
  const options = Object.fromEntries(argv.map((arg) => {
    const match = arg.match(/^--([a-z-]+)=(.*)$/);
    if (!match) throw new Error('clarity_oauth_arguments_invalid');
    return [match[1], match[2]];
  }));
  if (Object.keys(options).some((key) => !['confirm', 'sender', 'project-id', 'port'].includes(key))
    || options.confirm !== 'authorize-clarity-client-10' || !options.sender || !options['project-id']) {
    throw new Error('Usage: node scripts/clarityOAuth.js --confirm=authorize-clarity-client-10 --sender=<verified exact Microsoft sender> --project-id=<verified Clarity project ID> [--port=8787]');
  }
  const port = Number(options.port || 8787);
  if (!Number.isSafeInteger(port) || port < 1024 || port > 65535) throw new Error('clarity_oauth_port_invalid');
  if (!env.DATABASE_URL) throw new Error('clarity_database_url_missing');
  const { authorizationUrl, pending } = createOAuthAuthorization({ clientId: PILOT_CLIENT_ID, mailboxEmail: PILOT_MAILBOX, digestSender: options.sender, projectId: options['project-id'], redirectUri: `http://127.0.0.1:${port}/oauth/callback`, env });
  const { Pool } = require('pg');
  const { ClarityStore } = require('../packages/max/clarity/store');
  // A standalone pool, not db.js's shared application pool.
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const store = new ClarityStore(pool);
  let server;
  let timeout;
  try {
    // Read-only schema/connection check before asking the owner for consent.
    await pool.query('SELECT client_id FROM clarity_integrations WHERE client_id=$1', [PILOT_CLIENT_ID]);
    await new Promise((resolve, reject) => {
      server = http.createServer(async (request, response) => {
        response.setHeader('Content-Type', 'text/plain; charset=utf-8');
        response.setHeader('Cache-Control', 'no-store');
        response.setHeader('Referrer-Policy', 'no-referrer');
        const url = new URL(request.url, `http://127.0.0.1:${port}`);
        if (request.method !== 'GET' || url.pathname !== '/oauth/callback') { response.writeHead(404); response.end('Not found.'); return; }
        if (url.searchParams.get('state') !== pending.state) { response.writeHead(400); response.end('Invalid OAuth state. Restart enrollment if needed.'); return; }
        if (pending.used) { response.writeHead(409); response.end('This authorization callback has already been handled.'); return; }
        if (url.searchParams.has('error')) { response.writeHead(400); response.end('Authorization was not granted. No polling has been enabled.'); reject(new Error('clarity_oauth_consent_denied')); return; }
        try {
          await completeOAuthAuthorization({ pending, code: url.searchParams.get('code'), state: url.searchParams.get('state'), store, env });
          response.end('Clarity readonly authorization saved securely. Ingestion remains disabled. You can close this tab.');
          resolve();
        } catch (error) {
          response.writeHead(400);
          response.end('Authorization could not be saved. No polling has been enabled. See the sanitized terminal error.');
          reject(error);
        }
      });
      server.on('error', reject);
      server.listen(port, '127.0.0.1', () => {
        console.log('Open the following Google URL on this computer and authorize only jacob@goanchorcleaning.com.');
        console.log('The dedicated grant reads mail via Gmail REST; it cannot send, delete, or modify mail. Ingestion stays disabled.');
        console.log(authorizationUrl);
      });
      timeout = setTimeout(() => reject(new Error('clarity_oauth_consent_timeout')), 10 * 60 * 1000);
    });
    console.log('Encrypted readonly grant saved for client 10. Ingestion is still disabled; separate activation approval is required.');
  } finally {
    clearTimeout(timeout);
    if (server) await new Promise((resolve) => server.close(resolve));
    await pool.end();
  }
}

if (require.main === module) main().catch((error) => {
  const safe = /^clarity_[a-z0-9_]+$/.test(error.code || error.message || '') ? error.code || error.message : 'clarity_oauth_setup_failed';
  console.error(safe);
  process.exitCode = 1;
});

module.exports = { main };
