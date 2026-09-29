'use strict';

/**
 * Bootstrap Google OAuth refresh token for Anchor reply ingestion (IMAP XOAUTH2).
 *
 * Scope: https://mail.google.com/ (required for IMAP — gmail.readonly is REST-only).
 *
 * Usage:
 *   node getAnchorMailboxToken.js
 *   npm run anchor:mailbox:oauth
 *
 * Writes anchor_mailbox_token.json (gitignored). Set Railway env:
 *   ANCHOR_GOOGLE_REFRESH_TOKEN=<refresh_token value only>
 */

require('dotenv').config();

const fs = require('fs');
const http = require('http');
const path = require('path');
const { google } = require('googleapis');
const { GOOGLE_MAIL_IMAP_SCOPE } = require('./utils/googleMailboxOAuth');

const REDIRECT_URI = 'http://localhost:3002';
const SCOPES = [GOOGLE_MAIL_IMAP_SCOPE];
const TOKEN_PATH = path.join(__dirname, 'anchor_mailbox_token.json');
const CREDENTIALS_PATH = path.join(__dirname, 'gmail_credentials.json');

function parseArgs(argv = process.argv.slice(2)) {
  const options = { help: false };
  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') options.help = true;
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return options;
}

function printHelp() {
  console.log(`Anchor mailbox OAuth bootstrap (IMAP XOAUTH2)

Usage:
  node getAnchorMailboxToken.js
  npm run anchor:mailbox:oauth

Sign in as jacob@goanchorcleaning.com when prompted.

Output:
  Writes anchor_mailbox_token.json (gitignored).
  Set ANCHOR_GOOGLE_REFRESH_TOKEN in Railway to the refresh_token value only.

Scope: ${GOOGLE_MAIL_IMAP_SCOPE}
`);
}

function loadCredentials() {
  if (process.env.GMAIL_CREDENTIALS) {
    return JSON.parse(process.env.GMAIL_CREDENTIALS);
  }
  if (fs.existsSync(CREDENTIALS_PATH)) {
    return JSON.parse(fs.readFileSync(CREDENTIALS_PATH, 'utf8'));
  }
  if (process.env.GOOGLE_CLIENT_ID && process.env.GOOGLE_CLIENT_SECRET) {
    return {
      installed: {
        client_id: process.env.GOOGLE_CLIENT_ID,
        client_secret: process.env.GOOGLE_CLIENT_SECRET,
      },
    };
  }
  throw new Error(
    'Missing OAuth client credentials. Set GOOGLE_CLIENT_ID + GOOGLE_CLIENT_SECRET or GMAIL_CREDENTIALS.'
  );
}

function createOAuthClient() {
  const credentials = loadCredentials();
  const credKeys = credentials.installed || credentials.web;
  if (!credKeys?.client_id || !credKeys?.client_secret) {
    throw new Error('OAuth credentials must include client_id and client_secret');
  }
  return new google.auth.OAuth2(credKeys.client_id, credKeys.client_secret, REDIRECT_URI);
}

function writeTokenFile(tokens) {
  fs.writeFileSync(TOKEN_PATH, `${JSON.stringify(tokens, null, 2)}\n`, 'utf8');
  return TOKEN_PATH;
}

function printSuccessInstructions(tokens, writtenPath) {
  console.log('\n✓ Anchor mailbox OAuth tokens received.\n');
  if (writtenPath) console.log(`Wrote local token file (gitignored): ${writtenPath}`);
  console.log('\n── Railway setup ───────────────────────────────────');
  console.log('Set ANCHOR_GOOGLE_REFRESH_TOKEN to the refresh_token value below.');
  console.log('Also ensure GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET are set.');
  console.log('Do not commit anchor_mailbox_token.json.');
  console.log('\n── Refresh token (env value) ───────────────────────');
  console.log(tokens.refresh_token || '(missing — revoke app access and re-run with consent)');
  console.log('\n── Full token JSON (local only) ────────────────────');
  console.log(JSON.stringify(tokens, null, 2));
  console.log('────────────────────────────────────────────────────\n');
}

async function runBrowserConsentFlow() {
  const oAuth2Client = createOAuthClient();
  const authUrl = oAuth2Client.generateAuthUrl({
    access_type: 'offline',
    prompt: 'consent',
    scope: SCOPES,
  });

  console.log('\nAnchor mailbox OAuth bootstrap');
  console.log('Sign in as jacob@goanchorcleaning.com\n');
  console.log('Open this URL in your browser:\n');
  console.log(authUrl);
  console.log('\nWaiting for Google to redirect to localhost:3002 ...\n');

  await new Promise((resolve, reject) => {
    const server = http.createServer(async (req, res) => {
      try {
        const url = new URL(req.url, REDIRECT_URI);
        const code = url.searchParams.get('code');
        if (!code) {
          res.writeHead(400);
          res.end('Missing code parameter.');
          return;
        }
        const { tokens } = await oAuth2Client.getToken(code);
        const writtenPath = writeTokenFile(tokens);
        printSuccessInstructions(tokens, writtenPath);
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end('<html><body><h2>Authorization successful!</h2><p>You can close this tab.</p></body></html>');
        server.close(() => resolve(tokens));
      } catch (err) {
        res.writeHead(500);
        res.end('Authorization failed. Check the terminal.');
        server.close(() => reject(err));
      }
    });
    server.on('error', reject);
    server.listen(3002);
  });
}

async function main(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  if (options.help) {
    printHelp();
    return { ok: true, help: true };
  }
  await runBrowserConsentFlow();
  return { ok: true };
}

module.exports = {
  SCOPES,
  TOKEN_PATH,
  printHelp,
};

if (require.main === module) {
  main().catch((err) => {
    console.error(err?.message || err);
    process.exitCode = 1;
  });
}
