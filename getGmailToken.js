'use strict';

/**
 * Generate or refresh GMAIL_TOKEN for personal Gmail (market-intel / readonly).
 *
 * Usage:
 *   node getGmailToken.js
 *   npm run gmail:token
 *   node getGmailToken.js --refresh
 *
 * Writes local gmail_token.json (gitignored). Does not commit secrets.
 * Sign in with the personal mailbox you want market-intel to read
 * (e.g. jzmaynard7@gmail.com), not the Riley work account.
 */

require('dotenv').config();

const fs = require('fs');
const http = require('http');
const path = require('path');
const { google } = require('googleapis');

const REDIRECT_URI = 'http://localhost:3001';
const SCOPES = ['https://www.googleapis.com/auth/gmail.readonly'];
const TOKEN_PATH = path.join(__dirname, 'gmail_token.json');
const CREDENTIALS_PATH = path.join(__dirname, 'gmail_credentials.json');
const DOWNLOADS_CREDENTIALS_PATH = path.join(
  process.env.HOME || '.',
  'Downloads',
  'riley_credentials.json'
);

function parseArgs(argv = process.argv.slice(2)) {
  const options = { refresh: false, help: false };
  for (const arg of argv) {
    if (arg === '--refresh') {
      options.refresh = true;
      continue;
    }
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    throw new Error(`Unknown argument: ${arg}`);
  }
  return options;
}

function printHelp() {
  console.log(`Personal Gmail token helper (readonly)

Usage:
  node getGmailToken.js
  npm run gmail:token
  node getGmailToken.js --refresh

Options:
  --refresh   Refresh an existing GMAIL_TOKEN / gmail_token.json without browser consent
  --help      Show this help

Output:
  Writes gmail_token.json in the repo root (gitignored).
  Prints setup instructions for GMAIL_TOKEN env usage.

Use the personal Gmail account intended for market-intel ingestion.
Riley continues to use RILEY_* tokens separately.
`);
}

function loadCredentials() {
  if (process.env.GMAIL_CREDENTIALS) {
    return JSON.parse(process.env.GMAIL_CREDENTIALS);
  }
  if (fs.existsSync(CREDENTIALS_PATH)) {
    return JSON.parse(fs.readFileSync(CREDENTIALS_PATH, 'utf8'));
  }
  if (fs.existsSync(DOWNLOADS_CREDENTIALS_PATH)) {
    return JSON.parse(fs.readFileSync(DOWNLOADS_CREDENTIALS_PATH, 'utf8'));
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
    'No Gmail OAuth credentials found. Set GMAIL_CREDENTIALS, provide gmail_credentials.json, or set GOOGLE_CLIENT_ID + GOOGLE_CLIENT_SECRET.'
  );
}

function createOAuthClient() {
  const credentials = loadCredentials();
  const credKeys = credentials.installed || credentials.web;
  if (!credKeys?.client_id || !credKeys?.client_secret) {
    throw new Error('Gmail OAuth credentials must include client_id and client_secret');
  }
  return {
    oAuth2Client: new google.auth.OAuth2(
      credKeys.client_id,
      credKeys.client_secret,
      REDIRECT_URI
    ),
    client_id: credKeys.client_id,
    client_secret: credKeys.client_secret,
  };
}

function loadExistingToken() {
  if (process.env.GMAIL_TOKEN) {
    return JSON.parse(process.env.GMAIL_TOKEN);
  }
  if (fs.existsSync(TOKEN_PATH)) {
    return JSON.parse(fs.readFileSync(TOKEN_PATH, 'utf8'));
  }
  return null;
}

function writeTokenFile(tokens) {
  fs.writeFileSync(TOKEN_PATH, `${JSON.stringify(tokens, null, 2)}\n`, 'utf8');
  return TOKEN_PATH;
}

function printSuccessInstructions(tokens, { writtenPath, refreshed = false } = {}) {
  console.log(refreshed ? '\n✓ Gmail token refreshed.\n' : '\n✓ Personal Gmail OAuth tokens received.\n');
  if (writtenPath) {
    console.log(`Wrote local token file (gitignored): ${writtenPath}`);
  }
  console.log('\n── Setup ────────────────────────────────────────────');
  console.log('1. Prefer leaving gmail_token.json in place for local market-intel runs.');
  console.log('2. Or set env var GMAIL_TOKEN to the full JSON below (not just refresh_token).');
  console.log('3. Use personal inbox market-intel auth:');
  console.log('     npm run market:intel:preflight -- --show-account --token-source=gmail --days=365 --label=MARKET_INTEL --limit=10');
  console.log('4. Confirm Authenticated account is your personal Gmail, not the Riley work mailbox.');
  console.log('5. Do not commit gmail_token.json / credentials.');
  console.log('\n── Token JSON ───────────────────────────────────────');
  console.log(JSON.stringify(tokens, null, 2));
  if (!tokens.refresh_token) {
    console.warn(
      '\n⚠ Google did not return a refresh_token. Revoke this app for the Google account, then re-run with consent.'
    );
  }
  console.log('────────────────────────────────────────────────────\n');
}

async function refreshExistingToken() {
  const existing = loadExistingToken();
  if (!existing?.refresh_token) {
    throw new Error(
      'No refreshable token found. Run without --refresh to authorize in the browser first.'
    );
  }

  const { client_id, client_secret } = createOAuthClient();
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id,
      client_secret,
      refresh_token: existing.refresh_token,
      grant_type: 'refresh_token',
    }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const details = data.error_description || data.error || res.statusText;
    throw new Error(`Google token refresh failed: ${details}`);
  }

  const tokens = {
    ...existing,
    access_token: data.access_token,
    refresh_token: existing.refresh_token,
    expiry_date: Date.now() + Number(data.expires_in || 3600) * 1000,
    token_type: data.token_type || existing.token_type || 'Bearer',
    scope: data.scope || existing.scope || SCOPES.join(' '),
  };
  const writtenPath = writeTokenFile(tokens);
  printSuccessInstructions(tokens, { writtenPath, refreshed: true });
  return tokens;
}

async function runBrowserConsentFlow() {
  const { oAuth2Client } = createOAuthClient();
  const authUrl = oAuth2Client.generateAuthUrl({
    access_type: 'offline',
    prompt: 'consent',
    scope: SCOPES,
  });

  console.log('\nPersonal Gmail token helper (readonly scope)');
  console.log('Sign in with the personal mailbox you want market-intel to read.\n');
  console.log('Open this URL in your browser:\n');
  console.log(authUrl);
  console.log('\nWaiting for Google to redirect to localhost:3001 ...\n');

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
        printSuccessInstructions(tokens, { writtenPath });

        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(
          '<html><body><h2>Authorization successful!</h2><p>You can close this tab.</p></body></html>'
        );

        server.close(() => resolve(tokens));
      } catch (err) {
        res.writeHead(500);
        res.end('Authorization failed. Check the terminal.');
        server.close(() => reject(err));
      }
    });

    server.on('error', reject);
    server.listen(3001);
  });
}

async function main(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  if (options.help) {
    printHelp();
    return { ok: true, help: true };
  }
  if (options.refresh) {
    await refreshExistingToken();
    return { ok: true, refreshed: true };
  }
  await runBrowserConsentFlow();
  return { ok: true, refreshed: false };
}

module.exports = {
  SCOPES,
  TOKEN_PATH,
  loadCredentials,
  loadExistingToken,
  parseArgs,
  printHelp,
  writeTokenFile,
};

if (require.main === module) {
  main().catch((err) => {
    console.error(err && err.message ? err.message : err);
    process.exitCode = 1;
  });
}
