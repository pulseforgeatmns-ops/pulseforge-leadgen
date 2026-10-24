'use strict';

/**
 * Generate a Google Ads OAuth refresh token for Penny (Anchor tenant).
 *
 * Scope: https://www.googleapis.com/auth/adwords
 *
 * Usage:
 *   node getGoogleAdsToken.js --print-auth-url
 *   node getGoogleAdsToken.js --code <AUTH_CODE>
 *   node getGoogleAdsToken.js
 *   npm run anchor:google-ads:oauth
 *
 * Credentials (never printed):
 *   GOOGLE_ADS_CLIENT_ID
 *   GOOGLE_ADS_CLIENT_SECRET
 *
 * Writes gitignored files only. Does not print client secret or refresh token.
 * Sign in with the Google account that can access Anchor Ads customer 417-116-4743.
 */

require('dotenv').config({ quiet: true });

const fs = require('fs');
const http = require('http');
const path = require('path');

const ADWORDS_SCOPE = 'https://www.googleapis.com/auth/adwords';
const SCOPES = Object.freeze([ADWORDS_SCOPE]);
const TOKEN_PATH = path.join(__dirname, 'google_ads_token.json');
const ENV_PATH = path.join(__dirname, '.env.anchor-google-ads');
const CREDENTIALS_PATH = path.join(__dirname, 'google_ads_credentials.json');
const DEFAULT_REDIRECT_URI = 'http://127.0.0.1:3003/oauth/callback';
const DEFAULT_LISTEN_HOST = '127.0.0.1';
const DEFAULT_LISTEN_PORT = 3003;
const DEFAULT_MANAGER_ACCOUNT_ID = '2108285531';
const DEFAULT_CUSTOMER_ID = '4171164743';
const TOKEN_ENDPOINT = 'https://oauth2.googleapis.com/token';
const AUTH_ENDPOINT = 'https://accounts.google.com/o/oauth2/v2/auth';

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    help: false,
    printAuthUrl: false,
    code: null,
    redirectUri: null,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg === '--print-auth-url') {
      options.printAuthUrl = true;
      continue;
    }
    if (arg === '--code') {
      const value = argv[i + 1];
      if (!value || value.startsWith('--')) {
        throw new Error('--code requires an authorization code');
      }
      options.code = value;
      i += 1;
      continue;
    }
    if (arg.startsWith('--code=')) {
      options.code = arg.slice('--code='.length);
      if (!options.code) throw new Error('--code requires an authorization code');
      continue;
    }
    if (arg === '--redirect-uri') {
      const value = argv[i + 1];
      if (!value || value.startsWith('--')) {
        throw new Error('--redirect-uri requires a URI');
      }
      options.redirectUri = value;
      i += 1;
      continue;
    }
    if (arg.startsWith('--redirect-uri=')) {
      options.redirectUri = arg.slice('--redirect-uri='.length);
      if (!options.redirectUri) throw new Error('--redirect-uri requires a URI');
      continue;
    }
    throw new Error(`Unknown argument: ${arg}`);
  }
  return options;
}

function printHelp() {
  console.log(`Google Ads OAuth refresh token helper (Penny / Anchor)

Usage:
  node getGoogleAdsToken.js --print-auth-url
  node getGoogleAdsToken.js --code <AUTH_CODE>
  node getGoogleAdsToken.js
  npm run anchor:google-ads:oauth

Sign in as the Google account with access to Anchor Ads 417-116-4743.

Required env:
  GOOGLE_ADS_CLIENT_ID
  GOOGLE_ADS_CLIENT_SECRET

Optional env:
  GOOGLE_ADS_OAUTH_REDIRECT_URI   (default ${DEFAULT_REDIRECT_URI})
  GOOGLE_ADS_MANAGER_ACCOUNT_ID   (default ${DEFAULT_MANAGER_ACCOUNT_ID})
  ANCHOR_GOOGLE_ADS_CUSTOMER_ID   (default ${DEFAULT_CUSTOMER_ID})

Output (gitignored, values not printed):
  google_ads_token.json
  .env.anchor-google-ads

Scope: ${ADWORDS_SCOPE}
`);
}

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function resolveRedirectUri(options = {}) {
  return asText(options.redirectUri)
    || asText(process.env.GOOGLE_ADS_OAUTH_REDIRECT_URI)
    || DEFAULT_REDIRECT_URI;
}

function loadCredentials(env = process.env) {
  if (asText(env.GOOGLE_ADS_CLIENT_ID) && asText(env.GOOGLE_ADS_CLIENT_SECRET)) {
    return {
      client_id: asText(env.GOOGLE_ADS_CLIENT_ID),
      client_secret: asText(env.GOOGLE_ADS_CLIENT_SECRET),
    };
  }
  if (fs.existsSync(CREDENTIALS_PATH)) {
    const parsed = JSON.parse(fs.readFileSync(CREDENTIALS_PATH, 'utf8'));
    const credKeys = parsed.installed || parsed.web || parsed;
    if (asText(credKeys.client_id) && asText(credKeys.client_secret)) {
      return {
        client_id: asText(credKeys.client_id),
        client_secret: asText(credKeys.client_secret),
      };
    }
  }
  const err = new Error(
    'Missing Google Ads OAuth credentials. Set GOOGLE_ADS_CLIENT_ID and GOOGLE_ADS_CLIENT_SECRET.'
  );
  err.code = 'google_ads_oauth_credentials_missing';
  err.missing = ['GOOGLE_ADS_CLIENT_ID', 'GOOGLE_ADS_CLIENT_SECRET'].filter(
    (key) => !asText(env[key])
  );
  throw err;
}

function buildAuthUrl({ clientId, redirectUri, scope = ADWORDS_SCOPE } = {}) {
  const url = new URL(AUTH_ENDPOINT);
  url.searchParams.set('client_id', clientId);
  url.searchParams.set('redirect_uri', redirectUri);
  url.searchParams.set('response_type', 'code');
  url.searchParams.set('scope', scope);
  url.searchParams.set('access_type', 'offline');
  url.searchParams.set('prompt', 'consent');
  url.searchParams.set('include_granted_scopes', 'false');
  return url.toString();
}

function tokenErrorMessage(data, fallback) {
  return asText(data?.error_description) || asText(data?.error) || fallback;
}

async function exchangeCode(code, { credentials, redirectUri, fetchImpl = fetch } = {}) {
  const creds = credentials || loadCredentials();
  const body = new URLSearchParams({
    code,
    client_id: creds.client_id,
    client_secret: creds.client_secret,
    redirect_uri: redirectUri,
    grant_type: 'authorization_code',
  });
  const res = await fetchImpl(TOKEN_ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(`Google token exchange failed: ${tokenErrorMessage(data, res.statusText)}`);
    err.code = 'google_ads_oauth_exchange_failed';
    err.status = res.status;
    throw err;
  }
  if (!asText(data.refresh_token)) {
    const err = new Error(
      'Google did not return a refresh_token. Revoke this app for the Google account, then re-run with consent.'
    );
    err.code = 'google_ads_oauth_refresh_missing';
    throw err;
  }
  return data;
}

function writeTokenFile(tokens, dest = TOKEN_PATH) {
  fs.writeFileSync(dest, `${JSON.stringify(tokens, null, 2)}\n`, 'utf8');
  fs.chmodSync(dest, 0o600);
  return dest;
}

function resolveBindingIds(env = process.env) {
  return {
    managerAccountId: asText(env.GOOGLE_ADS_MANAGER_ACCOUNT_ID) || DEFAULT_MANAGER_ACCOUNT_ID,
    customerId: asText(env.ANCHOR_GOOGLE_ADS_CUSTOMER_ID) || DEFAULT_CUSTOMER_ID,
  };
}

function writeBindingEnvFile(refreshToken, dest = ENV_PATH, env = process.env) {
  const ids = resolveBindingIds(env);
  const lines = [
    `GOOGLE_ADS_MANAGER_ACCOUNT_ID=${ids.managerAccountId}`,
    `ANCHOR_GOOGLE_ADS_CUSTOMER_ID=${ids.customerId}`,
    `ANCHOR_GOOGLE_ADS_REFRESH_TOKEN=${refreshToken}`,
    '',
  ];
  fs.writeFileSync(dest, lines.join('\n'), 'utf8');
  fs.chmodSync(dest, 0o600);
  return {
    path: dest,
    managerAccountId: ids.managerAccountId,
    customerId: ids.customerId,
    hasRefreshToken: Boolean(asText(refreshToken)),
  };
}

function formatSuccessReport({ tokenPath, envPath, managerAccountId, customerId } = {}) {
  return [
    'Google Ads OAuth refresh token generated: yes',
    `Wrote gitignored token file: ${tokenPath}`,
    `Wrote gitignored binding env file: ${envPath}`,
    `GOOGLE_ADS_MANAGER_ACCOUNT_ID=${managerAccountId}`,
    `ANCHOR_GOOGLE_ADS_CUSTOMER_ID=${customerId}`,
    'ANCHOR_GOOGLE_ADS_REFRESH_TOKEN=[written; value not printed]',
    'Client secret was not printed.',
    'Load the binding env (local only, do not commit):',
    '  set -a && . ./.env.anchor-google-ads && set +a',
  ].join('\n');
}

function printAuthUrlOnly(authUrl) {
  console.log('Open this URL in the Google account that can access Anchor Ads 417-116-4743:\n');
  console.log(authUrl);
  console.log('\nAfter consent, exchange the code with:');
  console.log('  node getGoogleAdsToken.js --code <AUTH_CODE>');
}

async function runBrowserConsentFlow({ credentials, redirectUri }) {
  const authUrl = buildAuthUrl({
    clientId: credentials.client_id,
    redirectUri,
  });
  const parsed = new URL(redirectUri);
  const listenHost = parsed.hostname || DEFAULT_LISTEN_HOST;
  const listenPort = parsed.port ? Number(parsed.port) : DEFAULT_LISTEN_PORT;

  console.log('Google Ads OAuth bootstrap (adwords scope)');
  console.log('Sign in as the Google account with access to Anchor Ads 417-116-4743.\n');
  printAuthUrlOnly(authUrl);
  console.log(`\nWaiting for Google to redirect to ${redirectUri} ...\n`);

  return new Promise((resolve, reject) => {
    const server = http.createServer(async (req, res) => {
      try {
        const url = new URL(req.url, redirectUri);
        if (url.pathname !== parsed.pathname && parsed.pathname !== '/') {
          res.writeHead(404);
          res.end('Not found');
          return;
        }
        const error = url.searchParams.get('error');
        if (error) {
          res.writeHead(400);
          res.end('Authorization denied.');
          server.close(() => reject(new Error(`Google OAuth error: ${error}`)));
          return;
        }
        const code = url.searchParams.get('code');
        if (!code) {
          res.writeHead(400);
          res.end('Missing code parameter.');
          return;
        }
        const tokens = await exchangeCode(code, { credentials, redirectUri });
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end('<html><body><h2>Authorization successful.</h2><p>You can close this tab.</p></body></html>');
        server.close(() => resolve(tokens));
      } catch (err) {
        res.writeHead(500);
        res.end('Authorization failed. Check the terminal.');
        server.close(() => reject(err));
      }
    });
    server.on('error', reject);
    server.listen(listenPort, listenHost);
  });
}

function persistTokens(tokens, env = process.env) {
  const tokenPath = writeTokenFile(tokens);
  const binding = writeBindingEnvFile(tokens.refresh_token, ENV_PATH, env);
  return {
    generated: true,
    tokenPath,
    envPath: binding.path,
    managerAccountId: binding.managerAccountId,
    customerId: binding.customerId,
    hasRefreshToken: binding.hasRefreshToken,
  };
}

async function main(argv = process.argv.slice(2), env = process.env) {
  const options = parseArgs(argv);
  if (options.help) {
    printHelp();
    return { ok: true, help: true };
  }

  const credentials = loadCredentials(env);
  const redirectUri = resolveRedirectUri(options);

  if (options.printAuthUrl) {
    const authUrl = buildAuthUrl({
      clientId: credentials.client_id,
      redirectUri,
    });
    printAuthUrlOnly(authUrl);
    return { ok: true, authUrlPrinted: true };
  }

  let tokens;
  if (options.code) {
    tokens = await exchangeCode(options.code, { credentials, redirectUri });
  } else {
    tokens = await runBrowserConsentFlow({ credentials, redirectUri });
  }

  const persisted = persistTokens(tokens, env);
  console.log(`\n${formatSuccessReport(persisted)}\n`);
  return { ok: true, ...persisted };
}

module.exports = {
  ADWORDS_SCOPE,
  SCOPES,
  TOKEN_PATH,
  ENV_PATH,
  CREDENTIALS_PATH,
  DEFAULT_REDIRECT_URI,
  DEFAULT_MANAGER_ACCOUNT_ID,
  DEFAULT_CUSTOMER_ID,
  parseArgs,
  printHelp,
  loadCredentials,
  buildAuthUrl,
  exchangeCode,
  writeTokenFile,
  writeBindingEnvFile,
  formatSuccessReport,
  resolveBindingIds,
  persistTokens,
  main,
};

if (require.main === module) {
  main().catch((err) => {
    console.error(err && err.message ? err.message : err);
    process.exitCode = 1;
  });
}
