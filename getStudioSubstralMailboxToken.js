'use strict';

/**
 * Bootstrap Google OAuth refresh token for Studio Substral (hello@studiosubstral.com).
 *
 * Scope: https://mail.google.com/ (required for IMAP/SMTP XOAUTH2).
 *
 * Usage:
 *   node getStudioSubstralMailboxToken.js
 *
 * Writes studio_substral_mailbox_token.json (gitignored). Set Railway env:
 *   STUDIO_SUBSTRAL_GOOGLE_REFRESH_TOKEN=<refresh_token value only>
 */

require('dotenv').config();

const fs = require('fs');
const http = require('http');
const path = require('path');
const { google } = require('googleapis');
const { GOOGLE_MAIL_IMAP_SCOPE } = require('./utils/googleMailboxOAuth');

const REDIRECT_URI = 'http://localhost:3003';
const SCOPES = [GOOGLE_MAIL_IMAP_SCOPE];
const TOKEN_PATH = path.join(__dirname, 'studio_substral_mailbox_token.json');
const CREDENTIALS_PATH = path.join(__dirname, 'gmail_credentials.json');

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
  return new google.auth.OAuth2(credKeys.client_id, credKeys.client_secret, REDIRECT_URI);
}

async function main() {
  const oAuth2Client = createOAuthClient();
  const authUrl = oAuth2Client.generateAuthUrl({
    access_type: 'offline',
    prompt: 'consent',
    scope: SCOPES,
  });

  console.log('\nStudio Substral mailbox OAuth bootstrap');
  console.log('Sign in as hello@studiosubstral.com\n');
  console.log(authUrl);
  console.log('\nWaiting for redirect to localhost:3003 ...\n');

  await new Promise((resolve, reject) => {
    const server = http.createServer(async (req, res) => {
      try {
        const url = new URL(req.url, REDIRECT_URI);
        const code = url.searchParams.get('code');
        if (!code) {
          res.writeHead(400);
          res.end('Missing code');
          return;
        }
        const { tokens } = await oAuth2Client.getToken(code);
        fs.writeFileSync(TOKEN_PATH, `${JSON.stringify(tokens, null, 2)}\n`, 'utf8');
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end('<html><body><h2>Studio Substral OAuth OK</h2></body></html>');
        server.close(() => resolve(tokens));
        const credentials = loadCredentials();
        const credKeys = credentials.installed || credentials.web;
        console.log('\nOAuth client used for this refresh token:');
        console.log(`  client_id suffix: ...${String(credKeys.client_id).slice(-8)}`);
        console.log(`  scope granted: ${tokens.scope || SCOPES.join(' ')}`);
        if (tokens.access_token) {
          try {
            const profileRes = await fetch('https://openidconnect.googleapis.com/v1/userinfo', {
              headers: { Authorization: `Bearer ${tokens.access_token}` },
            });
            if (profileRes.ok) {
              const profile = await profileRes.json();
              console.log(`  signed-in mailbox: ${profile.email || '(unknown)'}`);
              if (profile.email && profile.email.toLowerCase() !== 'hello@studiosubstral.com') {
                console.warn('  WARNING: expected hello@studiosubstral.com — regenerate with the correct account.');
              }
            }
          } catch (_profileErr) {
            // userinfo optional at bootstrap
          }
        }
        console.log('\nSet STUDIO_SUBSTRAL_GOOGLE_REFRESH_TOKEN to:\n');
        console.log(tokens.refresh_token || '(no refresh_token — re-run with prompt=consent)');
      } catch (err) {
        server.close(() => reject(err));
      }
    });
    server.listen(3003);
  });
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err?.message || err);
    process.exitCode = 1;
  });
}
