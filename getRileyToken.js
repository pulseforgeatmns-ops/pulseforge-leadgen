/**
 * getRileyToken.js
 * ─────────────────
 * Generates a fresh Gmail OAuth token for Riley using a localhost redirect.
 *
 * Usage:
 *   node getRileyToken.js
 *
 * 1. Reads credentials from GMAIL_CREDENTIALS env var or ~/Downloads/riley_credentials.json
 * 2. Prints an auth URL — open it in your browser and grant access
 * 3. Google redirects to localhost:3000 — the code is captured automatically
 * 4. Prints the full token JSON — copy it into Railway as GMAIL_TOKEN
 */

require('dotenv').config();
const { google } = require('googleapis');
const http = require('http');
const fs = require('fs');
const path = require('path');

const SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/gmail.send',
  'https://www.googleapis.com/auth/gmail.modify'
];

const REDIRECT_URI = 'http://localhost:3000';

function loadCredentials() {
  if (process.env.GMAIL_CREDENTIALS) {
    return JSON.parse(process.env.GMAIL_CREDENTIALS);
  }
  const fallback = path.join(process.env.HOME || '~', 'Downloads', 'riley_credentials.json');
  if (fs.existsSync(fallback)) {
    return JSON.parse(fs.readFileSync(fallback, 'utf8'));
  }
  throw new Error('No credentials found. Set GMAIL_CREDENTIALS env var or place riley_credentials.json in ~/Downloads');
}

async function main() {
  const credentials = loadCredentials();
  const { client_secret, client_id } = credentials.installed;

  const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, REDIRECT_URI);

  const authUrl = oAuth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: SCOPES,
    prompt: 'consent',
  });

  console.log('\n🔐 Riley — Gmail Token Generator');
  console.log('─────────────────────────────────────────');
  console.log('\nOpen this URL in your browser:\n');
  console.log(authUrl);
  console.log('\n─────────────────────────────────────────');
  console.log('Waiting for Google to redirect to localhost:3000...\n');

  const code = await new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      const url = new URL(req.url, REDIRECT_URI);
      const code = url.searchParams.get('code');
      if (!code) {
        res.writeHead(400);
        res.end('Missing code parameter.');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end('<html><body><h2>✓ Authorization successful — you can close this tab.</h2></body></html>');
      server.close();
      resolve(code);
    });
    server.on('error', reject);
    server.listen(3000);
  });

  const { tokens } = await oAuth2Client.getToken(code);

  console.log('✓ Token exchange successful!\n');
  console.log('─────────────────────────────────────────');
  console.log('Copy the JSON below into Railway as GMAIL_TOKEN:\n');
  console.log(JSON.stringify(tokens, null, 2));
  console.log('\n─────────────────────────────────────────');

  const localPath = path.join(__dirname, 'gmail_token.json');
  fs.writeFileSync(localPath, JSON.stringify(tokens, null, 2));
  console.log(`\n✓ Also saved to ${localPath}`);
}

main().catch(err => {
  console.error(err.message);
  process.exit(1);
});
