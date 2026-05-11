/**
 * getRileyToken.js
 * ─────────────────
 * Generates a fresh Gmail OAuth token for Riley.
 *
 * Usage:
 *   node getRileyToken.js
 *
 * 1. Reads credentials from GMAIL_CREDENTIALS env var or ~/Downloads/riley_credentials.json
 * 2. Prints an auth URL — open it in your browser and grant access
 * 3. Paste the auth code when prompted
 * 4. Prints the full token JSON — copy it into Railway as GMAIL_TOKEN
 */

require('dotenv').config();
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/gmail.send',
  'https://www.googleapis.com/auth/gmail.modify'
];

// Installed-app redirect URI — works without a live redirect server
const REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob';

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
    prompt: 'consent', // force refresh_token to be included
  });

  console.log('\n🔐 Riley — Gmail Token Generator');
  console.log('─────────────────────────────────────────');
  console.log('\nOpen this URL in your browser and grant access:\n');
  console.log(authUrl);
  console.log('\n─────────────────────────────────────────');
  console.log('After authorizing, Google will show you a code. Paste it below.\n');

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

  rl.question('Auth code: ', async (code) => {
    rl.close();
    try {
      const { tokens } = await oAuth2Client.getToken(code.trim());
      console.log('\n✓ Token exchange successful!\n');
      console.log('─────────────────────────────────────────');
      console.log('Copy the JSON below into Railway as GMAIL_TOKEN:\n');
      console.log(JSON.stringify(tokens, null, 2));
      console.log('\n─────────────────────────────────────────');

      // Also save locally for immediate use
      const localPath = path.join(__dirname, 'gmail_token.json');
      fs.writeFileSync(localPath, JSON.stringify(tokens, null, 2));
      console.log(`\n✓ Also saved to ${localPath}`);
    } catch (err) {
      console.error('\n✗ Token exchange failed:', err.response?.data || err.message);
      process.exit(1);
    }
  });
}

main().catch(err => {
  console.error(err.message);
  process.exit(1);
});
