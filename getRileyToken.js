require('dotenv').config();
const { google } = require('googleapis');
const http = require('http');
const fs = require('fs');
const path = require('path');

const REDIRECT_URI = 'http://localhost:3001';
const SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/gmail.send',
  'https://www.googleapis.com/auth/gmail.modify'
];

function loadCredentials() {
  if (process.env.GMAIL_CREDENTIALS) return JSON.parse(process.env.GMAIL_CREDENTIALS);
  const fallback = path.join(process.env.HOME || '~', 'Downloads', 'riley_credentials.json');
  if (fs.existsSync(fallback)) return JSON.parse(fs.readFileSync(fallback, 'utf8'));
  throw new Error('No credentials found. Set GMAIL_CREDENTIALS or place riley_credentials.json in ~/Downloads');
}

const credentials = loadCredentials();
const credKeys = credentials.installed || credentials.web;
const { client_id, client_secret } = credKeys;
const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, REDIRECT_URI);

const authUrl = oAuth2Client.generateAuthUrl({
  access_type: 'offline',
  prompt: 'consent',
  scope: SCOPES,
});
console.log('\nOpen this URL in your browser:\n');
console.log(authUrl);

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, REDIRECT_URI);
  const code = url.searchParams.get('code');

  if (!code) {
    res.writeHead(400);
    res.end('Missing code parameter.');
    return;
  }

  const { tokens } = await oAuth2Client.getToken(code);
  console.log('\n✓ Riley OAuth tokens received.\n');
  console.log('Set these Railway env vars:\n');
  console.log(`RILEY_ACCESS_TOKEN=${tokens.access_token || ''}`);
  console.log(`RILEY_REFRESH_TOKEN=${tokens.refresh_token || ''}`);
  if (tokens.expiry_date) console.log(`RILEY_TOKEN_EXPIRY=${tokens.expiry_date}`);
  console.log('\nFull token JSON, for local fallback only:\n');
  console.log(JSON.stringify(tokens, null, 2));

  if (!tokens.refresh_token) {
    console.warn('\n⚠ Google did not return a refresh token. Revoke the old app grant for this Google account, then run this script again.');
  }

  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.end('<html><body><h2>Authorization successful! You can close this tab.</h2></body></html>');

  server.close();
  process.exit(0);
});

server.listen(3001);
console.log('\nWaiting for Google to redirect...\n');
