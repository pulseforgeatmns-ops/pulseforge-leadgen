require('dotenv').config();
const { google } = require('googleapis');
const http = require('http');
const fs = require('fs');

const CREDENTIALS_PATH = './sheets_credentials.json';
const REDIRECT_URI = 'http://localhost:3001';
const SCOPES = [
  'https://www.googleapis.com/auth/spreadsheets',
  'https://www.googleapis.com/auth/drive.file',
];

const credentials = JSON.parse(fs.readFileSync(CREDENTIALS_PATH));
const { client_id, client_secret } = credentials.installed || credentials.web;

const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, REDIRECT_URI);

const authUrl = oAuth2Client.generateAuthUrl({
  access_type: 'offline',
  scope: SCOPES,
  prompt: 'consent',
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

  console.log('\n✓ Tokens received.');
  console.log('\nAdd this to Railway as GOOGLE_SHEETS_REFRESH_TOKEN:\n');
  console.log(tokens.refresh_token);

  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.end('<html><body><h2>Authorization successful! You can close this tab.</h2></body></html>');

  server.close();
  process.exit(0);
});

server.listen(3001);
console.log('\nWaiting for Google to redirect...\n');
