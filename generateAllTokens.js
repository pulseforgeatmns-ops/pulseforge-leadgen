require('dotenv').config();
const { google } = require('googleapis');
const http = require('http');

const REDIRECT_URI = 'http://localhost:3001';
const SCOPES = [
  'https://www.googleapis.com/auth/spreadsheets',
  'https://www.googleapis.com/auth/drive.file',
  'https://www.googleapis.com/auth/business.manage',
  'https://www.googleapis.com/auth/calendar',
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/gmail.send',
  'https://www.googleapis.com/auth/gmail.modify',
];

const oAuth2Client = new google.auth.OAuth2(
  process.env.GOOGLE_CLIENT_ID,
  process.env.GOOGLE_CLIENT_SECRET,
  REDIRECT_URI
);

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

  console.log('\n✓ Tokens received.\n');
  console.log('── Full token JSON ──────────────────────────────────');
  console.log(JSON.stringify(tokens, null, 2));
  console.log('\n── Refresh token ────────────────────────────────────');
  console.log(tokens.refresh_token);
  console.log('\n── Next steps ───────────────────────────────────────');
  console.log('Add the following env vars to Railway, all using the refresh_token value above:');
  console.log('  GOOGLE_REFRESH_TOKEN');
  console.log('  GOOGLE_SHEETS_REFRESH_TOKEN');
  console.log('  GMAIL_TOKEN  →  use the full JSON above (not just the refresh_token)');
  console.log('─────────────────────────────────────────────────────\n');

  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.end('<html><body><h2>Authorization successful! You can close this tab.</h2></body></html>');

  server.close();
  process.exit(0);
});

server.listen(3001);
console.log('\nWaiting for Google to redirect...\n');
