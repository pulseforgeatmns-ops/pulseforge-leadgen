// One-shot script — get a Google Calendar refresh token via localhost OAuth
// Usage: GOOGLE_CLIENT_ID=xxx GOOGLE_CLIENT_SECRET=xxx node getCalendarToken.js
const http = require('http');
const url  = require('url');
const axios = require('axios');

const CLIENT_ID     = process.env.GOOGLE_CLIENT_ID;
const CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const REDIRECT_URI  = 'http://localhost:3001/oauth/callback';
const SCOPE         = 'https://www.googleapis.com/auth/calendar';
const PORT          = 3001;

if (!CLIENT_ID || !CLIENT_SECRET) {
  console.error('Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET before running.');
  process.exit(1);
}

const authUrl =
  'https://accounts.google.com/o/oauth2/v2/auth' +
  `?client_id=${CLIENT_ID}` +
  `&redirect_uri=${encodeURIComponent(REDIRECT_URI)}` +
  `&response_type=code` +
  `&scope=${encodeURIComponent(SCOPE)}` +
  `&access_type=offline` +
  `&prompt=consent`;

console.log('\n── Google Calendar OAuth ──────────────────────────────');
console.log('Open this URL in your browser:\n');
console.log(authUrl);
console.log('\nWaiting for callback on http://localhost:' + PORT + ' …\n');

const server = http.createServer(async (req, res) => {
  const { pathname, query } = url.parse(req.url, true);
  if (pathname !== '/oauth/callback') {
    res.writeHead(404); res.end(); return;
  }

  const code = query.code;
  if (!code) {
    res.writeHead(400); res.end('Missing code'); return;
  }

  try {
    const tokenRes = await axios.post('https://oauth2.googleapis.com/token', {
      code,
      client_id:     CLIENT_ID,
      client_secret: CLIENT_SECRET,
      redirect_uri:  REDIRECT_URI,
      grant_type:    'authorization_code',
    });

    const { refresh_token, access_token } = tokenRes.data;

    console.log('\n✅ Token exchange successful!\n');
    if (refresh_token) {
      console.log('Add this to Railway / .env:\n');
      console.log(`GOOGLE_CALENDAR_REFRESH_TOKEN=${refresh_token}\n`);
    } else {
      console.log('⚠️  No refresh_token returned. The account may already have one issued.');
      console.log('    Revoke access at https://myaccount.google.com/permissions and re-run.\n');
      console.log('Access token (short-lived):', access_token);
    }

    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end('<h2>✅ Done — check your terminal for the refresh token. You can close this tab.</h2>');
  } catch (err) {
    console.error('Token exchange failed:', err.response?.data || err.message);
    res.writeHead(500); res.end('Token exchange failed — see terminal');
  }

  server.close();
});

server.listen(PORT);
