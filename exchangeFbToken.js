/**
 * One-time script: exchange a short-lived Facebook Page access token
 * for a long-lived one (~60 days).
 *
 * Usage:
 *   FACEBOOK_APP_SECRET=<secret> \
 *   FACEBOOK_PAGE_ACCESS_TOKEN=<short-lived-token> \
 *   node exchangeFbToken.js
 *
 * App ID is hardcoded below — change if needed.
 */

require('dotenv').config();
const https = require('https');

const APP_ID        = '1206413571437615';
const APP_SECRET    = process.env.FACEBOOK_APP_SECRET;
const SHORT_TOKEN   = process.env.FACEBOOK_PAGE_ACCESS_TOKEN;

if (!APP_SECRET) {
  console.error('Missing FACEBOOK_APP_SECRET — set it as an env var before running');
  process.exit(1);
}
if (!SHORT_TOKEN) {
  console.error('Missing FACEBOOK_PAGE_ACCESS_TOKEN — set it as an env var before running');
  process.exit(1);
}

const url = `https://graph.facebook.com/oauth/access_token` +
  `?grant_type=fb_exchange_token` +
  `&client_id=${APP_ID}` +
  `&client_secret=${encodeURIComponent(APP_SECRET)}` +
  `&fb_exchange_token=${encodeURIComponent(SHORT_TOKEN)}`;

https.get(url, (res) => {
  let body = '';
  res.on('data', chunk => body += chunk);
  res.on('end', () => {
    try {
      const data = JSON.parse(body);
      if (data.error) {
        console.error('Facebook API error:', JSON.stringify(data.error, null, 2));
        process.exit(1);
      }
      console.log('\n✓ Long-lived token:');
      console.log(data.access_token);
      if (data.expires_in) {
        const days = Math.round(data.expires_in / 86400);
        console.log(`\nExpires in: ~${days} days`);
      }
      console.log('\nSet this in Railway as FACEBOOK_PAGE_ACCESS_TOKEN');
    } catch {
      console.error('Failed to parse response:', body);
      process.exit(1);
    }
  });
}).on('error', err => {
  console.error('Request failed:', err.message);
  process.exit(1);
});
