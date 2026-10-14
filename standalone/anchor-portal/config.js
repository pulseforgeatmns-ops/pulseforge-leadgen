'use strict';
const MARKER = 'anchor-portal-synthetic-only-v1';
function config(env) {
  for (const key of Object.keys(env)) {
    if ((!key.startsWith('PORTAL_') && /(_API_KEY|_TOKEN|_SECRET|_PASSWORD|_DATABASE_URL)$/.test(key) && env[key]) || (/^(DATABASE_URL|DATABASE_SSL|ADMIN_|DASHBOARD_|APP_URL|SESSION_SECRET|PG|BREVO_|TWILIO_|ANTHROPIC_|OPENAI_|GOOGLE_|GMAIL_|FACEBOOK_|LINKEDIN_|BUFFER_|STRIPE_|CRON_|SIGNAL_)/.test(key) && env[key]) || (key.startsWith('PORTAL_') && !['PORTAL_MODE','PORTAL_DATABASE_URL','PORTAL_ORIGIN','PORTAL_SESSION_SECRET','PORTAL_SYNTHETIC_MARKER'].includes(key))) throw new Error('Unexpected application configuration: ' + key);
  }
  if (!['local', 'hosted'].includes(env.PORTAL_MODE) || env.PORTAL_SYNTHETIC_MARKER !== MARKER) throw new Error('Explicit synthetic mode and marker required');
  const origin = new URL(env.PORTAL_ORIGIN);
  const db = new URL(env.PORTAL_DATABASE_URL);
  const port = Number(env.PORT);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('Explicit PORT required');
  if (origin.href !== origin.origin + '/' || origin.username || origin.password) throw new Error('Canonical origin required');
  if (!['postgres:', 'postgresql:'].includes(db.protocol) || db.pathname !== '/anchor_portal_synthetic' || db.search || db.hash) throw new Error('Dedicated synthetic database required');
  if (!/^[a-f0-9]{64,}$/i.test(env.PORTAL_SESSION_SECRET || '')) throw new Error('Secure session secret required');
  if (env.PORTAL_MODE === 'local' && (origin.origin !== `http://127.0.0.1:${port}` || db.hostname !== '127.0.0.1')) throw new Error('Local mode requires loopback');
  if (env.PORTAL_MODE === 'hosted' && (origin.protocol !== 'https:' || ['localhost','127.0.0.1'].includes(origin.hostname) || !db.password)) throw new Error('Hosted mode requires HTTPS and authenticated dedicated database');
  return { mode: env.PORTAL_MODE, origin: origin.origin, port, databaseUrl: db.href, secret: env.PORTAL_SESSION_SECRET, marker: MARKER };
}
module.exports = { config, MARKER };
