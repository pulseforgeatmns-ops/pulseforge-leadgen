'use strict';

// Phase A2 review harness: serves the real dashboard + Calls surfaces against
// a seeded disposable PostgreSQL so before/after screenshots can be captured
// locally. Not part of the production deployment.
//
//   node scripts/phaseA2Preview.js
//
// Serves:
//   /setter            current Calls page (real router + real data)
//   /dashboard         current Command Center page
//   /before/setter     pre-Phase-A2 setter HTML (from git HEAD)
//   /before/dashboard  pre-Phase-A2 dashboard HTML (from git HEAD)

const path = require('path');
const { execFileSync } = require('child_process');
const { startDisposablePostgres } = require('../test/helpers/disposablePostgres');

const root = path.join(__dirname, '..');

const BASE_SCHEMA = `
  CREATE EXTENSION IF NOT EXISTS pgcrypto;
  CREATE TABLE clients (id SERIAL PRIMARY KEY, name TEXT NOT NULL, active BOOLEAN NOT NULL DEFAULT true);
  CREATE TABLE users (
    id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL DEFAULT 'x', role TEXT NOT NULL, client_id INTEGER,
    active BOOLEAN NOT NULL DEFAULT true, created_at TIMESTAMPTZ DEFAULT NOW(), last_login_at TIMESTAMPTZ
  );
  CREATE TABLE companies (id SERIAL PRIMARY KEY, client_id INTEGER NOT NULL, name TEXT, location TEXT, website TEXT);
  CREATE TABLE prospects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id INTEGER NOT NULL REFERENCES clients(id),
    company_id INTEGER,
    first_name TEXT, last_name TEXT, email TEXT, phone TEXT, job_title TEXT,
    icp_score INTEGER, status TEXT DEFAULT 'cold', source TEXT, vertical TEXT,
    service_area_match TEXT, city TEXT,
    do_not_contact BOOLEAN DEFAULT false,
    is_synthetic BOOLEAN NOT NULL DEFAULT false, synthetic_label TEXT, callback_completed_at TIMESTAMPTZ,
    notes TEXT, callback_at TIMESTAMPTZ, is_hot BOOLEAN DEFAULT false,
    setter_status TEXT DEFAULT 'new', setter_visible BOOLEAN DEFAULT false,
    setter_updated_at TIMESTAMPTZ DEFAULT NOW(),
    enrichment_attempted BOOLEAN DEFAULT false, assigned_setter_id INTEGER,
    last_contacted_at TIMESTAMPTZ, mrr_value NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW()
  );
  CREATE TABLE touchpoints (
    id SERIAL PRIMARY KEY, prospect_id UUID, channel TEXT, action_type TEXT,
    content_summary TEXT, outcome TEXT, sentiment TEXT, agent_id TEXT, external_ref TEXT,
    client_id INTEGER, created_at TIMESTAMPTZ DEFAULT NOW()
  );
  CREATE TABLE agent_actions (
    id SERIAL PRIMARY KEY, created_by TEXT, action_type TEXT, title TEXT, description TEXT,
    payload JSONB, status TEXT, executed_at TIMESTAMPTZ, result TEXT, client_id INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
  );
  CREATE TABLE agent_log (id SERIAL PRIMARY KEY, agent_name TEXT, action TEXT, created_at TIMESTAMPTZ DEFAULT NOW());
  CREATE TABLE pending_comments (id SERIAL PRIMARY KEY, status TEXT, created_at TIMESTAMPTZ DEFAULT NOW());
  CREATE TABLE post_analytics (id SERIAL PRIMARY KEY, platform TEXT, created_at TIMESTAMPTZ DEFAULT NOW());
  CREATE TABLE activity_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    lead_id UUID REFERENCES prospects(id) ON DELETE CASCADE,
    action_type TEXT NOT NULL, notes TEXT, setter_id TEXT, client_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
  );
  INSERT INTO clients (id, name) VALUES (1, 'Pulseforge');
  SELECT setval(pg_get_serial_sequence('clients', 'id'), 20);
  INSERT INTO users (name, email, role, client_id) VALUES
    ('William Hernandez', 'william@pulseforge.local', 'setter', 1),
    ('Levi Closer', 'levi@pulseforge.local', 'closer', 1),
    ('Jacob Maynard', 'admin@pulseforge.local', 'admin', NULL);
`;

const SEED_PROSPECTS = [
  ['Granite State Cleaning', 'Pat', 'Kelly', 'pat@granitestateclean.example', '603-555-1234', 'cleaning', 82, 'new', 'Manchester NH', 'Granite State Cleaning — granitestateclean.example'],
  ['Elm Street Bistro', 'Jordan', 'Rowe', 'jordan@elmstbistro.example', '(603) 555-2201', 'restaurant', 74, 'contacted', 'Manchester NH', 'Elm Street Bistro — elmstbistro.example'],
  ['Queen City Fitness', 'Sam', 'Ortiz', 'sam@qcfit.example', '6035558845', 'fitness', 71, 'follow_up', 'Manchester NH', 'Queen City Fitness — qcfit.example'],
  ['Riverside Salon', 'Alex', 'Nguyen', 'alex@riversidesalon.example', '603 555 7710', 'salon', 68, 'new', 'Bedford NH', 'Riverside Salon — riversidesalon.example'],
  ['Merrimack Auto Care', 'Casey', 'Dube', 'casey@merrimackauto.example', '603-555-0042', 'auto', 77, 'booked', 'Manchester NH', 'Merrimack Auto Care — merrimackauto.example'],
];

async function main() {
  const pg = await startDisposablePostgres('phase-a2-preview-');
  process.env.DATABASE_URL = pg.connectionString;
  process.env.DATABASE_SSL = 'false';
  delete process.env.BREVO_API_KEY;

  const pool = require('../db');
  await pool.query(BASE_SCHEMA);

  const express = require('express');
  const app = express();
  app.use(express.json());
  // Fixed operator session so every surface renders authenticated.
  const setter = (await pool.query(`SELECT id, name, email, role, client_id FROM users WHERE role = 'setter'`)).rows[0];
  app.use((req, res, next) => {
    req.session = { user: setter, active_client_id: 1 };
    next();
  });
  app.use('/shared', express.static(path.join(root, 'public', 'shared')));
  app.use(express.static(root));
  app.use('/', require('../routes/workspace'));
  app.use('/setter', require('../routes/setter'));
  app.use('/api/setter', require('../routes/setter'));
  app.get('/dashboard', (_req, res) => res.sendFile(path.join(root, 'public', 'dashboard.html')));
  app.get('/api/me', (req, res) => res.json({ user: req.session.user, active_client_id: 1 }));

  // "Before" surfaces from git HEAD for comparison screenshots.
  const beforeSetter = execFileSync('git', ['show', 'HEAD:public/setter-dashboard.html'], { cwd: root, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024 });
  const beforeDashboard = execFileSync('git', ['show', 'HEAD:public/dashboard.html'], { cwd: root, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024 });
  app.get('/before/setter', (_req, res) => res.type('html').send(beforeSetter));
  app.get('/before/dashboard', (_req, res) => res.type('html').send(beforeDashboard));

  const server = app.listen(4620, '127.0.0.1', async () => {
    console.log('[phase-a2-preview] http://127.0.0.1:4620/setter  (Calls, after)');
    console.log('[phase-a2-preview] http://127.0.0.1:4620/dashboard  (Command Center, after)');
    console.log('[phase-a2-preview] http://127.0.0.1:4620/before/setter  (before)');
    console.log('[phase-a2-preview] http://127.0.0.1:4620/before/dashboard  (before)');
  });

  // Seed after listen; setter router migrations run on first request.
  await fetch('http://127.0.0.1:4620/setter/api/leads').catch(() => {});
  await new Promise(resolve => setTimeout(resolve, 1500));
  for (const [company, first, last, email, phone, vertical, score, stage, area, notes] of SEED_PROSPECTS) {
    const companyRow = await pool.query(
      `INSERT INTO companies (client_id, name, location) VALUES (1, $1, $2) RETURNING id`,
      [company, area]
    );
    await pool.query(`
      INSERT INTO prospects
        (client_id, company_id, first_name, last_name, email, phone, job_title, icp_score,
         status, source, vertical, service_area_match, setter_status, setter_visible, notes,
         callback_at, is_hot, booked_at)
      VALUES (1, $1, $2, $3, $4, $5, 'Owner', $6, 'cold', 'scout', $7, $8, $9, true, $10,
        CASE WHEN $9 = 'follow_up' THEN NOW() + INTERVAL '3 hours' END,
        $6 >= 80,
        CASE WHEN $9 = 'booked' THEN NOW() - INTERVAL '1 day' END)
    `, [companyRow.rows[0].id, first, last, email, phone, score, vertical, area, stage, notes]);
  }
  console.log('[phase-a2-preview] seeded', SEED_PROSPECTS.length, 'prospects — ready');

  process.on('SIGINT', async () => {
    server.close();
    await pool.end().catch(() => {});
    await pg.stop();
    process.exit(0);
  });
}

main().catch(err => {
  console.error('[phase-a2-preview] failed:', err);
  process.exit(1);
});
