'use strict';

const pool = require('../db');
const { bcrypt } = require('../middleware/auth');
const { ROLE_CHECK } = require('./userRoles');

const ANCHOR_OPERATOR_CLIENT_ID = 10;
const DEMO_LOCATION_SLUG = 'demo-riverside-law';

async function refreshUsersRoleConstraint() {
  await pool.query('SELECT pg_advisory_lock(91720260518)');
  try {
    const { rows: existing } = await pool.query(`
      SELECT con.conname
      FROM pg_constraint con
      JOIN pg_class cls ON cls.oid = con.conrelid
      WHERE cls.relname = 'users'
        AND con.contype = 'c'
        AND pg_get_constraintdef(con.oid) ILIKE '%role%'
    `);
    for (const row of existing) {
      await pool.query(`ALTER TABLE users DROP CONSTRAINT IF EXISTS "${row.conname}"`);
    }
    await pool.query(`
      ALTER TABLE users ADD CONSTRAINT users_role_check
      CHECK (role IN (${ROLE_CHECK}))
    `);
  } finally {
    await pool.query('SELECT pg_advisory_unlock(91720260518)');
  }
}

async function ensureAnchorPortalTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS anchor_portal_locations (
      id SERIAL PRIMARY KEY,
      operator_client_id INTEGER NOT NULL DEFAULT ${ANCHOR_OPERATOR_CLIENT_ID},
      name TEXT NOT NULL,
      slug TEXT NOT NULL UNIQUE,
      address TEXT,
      is_demo BOOLEAN NOT NULL DEFAULT false,
      next_scheduled_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS anchor_portal_scope_sections (
      id SERIAL PRIMARY KEY,
      location_id INTEGER NOT NULL REFERENCES anchor_portal_locations(id) ON DELETE CASCADE,
      title TEXT NOT NULL,
      sort_order INTEGER NOT NULL DEFAULT 0
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS anchor_portal_scope_items (
      id SERIAL PRIMARY KEY,
      section_id INTEGER NOT NULL REFERENCES anchor_portal_scope_sections(id) ON DELETE CASCADE,
      label TEXT NOT NULL,
      sort_order INTEGER NOT NULL DEFAULT 0
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS anchor_portal_user_locations (
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      location_id INTEGER NOT NULL REFERENCES anchor_portal_locations(id) ON DELETE CASCADE,
      PRIMARY KEY (user_id, location_id)
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS anchor_portal_visits (
      id SERIAL PRIMARY KEY,
      location_id INTEGER NOT NULL REFERENCES anchor_portal_locations(id) ON DELETE CASCADE,
      cleaner_id INTEGER REFERENCES users(id),
      scheduled_at TIMESTAMPTZ,
      started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      status TEXT NOT NULL DEFAULT 'scheduled'
        CHECK (status IN ('scheduled', 'in_progress', 'completed', 'cancelled')),
      visit_notes TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS anchor_portal_visit_items (
      id SERIAL PRIMARY KEY,
      visit_id INTEGER NOT NULL REFERENCES anchor_portal_visits(id) ON DELETE CASCADE,
      section_title TEXT NOT NULL,
      item_label TEXT NOT NULL,
      sort_order INTEGER NOT NULL DEFAULT 0,
      outcome TEXT NOT NULL DEFAULT 'pending'
        CHECK (outcome IN ('pending', 'completed', 'unable_to_complete', 'issue_detected', 'not_applicable')),
      exception_reason TEXT,
      completed_at TIMESTAMPTZ
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS anchor_portal_issues (
      id SERIAL PRIMARY KEY,
      location_id INTEGER NOT NULL REFERENCES anchor_portal_locations(id) ON DELETE CASCADE,
      visit_id INTEGER REFERENCES anchor_portal_visits(id) ON DELETE SET NULL,
      reported_by_user_id INTEGER REFERENCES users(id),
      source TEXT NOT NULL DEFAULT 'client'
        CHECK (source IN ('client', 'cleaner', 'operator')),
      title TEXT,
      description TEXT NOT NULL,
      room_category TEXT,
      severity TEXT NOT NULL DEFAULT 'medium'
        CHECK (severity IN ('low', 'medium', 'high')),
      status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'acknowledged', 'resolved')),
      acknowledged_at TIMESTAMPTZ,
      acknowledged_by INTEGER REFERENCES users(id),
      assigned_to INTEGER REFERENCES users(id),
      resolution TEXT,
      resolved_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS anchor_portal_evidence (
      id SERIAL PRIMARY KEY,
      visit_id INTEGER REFERENCES anchor_portal_visits(id) ON DELETE CASCADE,
      issue_id INTEGER REFERENCES anchor_portal_issues(id) ON DELETE CASCADE,
      kind TEXT NOT NULL DEFAULT 'photo',
      mime_type TEXT,
      data_url TEXT NOT NULL,
      caption TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (visit_id IS NOT NULL OR issue_id IS NOT NULL)
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_anchor_portal_visits_location_scheduled
      ON anchor_portal_visits (location_id, scheduled_at DESC)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_anchor_portal_issues_location_status
      ON anchor_portal_issues (location_id, status, created_at DESC)
  `);
}

function demoScopeSections() {
  return [
    {
      title: 'BREAK ROOM / KITCHEN',
      items: [
        'Counters wiped and sanitized',
        'Sink cleaned and fixtures polished',
        'Tables and chairs wiped',
        'Sweep / vacuum floor',
        'Mop hard surfaces',
        'Trash emptied and liners replaced',
        'Crumbs under break table (detail check)',
      ],
    },
    {
      title: 'RESTROOMS',
      items: [
        'Toilets cleaned and sanitized',
        'Sinks and mirrors',
        'Floors mopped',
        'Supplies restocked',
      ],
    },
    {
      title: 'RECEPTION / COMMON',
      items: [
        'Glass entry doors cleaned',
        'Reception desk wiped',
        'Vacuum common area rugs',
      ],
    },
  ];
}

async function insertScopeForLocation(locationId, sections = demoScopeSections()) {
  for (let si = 0; si < sections.length; si += 1) {
    const section = sections[si];
    const { rows: secRows } = await pool.query(
      `INSERT INTO anchor_portal_scope_sections (location_id, title, sort_order)
       VALUES ($1, $2, $3)
       RETURNING id`,
      [locationId, section.title, si]
    );
    const sectionId = secRows[0].id;
    for (let ii = 0; ii < section.items.length; ii += 1) {
      await pool.query(
        `INSERT INTO anchor_portal_scope_items (section_id, label, sort_order)
         VALUES ($1, $2, $3)`,
        [sectionId, section.items[ii], ii]
      );
    }
  }
}

async function copyScopeToVisitItems(visitId, locationId) {
  const { rows } = await pool.query(
    `SELECT s.title AS section_title, i.label AS item_label, i.sort_order
     FROM anchor_portal_scope_sections s
     JOIN anchor_portal_scope_items i ON i.section_id = s.id
     WHERE s.location_id = $1
     ORDER BY s.sort_order, i.sort_order`,
    [locationId]
  );
  for (const row of rows) {
    await pool.query(
      `INSERT INTO anchor_portal_visit_items
         (visit_id, section_title, item_label, sort_order, outcome)
       VALUES ($1, $2, $3, $4, 'pending')`,
      [visitId, row.section_title, row.item_label, row.sort_order]
    );
  }
}

async function ensureDemoUser({ email, name, role, password, locationId }) {
  const normalized = email.toLowerCase().trim();
  const hash = await bcrypt.hash(password, 12);
  const { rows } = await pool.query(
    `INSERT INTO users (name, email, password_hash, role, client_id, active, email_verified)
     VALUES ($1, $2, $3, $4, $5, true, true)
     ON CONFLICT (email) DO UPDATE SET
       name = EXCLUDED.name,
       role = EXCLUDED.role,
       client_id = EXCLUDED.client_id,
       password_hash = EXCLUDED.password_hash,
       active = true,
       email_verified = true
     RETURNING id`,
    [name, normalized, hash, role, ANCHOR_OPERATOR_CLIENT_ID]
  );
  const userId = rows[0].id;
  if (locationId) {
    await pool.query(
      `INSERT INTO anchor_portal_user_locations (user_id, location_id)
       VALUES ($1, $2)
       ON CONFLICT DO NOTHING`,
      [userId, locationId]
    );
  }
  return userId;
}

async function seedDemoPortalData() {
  const { rows: existing } = await pool.query(
    `SELECT id FROM anchor_portal_locations WHERE slug = $1 LIMIT 1`,
    [DEMO_LOCATION_SLUG]
  );
  if (existing.length) return { seeded: false, locationId: existing[0].id };

  const demoPassword = process.env.ANCHOR_PORTAL_DEMO_PASSWORD || 'AnchorDemo2026!';

  const nextService = new Date();
  nextService.setDate(nextService.getDate() + ((3 - nextService.getDay() + 7) % 7) || 3);
  nextService.setHours(18, 0, 0, 0);

  const { rows: locRows } = await pool.query(
    `INSERT INTO anchor_portal_locations
       (operator_client_id, name, slug, address, is_demo, next_scheduled_at)
     VALUES ($1, $2, $3, $4, true, $5)
     RETURNING id`,
    [
      ANCHOR_OPERATOR_CLIENT_ID,
      'Demo — Riverside Law Office',
      DEMO_LOCATION_SLUG,
      '245 Elm St, Manchester, NH',
      nextService.toISOString(),
    ]
  );
  const locationId = locRows[0].id;
  await insertScopeForLocation(locationId);

  const cleanerId = await ensureDemoUser({
    email: 'demo-cleaner@goanchorcleaning.com',
    name: 'Jordan Lee',
    role: 'cleaner',
    password: demoPassword,
    locationId,
  });

  await ensureDemoUser({
    email: 'demo-client@goanchorcleaning.com',
    name: 'Alex Morgan',
    role: 'facility_client',
    password: demoPassword,
    locationId,
  });

  const now = new Date();
  const visitHistory = [];
  for (let i = 8; i >= 1; i -= 1) {
    const scheduled = new Date(now);
    scheduled.setDate(scheduled.getDate() - i * 3);
    scheduled.setHours(18, 42, 0, 0);
    const completed = new Date(scheduled);
    completed.setMinutes(completed.getMinutes() + 47);
    visitHistory.push({ scheduled, completed, withException: i === 3 });
  }

  let resolvedIssueVisitId = null;
  for (const entry of visitHistory) {
    const { rows: vRows } = await pool.query(
      `INSERT INTO anchor_portal_visits
         (location_id, cleaner_id, scheduled_at, started_at, completed_at, status, visit_notes)
       VALUES ($1, $2, $3, $4, $5, 'completed', $6)
       RETURNING id`,
      [
        locationId,
        cleanerId,
        entry.scheduled.toISOString(),
        new Date(entry.scheduled.getTime() + 5 * 60000).toISOString(),
        entry.completed.toISOString(),
        entry.withException
          ? 'Break table area needed extra attention — crumbs cleared and logged.'
          : 'Standard Tuesday service completed without exceptions.',
      ]
    );
    const visitId = vRows[0].id;
    await copyScopeToVisitItems(visitId, locationId);

    if (entry.withException) {
      resolvedIssueVisitId = visitId;
      await pool.query(
        `UPDATE anchor_portal_visit_items
         SET outcome = 'issue_detected',
             exception_reason = 'Crumbs accumulated under conference/break table — detail pass completed.',
             completed_at = $2
         WHERE visit_id = $1 AND item_label ILIKE '%crumbs%'`,
        [visitId, entry.completed.toISOString()]
      );
      await pool.query(
        `UPDATE anchor_portal_visit_items
         SET outcome = 'completed', completed_at = $2
         WHERE visit_id = $1 AND outcome = 'pending'`,
        [visitId, entry.completed.toISOString()]
      );
    } else {
      await pool.query(
        `UPDATE anchor_portal_visit_items
         SET outcome = 'completed', completed_at = $2
         WHERE visit_id = $1`,
        [visitId, entry.completed.toISOString()]
      );
    }
  }

  const issueCreated = new Date(now);
  issueCreated.setDate(issueCreated.getDate() - 12);
  const acknowledged = new Date(issueCreated);
  acknowledged.setHours(acknowledged.getHours() + 4);
  const resolved = new Date(acknowledged);
  resolved.setDate(resolved.getDate() + 1);

  await pool.query(
    `INSERT INTO anchor_portal_issues
       (location_id, visit_id, source, title, description, room_category, severity, status,
        acknowledged_at, resolution, resolved_at, created_at)
     VALUES ($1, $2, 'client', $3, $4, $5, 'medium', 'resolved', $6, $7, $8, $9)`,
    [
      locationId,
      resolvedIssueVisitId,
      'Kitchen sink not fully cleaned',
      'Kitchen sink still had water spots and residue after Tuesday service.',
      'BREAK ROOM / KITCHEN',
      acknowledged.toISOString(),
      'Re-cleaned sink and fixtures on follow-up pass; added detail check to kitchen scope for this location.',
      resolved.toISOString(),
      issueCreated.toISOString(),
    ]
  );

  const todayVisit = new Date();
  todayVisit.setHours(17, 30, 0, 0);
  await pool.query(
    `INSERT INTO anchor_portal_visits
       (location_id, cleaner_id, scheduled_at, status)
     VALUES ($1, $2, $3, 'scheduled')`,
    [locationId, cleanerId, todayVisit.toISOString()]
  );

  return { seeded: true, locationId, demoPassword };
}

async function ensureAnchorPortalSchema({ seedDemo = true } = {}) {
  await refreshUsersRoleConstraint();
  await ensureAnchorPortalTables();
  if (seedDemo) {
    return seedDemoPortalData();
  }
  return { seeded: false };
}

module.exports = {
  ANCHOR_OPERATOR_CLIENT_ID,
  DEMO_LOCATION_SLUG,
  ensureAnchorPortalSchema,
  ensureAnchorPortalTables,
  refreshUsersRoleConstraint,
  copyScopeToVisitItems,
  demoScopeSections,
  insertScopeForLocation,
  seedDemoPortalData,
};
