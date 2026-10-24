'use strict';

function createPortalSchema(pool) {
const bcrypt = require('bcryptjs');
const { withSchemaTransaction, refreshUsersRoleConstraintOnClient, refreshUsersRoleConstraint: refreshRoles } = require('./usersRoleConstraint');

const ANCHOR_OPERATOR_CLIENT_ID = 10;
const DEMO_LOCATION_SLUG = 'demo-riverside-law';

async function refreshUsersRoleConstraint() {
  return refreshRoles(pool);
}

async function ensureAnchorPortalTables(db = pool) {
  const { rows: [schema] } = await db.query('SELECT current_schema() AS name');
  if (schema.name !== 'public') throw new Error('Portal creation schema must be public; explicit operator review required');
  // Reject shadow tables rather than applying portal DDL to another schema.
  for (const name of ['locations', 'scope_sections', 'scope_items', 'user_locations', 'visits', 'visit_items', 'issues', 'evidence']) {
    const { rows } = await db.query(`SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass($1)`, [`anchor_portal_${name}`]);
    if (rows.length && rows[0].nspname !== 'public') throw new Error('Portal tables must resolve to public; explicit operator review required');
  }

  await db.query(`
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

  await db.query(`
    CREATE TABLE IF NOT EXISTS anchor_portal_scope_sections (
      id SERIAL PRIMARY KEY,
      location_id INTEGER NOT NULL REFERENCES anchor_portal_locations(id) ON DELETE CASCADE,
      title TEXT NOT NULL,
      sort_order INTEGER NOT NULL DEFAULT 0
    )
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS anchor_portal_scope_items (
      id SERIAL PRIMARY KEY,
      section_id INTEGER NOT NULL REFERENCES anchor_portal_scope_sections(id) ON DELETE CASCADE,
      label TEXT NOT NULL,
      sort_order INTEGER NOT NULL DEFAULT 0
    )
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS anchor_portal_user_locations (
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      location_id INTEGER NOT NULL REFERENCES anchor_portal_locations(id) ON DELETE CASCADE,
      PRIMARY KEY (user_id, location_id)
    )
  `);

  await db.query(`
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

  await db.query(`
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

  await db.query(`
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

  await db.query(`
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

  // Additive upgrades preserve existing locations, service history and issue statuses.
  await db.query('ALTER TABLE anchor_portal_visits ADD COLUMN IF NOT EXISTS completed_by INTEGER REFERENCES users(id)');
  await db.query('ALTER TABLE anchor_portal_visits ADD COLUMN IF NOT EXISTS request_id TEXT');
  await db.query('ALTER TABLE anchor_portal_issues ADD COLUMN IF NOT EXISTS issue_type TEXT');
  await db.query('ALTER TABLE anchor_portal_issues ADD COLUMN IF NOT EXISTS request_id TEXT');
  await db.query("ALTER TABLE anchor_portal_evidence ADD COLUMN IF NOT EXISTS purpose TEXT NOT NULL DEFAULT 'service'");
  await db.query('CREATE UNIQUE INDEX IF NOT EXISTS idx_anchor_portal_report_request ON anchor_portal_visits (completed_by, request_id) WHERE request_id IS NOT NULL');
  await db.query('CREATE UNIQUE INDEX IF NOT EXISTS idx_anchor_portal_issue_request ON anchor_portal_issues (reported_by_user_id, request_id) WHERE request_id IS NOT NULL');

  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_anchor_portal_visits_location_scheduled
      ON anchor_portal_visits (location_id, scheduled_at DESC)
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_anchor_portal_issues_location_status
      ON anchor_portal_issues (location_id, status, created_at DESC)
  `);
}

// IF NOT EXISTS alone does not establish readiness on a pre-existing schema.
async function verifyAnchorPortalUpgrade(db) {
  const expected = [
    ['anchor_portal_visits', 'completed_by', 'integer', false],
    ['anchor_portal_visits', 'request_id', 'text', false],
    ['anchor_portal_issues', 'issue_type', 'text', false],
    ['anchor_portal_issues', 'request_id', 'text', false],
    ['anchor_portal_evidence', 'purpose', 'text', true],
  ];
  for (const [table, column, type, notNull] of expected) {
    const { rows: [actual] } = await db.query(`
      SELECT format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull AS not_null,
             pg_get_expr(d.adbin, d.adrelid) AS default_value
      FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
      WHERE a.attrelid=to_regclass($1) AND a.attname=$2 AND NOT a.attisdropped
    `, [table, column]);
    if (!actual || actual.type !== type || actual.not_null !== notNull
        || (column === 'purpose' && actual.default_value !== "'service'::text")) {
      throw new Error(`Portal schema mismatch: ${table}.${column}`);
    }
  }
  const { rows: foreignKeys } = await db.query(`
    SELECT 1 FROM pg_constraint c
    WHERE c.conrelid='anchor_portal_visits'::regclass AND c.contype='f' AND c.convalidated
      AND c.confrelid='users'::regclass
      AND c.conkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=c.conrelid AND attname='completed_by')]
      AND c.confkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=c.confrelid AND attname='id')]
      AND c.confdeltype='a' AND c.confupdtype='a'
  `);
  if (!foreignKeys.length) throw new Error('Portal schema mismatch: completed_by foreign key');
  for (const [table, name, author] of [
    ['anchor_portal_visits', 'idx_anchor_portal_report_request', 'completed_by'],
    ['anchor_portal_issues', 'idx_anchor_portal_issue_request', 'reported_by_user_id'],
  ]) {
    const { rows: [index] } = await db.query(`
      SELECT i.indisunique, i.indisvalid, i.indisready, i.indnkeyatts, i.indnatts,
        pg_get_expr(i.indpred,i.indrelid) AS predicate,
        ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY k(attnum,ord)
          JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum ORDER BY k.ord) AS columns
      FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
      WHERE i.indrelid=to_regclass($1) AND c.relname=$2
    `, [table, name]);
    if (!index || !index.indisunique || !index.indisvalid || !index.indisready
        || index.indnkeyatts !== 2 || index.indnatts !== 2
        || index.predicate !== '(request_id IS NOT NULL)'
        || JSON.stringify(index.columns) !== JSON.stringify([author, 'request_id'])) {
      throw new Error(`Portal schema mismatch: ${name}`);
    }
  }
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

async function copyScopeToVisitItems(visitId, locationId, db = pool) {
  const { rows } = await db.query(
    `SELECT s.title AS section_title, i.label AS item_label, i.sort_order
     FROM anchor_portal_scope_sections s
     JOIN anchor_portal_scope_items i ON i.section_id = s.id
     WHERE s.location_id = $1
     ORDER BY s.sort_order, i.sort_order`,
    [locationId]
  );
  for (const row of rows) {
    await db.query(
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

  const demoPassword = process.env.ANCHOR_PORTAL_DEMO_PASSWORD;
  if (!demoPassword || demoPassword.length < 16) throw new Error('Explicit demo setup requires a configured password of at least 16 characters.');

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

async function ensureAnchorPortalSchema({ seedDemo = false } = {}) {
  await withSchemaTransaction(pool, async client => {
    await refreshUsersRoleConstraintOnClient(client);
    await ensureAnchorPortalTables(client);
    await verifyAnchorPortalUpgrade(client);
  });
  if (seedDemo) {
    return seedDemoPortalData();
  }
  return { seeded: false };
}

return {
  ANCHOR_OPERATOR_CLIENT_ID,
  DEMO_LOCATION_SLUG,
  ensureAnchorPortalSchema,
  ensureAnchorPortalTables,
  verifyAnchorPortalUpgrade,
  refreshUsersRoleConstraint,
  copyScopeToVisitItems,
  demoScopeSections,
  insertScopeForLocation,
  seedDemoPortalData,
};

}
let defaultInstance;
function defaults() { return defaultInstance ||= createPortalSchema(require('../db')); }
module.exports = { createPortalSchema };
Object.defineProperty(module.exports, 'ANCHOR_OPERATOR_CLIENT_ID', { enumerable: true, get: () => defaults().ANCHOR_OPERATOR_CLIENT_ID });
Object.defineProperty(module.exports, 'DEMO_LOCATION_SLUG', { enumerable: true, get: () => defaults().DEMO_LOCATION_SLUG });
Object.defineProperty(module.exports, 'ensureAnchorPortalSchema', { enumerable: true, get: () => defaults().ensureAnchorPortalSchema });
Object.defineProperty(module.exports, 'ensureAnchorPortalTables', { enumerable: true, get: () => defaults().ensureAnchorPortalTables });
Object.defineProperty(module.exports, 'verifyAnchorPortalUpgrade', { enumerable: true, get: () => defaults().verifyAnchorPortalUpgrade });
Object.defineProperty(module.exports, 'refreshUsersRoleConstraint', { enumerable: true, get: () => defaults().refreshUsersRoleConstraint });
Object.defineProperty(module.exports, 'copyScopeToVisitItems', { enumerable: true, get: () => defaults().copyScopeToVisitItems });
Object.defineProperty(module.exports, 'demoScopeSections', { enumerable: true, get: () => defaults().demoScopeSections });
Object.defineProperty(module.exports, 'insertScopeForLocation', { enumerable: true, get: () => defaults().insertScopeForLocation });
Object.defineProperty(module.exports, 'seedDemoPortalData', { enumerable: true, get: () => defaults().seedDemoPortalData });
