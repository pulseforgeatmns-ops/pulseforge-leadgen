const pool = require('../db');

const DEFAULT_CLIENT_ID = 1;

const CLIENT_COLUMNS = [
  ['name', 'text'],
  ['slug', 'text'],
  ['business_name', 'text'],
  ['vertical', 'text'],
  ['pin', 'text'],
  ['email', 'text'],
  ['phone', 'text'],
  ['address', 'text'],
  ['city', 'text'],
  ['state', 'text'],
  ['zip', 'text'],
  ['website', 'text'],
  ['gbp_url', 'text'],
  ['facebook_url', 'text'],
  ['license_number', 'text'],
  ['avg_job_value', 'text'],
  ['service_area', 'text[]'],
  ['verticals', 'text[]'],
  ['target_clients', 'text'],
  ['brand_voice', 'text'],
  ['differentiators', 'text'],
  ['lead_with', 'text'],
  ['never_say', 'text'],
  ['sender_name', 'text'],
  ['email_sequence', 'text'],
  ['vera_signoff', 'text'],
  ['vera_negative', 'text'],
  ['paige_themes', 'text'],
  ['max_email', 'text'],
  ['max_time', 'text'],
  ['active', 'boolean default true'],
  ['created_at', 'timestamptz default now()'],
];

const CLIENT_SCOPED_TABLES = [
  'prospects',
  'companies',
  'touchpoints',
  'agent_log',
  'agent_actions',
  'pending_comments',
  'post_analytics',
  'activity_log',
];

function normalizeClientId(value) {
  const parsed = parseInt(value || DEFAULT_CLIENT_ID, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_CLIENT_ID;
}

function getRuntimeClientId(params = {}) {
  return normalizeClientId(
    params.client_id ||
    params.clientId ||
    process.env.ACTIVE_CLIENT_ID ||
    process.env.CLIENT_ID ||
    DEFAULT_CLIENT_ID
  );
}

function getRequestClientId(req) {
  return normalizeClientId(
    req?.query?.client_id ||
    req?.body?.client_id ||
    req?.session?.active_client_id ||
    DEFAULT_CLIENT_ID
  );
}

async function ensureClientArchitecture() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS clients (
      id serial primary key
    )
  `);

  for (const [column, type] of CLIENT_COLUMNS) {
    await pool.query(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS ${column} ${type}`);
  }

  await pool.query(`
    UPDATE clients
    SET name = COALESCE(NULLIF(name, ''), NULLIF(business_name, ''), 'Client ' || id)
    WHERE name IS NULL OR name = ''
  `);
  await pool.query(`
    UPDATE clients
    SET slug = LOWER(REGEXP_REPLACE(COALESCE(NULLIF(slug, ''), NULLIF(name, ''), 'client-' || id), '[^a-z0-9]+', '-', 'g'))
    WHERE slug IS NULL OR slug = ''
  `);
  await pool.query(`UPDATE clients SET active = true WHERE active IS NULL`);
  await pool.query(`ALTER TABLE clients ALTER COLUMN name SET NOT NULL`);
  await pool.query(`ALTER TABLE clients ALTER COLUMN slug SET NOT NULL`);
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'clients_slug_key'
          AND conrelid = 'clients'::regclass
      ) THEN
        ALTER TABLE clients ADD CONSTRAINT clients_slug_key UNIQUE (slug);
      END IF;
    END $$;
  `);

  await pool.query(`
    INSERT INTO clients (id, name, slug, business_name, vertical, email, city, state, active)
    VALUES (1, 'Pulseforge', 'pulseforge', 'Pulseforge', 'marketing automation', 'jacob@gopulseforge.com', 'Manchester', 'NH', true)
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      slug = EXCLUDED.slug,
      business_name = EXCLUDED.business_name,
      vertical = EXCLUDED.vertical,
      email = EXCLUDED.email,
      city = COALESCE(clients.city, EXCLUDED.city),
      state = COALESCE(clients.state, EXCLUDED.state),
      active = true
  `);

  await pool.query(`
    INSERT INTO clients (
      id, name, slug, business_name, vertical, email, phone, address, city, state, zip,
      website, gbp_url, license_number, avg_job_value, service_area,
      verticals, target_clients, brand_voice, differentiators,
      lead_with, never_say, sender_name, email_sequence,
      vera_signoff, vera_negative, paige_themes,
      max_email, max_time, active
    ) VALUES (
      2,
      'Mountain State Home Innovations',
      'mshi',
      'Mountain State Home Innovations',
      'home_renovation',
      'mshomeinnovations@gmail.com',
      '304-483-3655',
      '4115 Paint Creek Rd',
      'Charleston',
      'WV',
      '25083',
      'PENDING_BUILD',
      'https://share.google/KeVYcU4QxVwfur0cN',
      'WV065578',
      '$10,000-$25,000',
      ARRAY['Charleston','Dunbar','St Albans','Scott Depot','Teays Valley',
            'Hurricane','Huntington','Barboursville',
            'Kanawha County','Putnam County','Cabell County'],
      ARRAY['home_renovation','exterior_remodeling','decks','siding',
            'windows','interior_renovation','emergency_repair'],
      'Landlord associations, HOAs, banks for foreclosure property fix-ups',
      'Energetic, professional but personable to suit the scenario',
      'Locally owned and operated, all work done by owners, personal customer interaction throughout entire process, communication is the most important part of a project, competitively priced, free estimates, licensed and insured WV065578, 15+ years combined experience, previously subcontracted for Tri-State Exterior Solutions, St Albans Windows and Secure Construction, handles emergency calls',
      'Customer satisfaction focus with communication being key',
      'No direct attacks towards us or any customer',
      'Brad & Dustin',
      'home_renovation',
      'Brad & Dustin, MSHI',
      'Flag negative reviews — Brad and Dustin will reply personally',
      'Seasonal exterior tips for home safety and upkeep, landscaping, types of work performed through each season, project spotlights, before/after project features, technical tips (e.g. no concrete pours under certain temperatures in winter)',
      'mshomeinnovations@gmail.com',
      '8:00 AM EST',
      true
    )
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      slug = EXCLUDED.slug,
      business_name = EXCLUDED.business_name,
      vertical = EXCLUDED.vertical,
      email = EXCLUDED.email,
      phone = EXCLUDED.phone,
      address = EXCLUDED.address,
      city = EXCLUDED.city,
      state = EXCLUDED.state,
      zip = EXCLUDED.zip,
      website = EXCLUDED.website,
      gbp_url = EXCLUDED.gbp_url,
      license_number = EXCLUDED.license_number,
      avg_job_value = EXCLUDED.avg_job_value,
      service_area = EXCLUDED.service_area,
      verticals = EXCLUDED.verticals,
      target_clients = EXCLUDED.target_clients,
      brand_voice = EXCLUDED.brand_voice,
      differentiators = EXCLUDED.differentiators,
      lead_with = EXCLUDED.lead_with,
      never_say = EXCLUDED.never_say,
      sender_name = EXCLUDED.sender_name,
      email_sequence = EXCLUDED.email_sequence,
      vera_signoff = EXCLUDED.vera_signoff,
      vera_negative = EXCLUDED.vera_negative,
      paige_themes = EXCLUDED.paige_themes,
      max_email = EXCLUDED.max_email,
      max_time = EXCLUDED.max_time,
      active = true
  `);

  await pool.query(`SELECT setval(pg_get_serial_sequence('clients', 'id'), GREATEST((SELECT MAX(id) FROM clients), 1))`);

  await pool.query(`
    INSERT INTO clients (name, slug, email, city, state, active)
    VALUES ('McLeod Legal Services', 'mcleod', 'ashley@mcleodlegal.com',
            'Manchester', 'NH', false)
    ON CONFLICT (slug) DO NOTHING
  `);

  for (const table of CLIENT_SCOPED_TABLES) {
    await pool.query(`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS client_id integer REFERENCES clients(id) DEFAULT 1`);
    await pool.query(`UPDATE ${table} SET client_id = 1 WHERE client_id IS NULL`);
  }

  await ensureProspectStatusConstraint();
}

// Canonical prospect.status domain. 'hot' is no longer a status (use is_hot);
// 'booked' lives in setter_status/closer pipeline, not prospects.status.
const PROSPECT_STATUSES = ['cold', 'contacted', 'warm', 'dead', 'disqualified', 'closed'];

async function ensureProspectStatusConstraint() {
  // Normalize any legacy/out-of-domain values before tightening the constraint
  // so the ADD CONSTRAINT cannot fail on existing rows.
  await pool.query(`UPDATE prospects SET status = 'warm' WHERE status = 'hot'`);
  await pool.query(`UPDATE prospects SET status = 'closed' WHERE status = 'booked'`);
  await pool.query(
    `UPDATE prospects SET status = 'cold' WHERE status IS NOT NULL AND NOT (status = ANY ($1))`,
    [PROSPECT_STATUSES]
  );

  await pool.query(`ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_status_check`);
  await pool.query(
    `ALTER TABLE prospects ADD CONSTRAINT prospects_status_check
       CHECK (status = ANY (ARRAY['cold','contacted','warm','dead','disqualified','closed']))`
  );
}

async function getClientConfig(clientId = DEFAULT_CLIENT_ID) {
  const id = normalizeClientId(clientId);
  const result = await pool.query(
    'SELECT * FROM clients WHERE id = $1 AND active = true',
    [id]
  );
  return result.rows[0];
}

async function getActiveClients() {
  const result = await pool.query(
    'SELECT id, name, slug, email, city, state FROM clients WHERE active = true ORDER BY id'
  );
  return result.rows;
}

module.exports = {
  DEFAULT_CLIENT_ID,
  CLIENT_SCOPED_TABLES,
  PROSPECT_STATUSES,
  ensureClientArchitecture,
  ensureProspectStatusConstraint,
  getActiveClients,
  getClientConfig,
  getRequestClientId,
  getRuntimeClientId,
  normalizeClientId,
};
