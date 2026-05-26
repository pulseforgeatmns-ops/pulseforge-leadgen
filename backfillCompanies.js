require('dotenv').config();
const pool = require('./db');

function normalizeDomain(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    return new URL(raw.includes('://') ? raw : `https://${raw}`)
      .hostname
      .replace(/^www\./i, '')
      .toLowerCase();
  } catch {
    const domain = raw
      .replace(/^https?:\/\//i, '')
      .replace(/^www\./i, '')
      .split(/[/?#\s]/)[0]
      .replace(/[.,;:]+$/g, '')
      .toLowerCase();
    return domain || null;
  }
}

function parseCompanyNotes(notes) {
  const parts = String(notes || '').split(' — ');
  if (parts.length < 2) return null;

  const name = parts[0].trim();
  const domain = normalizeDomain(parts.slice(1).join(' — '));
  if (!name || !domain) return null;

  return { name, domain };
}

async function ensureCompanyColumns() {
  await pool.query(`
    ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS domain TEXT
  `);
}

async function findOrCreateCompany({ name, domain, clientId }) {
  const existing = await pool.query(
    `SELECT id
       FROM companies
      WHERE client_id = $2
        AND LOWER(TRIM(name)) = LOWER(TRIM($1))
      LIMIT 1`,
    [name, clientId]
  );
  if (existing.rows.length) {
    await pool.query(
      `UPDATE companies
          SET domain = COALESCE(domain, $1),
              updated_at = NOW()
        WHERE id = $2
          AND client_id = $3`,
      [domain, existing.rows[0].id, clientId]
    );
    return existing.rows[0].id;
  }

  const inserted = await pool.query(
    `INSERT INTO companies (name, domain, client_id, created_at)
     VALUES ($1, $2, $3, NOW())
     ON CONFLICT DO NOTHING
     RETURNING id`,
    [name, domain, clientId]
  );
  if (inserted.rows.length) return inserted.rows[0].id;

  const fallback = await pool.query(
    `SELECT id
       FROM companies
      WHERE client_id = $2
        AND LOWER(TRIM(name)) = LOWER(TRIM($1))
      LIMIT 1`,
    [name, clientId]
  );
  return fallback.rows[0]?.id || null;
}

async function backfillCompanies() {
  await ensureCompanyColumns();

  const prospects = await pool.query(
    `SELECT id, notes, client_id
       FROM prospects
      WHERE company_id IS NULL
        AND notes LIKE '% — %'
      ORDER BY created_at ASC NULLS LAST, id`
  );

  let linked = 0;
  let skipped = 0;

  for (const prospect of prospects.rows) {
    const parsed = parseCompanyNotes(prospect.notes);
    if (!parsed) {
      skipped++;
      continue;
    }

    const companyId = await findOrCreateCompany({
      name: parsed.name,
      domain: parsed.domain,
      clientId: prospect.client_id,
    });

    if (!companyId) {
      skipped++;
      continue;
    }

    await pool.query(
      `UPDATE prospects
          SET company_id = $1,
              notes = NULL,
              updated_at = NOW()
        WHERE id = $2
          AND client_id = $3`,
      [companyId, prospect.id, prospect.client_id]
    );

    linked++;
    console.log(`Linked ${parsed.name} to prospect ${prospect.id}`);
  }

  console.log(`Backfill complete: linked ${linked}, skipped ${skipped}`);
}

if (require.main === module) {
  backfillCompanies()
    .catch(err => {
      console.error(err.stack || err.message);
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}

module.exports = { backfillCompanies, parseCompanyNotes, normalizeDomain };
