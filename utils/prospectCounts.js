const ACTIVE_STATUS_SQL = "'contacted','warm'";
const COLD_STATUS_SQL = "'cold'";
const DEAD_STATUS_SQL = "'dead','disqualified','bounced','do_not_email'";

function prospectStateCountColumns(alias = 'p', { includeDead = true } = {}) {
  const id = `${alias}.id`;
  const status = `${alias}.status`;
  const active = `COUNT(${id}) FILTER (WHERE ${status} IN (${ACTIVE_STATUS_SQL}))::int`;
  const cold = `COUNT(${id}) FILTER (WHERE ${status} IN (${COLD_STATUS_SQL}))::int`;
  const dead = `COUNT(${id}) FILTER (WHERE ${status} IN (${DEAD_STATUS_SQL}))::int`;
  const total = includeDead ? `(${active} + ${cold} + ${dead})` : `(${active} + ${cold})`;

  return [
    `${active} AS active_count`,
    `${cold} AS cold_count`,
    includeDead ? `${dead} AS dead_count` : null,
    `${total}::int AS classified_count`,
    `${active} AS prospect_count`,
  ].filter(Boolean).join(',\n          ');
}

function normalizeCountRow(row = {}, includeDead = true) {
  const out = {
    active: Number(row.active_count || row.active || 0),
    cold: Number(row.cold_count || row.cold || 0),
  };
  if (includeDead) out.dead = Number(row.dead_count || row.dead || 0);
  out.total = Number(row.classified_count || row.total || (out.active + out.cold + (out.dead || 0)));
  return out;
}

async function getProspectCounts(pool, { clientId = null, includeDead = true } = {}) {
  const params = [];
  const where = [];
  if (clientId !== null && clientId !== undefined) {
    params.push(clientId);
    where.push(`p.client_id = $${params.length}`);
  }

  const result = await pool.query(`
    SELECT
      ${prospectStateCountColumns('p', { includeDead })}
    FROM prospects p
    ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
  `, params);

  return normalizeCountRow(result.rows[0], includeDead);
}

async function getProspectCountsByClient(pool, { clientId = null, includeDead = true } = {}) {
  const params = [];
  const where = [];
  if (clientId !== null && clientId !== undefined) {
    params.push(clientId);
    where.push(`c.id = $${params.length}`);
  }

  const result = await pool.query(`
    SELECT
      c.id AS client_id,
      c.name AS client_name,
      c.slug,
      ${prospectStateCountColumns('p', { includeDead })}
    FROM clients c
    LEFT JOIN prospects p ON p.client_id = c.id
    ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
    GROUP BY c.id, c.name, c.slug
    ORDER BY c.created_at ASC, c.id ASC
  `, params);

  return result.rows.map(row => ({
    client_id: row.client_id,
    client_name: row.client_name,
    slug: row.slug,
    ...normalizeCountRow(row, includeDead),
  }));
}

module.exports = {
  prospectStateCountColumns,
  getProspectCounts,
  getProspectCountsByClient,
};
