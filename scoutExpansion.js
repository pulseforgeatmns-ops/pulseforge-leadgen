/**
 * Scout market expansion — saturation detection, adjacent-market queueing,
 * and yield reporting for Max's daily digest.
 */
const axios = require('axios');
const pool = require('./db');
const { normalizeClientId } = require('./utils/clientContext');
const { normalizeVertical } = require('./utils/normalize');

const SATURATION_THRESHOLD = 5;
const LOOKBACK_DAYS = 7;
const QUEUE_BATCH_LIMIT = 3;

const EXPANSION_MAPS = {
  'Manchester NH': ['Nashua NH', 'Concord NH', 'Portsmouth NH', 'Dover NH', 'Laconia NH', 'Keene NH'],
  'Nashville TN': ['Brentwood TN', 'Franklin TN', 'Murfreesboro TN', 'Hendersonville TN', 'Smyrna TN', 'Gallatin TN'],
  'Charleston WV': ['Huntington WV', 'Parkersburg WV', 'Morgantown WV', 'Beckley WV'],
  'Huntington WV': ['Charleston WV', 'Ashland KY', 'Parkersburg WV'],
};

function getKnownLocations() {
  const set = new Set();
  for (const [base, adjacent] of Object.entries(EXPANSION_MAPS)) {
    set.add(base);
    for (const loc of adjacent) set.add(loc);
  }
  return [...set].sort((a, b) => b.length - a.length);
}

const KNOWN_LOCATIONS = getKnownLocations();

function getWeekStart(date = new Date()) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const day = d.getUTCDay();
  const diff = day === 0 ? -6 : 1 - day;
  d.setUTCDate(d.getUTCDate() + diff);
  return d.toISOString().slice(0, 10);
}

function matchProspectLocation(notes, serviceAreaMatch) {
  const hay = `${notes || ''} ${serviceAreaMatch || ''}`.toLowerCase();
  for (const loc of KNOWN_LOCATIONS) {
    const cityToken = loc.split(/\s+/)[0].toLowerCase();
    if (hay.includes(loc.toLowerCase()) || (cityToken.length >= 3 && hay.includes(cityToken))) {
      return loc;
    }
  }
  return null;
}

async function ensureScoutExpansionTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS scout_yield (
      id SERIAL PRIMARY KEY,
      client_id INTEGER NOT NULL REFERENCES clients(id),
      vertical TEXT NOT NULL,
      location TEXT NOT NULL,
      prospects_found INTEGER NOT NULL DEFAULT 0,
      week_start DATE NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (client_id, vertical, location, week_start)
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS scout_expansion_queue (
      id SERIAL PRIMARY KEY,
      client_id INTEGER NOT NULL REFERENCES clients(id),
      vertical TEXT NOT NULL,
      location TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'completed', 'failed')),
      triggered_by TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_scout_expansion_queue_pending
    ON scout_expansion_queue (client_id, status, created_at)
    WHERE status = 'pending'
  `);
}

async function countProspectsByCombo(clientId, sinceDays = LOOKBACK_DAYS) {
  const { rows } = await pool.query(
    `SELECT vertical, notes, service_area_match
     FROM prospects
     WHERE client_id = $1
       AND source = 'scout'
       AND created_at > NOW() - ($2::int || ' days')::interval`,
    [clientId, sinceDays]
  );

  const counts = new Map();
  for (const row of rows) {
    const location = matchProspectLocation(row.notes, row.service_area_match);
    const vertical = normalizeVertical(row.vertical);
    if (!location || !vertical) continue;
    const key = `${vertical}\0${location}`;
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return counts;
}

async function getTrackedCombos(clientId) {
  const weekStart = getWeekStart();
  const { rows: clientRows } = await pool.query(
    `SELECT state FROM clients WHERE id = $1`,
    [clientId]
  );
  const clientState = clientRows[0]?.state || null;
  const { rows } = await pool.query(
    `SELECT DISTINCT vertical, location
     FROM scout_yield
     WHERE client_id = $1 AND week_start >= $2::date - INTERVAL '28 days'`,
    [clientId, weekStart]
  );
  const combos = new Set();
  for (const row of rows) {
    const vertical = normalizeVertical(row.vertical);
    if (vertical) combos.add(`${vertical}\0${row.location}`);
  }

  const eligibleBases = clientState
    ? Object.keys(EXPANSION_MAPS).filter(base => base.split(' ').pop() === clientState)
    : [];
  for (const base of eligibleBases) {
    const { rows: verticals } = await pool.query(
      `SELECT DISTINCT vertical FROM prospects
       WHERE client_id = $1 AND source = 'scout' AND vertical IS NOT NULL`,
      [clientId]
    );
    for (const row of verticals) {
      const vertical = normalizeVertical(row.vertical);
      if (vertical) combos.add(`${vertical}\0${base}`);
    }
  }

  const recentCounts = await countProspectsByCombo(clientId);
  for (const key of recentCounts.keys()) combos.add(key);

  return combos;
}

async function recordYieldSnapshot(clientId) {
  const weekStart = getWeekStart();
  const counts = await countProspectsByCombo(clientId);
  const combos = await getTrackedCombos(clientId);
  const recorded = [];

  for (const key of combos) {
    const [rawVertical, location] = key.split('\0');
    const vertical = normalizeVertical(rawVertical);
    if (!vertical) continue;
    const prospectsFound = counts.get(key) || 0;
    await pool.query(
      `INSERT INTO scout_yield (client_id, vertical, location, prospects_found, week_start)
       VALUES ($1, $2, $3, $4, $5::date)
       ON CONFLICT (client_id, vertical, location, week_start)
       DO UPDATE SET prospects_found = EXCLUDED.prospects_found, created_at = NOW()`,
      [clientId, vertical, location, prospectsFound, weekStart]
    );
    recorded.push({
      vertical,
      location,
      prospects_found: prospectsFound,
      week_start: weekStart,
      saturated: prospectsFound < SATURATION_THRESHOLD,
    });
  }

  return recorded;
}

async function hasMarketBeenTried(clientId, vertical, location) {
  const { rows: yieldRows } = await pool.query(
    `SELECT 1 FROM scout_yield
     WHERE client_id = $1 AND vertical = $2 AND location = $3
     LIMIT 1`,
    [clientId, vertical, location]
  );
  if (yieldRows.length) return true;

  const { rows: queueRows } = await pool.query(
    `SELECT 1 FROM scout_expansion_queue
     WHERE client_id = $1 AND vertical = $2 AND location = $3
       AND status IN ('pending', 'completed')
     LIMIT 1`,
    [clientId, vertical, location]
  );
  if (queueRows.length) return true;

  const cityToken = location.split(/\s+/)[0];
  const { rows: prospectRows } = await pool.query(
    `SELECT 1 FROM prospects
     WHERE client_id = $1 AND source = 'scout' AND vertical = $2
       AND (
         service_area_match ILIKE '%' || $3 || '%'
         OR notes ILIKE '%' || $3 || '%'
         OR service_area_match ILIKE '%' || $4 || '%'
         OR notes ILIKE '%' || $4 || '%'
       )
     LIMIT 1`,
    [clientId, vertical, location, cityToken]
  );
  return prospectRows.length > 0;
}

async function queueExpansionsForSaturated(clientId) {
  const weekStart = getWeekStart();
  const { rows: clientRows } = await pool.query(
    `SELECT state FROM clients WHERE id = $1`,
    [clientId]
  );
  const clientState = clientRows[0]?.state || null;
  const { rows: saturatedRows } = await pool.query(
    `SELECT vertical, location, prospects_found
     FROM scout_yield
     WHERE client_id = $1
       AND week_start = $2::date
       AND prospects_found < $3`,
    [clientId, weekStart, SATURATION_THRESHOLD]
  );

  const queued = [];
  for (const row of saturatedRows) {
    const vertical = normalizeVertical(row.vertical);
    if (!vertical) continue;
    const rowState = row.location.split(' ').pop();
    if (clientState && rowState !== clientState) continue;
    const adjacent = EXPANSION_MAPS[row.location];
    if (!adjacent?.length) continue;

    for (const nextLocation of adjacent) {
      const tried = await hasMarketBeenTried(clientId, vertical, nextLocation);
      if (tried) continue;

      const insert = await pool.query(
        `INSERT INTO scout_expansion_queue (client_id, vertical, location, status, triggered_by)
         SELECT $1, $2, $3, 'pending', $4
         WHERE NOT EXISTS (
           SELECT 1 FROM scout_expansion_queue
           WHERE client_id = $1 AND vertical = $2 AND location = $3
             AND status IN ('pending', 'completed')
         )
         RETURNING id, vertical, location, triggered_by`,
        [
          clientId,
          vertical,
          nextLocation,
          `saturation:${vertical}:${row.location}`,
        ]
      );
      if (insert.rows[0]) queued.push(insert.rows[0]);
    }
  }

  return queued;
}

async function fireScoutCron({ clientId, vertical, location }) {
  const appUrl = (process.env.APP_URL || 'https://pulseforge-leadgen-production.up.railway.app').replace(/\/$/, '');
  const url = `${appUrl}/cron/scout?client_id=${clientId}&industry=${vertical}&location=${encodeURIComponent(location)}&max=75&secret=${process.env.CRON_SECRET}`;
  const res = await axios.post(url, null, { timeout: 15000, validateStatus: () => true });
  if (res.status >= 400) {
    throw new Error(`Scout cron returned HTTP ${res.status}: ${JSON.stringify(res.data)}`);
  }
  return res.data;
}

async function processExpansionQueue(clientId, limit = QUEUE_BATCH_LIMIT) {
  const { rows: pending } = await pool.query(
    `SELECT id, client_id, vertical, location, triggered_by
     FROM scout_expansion_queue
     WHERE client_id = $1 AND status = 'pending'
     ORDER BY created_at ASC
     LIMIT $2`,
    [clientId, limit]
  );

  const results = [];
  for (const item of pending) {
    try {
      await fireScoutCron({
        clientId: item.client_id,
        vertical: item.vertical,
        location: item.location,
      });
      await pool.query(
        `UPDATE scout_expansion_queue SET status = 'completed' WHERE id = $1`,
        [item.id]
      );
      results.push({ ...item, status: 'completed' });
    } catch (err) {
      console.error(`[scoutExpansion] queue item ${item.id} failed:`, err.message);
      await pool.query(
        `UPDATE scout_expansion_queue SET status = 'failed' WHERE id = $1`,
        [item.id]
      );
      results.push({ ...item, status: 'failed', error: err.message });
    }
  }

  return results;
}

async function runSaturationCycle(clientId) {
  await ensureScoutExpansionTables();
  const yields = await recordYieldSnapshot(clientId);
  const queued = await queueExpansionsForSaturated(clientId);
  return { yields, queued };
}

async function runScoutExpansionCron(clientId) {
  const normalizedId = normalizeClientId(clientId);
  const saturation = await runSaturationCycle(normalizedId);
  const processed = await processExpansionQueue(normalizedId, QUEUE_BATCH_LIMIT);
  return {
    client_id: normalizedId,
    yields_recorded: saturation.yields.length,
    saturated_count: saturation.yields.filter(y => y.saturated).length,
    newly_queued: saturation.queued.length,
    queue_processed: processed,
  };
}

async function getExpansionReport(clientId) {
  await ensureScoutExpansionTables();
  const normalizedId = normalizeClientId(clientId);
  const weekStart = getWeekStart();

  const { rows: yieldRows } = await pool.query(
    `SELECT vertical, location, prospects_found, week_start, created_at
     FROM scout_yield
     WHERE client_id = $1 AND week_start = $2::date
     ORDER BY vertical, location`,
    [normalizedId, weekStart]
  );

  const yieldByCombo = yieldRows.map(r => ({
    vertical: r.vertical,
    location: r.location,
    prospects_found: r.prospects_found,
    week_start: r.week_start,
    saturated: r.prospects_found < SATURATION_THRESHOLD,
  }));

  const saturatedThisWeek = yieldByCombo.filter(r => r.saturated);

  const { rows: queuedRows } = await pool.query(
    `SELECT id, vertical, location, status, triggered_by, created_at
     FROM scout_expansion_queue
     WHERE client_id = $1 AND status = 'pending'
     ORDER BY created_at ASC`,
    [normalizedId]
  );

  const { rows: expandedRows } = await pool.query(
    `SELECT q.id, q.vertical, q.location, q.triggered_by, q.created_at AS queued_at,
            COUNT(p.id)::int AS prospects_since_queue
     FROM scout_expansion_queue q
     LEFT JOIN prospects p ON p.client_id = q.client_id
       AND p.source = 'scout'
       AND p.vertical = q.vertical
       AND p.created_at >= q.created_at
       AND (
         p.service_area_match ILIKE '%' || SPLIT_PART(q.location, ' ', 1) || '%'
         OR p.notes ILIKE '%' || SPLIT_PART(q.location, ' ', 1) || '%'
         OR p.service_area_match ILIKE '%' || q.location || '%'
         OR p.notes ILIKE '%' || q.location || '%'
       )
     WHERE q.client_id = $1
       AND q.status = 'completed'
     GROUP BY q.id, q.vertical, q.location, q.triggered_by, q.created_at
     HAVING COUNT(p.id) > 0
     ORDER BY q.created_at DESC`,
    [normalizedId]
  );

  const successfullyExpanded = expandedRows.map(r => ({
    vertical: r.vertical,
    location: r.location,
    triggered_by: r.triggered_by,
    queued_at: r.queued_at,
    prospects_found: r.prospects_since_queue,
  }));

  return {
    client_id: normalizedId,
    week_start: weekStart,
    yieldByCombo,
    saturatedThisWeek,
    queuedForExpansion: queuedRows,
    successfullyExpanded,
  };
}

module.exports = {
  EXPANSION_MAPS,
  SATURATION_THRESHOLD,
  ensureScoutExpansionTables,
  getExpansionReport,
  runSaturationCycle,
  processExpansionQueue,
  runScoutExpansionCron,
  matchProspectLocation,
};
