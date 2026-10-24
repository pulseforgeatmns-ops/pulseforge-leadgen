const pool = require('../db');

const PROSPEO_DAILY_CAP = parseInt(process.env.PROSPEO_DAILY_CAP || '950', 10);
const BREAKER_429_DURATION_MS = 60 * 60 * 1000; // 1 hour

let breakerOpenUntil = 0;

async function ensureUsageTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS api_usage (
      api TEXT NOT NULL,
      day DATE NOT NULL,
      count INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (api, day)
    )
  `);
}

async function checkProspeoQuota() {
  if (Date.now() < breakerOpenUntil) {
    return { ok: false, reason: 'breaker_429_open', cap: PROSPEO_DAILY_CAP, count: null };
  }
  await ensureUsageTable();
  const res = await pool.query(`
    SELECT count FROM api_usage WHERE api = 'prospeo' AND day = CURRENT_DATE
  `);
  const count = res.rows[0]?.count ?? 0;
  if (count >= PROSPEO_DAILY_CAP) {
    return { ok: false, reason: 'daily_cap_reached', cap: PROSPEO_DAILY_CAP, count };
  }
  return { ok: true, cap: PROSPEO_DAILY_CAP, count };
}

async function recordProspeoCall() {
  await ensureUsageTable();
  await pool.query(`
    INSERT INTO api_usage (api, day, count)
    VALUES ('prospeo', CURRENT_DATE, 1)
    ON CONFLICT (api, day) DO UPDATE SET count = api_usage.count + 1
  `);
}

function trip429() {
  breakerOpenUntil = Date.now() + BREAKER_429_DURATION_MS;
  console.log(`[Prospeo] 429 received — breaker open for ${BREAKER_429_DURATION_MS / 60000} minutes`);
}

function getBreakerState() {
  return {
    cap: PROSPEO_DAILY_CAP,
    breakerOpenUntil,
    breakerOpen: Date.now() < breakerOpenUntil,
  };
}

module.exports = {
  PROSPEO_DAILY_CAP,
  ensureUsageTable,
  checkProspeoQuota,
  recordProspeoCall,
  trip429,
  getBreakerState,
};
