require('dotenv').config();
const axios = require('axios');
const pool  = require('./db');

const AGENT_NAME = 'warm_signal';
const SHEET_ID   = '1DPVhzWGCHrAInxPDqeU5_Br208bPAf6qIgUy3wIp7QE';

// ── GOOGLE AUTH ───────────────────────────────────────────────────────
async function getAccessToken() {
  const res = await axios.post('https://oauth2.googleapis.com/token', {
    client_id:     process.env.GOOGLE_CLIENT_ID,
    client_secret: process.env.GOOGLE_CLIENT_SECRET,
    refresh_token: process.env.GOOGLE_SHEETS_REFRESH_TOKEN,
    grant_type:    'refresh_token',
  });
  return res.data.access_token;
}

// ── SHEET HELPERS ─────────────────────────────────────────────────────
async function getSheetRows(token) {
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/Setter%20Leads!A:O`;
  const res = await axios.get(url, { headers: { Authorization: `Bearer ${token}` } });
  return res.data.values || [];
}

async function updateSetterNotes(token, rowIndex, value) {
  const range = `Setter Leads!O${rowIndex + 1}`;
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/${encodeURIComponent(range)}?valueInputOption=RAW`;
  try {
    await axios.put(
      url,
      { range, majorDimension: 'ROWS', values: [[value]] },
      { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } }
    );
  } catch (err) {
    console.error('[warm_signal] Sheets write error:', JSON.stringify(err.response?.data || err.message));
    throw err;
  }
}

// ── MAIN ──────────────────────────────────────────────────────────────
async function run() {
  console.log('\n🔥 Warm Signal Agent');
  console.log('─────────────────────────────────\n');

  // 1. Query warm prospects with 2+ opens in last 7 days
  const { rows: prospects } = await pool.query(`
    SELECT p.id, p.first_name, p.last_name, p.email, p.notes, p.status,
      COUNT(t.id) as open_count,
      MAX(t.created_at) as last_open
    FROM prospects p
    JOIN touchpoints t ON t.prospect_id = p.id
    WHERE t.action_type = 'email_opened'
      AND t.created_at > NOW() - INTERVAL '7 days'
      AND p.do_not_contact = false
    GROUP BY p.id
    HAVING COUNT(t.id) >= 2
    ORDER BY last_open DESC
  `);

  if (!prospects.length) {
    console.log('No warm signals found.');
    return;
  }

  console.log(`Found ${prospects.length} prospect(s) with 2+ opens in last 7 days.\n`);

  // 2. Get sheet data
  const token = await getAccessToken();
  const rows  = await getSheetRows(token);

  let flagged = 0;

  for (const prospect of prospects) {
    const bizName = (prospect.notes || '').split('—')[0].trim() ||
                    `${prospect.first_name} ${prospect.last_name}`.trim();

    if (!bizName) continue;

    // Find matching row by column C (BUSINESS NAME), case-insensitive
    const rowIndex = rows.findIndex((r, i) => {
      if (i === 0) return false; // skip header
      const cellBiz = (r[2] || '').trim().toLowerCase();
      const dbBiz = bizName.toLowerCase();
      return cellBiz === dbBiz ||
        cellBiz.includes(dbBiz) ||
        dbBiz.includes(cellBiz) ||
        cellBiz.split(' ')[0] === dbBiz.split(' ')[0];
    });

    if (rowIndex === -1) {
      console.log(`  [no sheet match] ${bizName}`);
      continue;
    }

    const currentNotes = rows[rowIndex][14] || '';
    if (currentNotes.includes('🔥 2ND OPEN')) {
      console.log(`  [already flagged] ${bizName}`);
      continue;
    }

    // Format date as "Mon DD"
    const dateStr = new Date(prospect.last_open).toLocaleDateString('en-US', {
      month: 'short', day: 'numeric'
    });
    const flagValue = `🔥 2ND OPEN — ${dateStr}`;

    await updateSetterNotes(token, rowIndex, flagValue);
    console.log(`  [flagged] ${bizName} → "${flagValue}" (row ${rowIndex + 1})`);

    await pool.query(
      `INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at)
       VALUES ($1, $2, $3, $4, 'success', NOW())`,
      [
        AGENT_NAME,
        'flag_sheet',
        prospect.id,
        JSON.stringify({ biz_name: bizName, flag: flagValue, sheet_row: rowIndex + 1 }),
      ]
    );

    flagged++;
  }

  console.log(`\n─────────────────────────────────`);
  console.log(`Flagged ${flagged} prospect(s) in Setter Lead List.`);
  console.log(`─────────────────────────────────\n`);
}

run().catch(err => {
  console.error('[warm_signal] Fatal error:', err.message);
  process.exit(1);
});
