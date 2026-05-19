require('dotenv').config();
const pool = require('./db');

const DRY_RUN = process.argv.includes('--dry-run');

// ── JUNK DETECTION (mirrors validateProspect in leadgen.js) ───────────

const JUNK_EXACT = new Set([
  'sitemap','home','contact','index','about','services','products',
  'blog','news','faq','login','register','search','menu','careers',
  'jobs','employment','privacy','terms','404','error','page'
]);

function isJunk(name) {
  if (!name || typeof name !== 'string') return true;
  const n = name.trim();
  if (n.length < 4) return true;
  if (/^CONTACT:/i.test(n)) return true;
  if (/\b\d+\s+\w[\w\s]*\s+(Rd|St|Ave|Blvd|Dr|Route|Rte|Unit|Suite|Ste|Hwy|Ln|Ct|Way|Pl|Pkwy)\b/i.test(n)) return true;
  if (/\b[A-Z]{2}\s+\d{5}\b/.test(n)) return true;
  if (/\b(jobs?|employment|hiring|careers?|openings?|recruiting|staffing)\b/i.test(n)) return true;
  if (/\b20[2-9][0-9]\b/.test(n)) return true;
  if (JUNK_EXACT.has(n.toLowerCase())) return true;
  return false;
}

function extractBusiness(notes) {
  if (!notes) return null;
  return notes.split('—')[0].trim() || null;
}

function dataScore(row) {
  return (row.email ? 1 : 0) + (row.phone ? 1 : 0);
}

// ── MAIN ──────────────────────────────────────────────────────────────

async function run() {
  console.log(DRY_RUN ? '--- DRY RUN — no deletes will happen ---\n' : '--- LIVE RUN ---\n');

  const { rows } = await pool.query(`
    SELECT id, first_name, last_name, email, phone, notes, status
    FROM prospects
    ORDER BY id
  `);

  console.log(`Loaded ${rows.length} prospects.\n`);

  // ── PHASE 1: Junk ────────────────────────────────────────────────
  const junkIds = [];
  console.log('=== PHASE 1: Junk detection ===');

  for (const row of rows) {
    const business = extractBusiness(row.notes);
    // Also check first_name in case CONTACT: ended up there
    const nameToCheck = business || `${row.first_name} ${row.last_name}`.trim();
    if (isJunk(nameToCheck)) {
      console.log(`  [JUNK] id=${String(row.id).padEnd(6)} "${nameToCheck}"`);
      junkIds.push(row.id);
    }
  }
  console.log(`\n  → ${junkIds.length} junk records found.\n`);

  // ── PHASE 2: Duplicates ───────────────────────────────────────────
  const junkSet = new Set(junkIds);
  const dupIds = [];
  const groups = {};

  for (const row of rows) {
    if (junkSet.has(row.id)) continue;
    const business = extractBusiness(row.notes);
    const raw = business || `${row.first_name} ${row.last_name}`.trim();
    // Normalize: lowercase, strip punctuation, collapse spaces
    const key = raw.toLowerCase().replace(/[^\w\s]/g, '').replace(/\s+/g, ' ').trim();
    if (!key || key.length < 3) continue;
    if (!groups[key]) groups[key] = [];
    groups[key].push(row);
  }

  console.log('=== PHASE 2: Duplicate detection ===');

  for (const [key, group] of Object.entries(groups)) {
    if (group.length < 2) continue;

    // Sort: most data first, then by lowest id (oldest)
    const sorted = [...group].sort((a, b) =>
      dataScore(b) - dataScore(a) || a.id - b.id
    );
    const keeper = sorted[0];
    const toDelete = sorted.slice(1);

    console.log(`  [DUPE] "${key}"`);
    console.log(`         keep  id=${keeper.id} (email=${keeper.email || '—'} phone=${keeper.phone || '—'})`);
    toDelete.forEach(r =>
      console.log(`         purge id=${r.id} (email=${r.email || '—'} phone=${r.phone || '—'})`)
    );
    dupIds.push(...toDelete.map(r => r.id));
  }
  console.log(`\n  → ${dupIds.length} duplicate records found.\n`);

  // ── SUMMARY ──────────────────────────────────────────────────────
  const allIds = [...new Set([...junkIds, ...dupIds])];
  console.log(`=== SUMMARY ===`);
  console.log(`  Junk records   : ${junkIds.length}`);
  console.log(`  Duplicate rows : ${dupIds.length}`);
  console.log(`  Total to delete: ${allIds.length}`);

  if (DRY_RUN) {
    console.log('\nDry run — nothing deleted. Re-run without --dry-run to apply.\n');
    await pool.end();
    return;
  }

  if (allIds.length === 0) {
    console.log('\nNothing to delete.\n');
    await pool.end();
    return;
  }

  // Delete child records first to avoid FK violations
  await pool.query(`DELETE FROM touchpoints WHERE prospect_id = ANY($1::uuid[])`, [allIds]);
  await pool.query(`DELETE FROM agent_log    WHERE prospect_id = ANY($1::uuid[])`, [allIds]);
  await pool.query(`DELETE FROM prospects    WHERE id          = ANY($1::uuid[])`, [allIds]);

  console.log(`\n✓ Deleted ${allIds.length} prospects + associated touchpoints and agent_log entries.\n`);
  await pool.end();
}

run().catch(err => { console.error(err); process.exit(1); });
