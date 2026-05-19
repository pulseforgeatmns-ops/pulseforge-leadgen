require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

// ── scoreLead() copied verbatim from leadgen.js ───────────────────
function scoreLead(lead) {
  const hay = (
    (lead.company || '') + ' ' +
    (lead.url     || '') + ' ' +
    (lead.snippet || '')
  ).toLowerCase();
  const addr = (lead.address || '').toLowerCase();

  const TARGET_VERTICAL = [
    'clean','cleaning','cleaner','restaurant','cafe','diner','eatery',
    'hvac','heating','cooling','air conditioning','salon','hair','spa',
    'beauty','retail','shop','store','boutique','auto','automotive',
    'mechanic','repair'
  ];
  const ADJACENT_VERTICAL = [
    'landscap','lawn','property management','hotel','hospitality',
    'motel','gym','fitness'
  ];
  let vertical = 5;
  if (TARGET_VERTICAL.some(k => hay.includes(k)))         vertical = 25;
  else if (ADJACENT_VERTICAL.some(k => hay.includes(k)))  vertical = 15;

  const NH_SUBURBS = [
    'bedford','goffstown','hooksett','londonderry','auburn','candia',
    'derry','merrimack','nashua','concord'
  ];
  const locHay = addr || hay;
  let location = 0;
  if (locHay.includes('manchester'))                       location = 20;
  else if (NH_SUBURBS.some(c => locHay.includes(c)))      location = 15;
  else if (locHay.includes(' nh') || locHay.includes('new hampshire')) location = 8;

  const hasEmail = lead.email && lead.email !== '—' && lead.email.includes('@');
  const hasPhone = !!(lead.phone && lead.phone !== '');
  let contact = 0;
  if (hasEmail && hasPhone) contact = 20;
  else if (hasEmail)        contact = 12;
  else if (hasPhone)        contact = 8;

  const JUNK_DOMAINS = ['yelp','facebook','google','yellowpages','bbb.org','tripadvisor'];
  const hasRealUrl = lead.url && !JUNK_DOMAINS.some(d => lead.url.includes(d));
  const hasSocial  = /instagram|facebook|social|twitter|tiktok|linkedin/.test(hay);
  let web = 0;
  if (hasRealUrl && hasSocial) web = 20;
  else if (hasRealUrl)         web = 12;

  const SIZE_STRONG = ['llc','inc','corp','commercial','team','staff',' locations'];
  const hasStrong = SIZE_STRONG.some(k => hay.includes(k));
  const hasBasic  = hasPhone || !!(lead.address);
  let size = 0;
  if (hasStrong)     size = 15;
  else if (hasBasic) size = 8;

  return Math.min(vertical + location + contact + web + size, 100);
}

async function run() {
  const { rows } = await pool.query(`
    SELECT id, first_name, last_name, email, phone, notes, icp_score
    FROM prospects
    ORDER BY created_at ASC
  `);

  console.log(`Rescoring ${rows.length} prospects...\n`);

  let updated = 0;

  for (const p of rows) {
    const parts   = (p.notes || '').split(' — ');
    const company = parts[0]?.trim() || '';
    const url     = parts[1]?.trim() || '';

    const lead = { company, url, email: p.email, phone: p.phone, snippet: '', address: '' };
    const newScore = scoreLead(lead);
    const oldScore = p.icp_score ?? 0;

    if (newScore !== oldScore) {
      await pool.query('UPDATE prospects SET icp_score = $1 WHERE id = $2', [newScore, p.id]);
      console.log(`Updated ${(company || p.first_name || p.id).padEnd(45)} ${String(oldScore).padStart(3)} → ${newScore}`);
      updated++;
    }
  }

  console.log(`\n${updated} prospect${updated !== 1 ? 's' : ''} rescored (${rows.length - updated} unchanged).`);
  await pool.end();
}

run().catch(err => { console.error(err.message); process.exit(1); });
