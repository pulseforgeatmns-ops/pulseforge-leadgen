#!/usr/bin/env node
/*
 * cleaningPilotReport.js — live test-pass report for the cleaning pilot.
 *
 * Run this AFTER a live Scout pass for client_id=10 (see command below). It
 * reads the prospects Scout just wrote and prints the validation report Jacob
 * needs before scaling: total leads, score distribution, enrichment hit-rate
 * (how many got a reachable owner/manager contact), and a sample of scored
 * leads. Per-lead ICP COMPONENT breakdowns are printed live by Scout itself
 * (the `ICP[cleaning] ...` stdout lines) — capture the run output to pair them
 * with these rows.
 *
 * Requires DATABASE_URL (Railway). Reads only — writes nothing.
 *
 *   # 1. live test pass (law firms, Manchester cluster, Places-primary):
 *   node leadgen.js --client_id 10 --industry "law firm" --location "Manchester NH" --max 15 | tee /tmp/cleaning-pass.log
 *   # 2. report:
 *   node scripts/cleaningPilotReport.js --vertical law_firm
 */
const pool = require('../db');

const CLIENT_ID = 10;
const THRESHOLD = 60; // CLEANING_SETTER_THRESHOLD in leadgen.js

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

async function main() {
  const vertical = arg('vertical', 'law_firm');
  const since = arg('since', '24 hours'); // window of the test pass

  const { rows } = await pool.query(
    `SELECT p.id, p.first_name, p.last_name, p.email, p.phone, p.icp_score,
            p.vertical, p.setter_visible, p.service_area_match, p.created_at,
            c.name AS company, c.location AS company_location, c.website
       FROM prospects p
       LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.client_id = $1
        AND p.vertical = $2
        AND p.created_at > NOW() - ($3 || ' ')::interval
      ORDER BY p.icp_score DESC NULLS LAST`,
    [CLIENT_ID, vertical, since]
  );

  const n = rows.length;
  console.log(`\n=== Cleaning pilot live report — client_id=${CLIENT_ID}, vertical=${vertical} ===`);
  console.log(`Window: last ${since}.  Setter-qualifying threshold: ${THRESHOLD}\n`);
  if (!n) {
    console.log('No prospects found for this window/vertical. Did the Scout pass run and save?');
    return;
  }

  // Distribution
  const buckets = { qualify: 0, review: 0, weak: 0, cull: 0 };
  for (const r of rows) {
    const s = r.icp_score || 0;
    if (s >= THRESHOLD) buckets.qualify++;
    else if (s >= 40)   buckets.review++;
    else if (s >= 25)   buckets.weak++;
    else                buckets.cull++;
  }
  const scores = rows.map(r => r.icp_score || 0);
  const avg = Math.round(scores.reduce((a, b) => a + b, 0) / n);
  const median = scores.slice().sort((a, b) => a - b)[Math.floor(n / 2)];

  console.log(`Total leads saved: ${n}`);
  console.log('Score distribution:');
  console.log(`  ${THRESHOLD}-100 (setter-qualifying): ${buckets.qualify}`);
  console.log(`  40-${THRESHOLD - 1}  (review):          ${buckets.review}`);
  console.log(`  25-39  (weak):            ${buckets.weak}`);
  console.log(`  0-24   (cull):            ${buckets.cull}`);
  console.log(`  avg ${avg} | median ${median}\n`);

  // Enrichment hit-rate
  const hasEmail = r => !!(r.email && r.email !== '—' && String(r.email).includes('@'));
  const hasPhone = r => !!(r.phone && String(r.phone).trim());
  const hasName  = r => !!(r.first_name && String(r.first_name).trim());
  const emailN = rows.filter(hasEmail).length;
  const phoneN = rows.filter(hasPhone).length;
  const nameN  = rows.filter(hasName).length;
  const reachable = rows.filter(r => hasName(r) && (hasEmail(r) || hasPhone(r))).length;
  const anyContact = rows.filter(r => hasEmail(r) || hasPhone(r)).length;
  const pct = x => `${Math.round((x / n) * 100)}%`;

  console.log('Enrichment hit-rate (the part that makes or breaks a pilot):');
  console.log(`  reachable decision-maker (named + email/phone): ${reachable}/${n} (${pct(reachable)})`);
  console.log(`  named contact:                                  ${nameN}/${n} (${pct(nameN)})`);
  console.log(`  email:                                          ${emailN}/${n} (${pct(emailN)})`);
  console.log(`  phone:                                          ${phoneN}/${n} (${pct(phoneN)})`);
  console.log(`  any contact (email or phone):                   ${anyContact}/${n} (${pct(anyContact)})\n`);

  // Sample
  const sampleCount = Math.min(10, n);
  console.log(`Sample of ${sampleCount} scored leads (high→low):`);
  console.log('─'.repeat(78));
  for (const r of rows.slice(0, sampleCount)) {
    const name = [r.first_name, r.last_name].filter(Boolean).join(' ') || '(no contact name)';
    console.log(`${String(r.icp_score ?? 0).padStart(3)}  ${r.company || '(unknown)'}  [${r.company_location || 'no location'}]`);
    console.log(`     contact: ${name} | ${hasEmail(r) ? r.email : 'no email'} | ${hasPhone(r) ? r.phone : 'no phone'}`);
    console.log(`     setter_visible: ${r.setter_visible} | service_area_match: ${r.service_area_match || '—'}`);
  }
  console.log('\nFor per-lead ICP component breakdowns, see the `ICP[cleaning]` lines in the Scout run log.');
}

main()
  .catch(err => { console.error('[cleaningPilotReport] error:', err.message); process.exitCode = 1; })
  .finally(() => { /* shared pool — do not pool.end() */ });
