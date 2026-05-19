require('dotenv').config();
const { google } = require('googleapis');
const { Pool } = require('pg');

const SHEET_ID = '1DPVhzWGCHrAInxPDqeU5_Br208bPAf6qIgUy3wIp7QE';
const TAB_NAME = 'Setter Leads';

const JUNK_KEYWORDS = [
  'obituary', 'project stories', 'annual statewide', 'community health needs',
  'remodeling in salem', 'north shore, ma', 'nashua —', 'serving north shore'
];

const JUNK_DOMAINS = [
  'manta.com', 'blackdogbuilders', 'bradfordcm.com', 'monadnockcom',
  'qualitylandscapi', 'yelp.com', 'yellowpages.com', 'bbb.org', 'indeed.com',
  'linkedin.com', 'facebook.com', 'opentable.com', 'theknot.com'
];

function getTier(score) {
  if (score >= 80) return '🔥 TIER 1 - WARM';
  if (score >= 65) return '⭐ TIER 2 - HIGH ICP';
  return '📋 TIER 3 - MEDIUM ICP';
}

function isJunk(company, website) {
  const c = (company || '').toLowerCase();
  const w = (website || '').toLowerCase();
  if (JUNK_KEYWORDS.some(k => c.includes(k))) return true;
  if (JUNK_DOMAINS.some(d => w.includes(d))) return true;
  return false;
}

async function getAuthClient() {
  const auth = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET
  );
  auth.setCredentials({ refresh_token: process.env.GOOGLE_SHEETS_REFRESH_TOKEN });
  return auth;
}

async function fixMalformattedRows() {
  const authClient = await getAuthClient();
  const sheets = google.sheets({ version: 'v4', auth: authClient });

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${TAB_NAME}!A1:K200`,
  });

  const rows = res.data.values || [];
  console.log(`Total rows read: ${rows.length}`);

  const updates = [];

  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const colA = (row[0] || '').trim();

    if (/^\d{4}-\d{2}-\d{2}$/.test(colA)) {
      const vertical = row[1] || '';
      const company  = row[2] || '';
      const contact  = row[3] || '';
      const email    = row[4] || '';
      const phone    = row[5] || '';
      const website  = row[6] || '';

      if (isJunk(company, website)) {
        console.log(`  🗑  Junk: ${company} — clearing row ${i + 1}`);
        updates.push({
          range: `${TAB_NAME}!A${i + 1}:K${i + 1}`,
          values: [['', '', '', '', '', '', '', '', '', '', '']],
        });
        continue;
      }

      const score = 50;
      const tier  = getTier(score);

      const fixedRow = [
        tier, '', company, contact, email, phone, website, vertical, score, 'new', 'scout'
      ];

      updates.push({
        range: `${TAB_NAME}!A${i + 1}:K${i + 1}`,
        values: [fixedRow],
      });
      console.log(`  ✅ Fixed row ${i + 1}: ${company}`);
    }
  }

  if (updates.length === 0) {
    console.log('No malformatted rows found.');
    return;
  }

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { valueInputOption: 'USER_ENTERED', data: updates },
  });

  console.log(`\n✅ Done — ${updates.length} rows updated.`);
}

(async () => {
  console.log('🔧 Fixing malformatted rows...\n');
  await fixMalformattedRows();
})();
