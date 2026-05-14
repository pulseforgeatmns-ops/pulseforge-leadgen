'use strict';

const axios = require('axios');

const DEFAULT_SETTER_SHEET_ID = '1DPVhzWGCHrAInxPDqeU5_Br208bPAf6qIgUy3wIp7QE';
const DEFAULT_SETTER_SHEET_NAME = 'Setter Leads';
const SHEET_COLUMNS = 15; // A:O. Warm Signal writes notes/flags to O.

function setterConfig() {
  return {
    sheetId: process.env.SETTER_SHEET_ID || DEFAULT_SETTER_SHEET_ID,
    sheetName: process.env.SETTER_SHEET_NAME || DEFAULT_SETTER_SHEET_NAME,
  };
}

function hasSetterSheetAuth() {
  return Boolean(
    process.env.GOOGLE_CLIENT_ID &&
    process.env.GOOGLE_CLIENT_SECRET &&
    process.env.GOOGLE_SHEETS_REFRESH_TOKEN
  );
}

async function getAccessToken() {
  const res = await axios.post('https://oauth2.googleapis.com/token', {
    client_id: process.env.GOOGLE_CLIENT_ID,
    client_secret: process.env.GOOGLE_CLIENT_SECRET,
    refresh_token: process.env.GOOGLE_SHEETS_REFRESH_TOKEN,
    grant_type: 'refresh_token',
  });
  return res.data.access_token;
}

function valuesUrl(sheetId, range) {
  return `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(range)}`;
}

async function getSetterRows(token) {
  const { sheetId, sheetName } = setterConfig();
  const res = await axios.get(valuesUrl(sheetId, `${sheetName}!A:O`), {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data.values || [];
}

function businessNameForLead(lead) {
  return String(lead.company || lead.business_name || '')
    .replace(/^CONTACT:\s*/i, '')
    .trim();
}

function makeSetterRow(lead, vertical) {
  const businessName = businessNameForLead(lead);
  const contact = lead.contact && lead.contact !== '—' ? lead.contact : '';
  const email = lead.email && lead.email !== '—' ? lead.email : '';
  const phone = lead.phone || '';
  const title = lead.title && lead.title !== '—' ? lead.title : '';
  const website = lead.url || '';
  const score = Number(lead.score || 0);
  const date = new Date().toISOString().slice(0, 10);

  return [
    date,                 // A: Date added
    vertical || '',        // B: Vertical
    businessName,          // C: Business name (used by warmSignalAgent)
    contact,               // D: Contact
    title,                 // E: Title
    email,                 // F: Email
    phone,                 // G: Phone
    website,               // H: Website
    score,                 // I: Score
    'new',                 // J: Setter status
    'scout',               // K: Source
    '',                    // L: City / location
    '',                    // M: Last contact
    '',                    // N: Next follow-up
    `Scout qualified ${score}/100 on ${date}`, // O: Notes / warm flags
  ];
}

function sheetBusinessSet(rows) {
  return new Set(
    rows
      .slice(1)
      .map(row => String(row[2] || '').trim().toLowerCase())
      .filter(Boolean)
  );
}

async function appendQualifiedScoutLead(lead, vertical) {
  if (!hasSetterSheetAuth()) {
    console.warn('[setter_sheet] Missing Google Sheets auth; skipping setter handoff');
    return { skipped: true, reason: 'missing_auth' };
  }

  const businessName = businessNameForLead(lead);
  if (!businessName) return { skipped: true, reason: 'missing_business_name' };
  if (Number(lead.score || 0) < 40) return { skipped: true, reason: 'below_threshold' };

  const token = await getAccessToken();
  const rows = await getSetterRows(token);
  const existingBusinesses = sheetBusinessSet(rows);
  if (existingBusinesses.has(businessName.toLowerCase())) {
    return { skipped: true, reason: 'duplicate_sheet_row' };
  }

  const { sheetId, sheetName } = setterConfig();
  const row = makeSetterRow(lead, vertical);
  while (row.length < SHEET_COLUMNS) row.push('');

  await axios.post(
    `${valuesUrl(sheetId, `${sheetName}!A:O`)}:append?valueInputOption=RAW&insertDataOption=INSERT_ROWS`,
    { values: [row] },
    {
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
    }
  );

  return { appended: true, businessName };
}

async function appendQualifiedScoutLeads(leads, vertical) {
  const result = { appended: 0, skipped: 0, failed: 0 };
  for (const lead of leads) {
    try {
      const handoff = await appendQualifiedScoutLead(lead, vertical);
      if (handoff.appended) result.appended++;
      else result.skipped++;
    } catch (err) {
      result.failed++;
      console.error(`[setter_sheet] Failed handoff for ${businessNameForLead(lead) || 'unknown'}:`, err.message);
    }
  }
  return result;
}

module.exports = {
  appendQualifiedScoutLead,
  appendQualifiedScoutLeads,
  hasSetterSheetAuth,
  makeSetterRow,
};
