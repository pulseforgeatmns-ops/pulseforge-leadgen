require('dotenv').config();
const axios = require('axios');
const pool = require('../db');
const { insertBrevoEvent } = require('../utils/brevoEvents');

const LIMIT = 500;
const DAYS = 90;

function isoDate(date) {
  return date.toISOString().slice(0, 10);
}

async function reclassifyLegacyProxyTouchpoints() {
  const result = await pool.query(`
    UPDATE touchpoints
    SET action_type = 'email_opened_proxy'
    WHERE action_type IN ('open', 'email_opened')
      AND COALESCE(outcome::text, '') ~*
        '"event"\\s*:\\s*"(loadedByProxy|loaded_by_proxy|proxyOpen|proxy_open|uniqueLoadedByProxy|unique_loaded_by_proxy|uniqueProxyOpen|unique_proxy_open)"'
    RETURNING id
  `);
  return result.rowCount;
}

async function fetchEvents(offset) {
  const end = new Date();
  const start = new Date(end.getTime() - DAYS * 24 * 60 * 60 * 1000);
  const res = await axios.get('https://api.brevo.com/v3/smtp/statistics/events', {
    headers: {
      'api-key': process.env.BREVO_API_KEY,
      accept: 'application/json',
    },
    params: {
      limit: LIMIT,
      offset,
      startDate: isoDate(start),
      endDate: isoDate(end),
      sort: 'asc',
    },
    timeout: 30000,
  });
  return res.data?.events || [];
}

async function main() {
  if (!process.env.BREVO_API_KEY) {
    throw new Error('BREVO_API_KEY is required');
  }

  const reclassifiedTouchpoints = await reclassifyLegacyProxyTouchpoints();
  console.log(`Reclassified ${reclassifiedTouchpoints} legacy proxy-open touchpoint(s).`);

  let offset = 0;
  let totalFetched = 0;
  let inserted = 0;
  let updated = 0;
  let skippedDuplicate = 0;
  let skipped = 0;
  const fetchedByType = {};
  const canonicalByType = {};
  const insertedByType = {};
  const updatedByType = {};
  const skippedByReason = {};

  while (true) {
    const events = await fetchEvents(offset);
    if (!events.length) break;

    for (const event of events) {
      const result = await insertBrevoEvent(event);
      totalFetched++;
      const rawType = String(event.event || event.event_type || event.type || '(missing)');
      fetchedByType[rawType] = (fetchedByType[rawType] || 0) + 1;
      if (result.inserted) inserted++;
      if (result.updated) updated++;
      if (result.duplicate) skippedDuplicate++;
      if (result.event_type) {
        canonicalByType[result.event_type] = (canonicalByType[result.event_type] || 0) + 1;
        if (result.inserted) {
          insertedByType[result.event_type] = (insertedByType[result.event_type] || 0) + 1;
        }
        if (result.updated) {
          updatedByType[result.event_type] = (updatedByType[result.event_type] || 0) + 1;
        }
      }
      if (result.skipped) {
        skipped++;
        const reason = result.reason || 'unknown';
        skippedByReason[reason] = (skippedByReason[reason] || 0) + 1;
      }

      if (totalFetched % 1000 === 0) {
        console.log(`Processed ${totalFetched} Brevo events. Inserted ${inserted}. Duplicates ${skippedDuplicate}.`);
      }
    }

    if (events.length < LIMIT) break;
    offset += events.length;
  }

  console.log(`Done. Total fetched: ${totalFetched}. Inserted: ${inserted}. Reclassified: ${updated}. Skipped as duplicate: ${skippedDuplicate}. Skipped as unmappable: ${skipped}.`);
  console.log('Fetched Brevo event types:', fetchedByType);
  console.log('Canonical event coverage:', canonicalByType);
  console.log('New rows inserted by canonical type:', insertedByType);
  console.log('Existing rows reclassified by canonical type:', updatedByType);
  if (skipped) console.log('Skipped reasons:', skippedByReason);
}

main()
  .catch(err => {
    console.error('Brevo backfill failed:', err.response?.data || err.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
