require('dotenv').config();
const axios = require('axios');
const pool = require('../db');
const { insertBrevoEvent } = require('../utils/brevoEvents');

const LIMIT = 500;
const DAYS = 90;

function isoDate(date) {
  return date.toISOString().slice(0, 10);
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

  let offset = 0;
  let totalFetched = 0;
  let inserted = 0;
  let skippedDuplicate = 0;

  while (true) {
    const events = await fetchEvents(offset);
    if (!events.length) break;

    for (const event of events) {
      const result = await insertBrevoEvent(event);
      totalFetched++;
      if (result.inserted) inserted++;
      if (result.duplicate) skippedDuplicate++;

      if (totalFetched % 1000 === 0) {
        console.log(`Processed ${totalFetched} Brevo events. Inserted ${inserted}. Duplicates ${skippedDuplicate}.`);
      }
    }

    if (events.length < LIMIT) break;
    offset += events.length;
  }

  console.log(`Done. Total fetched: ${totalFetched}. Inserted: ${inserted}. Skipped as duplicate: ${skippedDuplicate}.`);
}

main()
  .catch(err => {
    console.error('Brevo backfill failed:', err.response?.data || err.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
