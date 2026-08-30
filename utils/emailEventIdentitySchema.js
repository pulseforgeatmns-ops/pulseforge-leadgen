'use strict';

let schemaReady = false;
let schemaPromise = null;

async function ensureSenderIdentityEventSchema(pool) {
  if (!pool || schemaReady) return;
  if (!schemaPromise) {
    schemaPromise = (async () => {
      await pool.query(`
        ALTER TABLE email_events
          ADD COLUMN IF NOT EXISTS sender_identity_status TEXT,
          ADD COLUMN IF NOT EXISTS sender_identity_reason TEXT
      `);
      schemaReady = true;
    })().catch((err) => {
      schemaPromise = null;
      if (err.code === '42P01' || err.code === '42703') return;
      throw err;
    });
  }
  await schemaPromise;
}

function resetSenderIdentityEventSchemaForTests() {
  schemaReady = false;
  schemaPromise = null;
}

module.exports = {
  ensureSenderIdentityEventSchema,
  resetSenderIdentityEventSchemaForTests,
};
