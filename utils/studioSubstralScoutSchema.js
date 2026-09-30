'use strict';

const { ensureWebsiteOpportunitySchema } = require('../services/websiteOpportunityPersistence');

let schemaReady;

async function ensureStudioSubstralScoutSchema(db) {
  if (!db) return;
  if (!schemaReady) {
    schemaReady = (async () => {
      await ensureWebsiteOpportunitySchema(db);
      await db.query(`
        ALTER TABLE prospects
          ADD COLUMN IF NOT EXISTS studio_fit_score INTEGER,
          ADD COLUMN IF NOT EXISTS studio_category TEXT,
          ADD COLUMN IF NOT EXISTS website_pain_summary TEXT,
          ADD COLUMN IF NOT EXISTS business_strength_signals JSONB DEFAULT '[]'::jsonb,
          ADD COLUMN IF NOT EXISTS proof_gap_summary TEXT,
          ADD COLUMN IF NOT EXISTS recommended_outreach_angle TEXT,
          ADD COLUMN IF NOT EXISTS studio_outreach_status TEXT,
          ADD COLUMN IF NOT EXISTS studio_reject_reason TEXT,
          ADD COLUMN IF NOT EXISTS studio_confidence TEXT,
          ADD COLUMN IF NOT EXISTS studio_scout_intelligence JSONB
      `);
      await db.query(`
        CREATE INDEX IF NOT EXISTS prospects_studio_substral_fit_idx
          ON prospects (client_id, studio_fit_score DESC NULLS LAST)
          WHERE studio_fit_score IS NOT NULL
      `);
    })().catch((err) => {
      schemaReady = null;
      throw err;
    });
  }
  return schemaReady;
}

module.exports = {
  ensureStudioSubstralScoutSchema,
};
