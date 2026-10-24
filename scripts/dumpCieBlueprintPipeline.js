#!/usr/bin/env node
'use strict';

/**
 * Dump normalizedFacts → prepare → sections for a CIE interview session.
 * Usage: node scripts/dumpCieBlueprintPipeline.js [--session <uuid>] [--client-id 17]
 * Set CIE_BLUEPRINT_PIPELINE_DIAG=1 to mirror generateBlueprint server logs.
 */

const pool = require('../db');
const {
  prepareNormalizedFactsForBrief,
  rehydrateNormalizedFactsFromAnswers,
  sectionsFromNormalizedFacts,
  pickBlueprintPipelineFactSlice,
  pickBlueprintPipelineSectionSummaries,
  logBlueprintPipelineDiagnostics,
} = require('../services/clientIntelligenceInterview');

function parseArgs(argv) {
  const out = { sessionId: null, clientId: 17 };
  for (let i = 2; i < argv.length; i += 1) {
    if (argv[i] === '--session') out.sessionId = argv[++i];
    else if (argv[i] === '--client-id') out.clientId = Number(argv[++i]);
  }
  return out;
}

async function resolveSessionId(clientId, sessionId) {
  if (sessionId) return sessionId;
  const result = await pool.query(
    `SELECT id FROM cie_interview_sessions WHERE client_id = $1 ORDER BY updated_at DESC LIMIT 1`,
    [clientId]
  );
  return result.rows[0]?.id || null;
}

async function main() {
  const { sessionId: requested, clientId } = parseArgs(process.argv);
  const sessionId = await resolveSessionId(clientId, requested);
  if (!sessionId) {
    console.error('No interview session found.');
    process.exit(1);
  }

  const row = (
    await pool.query(`SELECT id, client_id, interview_state FROM cie_interview_sessions WHERE id = $1`, [
      sessionId,
    ])
  ).rows[0];
  const state = row.interview_state || {};
  const raw = state.normalizedFacts || {};

  process.env.CIE_BLUEPRINT_PIPELINE_DIAG = '1';
  logBlueprintPipelineDiagnostics(sessionId, row.client_id, 'raw_normalizedFacts_before_prepare', {
    fields: pickBlueprintPipelineFactSlice(raw),
    answerKeys: Object.keys(state.answers || {}),
  });

  const rehydrated = rehydrateNormalizedFactsFromAnswers(state);
  logBlueprintPipelineDiagnostics(sessionId, row.client_id, 'rehydrated_normalizedFacts_before_prepare', {
    fields: pickBlueprintPipelineFactSlice(rehydrated),
  });

  const prepared = prepareNormalizedFactsForBrief(rehydrated);
  logBlueprintPipelineDiagnostics(sessionId, row.client_id, 'prepared_normalizedFacts_after_prepare', {
    fields: pickBlueprintPipelineFactSlice(prepared),
  });

  const sections = sectionsFromNormalizedFacts(prepared, {});
  logBlueprintPipelineDiagnostics(sessionId, row.client_id, 'sections_from_normalized_facts', {
    sections: pickBlueprintPipelineSectionSummaries(sections),
  });

  await pool.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
