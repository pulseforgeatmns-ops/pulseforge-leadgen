#!/usr/bin/env node
'use strict';

/**
 * SPEC-083 — CIE CLI smoke helper.
 *
 * Usage:
 *   npm run client:intel:interview -- --client=1 --notes="..."
 *   npm run client:intel:interview -- --client=1 --interactive
 */

const {
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
  createMemoryStore,
} = require('../services/clientIntelligenceInterview');

function argValue(name) {
  const prefix = `--${name}=`;
  const hit = process.argv.find((a) => a.startsWith(prefix));
  if (hit) return hit.slice(prefix.length);
  const idx = process.argv.indexOf(`--${name}`);
  if (idx >= 0 && process.argv[idx + 1] && !process.argv[idx + 1].startsWith('--')) {
    return process.argv[idx + 1];
  }
  return null;
}

async function main() {
  const clientId = argValue('client') || '1';
  const notes = argValue('notes');
  const approve = process.argv.includes('--approve');
  const useMemory = process.argv.includes('--memory') || !process.env.DATABASE_URL;
  const opts = useMemory ? { store: createMemoryStore() } : {};

  if (notes) {
    const started = await startClientInterview({ clientId, notes }, opts);
    console.log(JSON.stringify(started, null, 2));
    if (approve && started.blueprint) {
      const result = await approveBlueprint(started.blueprint.id, opts);
      console.log(JSON.stringify(result, null, 2));
    }
    return;
  }

  const answers = [
    'Aji Home Services — we provide premium residential cleaning.',
    'Recurring home cleaning, deep cleans, and move-out cleans.',
    'Busy homeowners and property managers in coastal suburbs.',
    'One-off bargain hunters and commercial warehouses.',
    'Coastal SC / Myrtle Beach metro.',
    'Reliable crews and clear communication.',
    'Friendly and professional.',
    'Book 20 qualified cleaning appointments in 90 days.',
    'Appointments booked, show rate, and close rate.',
  ];

  const started = await startClientInterview({ clientId }, opts);
  let turn = started;
  console.log('Q:', turn.message);
  for (const answer of answers) {
    turn = await postInterviewMessage(turn.interviewId || started.interviewId, answer, opts);
    if (turn.message && turn.nextAction !== 'GENERATE_BLUEPRINT' && turn.nextAction !== 'COMPLETE') {
      console.log('Q:', turn.message);
    }
  }
  console.log(JSON.stringify({
    status: turn.status,
    blueprintId: turn.blueprint && turn.blueprint.id,
    confidenceSummary: turn.blueprint && turn.blueprint.confidenceSummary,
  }, null, 2));

  if (approve && turn.blueprint) {
    const result = await approveBlueprint(turn.blueprint.id, opts);
    console.log(JSON.stringify(result, null, 2));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
