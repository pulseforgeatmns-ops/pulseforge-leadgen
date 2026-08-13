'use strict';

/**
 * Content Outcome CLI (SPEC-092 / planning draft SPEC-085).
 *
 * Uses the same service as API/UI — no duplicate business logic.
 *
 * Usage:
 *   node scripts/contentOutcome.js publish --client-id 1 --url "https://..." --objective awareness --topic "..."
 *   node scripts/contentOutcome.js performance --client-id 1 --publication-id <uuid> --impressions 8400
 *   node scripts/contentOutcome.js add-outcome --client-id 1 --publication-id <uuid> --type partner_conversation --attribution direct
 *   node scripts/contentOutcome.js add-signal --client-id 1 --publication-id <uuid> --type message_resonance --description "..."
 *   node scripts/contentOutcome.js show --client-id 1 --publication-id <uuid>
 *   node scripts/contentOutcome.js list --client-id 1 [--limit 10]
 */

require('dotenv').config();
const pool = require('../db');
const {
  createContentOutcomeService,
  createPostgresContentOutcomeStore,
} = require('../services/contentOutcome');
const { tokenizeArgs, assertAllowed } = require('../utils/maxCli');

function parseArgs(argv = process.argv.slice(2)) {
  const command = argv[0] || 'help';
  const rest = argv.slice(1);
  const parsed = tokenizeArgs(rest);
  return { command, parsed };
}

function value(parsed, key) {
  return parsed.values.get(key);
}

async function main() {
  const { command, parsed } = parseArgs();
  if (command === 'help' || command === '--help' || command === '-h') {
    printHelp();
    return;
  }

  const store = createPostgresContentOutcomeStore(pool);
  const service = createContentOutcomeService({ store });

  switch (command) {
    case 'publish': {
      assertAllowed(parsed, {
        values: [
          '--client-id',
          '--url',
          '--external-post-id',
          '--objective',
          '--topic',
          '--thesis',
          '--format',
          '--audience',
          '--channel',
          '--title',
          '--body',
          '--pending-comment-id',
          '--published-at',
          '--campaign-id',
          '--artifact-id',
        ],
      });
      const clientId = requireInt(value(parsed, '--client-id'), '--client-id');
      const result = await service.createPublication({
        client_id: clientId,
        tenant_id: String(clientId),
        content_artifact_id: value(parsed, '--artifact-id'),
        pending_comment_id: value(parsed, '--pending-comment-id'),
        title: value(parsed, '--title'),
        body: value(parsed, '--body'),
        channel: value(parsed, '--channel') || 'linkedin',
        external_url: value(parsed, '--url'),
        external_post_id: value(parsed, '--external-post-id'),
        objective: value(parsed, '--objective'),
        topic: value(parsed, '--topic'),
        thesis: value(parsed, '--thesis'),
        format: value(parsed, '--format'),
        intended_audience: value(parsed, '--audience'),
        published_at: value(parsed, '--published-at'),
        campaign_id: value(parsed, '--campaign-id'),
      });
      printJson(result);
      break;
    }
    case 'performance': {
      assertAllowed(parsed, {
        values: [
          '--client-id',
          '--publication-id',
          '--observed-at',
          '--impressions',
          '--members-reached',
          '--reactions',
          '--comments',
          '--reposts',
          '--saves',
          '--profile-views',
          '--followers-gained',
          '--connection-requests',
        ],
      });
      const clientId = requireInt(value(parsed, '--client-id'), '--client-id');
      const publicationId = requireText(
        value(parsed, '--publication-id'),
        '--publication-id'
      );
      const snapshot = await service.addPerformanceSnapshot(
        String(clientId),
        publicationId,
        {
          observed_at: value(parsed, '--observed-at'),
          impressions: value(parsed, '--impressions'),
          members_reached: value(parsed, '--members-reached'),
          reactions: value(parsed, '--reactions'),
          comments: value(parsed, '--comments'),
          reposts: value(parsed, '--reposts'),
          saves: value(parsed, '--saves'),
          profile_views_attributed: value(parsed, '--profile-views'),
          followers_gained: value(parsed, '--followers-gained'),
          connection_requests: value(parsed, '--connection-requests'),
        }
      );
      printJson(snapshot);
      break;
    }
    case 'add-outcome': {
      assertAllowed(parsed, {
        values: [
          '--client-id',
          '--publication-id',
          '--type',
          '--attribution',
          '--description',
          '--occurred-at',
          '--company-id',
          '--person-id',
          '--interaction-id',
          '--evidence-id',
          '--confidence',
        ],
      });
      const clientId = requireInt(value(parsed, '--client-id'), '--client-id');
      const publicationId = requireText(
        value(parsed, '--publication-id'),
        '--publication-id'
      );
      const outcome = await service.addBusinessOutcome(String(clientId), publicationId, {
        outcome_type: requireText(value(parsed, '--type'), '--type'),
        attribution: value(parsed, '--attribution') || 'unknown',
        description: value(parsed, '--description'),
        occurred_at: value(parsed, '--occurred-at'),
        company_id: value(parsed, '--company-id'),
        person_id: value(parsed, '--person-id'),
        interaction_id: value(parsed, '--interaction-id'),
        evidence_id: value(parsed, '--evidence-id'),
        confidence: value(parsed, '--confidence'),
      });
      printJson(outcome);
      break;
    }
    case 'add-signal': {
      assertAllowed(parsed, {
        values: [
          '--client-id',
          '--publication-id',
          '--type',
          '--description',
          '--observed-at',
          '--audience-type',
          '--sentiment',
          '--strength',
          '--evidence-id',
        ],
      });
      const clientId = requireInt(value(parsed, '--client-id'), '--client-id');
      const publicationId = requireText(
        value(parsed, '--publication-id'),
        '--publication-id'
      );
      const signal = await service.addQualitativeSignal(String(clientId), publicationId, {
        signal_type: requireText(value(parsed, '--type'), '--type'),
        description: requireText(value(parsed, '--description'), '--description'),
        observed_at: value(parsed, '--observed-at'),
        audience_type: value(parsed, '--audience-type'),
        sentiment: value(parsed, '--sentiment'),
        strength: value(parsed, '--strength'),
        evidence_id: value(parsed, '--evidence-id'),
      });
      printJson(signal);
      break;
    }
    case 'show': {
      assertAllowed(parsed, {
        values: ['--client-id', '--publication-id'],
      });
      const clientId = requireInt(value(parsed, '--client-id'), '--client-id');
      const publicationId = requireText(
        value(parsed, '--publication-id'),
        '--publication-id'
      );
      const full = await service.getPublicationOutcome(String(clientId), publicationId);
      printJson(full);
      break;
    }
    case 'list': {
      assertAllowed(parsed, {
        values: [
          '--client-id',
          '--channel',
          '--objective',
          '--topic',
          '--format',
          '--limit',
          '--from',
          '--to',
        ],
        flags: ['--json'],
      });
      const clientId = requireInt(value(parsed, '--client-id'), '--client-id');
      const result = await service.listContentOutcomes({
        tenantId: String(clientId),
        clientId,
        channel: value(parsed, '--channel'),
        objective: value(parsed, '--objective'),
        topic: value(parsed, '--topic'),
        format: value(parsed, '--format'),
        dateFrom: value(parsed, '--from'),
        dateTo: value(parsed, '--to'),
        limit: value(parsed, '--limit') || 20,
      });
      printJson(result);
      break;
    }
    default:
      console.error(`Unknown command: ${command}`);
      printHelp();
      process.exitCode = 1;
  }
}

function requireInt(raw, name) {
  const n = Number(raw);
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return n;
}

function requireText(raw, name) {
  if (raw == null || String(raw).trim() === '') {
    throw new Error(`${name} is required`);
  }
  return String(raw).trim();
}

function printJson(value) {
  console.log(JSON.stringify(value, null, 2));
}

function printHelp() {
  console.log(`content-outcome — Content Outcome Intelligence CLI

Commands:
  publish       Create artifact + publication
  performance   Add immutable performance snapshot
  add-outcome   Add business outcome
  add-signal    Add qualitative signal
  show          Show full publication outcome history
  list          List outcomes + comparison aggregates

Examples:
  node scripts/contentOutcome.js publish --client-id 1 --url "https://linkedin.com/..." --objective thought_leadership --topic "software should learn you" --title "Breakout post"
  node scripts/contentOutcome.js performance --client-id 1 --publication-id <id> --impressions 21300 --reactions 420 --comments 85
  node scripts/contentOutcome.js add-outcome --client-id 1 --publication-id <id> --type partner_conversation --attribution direct --description "Muhammad DM"
  node scripts/contentOutcome.js add-signal --client-id 1 --publication-id <id> --type language_adoption --description "Commenters repeated software should learn you"
  node scripts/contentOutcome.js show --client-id 1 --publication-id <id>
  node scripts/contentOutcome.js list --client-id 1 --limit 10
`);
}

if (require.main === module) {
  main()
    .catch((err) => {
      console.error(err && err.stack ? err.stack : err);
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}

module.exports = { parseArgs, main };
