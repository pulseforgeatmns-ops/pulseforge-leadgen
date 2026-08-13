'use strict';

/**
 * SPEC-092 — Content Outcome Intelligence CLI
 *
 *   npm run content:outcome -- publish --client-id=1 --artifact-id=123 ...
 *   npm run content:outcome -- performance --publication-id=... --impressions=8400
 *   npm run content:outcome -- add-outcome --publication-id=... --type=partner_conversation
 *   npm run content:outcome -- add-signal --publication-id=... --type=message_resonance --description="..."
 *   npm run content:outcome -- show --publication-id=...
 *   npm run content:outcome -- list --client-id=1
 *   npm run content:outcome -- compare --client-id=1
 */

require('dotenv').config();

const {
  tokenizeArgs,
  assertAllowed,
  optionalPositiveInteger,
  optionalTimestamp,
} = require('../utils/maxCli');
const {
  CHANNELS,
  OBJECTIVES,
  BUSINESS_OUTCOME_TYPES,
  ATTRIBUTION_LEVELS,
  SIGNAL_TYPES,
  ContentOutcomeError,
  ensureContentOutcomeSchema,
  createContentPublication,
  addPerformanceSnapshot,
  addBusinessOutcome,
  addQualitativeSignal,
  getPublicationOutcome,
  listContentOutcomes,
  compareContentOutcomes,
} = require('../services/contentOutcomeIntelligence');

function printHelp() {
  console.log(`Content Outcome Intelligence (SPEC-092)

Usage:
  node scripts/contentOutcome.js <command> [options]

Commands:
  publish       Create a content publication
  performance   Add an immutable performance snapshot
  add-outcome   Add a business outcome
  add-signal    Add a qualitative signal
  show          Show full outcome history for a publication
  list          List content outcomes for a tenant
  compare       Deterministic aggregates (no recommendations)

Common options:
  --client-id=<n>           Tenant / client id (default 1)
  --publication-id=<uuid>   Publication id
  --json                    JSON output
  --help

publish:
  --artifact-id=<id>        Paige pending_comments id (required)
  --channel=linkedin
  --published-at=<iso>
  --objective=<enum>
  --topic=...
  --thesis=...
  --format=...
  --audience=a,b,c
  --external-url=...
  --external-post-id=...
  --campaign-id=...

performance:
  --observed-at=<iso>
  --impressions=<n> --reactions=<n> --comments=<n> --reposts=<n>
  --saves=<n> --members-reached=<n> --followers-gained=<n>
  --profile-views=<n> --connection-requests=<n>

add-outcome:
  --type=<enum> --attribution=<enum> --occurred-at=<iso>
  --description=... --company-id=... --person-id=...
  --interaction-id=... --evidence-id=... --confidence=<0..1>

add-signal:
  --type=<enum> --description=... --observed-at=<iso>
  --audience-type=... --sentiment=... --strength=... --evidence-id=...

Channels: ${CHANNELS.join(', ')}
Objectives: ${OBJECTIVES.join(', ')}
Outcome types: ${BUSINESS_OUTCOME_TYPES.join(', ')}
Attribution: ${ATTRIBUTION_LEVELS.join(', ')}
Signal types: ${SIGNAL_TYPES.join(', ')}
`);
}

function parseCommand(argv) {
  const args = [...argv];
  const command = args.shift();
  if (!command || command === '--help' || command === '-h') {
    return { command: 'help', parsed: tokenizeArgs([]) };
  }
  const parsed = tokenizeArgs(args);
  return { command, parsed };
}

function value(parsed, name) {
  return parsed.values.get(name);
}

function flag(parsed, name) {
  return parsed.flags.has(name);
}

function requireValue(parsed, name, label) {
  const v = value(parsed, name);
  if (v == null || v === '') throw new Error(`${label} is required`);
  return v;
}

async function runPublish(parsed) {
  assertAllowed(parsed, {
    values: [
      '--client-id',
      '--artifact-id',
      '--channel',
      '--published-at',
      '--objective',
      '--topic',
      '--thesis',
      '--format',
      '--audience',
      '--external-url',
      '--external-post-id',
      '--campaign-id',
    ],
    flags: ['--json', '--help'],
  });
  await ensureContentOutcomeSchema();
  return createContentPublication({
    tenant_id: optionalPositiveInteger(value(parsed, '--client-id'), 'client-id') || 1,
    content_artifact_id: requireValue(parsed, '--artifact-id', 'artifact-id'),
    channel: value(parsed, '--channel') || 'linkedin',
    published_at:
      optionalTimestamp(value(parsed, '--published-at'), 'published-at') ||
      new Date().toISOString(),
    objective: value(parsed, '--objective'),
    topic: value(parsed, '--topic'),
    thesis: value(parsed, '--thesis'),
    format: value(parsed, '--format'),
    intended_audience: value(parsed, '--audience'),
    external_url: value(parsed, '--external-url'),
    external_post_id: value(parsed, '--external-post-id'),
    campaign_id: value(parsed, '--campaign-id'),
  });
}

async function runPerformance(parsed) {
  assertAllowed(parsed, {
    values: [
      '--client-id',
      '--publication-id',
      '--observed-at',
      '--impressions',
      '--reactions',
      '--comments',
      '--reposts',
      '--saves',
      '--members-reached',
      '--followers-gained',
      '--profile-views',
      '--connection-requests',
    ],
    flags: ['--json', '--help'],
  });
  const publicationId = requireValue(
    parsed,
    '--publication-id',
    'publication-id'
  );
  const tenantId =
    optionalPositiveInteger(value(parsed, '--client-id'), 'client-id') || 1;
  return addPerformanceSnapshot(publicationId, {
    tenant_id: tenantId,
    observed_at: optionalTimestamp(value(parsed, '--observed-at'), 'observed-at'),
    impressions: value(parsed, '--impressions'),
    reactions: value(parsed, '--reactions'),
    comments: value(parsed, '--comments'),
    reposts: value(parsed, '--reposts'),
    saves: value(parsed, '--saves'),
    members_reached: value(parsed, '--members-reached'),
    followers_gained: value(parsed, '--followers-gained'),
    profile_views_attributed: value(parsed, '--profile-views'),
    connection_requests: value(parsed, '--connection-requests'),
  });
}

async function runAddOutcome(parsed) {
  assertAllowed(parsed, {
    values: [
      '--client-id',
      '--publication-id',
      '--type',
      '--attribution',
      '--occurred-at',
      '--description',
      '--company-id',
      '--person-id',
      '--interaction-id',
      '--evidence-id',
      '--confidence',
    ],
    flags: ['--json', '--help'],
  });
  const publicationId = requireValue(
    parsed,
    '--publication-id',
    'publication-id'
  );
  return addBusinessOutcome(publicationId, {
    tenant_id:
      optionalPositiveInteger(value(parsed, '--client-id'), 'client-id') || 1,
    outcome_type: requireValue(parsed, '--type', 'type'),
    attribution: value(parsed, '--attribution') || 'unknown',
    occurred_at: optionalTimestamp(value(parsed, '--occurred-at'), 'occurred-at'),
    description: value(parsed, '--description'),
    company_id: value(parsed, '--company-id'),
    person_id: value(parsed, '--person-id'),
    interaction_id: value(parsed, '--interaction-id'),
    evidence_id: value(parsed, '--evidence-id'),
    confidence: value(parsed, '--confidence'),
  });
}

async function runAddSignal(parsed) {
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
    flags: ['--json', '--help'],
  });
  const publicationId = requireValue(
    parsed,
    '--publication-id',
    'publication-id'
  );
  return addQualitativeSignal(publicationId, {
    tenant_id:
      optionalPositiveInteger(value(parsed, '--client-id'), 'client-id') || 1,
    signal_type: requireValue(parsed, '--type', 'type'),
    description: requireValue(parsed, '--description', 'description'),
    observed_at: optionalTimestamp(value(parsed, '--observed-at'), 'observed-at'),
    audience_type: value(parsed, '--audience-type'),
    sentiment: value(parsed, '--sentiment'),
    strength: value(parsed, '--strength'),
    evidence_id: value(parsed, '--evidence-id'),
  });
}

async function runShow(parsed) {
  assertAllowed(parsed, {
    values: ['--client-id', '--publication-id'],
    flags: ['--json', '--help'],
  });
  return getPublicationOutcome(
    requireValue(parsed, '--publication-id', 'publication-id'),
    {
      tenantId:
        optionalPositiveInteger(value(parsed, '--client-id'), 'client-id') || 1,
    }
  );
}

async function runList(parsed) {
  assertAllowed(parsed, {
    values: [
      '--client-id',
      '--channel',
      '--objective',
      '--topic',
      '--from',
      '--to',
      '--limit',
    ],
    flags: ['--json', '--help'],
  });
  return listContentOutcomes({
    tenantId:
      optionalPositiveInteger(value(parsed, '--client-id'), 'client-id') || 1,
    channel: value(parsed, '--channel'),
    objective: value(parsed, '--objective'),
    topic: value(parsed, '--topic'),
    from: optionalTimestamp(value(parsed, '--from'), 'from'),
    to: optionalTimestamp(value(parsed, '--to'), 'to'),
    limit: value(parsed, '--limit'),
  });
}

async function runCompare(parsed) {
  assertAllowed(parsed, {
    values: [
      '--client-id',
      '--channel',
      '--objective',
      '--topic',
      '--from',
      '--to',
      '--limit',
    ],
    flags: ['--json', '--help'],
  });
  return compareContentOutcomes({
    tenantId:
      optionalPositiveInteger(value(parsed, '--client-id'), 'client-id') || 1,
    channel: value(parsed, '--channel'),
    objective: value(parsed, '--objective'),
    topic: value(parsed, '--topic'),
    from: optionalTimestamp(value(parsed, '--from'), 'from'),
    to: optionalTimestamp(value(parsed, '--to'), 'to'),
    limit: value(parsed, '--limit'),
  });
}

function formatText(command, result) {
  if (command === 'list') {
    const lines = [`${result.length} content outcome(s)`];
    for (const item of result) {
      const p = item.publication;
      lines.push(
        `- ${p.id} | ${p.channel} | ${p.objective || '-'} | ${p.topic || '-'} | snaps=${item.performanceSnapshots.length} outcomes=${item.businessOutcomes.length} signals=${item.qualitativeSignals.length}`
      );
    }
    return lines.join('\n');
  }
  if (command === 'compare') {
    return [
      `totalPublications: ${result.totalPublications}`,
      `medianImpressions: ${result.medianImpressions}`,
      `averageComments: ${result.averageComments}`,
      `qualifiedConversations: ${result.totalQualifiedConversations}`,
      `partnerConversations: ${result.totalPartnerConversations}`,
      `meetings: ${result.totalMeetings}`,
      `recommendsStrategy: ${result.recommendsStrategy}`,
    ].join('\n');
  }
  if (command === 'show') {
    const p = result.publication;
    return [
      `publication: ${p.id}`,
      `artifact: ${p.content_artifact_id}`,
      `channel: ${p.channel}`,
      `objective: ${p.objective || '-'}`,
      `snapshots: ${result.performanceSnapshots.length}`,
      `business outcomes: ${result.businessOutcomes.length}`,
      `signals: ${result.qualitativeSignals.length}`,
      `timeline events: ${result.timeline.length}`,
    ].join('\n');
  }
  if (result && result.id) {
    return `${command} ok: ${result.id}`;
  }
  return JSON.stringify(result, null, 2);
}

async function main(argv = process.argv.slice(2)) {
  const { command, parsed } = parseCommand(argv);
  if (command === 'help' || flag(parsed, '--help')) {
    printHelp();
    return 0;
  }

  const runners = {
    publish: runPublish,
    performance: runPerformance,
    'add-outcome': runAddOutcome,
    'add-signal': runAddSignal,
    show: runShow,
    list: runList,
    compare: runCompare,
  };
  const runner = runners[command];
  if (!runner) {
    throw new Error(`Unknown command: ${command}`);
  }

  const result = await runner(parsed);
  if (flag(parsed, '--json')) {
    console.log(JSON.stringify(result, null, 2));
  } else {
    console.log(formatText(command, result));
  }
  return 0;
}

if (require.main === module) {
  main().then(
    (code) => process.exit(code),
    (err) => {
      const message =
        err instanceof ContentOutcomeError
          ? `${err.code}: ${err.message}`
          : err && err.message
            ? err.message
            : String(err);
      console.error(message);
      process.exit(1);
    }
  );
}

module.exports = {
  main,
  parseCommand,
  printHelp,
};
