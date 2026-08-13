'use strict';

/**
 * SPEC-092 Content Outcome Intelligence CLI.
 *
 *   npm run content:outcome -- publish --client-id=1 --artifact=pc-123 --channel=linkedin
 *   npm run content:outcome -- performance --id=<pubId> --impressions=8400 --observed-at=...
 *   npm run content:outcome -- add-outcome --id=<pubId> --type=partner_conversation --attribution=direct
 *   npm run content:outcome -- add-signal --id=<pubId> --type=message_resonance --description="..."
 *   npm run content:outcome -- show --id=<pubId>
 *   npm run content:outcome -- list --client-id=1
 *   npm run content:outcome -- compare --client-id=1 --group-by=objective
 */

try {
  require('dotenv').config();
} catch (_) {
  // optional in unit/help paths
}

const {
  CHANNELS,
  OBJECTIVES,
  BUSINESS_OUTCOME_TYPES,
  ATTRIBUTION_LEVELS,
  SIGNAL_TYPES,
  ContentOutcomeError,
  createContentPublication,
  addPerformanceSnapshot,
  addBusinessOutcome,
  addQualitativeSignal,
  getPublicationOutcome,
  listContentOutcomes,
  compareContentOutcomes,
  toIntelligencePayload,
} = require('../services/contentOutcomeIntelligence');

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    command: null,
    clientId: null,
    id: null,
    artifact: null,
    channel: 'linkedin',
    objective: null,
    topic: null,
    thesis: null,
    format: null,
    audience: null,
    campaignId: null,
    title: null,
    url: null,
    externalPostId: null,
    publishedAt: null,
    observedAt: null,
    occurredAt: null,
    impressions: null,
    membersReached: null,
    reactions: null,
    comments: null,
    reposts: null,
    saves: null,
    profileViews: null,
    followersGained: null,
    connectionRequests: null,
    type: null,
    attribution: 'unknown',
    description: null,
    companyId: null,
    personId: null,
    interactionId: null,
    evidenceId: null,
    confidence: null,
    audienceType: null,
    sentiment: null,
    strength: null,
    groupBy: 'objective',
    limit: null,
    json: false,
    help: false,
  };

  const positionals = [];
  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg === '--json') {
      options.json = true;
      continue;
    }
    if (!arg.startsWith('--')) {
      positionals.push(arg);
      continue;
    }
    const eq = arg.indexOf('=');
    if (eq === -1) {
      throw new Error(`Unknown argument: ${arg}`);
    }
    const key = arg.slice(2, eq);
    const value = arg.slice(eq + 1);
    const map = {
      'client-id': 'clientId',
      id: 'id',
      artifact: 'artifact',
      'content-artifact-id': 'artifact',
      channel: 'channel',
      objective: 'objective',
      topic: 'topic',
      thesis: 'thesis',
      format: 'format',
      audience: 'audience',
      'intended-audience': 'audience',
      'campaign-id': 'campaignId',
      title: 'title',
      url: 'url',
      'external-url': 'url',
      'external-post-id': 'externalPostId',
      'published-at': 'publishedAt',
      'observed-at': 'observedAt',
      'occurred-at': 'occurredAt',
      impressions: 'impressions',
      'members-reached': 'membersReached',
      reactions: 'reactions',
      comments: 'comments',
      reposts: 'reposts',
      saves: 'saves',
      'profile-views': 'profileViews',
      'followers-gained': 'followersGained',
      'connection-requests': 'connectionRequests',
      type: 'type',
      'outcome-type': 'type',
      'signal-type': 'type',
      attribution: 'attribution',
      description: 'description',
      'company-id': 'companyId',
      'person-id': 'personId',
      'interaction-id': 'interactionId',
      'evidence-id': 'evidenceId',
      confidence: 'confidence',
      'audience-type': 'audienceType',
      sentiment: 'sentiment',
      strength: 'strength',
      'group-by': 'groupBy',
      limit: 'limit',
    };
    const dest = map[key];
    if (!dest) throw new Error(`Unknown argument: --${key}`);
    options[dest] = value;
  }

  if (positionals[0]) options.command = positionals[0];
  return options;
}

function printHelp() {
  console.log(`Content Outcome Intelligence (SPEC-092)

Usage:
  npm run content:outcome -- <command> [options]

Commands:
  publish       Create a content publication
  performance   Add an immutable performance snapshot
  add-outcome   Add a business outcome
  add-signal    Add a qualitative signal
  show          Show full outcome history for a publication
  list          List content outcomes for a tenant
  compare       Deterministic aggregates (no recommendations)

Common options:
  --client-id=<n>           Tenant client_id (required for list/compare/publish)
  --id=<publicationId>      Publication id
  --json                    Print JSON only
  --help

publish:
  --artifact=<id>           Paige pending_comments id or manual key (required)
  --channel=${CHANNELS.join('|')}  (default linkedin)
  --objective=${OBJECTIVES.join('|')}
  --topic=... --thesis=... --format=... --audience=a,b
  --url=... --external-post-id=... --published-at=<iso> --title=...

performance:
  --observed-at=<iso>
  --impressions=N --reactions=N --comments=N --reposts=N --saves=N
  --members-reached=N --profile-views=N --followers-gained=N --connection-requests=N

add-outcome:
  --type=${BUSINESS_OUTCOME_TYPES.join('|')}
  --attribution=${ATTRIBUTION_LEVELS.join('|')}
  --description=... --occurred-at=<iso>
  --company-id=... --person-id=... --interaction-id=... --evidence-id=...
  --confidence=0..1

add-signal:
  --type=${SIGNAL_TYPES.join('|')}
  --description=... (required)
  --audience-type=... --sentiment=... --strength=... --evidence-id=...

compare:
  --group-by=objective|topic|format|intendedAudience|channel
`);
}

async function main() {
  const options = parseArgs();
  if (options.help || !options.command) {
    printHelp();
    process.exit(options.help ? 0 : 1);
  }

  let result;
  switch (options.command) {
    case 'publish': {
      result = await createContentPublication({
        clientId: options.clientId,
        contentArtifactId: options.artifact,
        channel: options.channel,
        objective: options.objective,
        topic: options.topic,
        thesis: options.thesis,
        format: options.format,
        intendedAudience: options.audience,
        campaignId: options.campaignId,
        title: options.title,
        externalUrl: options.url,
        externalPostId: options.externalPostId,
        publishedAt: options.publishedAt,
      });
      break;
    }
    case 'performance': {
      result = await addPerformanceSnapshot(options.id, {
        clientId: options.clientId,
        observedAt: options.observedAt,
        impressions: options.impressions,
        membersReached: options.membersReached,
        reactions: options.reactions,
        comments: options.comments,
        reposts: options.reposts,
        saves: options.saves,
        profileViewsAttributed: options.profileViews,
        followersGained: options.followersGained,
        connectionRequests: options.connectionRequests,
      });
      break;
    }
    case 'add-outcome': {
      result = await addBusinessOutcome(options.id, {
        clientId: options.clientId,
        outcomeType: options.type,
        attribution: options.attribution,
        description: options.description,
        occurredAt: options.occurredAt,
        companyId: options.companyId,
        personId: options.personId,
        interactionId: options.interactionId,
        evidenceId: options.evidenceId,
        confidence: options.confidence,
      });
      break;
    }
    case 'add-signal': {
      result = await addQualitativeSignal(options.id, {
        clientId: options.clientId,
        signalType: options.type,
        description: options.description,
        observedAt: options.observedAt,
        audienceType: options.audienceType,
        sentiment: options.sentiment,
        strength: options.strength,
        evidenceId: options.evidenceId,
      });
      break;
    }
    case 'show': {
      const full = await getPublicationOutcome(options.id, {
        clientId: options.clientId,
      });
      result = {
        ...full,
        intelligence: toIntelligencePayload(full),
      };
      break;
    }
    case 'list': {
      result = await listContentOutcomes({
        clientId: options.clientId,
        channel: options.channel !== 'linkedin' ? options.channel : undefined,
        objective: options.objective,
        topic: options.topic,
        limit: options.limit,
      });
      break;
    }
    case 'compare': {
      result = await compareContentOutcomes({
        clientId: options.clientId,
        objective: options.objective,
        topic: options.topic,
        groupBy: options.groupBy,
      });
      break;
    }
    default:
      throw new Error(`Unknown command: ${options.command}`);
  }

  if (options.json) {
    console.log(JSON.stringify(result, null, 2));
  } else {
    console.log(JSON.stringify(result, null, 2));
  }
}

main().catch((err) => {
  if (err instanceof ContentOutcomeError) {
    console.error(`${err.code}: ${err.message}`);
    process.exit(1);
  }
  console.error(err && err.message ? err.message : err);
  process.exit(1);
});
