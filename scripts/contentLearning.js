#!/usr/bin/env node
'use strict';

/**
 * SPEC-093 Paige Outcome Learning Loop CLI.
 *
 *   npm run content:learning -- evaluate --id=<pubId> --client-id=1
 *   npm run content:learning -- list --client-id=1
 *   npm run content:learning -- recompute --client-id=1
 *   npm run content:learning -- recommend --client-id=1 --objective=category_creation
 */

const {
  ContentLearningError,
  evaluateContentPublication,
  listContentLearnings,
  getContentLearning,
  generateContentRecommendation,
  recomputeContentLearnings,
} = require('../services/contentLearning');

function parseArgs(argv) {
  const args = { _: [] };
  for (const raw of argv) {
    if (raw.startsWith('--')) {
      const eq = raw.indexOf('=');
      if (eq === -1) args[raw.slice(2)] = true;
      else args[raw.slice(2, eq)] = raw.slice(eq + 1);
    } else {
      args._.push(raw);
    }
  }
  return args;
}

function usage() {
  console.log(`Paige Outcome Learning Loop (SPEC-093)

Usage:
  npm run content:learning -- <command> [options]

Commands:
  evaluate      Evaluate one SPEC-092 publication into learnings
  list          List content learnings for a tenant
  show          Show one learning by id
  recompute     Re-evaluate all publications for a tenant
  recommend     Ask Paige for the next content experiment

Options:
  --client-id=N
  --id=<uuid>
  --objective=<objective>
  --channel=linkedin
  --limit=N
`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const cmd = args._[0];
  if (!cmd || cmd === 'help' || args.help) {
    usage();
    process.exit(cmd ? 0 : 1);
  }

  const clientId = args['client-id'] || args.clientId || args.tenantId;
  let result;

  switch (cmd) {
    case 'evaluate': {
      if (!args.id) throw new ContentLearningError('id_required', '--id required');
      result = await evaluateContentPublication(args.id, { clientId });
      break;
    }
    case 'list': {
      result = await listContentLearnings({
        clientId,
        status: args.status,
        learningType: args['learning-type'] || args.learningType,
        objective: args.objective,
        topic: args.topic,
        channel: args.channel,
        limit: args.limit,
      });
      break;
    }
    case 'show': {
      if (!args.id) throw new ContentLearningError('id_required', '--id required');
      result = await getContentLearning(args.id, { clientId });
      break;
    }
    case 'recompute': {
      result = await recomputeContentLearnings({
        clientId,
        limit: args.limit,
      });
      break;
    }
    case 'recommend': {
      result = await generateContentRecommendation({
        clientId,
        tenantId: clientId,
        objective:
          args.goal ||
          'Build qualified attention and category understanding before the public Max reveal.',
        learningObjective: args.objective || 'category_creation',
        channel: args.channel || 'linkedin',
        campaignId: args['campaign-id'] || args.campaignId,
        topic: args.topic,
        limit: args.limit,
      });
      break;
    }
    default:
      usage();
      process.exit(1);
  }

  console.log(JSON.stringify(result, null, 2));
}

main().catch((err) => {
  if (err instanceof ContentLearningError) {
    console.error(`Error (${err.code}): ${err.message}`);
    process.exit(1);
  }
  console.error(err);
  process.exit(1);
});
