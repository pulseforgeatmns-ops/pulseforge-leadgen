#!/usr/bin/env node
'use strict';

/**
 * SPEC-113 — compile market documents into a published AIM.
 *
 *   node scripts/aicCompile.js --client fedir-aic
 *   node scripts/aicCompile.js --client fedir-aic --approve --publish
 */

const {
  getCompiler,
  loadFixtureDocuments,
} = require('../services/acquisitionIntelligenceCompiler');
const { qualify } = require('../services/acquisitionIntelligenceModel');

function arg(name, fallback = null) {
  const idx = process.argv.indexOf(`--${name}`);
  if (idx === -1) return fallback;
  return process.argv[idx + 1] && !String(process.argv[idx + 1]).startsWith('--')
    ? process.argv[idx + 1]
    : true;
}

function main() {
  const clientKey = String(arg('client', 'fedir-aic'));
  const compiler = getCompiler();
  const workspace = compiler.ingestAndCompile(
    { clientKey, clientName: arg('name') || clientKey },
    loadFixtureDocuments()
  );

  console.log(`Workspace ${workspace.id}`);
  console.log(`Status: ${workspace.status}`);
  console.log(`Documents: ${workspace.documents.map((d) => d.title).join(', ')}`);
  console.log(`Concepts: ${workspace.concepts.length}`);
  console.log(`Edges: ${workspace.edges.length}`);
  for (const concept of workspace.concepts.slice(0, 12)) {
    console.log(`  [${concept.type}] ${concept.label} ← ${concept.provenance.documentTitle} / ${concept.provenance.section}`);
  }

  if (!arg('approve') && !arg('publish')) return;

  compiler.approve(workspace.id, { operator: 'cli' });
  if (!arg('publish')) {
    console.log('Approved. Pass --publish to emit a runtime AIM.');
    return;
  }
  const result = compiler.publish(workspace.id);
  console.log(`Published AIM ${result.aim.id} status=${result.aim.status}`);
  console.log(`Mission: ${result.aim.mission.transformation}`);

  if (arg('qualify')) {
    const q = qualify(clientKey, {
      id: 'cli-prospect',
      name: arg('name') || 'CLI prospect',
      jobTitle: 'Founder',
      description: 'Owner-operated. I do everything myself.',
      signals: [{ type: 'hiring', label: 'job postings; hiring repeatedly' }],
    });
    console.log(`Recommendation: ${q.qualification.overallRecommendation.id}`);
    if (q.qualification.topPain) {
      console.log(`Top pain: ${q.qualification.topPain.label} ${q.qualification.topPain.percent}%`);
    }
  }
}

main();
