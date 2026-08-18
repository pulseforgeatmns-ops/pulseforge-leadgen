#!/usr/bin/env node
'use strict';

/**
 * SPEC-112 — inspect an Acquisition Intelligence Model.
 *
 *   node scripts/aimInspect.js
 *   node scripts/aimInspect.js --client fedir
 *   node scripts/aimInspect.js --qualify --name "North Loop Agency" --signals "owner replying to reviews,job postings"
 */

const {
  getAim,
  qualify,
  pilotStatus,
  FEDIR_CLIENT_KEY,
} = require('../services/acquisitionIntelligenceModel');

function arg(name, fallback = null) {
  const idx = process.argv.indexOf(`--${name}`);
  if (idx === -1) return fallback;
  return process.argv[idx + 1] && !String(process.argv[idx + 1]).startsWith('--')
    ? process.argv[idx + 1]
    : true;
}

function main() {
  const clientKey = String(arg('client', FEDIR_CLIENT_KEY));
  const model = getAim(clientKey);
  if (!model) {
    console.error(`AIM not found: ${clientKey}`);
    process.exit(1);
  }

  if (arg('qualify')) {
    const signals = String(arg('signals', ''))
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
      .map((label) => ({ type: 'observed', label }));
    const prospect = {
      id: arg('id') || 'cli-prospect',
      name: arg('name') || 'CLI prospect',
      industry: arg('industry') || '',
      jobTitle: arg('title') || 'Founder',
      description: arg('description') || '',
      signals,
    };
    const result = qualify(clientKey, prospect);
    const q = result.qualification;
    console.log(`Prospect: ${q.prospectName}`);
    console.log(`ICP Fit: ${q.dimensions.icpFit.score}`);
    console.log(`Pain Match: ${q.dimensions.painMatch.score}`);
    console.log(`Evidence Quality: ${q.dimensions.evidenceQuality.score}`);
    console.log(`Buying Readiness: ${q.dimensions.buyingReadiness.score}`);
    console.log(`Confidence: ${q.dimensions.confidence.score}`);
    console.log(
      `Recommendation: ${q.overallRecommendation.id} — ${q.overallRecommendation.reason}`
    );
    if (q.topPain) console.log(`Top pain: ${q.topPain.label} ${q.topPain.percent}%`);
    if (result.briefing && result.briefing.available) {
      console.log(`Paige CTA: ${result.briefing.cta}`);
    }
    return;
  }

  if (arg('pilot')) {
    console.log(JSON.stringify(pilotStatus(clientKey), null, 2));
    return;
  }

  console.log(`AIM ${model.id} (${model.clientName})`);
  console.log(`Status: ${model.status}`);
  console.log(`Mission: ${model.mission.transformation}`);
  console.log(`Current: ${model.transformation.currentState}`);
  console.log(`Future:  ${model.transformation.futureState}`);
  console.log('ICP:');
  for (const key of ['company', 'founder', 'size', 'geography', 'exclusions']) {
    const field = model.icp[key];
    console.log(`  ${key}: ${field.known ? field.reasoning : `(unknown) ${field.unknowns[0] || ''}`}`);
  }
  console.log('Pain ontology:');
  for (const category of model.painOntology.categories) {
    console.log(`  ${category.label}`);
    for (const problem of category.problems) {
      console.log(`    - ${problem.label}: ${problem.signals.join('; ')}`);
    }
  }
}

main();
