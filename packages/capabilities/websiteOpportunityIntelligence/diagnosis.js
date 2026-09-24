'use strict';

const { EVIDENCE_CLASS, PROHIBITED_CLAIM_PATTERNS } = require('./types');
const { topFindings } = require('./evidence');

function assertConservativeLanguage(text) {
  const value = String(text || '');
  for (const pattern of PROHIBITED_CLAIM_PATTERNS) {
    if (pattern.test(value)) {
      throw new Error(`Prohibited claim detected: ${pattern}`);
    }
  }
  return value;
}

function buildCommercialDiagnosis({ findings, business, economics, score }) {
  const material = topFindings(findings, 8);
  const measuredPerf = findings.filter(
    (f) => f.category === 'performance' && f.evidence_class === EVIDENCE_CLASS.MEASURED
  );
  const convGap = findings.some((f) => f.id === 'conv_no_obvious_path' || f.id === 'dom_no_contact_nav');

  const whatsWrong = [];
  for (const f of material.slice(0, 5)) {
    whatsWrong.push(f.summary);
  }
  if (!whatsWrong.length) whatsWrong.push('Insufficient verified findings to diagnose specific issues');

  const commerciallyImportant = [];
  const cosmetic = [];
  for (const f of material) {
    if (f.category === 'performance' || f.id === 'conv_no_obvious_path' || f.id === 'tech_no_https') {
      commerciallyImportant.push(f.summary);
    } else if (/missing meta description|lang attribute/i.test(f.summary)) {
      cosmetic.push(f.summary);
    } else {
      commerciallyImportant.push(f.summary);
    }
  }

  let redesignWarranted = 'unclear';
  if (score?.deficiency_only_risk) redesignWarranted = 'unlikely';
  else if (measuredPerf.length >= 2 && convGap) redesignWarranted = 'plausible';
  else if (measuredPerf.length === 0 && convGap) redesignWarranted = 'narrow_remediation_first';

  const narrowerRemediation = [];
  if (measuredPerf.length === 1) narrowerRemediation.push('Performance optimization may address the primary measured issue');
  if (convGap) narrowerRemediation.push('Conversion path improvements (contact visibility, CTA clarity) may suffice');
  if (!narrowerRemediation.length) narrowerRemediation.push('No narrow remediation path identified from current evidence');

  const adviseFirst = commerciallyImportant.slice(0, 3);
  if (!adviseFirst.length) adviseFirst.push('Gather additional deterministic measurements before advising');

  const worthAcquiring =
    score?.opportunity_score >= 45 &&
    !score?.deficiency_only_risk &&
    (business.email || business.phone);

  const changeConclusion = [];
  if (!measuredPerf.length) changeConclusion.push('Mobile/desktop PageSpeed measurements would increase confidence');
  if (!business.email && !business.phone) changeConclusion.push('Verified decision-maker contact would change recommendation');
  if (economics?.estimated_contribution < 500) changeConclusion.push('Higher contract scope or lower hour estimate would change economics');

  const diagnosis = {
    what_is_wrong: whatsWrong,
    commercially_important: commerciallyImportant,
    cosmetic_or_low_priority: cosmetic,
    redesign_warranted: redesignWarranted,
    narrower_remediation_may_suffice: narrowerRemediation,
    advise_first: adviseFirst,
    worth_acquiring: worthAcquiring,
    evidence_that_would_change_conclusion: changeConclusion,
    doctrine: 'Discover → Diagnose → Advise',
  };

  for (const key of Object.keys(diagnosis)) {
    if (typeof diagnosis[key] === 'string') assertConservativeLanguage(diagnosis[key]);
    if (Array.isArray(diagnosis[key])) diagnosis[key].forEach(assertConservativeLanguage);
  }

  return diagnosis;
}

module.exports = {
  buildCommercialDiagnosis,
  assertConservativeLanguage,
};
