'use strict';

const { EVIDENCE_CLASS, PROHIBITED_CLAIM_PATTERNS, DIAGNOSIS_CLASS } = require('./types');
const { topFindings, partitionEvidence } = require('./evidence');

function assertConservativeLanguage(text) {
  const value = String(text || '');
  for (const pattern of PROHIBITED_CLAIM_PATTERNS) {
    if (pattern.test(value)) {
      throw new Error(`Prohibited claim detected: ${pattern}`);
    }
  }
  return value;
}

function classifyDiagnosis({ findings, score, inferredFindings = [] }) {
  const { measured, observed } = partitionEvidence(findings);
  const sourceCount = measured.length + observed.length;

  if (sourceCount < 3) {
    return {
      diagnosis_class: DIAGNOSIS_CLASS.INSUFFICIENT_EVIDENCE,
      rationale: 'Fewer than 3 MEASURED/OBSERVED findings — cannot responsibly diagnose',
    };
  }

  const deficiency = score?.score_components?.website_deficiency?.score ?? 0;
  const materialPerf = measured.filter((f) => f.category === 'performance').length;
  const convGap = observed.some((f) =>
    f.id === 'conv_no_obvious_path' || f.id === 'dom_no_contact_nav'
  );
  const techFailures = measured.filter((f) =>
    f.id === 'tech_http_error' || f.id === 'seo_sitemap_missing' || f.id === 'seo_robots_missing'
  ).length;
  const slowFetch = measured.some((f) =>
    f.ref === 'performance:fetch_ms' && (f.measurement?.fetch_ms ?? 0) >= 4000
  );

  const hasRedesignInference = inferredFindings.some((f) => f.id === 'inf_redesign_consideration');
  const hasHealthyInference = inferredFindings.some((f) => f.id === 'inf_healthy_site');

  if (deficiency <= 5 && !convGap && !slowFetch && (hasHealthyInference || materialPerf === 0)) {
    return {
      diagnosis_class: DIAGNOSIS_CLASS.HEALTHY_SITE,
      rationale: 'Low deficiency score with no material performance or conversion gaps',
    };
  }

  if (hasRedesignInference || (deficiency >= 15 && (materialPerf >= 2 || techFailures >= 2 || slowFetch))) {
    return {
      diagnosis_class: DIAGNOSIS_CLASS.REDESIGN_CANDIDATE,
      rationale: 'Multiple material deficiencies or measured performance/technical failures support broader redesign consideration',
    };
  }

  if (deficiency >= 6 || convGap || materialPerf >= 1) {
    return {
      diagnosis_class: DIAGNOSIS_CLASS.TARGETED_REMEDIATION,
      rationale: 'Specific improvements warranted but evidence does not support broad redesign',
    };
  }

  return {
    diagnosis_class: DIAGNOSIS_CLASS.INSUFFICIENT_EVIDENCE,
    rationale: 'Evidence mix does not support a confident commercial website diagnosis',
  };
}

function buildCommercialDiagnosis({ findings, business, economics, score, inferredFindings = [] }) {
  const material = topFindings(findings.filter((f) => f.evidence_class !== EVIDENCE_CLASS.INFERRED), 8);
  const classification = classifyDiagnosis({ findings, score, inferredFindings });

  const whatsWrong = material.slice(0, 5).map((f) => ({
    ref: f.ref || f.id,
    evidence_class: f.evidence_class,
    summary: f.summary,
  }));
  if (!whatsWrong.length) {
    whatsWrong.push({
      ref: null,
      evidence_class: EVIDENCE_CLASS.UNKNOWN,
      summary: 'Insufficient verified findings to diagnose specific issues',
    });
  }

  const commerciallyImportant = [];
  const cosmetic = [];
  for (const f of material) {
    const entry = { ref: f.ref || f.id, evidence_class: f.evidence_class, summary: f.summary };
    if (f.category === 'performance' || f.id === 'conv_no_obvious_path' || f.id === 'tech_no_https') {
      commerciallyImportant.push(entry);
    } else if (/missing meta description|lang attribute/i.test(f.summary)) {
      cosmetic.push(entry);
    } else if (f.category !== 'business') {
      commerciallyImportant.push(entry);
    }
  }

  const boundedInferences = inferredFindings.map((f) => ({
    ref: f.ref || f.id,
    evidence_class: EVIDENCE_CLASS.INFERRED,
    summary: f.summary,
    derived_from: f.derived_from || [],
  }));

  let redesignWarranted = 'unclear';
  if (classification.diagnosis_class === DIAGNOSIS_CLASS.HEALTHY_SITE) redesignWarranted = 'unlikely';
  else if (classification.diagnosis_class === DIAGNOSIS_CLASS.REDESIGN_CANDIDATE) redesignWarranted = 'plausible';
  else if (classification.diagnosis_class === DIAGNOSIS_CLASS.TARGETED_REMEDIATION) redesignWarranted = 'narrow_remediation_first';

  const narrowerRemediation = boundedInferences
    .filter((f) => /remediation|targeted|CTA|performance investigation/i.test(f.summary))
    .map((f) => f.summary);
  if (!narrowerRemediation.length) {
    narrowerRemediation.push('No narrow remediation path identified from current bounded inferences');
  }

  const adviseFirst = boundedInferences.slice(0, 3).map((f) => f.summary);
  if (!adviseFirst.length) adviseFirst.push('Gather additional deterministic measurements before advising');

  const worthAcquiring =
    score?.opportunity_score >= 45 &&
    !score?.deficiency_only_risk &&
    classification.diagnosis_class !== DIAGNOSIS_CLASS.HEALTHY_SITE &&
    (business.email || business.phone);

  const changeConclusion = [];
  const { measured } = partitionEvidence(findings);
  if (!measured.some((f) => f.source === 'pagespeed_insights')) {
    changeConclusion.push('Mobile/desktop PageSpeed measurements would increase confidence');
  }
  if (!business.email && !business.phone) {
    changeConclusion.push('Verified decision-maker contact would change recommendation');
  }
  if (economics?.economic_confidence === 'LOW' || economics?.economic_confidence === 'UNKNOWN') {
    changeConclusion.push('Prospect-specific scope evidence would change economics confidence');
  }

  const diagnosis = {
    diagnosis_class: classification.diagnosis_class,
    diagnosis_rationale: classification.rationale,
    what_is_wrong: whatsWrong,
    commercially_important: commerciallyImportant,
    cosmetic_or_low_priority: cosmetic,
    bounded_inferences: boundedInferences,
    redesign_warranted: redesignWarranted,
    narrower_remediation_may_suffice: narrowerRemediation,
    advise_first: adviseFirst,
    worth_acquiring: worthAcquiring,
    evidence_that_would_change_conclusion: changeConclusion,
    supporting_evidence_refs: [
      ...commerciallyImportant.map((e) => e.ref),
      ...boundedInferences.flatMap((e) => e.derived_from || []),
    ].filter(Boolean),
    doctrine: 'Discover → Diagnose → Advise',
  };

  for (const key of Object.keys(diagnosis)) {
    if (typeof diagnosis[key] === 'string') assertConservativeLanguage(diagnosis[key]);
    if (Array.isArray(diagnosis[key])) {
      for (const item of diagnosis[key]) {
        if (typeof item === 'string') assertConservativeLanguage(item);
        else if (item?.summary) assertConservativeLanguage(item.summary);
      }
    }
  }

  return diagnosis;
}

module.exports = {
  buildCommercialDiagnosis,
  classifyDiagnosis,
  assertConservativeLanguage,
  DIAGNOSIS_CLASS,
};
