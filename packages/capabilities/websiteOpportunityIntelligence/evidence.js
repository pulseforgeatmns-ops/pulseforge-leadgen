'use strict';

const { EVIDENCE_CLASS, buildFinding } = require('./types');

function assertEvidenceClass(value) {
  const normalized = String(value || '').toUpperCase();
  if (!Object.values(EVIDENCE_CLASS).includes(normalized)) {
    throw new Error(`Invalid evidence class: ${value}`);
  }
  return normalized;
}

function isMeasuredFinding(finding) {
  return assertEvidenceClass(finding.evidence_class) === EVIDENCE_CLASS.MEASURED;
}

function preserveUnknown(value, fallback = EVIDENCE_CLASS.UNKNOWN) {
  if (value == null || value === '') return fallback;
  return value;
}

function mergeFindings(...groups) {
  const out = [];
  for (const group of groups) {
    if (!group) continue;
    if (Array.isArray(group)) out.push(...group);
    else if (Array.isArray(group.findings)) out.push(...group.findings);
  }
  return out.map((f) => ({
    ...buildFinding(f),
    evidence_class: assertEvidenceClass(f.evidence_class || EVIDENCE_CLASS.UNKNOWN),
  }));
}

function collectEvidenceRefs(findings) {
  return findings
    .filter((f) => f.ref || f.id)
    .map((f) => ({
      ref: f.ref || f.id,
      evidence_class: f.evidence_class,
      summary: f.summary,
      source: f.source,
      observed_at: f.observed_at,
    }));
}

function enforceEvidenceIntegrity(assessment, findings) {
  const measured = findings.filter(isMeasuredFinding);
  for (const m of measured) {
    if (m.measurement == null && m.detail == null) {
      throw new Error(`MEASURED finding missing measurement: ${m.id || m.summary}`);
    }
  }
  for (const f of findings) {
    if (f.evidence_class === EVIDENCE_CLASS.UNKNOWN && /definitely|clearly losing|costing you/i.test(f.summary)) {
      throw new Error(`UNKNOWN evidence cannot carry negative sales assertion: ${f.summary}`);
    }
  }
  return {
    ...assessment,
    evidence_refs: collectEvidenceRefs(findings),
  };
}

function topFindings(findings, limit = 3) {
  const priority = {
    [EVIDENCE_CLASS.MEASURED]: 4,
    [EVIDENCE_CLASS.OBSERVED]: 3,
    [EVIDENCE_CLASS.INFERRED]: 2,
    [EVIDENCE_CLASS.UNKNOWN]: 1,
  };
  return [...findings]
    .sort((a, b) => (priority[b.evidence_class] || 0) - (priority[a.evidence_class] || 0))
    .slice(0, limit);
}

module.exports = {
  assertEvidenceClass,
  isMeasuredFinding,
  preserveUnknown,
  mergeFindings,
  collectEvidenceRefs,
  enforceEvidenceIntegrity,
  topFindings,
};
