'use strict';

/**
 * Shared Operator Review Digest pattern for Max/Growth review-heavy artifacts.
 *
 * Operators see a concise digest first (decision, why, included/excluded,
 * watchouts, next step, primary actions). Full evidence stays behind an
 * optional "View evidence" surface — collapsed by default.
 *
 * Guardrails: presentation only. Does not send, write CRM, export, or
 * change accounts. Producers remain responsible for those invariants.
 */

const DIGEST_SECTION_KEYS = Object.freeze([
  'recommendedDecision',
  'whyRecommended',
  'included',
  'excluded',
  'keyWatchouts',
  'nextStepAfterApproval',
  'primaryActions',
]);

const DIGEST_SECTION_TITLES = Object.freeze({
  recommendedDecision: 'Recommended decision',
  whyRecommended: 'Why this is recommended',
  included: 'What is included',
  excluded: 'What is excluded / held back',
  keyWatchouts: 'Key watchouts',
  nextStepAfterApproval: 'Next step after approval',
  primaryActions: 'Primary actions',
});

const VIEW_EVIDENCE_LABEL = 'View evidence';
const EVIDENCE_COLLAPSED_NOTE =
  'Full sourced records are available under View evidence (collapsed by default).';

/**
 * @param {object} input
 * @param {string} [input.kind]
 * @param {string} [input.title]
 * @param {string|object} [input.recommendedDecision]
 * @param {string|string[]} [input.whyRecommended]
 * @param {string|string[]} [input.included]
 * @param {string|string[]} [input.excluded]
 * @param {string|string[]} [input.keyWatchouts]
 * @param {string} [input.nextStepAfterApproval]
 * @param {Array<{id?:string,label:string,style?:string}>|string[]} [input.primaryActions]
 * @param {object} [input.evidence]
 * @param {string} [input.disclaimer]
 * @param {object} [input.meta]
 */
function buildOperatorReviewDigest(input = {}) {
  const primaryActions = normalizePrimaryActions(input.primaryActions);
  const evidence = normalizeEvidence(input.evidence);

  return {
    kind: input.kind || 'operator_review_digest',
    title: input.title || 'Operator Review',
    pattern: 'operator_review_digest',
    recommendedDecision: asText(input.recommendedDecision),
    whyRecommended: asLineList(input.whyRecommended),
    included: asLineList(input.included),
    excluded: asLineList(input.excluded),
    keyWatchouts: asLineList(input.keyWatchouts),
    nextStepAfterApproval: asText(input.nextStepAfterApproval),
    primaryActions,
    evidence,
    evidenceCollapsedByDefault: true,
    viewEvidenceLabel: VIEW_EVIDENCE_LABEL,
    disclaimer: input.disclaimer || null,
    meta: input.meta && typeof input.meta === 'object' ? { ...input.meta } : {},
    // Presentation-only guardrail flags (producers may override honestly)
    reviewOnly: input.reviewOnly !== false,
    outreachCopyGenerated: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

function normalizePrimaryActions(actions) {
  if (!Array.isArray(actions) || !actions.length) return [];
  return actions
    .map((a, i) => {
      if (a == null) return null;
      if (typeof a === 'string') {
        const label = a.trim();
        if (!label) return null;
        return {
          id: slugifyAction(label) || `action_${i + 1}`,
          label,
          style: i === 0 ? 'primary' : 'secondary',
        };
      }
      if (typeof a === 'object') {
        const label = String(a.label || a.title || '').trim();
        if (!label) return null;
        return {
          id: String(a.id || slugifyAction(label) || `action_${i + 1}`),
          label,
          style: a.style || (i === 0 ? 'primary' : 'secondary'),
          message: a.message || null,
        };
      }
      return null;
    })
    .filter(Boolean);
}

function normalizeEvidence(evidence) {
  if (!evidence || typeof evidence !== 'object') {
    return {
      collapsedByDefault: true,
      label: VIEW_EVIDENCE_LABEL,
      sections: [],
      records: [],
      rejectedOrHeld: [],
      auditNotes: [],
    };
  }
  return {
    collapsedByDefault: evidence.collapsedByDefault !== false,
    label: evidence.label || VIEW_EVIDENCE_LABEL,
    sections: Array.isArray(evidence.sections) ? evidence.sections : [],
    records: Array.isArray(evidence.records) ? evidence.records : [],
    rejectedOrHeld: Array.isArray(evidence.rejectedOrHeld)
      ? evidence.rejectedOrHeld
      : [],
    auditNotes: asLineList(evidence.auditNotes),
  };
}

function asText(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'object' && value.label) return String(value.label).trim();
  return String(value).trim();
}

function asLineList(value) {
  if (value == null) return [];
  if (Array.isArray(value)) {
    return value
      .map((v) => {
        if (v == null) return '';
        if (typeof v === 'string') return v.trim();
        if (typeof v === 'object') {
          if (v.label) return String(v.label).trim();
          if (v.text) return String(v.text).trim();
          if (v.companyName || v.company) {
            return String(v.companyName || v.company).trim();
          }
        }
        return String(v).trim();
      })
      .filter(Boolean);
  }
  const t = String(value).trim();
  return t ? [t] : [];
}

function slugifyAction(label) {
  return String(label || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_|_$/g, '')
    .slice(0, 48);
}

/**
 * Format digest body only (no evidence dump).
 * @param {object} digest
 */
function formatOperatorReviewDigestMessage(digest) {
  const d = digest || {};
  const lines = [];
  if (d.title) {
    lines.push(d.title);
    lines.push('');
  }

  const pushSection = (key, bodyLines) => {
    const title = DIGEST_SECTION_TITLES[key] || key;
    lines.push(`## ${title}`);
    if (!bodyLines || !bodyLines.length) {
      lines.push('_None._');
    } else {
      bodyLines.forEach((line) => lines.push(line));
    }
    lines.push('');
  };

  pushSection('recommendedDecision', d.recommendedDecision ? [d.recommendedDecision] : []);
  pushSection(
    'whyRecommended',
    (d.whyRecommended || []).map((line) =>
      line.startsWith('-') || line.startsWith('•') ? line : `- ${line}`
    )
  );
  pushSection(
    'included',
    (d.included || []).map((line) =>
      line.startsWith('-') || line.startsWith('•') ? line : `- ${line}`
    )
  );
  pushSection(
    'excluded',
    (d.excluded || []).map((line) =>
      line.startsWith('-') || line.startsWith('•') ? line : `- ${line}`
    )
  );
  pushSection(
    'keyWatchouts',
    (d.keyWatchouts || []).map((line) =>
      line.startsWith('-') || line.startsWith('•') ? line : `- ${line}`
    )
  );
  pushSection(
    'nextStepAfterApproval',
    d.nextStepAfterApproval ? [d.nextStepAfterApproval] : []
  );

  const actions = d.primaryActions || [];
  pushSection(
    'primaryActions',
    actions.map((a) => `- ${a.label || a}`)
  );

  lines.push(EVIDENCE_COLLAPSED_NOTE);
  if (d.disclaimer) {
    lines.push('');
    lines.push(d.disclaimer);
  }
  return lines.join('\n').trim();
}

/**
 * Format evidence payload for optional expansion (not the default operator view).
 * @param {object} evidence
 * @param {object} [opts]
 */
function formatOperatorReviewEvidenceMessage(evidence, opts = {}) {
  const e = normalizeEvidence(evidence);
  const lines = [];
  lines.push(`## ${e.label || VIEW_EVIDENCE_LABEL}`);
  if (opts.collapsedNote !== false) {
    lines.push('_Collapsed by default — expand only when you need full sourced records._');
  }
  lines.push('');

  if ((e.sections || []).length) {
    e.sections.forEach((section, idx) => {
      const title = section.title || `Evidence section ${idx + 1}`;
      lines.push(`### ${title}`);
      if (section.intro) lines.push(section.intro);
      lines.push('');
      const records = section.records || section.rows || [];
      if (!records.length) {
        lines.push('_None._');
        lines.push('');
        return;
      }
      records.forEach((row, i) => {
        lines.push(formatEvidenceRecord(row, i + 1));
        lines.push('');
      });
    });
  } else if ((e.records || []).length) {
    e.records.forEach((row, i) => {
      lines.push(formatEvidenceRecord(row, i + 1));
      lines.push('');
    });
  } else {
    lines.push('_No evidence records._');
    lines.push('');
  }

  if ((e.rejectedOrHeld || []).length) {
    lines.push('### Rejected / held candidates');
    lines.push('');
    e.rejectedOrHeld.forEach((row, i) => {
      lines.push(formatEvidenceRecord(row, i + 1));
      lines.push('');
    });
  }

  if ((e.auditNotes || []).length) {
    lines.push('### Audit notes');
    e.auditNotes.forEach((note) => lines.push(`- ${note}`));
    lines.push('');
  }

  return lines.join('\n').trim();
}

function formatEvidenceRecord(row, index) {
  const r = row || {};
  const company = r.companyName || r.company || r.name || '—';
  const lines = [
    `${index}. **${company}**`,
    `   - Company: ${company}`,
    `   - Location: ${r.location || '—'}`,
    `   - Source URL: ${r.sourceUrl || r.website || r.url || '—'}`,
    `   - Why it fits / fit rationale: ${
      r.fitRationale || r.whyItFits || r.fitReason || '—'
    }`,
    `   - Risk/uncertainty: ${
      r.riskUncertainty || r.risks || r.statusReason || '—'
    }`,
    `   - Confidence: ${r.confidence || '—'}`,
    `   - Review status: ${r.reviewStatus || r.status || '—'}`,
  ];
  if (r.suggestedContactRole) {
    lines.push(`   - Suggested contact role: ${r.suggestedContactRole}`);
  }
  if (r.relationship) {
    lines.push(`   - Relationship: ${r.relationship}`);
  }
  if (r.rejectionReason || r.statusReason) {
    lines.push(
      `   - Rejection / hold reason: ${r.rejectionReason || r.statusReason}`
    );
  }
  if (r.auditNote) {
    lines.push(`   - Audit note: ${r.auditNote}`);
  }
  if (r.doNotOutreach) {
    lines.push('   - Outreach: do not include in campaign outreach');
  }
  return lines.join('\n');
}

/**
 * Full artifact message: digest first, then optional evidence (collapsed marker).
 * Default omitEvidence=true so operators are not dumped into full records.
 *
 * @param {object} digest - from buildOperatorReviewDigest
 * @param {object} [opts]
 * @param {boolean} [opts.includeEvidence=false]
 * @param {string} [opts.closingQuestion]
 */
function formatOperatorReviewArtifactMessage(digest, opts = {}) {
  const includeEvidence = opts.includeEvidence === true;
  const parts = [formatOperatorReviewDigestMessage(digest)];

  if (includeEvidence && digest && digest.evidence) {
    parts.push('');
    parts.push(formatOperatorReviewEvidenceMessage(digest.evidence));
  }

  if (opts.closingQuestion) {
    parts.push('');
    parts.push(opts.closingQuestion);
  }

  return parts.join('\n').trim();
}

/**
 * Index helpers for tests / UI: digest body ends before evidence marker.
 * @param {string} message
 */
function splitDigestAndEvidence(message) {
  const text = String(message || '');
  const evidenceIdx = text.search(
    /(?:^|\n)##\s*View evidence\b|(?:^|\n)Full sourced records are available under View evidence/i
  );
  if (evidenceIdx < 0) {
    return { digest: text.trim(), evidence: '', evidenceCollapsed: true };
  }
  // Prefer splitting at the collapsed note that sits at end of digest.
  const noteIdx = text.indexOf(EVIDENCE_COLLAPSED_NOTE);
  if (noteIdx >= 0) {
    const digest = text.slice(0, noteIdx + EVIDENCE_COLLAPSED_NOTE.length).trim();
    const after = text.slice(noteIdx + EVIDENCE_COLLAPSED_NOTE.length).trim();
    const evidenceStart = after.search(/(?:^|\n)##\s*View evidence\b/i);
    return {
      digest,
      evidence: evidenceStart >= 0 ? after.slice(evidenceStart).trim() : '',
      evidenceCollapsed: true,
    };
  }
  return {
    digest: text.slice(0, evidenceIdx).trim(),
    evidence: text.slice(evidenceIdx).trim(),
    evidenceCollapsed: true,
  };
}

module.exports = {
  DIGEST_SECTION_KEYS,
  DIGEST_SECTION_TITLES,
  VIEW_EVIDENCE_LABEL,
  EVIDENCE_COLLAPSED_NOTE,
  buildOperatorReviewDigest,
  formatOperatorReviewDigestMessage,
  formatOperatorReviewEvidenceMessage,
  formatOperatorReviewArtifactMessage,
  formatEvidenceRecord,
  splitDigestAndEvidence,
  normalizePrimaryActions,
  normalizeEvidence,
  asLineList,
};
