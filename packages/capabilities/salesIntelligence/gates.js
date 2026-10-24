'use strict';

/**
 * Sales Intelligence quality gates (SPEC-048).
 */

const {
  GATE_REASONS,
  CONFIDENCE_LABEL,
  buildGateRejection,
  buildSalesIntelligenceProfile,
} = require('./types');
const { buildClientPlaybook } = require('../playbook/types');

/**
 * Apply profile-level gates. Mutates a copy via rebuild.
 * @param {object} profile
 * @param {object} [ctx]
 * @returns {object} profile with gateRejections + sendable
 */
function applyProfileGates(profile, ctx = {}) {
  const rejections = [];
  const playbook = ctx.playbook ? buildClientPlaybook(ctx.playbook) : null;

  // Wrong industry vs playbook avoid list
  if (playbook && profile.industry) {
    const industry = String(profile.industry).toLowerCase();
    const avoid = playbook.idealCustomer.industriesToAvoid || [];
    for (const term of avoid) {
      if (term && industry.includes(String(term).toLowerCase())) {
        rejections.push(
          buildGateRejection({
            reason: GATE_REASONS.WRONG_INDUSTRY,
            evidence: `Industry "${profile.industry}" matches playbook avoid term "${term}"`,
            regenerationRecommendation:
              'Exclude prospect or re-derive with correct industry classification',
          })
        );
      }
    }
    const targets = [
      ...(playbook.targetMarkets || []),
      ...(playbook.idealCustomer.primaryMarkets || []),
    ].map((t) => String(t).toLowerCase());
    if (
      targets.length &&
      !targets.some(
        (t) =>
          industry.includes(t) ||
          t.includes(industry) ||
          industry.split(/\s+/).some((w) => w.length > 3 && t.includes(w))
      )
    ) {
      // Soft: only reject when industry is known and clearly unrelated
      if (profile.confidence === CONFIDENCE_LABEL.LOW && !profile.buying_signals.length) {
        rejections.push(
          buildGateRejection({
            reason: GATE_REASONS.WRONG_INDUSTRY,
            evidence: `Industry "${profile.industry}" does not overlap playbook targets (${targets.slice(0, 3).join(', ')})`,
            regenerationRecommendation:
              'Confirm industry evidence before generating outreach',
          })
        );
      }
    }
  }

  if (!profile.industry) {
    rejections.push(
      buildGateRejection({
        reason: GATE_REASONS.WRONG_INDUSTRY,
        evidence: 'No industry evidenced on prospect or Company Intelligence',
        regenerationRecommendation: 'Enrich industry before sales intelligence',
      })
    );
  }

  if (
    profile.decision_maker_confidence === CONFIDENCE_LABEL.LOW &&
    !profile.decision_maker
  ) {
    rejections.push(
      buildGateRejection({
        reason: GATE_REASONS.WRONG_BUYER,
        evidence: 'No decision-maker inference with supporting evidence',
        regenerationRecommendation: 'Enrich contact title before outreach',
      })
    );
  }

  const claims = profile.personalization_claims || [];
  const unverified = claims.filter((c) => !c.verified || !c.evidenceRef);
  for (const c of unverified) {
    rejections.push(
      buildGateRejection({
        reason: GATE_REASONS.UNSUPPORTED_PERSONALIZATION,
        evidence: `Claim lacks verified evidence: "${c.claim}"`,
        regenerationRecommendation:
          'Remove unsupported claim or attach evidenceRef from Company Intelligence / Signals',
      })
    );
  }

  const verified = claims.filter((c) => c.verified && c.evidenceRef);
  if (!verified.length) {
    rejections.push(
      buildGateRejection({
        reason: GATE_REASONS.UNSUPPORTED_PERSONALIZATION,
        evidence: 'No verified personalization claims',
        regenerationRecommendation:
          'Require at least one evidence-backed company observation',
      })
    );
  }

  if (
    !profile.recommended_angle ||
    /^credibility-first|^generic/i.test(profile.recommended_angle)
  ) {
    if ((profile.confidenceScore || 0) < 0.5) {
      rejections.push(
        buildGateRejection({
          reason: GATE_REASONS.GENERIC_VALUE_PROPOSITION,
          evidence: `Weak angle: "${profile.recommended_angle || '(empty)'}"`,
          regenerationRecommendation:
            'Derive angle from Active Business Signals or Opportunity Brief',
        })
      );
    }
  }

  if (
    profile.confidence === CONFIDENCE_LABEL.LOW ||
    (profile.confidenceScore != null && profile.confidenceScore < 0.45)
  ) {
    rejections.push(
      buildGateRejection({
        reason: GATE_REASONS.LOW_REASONING_CONFIDENCE,
        evidence: `confidence=${profile.confidence} score=${profile.confidenceScore}`,
        regenerationRecommendation:
          'Gather industry, buyer title, or Active signals before generating copy',
      })
    );
  }

  return buildSalesIntelligenceProfile({
    ...profile,
    gateRejections: rejections,
    sendable: rejections.length === 0,
  });
}

/**
 * Gate generated outreach copy against writing principles.
 * @param {string} letterBody
 * @param {object} profile
 * @param {object} [opts]
 * @returns {object[]} gate rejections
 */
function gateOutreachCopy(letterBody, profile, opts = {}) {
  const rejections = [];
  const body = String(letterBody || '');
  const clientNames = [
    ...(opts.clientNames || []),
    'Anchor',
    'AS Cleaning',
    'Pulseforge',
  ].filter(Boolean);

  if (!body.trim()) {
    rejections.push(
      buildGateRejection({
        reason: GATE_REASONS.GENERIC_VALUE_PROPOSITION,
        evidence: 'Empty letter body',
        regenerationRecommendation: 'Regenerate from Sales Intelligence messaging strategy',
      })
    );
    return rejections;
  }

  // Prospect first: first content paragraph should not lead with client brand
  const paragraphs = body
    .split(/\n\s*\n/)
    .map((p) => p.trim())
    .filter(Boolean);
  // Skip salutation
  const firstContent =
    paragraphs.find((p) => !/^dear\b/i.test(p) && !/^hello\b/i.test(p)) ||
    paragraphs[0] ||
    '';

  for (const name of clientNames) {
    const re = new RegExp(`^(we\\s+at\\s+)?${escapeRe(name)}\\b`, 'i');
    if (re.test(firstContent) || /^we\s+(provide|offer|specialize|deliver)\b/i.test(firstContent)) {
      rejections.push(
        buildGateRejection({
          reason: GATE_REASONS.PROSPECT_AFTER_ANCHOR,
          evidence: `Opening leads with client/services: "${firstContent.slice(0, 80)}"`,
          regenerationRecommendation:
            'Rewrite opening to demonstrate prospect understanding before introducing Anchor',
        })
      );
      break;
    }
  }

  // Client appears before company name in body
  const company = profile && profile.company;
  if (company) {
    const companyIdx = body.toLowerCase().indexOf(String(company).toLowerCase());
    for (const name of clientNames) {
      const idx = body.toLowerCase().indexOf(String(name).toLowerCase());
      if (idx >= 0 && (companyIdx < 0 || idx < companyIdx)) {
        // Only fail if Anchor appears in first paragraph region
        const firstChunk = body.slice(0, Math.min(body.length, 280));
        if (firstChunk.toLowerCase().includes(String(name).toLowerCase())) {
          rejections.push(
            buildGateRejection({
              reason: GATE_REASONS.PROSPECT_AFTER_ANCHOR,
              evidence: `"${name}" appears before prospect company in opening region`,
              regenerationRecommendation: 'Open with prospect business context first',
            })
          );
          break;
        }
      }
    }
  }

  const verified = ((profile && profile.personalization_claims) || []).filter(
    (c) => c.verified
  );
  if (verified.length) {
    const hasClaim = verified.some((c) => {
      const token = String(c.claim).split(/\s+/).slice(0, 4).join(' ');
      return token && body.toLowerCase().includes(token.toLowerCase().slice(0, 24));
    });
    const hasCompany =
      company && body.toLowerCase().includes(String(company).toLowerCase());
    const hasIndustry =
      profile.industry &&
      body.toLowerCase().includes(String(profile.industry).toLowerCase());
    if (!hasClaim && !hasCompany && !hasIndustry) {
      rejections.push(
        buildGateRejection({
          reason: GATE_REASONS.UNSUPPORTED_PERSONALIZATION,
          evidence: 'Letter lacks verified company-specific observation',
          regenerationRecommendation:
            'Include at least one verified personalization claim from the Sales Intelligence Profile',
        })
      );
    }
  }

  // Repeated phrase heuristic
  const sentences = body.split(/[.!?]\s+/).map((s) => s.trim().toLowerCase()).filter((s) => s.length > 20);
  const seen = new Set();
  for (const s of sentences) {
    const key = s.slice(0, 40);
    if (seen.has(key)) {
      rejections.push(
        buildGateRejection({
          reason: GATE_REASONS.REPEATED_PHRASES,
          evidence: `Repeated phrasing: "${s.slice(0, 60)}"`,
          regenerationRecommendation: 'Regenerate with varied natural language',
        })
      );
      break;
    }
    seen.add(key);
  }

  if (profile && profile.sendable === false) {
    rejections.push(
      buildGateRejection({
        reason: GATE_REASONS.LOW_REASONING_CONFIDENCE,
        evidence: 'Underlying Sales Intelligence Profile is marked non-sendable',
        regenerationRecommendation: 'Resolve profile gate rejections before generating copy',
      })
    );
  }

  return rejections;
}

function escapeRe(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

module.exports = {
  applyProfileGates,
  gateOutreachCopy,
};
