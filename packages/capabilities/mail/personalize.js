'use strict';

/**
 * Letter personalization for mail packages (SPEC-033 / ADR-014 / ADR-015 / SPEC-048).
 * Prefer Sales Intelligence messaging strategy over playbook openers (ADR-032).
 */

const { buildOpener } = require('../playbook/apply');
const { buildClientPlaybook, brandVoiceLabel } = require('../playbook/types');
const {
  buildLetter,
  buildEnvelope,
  buildPersonalizationSummary,
  buildInsertItem,
  DEFAULT_INSERT_CHECKLIST,
  DEFAULT_CONFIDENCE_THRESHOLD,
  PACKAGE_STATUS,
} = require('./types');
const {
  validateProspectForMail,
  resolveMailingAddress,
  resolveRecipient,
  resolveCompanyName,
} = require('./validate');
const {
  openingFromProfile,
} = require('../salesIntelligence/derive');
const { gateOutreachCopy } = require('../salesIntelligence/gates');
const { evaluateHumanTest } = require('../salesIntelligence/humanTest');

/**
 * @param {object} prospect
 * @param {object} [ctx]
 * @returns {object} MailPackage fields (without id/status finalization)
 */
function composeMailPackage(prospect, ctx = {}) {
  const playbook = ctx.playbook ? buildClientPlaybook(ctx.playbook) : null;
  const strategy = ctx.campaignStrategy || ctx.strategy || null;
  const mailMerge = ctx.mailMergeRow || null;
  const brief = prospect.opportunityBrief || ctx.opportunityBrief || null;
  const intel = prospect.companyIntelligence || ctx.companyIntelligence || null;
  const salesProfile = resolveSalesProfile(prospect, ctx, mailMerge);

  const companyName = resolveCompanyName(prospect);
  const recipient = resolveRecipient({
    ...prospect,
    ...(ctx.recipientOverride ? { recipientName: ctx.recipientOverride } : {}),
    companyIntelligence: intel,
  });
  const mailingAddress = ctx.addressInvalid
    ? ''
    : resolveMailingAddress({ ...prospect, companyIntelligence: intel });

  const opener = playbook
    ? buildOpener(prospect, playbook)
    : {
        sentence:
          (mailMerge && mailMerge.personalizationSentence) ||
          (companyName
            ? `Reached out because ${companyName} looks like a strong fit for reliable commercial service.`
            : 'Reached out to introduce our commercial service.'),
        hook:
          (mailMerge && mailMerge.openingHook) ||
          'Would a brief walkthrough conversation be useful?',
      };

  // SPEC-048: prefer Sales Intelligence for opening / CTA / positioning
  const profileOpening = salesProfile
    ? openingFromProfile(salesProfile)
    : null;
  const personalizedOpening =
    profileOpening ||
    (mailMerge && mailMerge.personalizationSentence) ||
    opener.sentence;

  const valueProposition =
    (salesProfile &&
      salesProfile.messaging_strategy &&
      salesProfile.messaging_strategy.positioning) ||
    (salesProfile && salesProfile.recommended_angle) ||
    (mailMerge && mailMerge.recommendedOffer && playbook
      ? playbook.valuePropositions[0]
      : null) ||
    (playbook && playbook.valuePropositions[0]) ||
    (strategy &&
      Array.isArray(strategy.valuePropositions) &&
      strategy.valuePropositions[0]) ||
    'Reliable, owner-attentive commercial service';

  const offer =
    (salesProfile && salesProfile.call_to_action) ||
    (mailMerge && mailMerge.recommendedOffer) ||
    (playbook && playbook.offers[0]) ||
    (strategy && Array.isArray(strategy.offers) && strategy.offers[0]) ||
    'a brief conversation';

  const cta =
    salesProfile && salesProfile.call_to_action
      ? String(salesProfile.call_to_action)
      : typeof offer === 'string'
        ? /offer|walkthrough|schedule/i.test(offer)
          ? offer
          : `If helpful, we would be glad to schedule ${offer.toLowerCase()}.`
        : 'If helpful, we would be glad to schedule a brief conversation.';

  const clientName =
    (playbook && playbook.name && playbook.name.split('—')[0].trim()) ||
    (ctx.clientName != null ? String(ctx.clientName) : null) ||
    'Our team';

  const signature =
    ctx.signature ||
    `${clientName}\n${
      playbook && playbook.idealCustomer && playbook.idealCustomer.geographicCoverage
        ? playbook.idealCustomer.geographicCoverage
        : ''
    }`.trim();

  const hook =
    (salesProfile &&
      salesProfile.messaging_strategy &&
      salesProfile.messaging_strategy.cta) ||
    (mailMerge && mailMerge.openingHook) ||
    opener.hook;

  // Prospect-first body: understanding before Anchor / client services
  const body = [
    `Dear ${recipient.name},`,
    '',
    personalizedOpening,
    '',
    buildMidParagraph(salesProfile, companyName, valueProposition, clientName),
    '',
    cta,
    '',
    hook,
    '',
    'Sincerely,',
    signature,
  ].join('\n');

  const letter = buildLetter({
    recipientName: recipient.name,
    companyName,
    personalizedOpening,
    valueProposition: String(valueProposition),
    cta,
    signature,
    body,
  });

  const returnAddress = resolveReturnAddress(ctx, playbook);
  const envelope = buildEnvelope({
    recipientName: recipient.name,
    companyName,
    mailingAddress,
    returnAddress,
  });

  const facts = collectPersonalizationFacts(prospect, {
    playbook,
    mailMerge,
    brief,
    intel,
    opener: { sentence: personalizedOpening, hook },
    valueProposition,
    salesProfile,
  });

  const whySelected =
    (salesProfile && salesProfile.recommended_angle) ||
    (brief && (brief.whyFit || brief.whySelected)) ||
    (prospect.whyFit != null ? String(prospect.whyFit) : null) ||
    (prospect.rankingReason != null ? String(prospect.rankingReason) : null) ||
    buildWhySelected(prospect, playbook, facts);

  let confidence = scoreLetterConfidence({
    prospect,
    letter,
    facts,
    playbook,
    mailingAddress,
    recipient,
    mailMerge,
    brief,
    salesProfile,
  });

  const copyGates = gateOutreachCopy(letter.body, salesProfile || {
    company: companyName,
    industry: prospect.industry,
    personalization_claims: [],
    sendable: true,
  }, {
    clientNames: [clientName, 'Anchor', 'AS Cleaning'].filter(Boolean),
  });

  const operatorConfidence = evaluateHumanTest({
    profile: salesProfile || {
      company: companyName,
      industry: prospect.industry || '',
      decision_maker: recipient.name,
      personalization_claims: (facts || []).map((f) => ({
        claim: f,
        evidenceRef: 'mail_fact',
        verified: true,
      })),
      sendable: copyGates.length === 0,
      recommended_angle: String(valueProposition),
      call_to_action: cta,
    },
    letterBody: letter.body,
  });

  if (copyGates.length) {
    confidence = Math.min(confidence, 0.4);
  }
  if (salesProfile && salesProfile.sendable === false) {
    confidence = Math.min(confidence, 0.35);
  }

  const validation = validateProspectForMail(prospect, {
    confidence,
    confidenceThreshold: ctx.confidenceThreshold || DEFAULT_CONFIDENCE_THRESHOLD,
    letterText: letter.body,
    skipped: ctx.skipped || prospect.skipped,
    addressInvalid: ctx.addressInvalid || prospect.addressInvalid,
  });

  const missingDataWarnings = [
    ...validation.reasons.map(reasonToWarning),
    ...validation.warnings,
    ...copyGates.map(
      (g) => `Sales Intelligence gate: ${g.reason} — ${g.evidence}`
    ),
  ];

  if (salesProfile && salesProfile.sendable === false) {
    missingDataWarnings.push(
      'Sales Intelligence Profile is non-sendable — resolve reasoning gates before Ready to Print'
    );
  }

  const personalizationSummary = buildPersonalizationSummary({
    whySelected,
    personalizationFacts: facts,
    letterConfidence: confidence,
    missingDataWarnings,
  });

  const insertChecklist = resolveInsertChecklist(ctx, playbook).map((item) =>
    buildInsertItem(item)
  );

  let status = validation.status;
  if (
    copyGates.length ||
    (salesProfile && salesProfile.sendable === false) ||
    (operatorConfidence && operatorConfidence.editInstinct)
  ) {
    status = PACKAGE_STATUS.NEEDS_REVIEW;
  }

  return {
    prospectId: prospect.id != null ? String(prospect.id) : null,
    status,
    letter,
    envelope,
    personalizationSummary,
    insertChecklist,
    confidence,
    warnings: missingDataWarnings,
    skipped: Boolean(ctx.skipped || prospect.skipped),
    addressInvalid: Boolean(ctx.addressInvalid || prospect.addressInvalid),
    usedCompanyFallback: recipient.usedCompanyFallback,
    salesIntelligence: salesProfile || null,
    businessIntelligence:
      prospect.businessIntelligenceProfile ||
      prospect.businessIntelligence ||
      ctx.businessIntelligence ||
      null,
    messagingStrategy:
      (salesProfile && salesProfile.messaging_strategy) || null,
    operatorConfidence,
    qualityGateRejections: copyGates,
  };
}

/**
 * Mid paragraph: establish relevance, then introduce client strengths.
 * @param {object|null} salesProfile
 * @param {string} companyName
 * @param {string} valueProposition
 * @param {string} clientName
 */
function buildMidParagraph(salesProfile, companyName, valueProposition, clientName) {
  if (salesProfile && salesProfile.messaging_strategy) {
    const ms = salesProfile.messaging_strategy;
    const proof = (ms.social_proof || []).slice(0, 2).join(', ');
    const positioning = ms.positioning || valueProposition;
    if (proof) {
      return `That is where ${clientName} helps — ${String(positioning).replace(/\.$/, '')}, grounded in ${proof.toLowerCase()}.`;
    }
    return `That is where ${clientName} helps — ${String(positioning).replace(/\.$/, '')}.`;
  }
  return `We focus on ${String(valueProposition).toLowerCase()} for teams like ${companyName || 'yours'}.`;
}

/**
 * @param {object} prospect
 * @param {object} ctx
 * @param {object|null} mailMerge
 * @returns {object|null}
 */
function resolveSalesProfile(prospect, ctx, mailMerge) {
  if (prospect.salesIntelligenceProfile) return prospect.salesIntelligenceProfile;
  if (ctx.salesIntelligenceProfile) return ctx.salesIntelligenceProfile;
  if (mailMerge && mailMerge.salesIntelligence) return mailMerge.salesIntelligence;
  const map = ctx.salesIntelligenceByProspectId || {};
  if (prospect.id != null && map[String(prospect.id)]) {
    return map[String(prospect.id)];
  }
  const company = String(prospect.companyName || '').toLowerCase();
  if (company && map[`company:${company}`]) return map[`company:${company}`];
  const list =
    ctx.salesIntelligenceProfiles ||
    (ctx.priorOutputs && ctx.priorOutputs.salesIntelligenceProfiles) ||
    [];
  if (Array.isArray(list) && list.length) {
    return (
      list.find(
        (p) =>
          (prospect.id != null && String(p.prospectId) === String(prospect.id)) ||
          String(p.company || '').toLowerCase() === company
      ) || null
    );
  }
  return null;
}

/**
 * @param {object} ctx
 * @param {object|null} playbook
 * @returns {string}
 */
function resolveReturnAddress(ctx, playbook) {
  if (ctx.returnAddress) return String(ctx.returnAddress).trim();
  if (playbook && playbook.returnAddress) return String(playbook.returnAddress).trim();
  const name =
    (playbook && playbook.name && playbook.name.split('—')[0].trim()) ||
    ctx.clientName ||
    'Pulseforge Client';
  const geo =
    (playbook &&
      playbook.idealCustomer &&
      playbook.idealCustomer.geographicCoverage) ||
    '';
  return [name, geo].filter(Boolean).join('\n');
}

/**
 * @param {object} ctx
 * @param {object|null} playbook
 * @returns {object[]}
 */
function resolveInsertChecklist(ctx, playbook) {
  if (Array.isArray(ctx.insertChecklist) && ctx.insertChecklist.length) {
    return ctx.insertChecklist;
  }
  if (playbook && Array.isArray(playbook.insertChecklist) && playbook.insertChecklist.length) {
    return playbook.insertChecklist;
  }
  return DEFAULT_INSERT_CHECKLIST.map((i) => ({ ...i, included: true }));
}

/**
 * @param {object} prospect
 * @param {object} ctx
 * @returns {string[]}
 */
function collectPersonalizationFacts(prospect, ctx) {
  const facts = [];
  if (prospect.industry) facts.push(`Industry: ${prospect.industry}`);
  if (prospect.address || prospect.mailingAddress) {
    facts.push(`Address: ${prospect.mailingAddress || prospect.address}`);
  }
  if (ctx.playbook) {
    facts.push(`Playbook voice: ${brandVoiceLabel(ctx.playbook.brandVoice)}`);
    if (ctx.valueProposition) facts.push(`Value prop: ${ctx.valueProposition}`);
  }
  if (ctx.salesProfile) {
    facts.push(`Sales angle: ${ctx.salesProfile.recommended_angle || ''}`);
    facts.push(`Buyer: ${ctx.salesProfile.decision_maker || ''}`);
    for (const c of (ctx.salesProfile.personalization_claims || []).slice(0, 3)) {
      if (c.verified) facts.push(`Claim: ${c.claim}`);
    }
  }
  if (ctx.mailMerge) {
    if (ctx.mailMerge.messagingPosture) {
      facts.push(`Messaging posture: ${ctx.mailMerge.messagingPosture}`);
    }
    if (Array.isArray(ctx.mailMerge.activeSignalTitles)) {
      for (const t of ctx.mailMerge.activeSignalTitles.slice(0, 3)) {
        facts.push(`Signal: ${t}`);
      }
    }
  }
  if (ctx.brief) {
    if (ctx.brief.bestOutreachAngle) {
      facts.push(`Outreach angle: ${ctx.brief.bestOutreachAngle}`);
    }
    if (Array.isArray(ctx.brief.talkingPoints)) {
      for (const t of ctx.brief.talkingPoints.slice(0, 2)) {
        facts.push(`Talking point: ${t}`);
      }
    }
  }
  if (ctx.intel && Array.isArray(ctx.intel.personalization)) {
    for (const p of ctx.intel.personalization.slice(0, 3)) {
      facts.push(typeof p === 'string' ? p : JSON.stringify(p));
    }
  }
  if (ctx.opener && ctx.opener.sentence) {
    facts.push(`Opener: ${ctx.opener.sentence}`);
  }
  return facts;
}

/**
 * @param {object} prospect
 * @param {object|null} playbook
 * @param {string[]} facts
 * @returns {string}
 */
function buildWhySelected(prospect, playbook, facts) {
  const parts = [];
  if (prospect.overallScore != null) {
    parts.push(`Ranked opportunity (score ${Number(prospect.overallScore).toFixed(2)})`);
  }
  if (prospect.industry) parts.push(`fits ${prospect.industry}`);
  if (playbook && playbook.targetMarkets && playbook.targetMarkets[0]) {
    parts.push(`aligned to ${playbook.targetMarkets[0]} beachhead`);
  }
  if (!parts.length && facts.length) {
    return `Selected from approved campaign list using ${facts.length} personalization fact(s).`;
  }
  if (!parts.length) return 'Selected from approved campaign prospect list.';
  return parts.join(' · ');
}

/**
 * @param {object} args
 * @returns {number}
 */
function scoreLetterConfidence(args) {
  let score = 0.35;
  if (args.mailingAddress) score += 0.15;
  if (args.recipient && args.recipient.name) score += 0.1;
  if (args.recipient && !args.recipient.usedCompanyFallback) score += 0.08;
  if (args.playbook) score += 0.1;
  if (args.salesProfile && args.salesProfile.sendable) score += 0.12;
  if (args.salesProfile && args.salesProfile.confidenceScore) {
    score += Math.min(0.1, Number(args.salesProfile.confidenceScore) * 0.1);
  }
  if (args.mailMerge && args.mailMerge.personalizationSentence) score += 0.06;
  if (args.brief && (args.brief.whyFit || args.brief.talkingPoints)) score += 0.06;
  if (args.facts && args.facts.length >= 3) score += 0.08;
  if (args.facts && args.facts.length >= 5) score += 0.04;
  if (args.letter && args.letter.companyName && args.letter.body.includes(args.letter.companyName)) {
    score += 0.05;
  }
  const base =
    Number.isFinite(Number(args.prospect && args.prospect.confidence))
      ? Number(args.prospect.confidence) * 0.15
      : 0;
  score += base;
  return Math.max(0, Math.min(1, Number(score.toFixed(3))));
}

/**
 * @param {string} reason
 * @returns {string}
 */
function reasonToWarning(reason) {
  switch (reason) {
    case 'missing_mailing_address':
      return 'Missing mailing address — blocks Ready to Print';
    case 'missing_company_name':
      return 'Missing company name — blocks Ready to Print';
    case 'missing_recipient':
      return 'Missing recipient (and no company fallback) — blocks Ready to Print';
    case 'personalization_confidence_below_threshold':
      return 'Personalization confidence below threshold — blocks Ready to Print';
    case 'address_marked_invalid':
      return 'Address marked invalid by operator — blocks Ready to Print';
    case 'placeholder_text':
      return 'Letter contains placeholder text — blocks Ready to Print';
    case 'skipped':
      return 'Prospect skipped';
    default:
      return reason;
  }
}

module.exports = {
  composeMailPackage,
  resolveReturnAddress,
  resolveInsertChecklist,
  collectPersonalizationFacts,
  scoreLetterConfidence,
  resolveSalesProfile,
  buildMidParagraph,
};
