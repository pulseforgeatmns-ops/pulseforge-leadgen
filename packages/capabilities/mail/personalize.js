'use strict';

/**
 * Letter personalization for mail packages (SPEC-033 / ADR-014 / ADR-015).
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
} = require('./types');
const {
  validateProspectForMail,
  resolveMailingAddress,
  resolveRecipient,
  resolveCompanyName,
} = require('./validate');

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

  const valueProposition =
    (mailMerge && mailMerge.recommendedOffer && playbook
      ? playbook.valuePropositions[0]
      : null) ||
    (playbook && playbook.valuePropositions[0]) ||
    (strategy &&
      Array.isArray(strategy.valuePropositions) &&
      strategy.valuePropositions[0]) ||
    'Reliable, owner-attentive commercial service';

  const offer =
    (mailMerge && mailMerge.recommendedOffer) ||
    (playbook && playbook.offers[0]) ||
    (strategy && Array.isArray(strategy.offers) && strategy.offers[0]) ||
    'a brief conversation';

  const cta =
    typeof offer === 'string'
      ? `If helpful, we would be glad to schedule ${offer.toLowerCase()}.`
      : 'If helpful, we would be glad to schedule a brief conversation.';

  const clientName =
    (playbook && playbook.name && playbook.name.split('—')[0].trim()) ||
    (ctx.clientName != null ? String(ctx.clientName) : null) ||
    'Our team';

  const signature =
    ctx.signature ||
    `${clientName}\n${playbook && playbook.idealCustomer && playbook.idealCustomer.geographicCoverage
      ? playbook.idealCustomer.geographicCoverage
      : ''}`.trim();

  const personalizedOpening =
    (mailMerge && mailMerge.personalizationSentence) || opener.sentence;

  const body = [
    `Dear ${recipient.name},`,
    '',
    personalizedOpening,
    '',
    `We focus on ${String(valueProposition).toLowerCase()} for teams like ${companyName || 'yours'}.`,
    '',
    cta,
    '',
    opener.hook,
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
    opener,
    valueProposition,
  });

  const whySelected =
    (brief && (brief.whyFit || brief.whySelected)) ||
    (prospect.whyFit != null ? String(prospect.whyFit) : null) ||
    (prospect.rankingReason != null ? String(prospect.rankingReason) : null) ||
    buildWhySelected(prospect, playbook, facts);

  const confidence = scoreLetterConfidence({
    prospect,
    letter,
    facts,
    playbook,
    mailingAddress,
    recipient,
    mailMerge,
    brief,
  });

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
  ];

  const personalizationSummary = buildPersonalizationSummary({
    whySelected,
    personalizationFacts: facts,
    letterConfidence: confidence,
    missingDataWarnings,
  });

  const insertChecklist = resolveInsertChecklist(ctx, playbook).map((item) =>
    buildInsertItem(item)
  );

  return {
    prospectId: prospect.id != null ? String(prospect.id) : null,
    status: validation.status,
    letter,
    envelope,
    personalizationSummary,
    insertChecklist,
    confidence,
    warnings: missingDataWarnings,
    skipped: Boolean(ctx.skipped || prospect.skipped),
    addressInvalid: Boolean(ctx.addressInvalid || prospect.addressInvalid),
    usedCompanyFallback: recipient.usedCompanyFallback,
  };
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
  if (args.playbook) score += 0.12;
  if (args.mailMerge && args.mailMerge.personalizationSentence) score += 0.08;
  if (args.brief && (args.brief.whyFit || args.brief.talkingPoints)) score += 0.08;
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
};
