'use strict';

/**
 * SPEC-065 — Deterministic market email evidence extraction.
 * Quote-backed only. Omit fields when no verbatim evidence exists.
 * No scoring, paraphrasing, or recommendations.
 */

const EXTRACTOR = 'deterministic_v1';

const ROLE_PATTERNS = [
  { re: /\b(founder|co-?founder)\b/i, value: 'founder' },
  { re: /\b(ceo|chief executive)\b/i, value: 'ceo' },
  { re: /\b(sdr|sales development)\b/i, value: 'sdr' },
  { re: /\b(ae|account executive)\b/i, value: 'account_executive' },
  { re: /\b(growth|demand gen)\b/i, value: 'growth' },
  { re: /\b(marketing|demand generation)\b/i, value: 'marketing' },
  { re: /\b(customer success|csm)\b/i, value: 'customer_success' },
  { re: /\b(owner|proprietor)\b/i, value: 'owner' },
];

const CTA_PHRASES = [
  /book\s+(a\s+)?demo/i,
  /schedule\s+(a\s+)?(call|demo|meeting)/i,
  /get\s+started/i,
  /start\s+(your\s+)?free\s+trial/i,
  /try\s+(it\s+)?free/i,
  /request\s+(a\s+)?demo/i,
  /talk\s+to\s+(sales|us)/i,
  /claim\s+(your\s+)?offer/i,
  /sign\s+up/i,
  /learn\s+more/i,
];

const OFFER_PATTERNS = [
  { re: /\b(\d+%?\s+off)\b/i, field: 'offer' },
  { re: /\b(free\s+trial)\b/i, field: 'offer' },
  { re: /\b(free\s+for\s+\d+\s+days)\b/i, field: 'offer' },
  { re: /\b(limited[- ]time\s+offer)\b/i, field: 'offer' },
];

const GUARANTEE_PATTERNS = [
  /\bmoney[- ]back\s+guarantee\b/i,
  /\b(\d+[-\s]?day)\s+guarantee\b/i,
  /\bsatisfaction\s+guarantee\b/i,
  /\bno[- ]risk\s+guarantee\b/i,
];

const PRICING_PATTERNS = [
  /\$\s?\d[\d,]*(?:\.\d{2})?(?:\s*\/\s*(?:mo|month|yr|year|seat))?/i,
  /\bfrom\s+\$\s?\d[\d,]*/i,
  /\bpricing\s+starts\b/i,
  /\bper\s+(?:month|seat|user)\b/i,
];

const URGENCY_PATTERNS = [
  /\bonly\s+\d+\s+(?:spots|seats|left)\b/i,
  /\bexpires?\s+(?:today|tonight|tomorrow|soon)\b/i,
  /\blast\s+chance\b/i,
  /\blimited\s+time\b/i,
  /\bact\s+now\b/i,
  /\bending\s+soon\b/i,
];

const SOCIAL_PROOF_PATTERNS = [
  /\b(\d[\d,]*)\+?\s+(?:customers|companies|teams|users)\b/i,
  /\bas\s+seen\s+(?:in|on)\b/i,
  /\btrusted\s+by\b/i,
  /\bcase\s+study\b/i,
  /\btestimonial\b/i,
  /\bG2\b/,
  /\bCapterra\b/,
];

const POSITIONING_PATTERNS = [
  { re: /\b(AI\s+SDR|AI[- ]powered\s+outreach)\b/i, value: 'ai_sdr' },
  { re: /\b(outbound\s+automation)\b/i, value: 'outbound_automation' },
  { re: /\b(sales\s+engagement)\b/i, value: 'sales_engagement' },
  { re: /\b(lead\s+generation|leadgen)\b/i, value: 'lead_generation' },
  { re: /\b(email\s+sequenc(?:e|ing))\b/i, value: 'email_sequencing' },
  { re: /\b(all[- ]in[- ]one\s+platform)\b/i, value: 'all_in_one' },
];

const FOUNDER_SIGNALS = [
  /\bi\s+(?:built|founded|started)\b/i,
  /\bas\s+(?:a\s+)?founder\b/i,
  /\bmy\s+(?:team|cofounder)\b/i,
];

const CORPORATE_SIGNALS = [
  /\bour\s+enterprise\b/i,
  /\bglobal\s+teams?\b/i,
  /\bfortune\s+500\b/i,
  /\bcompliance\s+(?:and|&)\s+security\b/i,
];

const PERSONALIZATION = [
  { re: /\blinkedin\b/i, value: 'linkedin_mention' },
  { re: /\b(?:google\s+)?reviews?\b/i, value: 'review_mention' },
  { re: /\bhir(?:e|ing|ed)\b/i, value: 'hiring_mention' },
  { re: /\b(?:series\s+[a-d]|funding|raised\s+\$)/i, value: 'funding_mention' },
  { re: /\b(?:salesforce|hubspot|outreach\.io|gong)\b/i, value: 'technology_mention' },
  { re: /\{\{[^}]+\}\}|%\w+%|\[(?:first[_ ]?name|company|name)\]/i, value: 'generic_merge_field' },
];

function obs({ category, field, valueText, evidenceQuote, evidencePath, valueJson }) {
  return {
    category,
    field,
    valueText: String(valueText || '').trim(),
    valueJson: valueJson && typeof valueJson === 'object' ? valueJson : {},
    evidenceQuote: String(evidenceQuote || '').trim(),
    evidencePath: evidencePath || 'body_text',
    extractor: EXTRACTOR,
  };
}

function clipQuote(text, max = 180) {
  const s = String(text || '').replace(/\s+/g, ' ').trim();
  if (s.length <= max) return s;
  return `${s.slice(0, max - 1)}…`;
}

function findFirstMatch(text, patterns) {
  const source = String(text || '');
  for (const pattern of patterns) {
    const re = pattern instanceof RegExp ? pattern : pattern.re;
    const match = source.match(re);
    if (match) {
      return {
        quote: clipQuote(match[0]),
        value: pattern.value || match[0],
        field: pattern.field,
      };
    }
  }
  return null;
}

function countImgTags(html) {
  const matches = String(html || '').match(/<img\b/gi);
  return matches ? matches.length : 0;
}

/**
 * @param {object} email
 * @param {object} [context]
 * @param {string} [context.companyName]
 * @param {string} [context.companyDomain]
 * @param {number} [context.sequencePosition]
 * @returns {object[]}
 */
function extractMarketEvidence(email, context = {}) {
  const subject = String(email.subject || '');
  const bodyText = String(email.bodyText || email.body_text || '');
  const bodyHtml = String(email.bodyHtml || email.body_html || '');
  const fromName = String(email.fromName || email.from_name || '');
  const fromEmail = String(email.fromEmail || email.from_email || '');
  const headers = email.headers && typeof email.headers === 'object' ? email.headers : {};
  const links = Array.isArray(email.links) ? email.links : [];
  const combined = `${subject}\n${bodyText}`;
  const out = [];

  const companyName = context.companyName || email.companyName || '';
  const companyDomain = context.companyDomain || email.companyDomain || '';

  if (companyName) {
    out.push(obs({
      category: 'identity',
      field: 'company',
      valueText: companyName,
      evidenceQuote: companyDomain || companyName,
      evidencePath: 'from',
      valueJson: companyDomain ? { domain: companyDomain } : {},
    }));
  }

  if (fromEmail) {
    out.push(obs({
      category: 'identity',
      field: 'sender',
      valueText: fromEmail,
      evidenceQuote: fromName ? `${fromName} <${fromEmail}>` : fromEmail,
      evidencePath: 'from',
    }));
  }

  const roleSource = `${fromName}\n${bodyText.slice(-600)}`;
  const roleMatch = findFirstMatch(roleSource, ROLE_PATTERNS);
  if (roleMatch) {
    out.push(obs({
      category: 'identity',
      field: 'role',
      valueText: roleMatch.value,
      evidenceQuote: roleMatch.quote,
      evidencePath: fromName && ROLE_PATTERNS.some((p) => p.re.test(fromName)) ? 'from' : 'body_text',
    }));
  }

  if (subject) {
    out.push(obs({
      category: 'campaign',
      field: 'subject_line',
      valueText: clipQuote(subject, 240),
      evidenceQuote: clipQuote(subject, 240),
      evidencePath: 'subject',
    }));
    out.push(obs({
      category: 'messaging',
      field: 'headline',
      valueText: clipQuote(subject, 240),
      evidenceQuote: clipQuote(subject, 240),
      evidencePath: 'subject',
    }));
  }

  const receivedAt = email.receivedAt || email.received_at;
  if (receivedAt) {
    const iso = new Date(receivedAt).toISOString();
    out.push(obs({
      category: 'campaign',
      field: 'date',
      valueText: iso,
      evidenceQuote: iso,
      evidencePath: 'headers',
      valueJson: { receivedAt: iso },
    }));
  }

  if (context.sequencePosition != null) {
    const pos = String(context.sequencePosition);
    out.push(obs({
      category: 'campaign',
      field: 'sequence_position',
      valueText: pos,
      evidenceQuote: `Touch ${pos}`,
      evidencePath: 'headers',
      valueJson: { touch: Number(context.sequencePosition) },
    }));
  }

  const inReplyTo = headers['In-Reply-To'] || headers['in-reply-to'] || '';
  const isReply = /^re\s*:/i.test(subject) || Boolean(inReplyTo);
  if (isReply) {
    const quote = /^re\s*:/i.test(subject) ? clipQuote(subject, 80) : clipQuote(inReplyTo, 80);
    out.push(obs({
      category: 'campaign',
      field: 'reply_indicator',
      valueText: 'reply',
      evidenceQuote: quote || 'In-Reply-To',
      evidencePath: /^re\s*:/i.test(subject) ? 'subject' : 'headers',
    }));
  }

  const ctaFromBody = findFirstMatch(combined, CTA_PHRASES);
  if (ctaFromBody) {
    out.push(obs({
      category: 'campaign',
      field: 'cta',
      valueText: ctaFromBody.quote.toLowerCase(),
      evidenceQuote: ctaFromBody.quote,
      evidencePath: subject && CTA_PHRASES.some((re) => re.test(subject)) ? 'subject' : 'body_text',
    }));
  } else if (links.length) {
    const demoLink = links.find((u) => /demo|trial|signup|get-started|book/i.test(String(u)));
    if (demoLink) {
      out.push(obs({
        category: 'campaign',
        field: 'cta',
        valueText: clipQuote(demoLink, 120),
        evidenceQuote: clipQuote(demoLink, 180),
        evidencePath: 'links',
      }));
    }
  }

  for (const pattern of OFFER_PATTERNS) {
    const match = combined.match(pattern.re);
    if (match) {
      out.push(obs({
        category: 'messaging',
        field: 'offer',
        valueText: clipQuote(match[0], 80).toLowerCase(),
        evidenceQuote: clipQuote(match[0]),
        evidencePath: 'body_text',
      }));
      break;
    }
  }

  const guarantee = findFirstMatch(combined, GUARANTEE_PATTERNS);
  if (guarantee) {
    out.push(obs({
      category: 'messaging',
      field: 'guarantee',
      valueText: guarantee.quote.toLowerCase(),
      evidenceQuote: guarantee.quote,
      evidencePath: 'body_text',
    }));
  }

  const pricing = findFirstMatch(combined, PRICING_PATTERNS);
  if (pricing) {
    out.push(obs({
      category: 'messaging',
      field: 'pricing_mention',
      valueText: pricing.quote,
      evidenceQuote: pricing.quote,
      evidencePath: 'body_text',
    }));
  }

  const urgency = findFirstMatch(combined, URGENCY_PATTERNS);
  if (urgency) {
    out.push(obs({
      category: 'messaging',
      field: 'urgency',
      valueText: urgency.quote.toLowerCase(),
      evidenceQuote: urgency.quote,
      evidencePath: 'body_text',
    }));
  }

  const social = findFirstMatch(combined, SOCIAL_PROOF_PATTERNS);
  if (social) {
    out.push(obs({
      category: 'messaging',
      field: 'social_proof',
      valueText: social.quote.toLowerCase(),
      evidenceQuote: social.quote,
      evidencePath: 'body_text',
    }));
  }

  const positioning = findFirstMatch(combined, POSITIONING_PATTERNS);
  if (positioning) {
    out.push(obs({
      category: 'messaging',
      field: 'positioning',
      valueText: positioning.value,
      evidenceQuote: positioning.quote,
      evidencePath: 'body_text',
    }));
  }

  const imgCount = countImgTags(bodyHtml);
  const textLen = bodyText.replace(/\s+/g, ' ').trim().length;
  let formatValue = 'plain_text';
  let formatQuote = clipQuote(bodyText.slice(0, 80) || subject || 'plain text');
  let formatPath = 'body_text';
  if (bodyHtml && imgCount >= 3 && textLen < 400) {
    formatValue = 'image_heavy';
    formatQuote = `<img> × ${imgCount}`;
    formatPath = 'body_html';
  } else if (bodyHtml) {
    formatValue = 'html';
    formatQuote = clipQuote(bodyHtml.replace(/<[^>]+>/g, ' ').slice(0, 80) || 'html');
    formatPath = 'body_html';
  }
  out.push(obs({
    category: 'format',
    field: 'format_style',
    valueText: formatValue,
    evidenceQuote: formatQuote,
    evidencePath: formatPath,
    valueJson: { imgCount, textLen },
  }));

  const founder = findFirstMatch(bodyText, FOUNDER_SIGNALS);
  const corporate = findFirstMatch(bodyText, CORPORATE_SIGNALS);
  if (founder && !corporate) {
    out.push(obs({
      category: 'format',
      field: 'voice_style',
      valueText: 'founder_style',
      evidenceQuote: founder.quote,
      evidencePath: 'body_text',
    }));
  } else if (corporate) {
    out.push(obs({
      category: 'format',
      field: 'voice_style',
      valueText: 'corporate_style',
      evidenceQuote: corporate.quote,
      evidencePath: 'body_text',
    }));
  }

  let personalizationHit = false;
  for (const pattern of PERSONALIZATION) {
    const match = combined.match(pattern.re);
    if (match) {
      personalizationHit = true;
      out.push(obs({
        category: 'personalization',
        field: 'signal',
        valueText: pattern.value,
        evidenceQuote: clipQuote(match[0]),
        evidencePath: 'body_text',
      }));
    }
  }
  if (!personalizationHit) {
    out.push(obs({
      category: 'personalization',
      field: 'signal',
      valueText: 'none',
      evidenceQuote: 'no personalization markers detected',
      evidencePath: 'body_text',
    }));
  }

  return out.filter((row) => row.valueText);
}

module.exports = {
  EXTRACTOR,
  extractMarketEvidence,
  clipQuote,
  findFirstMatch,
  countImgTags,
};
