'use strict';

// Pure extraction from the existing bounded business crawl. Business category
// evidence is never evidence of a founder, an address, or deliverability.
const { parseDocument } = require('htmlparser2');
const { normalizeDomain } = require('./canonicalEmailEligibility');
const { missionCompatibleVerticals, resolveReplenishmentVertical } = require('./replenishmentVertical');

const LIMITS = Object.freeze({ pageChars: 1_000_000, nodes: 20_000, depth: 40, fieldChars: 4096, statements: 40 });
const EXCLUDED_TAGS = new Set(['script', 'style', 'template', 'noscript', 'nav', 'footer', 'aside', 'article', 'blockquote', 'q', 'cite', 'form', 'iframe', 'svg']);
const EXCLUDED_CONTEXT = /(?:^|[-_\s])(?:testimonials?|reviews?|customers?|clients?|portfolio|case[-_ ]?stud(?:y|ies)|partners?|blog|news|quotes?|comments?|hidden|d-none|sr-only|visually-hidden)(?:$|[-_\s])/i;
const THIRD_PARTY_HEADING = /\b(?:testimonials?|reviews?|customers?|clients?|portfolio|case stud(?:y|ies)|partners?|blog|news)\b/i;
const NON_OPERATOR = /\b(?:software|insurance|training|courses?|consultants?|consulting|consultancy|marketing|web(?:site)? design|SEO|lead generation|jobs?|recruit(?:ing|ment)|suppliers?|supplies|distributors?|manufacturers?|manufacturing|directory|directories|marketplace|franchise opportunities|equipment rental|roofing|junk removal|waste hauling|restoration company|water damage restoration)\b/i;
const NEGATED = /\b(?:not|never|no longer|former|formerly|previously|used to)\b/i;
const MODIFIERS = '(?:(?:residential|commercial|professional|local|independent|family[- ]owned|owner[- ]operated|full[- ]service|licensed)\\s+)*';
const OPERATORS = [
  ['cleaning', '(?:cleaning|janitorial) (?:services?|compan(?:y|ies)|business(?:es)?)'],
  ['landscaping', '(?:landscap(?:ing|e) (?:compan(?:y|ies)|services?|contractors?)|lawn care(?: services?| company)?)'],
  ['painting', '(?:(?:house|interior|exterior) )?painting (?:services?|contractors?|compan(?:y|ies))'],
  ['hvac', '(?:HVAC|heating (?:and|&) cooling|air conditioning) (?:contractors?|compan(?:y|ies)|services?)'],
  ['electrician', '(?:electrical contractors?|electricians?(?: company| services?)?)'],
  ['home_services', '(?:home repairs?(?: services?| company)?|handyman(?: services?| company)?)'],
  ['restaurant', '(?:restaurants?|catering (?:company|services?))'],
  ['salon', '(?:(?:hair|beauty) salons?|barber shops?|day spas?)'],
  ['fitness', '(?:gyms?|fitness (?:studios?|centers?)|personal training (?:studio|services?))'],
  ['auto', '(?:(?:auto|automotive) (?:repair|service) (?:shops?|compan(?:y|ies))|mechanic shops?)'],
].map(([vertical, phrase]) => [vertical, new RegExp(`^${MODIFIERS}(?:${phrase})\\b`, 'i')]);

function excluded(node) {
  const a = node.attribs || {};
  return EXCLUDED_TAGS.has(node.name) || Object.hasOwn(a, 'hidden')
    || String(a['aria-hidden']).toLowerCase() === 'true'
    || /display\s*:\s*none|visibility\s*:\s*hidden/i.test(a.style || '')
    || /(?:schema\.org\/)?(?:Review|Comment|Quotation|Article|BlogPosting)(?:$|\s)/i.test(a.itemtype || '')
    || EXCLUDED_CONTEXT.test(`${a.class || ''} ${a.id || ''}`);
}

function inlineText(node, depth = 0) {
  if (depth > LIMITS.depth || excluded(node)) return null;
  if (node.type === 'text') return node.data;
  if (node.type === 'comment') return null;
  // Do not concatenate disjoint paragraphs/cards into a self-declaration.
  if (depth && /^(?:div|section|p|h[1-6]|ul|ol|li|table|br|hr)$/.test(node.name)) return null;
  const pieces = (node.children || []).map(child => inlineText(child, depth + 1));
  return pieces.includes(null) ? null : pieces.join(' ');
}

function pageStatements(page) {
  if (typeof page.text !== 'string' || page.text.length > LIMITS.pageChars) return null;
  const root = parseDocument(page.text);
  const stack = [{ node: root, depth: 0, blocked: false, sectionBlocked: false }];
  const statements = [];
  let count = 0;
  while (stack.length) {
    const { node, depth, blocked, sectionBlocked } = stack.pop();
    if (++count > LIMITS.nodes || depth > LIMITS.depth) return null;
    const unavailable = blocked || sectionBlocked || excluded(node);
    if (!unavailable && /^(?:h[12]|p)$/.test(node.name)) {
      const text = inlineText(node)?.replace(/\s+/g, ' ').trim();
      if (text?.length > LIMITS.fieldChars) return null;
      if (text) {
        for (const statement of text.split(/(?<=[.!?])\s+/)) {
          if (statement.length > 400) continue; // Never truncate into a qualifying statement.
          const self = statement.match(/^we (?:are|operate|run|provide|offer|specialize in)\s+(?:(?:an?|the)\s+)?(.+)$/i);
          const heading = /^h[12]$/.test(node.name);
          if (!self && !heading) continue;
          statements.push({ text: statement, subject: self ? self[1] : statement, self: Boolean(self), sourceUrl: page.url });
          if (statements.length > LIMITS.statements) return null;
        }
      }
    }
    // Unmarked testimonial/portfolio sections still have semantic headings.
    // Carry that context through sibling cards, until the next sibling heading;
    // nested headings cannot clear a third-party ancestor's context.
    let siblingSectionBlocked = false;
    let siblingSectionLevel = 7;
    const children = [];
    for (const child of node.children || []) {
      if (/^h[1-6]$/.test(child.name) && !excluded(child)) {
        const level = Number(child.name[1]);
        if (!siblingSectionBlocked || level <= siblingSectionLevel) {
          siblingSectionBlocked = THIRD_PARTY_HEADING.test(inlineText(child) || '');
          siblingSectionLevel = level;
        }
      }
      children.push({ node: child, depth: depth + 1, blocked: blocked || excluded(node),
        sectionBlocked: sectionBlocked || siblingSectionBlocked });
    }
    for (const child of children.reverse()) stack.push(child);
  }
  return statements;
}

function extractSmallBusinessEvidence(pages, domain, context, candidate = {}) {
  if (!Array.isArray(pages) || pages.length > 3) return null;
  // New website facts supplement the observed company record. They must not
  // erase prior supplier/non-operator evidence just by replacing description.
  for (const record of [candidate, ...(Array.isArray(candidate.classificationObservations) ? candidate.classificationObservations : [])]) {
    if (!record || typeof record !== 'object') continue;
    const descriptive = [record.name, record.company, record.description, record.businessType, record.snippet,
      ...(Array.isArray(record.signals) ? record.signals.map(value => value?.label || value?.text || value?.type) : [])];
    if (descriptive.some(value => typeof value === 'string' && (NON_OPERATOR.test(value) || NEGATED.test(value)))) return null;
  }
  const allowed = missionCompatibleVerticals(context);
  const observations = [];
  const seen = new Set();
  for (const page of pages) {
    if (typeof page?.url !== 'string' || page.url.length > 2048 || !/^https?:\/\//i.test(page.url)
      || normalizeDomain(page.url) !== domain) return null;
    const statements = pageStatements(page);
    if (!statements) return null;
    for (const statement of statements) {
      const operator = OPERATORS.find(([, pattern]) => pattern.test(statement.subject));
      const vertical = operator?.[0];
      // A later self-description as a supplier/vendor cannot be discarded after
      // a favorable heading on the first page. Keep contradictory batches held.
      if (NON_OPERATOR.test(statement.text) || NEGATED.test(statement.text)) return null;
      const otherVertical = resolveReplenishmentVertical({ description: statement.text }, context);
      if (otherVertical && !allowed.includes(otherVertical)) return null;
      if (!vertical) continue;
      if (!statement.self) {
        const suffix = statement.subject.slice(operator[1].exec(statement.subject)[0].length).replace(/[.!?]$/, '').trim();
        if (suffix && !/^(?:in|near|serving|across|throughout|based in)\s+[\p{L}\p{M} ,.'-]+$/u.test(suffix)) continue;
      }
      if (!allowed.includes(vertical)) return null;
      const key = `${vertical}:${statement.text}`;
      if (!seen.has(key)) {
        seen.add(key);
        observations.push({ vertical, quote: statement.text, source_url: statement.sourceUrl });
        if (observations.length > LIMITS.statements) return null;
      }
    }
  }
  return observations.length ? observations : null;
}

module.exports = { extractSmallBusinessEvidence };
