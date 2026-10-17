'use strict';

// Pure, bounded extraction from the existing crawl result. This never fetches,
// verifies, generates an address, or changes the contact admission policy.
const { parseDocument } = require('htmlparser2');
const { normalizeDomain, resolveOfficialEnrichmentDomain, isAllowedObservedWebsiteEmail } = require('./canonicalEmailEligibility');
const { isGenericRoleEmail, isFounderLocalPartMatch } = require('./babrunContactClassification');

const LIMITS = Object.freeze({ pageChars: 1_000_000, recordChars: 8_192, nodes: 20_000, records: 200, jsonNodes: 2_000, depth: 40 });
const CARD_CLASSES = new Set(['person', 'person-card', 'person-profile', 'team-member', 'team-card', 'staff-member', 'staff-card', 'employee-card', 'profile-card', 'bio-card']);
const STAFF_CLASSES = new Set(['team-member', 'team-card', 'staff-member', 'staff-card', 'employee-card']);
const NAME_CLASSES = /^(?:(?:person|member|team|staff|profile|employee|owner)[-_])?name$/;
const TITLE_CLASSES = /^(?:(?:person|member|team|staff|profile|employee|owner)[-_])?(?:title|role|position|job[-_]title)$/;
const EXCLUDED_CONTEXT = /(?:^|[-_\s])(?:testimonials?|reviews?|customers?|clients?|quotes?|comments?|guests?|speakers?|authors?|partners?|alumni|former)(?:$|[-_\s])/i;
const retired = node => /(?:^|[-_\s])(?:former|alumni)(?:$|[-_\s])/i.test(`${node.attribs?.class || ''} ${node.attribs?.id || ''}`);
const clean = value => typeof value === 'string' ? value.replace(/\s+/g, ' ').trim() : '';
const tokens = value => clean(value).toLowerCase().split(/\s+/).filter(Boolean);
const unique = values => [...new Set(values.filter(Boolean))];
const hasProp = (node, prop) => tokens(node.attribs?.itemprop).includes(prop.toLowerCase());
const schemaType = (value, type) => (Array.isArray(value) ? value : [value]).some(v => typeof v === 'string' && new RegExp(`^(?:https?://schema\\.org/)?${type}$`, 'i').test(v));
const companyUrlDomain = value => typeof value === 'string' && /^https?:\/\//i.test(value) ? normalizeDomain(value) : null;

function ownerTitle(value) {
  const title = clean(value);
  const roles = title.toLowerCase().split(/\s*(?:&|\/|,|\band\b)\s*/);
  return roles.some(role => /^(?:co[- ]?)?(?:owner|founder)$/.test(role))
    && roles.every(role => /^(?:(?:co[- ]?)?(?:owner|founder)|president|principal|proprietor|ceo|operator|managing director)$/.test(role)) ? title : null;
}

function personName(value) {
  const name = clean(value);
  if (name.length > 100 || name.split(' ').length < 2 || name.split(' ').length > 5) return null;
  if (!/^[\p{L}][\p{L}\p{M} .'-]*$/u.test(name)) return null;
  if (/\b(?:unknown|owner|founder|team|admin|contact|office|sales|support|info|hello|company|staff|meet|our)\b/i.test(name)) return null;
  return name;
}

function hidden(node) {
  const attrs = node.attribs || {};
  return Object.hasOwn(attrs, 'hidden') || String(attrs['aria-hidden']).toLowerCase() === 'true'
    || /(?:display\s*:\s*none|visibility\s*:\s*hidden)/i.test(attrs.style || '')
    || tokens(attrs.class).some(value => ['hidden', 'd-none', 'sr-only', 'visually-hidden'].includes(value));
}

function excluded(node) {
  return hidden(node) || ['template', 'noscript', 'style', 'iframe', 'svg'].includes(node.name)
    || EXCLUDED_CONTEXT.test(`${node.attribs?.class || ''} ${node.attribs?.id || ''}`)
    || schemaType(node.attribs?.itemtype, '(?:Review|Comment|Quotation|Article|BlogPosting)');
}

function card(node) {
  return schemaType(node.attribs?.itemtype, 'Person')
    || tokens(node.attribs?.class).some(value => CARD_CLASSES.has(value.replace(/_/g, '-')));
}

function descendants(root) {
  const out = [], stack = [{ node: root, depth: 0 }];
  while (stack.length) {
    const { node, depth } = stack.pop();
    if (out.length >= LIMITS.nodes || depth > LIMITS.depth) return null;
    out.push(node);
    for (const child of [...(node.children || [])].reverse()) stack.push({ node: child, depth: depth + 1 });
  }
  return out;
}

function visibleText(node) {
  if (excluded(node) || ['script', 'template'].includes(node.name)) return '';
  if (node.type === 'text') return node.data;
  return (node.children || []).map(visibleText).join(' ');
}

function fieldValue(node) {
  // Microdata uses content on meta elements. Other fields use visible content.
  return clean(node.name === 'meta' ? node.attribs?.content : visibleText(node));
}

function sourceEvidence(html, node, page, type, extra = {}) {
  const start = node.startIndex, end = node.endIndex + 1;
  if (!Number.isInteger(start) || !Number.isInteger(end) || end <= start || end - start > LIMITS.recordChars) return null;
  const record = html.slice(start, end);
  // Unclosed/repaired records are not reliable boundaries for attribution.
  if (!new RegExp(`</${node.name}\\s*>$`, 'i').test(record)) return null;
  return { version: 1, type, sourceUrl: page.url, start, end, record, ...extra };
}

function candidate(name, title, email, evidence, domain) {
  name = personName(name); title = clean(title); email = clean(email).replace(/^mailto:/i, '');
  if (!name || !title || !evidence || !isAllowedObservedWebsiteEmail(email, domain)) return null;
  return { email, contact: name, title, sourceUrl: evidence.sourceUrl,
    ownerQualified: Boolean(ownerTitle(title)) && !isGenericRoleEmail(email) && isFounderLocalPartMatch(email, name),
    websiteOwnerEvidence: { ...evidence, email, contactName: name, contactTitle: title } };
}

function htmlCandidate(html, node, page, domain, conflictOnly = false) {
  const evidence = sourceEvidence(html, node, page, 'html_person_record');
  if (!evidence) return null;
  const nodes = descendants(node);
  if (!nodes || nodes.some(n => n !== node && (card(n) || Object.hasOwn(n.attribs || {}, 'itemscope')))) return null;
  // Skip whole records containing hidden or executable fragments rather than
  // allowing their fields or accidentally joining disconnected visible text.
  if (nodes.some(n => (excluded(n) && !(n === node && conflictOnly && retired(n) && !hidden(n)))
    || ['script', 'template', 'noscript'].includes(n.name))) return null;
  const affiliations = nodes.filter(n => hasProp(n, 'worksFor') || hasProp(n, 'affiliation'));
  if (affiliations.some(n => n.parent !== node || normalizeDomain(n.attribs?.href || n.attribs?.content) !== domain)) return null;
  const staff = tokens(node.attribs?.class).some(value => STAFF_CLASSES.has(value.replace(/_/g, '-')));
  // A bare Person/profile may be a guest, customer, or owner of another firm.
  // Require positive company membership or a semantically labeled staff card.
  if (!staff && !(schemaType(node.attribs?.itemtype, 'Person') && affiliations.length)) return null;
  for (let parent = node.parent; parent; parent = parent.parent) {
    if (schemaType(parent.attribs?.itemtype, '(?:Organization|LocalBusiness|Corporation|ProfessionalService)')
      && normalizeDomain(parent.attribs?.itemid) !== domain) return null;
  }
  const names = nodes.filter(n => hasProp(n, 'name') || tokens(n.attribs?.class).some(v => NAME_CLASSES.test(v)));
  const headings = nodes.filter(n => /^h[1-6]$/.test(n.name));
  const nameNodes = names.length ? names : headings;
  if (nameNodes.length !== 1 || nameNodes[0].parent !== node) return null;
  const nameValues = unique(nameNodes.map(fieldValue));
  // A second heading/name is not silently ignored, even when a preferred field exists.
  if (nameValues.length !== 1 || headings.some(n => fieldValue(n) !== nameValues[0])) return null;
  const titleNodes = nodes.filter(n => hasProp(n, 'jobTitle') || tokens(n.attribs?.class).some(v => TITLE_CLASSES.test(v)));
  if (titleNodes.length !== 1 || titleNodes[0].parent !== node) return null;
  // Never search arbitrary bio descendants for an owner keyword. Only an
  // explicitly labeled title field in this record can supply the person's role.
  const titles = unique(titleNodes.map(fieldValue));
  if (titles.length !== 1) return null;
  // Count every address in the record before domain filtering. A staff or
  // third-party address alongside the owner's is ambiguity, not removable noise.
  const emails = unique((evidence.record.match(/[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g) || []).map(e => e.toLowerCase()));
  if (emails.length !== 1) return null;
  const observed = evidence.record.match(/[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g)[0];
  const emailNodes = nodes.filter(n => hasProp(n, 'email') || /^mailto:/i.test(n.attribs?.href || ''));
  if (emailNodes.length !== 1 || emailNodes[0].parent !== node) return null;
  // Do not bind an address appearing only in a data attribute, image URL, or bio.
  const boundEmails = unique(emailNodes.map(n => /^mailto:/i.test(n.attribs?.href || '')
    ? n.attribs.href.slice(7) : fieldValue(n)).map(e => e.toLowerCase()));
  if (boundEmails.length !== 1 || boundEmails[0] !== emails[0]) return null;
  return candidate(nameValues[0], titles[0], observed, evidence, domain);
}

function jsonCandidates(html, node, page, domain) {
  const evidence = sourceEvidence(html, node, page, 'json_ld_person');
  if (!evidence) return [];
  const raw = (node.children || []).map(child => child.data || '').join('');
  let parsed;
  try {
    parsed = JSON.parse(raw);
    // JSON.parse silently keeps the last duplicate property. Reject that
    // ambiguity before treating any field as attributable evidence.
    const keys = [], token = /"(?:\\.|[^"\\])*"|[{}]/g;
    for (const match of raw.matchAll(token)) {
      if (match[0] === '{') keys.push(new Set());
      else if (match[0] === '}') keys.pop();
      else if (/^\s*:/.test(raw.slice(match.index + match[0].length))) {
        const key = JSON.parse(match[0]);
        if (keys.at(-1).has(key)) return [];
        keys.at(-1).add(key);
      }
    }
  } catch { return []; }
  const out = [], stack = [{ value: parsed, pointer: '', depth: 0 }];
  let count = 0;
  while (stack.length) {
    const { value, pointer, depth, organizationDomain = null, foreignOrganization = false, organizationRelation = false } = stack.pop();
    if (++count > LIMITS.jsonNodes || depth > LIMITS.depth) return null;
    if (!value || typeof value !== 'object') continue;
    if (schemaType(value['@type'], '(?:Review|Comment|Quotation|Article|BlogPosting)')) continue;
    if (schemaType(value['@type'], 'Person')) {
      // No cross-record @id resolution or inherited names/titles/emails.
      const affiliations = [value.worksFor, value.affiliation].filter(Boolean).flat();
      const foreign = affiliations.some(org => !org || typeof org !== 'object' || companyUrlDomain(org.url) !== domain
        || (org['@id'] != null && companyUrlDomain(org['@id']) !== domain));
      const affiliated = affiliations.length > 0 && !foreign;
      const companyPerson = affiliated || (organizationRelation && organizationDomain === domain);
      if (pointer.length > 500) return null;
      if (companyPerson && !foreign && !foreignOrganization && typeof value.email === 'string') {
        const result = candidate(value.name, value.jobTitle, value.email, { ...evidence, jsonPointer: pointer }, domain);
        if (result) out.push(result);
      }
      continue;
    }
    const organization = schemaType(value['@type'], '(?:Organization|LocalBusiness|Corporation|ProfessionalService)');
    const organizationDomains = organization ? [value.url, value['@id']].filter(v => v != null).map(companyUrlDomain) : [];
    const scopedDomain = organization ? (organizationDomains.length && organizationDomains.every(d => d === domain) ? domain : null) : organizationDomain;
    const foreignScope = foreignOrganization || (organization && organizationDomains.some(d => d !== domain));
    for (const [key, child] of Object.entries(value).reverse()) {
      if (/^(?:review|reviews|testimonial|testimonials|comment|comments)$/i.test(key)) continue;
      stack.push({ value: child, pointer: `${pointer}/${key.replace(/~/g, '~0').replace(/\//g, '~1')}`, depth: depth + 1,
        organizationDomain: scopedDomain, foreignOrganization: foreignScope,
        organizationRelation: organization ? /^(?:founder|founders|employee|employees|member|members)$/.test(key)
          : Array.isArray(value) && organizationRelation });
    }
  }
  return out;
}

function extractWebsiteOwnerCandidates(pages, companyDomain) {
  if (typeof companyDomain !== 'string') return [];
  const domain = resolveOfficialEnrichmentDomain({ domain: companyDomain });
  if (!domain) return [];
  const results = [];
  for (const page of (pages || []).slice(0, 8)) {
    if (typeof page?.url !== 'string' || !/^https?:\/\//i.test(page.url) || normalizeDomain(page.url) !== domain) continue;
    if (page.url.length > 2048) return [];
    const html = typeof page.text === 'string' ? page.text : '';
    if (html.length > LIMITS.pageChars) return [];
    if (!html) continue;
    const root = parseDocument(html, { withStartIndices: true, withEndIndices: true });
    const nodes = descendants(root);
    if (!nodes) return [];
    let records = 0;
    const blocked = new Set(), retiredContext = new Set();
    for (const node of nodes) {
      const unavailable = blocked.has(node.parent) || excluded(node);
      if (unavailable) blocked.add(node);
      const conflictOnly = retiredContext.has(node.parent) || retired(node);
      if (conflictOnly) retiredContext.add(node);
      const json = node.name === 'script' && String(node.attribs?.type).toLowerCase() === 'application/ld+json';
      if (!json && !card(node)) continue;
      if (++records > LIMITS.records || node.endIndex + 1 - node.startIndex > LIMITS.recordChars) return [];
      if (unavailable && !conflictOnly) continue;
      const found = json ? jsonCandidates(html, node, page, domain)
        : [htmlCandidate(html, node, page, domain, conflictOnly)].filter(Boolean);
      if (!found) return [];
      for (const result of found) {
        results.push(conflictOnly ? { ...result, ownerQualified: false, roleConflict: true } : result);
      }
    }
  }
  // Contradictory complete records cannot be resolved by document order.
  const namesByEmail = new Map(), emailsByName = new Map(), conflictingRoles = new Set();
  for (const item of results) {
    const email = item.email.toLowerCase(), name = item.contact.toLowerCase();
    if (!namesByEmail.has(email)) namesByEmail.set(email, new Set());
    if (!emailsByName.has(name)) emailsByName.set(name, new Set());
    namesByEmail.get(email).add(name); emailsByName.get(name).add(email);
    if (item.roleConflict || !ownerTitle(item.title)) conflictingRoles.add(`${email}:${name}`);
  }
  return results.filter(item => item.ownerQualified && !conflictingRoles.has(`${item.email.toLowerCase()}:${item.contact.toLowerCase()}`)
    && namesByEmail.get(item.email.toLowerCase()).size === 1 && emailsByName.get(item.contact.toLowerCase()).size === 1)
    .map(({ ownerQualified, ...item }) => item);
}

module.exports = { extractWebsiteOwnerCandidates, LIMITS };
