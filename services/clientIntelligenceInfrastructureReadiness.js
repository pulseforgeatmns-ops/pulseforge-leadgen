'use strict';

/**
 * SPEC-087 — Growth Infrastructure Readiness Conversation.
 *
 * Post–Blueprint-approval diagnostic: can this business capture, convert, and
 * track demand before campaigns begin? Separate from Growth Conversation
 * (market prioritization). Assessment only — no DNS/GBP/social/tracking
 * mutations and no campaign generation.
 */

const ARTIFACT_KIND = 'growth_infrastructure_readiness_report';
const REPORT_TITLE = 'Growth Infrastructure Readiness Report';
const REPORT_DISCLAIMER =
  'Assessment only — no DNS, GBP, social, or tracking changes without explicit approval. No passwords requested. No campaigns or prospect lists generated. Client-stated facts stay separate from automated observations until reviewed.';

const SECTION_TITLES = Object.freeze({
  demandCaptureRisks: 'Can prospects reach you?',
  trustDiscoverabilityGaps: 'Can prospects trust you?',
  trackingGaps: 'Can we measure what works?',
  conversionFollowUpGaps: 'Can inquiries become booked opportunities?',
  maxCanCheck: 'What Max can check automatically',
  operatorClientMustComplete: 'What the operator/client must complete',
  recommendedSetupSequence: 'Recommended setup sequence',
});

/** Binary readiness facts — avoid "partial" unless there is specific incomplete evidence. */
const BINARY_ITEM_IDS = Object.freeze(
  new Set([
    'domain_owned',
    'domain_connected',
    'ssl_active',
    'spf_dkim_dmarc',
    'branded_email',
    'dns_provider_known',
    'website_exists',
    'contact_form_works',
    'phone_email_visible',
    'analytics_pixels',
    'gbp_exists',
    'gbp_claimed',
    'gbp_photos',
    'gbp_reviews',
    'review_count',
    'facebook_present',
    'instagram_present',
    'linkedin_present',
    'google_analytics',
    'search_console',
    'call_tracking',
    'form_tracking',
    'crm_source_tracking',
    'conversion_events',
    'utm_discipline',
    'contact_forms',
    'phone_routing',
    'email_routing',
    'booking_link',
    'crm_exists',
    'logo',
  ])
);

/**
 * Practical setup blockers first — confirm ownership/connection before outreach.
 * Item ids not listed fall through by area preference.
 */
const SETUP_SEQUENCE_ITEM_PRIORITY = Object.freeze([
  'domain_owned',
  'domain_connected',
  'branded_email',
  'spf_dkim_dmarc',
  'phone_email_visible',
  'contact_forms',
  'contact_form_works',
  'clear_cta',
  'estimate_request_flow',
  'gbp_exists',
  'gbp_claimed',
  'gbp_nap',
  'gbp_photos',
  'gbp_reviews',
  'google_analytics',
  'form_tracking',
  'crm_source_tracking',
  'call_tracking',
  'search_console',
  'estimate_process',
  'proposal_template',
  'walkthrough_process',
  'follow_up_cadence',
  'crm_exists',
  'stages_defined',
  'contacts_captured',
  'missed_lead_process',
  'response_time_expectation',
]);

const ITEM_STATUSES = Object.freeze([
  'ready',
  'partial',
  'missing',
  'unknown',
  'not_applicable',
]);

const OWNERS = Object.freeze([
  'max_can_check',
  'operator_guided',
  'client_required',
]);

const PRIORITIES = Object.freeze(['high', 'medium', 'low']);

const CONVERSATION_STEPS = Object.freeze([
  'opening',
  'website_domain',
  'gbp',
  'lead_flow',
  'estimates',
  'tracking',
  'assets',
  'report',
]);

/** @typedef {{ id: string, label: string, owner: string, priority: string, defaultNextStep: string }} ReadinessItemDef */
/** @typedef {{ id: string, label: string, items: ReadinessItemDef[] }} ReadinessAreaDef */

/** @type {ReadinessAreaDef[]} */
const READINESS_AREAS = Object.freeze([
  {
    id: 'domain_dns',
    label: 'Domain and DNS',
    items: [
      item('domain_owned', 'Domain owned', 'client_required', 'high', 'Confirm domain registrar ownership.'),
      item('domain_connected', 'Domain connected to website', 'max_can_check', 'high', 'Point domain A/CNAME to the live site.'),
      item('ssl_active', 'SSL active', 'max_can_check', 'high', 'Enable HTTPS / valid certificate on the site.'),
      item('spf_dkim_dmarc', 'SPF/DKIM/DMARC present', 'operator_guided', 'high', 'Add email authentication records at the DNS provider.'),
      item('branded_email', 'Branded email available', 'client_required', 'high', 'Create a branded mailbox (e.g. hello@domain).'),
      item('dns_provider_known', 'DNS provider known', 'client_required', 'medium', 'Identify DNS host (Cloudflare, GoDaddy, etc.).'),
    ],
  },
  {
    id: 'website',
    label: 'Website',
    items: [
      item('website_exists', 'Website exists', 'max_can_check', 'high', 'Publish a live website on the domain.'),
      item('mobile_usability', 'Mobile usability', 'max_can_check', 'high', 'Fix mobile layout and tap targets.'),
      item('clear_services', 'Clear services', 'operator_guided', 'high', 'List primary services above the fold.'),
      item('clear_service_area', 'Clear service area', 'operator_guided', 'high', 'State cities/markets served.'),
      item('clear_cta', 'Clear CTA', 'operator_guided', 'high', 'Add one primary call-to-action (call / form / book).'),
      item('contact_form_works', 'Contact form works', 'operator_guided', 'high', 'Test form delivery end-to-end.'),
      item('phone_email_visible', 'Phone/email visible', 'max_can_check', 'high', 'Show phone and email in header/footer.'),
      item('trust_proof', 'Trust proof present', 'operator_guided', 'medium', 'Add reviews, licenses, or proof points.'),
      item('seo_metadata', 'Basic SEO metadata', 'max_can_check', 'medium', 'Set title, meta description, and Open Graph basics.'),
      item('analytics_pixels', 'Analytics/pixels installed', 'max_can_check', 'medium', 'Install GA4 and any ad pixels.'),
    ],
  },
  {
    id: 'gbp',
    label: 'Google Business Profile',
    items: [
      item('gbp_exists', 'GBP exists', 'max_can_check', 'high', 'Create a Google Business Profile listing.'),
      item('gbp_claimed', 'GBP claimed', 'client_required', 'high', 'Claim and verify the GBP (client/operator required).'),
      item('gbp_nap', 'Correct name/address/service area', 'client_required', 'high', 'Correct NAP and service area in GBP.'),
      item('gbp_categories', 'Correct categories', 'operator_guided', 'medium', 'Set primary/secondary categories.'),
      item('gbp_services', 'Services listed', 'operator_guided', 'medium', 'Add services with clear descriptions.'),
      item('gbp_photos', 'Photos present', 'client_required', 'medium', 'Upload exterior, team, and work photos.'),
      item('gbp_reviews', 'Reviews present', 'client_required', 'high', 'Collect initial Google reviews.'),
      item('gbp_contact', 'Website/phone connected', 'operator_guided', 'high', 'Link website and phone on GBP.'),
      item('gbp_messaging', 'Messaging/calls enabled where appropriate', 'client_required', 'low', 'Enable messaging/calls if the team can respond.'),
    ],
  },
  {
    id: 'reviews',
    label: 'Reviews and Reputation',
    items: [
      item('review_count', 'Current review count', 'max_can_check', 'medium', 'Document current Google review count.'),
      item('average_rating', 'Average rating', 'max_can_check', 'medium', 'Document average star rating.'),
      item('review_recency', 'Review recency', 'max_can_check', 'medium', 'Aim for recent reviews (last 90 days).'),
      item('review_request_process', 'Review request process', 'operator_guided', 'high', 'Define a post-job review ask process.'),
      item('review_response_process', 'Response process', 'operator_guided', 'medium', 'Respond to new reviews within a set SLA.'),
      item('testimonials_available', 'Testimonials available for website/outreach', 'client_required', 'medium', 'Collect permissioned testimonials for the site.'),
    ],
  },
  {
    id: 'social',
    label: 'Social Profiles',
    items: [
      item('facebook_present', 'Facebook present', 'max_can_check', 'medium', 'Create or claim the Facebook Page.'),
      item('instagram_present', 'Instagram present', 'max_can_check', 'medium', 'Create or claim the Instagram profile.'),
      item('linkedin_present', 'LinkedIn present if relevant', 'max_can_check', 'low', 'Create a LinkedIn Page if B2B-relevant.'),
      item('profile_branding', 'Profile branding consistent', 'operator_guided', 'medium', 'Align name, logo, and cover across profiles.'),
      item('contact_info_consistent', 'Contact info consistent', 'operator_guided', 'medium', 'Match phone/website/NAP across profiles.'),
      item('recent_activity', 'Recent activity', 'client_required', 'low', 'Post recent proof or updates.'),
      item('proof_photos', 'Useful proof/photos available', 'client_required', 'medium', 'Gather photos usable on social and the website.'),
    ],
  },
  {
    id: 'tracking',
    label: 'Tracking and Analytics',
    items: [
      item('google_analytics', 'Google Analytics', 'operator_guided', 'high', 'Install GA4 with admin access shared appropriately.'),
      item('search_console', 'Search Console', 'operator_guided', 'medium', 'Verify domain in Search Console.'),
      item('call_tracking', 'Call tracking', 'operator_guided', 'medium', 'Add call tracking or at least call logging.'),
      item('form_tracking', 'Form tracking', 'operator_guided', 'high', 'Track form submits as conversions.'),
      item('crm_source_tracking', 'CRM source tracking', 'operator_guided', 'high', 'Capture lead source on every contact.'),
      item('conversion_events', 'Conversion events', 'operator_guided', 'medium', 'Define and fire key conversion events.'),
      item('utm_discipline', 'UTM discipline', 'operator_guided', 'medium', 'Use consistent UTM parameters on campaigns.'),
    ],
  },
  {
    id: 'lead_capture',
    label: 'Lead Capture',
    items: [
      item('contact_forms', 'Contact forms', 'operator_guided', 'high', 'Ensure a working contact/estimate form.'),
      item('phone_routing', 'Phone routing', 'client_required', 'high', 'Route calls to a monitored number.'),
      item('email_routing', 'Email routing', 'client_required', 'high', 'Route inbound email to a monitored inbox.'),
      item('booking_link', 'Booking/calendar link', 'operator_guided', 'medium', 'Add a booking link if discovery calls are used.'),
      item('estimate_request_flow', 'Estimate request flow', 'operator_guided', 'high', 'Define how estimate requests are received.'),
      item('response_time_expectation', 'Response time expectation', 'operator_guided', 'high', 'Set and publish a response-time expectation.'),
      item('missed_lead_process', 'Missed lead process', 'operator_guided', 'high', 'Define what happens when a lead is missed.'),
    ],
  },
  {
    id: 'crm_pipeline',
    label: 'CRM and Pipeline',
    items: [
      item('crm_exists', 'CRM exists', 'operator_guided', 'high', 'Use a CRM (Pulseforge or other) for all leads.'),
      item('stages_defined', 'Stages defined', 'operator_guided', 'high', 'Define pipeline stages.'),
      item('contacts_captured', 'Contacts captured', 'operator_guided', 'high', 'Log every inquiry as a contact.'),
      item('opportunities_tracked', 'Opportunities tracked', 'operator_guided', 'medium', 'Track opportunities with value/stage.'),
      item('follow_up_reminders', 'Follow-up reminders', 'operator_guided', 'high', 'Use reminders for next touches.'),
      item('owner_assigned', 'Owner assigned', 'operator_guided', 'medium', 'Assign an owner to every open lead.'),
      item('lost_reasons', 'Lost reasons captured', 'operator_guided', 'low', 'Capture lost reasons for learning.'),
    ],
  },
  {
    id: 'sales_process',
    label: 'Sales Process',
    items: [
      item('estimate_process', 'Estimate process', 'client_required', 'high', 'Document how estimates are scoped.'),
      item('proposal_template', 'Proposal template', 'operator_guided', 'medium', 'Create a reusable proposal template.'),
      item('pricing_inputs', 'Pricing inputs', 'client_required', 'high', 'List pricing inputs (sq ft, frequency, etc.).'),
      item('walkthrough_process', 'Walkthrough process', 'client_required', 'high', 'Define walkthrough steps and checklist.'),
      item('qualification_questions', 'Qualification questions', 'operator_guided', 'medium', 'Write qualification questions for inbound.'),
      item('follow_up_cadence', 'Follow-up cadence', 'operator_guided', 'high', 'Set a follow-up cadence after estimates.'),
      item('close_language', 'Close/next-step language', 'operator_guided', 'medium', 'Standardize close and next-step language.'),
    ],
  },
  {
    id: 'brand_assets',
    label: 'Brand Assets',
    items: [
      item('logo', 'Logo', 'client_required', 'medium', 'Provide a usable logo file.'),
      item('colors_fonts', 'Colors/fonts', 'client_required', 'low', 'Share brand colors and fonts.'),
      item('photos', 'Photos', 'client_required', 'high', 'Provide real work/team/location photos.'),
      item('before_after', 'Before/after examples', 'client_required', 'medium', 'Collect before/after examples where relevant.'),
      item('service_descriptions', 'Service descriptions', 'operator_guided', 'medium', 'Write clear service descriptions.'),
      item('proof_points', 'Proof points', 'client_required', 'medium', 'List licenses, guarantees, and differentiators.'),
      item('differentiation', 'Differentiation', 'operator_guided', 'medium', 'State why customers choose this business.'),
      item('voice_tone', 'Voice/tone', 'operator_guided', 'low', 'Confirm voice/tone for outreach and site.'),
    ],
  },
]);

const QUESTION_BANK = Object.freeze([
  {
    id: 'website_domain',
    step: 'website_domain',
    prompt:
      'What is the website URL and domain? If there is no site yet, say so — and tell me whether the domain is owned.',
    areas: ['domain_dns', 'website'],
  },
  {
    id: 'gbp',
    step: 'gbp',
    prompt:
      'Does a Google Business Profile already exist, and is it claimed/verified? Anything you know about reviews or photos helps.',
    areas: ['gbp', 'reviews'],
  },
  {
    id: 'lead_flow',
    step: 'lead_flow',
    prompt:
      'How do leads currently arrive (phone, form, email, walk-in, referral), and where do they go after that?',
    areas: ['lead_capture', 'crm_pipeline'],
  },
  {
    id: 'estimates',
    step: 'estimates',
    prompt:
      'How are estimates or proposals handled today — walkthrough, pricing inputs, templates, and follow-up?',
    areas: ['sales_process'],
  },
  {
    id: 'tracking',
    step: 'tracking',
    prompt:
      'What tracking exists today — Google Analytics, Search Console, call/form tracking, CRM source fields, or UTMs?',
    areas: ['tracking'],
  },
  {
    id: 'assets',
    step: 'assets',
    prompt:
      'What brand assets are ready — logo, photos, before/after examples, service descriptions, proof points, or social profiles?',
    areas: ['brand_assets', 'social'],
  },
]);

function item(id, label, owner, priority, defaultNextStep) {
  return Object.freeze({ id, label, owner, priority, defaultNextStep });
}

function shortName(name) {
  const s = String(name || '').trim();
  if (!s) return 'the business';
  if (/\banchor\s+cleaning\b/i.test(s)) return 'Anchor Cleaning';
  if (/^anchor\b/i.test(s)) return 'Anchor';
  return s;
}

function extractBusinessName(blueprint) {
  const sections = (blueprint && blueprint.sections) || {};
  const identity = sections.identity && sections.identity.summary
    ? String(sections.identity.summary)
    : '';
  if (/\banchor\s+cleaning\b/i.test(identity)) return 'Anchor Cleaning';
  const named = identity.match(
    /^([A-Z][a-zA-Z0-9&'.-]+(?:\s+[A-Z][a-zA-Z0-9&'.-]+){0,5})\s+(?:is|are)\b/
  );
  if (named) return shortName(named[1]);
  return shortName(identity.split(/[.!]/)[0] || 'the business');
}

function emptyItemState(def) {
  return {
    id: def.id,
    label: def.label,
    status: 'unknown',
    evidence: '',
    owner: def.owner,
    priority: def.priority,
    recommended_next_step: def.defaultNextStep,
    source: 'unknown',
  };
}

function buildEmptyAreas() {
  const areas = {};
  for (const area of READINESS_AREAS) {
    const items = {};
    for (const def of area.items) {
      items[def.id] = emptyItemState(def);
    }
    areas[area.id] = {
      id: area.id,
      label: area.label,
      status: 'unknown',
      items,
    };
  }
  return areas;
}

function cloneAreas(areas) {
  return JSON.parse(JSON.stringify(areas || buildEmptyAreas()));
}

function setItem(areas, areaId, itemId, patch) {
  if (!areas[areaId] || !areas[areaId].items[itemId]) return;
  const current = areas[areaId].items[itemId];
  const status = patch.status && ITEM_STATUSES.includes(patch.status)
    ? patch.status
    : current.status;
  areas[areaId].items[itemId] = {
    ...current,
    ...patch,
    status,
    owner: OWNERS.includes(patch.owner) ? patch.owner : current.owner,
    priority: PRIORITIES.includes(patch.priority)
      ? patch.priority
      : current.priority,
  };
}

function areaAggregateStatus(area) {
  const statuses = Object.values(area.items || {}).map((i) => i.status);
  if (!statuses.length) return 'unknown';
  if (statuses.every((s) => s === 'ready' || s === 'not_applicable')) return 'ready';
  if (statuses.every((s) => s === 'unknown')) return 'unknown';
  if (statuses.some((s) => s === 'missing')) return 'missing';
  if (statuses.some((s) => s === 'partial' || s === 'ready')) return 'partial';
  return 'unknown';
}

function refreshAreaStatuses(areas) {
  for (const area of Object.values(areas)) {
    area.status = areaAggregateStatus(area);
  }
  return areas;
}

function extractUrl(text) {
  const m = String(text || '').match(
    /https?:\/\/[^\s]+|www\.[^\s]+|[a-z0-9][-a-z0-9]+\.(?:com|net|org|io|co|biz|us)(?:\/[^\s]*)?/i
  );
  return m ? m[0].replace(/[),.]+$/, '') : '';
}

function looksNegative(text) {
  return /\b(no|none|not yet|don't have|do not have|doesn't have|missing|n\/a|na)\b/i.test(
    text || ''
  );
}

function looksPositive(text) {
  return /\b(yes|we have|we've got|already|live|working|set up|setup|claimed|verified|active)\b/i.test(
    text || ''
  );
}

function looksUncertain(text) {
  return /\b(not sure|unsure|don't know|do not know|unknown|maybe|i think|probably|need to (check|verify)|needs? verification|haven't checked|have not checked)\b/i.test(
    text || ''
  );
}

/**
 * Map internal status → owner-friendly label for binary / common facts.
 * Keep internal status codes stable; labels are presentation only.
 */
function statusLabelForItem(itemId, status, context = {}) {
  const s = ITEM_STATUSES.includes(status) ? status : 'unknown';
  if (itemId === 'domain_owned') {
    if (s === 'ready') return 'confirmed';
    if (s === 'missing') return 'not owned';
    return 'unconfirmed';
  }
  if (itemId === 'domain_connected') {
    if (s === 'ready') return 'connected';
    if (s === 'missing') return 'not connected';
    return 'needs verification';
  }
  if (itemId === 'branded_email') {
    if (s === 'ready') return 'present';
    if (s === 'missing') {
      return context.domainOwnedReady ? 'needs setup' : 'not present';
    }
    if (s === 'partial') return 'needs setup';
    return 'unknown';
  }
  if (s === 'ready') return 'ready';
  if (s === 'missing') return 'missing';
  if (s === 'partial') return 'partial';
  if (s === 'not_applicable') return 'not applicable';
  return 'needs verification';
}

/**
 * Infer status from conversational evidence.
 * Never mark missing from uncertainty or lack of an automated check.
 */
function statusFromEvidence(text, { binary = false, allowPartial = !binary } = {}) {
  if (looksUncertain(text)) return 'unknown';
  if (looksNegative(text)) return 'missing';
  if (looksPositive(text)) return 'ready';
  if (binary || !allowPartial) return 'unknown';
  return 'partial';
}

/** Polarity for one matched term — avoids "we have GA4 but no UTMs" flipping GA to missing. */
function statusNearMatch(text, re, opts = {}) {
  const raw = String(text || '');
  const lower = raw.toLowerCase();
  const m = raw.match(re);
  if (!m) return 'unknown';
  const idx = lower.indexOf(m[0].toLowerCase());
  if (idx < 0) return 'unknown';
  const before = lower.slice(Math.max(0, idx - 28), idx);
  const around = raw.slice(Math.max(0, idx - 40), Math.min(raw.length, idx + m[0].length + 24));
  if (looksUncertain(around)) return 'unknown';
  // Negation must sit immediately before the term ("no GA4", "don't have analytics").
  if (
    /\b(no|without|not have|don't have|do not have|doesn't have)\b[\w\s'’-]{0,22}$/i.test(
      before
    )
  ) {
    return 'missing';
  }
  const after = lower.slice(idx, Math.min(lower.length, idx + m[0].length + 18));
  if (
    /\b(yes|we have|we've got|already|live|working|set up|setup|have)\b/i.test(before) ||
    /\b(ready|live|working|set up|setup|active)\b/i.test(after)
  ) {
    return 'ready';
  }
  return statusFromEvidence(around, opts);
}

function detectReportRequest(message) {
  return /\b(report|wrap up|summarize|readiness report|how ready|setup sequence|enough for now)\b/i.test(
    message || ''
  );
}

/**
 * Apply heuristic answer extraction for a conversation step.
 * Conservative: unknown stays unknown unless the message clearly speaks.
 */
function applyAnswerToAreas(areas, stepId, userMessage) {
  const text = String(userMessage || '').trim();
  const lower = text.toLowerCase();
  const next = cloneAreas(areas);
  const url = extractUrl(text);

  const mark = (areaId, itemId, status, evidence, source = 'client_stated') => {
    const defArea = READINESS_AREAS.find((a) => a.id === areaId);
    const def = defArea && defArea.items.find((i) => i.id === itemId);
    setItem(next, areaId, itemId, {
      status,
      evidence: evidence || text.slice(0, 280),
      source,
      recommended_next_step:
        status === 'ready'
          ? 'No action needed.'
          : (def && def.defaultNextStep) || '',
    });
  };

  if (stepId === 'website_domain') {
    if (url) {
      mark('website', 'website_exists', 'ready', `Website URL stated: ${url}`);
      // URL alone does not prove DNS points at the site — needs verification.
      mark(
        'domain_dns',
        'domain_connected',
        'unknown',
        `Domain referenced via ${url}; website connection not independently verified`
      );
      mark(
        'domain_dns',
        'domain_owned',
        'unknown',
        'Domain referenced; ownership not independently verified'
      );
      mark(
        'domain_dns',
        'ssl_active',
        /https:\/\//i.test(url) ? 'ready' : 'unknown',
        /https:\/\//i.test(url)
          ? `HTTPS present in stated URL: ${url}`
          : `URL stated without HTTPS evidence: ${url}`
      );
    } else if (
      /no (website|site)|don't have a (website|site)|no site yet/i.test(lower) &&
      !looksUncertain(text)
    ) {
      mark('website', 'website_exists', 'missing', text);
      mark('domain_dns', 'domain_connected', 'missing', text);
    }
    if (/domain (is )?owned|we own the domain|registered/i.test(lower) && !looksUncertain(text)) {
      mark('domain_dns', 'domain_owned', 'ready', text);
    }
    if (/no domain|don't own|do not own/i.test(lower) && !looksUncertain(text)) {
      mark('domain_dns', 'domain_owned', 'missing', text);
    }
    if (/spf|dkim|dmarc/i.test(lower)) {
      mark(
        'domain_dns',
        'spf_dkim_dmarc',
        statusFromEvidence(text, { binary: true }),
        text
      );
    }
    if (
      /branded email|@[a-z0-9.-]+\.[a-z]{2,}/i.test(lower) &&
      !/@gmail\.|@yahoo\.|@hotmail\./i.test(lower)
    ) {
      mark(
        'domain_dns',
        'branded_email',
        looksUncertain(text) ? 'unknown' : 'ready',
        text
      );
    } else if (/gmail|yahoo|hotmail|no branded/i.test(lower) && !looksUncertain(text)) {
      mark('domain_dns', 'branded_email', 'missing', text);
    }
    if (/cloudflare|godaddy|namecheap|google domains|route ?53|dns provider/i.test(lower)) {
      mark('domain_dns', 'dns_provider_known', 'ready', text);
    }
    if (/mobile|responsive/i.test(lower)) {
      mark(
        'website',
        'mobile_usability',
        statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
    if (/services?\b/i.test(lower)) {
      mark(
        'website',
        'clear_services',
        statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
    if (/service area|we serve|coverage/i.test(lower)) {
      mark(
        'website',
        'clear_service_area',
        statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
    if (/\bcta\b|call to action|contact (us )?button|book now/i.test(lower)) {
      mark(
        'website',
        'clear_cta',
        statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
    if (/form/i.test(lower)) {
      mark(
        'website',
        'contact_form_works',
        statusFromEvidence(text, { binary: true }),
        text
      );
    }
    if (/phone|email/i.test(lower)) {
      mark(
        'website',
        'phone_email_visible',
        statusFromEvidence(text, { binary: true }),
        text
      );
    }
  }

  if (stepId === 'gbp') {
    if (/google business|gbp|gmb|business profile/i.test(lower) || looksPositive(text)) {
      if (
        /no (gbp|google business|profile)|don't have|do not have/i.test(lower) &&
        !looksUncertain(text)
      ) {
        mark('gbp', 'gbp_exists', 'missing', text);
        mark('gbp', 'gbp_claimed', 'missing', text);
      } else if (looksUncertain(text) && /google business|gbp|gmb|business profile/i.test(lower)) {
        mark(
          'gbp',
          'gbp_exists',
          'unknown',
          'GBP status uncertain; profile not independently checked'
        );
        mark('gbp', 'gbp_claimed', 'unknown', 'Claim/verification not confirmed');
      } else if (/google business|gbp|gmb|business profile/i.test(lower) || looksPositive(text)) {
        mark('gbp', 'gbp_exists', 'ready', text);
        if (/claimed|verified/i.test(lower)) {
          mark(
            'gbp',
            'gbp_claimed',
            /not (claimed|verified)|unclaimed|unverified/i.test(lower)
              ? 'missing'
              : 'ready',
            text
          );
        } else {
          mark(
            'gbp',
            'gbp_claimed',
            'unknown',
            'Existence mentioned; claim/verification not confirmed'
          );
        }
      }
    }
    if (/review/i.test(lower)) {
      // Reviews stay unknown until GBP/review profile is actually checked,
      // unless the client clearly states there are none.
      if (looksUncertain(text)) {
        mark(
          'reviews',
          'review_count',
          'unknown',
          'Review status uncertain; GBP/review profile not checked'
        );
        mark(
          'gbp',
          'gbp_reviews',
          'unknown',
          'Review status uncertain; GBP/review profile not checked'
        );
      } else if (looksNegative(text) && !/\d+\s*reviews?/i.test(text)) {
        mark('reviews', 'review_count', 'missing', text);
        mark('gbp', 'gbp_reviews', 'missing', text);
      } else {
        const count = text.match(/(\d+)\s*reviews?/i);
        const evidence = count
          ? `Client stated ${count[1]} reviews; GBP/review profile not independently checked`
          : 'Reviews mentioned; GBP/review profile not independently checked';
        mark('reviews', 'review_count', 'unknown', evidence);
        mark('gbp', 'gbp_reviews', 'unknown', evidence);
      }
    }
    if (/photo/i.test(lower)) {
      mark(
        'gbp',
        'gbp_photos',
        statusFromEvidence(text, { binary: true }),
        looksPositive(text) && !looksNegative(text)
          ? `${text.slice(0, 200)} (photos not independently verified on GBP)`
          : text
      );
      // Thin/incomplete photos are incomplete evidence — rare justified partial.
      if (/thin|few|need more|outdated/i.test(lower) && !looksNegative(text) && !looksUncertain(text)) {
        mark(
          'gbp',
          'gbp_photos',
          'partial',
          `${text.slice(0, 200)} (some photos indicated; completeness needs verification)`
        );
      }
    }
    if (/service area|nap|address|categories/i.test(lower)) {
      if (/service area|nap|address/i.test(lower)) {
        mark(
          'gbp',
          'gbp_nap',
          statusFromEvidence(text, { allowPartial: true }),
          text
        );
      }
    }
    if (/request review|ask for review|review process/i.test(lower)) {
      mark(
        'reviews',
        'review_request_process',
        statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
  }

  if (stepId === 'lead_flow') {
    if (/phone|call/i.test(lower)) {
      mark(
        'lead_capture',
        'phone_routing',
        statusFromEvidence(text, { binary: true }),
        text
      );
    }
    if (/form/i.test(lower)) {
      mark(
        'lead_capture',
        'contact_forms',
        statusFromEvidence(text, { binary: true }),
        text
      );
    }
    if (/email/i.test(lower)) {
      mark(
        'lead_capture',
        'email_routing',
        statusFromEvidence(text, { binary: true }),
        text
      );
    }
    if (/calendar|booking|calendly|schedule/i.test(lower)) {
      mark(
        'lead_capture',
        'booking_link',
        statusFromEvidence(text, { binary: true }),
        text
      );
    }
    if (/crm|pipeline|spreadsheet|inbox|goes to|we put them/i.test(lower)) {
      if (looksUncertain(text)) {
        mark('crm_pipeline', 'crm_exists', 'unknown', text);
      } else if (/no crm|don't have a crm|spreadsheet only|just (the )?inbox/i.test(lower)) {
        mark('crm_pipeline', 'crm_exists', 'missing', text);
        mark('crm_pipeline', 'stages_defined', 'missing', text);
      } else if (/crm|pulseforge|hubspot|salesforce|jobber|housecall/i.test(lower)) {
        mark('crm_pipeline', 'crm_exists', 'ready', text);
        mark(
          'crm_pipeline',
          'contacts_captured',
          'unknown',
          'CRM mentioned; contact capture not independently verified'
        );
      } else {
        mark('crm_pipeline', 'crm_exists', 'unknown', text);
      }
    }
    if (/missed|after hours|voicemail|no process/i.test(lower)) {
      mark(
        'lead_capture',
        'missed_lead_process',
        /no process|nothing happens|fall through/i.test(lower) && !looksUncertain(text)
          ? 'missing'
          : statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
    if (/response time|within \d+|same day|asap/i.test(lower)) {
      mark(
        'lead_capture',
        'response_time_expectation',
        statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
    if (/estimate request|request (an )?estimate/i.test(lower)) {
      mark(
        'lead_capture',
        'estimate_request_flow',
        statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
  }

  if (stepId === 'estimates') {
    if (/estimate|quote/i.test(lower)) {
      mark(
        'sales_process',
        'estimate_process',
        statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
    if (/proposal|template/i.test(lower)) {
      mark(
        'sales_process',
        'proposal_template',
        statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
    if (/pric|rate|sq\.? ?ft|square foot|hourly/i.test(lower)) {
      mark(
        'sales_process',
        'pricing_inputs',
        statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
    if (/walkthrough|site visit|walk-through/i.test(lower)) {
      mark(
        'sales_process',
        'walkthrough_process',
        statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
    if (/follow[- ]?up|cadence/i.test(lower)) {
      mark(
        'sales_process',
        'follow_up_cadence',
        statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
    if (/qualif/i.test(lower)) {
      mark(
        'sales_process',
        'qualification_questions',
        statusFromEvidence(text, { allowPartial: true }),
        text
      );
    }
  }

  if (stepId === 'tracking') {
    const trackMap = [
      [/google analytics|\bga4\b|\bga\b/i, 'google_analytics'],
      [/search console|\bgsc\b/i, 'search_console'],
      [/call tracking|callrail|whatConverts/i, 'call_tracking'],
      [/form tracking|form conversion/i, 'form_tracking'],
      [/source tracking|lead source|utm.*crm|crm source/i, 'crm_source_tracking'],
      [/conversion event|goals?/i, 'conversion_events'],
      [/\butms?\b/i, 'utm_discipline'],
    ];
    let any = false;
    for (const [re, id] of trackMap) {
      if (re.test(lower)) {
        any = true;
        // Binary tracking facts: ready / missing / unknown only.
        // Client clear "no X" is evidence; Max does not invent missing without inspection.
        const status = statusNearMatch(text, re, { binary: true });
        const evidence =
          status === 'ready' && id === 'google_analytics'
            ? `${text.slice(0, 200)} (client-stated; site tag not independently inspected)`
            : status === 'missing'
              ? text
              : `${text.slice(0, 200)} (needs verification; not independently inspected)`;
        mark('tracking', id, status, evidence);
      }
    }
    // Vague "no" / "nothing" without naming tools → unknown, not mass-missing.
    if (!any && (looksNegative(text) || looksUncertain(text))) {
      for (const def of READINESS_AREAS.find((a) => a.id === 'tracking').items) {
        mark(
          'tracking',
          def.id,
          'unknown',
          'Tracking not confirmed; site/accounts not independently inspected'
        );
      }
    }
  }

  if (stepId === 'assets') {
    const assetMap = [
      [/logo/i, 'logo', true],
      [/color|font|brand kit/i, 'colors_fonts', false],
      [/photos?\b|pictures?\b|images?\b/i, 'photos', false],
      [/before.?after/i, 'before_after', false],
      [/service description/i, 'service_descriptions', false],
      [/proof|license|testimonial|guarantee/i, 'proof_points', false],
      [/differentiat|why (customers )?choose|unique/i, 'differentiation', false],
      [/voice|tone|brand voice/i, 'voice_tone', false],
    ];
    for (const [re, id, binary] of assetMap) {
      if (re.test(lower)) {
        mark(
          'brand_assets',
          id,
          statusNearMatch(text, re, { binary: !!binary, allowPartial: !binary }),
          text
        );
      }
    }
    if (/facebook/i.test(lower)) {
      mark(
        'social',
        'facebook_present',
        statusNearMatch(text, /facebook/i, { binary: true }),
        text
      );
    }
    if (/instagram/i.test(lower)) {
      mark(
        'social',
        'instagram_present',
        statusNearMatch(text, /instagram/i, { binary: true }),
        text
      );
    }
    if (/linkedin/i.test(lower)) {
      if (/not relevant|n\/a/i.test(lower)) {
        mark('social', 'linkedin_present', 'not_applicable', text);
      } else {
        mark(
          'social',
          'linkedin_present',
          statusNearMatch(text, /linkedin/i, { binary: true }),
          text
        );
      }
    }
  }

  return refreshAreaStatuses(next);
}

function domainOwnedReady(areas) {
  const item =
    areas &&
    areas.domain_dns &&
    areas.domain_dns.items &&
    areas.domain_dns.items.domain_owned;
  return !!(item && item.status === 'ready');
}

function listGapItems(areas, predicate) {
  const out = [];
  const owned = domainOwnedReady(areas);
  for (const area of Object.values(areas || {})) {
    for (const it of Object.values(area.items || {})) {
      if (predicate(it, area)) {
        out.push({
          areaId: area.id,
          areaLabel: area.label,
          id: it.id,
          label: it.label,
          status: it.status,
          statusLabel: statusLabelForItem(it.id, it.status, {
            domainOwnedReady: owned,
          }),
          owner: it.owner,
          priority: it.priority,
          evidence: it.evidence || '',
          recommended_next_step: it.recommended_next_step || '',
          source: it.source || 'unknown',
        });
      }
    }
  }
  const rank = { high: 0, medium: 1, low: 2 };
  out.sort(
    (a, b) =>
      (rank[a.priority] ?? 9) - (rank[b.priority] ?? 9) ||
      a.areaLabel.localeCompare(b.areaLabel) ||
      a.label.localeCompare(b.label)
  );
  return out;
}

function overallStatus(areas) {
  const items = [];
  for (const area of Object.values(areas || {})) {
    items.push(...Object.values(area.items || {}));
  }
  if (!items.length) return 'unknown';
  const actionable = items.filter((i) => i.status !== 'not_applicable');
  if (!actionable.length) return 'unknown';
  if (actionable.every((i) => i.status === 'ready')) return 'ready';
  if (actionable.every((i) => i.status === 'unknown')) return 'unknown';
  const highMissing = actionable.some(
    (i) => i.priority === 'high' && (i.status === 'missing' || i.status === 'unknown')
  );
  if (highMissing && actionable.some((i) => i.status === 'missing')) return 'not_ready';
  if (actionable.some((i) => i.status === 'ready' || i.status === 'partial')) return 'partial';
  if (actionable.some((i) => i.status === 'missing')) return 'not_ready';
  return 'unknown';
}

function sequenceActionForGap(g) {
  const confirmByItem = {
    domain_owned: 'Confirm domain ownership at the registrar.',
    domain_connected: 'Confirm the domain is connected to the live website.',
    branded_email: 'Confirm a branded email mailbox exists (e.g. hello@domain).',
    spf_dkim_dmarc: 'Confirm SPF/DKIM/DMARC email authentication is in place.',
    phone_email_visible: 'Confirm the website shows a phone number and email.',
    contact_forms: 'Confirm a working contact or estimate form is on the site.',
    contact_form_works: 'Confirm form submissions arrive in a monitored inbox.',
    clear_cta: 'Confirm one clear estimate or walkthrough CTA on the site.',
    estimate_request_flow: 'Confirm how estimate or walkthrough requests are received.',
    gbp_exists: 'Confirm a Google Business Profile exists for the business.',
    gbp_claimed: 'Confirm the Google Business Profile is claimed and verified.',
    gbp_nap: 'Confirm GBP name, address, and service area are accurate.',
    gbp_photos: 'Confirm GBP has current photos.',
    gbp_reviews: 'Confirm reviews are present on the GBP/review profile.',
    google_analytics: 'Confirm lead/site tracking (e.g. GA4) exists before outreach.',
    form_tracking: 'Confirm form submissions are tracked as conversions.',
    crm_source_tracking: 'Confirm lead source is captured in the CRM.',
    call_tracking: 'Confirm call tracking or call logging is in place.',
    estimate_process: 'Confirm the estimate or proposal process is documented.',
    proposal_template: 'Confirm a reusable proposal template exists.',
    walkthrough_process: 'Confirm the walkthrough process and checklist.',
    follow_up_cadence: 'Confirm the follow-up cadence after estimates.',
  };
  if (confirmByItem[g.id] && (g.status === 'unknown' || g.status === 'partial' || g.status === 'missing')) {
    return confirmByItem[g.id];
  }
  const base = g.recommended_next_step || `Resolve ${g.label}.`;
  if (g.status === 'unknown' || g.status === 'partial') {
    if (/^confirm\b/i.test(base)) return base;
    return `Confirm: ${base}`;
  }
  return base;
}

function buildRecommendedSetupSequence(areas) {
  const gaps = listGapItems(
    areas,
    (it) => it.status === 'missing' || it.status === 'partial' || it.status === 'unknown'
  ).filter((g) => g.priority === 'high' || g.status === 'missing');

  const sequence = [];
  const seen = new Set();
  const preferAreaOrder = [
    'domain_dns',
    'website',
    'lead_capture',
    'gbp',
    'reviews',
    'tracking',
    'crm_pipeline',
    'sales_process',
    'brand_assets',
    'social',
  ];

  const pushGap = (g) => {
    const key = `${g.areaId}:${g.id}`;
    if (seen.has(key)) return;
    seen.add(key);
    sequence.push({
      order: sequence.length + 1,
      areaId: g.areaId,
      itemId: g.id,
      label: g.label,
      owner: g.owner,
      priority: g.priority,
      status: g.status,
      statusLabel: g.statusLabel,
      action: sequenceActionForGap(g),
    });
  };

  // 1) Practical blockers in explicit confirm order
  for (const itemId of SETUP_SEQUENCE_ITEM_PRIORITY) {
    const match = gaps.find((g) => g.id === itemId);
    if (match) pushGap(match);
    if (sequence.length >= 12) return sequence;
  }

  // 2) Remaining high-priority gaps by area preference
  for (const areaId of preferAreaOrder) {
    for (const g of gaps.filter((x) => x.areaId === areaId)) {
      pushGap(g);
      if (sequence.length >= 12) return sequence;
    }
  }
  return sequence;
}

/**
 * Build the Growth Infrastructure Readiness Report artifact.
 */
function isGapStatus(it, { includeHighUnknown = false } = {}) {
  if (it.status === 'missing' || it.status === 'partial') return true;
  if (includeHighUnknown && it.priority === 'high' && it.status === 'unknown') return true;
  return false;
}

function buildGrowthInfrastructureReadinessReport(areas, opts = {}) {
  const snapshot = refreshAreaStatuses(cloneAreas(areas));
  const businessName = shortName(opts.businessName || 'the business');
  const gaps = (pred) => listGapItems(snapshot, pred);

  // Enrich area items with status labels for UI consumers.
  for (const area of Object.values(snapshot)) {
    for (const it of Object.values(area.items || {})) {
      it.statusLabel = statusLabelForItem(it.id, it.status, {
        domainOwnedReady: domainOwnedReady(snapshot),
      });
    }
  }

  const demandCaptureRisks = gaps(
    (it, area) =>
      (area.id === 'lead_capture' || area.id === 'domain_dns' || area.id === 'website') &&
      isGapStatus(it, { includeHighUnknown: true })
  );
  const trustDiscoverabilityGaps = gaps(
    (it, area) =>
      (area.id === 'website' ||
        area.id === 'gbp' ||
        area.id === 'reviews' ||
        area.id === 'social' ||
        area.id === 'brand_assets') &&
      isGapStatus(it, { includeHighUnknown: true })
  );
  const trackingGaps = gaps(
    (it, area) =>
      area.id === 'tracking' &&
      (it.status === 'missing' ||
        it.status === 'partial' ||
        (it.priority === 'high' && it.status === 'unknown'))
  );
  const conversionFollowUpGaps = gaps(
    (it, area) =>
      (area.id === 'crm_pipeline' || area.id === 'sales_process') &&
      isGapStatus(it, { includeHighUnknown: true })
  );

  const maxCanCheck = gaps(
    (it) =>
      it.owner === 'max_can_check' &&
      (it.status === 'unknown' || it.status === 'partial' || it.status === 'missing')
  );
  const operatorClientMustComplete = gaps(
    (it) =>
      (it.owner === 'operator_guided' || it.owner === 'client_required') &&
      it.status !== 'ready' &&
      it.status !== 'not_applicable'
  );

  return {
    kind: ARTIFACT_KIND,
    title: REPORT_TITLE,
    businessName,
    overallStatus: overallStatus(snapshot),
    sectionTitles: { ...SECTION_TITLES },
    demandCaptureRisks,
    trustDiscoverabilityGaps,
    trackingGaps,
    conversionFollowUpGaps,
    maxCanCheck,
    operatorClientMustComplete,
    recommendedSetupSequence: buildRecommendedSetupSequence(snapshot),
    areas: snapshot,
    generatedAt: new Date().toISOString(),
    status: 'draft',
    directional: true,
    campaignsGenerated: false,
    assessmentOnly: true,
    disclaimer: REPORT_DISCLAIMER,
    blueprintId: opts.blueprintId || null,
    blueprintVersion: opts.blueprintVersion || null,
  };
}

function formatReadinessReportMessage(report) {
  const r = report || {};
  const titles = r.sectionTitles || SECTION_TITLES;
  const lines = [
    r.title || REPORT_TITLE,
    '',
    `Overall readiness: ${r.overallStatus || 'unknown'}`,
    '',
    `${titles.demandCaptureRisks}:`,
  ];
  const pushList = (arr, empty) => {
    if (!arr || !arr.length) {
      lines.push(`- ${empty}`);
      return;
    }
    for (const g of arr.slice(0, 6)) {
      const label = g.statusLabel || statusLabelForItem(g.id, g.status);
      lines.push(
        `- [${g.priority}] ${g.areaLabel}: ${g.label} (${label}) — owner: ${g.owner}`
      );
    }
  };
  pushList(r.demandCaptureRisks, 'Nothing blocking reachability from answers so far.');
  lines.push('', `${titles.trustDiscoverabilityGaps}:`);
  pushList(r.trustDiscoverabilityGaps, 'Nothing flagged yet — or still needs verification.');
  lines.push('', `${titles.trackingGaps}:`);
  pushList(r.trackingGaps, 'Nothing flagged yet — or still needs verification.');
  lines.push('', `${titles.conversionFollowUpGaps}:`);
  pushList(r.conversionFollowUpGaps, 'Nothing flagged yet from answers so far.');
  lines.push('', `${titles.maxCanCheck}:`);
  pushList(r.maxCanCheck, 'No outstanding Max-checkable items from current answers.');
  lines.push('', `${titles.operatorClientMustComplete}:`);
  pushList(
    r.operatorClientMustComplete,
    'No outstanding operator/client items from current answers.'
  );
  lines.push('', `${titles.recommendedSetupSequence}:`);
  if (r.recommendedSetupSequence && r.recommendedSetupSequence.length) {
    for (const step of r.recommendedSetupSequence.slice(0, 8)) {
      lines.push(
        `${step.order}. ${step.label} (${step.owner}) — ${step.action}`
      );
    }
  } else {
    lines.push('- Continue answering so I can sequence the highest-priority setup work.');
  }
  lines.push('', r.disclaimer || REPORT_DISCLAIMER);
  return lines.join('\n').trim();
}

function buildInfrastructureReadinessOpening(blueprint, opts = {}) {
  const name = shortName(opts.businessName || extractBusinessName(blueprint));
  const handoff = opts.growthHandoff || null;
  const hasFocus =
    handoff &&
    (handoff.primarySegment ||
      handoff.firstGrowthPlanPreview ||
      handoff.noCampaignOrProspectListYet);

  if (hasFocus) {
    const primary = handoff.primarySegment || 'the chosen first segment';
    const secondary = handoff.secondarySegment
      ? ` Secondary path: ${handoff.secondarySegment}.`
      : '';
    const market = handoff.targetMarket || handoff.geography || null;
    const proof =
      Array.isArray(handoff.proofNeeded) && handoff.proofNeeded.length
        ? handoff.proofNeeded.join(', ')
        : 'service checklist, photos/examples, clear response-time expectation, service area, walkthrough/estimate process';
    return [
      `Before we build a campaign or prospect list, I'd check whether ${name} has the infrastructure to capture and convert this demand.`,
      ``,
      `We're carrying the First Growth Plan focus: ${primary}${market ? ` in ${market}` : ''}.${secondary}`,
      `Conversion goal: ${handoff.conversionGoal || 'qualified conversations, walkthroughs, estimate requests'}.`,
      `Proof still needed before outreach: ${proof}.`,
      `No campaign or prospect list yet.`,
      ``,
      `This is Growth Infrastructure Readiness — making sure ${name} can catch the ball. I will not change DNS, GBP, social profiles, or tracking without explicit approval, and I will never ask for passwords here.`,
      ``,
      QUESTION_BANK[0].prompt,
    ].join('\n');
  }

  return [
    `Before we create demand, I want to make sure ${name} can capture and convert it. I'll check the basics first.`,
    ``,
    `This is Growth Infrastructure Readiness — separate from choosing which market to prioritize. I will not change DNS, GBP, social profiles, or tracking without explicit approval, and I will never ask for passwords here.`,
    ``,
    QUESTION_BANK[0].prompt,
  ].join('\n');
}

function nextQuestion(stepId) {
  const idx = QUESTION_BANK.findIndex((q) => q.step === stepId);
  if (idx < 0) return QUESTION_BANK[0];
  return QUESTION_BANK[idx + 1] || null;
}

function stepAfter(stepId) {
  const idx = CONVERSATION_STEPS.indexOf(stepId);
  if (idx < 0) return 'website_domain';
  return CONVERSATION_STEPS[Math.min(idx + 1, CONVERSATION_STEPS.length - 1)];
}

/**
 * Deterministic reply for the readiness conversation.
 *
 * @returns {{ message: string, step: string, areas: object, answers: object, report: object|null, intent: string|null }}
 */
function buildInfrastructureReadinessReply(userMessage, state, blueprint, opts = {}) {
  const prior = state || {};
  const currentStep = prior.step && prior.step !== 'opening' ? prior.step : 'website_domain';
  const answers = { ...(prior.answers || {}) };
  answers[currentStep] = {
    raw: String(userMessage || '').trim(),
    at: new Date().toISOString(),
  };

  let areas = applyAnswerToAreas(prior.areas || buildEmptyAreas(), currentStep, userMessage);
  const wantReport =
    detectReportRequest(userMessage) || currentStep === 'assets' || opts.forceReport;

  if (wantReport || currentStep === 'assets') {
    // Apply assets step before report when wrapping from assets.
    if (currentStep !== 'assets' && detectReportRequest(userMessage)) {
      // allow early wrap from any step
    }
    const report = buildGrowthInfrastructureReadinessReport(areas, {
      businessName: opts.businessName || extractBusinessName(blueprint),
      blueprintId: opts.blueprintId || (blueprint && blueprint.id) || null,
      blueprintVersion:
        opts.blueprintVersion || (blueprint && blueprint.version) || null,
    });
    return {
      message: [
        `Thanks — I have enough to draft the readiness picture.`,
        ``,
        formatReadinessReportMessage(report),
        ``,
        `We can keep refining any section, or move on when the high-priority setup sequence looks right. Still no campaigns from this report.`,
      ].join('\n'),
      step: 'report',
      areas,
      answers,
      report,
      intent: 'produce_report',
    };
  }

  const nxt = nextQuestion(currentStep);
  const nextStep = nxt ? nxt.step : 'report';
  if (!nxt) {
    const report = buildGrowthInfrastructureReadinessReport(areas, {
      businessName: opts.businessName || extractBusinessName(blueprint),
      blueprintId: opts.blueprintId || (blueprint && blueprint.id) || null,
      blueprintVersion:
        opts.blueprintVersion || (blueprint && blueprint.version) || null,
    });
    return {
      message: formatReadinessReportMessage(report),
      step: 'report',
      areas,
      answers,
      report,
      intent: 'produce_report',
    };
  }

  return {
    message: [
      `Got it — noted for ${currentStep.replace(/_/g, ' ')}.`,
      ``,
      nxt.prompt,
    ].join('\n'),
    step: nextStep,
    areas,
    answers,
    report: null,
    intent: 'advance',
  };
}

function containsForbiddenReadinessLanguage(text) {
  const s = String(text || '');
  return (
    /what(?:'| i)?s your password|send (?:me )?your password|login password/i.test(s) ||
    /I (?:changed|updated|modified) (?:your )?(?:DNS|GBP|Google Business|tracking pixel)/i.test(
      s
    ) ||
    /campaign is live|I built a prospect list|launching outreach now/i.test(s)
  );
}

module.exports = {
  ARTIFACT_KIND,
  REPORT_TITLE,
  REPORT_DISCLAIMER,
  SECTION_TITLES,
  ITEM_STATUSES,
  OWNERS,
  PRIORITIES,
  CONVERSATION_STEPS,
  READINESS_AREAS,
  QUESTION_BANK,
  BINARY_ITEM_IDS,
  SETUP_SEQUENCE_ITEM_PRIORITY,
  buildEmptyAreas,
  applyAnswerToAreas,
  buildGrowthInfrastructureReadinessReport,
  formatReadinessReportMessage,
  buildInfrastructureReadinessOpening,
  buildInfrastructureReadinessReply,
  detectReportRequest,
  extractBusinessName,
  containsForbiddenReadinessLanguage,
  overallStatus,
  listGapItems,
  statusLabelForItem,
  statusFromEvidence,
  stepAfter,
};
