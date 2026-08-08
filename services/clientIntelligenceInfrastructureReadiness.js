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
  'Assessment only — no DNS, GBP, social, or tracking changes were made. Client-stated facts stay separate from automated observations until reviewed.';

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
      mark('domain_dns', 'domain_connected', 'partial', `Domain referenced via ${url}`);
      mark('domain_dns', 'domain_owned', 'partial', 'Domain referenced; ownership not independently verified', 'client_stated');
      mark('domain_dns', 'ssl_active', /https:\/\//i.test(url) ? 'ready' : 'unknown', url);
    } else if (/no (website|site)|don't have a (website|site)|no site yet/i.test(lower)) {
      mark('website', 'website_exists', 'missing', text);
      mark('domain_dns', 'domain_connected', 'missing', text);
    }
    if (/domain (is )?owned|we own the domain|registered/i.test(lower)) {
      mark('domain_dns', 'domain_owned', 'ready', text);
    }
    if (/no domain|don't own|do not own/i.test(lower)) {
      mark('domain_dns', 'domain_owned', 'missing', text);
    }
    if (/spf|dkim|dmarc/i.test(lower)) {
      mark(
        'domain_dns',
        'spf_dkim_dmarc',
        looksNegative(text) ? 'missing' : 'partial',
        text
      );
    }
    if (/branded email|@[a-z0-9.-]+\.[a-z]{2,}/i.test(lower) && !/@gmail\.|@yahoo\.|@hotmail\./i.test(lower)) {
      mark('domain_dns', 'branded_email', 'ready', text);
    } else if (/gmail|yahoo|hotmail|no branded/i.test(lower)) {
      mark('domain_dns', 'branded_email', 'missing', text);
    }
    if (/cloudflare|godaddy|namecheap|google domains|route ?53|dns provider/i.test(lower)) {
      mark('domain_dns', 'dns_provider_known', 'ready', text);
    }
    if (/mobile|responsive/i.test(lower)) {
      mark('website', 'mobile_usability', looksNegative(text) ? 'missing' : 'partial', text);
    }
    if (/services?\b/i.test(lower)) {
      mark('website', 'clear_services', looksNegative(text) ? 'missing' : 'partial', text);
    }
    if (/service area|we serve|coverage/i.test(lower)) {
      mark('website', 'clear_service_area', looksNegative(text) ? 'missing' : 'partial', text);
    }
    if (/\bcta\b|call to action|contact (us )?button|book now/i.test(lower)) {
      mark('website', 'clear_cta', looksNegative(text) ? 'missing' : 'partial', text);
    }
    if (/form/i.test(lower)) {
      mark(
        'website',
        'contact_form_works',
        looksNegative(text) ? 'missing' : looksPositive(text) ? 'ready' : 'partial',
        text
      );
    }
    if (/phone|email/i.test(lower)) {
      mark(
        'website',
        'phone_email_visible',
        looksNegative(text) ? 'missing' : 'partial',
        text
      );
    }
  }

  if (stepId === 'gbp') {
    if (/google business|gbp|gmb|business profile/i.test(lower) || looksPositive(text)) {
      if (/no (gbp|google business|profile)|don't have|do not have/i.test(lower)) {
        mark('gbp', 'gbp_exists', 'missing', text);
        mark('gbp', 'gbp_claimed', 'missing', text);
      } else {
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
          mark('gbp', 'gbp_claimed', 'unknown', 'Existence mentioned; claim/verification not confirmed');
        }
      }
    }
    if (/review/i.test(lower)) {
      const count = text.match(/(\d+)\s*reviews?/i);
      if (count) {
        mark('reviews', 'review_count', Number(count[1]) > 0 ? 'partial' : 'missing', text);
        mark('gbp', 'gbp_reviews', Number(count[1]) > 0 ? 'partial' : 'missing', text);
      } else if (looksNegative(text)) {
        mark('reviews', 'review_count', 'missing', text);
        mark('gbp', 'gbp_reviews', 'missing', text);
      } else {
        mark('reviews', 'review_count', 'partial', text);
        mark('gbp', 'gbp_reviews', 'partial', text);
      }
    }
    if (/photo/i.test(lower)) {
      mark('gbp', 'gbp_photos', looksNegative(text) ? 'missing' : 'partial', text);
    }
    if (/request review|ask for review|review process/i.test(lower)) {
      mark(
        'reviews',
        'review_request_process',
        looksNegative(text) ? 'missing' : 'partial',
        text
      );
    }
  }

  if (stepId === 'lead_flow') {
    if (/phone|call/i.test(lower)) {
      mark('lead_capture', 'phone_routing', looksNegative(text) ? 'missing' : 'partial', text);
    }
    if (/form/i.test(lower)) {
      mark('lead_capture', 'contact_forms', looksNegative(text) ? 'missing' : 'partial', text);
    }
    if (/email/i.test(lower)) {
      mark('lead_capture', 'email_routing', looksNegative(text) ? 'missing' : 'partial', text);
    }
    if (/calendar|booking|calendly|schedule/i.test(lower)) {
      mark(
        'lead_capture',
        'booking_link',
        looksNegative(text) ? 'missing' : 'partial',
        text
      );
    }
    if (/crm|pipeline|spreadsheet|inbox|goes to|we put them/i.test(lower)) {
      if (/no crm|don't have a crm|spreadsheet only|just (the )?inbox/i.test(lower)) {
        mark('crm_pipeline', 'crm_exists', 'missing', text);
        mark('crm_pipeline', 'stages_defined', 'missing', text);
      } else if (/crm|pulseforge|hubspot|salesforce|jobber|housecall/i.test(lower)) {
        mark('crm_pipeline', 'crm_exists', 'ready', text);
        mark('crm_pipeline', 'contacts_captured', 'partial', text);
      } else {
        mark('crm_pipeline', 'crm_exists', 'partial', text);
      }
    }
    if (/missed|after hours|voicemail|no process/i.test(lower)) {
      mark(
        'lead_capture',
        'missed_lead_process',
        /no process|nothing happens|fall through/i.test(lower) ? 'missing' : 'partial',
        text
      );
    }
    if (/response time|within \d+|same day|asap/i.test(lower)) {
      mark('lead_capture', 'response_time_expectation', 'partial', text);
    }
    if (/estimate request|request (an )?estimate/i.test(lower)) {
      mark('lead_capture', 'estimate_request_flow', 'partial', text);
    }
  }

  if (stepId === 'estimates') {
    if (/estimate|quote/i.test(lower)) {
      mark(
        'sales_process',
        'estimate_process',
        looksNegative(text) ? 'missing' : 'partial',
        text
      );
    }
    if (/proposal|template/i.test(lower)) {
      mark(
        'sales_process',
        'proposal_template',
        looksNegative(text) ? 'missing' : 'partial',
        text
      );
    }
    if (/pric|rate|sq\.? ?ft|square foot|hourly/i.test(lower)) {
      mark(
        'sales_process',
        'pricing_inputs',
        looksNegative(text) ? 'missing' : 'partial',
        text
      );
    }
    if (/walkthrough|site visit|walk-through/i.test(lower)) {
      mark(
        'sales_process',
        'walkthrough_process',
        looksNegative(text) ? 'missing' : 'partial',
        text
      );
    }
    if (/follow[- ]?up|cadence/i.test(lower)) {
      mark(
        'sales_process',
        'follow_up_cadence',
        looksNegative(text) ? 'missing' : 'partial',
        text
      );
    }
    if (/qualif/i.test(lower)) {
      mark(
        'sales_process',
        'qualification_questions',
        looksNegative(text) ? 'missing' : 'partial',
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
      [/\butm\b/i, 'utm_discipline'],
    ];
    let any = false;
    for (const [re, id] of trackMap) {
      if (re.test(lower)) {
        any = true;
        mark(
          'tracking',
          id,
          looksNegative(text) ? 'missing' : looksPositive(text) ? 'ready' : 'partial',
          text
        );
      }
    }
    if (!any && looksNegative(text)) {
      for (const def of READINESS_AREAS.find((a) => a.id === 'tracking').items) {
        mark('tracking', def.id, 'missing', text);
      }
    }
  }

  if (stepId === 'assets') {
    const assetMap = [
      [/logo/i, 'logo'],
      [/color|font|brand kit/i, 'colors_fonts'],
      [/photo|picture|image/i, 'photos'],
      [/before.?after/i, 'before_after'],
      [/service description/i, 'service_descriptions'],
      [/proof|license|testimonial|guarantee/i, 'proof_points'],
      [/differentiat|why (customers )?choose|unique/i, 'differentiation'],
      [/voice|tone|brand voice/i, 'voice_tone'],
    ];
    for (const [re, id] of assetMap) {
      if (re.test(lower)) {
        mark(
          'brand_assets',
          id,
          looksNegative(text) ? 'missing' : looksPositive(text) ? 'ready' : 'partial',
          text
        );
      }
    }
    if (/facebook/i.test(lower)) {
      mark(
        'social',
        'facebook_present',
        looksNegative(text) ? 'missing' : 'partial',
        text
      );
    }
    if (/instagram/i.test(lower)) {
      mark(
        'social',
        'instagram_present',
        looksNegative(text) ? 'missing' : 'partial',
        text
      );
    }
    if (/linkedin/i.test(lower)) {
      mark(
        'social',
        'linkedin_present',
        looksNegative(text) ? 'missing' : /not relevant|n\/a/i.test(lower) ? 'not_applicable' : 'partial',
        text
      );
    }
  }

  return refreshAreaStatuses(next);
}

function listGapItems(areas, predicate) {
  const out = [];
  for (const area of Object.values(areas || {})) {
    for (const it of Object.values(area.items || {})) {
      if (predicate(it, area)) {
        out.push({
          areaId: area.id,
          areaLabel: area.label,
          id: it.id,
          label: it.label,
          status: it.status,
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

function buildRecommendedSetupSequence(areas) {
  const gaps = listGapItems(
    areas,
    (it) => it.status === 'missing' || it.status === 'partial' || it.status === 'unknown'
  ).filter((g) => g.priority === 'high' || g.status === 'missing');

  const sequence = [];
  const seen = new Set();
  const preferOrder = [
    'domain_dns',
    'website',
    'lead_capture',
    'gbp',
    'tracking',
    'crm_pipeline',
    'sales_process',
    'reviews',
    'brand_assets',
    'social',
  ];

  for (const areaId of preferOrder) {
    for (const g of gaps.filter((x) => x.areaId === areaId)) {
      const key = `${g.areaId}:${g.id}`;
      if (seen.has(key)) continue;
      seen.add(key);
      sequence.push({
        order: sequence.length + 1,
        areaId: g.areaId,
        itemId: g.id,
        label: g.label,
        owner: g.owner,
        priority: g.priority,
        status: g.status,
        action: g.recommended_next_step || `Resolve ${g.label}.`,
      });
      if (sequence.length >= 12) return sequence;
    }
  }
  return sequence;
}

/**
 * Build the Growth Infrastructure Readiness Report artifact.
 */
function buildGrowthInfrastructureReadinessReport(areas, opts = {}) {
  const snapshot = refreshAreaStatuses(cloneAreas(areas));
  const businessName = shortName(opts.businessName || 'the business');
  const gaps = (pred) => listGapItems(snapshot, pred);

  const demandCaptureRisks = gaps(
    (it, area) =>
      (area.id === 'lead_capture' || area.id === 'domain_dns' || area.id === 'website') &&
      (it.status === 'missing' || it.status === 'partial' || (it.priority === 'high' && it.status === 'unknown'))
  );
  const trustDiscoverabilityGaps = gaps(
    (it, area) =>
      (area.id === 'website' ||
        area.id === 'gbp' ||
        area.id === 'reviews' ||
        area.id === 'social' ||
        area.id === 'brand_assets') &&
      (it.status === 'missing' || it.status === 'partial')
  );
  const trackingGaps = gaps(
    (it, area) =>
      area.id === 'tracking' &&
      (it.status === 'missing' || it.status === 'partial' || it.status === 'unknown')
  );
  const conversionFollowUpGaps = gaps(
    (it, area) =>
      (area.id === 'crm_pipeline' || area.id === 'sales_process') &&
      (it.status === 'missing' || it.status === 'partial' || (it.priority === 'high' && it.status === 'unknown'))
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
    disclaimer: REPORT_DISCLAIMER,
    blueprintId: opts.blueprintId || null,
    blueprintVersion: opts.blueprintVersion || null,
  };
}

function formatReadinessReportMessage(report) {
  const r = report || {};
  const lines = [
    r.title || REPORT_TITLE,
    '',
    `Overall readiness: ${r.overallStatus || 'unknown'}`,
    '',
    'Demand capture risks:',
  ];
  const pushList = (arr, empty) => {
    if (!arr || !arr.length) {
      lines.push(`- ${empty}`);
      return;
    }
    for (const g of arr.slice(0, 6)) {
      lines.push(`- [${g.priority}] ${g.areaLabel}: ${g.label} (${g.status}) — owner: ${g.owner}`);
    }
  };
  pushList(r.demandCaptureRisks, 'None flagged yet from answers so far.');
  lines.push('', 'Trust / discoverability gaps:');
  pushList(r.trustDiscoverabilityGaps, 'None flagged yet.');
  lines.push('', 'Tracking gaps:');
  pushList(r.trackingGaps, 'None flagged yet.');
  lines.push('', 'Conversion / follow-up gaps:');
  pushList(r.conversionFollowUpGaps, 'None flagged yet.');
  lines.push('', 'What Max can check automatically:');
  pushList(r.maxCanCheck, 'No outstanding Max-checkable items from current answers.');
  lines.push('', 'What the operator/client must complete:');
  pushList(
    r.operatorClientMustComplete,
    'No outstanding operator/client items from current answers.'
  );
  lines.push('', 'Recommended setup sequence:');
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
  lines.push('This report does not generate campaigns.');
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
  ITEM_STATUSES,
  OWNERS,
  PRIORITIES,
  CONVERSATION_STEPS,
  READINESS_AREAS,
  QUESTION_BANK,
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
  stepAfter,
};
