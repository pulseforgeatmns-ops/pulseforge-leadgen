'use strict';

/**
 * Growth Workspace left-panel rendering helpers (SPEC-088 polish).
 * Shared by /client-intel and Node regression tests.
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.PulseforgeGrowthWorkspacePanel = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  const OWNER_LABELS = Object.freeze({
    client_required: 'Client/operator',
    operator_guided: 'Operator guided',
    max_can_check: 'Max can check',
  });

  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function formatOwnerLabel(owner) {
    if (owner == null || owner === '') return '';
    const key = String(owner);
    if (OWNER_LABELS[key]) return OWNER_LABELS[key];
    return key
      .split('_')
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(' ');
  }

  function shortBusinessName(name) {
    const s = String(name || '').trim();
    if (!s) return 'the business';
    if (/\banchor\s+cleaning\b/i.test(s) || /^anchor\b/i.test(s)) return 'Anchor';
    return s;
  }

  function sessionIdOf(session) {
    if (!session) return null;
    return session.sessionId || session.interviewId || session.id || null;
  }

  function isHistoricalPreviousPlan(session, currentSessionId) {
    if (!session) return false;
    const id = sessionIdOf(session);
    if (!id) return false;
    if (currentSessionId && String(id) === String(currentSessionId)) return false;
    return true;
  }

  function filterPreviousPlans(sessions, currentSessionId) {
    const list = Array.isArray(sessions) ? sessions : [];
    return list.filter((s) => isHistoricalPreviousPlan(s, currentSessionId));
  }

  /**
   * Structured setup-task guidance keyed by stable itemId / slug.
   * Fallback defaultSetupGuidance is only for unknown/custom tasks.
   */
  const TASK_GUIDANCE_ALIASES = Object.freeze({
    branded_email_available: 'branded_email',
    domain_connected_to_website: 'domain_connected',
    spf_dkim_dmarc_present: 'spf_dkim_dmarc',
    google_business_profile_claimed: 'gbp_claimed',
    gbp_name_address_service_area: 'gbp_nap',
    gbp_website_phone_connected: 'gbp_contact',
    reviews_present: 'gbp_reviews',
    photos_present: 'gbp_photos',
  });

  /** Canonical itemIds / slugs with task-specific guidance (no placeholder fallback). */
  const KNOWN_GROWTH_INFRA_GUIDANCE_IDS = Object.freeze([
    'branded_email',
    'domain_connected',
    'domain_owned',
    'spf_dkim_dmarc',
    'clear_cta',
    'clear_service_area',
    'clear_services',
    'contact_form_works',
    'contact_forms',
    'mobile_usability',
    'phone_email_visible',
    'gbp_claimed',
    'gbp_nap',
    'gbp_contact',
    'gbp_reviews',
    'gbp_photos',
    'review_request_process',
    'crm_exists',
    'contacts_captured',
    'stages_defined',
    'follow_up_reminders',
    'crm_source_tracking',
    'form_tracking',
    'google_analytics',
    'search_console',
    'call_tracking',
    'conversion_events',
    'estimate_process',
    'follow_up_cadence',
    'brand_assets_ready',
    'social_profiles_present',
  ]);

  function brandedEmailGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Before outreach, ' +
        name +
        ' should look legitimate and be easy to reply to. A branded email helps property managers trust the business and keeps replies organized.',
      whatToDo: 'Create a mailbox such as hello@domain or estimates@domain.',
      whatToConfirm: [
        'The mailbox can send and receive email.',
        'Replies go to the person responsible for new opportunities.',
        'The email is connected to the website/contact form if applicable.',
        'SPF, DKIM, and DMARC should be checked before outbound outreach.',
      ],
      whoOwnsIt: 'Client/operator',
      owner: 'Client/operator',
      completeWhen:
        name +
        ' has a working branded mailbox and someone is responsible for checking it.',
    };
  }

  function domainConnectedGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Before outreach, ' +
        name +
        ' needs a domain that loads the live website. Prospects and marketing links should land on the real site over HTTPS, not an old host or parking page.',
      whatToDo:
        'Confirm the domain points to the live website. If not, document the needed A/CNAME change for approval.',
      whatToConfirm: [
        'The domain loads the live website.',
        'Both www and non-www versions route correctly, or one redirects cleanly to the other.',
        'The site uses HTTPS.',
        'The domain shown in marketing materials matches the live site.',
        'No DNS or website changes are made without explicit approval.',
      ],
      whoOwnsIt: 'Max can check, operator/client approves changes.',
      owner: 'Max can check; operator approves fixes',
      completeWhen:
        'The domain for ' +
        name +
        ' loads the live website over HTTPS with www/non-www routing confirmed.',
    };
  }

  function domainOwnedGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Before outreach, ' +
        name +
        ' needs to know who controls the domain. Domain ownership is what lets the business connect the website, set up branded email, verify tools, and protect the brand.',
      whatToDo:
        'Confirm which registrar or platform owns the domain, and who has access to manage it.',
      whatToConfirm: [
        'The domain is registered and active.',
        name + ' knows where the domain is managed.',
        'The owner/operator knows who can approve DNS changes.',
        'The domain is not expired or at risk of renewal issues.',
        'No login credentials are shared inside Max.',
      ],
      whoOwnsIt: 'Client/operator',
      owner: 'Client/operator',
      completeWhen: 'Domain ownership and access path are confirmed.',
    };
  }

  function spfDkimDmarcGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Before any serious outbound outreach, ' +
        name +
        "'s domain should be authenticated so emails are more likely to reach inboxes and less likely to look suspicious. SPF, DKIM, and DMARC help receiving mail systems trust that messages from " +
        name +
        ' are legitimate.',
      whatToDo:
        'Check whether SPF, DKIM, and DMARC records exist for the sending domain. If records are missing or incorrect, document the DNS changes needed for approval.',
      whatToConfirm: [
        'SPF is present for the domain.',
        'DKIM is enabled for the email provider.',
        'DMARC is present, even if starting with a monitoring policy.',
        'The branded mailbox can send and receive successfully.',
        'No DNS changes are made without explicit approval.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        'SPF, DKIM, and DMARC are confirmed or the required DNS changes are documented for approval.',
    };
  }

  function clearCtaGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Before outreach, visitors should immediately know what action to take. ' +
        name +
        ' should have one obvious next step for commercial prospects, such as requesting an estimate, booking a walkthrough, or calling for availability.',
      whatToDo:
        'Choose one primary CTA for the website and growth materials. For ' +
        name +
        ', prefer estimate request or walkthrough request over vague language like "learn more."',
      whatToConfirm: [
        'The primary CTA is visible on the website.',
        'The CTA matches the commercial growth goal.',
        'The CTA leads to a working form, phone number, email, or booking path.',
        'The CTA does not create confusion with multiple competing actions.',
        'No website changes are published without approval.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        name +
        ' has one clear primary CTA for commercial prospects, and the path behind it works.',
    };
  }

  function clearServiceAreaGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Before outreach, prospects should know whether ' +
        name +
        ' serves their location. A clear service area keeps the first growth push focused on Greater Manchester and prevents wasted conversations outside the target market.',
      whatToDo:
        'State the priority service area clearly on the website and growth materials. For ' +
        name +
        ', use Greater Manchester first, especially Bedford, Hooksett, Londonderry, Auburn, and Goffstown.',
      whatToConfirm: [
        'The website names the primary service area.',
        'Priority towns match the approved Blueprint.',
        'The service area is easy to find from the homepage or contact/estimate path.',
        'Outreach and future prospect lists stay inside the approved market bound unless changed intentionally.',
        'No website changes are published without approval.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        name +
        "'s service area is clearly stated and matches the approved Growth Plan.",
    };
  }

  function clearServicesGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Before outreach, property managers should quickly understand what ' +
        name +
        ' actually provides. Clear services help prospects self-identify fit and reduce vague inquiries that do not match the commercial growth plan.',
      whatToDo:
        'List ' +
        name +
        "'s primary services clearly on the website and growth materials. Emphasize recurring commercial cleaning while still showing the current service mix.",
      whatToConfirm: [
        'The website clearly lists the main services.',
        'Recurring commercial cleaning is easy to understand.',
        'Short-term rental turnovers, office cleaning, deep cleans, move-in/move-out, and residential cleaning are represented accurately.',
        'Service descriptions do not overpromise capacity or specialized work ' +
          name +
          ' has not approved.',
        'No website changes are published without approval.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        name +
        "'s primary services are clearly stated and aligned with the approved Blueprint.",
    };
  }

  function contactFormWorksGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'If outreach creates interest, the form has to deliver every inquiry reliably. A broken form would make ' +
        name +
        ' look unresponsive and could lose qualified property manager opportunities before anyone sees them.',
      whatToDo:
        'Submit a test inquiry through the website form using a test name and email. Confirm the message arrives in the right inbox or lead tracker, and confirm someone knows who is responsible for replying.',
      whatToConfirm: [
        'The form can be submitted successfully.',
        'The submission arrives in a monitored inbox or lead tracker.',
        'The notification includes enough detail to follow up.',
        'The reply-to email or phone number is usable.',
        'The person responsible for new inquiries knows where to check.',
        'No tracking or website changes are made without approval.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        'A test form submission is received successfully and the follow-up owner is clear.',
    };
  }

  function contactFormsGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Outreach only works if interested prospects have a clear way to request an estimate or walkthrough. ' +
        name +
        ' needs a working contact or estimate form on the site before campaigns send traffic.',
      whatToDo:
        'Confirm the website has a contact or estimate form that is easy to find from the homepage and CTA path. Note any missing form for operator-approved website work.',
      whatToConfirm: [
        'A contact or estimate form exists on the live site.',
        'The form is reachable from the primary CTA.',
        'Required fields collect enough detail to follow up (name, contact, location or property type as needed).',
        'Spam protection does not block legitimate submissions.',
        'No website changes are published without approval.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        name + ' has a discoverable contact/estimate form ready for testing.',
    };
  }

  function mobileUsabilityGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Many property managers will open ' +
        name +
        "'s site on a phone. If buttons are hard to tap, text overflows, or the CTA is buried, outreach clicks will not convert.",
      whatToDo:
        'Open the live site on a phone-width viewport and check layout, tap targets, and the path to contact/estimate. Document any fixes needed for approval.',
      whatToConfirm: [
        'Primary content is readable without horizontal scrolling.',
        'Phone and CTA buttons are easy to tap.',
        'The contact or estimate path works on mobile.',
        'Images and menus do not obscure key actions.',
        'No website changes are published without approval.',
      ],
      whoOwnsIt: 'Max can check; operator approves fixes',
      owner: 'Max can check; operator approves fixes',
      completeWhen:
        name +
        "'s mobile layout and tap targets are usable, or needed fixes are documented for approval.",
    };
  }

  function phoneEmailVisibleGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'If a property manager is interested, they should not have to hunt for a way to reach ' +
        name +
        '. Visible contact information makes the business easier to trust and easier to contact from the website, Google profile, and outreach follow-up.',
      whatToDo:
        'Confirm the website shows a working phone number and branded email address in obvious places, especially the header, footer, contact page, and estimate/request flow.',
      whatToConfirm: [
        'Phone number is visible and tappable on mobile.',
        'Email address is visible or available through a clear contact path.',
        'Contact information matches the Google Business Profile and outreach materials.',
        'The listed phone/email are monitored by the right person.',
        'No website changes are published without approval.',
      ],
      whoOwnsIt: 'Max can check; operator approves fixes.',
      owner: 'Max can check; operator approves fixes',
      completeWhen:
        'Phone and email are easy to find, accurate, and monitored.',
    };
  }

  function gbpClaimedGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'A claimed Google Business Profile is how local search and Maps show ' +
        name +
        ' as a real business. Unclaimed or unverified listings limit trust and make it harder to control name, phone, and website details.',
      whatToDo:
        'Confirm whether the Google Business Profile exists and is claimed/verified by someone at ' +
        name +
        '. If not, outline the claim/verify steps for the client/operator — do not change GBP without approval.',
      whatToConfirm: [
        'A Google Business Profile listing exists for the business.',
        'The listing is claimed and verified by the owner/operator.',
        'The person with GBP access is known (no password sharing in Max).',
        'No GBP edits are made without explicit approval.',
      ],
      whoOwnsIt: 'Client/operator',
      owner: 'Client/operator',
      completeWhen:
        name +
        "'s Google Business Profile is claimed/verified, or the claim path is assigned to the client/operator.",
    };
  }

  function gbpNapGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Incorrect name, address, or service area on Google confuses prospects and weakens local trust. ' +
        name +
        "'s GBP should match the website and the approved Greater Manchester focus.",
      whatToDo:
        'Review the GBP name, address (or service-area business settings), and service area against the website and Blueprint. Document corrections for client/operator approval before any GBP edit.',
      whatToConfirm: [
        'Business name matches how ' + name + ' wants to appear publicly.',
        'Address or service-area setting is accurate for how the business operates.',
        'Service area aligns with the approved Growth Plan markets.',
        'Website NAP and GBP NAP do not conflict.',
        'No GBP changes are made without explicit approval.',
      ],
      whoOwnsIt: 'Client/operator',
      owner: 'Client/operator',
      completeWhen:
        'GBP name, address/service-area settings, and website NAP are aligned or correction steps are approved.',
    };
  }

  function gbpContactGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'When someone finds ' +
        name +
        ' on Google, the website and phone on the profile must work and match outreach materials. Broken or mismatched contact details waste warm interest.',
      whatToDo:
        'Confirm the GBP website URL and phone number match the live site and monitored contact paths. Document any link or phone updates for approval.',
      whatToConfirm: [
        'GBP website URL loads the live site.',
        'GBP phone number is correct and monitored.',
        'Contact details match the website header/footer.',
        'No GBP or website changes are made without explicit approval.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        'GBP website and phone match the live, monitored contact paths.',
    };
  }

  function gbpReviewsGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Reviews are a primary trust signal for property managers checking ' +
        name +
        ' before responding to outreach. A thin or empty review profile makes cold outreach harder to convert.',
      whatToDo:
        'Check current Google review count and recency. If reviews are thin, plan a client-approved review request process rather than fabricating or scraping reviews.',
      whatToConfirm: [
        'Current review count and average rating are documented.',
        'Recent reviews exist or a gap is clearly noted.',
        'There is a plan to request reviews from real customers after jobs.',
        'No fake reviews or review-gating practices are used.',
        'No GBP changes are made without approval.',
      ],
      whoOwnsIt: 'Client/operator',
      owner: 'Client/operator',
      completeWhen:
        'Review presence is documented and a real-customer review plan exists if the profile is thin.',
    };
  }

  function gbpPhotosGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Photos help prospects recognize ' +
        name +
        ' as a real operating business. Empty or stock-only GBP galleries reduce trust when someone checks the listing after outreach.',
      whatToDo:
        'Confirm GBP has current exterior, team, and work photos. If missing, collect client-approved photos and document an upload plan — do not publish without approval.',
      whatToConfirm: [
        'Exterior and/or team photos are present or queued for upload.',
        'Work photos represent real jobs ' + name + ' has permission to show.',
        'Photos are not misleading stock imagery presented as the business.',
        'No GBP photo uploads happen without explicit approval.',
      ],
      whoOwnsIt: 'Client/operator',
      owner: 'Client/operator',
      completeWhen:
        'GBP photos are present or a client-approved photo set is ready to upload.',
    };
  }

  function reviewRequestProcessGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Reviews will not grow on their own. ' +
        name +
        ' needs a simple post-job ask so satisfied customers know how to leave a Google review without awkward or non-compliant prompting.',
      whatToDo:
        'Define when and how the team asks for a Google review after a completed job (who asks, which link, and what language to use). Keep it optional and authentic.',
      whatToConfirm: [
        'A post-job trigger is defined (e.g. after final walkthrough or invoice).',
        'The Google review link or GBP short link is known to the team.',
        'Ask language is short, optional, and does not filter for positive-only reviews.',
        'Someone owns sending the ask.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        name + ' has a documented, owner-assigned review request process.',
    };
  }

  function crmExistsGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'If inquiries live only in inboxes or texts, ' +
        name +
        ' will lose follow-ups as volume grows. A CRM (Pulseforge or another system) is the system of record before outreach scales.',
      whatToDo:
        'Confirm which CRM or lead tracker will hold every new inquiry. If none exists, choose Pulseforge or another approved tool and name who will use it daily.',
      whatToConfirm: [
        'A CRM or equivalent lead tracker is selected.',
        'The owner/operator can access it (no passwords shared in Max).',
        'New inquiries will be logged there by default.',
        'The team agrees this is the source of truth for open leads.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        name + ' has a named CRM/lead tracker and a person responsible for using it.',
    };
  }

  function contactsCapturedGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Every missed log is a missed follow-up. ' +
        name +
        ' should capture every inquiry as a contact so outreach-driven demand does not disappear into voicemail or email threads.',
      whatToDo:
        'Agree that every phone, form, email, and referral inquiry is logged as a contact the same day, with name, channel, and next step.',
      whatToConfirm: [
        'There is a rule: no inquiry stays only in a personal inbox.',
        'Minimum fields are defined (name, company, channel, next step).',
        'Someone checks daily that new inquiries were logged.',
        'DNC or unsubscribe requests can be marked on the contact.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        'Every inquiry path has a same-day contact-logging rule and an owner.',
    };
  }

  function stagesDefinedGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Without stages, ' +
        name +
        ' cannot see which leads need a first reply, estimate, walkthrough, or decision. Clear stages keep follow-up disciplined after outreach starts.',
      whatToDo:
        'Define a short pipeline (for example: new → contacted → estimate sent → walkthrough booked → won/lost) and make sure the CRM uses those stages.',
      whatToConfirm: [
        'Pipeline stages are written down and shared with the reply owner.',
        'Stages match how ' + name + ' actually sells (estimate/walkthrough).',
        'Every open lead has a current stage.',
        'Won/lost outcomes are represented.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        name + ' has a shared stage list in the CRM and uses it on open leads.',
    };
  }

  function followUpRemindersGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Property managers rarely book on the first touch. ' +
        name +
        ' needs reminders so promised follow-ups happen even when the day gets busy.',
      whatToDo:
        'Turn on or define follow-up reminders in the CRM for next calls, estimate nudges, and post-walkthrough check-ins. Assign who closes each reminder.',
      whatToConfirm: [
        'Reminders can be set on a contact or opportunity.',
        'The reply owner sees due reminders daily.',
        'Completed touches clear or reschedule the reminder.',
        'No lead sits without a next-touch date once contacted.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        'Follow-up reminders are in use and owned for open commercial leads.',
    };
  }

  function crmSourceTrackingGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Without source tracking, ' +
        name +
        ' cannot tell whether a lead came from outreach, Google, referral, or the website. Source data is required before scaling channels.',
      whatToDo:
        'Add a required lead-source field (or equivalent) on every new contact and agree on a short source list (e.g. outreach, website form, Google, referral, other).',
      whatToConfirm: [
        'Lead source is captured on every new contact.',
        'Source values are consistent enough to report later.',
        'Website/form sources can be distinguished from outbound outreach.',
        'No tracking tool changes are made without approval.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        'Every new contact records a lead source using an agreed list.',
    };
  }

  function formTrackingGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'If form submits are not tracked, ' +
        name +
        ' cannot see whether website or campaign traffic produces inquiries. Form conversion tracking is part of capture readiness.',
      whatToDo:
        'Confirm form submissions fire a conversion event or CRM/create notification that can be counted. If missing, document the GA4/event or CRM wiring needed for approval.',
      whatToConfirm: [
        'A successful form submit is countable (analytics event and/or CRM create).',
        'Test submissions appear in the tracking path.',
        'Spam submits can be filtered or ignored in reporting.',
        'No tracking or website changes are made without approval.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        'Form submits are tracked as conversions or the approved setup steps are documented.',
    };
  }

  function googleAnalyticsGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Google Analytics (GA4) shows whether ' +
        name +
        "'s site receives traffic and which pages matter. Without it, growth work is flying blind after outreach or local search interest.",
      whatToDo:
        'Confirm GA4 is installed on the live site and that an operator/client admin can access the property. Do not install or change tags without approval.',
      whatToConfirm: [
        'GA4 is present on key site pages (or absence is documented).',
        'Admin access path is known (no password sharing in Max).',
        'Realtime or recent hits confirm the tag fires.',
        'No tracking installs happen without explicit approval.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        'GA4 is confirmed on the live site with a known access path, or install steps are documented for approval.',
    };
  }

  function searchConsoleGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Search Console shows how Google sees ' +
        name +
        "'s domain and which queries/pages get impressions. It is the baseline for organic visibility before content or local SEO work.",
      whatToDo:
        'Confirm the domain (or URL prefix) is verified in Google Search Console and that the right people can view it. Document verification steps for approval if missing.',
      whatToConfirm: [
        'Search Console property exists for the domain or site URL.',
        'Verification method is known to the owner/operator.',
        'Coverage/performance data can be viewed (or property is newly verified).',
        'No DNS or site-file verification changes are made without approval.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        'Search Console is verified with a known access path, or verification steps are documented for approval.',
    };
  }

  function callTrackingGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Phone is often the fastest path for commercial cleaning inquiries. ' +
        name +
        ' needs either call tracking or at least consistent call logging so outreach and website calls are not invisible.',
      whatToDo:
        'Decide whether to use a call-tracking number or simple CRM call logging. Confirm every inbound sales call is recorded as a touch with source when known. Do not swap public numbers without approval.',
      whatToConfirm: [
        'Inbound sales calls are logged somewhere durable.',
        'If call tracking is used, numbers map to the right campaigns/pages.',
        'The public phone number remains monitored during any transition.',
        'No phone or tracking changes go live without approval.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        'Call tracking or reliable call logging is in place for sales inquiries.',
    };
  }

  function conversionEventsGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Traffic without conversion events cannot tell ' +
        name +
        ' which pages or campaigns produce estimates, calls, or form fills. Key events are the measurement layer for growth.',
      whatToDo:
        'Define 2–4 key conversion events (e.g. form_submit, click_to_call, estimate_request) and confirm they fire — or document the approved implementation steps.',
      whatToConfirm: [
        'Key conversion events are named and agreed.',
        'At least the primary form or CTA path has an event.',
        'Test actions show up in analytics or the tag assistant equivalent.',
        'No tracking changes are made without approval.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        'Key conversion events are defined and firing, or implementation is documented for approval.',
    };
  }

  function estimateProcessGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'When outreach creates interest, ' +
        name +
        ' must know how estimates are scoped and delivered. An unclear estimate process slows replies and loses property manager deals.',
      whatToDo:
        'Document how estimates are scoped today (walkthrough vs remote, pricing inputs, who prepares them, and typical turnaround). Note gaps like missing proposal template separately.',
      whatToConfirm: [
        'Estimate steps are written down in plain language.',
        'Pricing inputs are listed (e.g. sq ft, frequency, scope).',
        'Someone is responsible for preparing estimates.',
        'Typical response time for an estimate request is known.',
      ],
      whoOwnsIt: 'Client/operator',
      owner: 'Client/operator',
      completeWhen:
        name +
        "'s estimate/scoping process is documented with an owner and turnaround expectation.",
    };
  }

  function followUpCadenceGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Estimates and walkthroughs often need a second and third touch. A written follow-up cadence keeps ' +
        name +
        ' consistent without sounding spammy.',
      whatToDo:
        'Set a simple cadence after estimate send or walkthrough (e.g. day 2 email/call, day 5 nudge, day 10 final check-in) and assign who runs it.',
      whatToConfirm: [
        'Cadence steps and timing are written down.',
        'The cadence stops on win, loss, or unsubscribe/DNC.',
        'Reminders exist for each step.',
        'Language stays helpful and specific to the estimate/walkthrough.',
      ],
      whoOwnsIt: 'Operator guided',
      owner: 'Operator guided',
      completeWhen:
        'A post-estimate follow-up cadence is documented and reminder-ready.',
    };
  }

  function brandAssetsReadyGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Outreach, the website, and GBP all need consistent brand assets. Missing logo or real photos makes ' +
        name +
        ' look unfinished when a prospect checks credentials.',
      whatToDo:
        'Confirm a usable logo file and a small set of real work/team/location photos are available for site, GBP, and outreach. Collect gaps from the client without requesting passwords.',
      whatToConfirm: [
        'A logo file suitable for web/email exists.',
        'At least a few real photos are available (not only stock).',
        'Assets may be used on the website and Google profile with approval.',
        'No public asset changes are published without approval.',
      ],
      whoOwnsIt: 'Client/operator',
      owner: 'Client/operator',
      completeWhen:
        name + ' has logo and core photo assets ready for approved use.',
    };
  }

  function socialProfilesPresentGuidance(businessName) {
    const name = shortBusinessName(businessName);
    return {
      whyThisMatters:
        'Prospects often check Facebook or Instagram after an email. Missing or unclaimed profiles create doubt about whether ' +
        name +
        ' is an active local business.',
      whatToDo:
        'Confirm which social profiles exist and are claimed. Note gaps for Facebook/Instagram (and LinkedIn if B2B-relevant). Do not create or change profiles without approval.',
      whatToConfirm: [
        'Existing profile URLs are documented.',
        'Name, logo, and contact info are consistent where profiles exist.',
        'Missing priority profiles are listed for client/operator decision.',
        'No social account creation or edits happen without approval.',
        'No passwords are shared in Max.',
      ],
      whoOwnsIt: 'Max can check; operator approves fixes',
      owner: 'Max can check; operator approves fixes',
      completeWhen:
        'Priority social profiles are confirmed present/claimed, or intentional gaps are documented for approval.',
    };
  }

  const TASK_GUIDANCE_BY_ID = Object.freeze({
    branded_email: brandedEmailGuidance,
    domain_connected: domainConnectedGuidance,
    domain_owned: domainOwnedGuidance,
    spf_dkim_dmarc: spfDkimDmarcGuidance,
    clear_cta: clearCtaGuidance,
    clear_service_area: clearServiceAreaGuidance,
    clear_services: clearServicesGuidance,
    contact_form_works: contactFormWorksGuidance,
    contact_forms: contactFormsGuidance,
    mobile_usability: mobileUsabilityGuidance,
    phone_email_visible: phoneEmailVisibleGuidance,
    gbp_claimed: gbpClaimedGuidance,
    gbp_nap: gbpNapGuidance,
    gbp_contact: gbpContactGuidance,
    gbp_reviews: gbpReviewsGuidance,
    gbp_photos: gbpPhotosGuidance,
    review_request_process: reviewRequestProcessGuidance,
    crm_exists: crmExistsGuidance,
    contacts_captured: contactsCapturedGuidance,
    stages_defined: stagesDefinedGuidance,
    follow_up_reminders: followUpRemindersGuidance,
    crm_source_tracking: crmSourceTrackingGuidance,
    form_tracking: formTrackingGuidance,
    google_analytics: googleAnalyticsGuidance,
    search_console: searchConsoleGuidance,
    call_tracking: callTrackingGuidance,
    conversion_events: conversionEventsGuidance,
    estimate_process: estimateProcessGuidance,
    follow_up_cadence: followUpCadenceGuidance,
    brand_assets_ready: brandAssetsReadyGuidance,
    social_profiles_present: socialProfilesPresentGuidance,
    // Related brand/social itemIds share practical guidance.
    photos: brandAssetsReadyGuidance,
    logo: brandAssetsReadyGuidance,
    facebook_present: socialProfilesPresentGuidance,
    instagram_present: socialProfilesPresentGuidance,
    linkedin_present: socialProfilesPresentGuidance,
  });

  function normalizeGuidanceKey(raw) {
    const key = String(raw || '').trim();
    if (!key) return '';
    if (TASK_GUIDANCE_ALIASES[key]) return TASK_GUIDANCE_ALIASES[key];
    if (TASK_GUIDANCE_BY_ID[key]) return key;
    return key;
  }

  function resolveGuidanceKey(task) {
    if (!task) return '';
    const itemId = normalizeGuidanceKey(task.itemId || '');
    if (itemId && TASK_GUIDANCE_BY_ID[itemId]) return itemId;
    const id = String(task.id || '');
    const suffix = id.includes(':') ? id.split(':').pop() : id;
    const fromSuffix = normalizeGuidanceKey(suffix);
    if (fromSuffix && TASK_GUIDANCE_BY_ID[fromSuffix]) return fromSuffix;
    // Title-like slugs (e.g. branded_email_available) passed as id/itemId.
    const alias = normalizeGuidanceKey(itemId || suffix);
    if (alias && TASK_GUIDANCE_BY_ID[alias]) return alias;
    return itemId || fromSuffix || '';
  }

  function defaultSetupGuidance(task, businessName) {
    const name = shortBusinessName(businessName);
    const title = (task && task.title) || 'this setup item';
    const action =
      (task && (task.description || task.action || task.recommended_next_step)) ||
      ('Confirm ' + title + ' is in place.');
    return {
      whyThisMatters:
        'Before outreach, ' +
        name +
        ' needs reliable capture and follow-up. Completing “' +
        title +
        '” reduces the chance inquiries are missed.',
      whatToDo: action,
      whatToConfirm: [
        'The change is live or documented.',
        'The person who owns replies knows how this works.',
        'Nothing here requires a password share or unapproved DNS/GBP change.',
      ],
      whoOwnsIt: formatOwnerLabel((task && task.owner) || 'operator_guided'),
      owner: formatOwnerLabel((task && task.owner) || 'operator_guided'),
      completeWhen: title + ' is confirmed and ready for outreach.',
    };
  }

  function resolveTaskGuidance(task, businessName) {
    if (!task) return null;
    if (task.guidance && typeof task.guidance === 'object') {
      const who =
        task.guidance.whoOwnsIt ||
        task.guidance.owner ||
        formatOwnerLabel(task.owner || 'operator_guided');
      return {
        whyThisMatters: task.guidance.whyThisMatters || '',
        whatToDo: task.guidance.whatToDo || '',
        whatToConfirm: Array.isArray(task.guidance.whatToConfirm)
          ? task.guidance.whatToConfirm
          : [],
        whoOwnsIt: who,
        owner: task.guidance.owner || who,
        completeWhen: task.guidance.completeWhen || '',
      };
    }
    const key = resolveGuidanceKey(task);
    const builder = key && TASK_GUIDANCE_BY_ID[key];
    if (typeof builder === 'function') {
      return builder(businessName);
    }
    if (task.type === 'setup' || task.itemId || /^setup:/.test(String(task.id || ''))) {
      return defaultSetupGuidance(task, businessName);
    }
    return defaultSetupGuidance(task, businessName);
  }

  function hasKnownTaskGuidance(task) {
    const key = resolveGuidanceKey(task);
    return Boolean(key && TASK_GUIDANCE_BY_ID[key]);
  }

  function planCardHtml(session, { primary, guidanceOpen } = {}) {
    if (!session) return '';
    const id = sessionIdOf(session);
    const sample = session.isSample
      ? '<span class="sample-badge">Sample / Dev</span>'
      : '';
    const plan = session.growthPlan || null;
    const pct =
      plan && typeof plan.percentComplete === 'number'
        ? plan.percentComplete
        : null;
    const done =
      session.resumeTarget === 'growth_complete' ||
      (plan && plan.status === 'complete');
    // When guidance is open, do not repeat the active task title on the plan card.
    const currentTitle =
      guidanceOpen
        ? null
        : plan && plan.currentTask && plan.currentTask.title
          ? plan.currentTask.title
          : done
            ? 'Growth Plan complete'
            : 'Ready to resume';
    const meta = [
      pct != null ? pct + '% complete' : null,
      primary
        ? currentTitle || (guidanceOpen ? 'Guidance open' : null)
        : done
          ? 'Completed plan'
          : 'Previous plan',
      session.blueprintVersion ? 'Blueprint v' + session.blueprintVersion : null,
    ]
      .filter(Boolean)
      .join(' · ');
    const cta = done ? 'View Completion' : 'Resume Growth Plan';
    const title = primary
      ? (session.businessName || 'Business') + ' · Growth Plan'
      : session.label ||
        (session.businessName || 'Business') + ' · Previous plan';

    return (
      '<article class="session-card' +
      (session.isSample ? ' sample' : '') +
      (primary ? ' current-plan-card' : ' previous-plan-card') +
      '" data-session-id="' +
      escapeHtml(id) +
      '" data-plan-role="' +
      (primary ? 'current' : 'previous') +
      '" data-simple-task-card="0">' +
      '<p class="session-card-title">' +
      escapeHtml(title) +
      sample +
      '</p>' +
      '<p class="session-card-meta">' +
      escapeHtml(meta) +
      '</p>' +
      '<div class="session-card-actions">' +
      '<button type="button" data-action="continue" data-session-id="' +
      escapeHtml(id) +
      '">' +
      escapeHtml(cta) +
      '</button>' +
      '<button type="button" class="secondary" data-action="view" data-session-id="' +
      escapeHtml(id) +
      '">View Blueprint</button>' +
      (primary
        ? '<button type="button" class="secondary" data-action="new">Start New Interview</button>'
        : '') +
      '</div>' +
      '</article>'
    );
  }

  function listHtml(items) {
    const list = Array.isArray(items) ? items.filter(Boolean) : [];
    if (!list.length) return '';
    return (
      '<ul class="task-guidance-list">' +
      list.map((item) => '<li>' + escapeHtml(item) + '</li>').join('') +
      '</ul>'
    );
  }

  /**
   * Exactly one expanded guidance card for the active setup task.
   * Must never be nested under Previous Plans or duplicated by a simple card.
   */
  function taskGuidanceCardHtml(task, opts) {
    if (!task) return '';
    const options = opts && typeof opts === 'object' ? opts : {};
    const businessName =
      options.businessName ||
      task.businessName ||
      (options.currentSession && options.currentSession.businessName) ||
      '';
    const ownerLabel = formatOwnerLabel(task.owner || 'operator_guided');
    const guidance = resolveTaskGuidance(task, businessName) || {};
    const mins = task.estimatedMinutes
      ? escapeHtml(String(task.estimatedMinutes)) + ' minutes'
      : '';

    return (
      '<article class="current-task-card task-guidance-card" id="taskGuidanceCard" data-task-id="' +
      escapeHtml(task.id || '') +
      '" data-role="task-guidance" data-active-task-card="1" data-simple-task-card="0">' +
      '<p class="kicker">Current Task Guidance</p>' +
      '<h3>' +
      escapeHtml(task.title || 'Next step') +
      '</h3>' +
      '<p class="task-meta">' +
      escapeHtml(
        task.description ||
          'Complete this recommendation, then mark it done to advance.'
      ) +
      (mins ? '<br>Estimated time · ' + mins : '') +
      '<br>Owner · ' +
      escapeHtml(ownerLabel) +
      (task.priority ? '<br>Priority · ' + escapeHtml(String(task.priority)) : '') +
      '</p>' +
      '<div class="task-guidance-body">' +
      '<section class="task-guidance-block" data-block="why">' +
      '<h4>Why this matters</h4>' +
      '<p>' +
      escapeHtml(guidance.whyThisMatters || '') +
      '</p>' +
      '</section>' +
      '<section class="task-guidance-block" data-block="do">' +
      '<h4>What to do</h4>' +
      '<p>' +
      escapeHtml(guidance.whatToDo || '') +
      '</p>' +
      '</section>' +
      '<section class="task-guidance-block" data-block="confirm">' +
      '<h4>What to confirm</h4>' +
      listHtml(guidance.whatToConfirm) +
      '</section>' +
      '<section class="task-guidance-block" data-block="owner">' +
      '<h4>Who owns it</h4>' +
      '<p>' +
      escapeHtml(guidance.whoOwnsIt || ownerLabel) +
      '</p>' +
      '</section>' +
      '<section class="task-guidance-block" data-block="complete">' +
      '<h4>Complete when</h4>' +
      '<p>' +
      escapeHtml(guidance.completeWhen || '') +
      '</p>' +
      '</section>' +
      '</div>' +
      '</article>'
    );
  }

  /**
   * Left panel for full-screen Growth Workspace.
   *
   * Sections:
   * 1. Current Growth Plan
   * 2. Current Task Guidance (only when guidanceOpen) — exactly one card
   * 3. Previous Plans (historical only; omitted when empty)
   */
  function renderGrowthWorkspaceLeftPanel(opts) {
    const options = opts && typeof opts === 'object' ? opts : {};
    const currentSession = options.currentSession || null;
    const currentSessionId =
      options.currentSessionId || sessionIdOf(currentSession);
    const previousSessions = filterPreviousPlans(
      options.previousSessions != null
        ? options.previousSessions
        : options.sessions,
      currentSessionId
    );
    const task = options.currentTask || null;
    const guidanceOpen = Boolean(options.guidanceOpen) && Boolean(task);
    const collapsePrevious = options.collapsePrevious !== false;
    const businessName =
      options.businessName ||
      (currentSession && currentSession.businessName) ||
      '';

    let html =
      '<section class="gw-left-section" data-section="current-plan">' +
      '<p class="blueprint-empty gw-section-label" style="margin:0 0 0.25rem">Current Growth Plan</p>' +
      (currentSession
        ? planCardHtml(currentSession, { primary: true, guidanceOpen })
        : '<p class="blueprint-empty">No active Growth Plan.</p>') +
      '</section>';

    if (guidanceOpen) {
      // Single structured card only — no section label duplicate, no simple card.
      html +=
        '<section class="gw-left-section" data-section="task-guidance">' +
        taskGuidanceCardHtml(task, { businessName, currentSession }) +
        '</section>';
    }

    if (previousSessions.length) {
      const body =
        '<div class="session-list previous-plan-list">' +
        previousSessions
          .map((s) => planCardHtml(s, { primary: false }))
          .join('') +
        '</div>';
      if (collapsePrevious) {
        html +=
          '<details class="previous-plans" data-section="previous-plans">' +
          '<summary>Previous Plans (' +
          previousSessions.length +
          ')</summary>' +
          body +
          '</details>';
      } else {
        html +=
          '<section class="gw-left-section previous-plans" data-section="previous-plans">' +
          '<p class="blueprint-empty gw-section-label" style="margin:0.85rem 0 0.25rem">Previous Plans (' +
          previousSessions.length +
          ')</p>' +
          body +
          '</section>';
      }
    }

    // Hard guarantee: left panel never includes compact/simple active-task cards.
    let out = html.replace(
      /<(?:div|article)\b[^>]*data-role="simple-task"[^>]*>[\s\S]*?<\/(?:div|article)>/gi,
      ''
    );
    if (guidanceOpen) {
      // In-flow spacer so Who owns it / Complete when clear the sticky footer.
      out +=
        '<div class="gw-guidance-scroll-spacer" data-role="guidance-scroll-spacer" aria-hidden="true"></div>';
    }
    return out;
  }

  function countMarkers(html, marker) {
    if (!html) return 0;
    let count = 0;
    let idx = 0;
    while (true) {
      const next = html.indexOf(marker, idx);
      if (next === -1) break;
      count += 1;
      idx = next + marker.length;
    }
    return count;
  }

  function analyzeLeftPanelHtml(html) {
    const source = String(html || '');
    const previousIdx = source.indexOf('data-section="previous-plans"');
    const previousHtml = previousIdx >= 0 ? source.slice(previousIdx) : '';
    const guidanceSectionMatch = source.match(
      /data-section="task-guidance"[\s\S]*?(?=<section class="gw-left-section"|<details class="previous-plans"|$)/
    );
    const guidanceSection = guidanceSectionMatch ? guidanceSectionMatch[0] : '';
    return {
      currentPlanCards: countMarkers(source, 'data-plan-role="current"'),
      previousPlanCards: countMarkers(source, 'data-plan-role="previous"'),
      taskGuidanceCards: countMarkers(source, 'data-role="task-guidance"'),
      activeTaskCards: countMarkers(source, 'data-active-task-card="1"'),
      simpleTaskCards: countMarkers(source, 'data-role="simple-task"'),
      taskGuidanceInPreviousPlans: countMarkers(
        previousHtml,
        'data-role="task-guidance"'
      ),
      guidanceSectionSimpleCards: countMarkers(
        guidanceSection,
        'data-role="simple-task"'
      ),
      hasPreviousPlansSection: previousIdx >= 0,
      rawOwnerLeaks: Boolean(
        /\bclient_required\b|\boperator_guided\b|\bmax_can_check\b/.test(source)
      ),
    };
  }

  return {
    OWNER_LABELS,
    TASK_GUIDANCE_ALIASES,
    KNOWN_GROWTH_INFRA_GUIDANCE_IDS,
    TASK_GUIDANCE_BY_ID,
    escapeHtml,
    formatOwnerLabel,
    shortBusinessName,
    sessionIdOf,
    filterPreviousPlans,
    planCardHtml,
    resolveGuidanceKey,
    resolveTaskGuidance,
    hasKnownTaskGuidance,
    defaultSetupGuidance,
    brandedEmailGuidance,
    domainConnectedGuidance,
    domainOwnedGuidance,
    spfDkimDmarcGuidance,
    clearCtaGuidance,
    clearServiceAreaGuidance,
    clearServicesGuidance,
    contactFormWorksGuidance,
    contactFormsGuidance,
    mobileUsabilityGuidance,
    phoneEmailVisibleGuidance,
    gbpClaimedGuidance,
    gbpNapGuidance,
    gbpContactGuidance,
    gbpReviewsGuidance,
    gbpPhotosGuidance,
    reviewRequestProcessGuidance,
    crmExistsGuidance,
    contactsCapturedGuidance,
    stagesDefinedGuidance,
    followUpRemindersGuidance,
    crmSourceTrackingGuidance,
    formTrackingGuidance,
    googleAnalyticsGuidance,
    searchConsoleGuidance,
    callTrackingGuidance,
    conversionEventsGuidance,
    estimateProcessGuidance,
    followUpCadenceGuidance,
    brandAssetsReadyGuidance,
    socialProfilesPresentGuidance,
    taskGuidanceCardHtml,
    renderGrowthWorkspaceLeftPanel,
    analyzeLeftPanelHtml,
  };
});
