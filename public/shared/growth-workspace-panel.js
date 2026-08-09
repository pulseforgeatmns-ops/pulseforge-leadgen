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
   * Structured setup-task guidance. Keys are itemId (preferred) or full task id.
   */
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
      completeWhen:
        'A test form submission is received successfully and the follow-up owner is clear.',
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
      completeWhen:
        'Phone and email are easy to find, accurate, and monitored.',
    };
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
      completeWhen: title + ' is confirmed and ready for outreach.',
    };
  }

  function resolveTaskGuidance(task, businessName) {
    if (!task) return null;
    if (task.guidance && typeof task.guidance === 'object') {
      return {
        whyThisMatters: task.guidance.whyThisMatters || '',
        whatToDo: task.guidance.whatToDo || '',
        whatToConfirm: Array.isArray(task.guidance.whatToConfirm)
          ? task.guidance.whatToConfirm
          : [],
        whoOwnsIt:
          task.guidance.whoOwnsIt ||
          formatOwnerLabel(task.owner || 'operator_guided'),
        completeWhen: task.guidance.completeWhen || '',
      };
    }
    const itemId = task.itemId || '';
    const id = String(task.id || '');
    if (itemId === 'branded_email' || /:branded_email$/.test(id)) {
      return brandedEmailGuidance(businessName);
    }
    if (itemId === 'domain_connected' || /:domain_connected$/.test(id)) {
      return domainConnectedGuidance(businessName);
    }
    if (itemId === 'domain_owned' || /:domain_owned$/.test(id)) {
      return domainOwnedGuidance(businessName);
    }
    if (itemId === 'spf_dkim_dmarc' || /:spf_dkim_dmarc$/.test(id)) {
      return spfDkimDmarcGuidance(businessName);
    }
    if (itemId === 'clear_cta' || /:clear_cta$/.test(id)) {
      return clearCtaGuidance(businessName);
    }
    if (itemId === 'clear_service_area' || /:clear_service_area$/.test(id)) {
      return clearServiceAreaGuidance(businessName);
    }
    if (itemId === 'clear_services' || /:clear_services$/.test(id)) {
      return clearServicesGuidance(businessName);
    }
    if (itemId === 'contact_form_works' || /:contact_form_works$/.test(id)) {
      return contactFormWorksGuidance(businessName);
    }
    if (itemId === 'phone_email_visible' || /:phone_email_visible$/.test(id)) {
      return phoneEmailVisibleGuidance(businessName);
    }
    if (task.type === 'setup' || itemId || /^setup:/.test(id)) {
      return defaultSetupGuidance(task, businessName);
    }
    return defaultSetupGuidance(task, businessName);
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
    escapeHtml,
    formatOwnerLabel,
    shortBusinessName,
    sessionIdOf,
    filterPreviousPlans,
    planCardHtml,
    resolveTaskGuidance,
    brandedEmailGuidance,
    domainConnectedGuidance,
    domainOwnedGuidance,
    spfDkimDmarcGuidance,
    clearCtaGuidance,
    clearServiceAreaGuidance,
    clearServicesGuidance,
    contactFormWorksGuidance,
    phoneEmailVisibleGuidance,
    taskGuidanceCardHtml,
    renderGrowthWorkspaceLeftPanel,
    analyzeLeftPanelHtml,
  };
});
