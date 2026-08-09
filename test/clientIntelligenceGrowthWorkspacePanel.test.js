'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  formatOwnerLabel,
  renderGrowthWorkspaceLeftPanel,
  analyzeLeftPanelHtml,
  taskGuidanceCardHtml,
  resolveTaskGuidance,
} = require('../public/shared/growth-workspace-panel');

const BRANDED_EMAIL_TASK = Object.freeze({
  id: 'setup:domain_dns:branded_email',
  itemId: 'branded_email',
  type: 'setup',
  title: 'Branded email available',
  description: 'Create a branded mailbox (e.g. hello@domain).',
  owner: 'client_required',
  priority: 'high',
  estimatedMinutes: 3,
  resumeAction: 'setup_task',
});

const DOMAIN_CONNECTED_TASK = Object.freeze({
  id: 'setup:domain_dns:domain_connected',
  itemId: 'domain_connected',
  type: 'setup',
  title: 'Domain connected to website',
  description: 'Point domain A/CNAME to the live site.',
  owner: 'max_can_check',
  priority: 'high',
  estimatedMinutes: 3,
  resumeAction: 'setup_task',
});

const DOMAIN_OWNED_TASK = Object.freeze({
  id: 'setup:domain_dns:domain_owned',
  itemId: 'domain_owned',
  type: 'setup',
  title: 'Domain owned',
  description: 'Confirm domain registrar ownership.',
  owner: 'client_required',
  priority: 'high',
  estimatedMinutes: 3,
  resumeAction: 'setup_task',
});

const SPF_DKIM_DMARC_TASK = Object.freeze({
  id: 'setup:domain_dns:spf_dkim_dmarc',
  itemId: 'spf_dkim_dmarc',
  type: 'setup',
  title: 'SPF/DKIM/DMARC present',
  description: 'Add email authentication records at the DNS provider.',
  owner: 'operator_guided',
  priority: 'high',
  estimatedMinutes: 3,
  resumeAction: 'setup_task',
});

const CLEAR_CTA_TASK = Object.freeze({
  id: 'setup:website:clear_cta',
  itemId: 'clear_cta',
  type: 'setup',
  title: 'Clear CTA',
  description: 'Add one primary call-to-action (call / form / book).',
  owner: 'operator_guided',
  priority: 'high',
  estimatedMinutes: 3,
  resumeAction: 'setup_task',
});

const CLEAR_SERVICE_AREA_TASK = Object.freeze({
  id: 'setup:website:clear_service_area',
  itemId: 'clear_service_area',
  type: 'setup',
  title: 'Clear service area',
  description: 'State cities/markets served.',
  owner: 'operator_guided',
  priority: 'high',
  estimatedMinutes: 3,
  resumeAction: 'setup_task',
});

const CLEAR_SERVICES_TASK = Object.freeze({
  id: 'setup:website:clear_services',
  itemId: 'clear_services',
  type: 'setup',
  title: 'Clear services',
  description: 'List primary services above the fold.',
  owner: 'operator_guided',
  priority: 'high',
  estimatedMinutes: 3,
  resumeAction: 'setup_task',
});

const CONTACT_FORM_WORKS_TASK = Object.freeze({
  id: 'setup:website:contact_form_works',
  itemId: 'contact_form_works',
  type: 'setup',
  title: 'Contact form works',
  description: 'Test form delivery end-to-end.',
  owner: 'operator_guided',
  priority: 'high',
  estimatedMinutes: 3,
  resumeAction: 'setup_task',
});

describe('Growth Workspace left panel', () => {
  it('formats readiness owner labels for operators', () => {
    assert.equal(formatOwnerLabel('client_required'), 'Client/operator');
    assert.equal(formatOwnerLabel('operator_guided'), 'Operator guided');
    assert.equal(formatOwnerLabel('max_can_check'), 'Max can check');
  });

  it('given one active task and no previous plans, renders one plan card and no Previous Plans', () => {
    const currentTask = { ...BRANDED_EMAIL_TASK };
    const currentSession = {
      sessionId: 'sess-active',
      businessName: 'Anchor Cleaning',
      resumeTarget: 'growth_workspace',
      growthPlan: {
        percentComplete: 40,
        status: 'in_progress',
        currentTask,
      },
    };

    const closed = renderGrowthWorkspaceLeftPanel({
      currentSession,
      previousSessions: [],
      currentTask,
      guidanceOpen: false,
      businessName: 'Anchor Cleaning',
    });
    const closedStats = analyzeLeftPanelHtml(closed);
    assert.equal(closedStats.currentPlanCards, 1);
    assert.equal(closedStats.taskGuidanceCards, 0);
    assert.equal(closedStats.simpleTaskCards, 0);
    assert.equal(closedStats.hasPreviousPlansSection, false);
    assert.equal(closedStats.taskGuidanceInPreviousPlans, 0);

    const opened = renderGrowthWorkspaceLeftPanel({
      currentSession,
      previousSessions: [],
      currentTask,
      guidanceOpen: true,
      businessName: 'Anchor Cleaning',
    });
    const openedStats = analyzeLeftPanelHtml(opened);
    assert.equal(openedStats.currentPlanCards, 1);
    assert.equal(openedStats.taskGuidanceCards, 1);
    assert.equal(openedStats.activeTaskCards, 1);
    assert.equal(openedStats.simpleTaskCards, 0);
    assert.equal(openedStats.guidanceSectionSimpleCards, 0);
    assert.equal(openedStats.hasPreviousPlansSection, false);
    assert.equal(openedStats.taskGuidanceInPreviousPlans, 0);
    assert.equal(openedStats.rawOwnerLeaks, false);
    assert.match(opened, /Client\/operator/);
    assert.doesNotMatch(opened, /client_required/);
    assert.equal(
      (opened.match(/data-role="task-guidance"/g) || []).length,
      1,
      'exactly one expanded task guidance card after opening guidance'
    );
    assert.equal(
      (opened.match(/Current Task Guidance/g) || []).length,
      1,
      'Current Task Guidance label appears once'
    );
    // Plan card should not repeat the task title while guidance is open.
    assert.doesNotMatch(
      opened.slice(0, opened.indexOf('data-section="task-guidance"')),
      /Branded email available/
    );
  });

  it('Open Task Guidance yields exactly one guidance card and no simple task cards', () => {
    const currentTask = { ...BRANDED_EMAIL_TASK };
    const currentSession = {
      sessionId: 'sess-active',
      businessName: 'Anchor Cleaning',
      growthPlan: {
        percentComplete: 40,
        status: 'in_progress',
        currentTask,
      },
    };

    // Simulate clicking Open Task Guidance twice — renderer stays idempotent.
    const first = renderGrowthWorkspaceLeftPanel({
      currentSession,
      previousSessions: [],
      currentTask,
      guidanceOpen: true,
      businessName: 'Anchor Cleaning',
    });
    const second = renderGrowthWorkspaceLeftPanel({
      currentSession,
      previousSessions: [],
      currentTask,
      guidanceOpen: true,
      businessName: 'Anchor Cleaning',
    });

    for (const html of [first, second]) {
      const stats = analyzeLeftPanelHtml(html);
      assert.equal(stats.taskGuidanceCards, 1);
      assert.equal(stats.activeTaskCards, 1, 'exactly one visible active-task card');
      assert.equal(stats.simpleTaskCards, 0);
      assert.equal(stats.guidanceSectionSimpleCards, 0);
      assert.doesNotMatch(html, /data-role="simple-task"/);
      assert.equal((html.match(/id="taskGuidanceCard"/g) || []).length, 1);
      assert.equal((html.match(/data-role="task-guidance"/g) || []).length, 1);
      assert.equal((html.match(/data-active-task-card="1"/g) || []).length, 1);
    }
  });

  it('expanded branded email guidance includes full structured sections', () => {
    const html = taskGuidanceCardHtml(
      { ...BRANDED_EMAIL_TASK },
      { businessName: 'Anchor Cleaning' }
    );
    assert.equal(analyzeLeftPanelHtml(html).taskGuidanceCards, 1);
    assert.match(html, /Branded email available/);
    assert.match(html, /Estimated time · 3 minutes/);
    assert.match(html, /Owner · Client\/operator/);
    assert.match(html, /Why this matters/);
    assert.match(
      html,
      /Before outreach, Anchor should look legitimate and be easy to reply to/
    );
    assert.match(html, /What to do/);
    assert.match(html, /hello@domain or estimates@domain/);
    assert.match(html, /What to confirm/);
    assert.match(html, /mailbox can send and receive email/);
    assert.match(html, /SPF, DKIM, and DMARC/);
    assert.match(html, /Who owns it/);
    assert.match(html, /Complete when/);
    assert.match(
      html,
      /Anchor has a working branded mailbox and someone is responsible for checking it/
    );

    const guidance = resolveTaskGuidance(BRANDED_EMAIL_TASK, 'Anchor Cleaning');
    assert.equal(
      guidance.whyThisMatters,
      'Before outreach, Anchor should look legitimate and be easy to reply to. A branded email helps property managers trust the business and keeps replies organized.'
    );
    assert.equal(
      guidance.whatToDo,
      'Create a mailbox such as hello@domain or estimates@domain.'
    );
    assert.deepEqual(guidance.whatToConfirm, [
      'The mailbox can send and receive email.',
      'Replies go to the person responsible for new opportunities.',
      'The email is connected to the website/contact form if applicable.',
      'SPF, DKIM, and DMARC should be checked before outbound outreach.',
    ]);
    assert.equal(
      guidance.completeWhen,
      'Anchor has a working branded mailbox and someone is responsible for checking it.'
    );
  });

  it('domain connected guidance is DNS/site checks, not email reply ownership', () => {
    const html = taskGuidanceCardHtml(
      { ...DOMAIN_CONNECTED_TASK },
      { businessName: 'Anchor Cleaning' }
    );
    assert.equal(analyzeLeftPanelHtml(html).taskGuidanceCards, 1);
    assert.match(html, /Domain connected to website/);
    assert.match(html, /Owner · Max can check/);
    assert.match(
      html,
      /Confirm the domain points to the live website\. If not, document the needed A\/CNAME change for approval\./
    );
    assert.match(html, /The domain loads the live website\./);
    assert.match(
      html,
      /Both www and non-www versions route correctly, or one redirects cleanly to the other\./
    );
    assert.match(html, /The site uses HTTPS\./);
    assert.match(
      html,
      /The domain shown in marketing materials matches the live site\./
    );
    assert.match(
      html,
      /No DNS or website changes are made without explicit approval\./
    );
    assert.match(html, /Max can check, operator\/client approves changes\./);
    assert.doesNotMatch(html, /The person who owns replies knows how this works/);

    const guidance = resolveTaskGuidance(
      DOMAIN_CONNECTED_TASK,
      'Anchor Cleaning'
    );
    assert.equal(
      guidance.whatToDo,
      'Confirm the domain points to the live website. If not, document the needed A/CNAME change for approval.'
    );
    assert.deepEqual(guidance.whatToConfirm, [
      'The domain loads the live website.',
      'Both www and non-www versions route correctly, or one redirects cleanly to the other.',
      'The site uses HTTPS.',
      'The domain shown in marketing materials matches the live site.',
      'No DNS or website changes are made without explicit approval.',
    ]);
    assert.equal(
      guidance.whoOwnsIt,
      'Max can check, operator/client approves changes.'
    );
    assert.doesNotMatch(
      guidance.whatToConfirm.join(' '),
      /owns replies/
    );
  });

  it('domain owned guidance covers registrar access, not lead-reply language', () => {
    const html = taskGuidanceCardHtml(
      { ...DOMAIN_OWNED_TASK },
      { businessName: 'Anchor Cleaning' }
    );
    assert.equal(analyzeLeftPanelHtml(html).taskGuidanceCards, 1);
    assert.match(html, /Domain owned/);
    assert.match(html, /Owner · Client\/operator/);
    assert.match(
      html,
      /Before outreach, Anchor needs to know who controls the domain/
    );
    assert.match(
      html,
      /Confirm which registrar or platform owns the domain, and who has access to manage it\./
    );
    assert.match(html, /The domain is registered and active\./);
    assert.match(html, /Anchor knows where the domain is managed\./);
    assert.match(
      html,
      /The owner\/operator knows who can approve DNS changes\./
    );
    assert.match(
      html,
      /The domain is not expired or at risk of renewal issues\./
    );
    assert.match(html, /No login credentials are shared inside Max\./);
    assert.match(html, /Domain ownership and access path are confirmed\./);
    assert.doesNotMatch(html, /The person who owns replies knows how this works/);

    const guidance = resolveTaskGuidance(DOMAIN_OWNED_TASK, 'Anchor Cleaning');
    assert.equal(
      guidance.whyThisMatters,
      'Before outreach, Anchor needs to know who controls the domain. Domain ownership is what lets the business connect the website, set up branded email, verify tools, and protect the brand.'
    );
    assert.equal(
      guidance.whatToDo,
      'Confirm which registrar or platform owns the domain, and who has access to manage it.'
    );
    assert.deepEqual(guidance.whatToConfirm, [
      'The domain is registered and active.',
      'Anchor knows where the domain is managed.',
      'The owner/operator knows who can approve DNS changes.',
      'The domain is not expired or at risk of renewal issues.',
      'No login credentials are shared inside Max.',
    ]);
    assert.equal(guidance.whoOwnsIt, 'Client/operator');
    assert.equal(
      guidance.completeWhen,
      'Domain ownership and access path are confirmed.'
    );
  });

  it('SPF/DKIM/DMARC guidance covers email auth, not generic capture copy', () => {
    const html = taskGuidanceCardHtml(
      { ...SPF_DKIM_DMARC_TASK },
      { businessName: 'Anchor Cleaning' }
    );
    assert.equal(analyzeLeftPanelHtml(html).taskGuidanceCards, 1);
    assert.match(html, /SPF\/DKIM\/DMARC present/);
    assert.match(html, /Owner · Operator guided/);
    assert.match(
      html,
      /Before any serious outbound outreach, Anchor&#39;s domain should be authenticated/
    );
    assert.match(
      html,
      /Check whether SPF, DKIM, and DMARC records exist for the sending domain/
    );
    assert.match(html, /SPF is present for the domain\./);
    assert.match(html, /DKIM is enabled for the email provider\./);
    assert.match(
      html,
      /DMARC is present, even if starting with a monitoring policy\./
    );
    assert.match(
      html,
      /The branded mailbox can send and receive successfully\./
    );
    assert.match(html, /No DNS changes are made without explicit approval\./);
    assert.match(
      html,
      /SPF, DKIM, and DMARC are confirmed or the required DNS changes are documented for approval\./
    );
    assert.doesNotMatch(html, /reliable capture and follow-up/);
    assert.doesNotMatch(html, /The person who owns replies knows how this works/);

    const guidance = resolveTaskGuidance(
      SPF_DKIM_DMARC_TASK,
      'Anchor Cleaning'
    );
    assert.equal(
      guidance.whyThisMatters,
      "Before any serious outbound outreach, Anchor's domain should be authenticated so emails are more likely to reach inboxes and less likely to look suspicious. SPF, DKIM, and DMARC help receiving mail systems trust that messages from Anchor are legitimate."
    );
    assert.equal(
      guidance.whatToDo,
      'Check whether SPF, DKIM, and DMARC records exist for the sending domain. If records are missing or incorrect, document the DNS changes needed for approval.'
    );
    assert.deepEqual(guidance.whatToConfirm, [
      'SPF is present for the domain.',
      'DKIM is enabled for the email provider.',
      'DMARC is present, even if starting with a monitoring policy.',
      'The branded mailbox can send and receive successfully.',
      'No DNS changes are made without explicit approval.',
    ]);
    assert.equal(guidance.whoOwnsIt, 'Operator guided');
    assert.equal(
      guidance.completeWhen,
      'SPF, DKIM, and DMARC are confirmed or the required DNS changes are documented for approval.'
    );
  });

  it('Clear CTA guidance covers commercial next-step copy, not generic capture language', () => {
    const html = taskGuidanceCardHtml(
      { ...CLEAR_CTA_TASK },
      { businessName: 'Anchor Cleaning' }
    );
    assert.equal(analyzeLeftPanelHtml(html).taskGuidanceCards, 1);
    assert.match(html, /Clear CTA/);
    assert.match(html, /Owner · Operator guided/);
    assert.match(
      html,
      /Before outreach, visitors should immediately know what action to take/
    );
    assert.match(
      html,
      /Anchor should have one obvious next step for commercial prospects/
    );
    assert.match(
      html,
      /Choose one primary CTA for the website and growth materials/
    );
    assert.match(
      html,
      /prefer estimate request or walkthrough request over vague language like &quot;learn more\.&quot;/
    );
    assert.match(html, /The primary CTA is visible on the website\./);
    assert.match(html, /The CTA matches the commercial growth goal\./);
    assert.match(
      html,
      /The CTA leads to a working form, phone number, email, or booking path\./
    );
    assert.match(
      html,
      /The CTA does not create confusion with multiple competing actions\./
    );
    assert.match(
      html,
      /No website changes are published without approval\./
    );
    assert.match(
      html,
      /Anchor has one clear primary CTA for commercial prospects, and the path behind it works\./
    );
    assert.doesNotMatch(html, /reliable capture and follow-up/);
    assert.doesNotMatch(html, /The person who owns replies knows how this works/);

    const guidance = resolveTaskGuidance(CLEAR_CTA_TASK, 'Anchor Cleaning');
    assert.equal(
      guidance.whyThisMatters,
      'Before outreach, visitors should immediately know what action to take. Anchor should have one obvious next step for commercial prospects, such as requesting an estimate, booking a walkthrough, or calling for availability.'
    );
    assert.equal(
      guidance.whatToDo,
      'Choose one primary CTA for the website and growth materials. For Anchor, prefer estimate request or walkthrough request over vague language like "learn more."'
    );
    assert.deepEqual(guidance.whatToConfirm, [
      'The primary CTA is visible on the website.',
      'The CTA matches the commercial growth goal.',
      'The CTA leads to a working form, phone number, email, or booking path.',
      'The CTA does not create confusion with multiple competing actions.',
      'No website changes are published without approval.',
    ]);
    assert.equal(guidance.whoOwnsIt, 'Operator guided');
    assert.equal(
      guidance.completeWhen,
      'Anchor has one clear primary CTA for commercial prospects, and the path behind it works.'
    );
  });

  it('Clear service area guidance covers market bound copy, not generic capture language', () => {
    const html = taskGuidanceCardHtml(
      { ...CLEAR_SERVICE_AREA_TASK },
      { businessName: 'Anchor Cleaning' }
    );
    assert.equal(analyzeLeftPanelHtml(html).taskGuidanceCards, 1);
    assert.match(html, /Clear service area/);
    assert.match(html, /Owner · Operator guided/);
    assert.match(
      html,
      /Before outreach, prospects should know whether Anchor serves their location/
    );
    assert.match(
      html,
      /keeps the first growth push focused on Greater Manchester/
    );
    assert.match(
      html,
      /State the priority service area clearly on the website and growth materials/
    );
    assert.match(
      html,
      /use Greater Manchester first, especially Bedford, Hooksett, Londonderry, Auburn, and Goffstown/
    );
    assert.match(html, /The website names the primary service area\./);
    assert.match(html, /Priority towns match the approved Blueprint\./);
    assert.match(
      html,
      /The service area is easy to find from the homepage or contact\/estimate path\./
    );
    assert.match(
      html,
      /Outreach and future prospect lists stay inside the approved market bound unless changed intentionally\./
    );
    assert.match(
      html,
      /No website changes are published without approval\./
    );
    assert.match(
      html,
      /Anchor&#39;s service area is clearly stated and matches the approved Growth Plan\./
    );
    assert.doesNotMatch(html, /reliable capture and follow-up/);
    assert.doesNotMatch(html, /The person who owns replies knows how this works/);

    const guidance = resolveTaskGuidance(
      CLEAR_SERVICE_AREA_TASK,
      'Anchor Cleaning'
    );
    assert.equal(
      guidance.whyThisMatters,
      'Before outreach, prospects should know whether Anchor serves their location. A clear service area keeps the first growth push focused on Greater Manchester and prevents wasted conversations outside the target market.'
    );
    assert.equal(
      guidance.whatToDo,
      'State the priority service area clearly on the website and growth materials. For Anchor, use Greater Manchester first, especially Bedford, Hooksett, Londonderry, Auburn, and Goffstown.'
    );
    assert.deepEqual(guidance.whatToConfirm, [
      'The website names the primary service area.',
      'Priority towns match the approved Blueprint.',
      'The service area is easy to find from the homepage or contact/estimate path.',
      'Outreach and future prospect lists stay inside the approved market bound unless changed intentionally.',
      'No website changes are published without approval.',
    ]);
    assert.equal(guidance.whoOwnsIt, 'Operator guided');
    assert.equal(
      guidance.completeWhen,
      "Anchor's service area is clearly stated and matches the approved Growth Plan."
    );
  });

  it('Clear services guidance covers commercial service mix, not generic capture language', () => {
    const html = taskGuidanceCardHtml(
      { ...CLEAR_SERVICES_TASK },
      { businessName: 'Anchor Cleaning' }
    );
    assert.equal(analyzeLeftPanelHtml(html).taskGuidanceCards, 1);
    assert.match(html, /Clear services/);
    assert.match(html, /Owner · Operator guided/);
    assert.match(
      html,
      /Before outreach, property managers should quickly understand what Anchor actually provides/
    );
    assert.match(
      html,
      /Clear services help prospects self-identify fit/
    );
    assert.match(
      html,
      /List Anchor&#39;s primary services clearly on the website and growth materials/
    );
    assert.match(
      html,
      /Emphasize recurring commercial cleaning while still showing the current service mix/
    );
    assert.match(html, /The website clearly lists the main services\./);
    assert.match(html, /Recurring commercial cleaning is easy to understand\./);
    assert.match(
      html,
      /Short-term rental turnovers, office cleaning, deep cleans, move-in\/move-out, and residential cleaning are represented accurately\./
    );
    assert.match(
      html,
      /Service descriptions do not overpromise capacity or specialized work Anchor has not approved\./
    );
    assert.match(
      html,
      /No website changes are published without approval\./
    );
    assert.match(
      html,
      /Anchor&#39;s primary services are clearly stated and aligned with the approved Blueprint\./
    );
    assert.doesNotMatch(html, /reliable capture and follow-up/);
    assert.doesNotMatch(html, /The person who owns replies knows how this works/);

    const guidance = resolveTaskGuidance(CLEAR_SERVICES_TASK, 'Anchor Cleaning');
    assert.equal(
      guidance.whyThisMatters,
      'Before outreach, property managers should quickly understand what Anchor actually provides. Clear services help prospects self-identify fit and reduce vague inquiries that do not match the commercial growth plan.'
    );
    assert.equal(
      guidance.whatToDo,
      "List Anchor's primary services clearly on the website and growth materials. Emphasize recurring commercial cleaning while still showing the current service mix."
    );
    assert.deepEqual(guidance.whatToConfirm, [
      'The website clearly lists the main services.',
      'Recurring commercial cleaning is easy to understand.',
      'Short-term rental turnovers, office cleaning, deep cleans, move-in/move-out, and residential cleaning are represented accurately.',
      'Service descriptions do not overpromise capacity or specialized work Anchor has not approved.',
      'No website changes are published without approval.',
    ]);
    assert.equal(guidance.whoOwnsIt, 'Operator guided');
    assert.equal(
      guidance.completeWhen,
      "Anchor's primary services are clearly stated and aligned with the approved Blueprint."
    );
  });

  it('Contact form works guidance covers test submission and follow-up ownership', () => {
    const html = taskGuidanceCardHtml(
      { ...CONTACT_FORM_WORKS_TASK },
      { businessName: 'Anchor Cleaning' }
    );
    assert.equal(analyzeLeftPanelHtml(html).taskGuidanceCards, 1);
    assert.match(html, /Contact form works/);
    assert.match(html, /Owner · Operator guided/);
    assert.match(
      html,
      /If outreach creates interest, the form has to deliver every inquiry reliably/
    );
    assert.match(
      html,
      /A broken form would make Anchor look unresponsive and could lose qualified property manager opportunities/
    );
    assert.match(
      html,
      /Submit a test inquiry through the website form using a test name and email/
    );
    assert.match(
      html,
      /Confirm the message arrives in the right inbox or lead tracker/
    );
    assert.match(html, /The form can be submitted successfully\./);
    assert.match(
      html,
      /The submission arrives in a monitored inbox or lead tracker\./
    );
    assert.match(
      html,
      /The notification includes enough detail to follow up\./
    );
    assert.match(html, /The reply-to email or phone number is usable\./);
    assert.match(
      html,
      /The person responsible for new inquiries knows where to check\./
    );
    assert.match(
      html,
      /No tracking or website changes are made without approval\./
    );
    assert.match(
      html,
      /A test form submission is received successfully and the follow-up owner is clear\./
    );
    assert.doesNotMatch(html, /reliable capture and follow-up/);
    assert.doesNotMatch(html, /The person who owns replies knows how this works/);

    const guidance = resolveTaskGuidance(
      CONTACT_FORM_WORKS_TASK,
      'Anchor Cleaning'
    );
    assert.equal(
      guidance.whyThisMatters,
      'If outreach creates interest, the form has to deliver every inquiry reliably. A broken form would make Anchor look unresponsive and could lose qualified property manager opportunities before anyone sees them.'
    );
    assert.equal(
      guidance.whatToDo,
      'Submit a test inquiry through the website form using a test name and email. Confirm the message arrives in the right inbox or lead tracker, and confirm someone knows who is responsible for replying.'
    );
    assert.deepEqual(guidance.whatToConfirm, [
      'The form can be submitted successfully.',
      'The submission arrives in a monitored inbox or lead tracker.',
      'The notification includes enough detail to follow up.',
      'The reply-to email or phone number is usable.',
      'The person responsible for new inquiries knows where to check.',
      'No tracking or website changes are made without approval.',
    ]);
    assert.equal(guidance.whoOwnsIt, 'Operator guided');
    assert.equal(
      guidance.completeWhen,
      'A test form submission is received successfully and the follow-up owner is clear.'
    );
  });

  it('never nests active task guidance inside Previous Plans', () => {
    const currentTask = { ...BRANDED_EMAIL_TASK };
    const currentSession = {
      sessionId: 'sess-active',
      businessName: 'Anchor Cleaning',
      growthPlan: { percentComplete: 40, status: 'in_progress', currentTask },
    };
    const previous = [
      {
        sessionId: 'sess-old',
        businessName: 'Old Co',
        label: 'Old Co · Previous plan',
        resumeTarget: 'growth_complete',
        growthPlan: { percentComplete: 100, status: 'complete', currentTask: null },
      },
    ];

    const html = renderGrowthWorkspaceLeftPanel({
      currentSession,
      previousSessions: previous,
      currentSessionId: 'sess-active',
      currentTask,
      guidanceOpen: true,
      businessName: 'Anchor Cleaning',
    });
    const stats = analyzeLeftPanelHtml(html);
    assert.equal(stats.currentPlanCards, 1);
    assert.equal(stats.previousPlanCards, 1);
    assert.equal(stats.taskGuidanceCards, 1);
    assert.equal(stats.simpleTaskCards, 0);
    assert.equal(stats.taskGuidanceInPreviousPlans, 0);
    assert.equal(stats.hasPreviousPlansSection, true);
    assert.match(html, /Previous Plans \(1\)/);
    assert.doesNotMatch(
      html.slice(html.indexOf('data-section="previous-plans"')),
      /task-guidance|Branded email available/
    );
  });

  it('opening guidance twice still yields a single guidance card markup helper', () => {
    const task = { ...BRANDED_EMAIL_TASK };
    const once = taskGuidanceCardHtml(task, { businessName: 'Anchor Cleaning' });
    const twice =
      taskGuidanceCardHtml(task, { businessName: 'Anchor Cleaning' }) +
      taskGuidanceCardHtml(task, { businessName: 'Anchor Cleaning' });
    assert.equal(analyzeLeftPanelHtml(once).taskGuidanceCards, 1);
    assert.equal(analyzeLeftPanelHtml(twice).taskGuidanceCards, 2);
    const panel = renderGrowthWorkspaceLeftPanel({
      currentSession: {
        sessionId: 's1',
        businessName: 'Anchor Cleaning',
        growthPlan: { percentComplete: 10, status: 'in_progress', currentTask: task },
      },
      previousSessions: [],
      currentTask: task,
      guidanceOpen: true,
      businessName: 'Anchor Cleaning',
    });
    assert.equal(analyzeLeftPanelHtml(panel).taskGuidanceCards, 1);
  });

  it('client-intel wires shared panel, Open Task Guidance, and no clipped absolute guidance', () => {
    const ui = fs.readFileSync(
      path.join(__dirname, '..', 'public', 'client-intel.html'),
      'utf8'
    );
    assert.match(ui, /growth-workspace-panel\.js/);
    assert.match(ui, /Open Task Guidance/);
    assert.match(ui, /taskGuidanceOpen/);
    assert.match(ui, /renderWorkspaceLeftNav/);
    assert.match(ui, /formatOwnerLabel/);
    assert.match(ui, /Client\/operator/);
    assert.match(ui, /data-role="simple-task"/);
    assert.match(ui, /guidanceOpen:\s*Boolean\(state\.taskGuidanceOpen\)/);
    assert.match(ui, /task-guidance-body/);
    assert.match(ui, /syncGrowthWorkspaceChrome/);
    assert.match(ui, /guidance-open/);
    assert.match(ui, /guidance-suppressed/);
    assert.match(ui, /--gw-sticky-footer-clearance/);
    assert.match(ui, /applyGuidanceFooterClearance/);
    assert.match(ui, /guidance-scroll-spacer/);
    assert.match(ui, /gw-guidance-scroll-spacer/);
    assert.match(ui, /footerHeight \+ 24|Math\.ceil\(footerHeight\)\) \+ 24/);
    assert.match(ui, /flex:\s*1\s+1\s+0/);
    assert.match(ui, /#savedSessions \[data-role="simple-task"\]/);
    assert.match(ui, /data-active-task-card="1"/);
    assert.match(ui, /els\.chatLog\.innerHTML = ''/);
    // Left-nav region may scroll, but the guidance card itself must not clip.
    assert.doesNotMatch(
      ui,
      /#savedSessions\.workspace-left-nav\s*\{[^}]*max-height:\s*48%/
    );
    assert.match(ui, /overflow:\s*visible/);
    assert.match(ui, /height:\s*auto/);
    // Legacy inline simple guidance markup removed from left-nav path.
    assert.doesNotMatch(
      ui,
      /html \+=\s*'\s*<div class="current-task-card task-guidance-card"/
    );
    // Open Task Guidance must not add a duplicate simple chat summary card.
    assert.doesNotMatch(
      ui,
      /addBubble\(\s*'assistant',\s*\n?\s*\(task\.title/
    );
  });

  it('guidance-open left panel HTML includes an end spacer for footer clearance', () => {
    const html = renderGrowthWorkspaceLeftPanel({
      currentSession: {
        sessionId: 's1',
        businessName: 'Anchor Cleaning',
        growthPlan: {
          percentComplete: 40,
          status: 'in_progress',
          currentTask: { ...BRANDED_EMAIL_TASK },
        },
      },
      previousSessions: [],
      currentTask: { ...BRANDED_EMAIL_TASK },
      guidanceOpen: true,
      businessName: 'Anchor Cleaning',
    });
    assert.equal(analyzeLeftPanelHtml(html).activeTaskCards, 1);
    assert.match(html, /data-role="guidance-scroll-spacer"/);
    assert.ok(
      html.indexOf('data-role="task-guidance"') <
        html.indexOf('data-role="guidance-scroll-spacer"'),
      'spacer must sit after the guidance card inside the scroll container'
    );
  });
});
