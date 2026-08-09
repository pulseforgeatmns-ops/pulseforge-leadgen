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
