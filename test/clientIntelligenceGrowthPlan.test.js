'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  buildGrowthPlan,
  applyTaskCompletion,
  resolveGrowthPlanResumeTarget,
  nextObjectiveDescription,
  completionStatusLine,
  shortBusinessName,
} = require('../services/clientIntelligenceGrowthPlan');
const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
  getResumePayload,
  completeGrowthPlanTask,
  resolveResumeTarget,
  startGrowthConversation,
} = require('../services/clientIntelligenceInterview');

const ANSWERS = [
  'Anchor Cleaning — commercial cleaning for professional offices.',
  'Recurring commercial cleaning and weekly office cleans.',
  'Property managers, facility managers, and professional offices.',
  'Lowest-price bargain hunters.',
  'Greater Manchester including Bedford and Hooksett.',
  'Reliable crews that do the work right without chasing.',
  'Calm professional reliable voice.',
  'Grow commercial cleaning in Greater Manchester.',
  'Clearer path to commercial opportunities in 90 days.',
];

async function approveFreshInterview(opts, clientId = 88) {
  const started = await startClientInterview({ clientId }, opts);
  let turn = started;
  for (const answer of ANSWERS) {
    turn = await postInterviewMessage(started.interviewId, answer, opts);
  }
  const approved = await approveBlueprint(turn.blueprint.id, opts);
  return { interviewId: started.interviewId, approved };
}

function sessionWithReport(base, reportPatch) {
  return {
    ...base,
    status: 'APPROVED',
    interview_state: {
      ...(base.interview_state || {}),
      businessName: 'Anchor Cleaning',
      growthInfrastructureReadinessReport: {
        kind: 'growth_infrastructure_readiness_report',
        overallStatus: 'partial',
        recommendedSetupSequence: [
          {
            order: 1,
            areaId: 'gbp',
            itemId: 'gbp_claimed',
            label: 'Connect Google Business Profile',
            action: 'Claim and verify GBP',
            owner: 'client_required',
            priority: 'high',
            status: 'missing',
            statusLabel: 'Missing',
          },
          {
            order: 2,
            areaId: 'domain_dns',
            itemId: 'branded_email',
            label: 'Configure domain email',
            action: 'Set up branded email',
            owner: 'operator_guided',
            priority: 'high',
            status: 'missing',
            statusLabel: 'Missing',
          },
        ],
        areas: {
          gbp: {
            id: 'gbp',
            items: {
              gbp_claimed: {
                id: 'gbp_claimed',
                status: 'missing',
                owner: 'client_required',
                priority: 'high',
                recommended_next_step: 'Claim GBP',
              },
            },
          },
          domain_dns: {
            id: 'domain_dns',
            items: {
              branded_email: {
                id: 'branded_email',
                status: 'missing',
                owner: 'operator_guided',
                priority: 'high',
                recommended_next_step: 'Configure branded email',
              },
            },
          },
        },
        ...reportPatch,
      },
      firstGrowthPlanPreview: { title: 'First Growth Plan Preview' },
      growthConversation: {
        status: 'preview_ready',
        turns: [{ speaker: 'assistant', message: 'ok' }],
        firstGrowthPlanPreview: { title: 'First Growth Plan Preview' },
      },
    },
  };
}

describe('SPEC-088 Growth Plan continuation', () => {
  it('builds tasks and resumes workspace instead of readiness dead end', () => {
    const session = sessionWithReport({ id: 's1', client_id: 10 });
    const plan = buildGrowthPlan(session, null);
    assert.equal(plan.status, 'in_progress');
    assert.ok(plan.percentComplete < 100);
    assert.equal(plan.currentTask.id, 'setup:gbp:gbp_claimed');
    assert.equal(
      resolveGrowthPlanResumeTarget(session, null),
      'growth_workspace'
    );
    assert.equal(resolveResumeTarget(session, null), 'growth_workspace');
  });

  it('completing a setup task advances to the next incomplete task', () => {
    const session = sessionWithReport({ id: 's1', client_id: 10 });
    const applied = applyTaskCompletion(session, 'setup:gbp:gbp_claimed', {});
    assert.equal(applied.nextTask.id, 'setup:domain_dns:branded_email');
    assert.ok(
      applied.growthPlan.completedTaskIds.includes('setup:gbp:gbp_claimed')
    );
  });

  it('resume payload after approval targets Growth Workspace', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const { interviewId } = await approveFreshInterview(opts, 91);
    const resume = await getResumePayload(interviewId, {
      ...opts,
      action: 'continue',
    });
    assert.equal(resume.resumeTarget, 'growth_workspace');
    assert.equal(resume.resumePhase, 'growth_workspace');
    assert.ok(resume.growthPlan);
    assert.ok(resume.growthPlan.currentTask);
    assert.equal(
      resume.growthPlan.currentTask.id,
      'milestone:growth_conversation'
    );
  });

  it('completeGrowthPlanTask persists and advances through readiness setup', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const { interviewId } = await approveFreshInterview(opts, 92);
    await startGrowthConversation(interviewId, opts);

    // Seed a readiness report with setup gaps (simulate completed assessment).
    const session = await store.getSession(interviewId);
    const seeded = sessionWithReport(session);
    await store.updateSession(interviewId, {
      interview_state: seeded.interview_state,
    });

    let resume = await getResumePayload(interviewId, opts);
    assert.equal(resume.resumeTarget, 'growth_workspace');
    assert.equal(resume.currentTask.id, 'setup:gbp:gbp_claimed');

    const done = await completeGrowthPlanTask(
      interviewId,
      'setup:gbp:gbp_claimed',
      opts
    );
    assert.equal(done.nextTask.id, 'setup:domain_dns:branded_email');
    assert.equal(done.growthPlan.currentTask.id, 'setup:domain_dns:branded_email');
  });

  it('UI/API markers use Resume Growth Plan and workspace tabs', () => {
    const ui = fs.readFileSync(
      path.join(__dirname, '..', 'public', 'client-intel.html'),
      'utf8'
    );
    const dash = fs.readFileSync(
      path.join(__dirname, '..', 'public', 'dashboard.html'),
      'utf8'
    );
    const routes = fs.readFileSync(
      path.join(__dirname, '..', 'routes', 'clientIntelligence.js'),
      'utf8'
    );

    assert.match(ui, /Resume Growth Plan/);
    assert.match(ui, /growth_workspace/);
    assert.match(ui, /workspaceTabs/);
    assert.match(ui, /Readiness Report/);
    assert.match(ui, /Previous Plans/);
    assert.match(ui, /Finish Review/);
    assert.match(ui, /Choose Next Objective/);
    assert.doesNotMatch(ui, /Mark Objectives Reviewed/);
    assert.match(
      ui,
      /no campaign, prospect list, or account changes begin without operator approval/i
    );
    assert.match(dash, /Resume Growth Plan/);
    assert.match(dash, /Previous Plans/);
    assert.match(dash, /cie-previous-plans/);
    assert.match(
      routes,
      /\/api\/v1\/interview\/:id\/growth-plan\/tasks\/:taskId\/complete/
    );
  });

  it('next-objective milestone uses client-facing checklist copy', () => {
    assert.equal(shortBusinessName('Anchor Cleaning'), 'Anchor');
    assert.equal(
      nextObjectiveDescription('Anchor Cleaning'),
      'Anchor has completed the current growth setup checklist. The next step is to choose what Max should help with next.'
    );

    const session = sessionWithReport({ id: 's1', client_id: 10 });
    // Mark both setup gaps ready so the plan advances to choose-next-objective.
    session.interview_state.growthInfrastructureReadinessReport.areas.gbp.items.gbp_claimed.status =
      'ready';
    session.interview_state.growthInfrastructureReadinessReport.areas.domain_dns.items.branded_email.status =
      'ready';
    session.interview_state.growthInfrastructureReadinessReport.recommendedSetupSequence =
      session.interview_state.growthInfrastructureReadinessReport.recommendedSetupSequence.map(
        (s) => ({ ...s, status: 'ready', statusLabel: 'Ready' })
      );
    session.interview_state.growthWork = {
      completedTaskIds: [],
      history: [
        {
          taskId: 'setup:lead_capture:email_routing',
          title: 'Email routing',
          completedAt: new Date().toISOString(),
          source: 'operator',
        },
      ],
      activeTaskId: null,
      updatedAt: new Date().toISOString(),
    };

    // Inject a completed setup task id into the plan task list via report sequence.
    session.interview_state.growthInfrastructureReadinessReport.recommendedSetupSequence.push(
      {
        order: 3,
        areaId: 'lead_capture',
        itemId: 'email_routing',
        label: 'Email routing',
        action: 'Route inbound email to a monitored inbox.',
        owner: 'client_required',
        priority: 'high',
        status: 'ready',
        statusLabel: 'Ready',
      }
    );
    session.interview_state.growthInfrastructureReadinessReport.areas.lead_capture = {
      id: 'lead_capture',
      items: {
        email_routing: {
          id: 'email_routing',
          status: 'ready',
          owner: 'client_required',
          priority: 'high',
          recommended_next_step: 'Route inbound email',
        },
      },
    };

    const plan = buildGrowthPlan(session, null);
    assert.ok(plan.currentTask);
    assert.equal(plan.currentTask.id, 'milestone:campaign_ready');
    assert.equal(
      plan.currentTask.description,
      'Anchor has completed the current growth setup checklist. The next step is to choose what Max should help with next.'
    );
    assert.doesNotMatch(
      plan.currentTask.description,
      /All current recommendations are complete/
    );
    assert.equal(plan.completionStatusLine, 'Completed setup: Email routing');
  });

  it('completion options use review-first next-objective labels', () => {
    const session = sessionWithReport({ id: 's-complete', client_id: 10 });
    session.interview_state.growthInfrastructureReadinessReport.areas.gbp.items.gbp_claimed.status =
      'ready';
    session.interview_state.growthInfrastructureReadinessReport.areas.domain_dns.items.branded_email.status =
      'ready';
    session.interview_state.growthInfrastructureReadinessReport.recommendedSetupSequence =
      session.interview_state.growthInfrastructureReadinessReport.recommendedSetupSequence.map(
        (s) => ({ ...s, status: 'ready', statusLabel: 'Ready' })
      );
    session.interview_state.growthWork = {
      completedTaskIds: ['milestone:campaign_ready'],
      history: [
        {
          taskId: 'milestone:campaign_ready',
          title: 'Choose next growth objective',
          completedAt: new Date().toISOString(),
          source: 'operator',
        },
      ],
      activeTaskId: null,
      updatedAt: new Date().toISOString(),
    };

    const plan = buildGrowthPlan(session, null);
    assert.equal(plan.status, 'complete');
    assert.deepEqual(
      plan.completionOptions.map((o) => o.label),
      [
        'Plan First Campaign',
        'Explore Another Market',
        'Improve Lead Conversion',
        'Create New Growth Plan',
      ]
    );
    assert.deepEqual(
      plan.completionOptions.map((o) => o.id),
      [
        'launch_campaign',
        'expand_market',
        'improve_conversion',
        'new_growth_plan',
      ]
    );

    const ui = fs.readFileSync(
      path.join(__dirname, '..', 'public', 'client-intel.html'),
      'utf8'
    );
    assert.match(ui, /Plan First Campaign/);
    assert.match(
      ui,
      /Max will help define the campaign strategy, validation criteria, and approval checkpoints/
    );
    assert.doesNotMatch(ui, /Launch New Campaign/);
    assert.doesNotMatch(ui, /Expand Market/);
    assert.doesNotMatch(ui, /Improve Conversion(?!s)/);
  });

  it('completion status line falls back when final completed task was not setup', () => {
    assert.equal(
      completionStatusLine(
        [
          {
            id: 'setup:lead_capture:email_routing',
            type: 'setup',
            title: 'Email routing',
          },
          {
            id: 'milestone:campaign_ready',
            type: 'milestone',
            title: 'Choose next growth objective',
          },
        ],
        [
          {
            taskId: 'setup:lead_capture:email_routing',
            title: 'Email routing',
          },
          {
            taskId: 'milestone:campaign_ready',
            title: 'Choose next growth objective',
          },
        ]
      ),
      'Growth infrastructure checklist complete'
    );
    assert.equal(
      completionStatusLine(
        [
          {
            id: 'setup:lead_capture:email_routing',
            type: 'setup',
            title: 'Email routing',
          },
        ],
        [
          {
            taskId: 'setup:lead_capture:email_routing',
            title: 'Email routing',
          },
        ]
      ),
      'Completed setup: Email routing'
    );
    assert.equal(completionStatusLine([], []), 'Growth infrastructure checklist complete');
  });
});
