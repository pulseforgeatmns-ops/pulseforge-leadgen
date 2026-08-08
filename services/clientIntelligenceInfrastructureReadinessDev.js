'use strict';

/**
 * Dev/test helpers for Growth Infrastructure Readiness (SPEC-087).
 *
 * Gated: not available in production unless CIE_GROWTH_INFRA_DEV_FIXTURES=1.
 * Fixtures feed the same buildInfrastructureReadinessReply → report path as
 * real answers. Never mutates DNS, GBP, social, analytics, or CRM.
 */

const path = require('path');
const {
  buildInfrastructureReadinessReply,
  buildEmptyAreas,
} = require('./clientIntelligenceInfrastructureReadiness');

const FIXTURE_DIR = path.join(
  __dirname,
  '..',
  'fixtures',
  'growthInfrastructureReadiness'
);

const FIXTURE_LOADERS = Object.freeze({
  anchor: () => require(path.join(FIXTURE_DIR, 'anchor.js')),
});

/**
 * Dev fixtures enabled when:
 * - CIE_GROWTH_INFRA_DEV_FIXTURES=1 (explicit opt-in, including production/staging), or
 * - NODE_ENV is development|test and CIE_GROWTH_INFRA_DEV_FIXTURES is not '0'
 *
 * Fail closed: production, staging, or unset NODE_ENV → disabled unless
 * CIE_GROWTH_INFRA_DEV_FIXTURES=1. Client-facing production must not show
 * the shortcut unless intentionally enabled.
 *
 * @param {NodeJS.ProcessEnv} [env]
 */
function isGrowthInfraDevFixturesEnabled(env = process.env) {
  const flag = String(env.CIE_GROWTH_INFRA_DEV_FIXTURES || '').trim();
  if (flag === '1' || /^true$/i.test(flag)) return true;
  if (flag === '0' || /^false$/i.test(flag)) return false;
  const nodeEnv = String(env.NODE_ENV || '').trim().toLowerCase();
  return nodeEnv === 'development' || nodeEnv === 'test';
}

function listGrowthInfraFixtures() {
  return Object.keys(FIXTURE_LOADERS).map((id) => {
    const f = FIXTURE_LOADERS[id]();
    return {
      id: f.id,
      label: f.label,
      sample: true,
      devOnly: true,
      businessName: f.businessName,
      description: f.description,
    };
  });
}

function getGrowthInfraFixture(fixtureId) {
  const key = String(fixtureId || '')
    .trim()
    .toLowerCase();
  const loader = FIXTURE_LOADERS[key];
  if (!loader) {
    const known = Object.keys(FIXTURE_LOADERS).join(', ') || '(none)';
    const err = new Error(
      `Unknown Growth Infrastructure fixture "${fixtureId}". Known: ${known}`
    );
    err.code = 'unknown_fixture';
    err.status = 400;
    throw err;
  }
  const fixture = loader();
  if (!fixture || !fixture.sample || !fixture.devOnly) {
    const err = new Error('Fixture is not marked sample/dev — refusing to load');
    err.code = 'fixture_not_dev';
    err.status = 500;
    throw err;
  }
  return fixture;
}

function annotateAnswersAsDevFixture(answers, fixture) {
  const out = { ...(answers || {}) };
  for (const key of Object.keys(out)) {
    const prev = out[key] || {};
    out[key] = {
      ...prev,
      sample: true,
      source: 'dev_fixture',
      fixtureId: fixture.id,
      note: 'SAMPLE/DEV fixture answer — not live client data',
    };
  }
  return out;
}

/**
 * Apply fixture step answers through the same reply/report builder as live chat.
 *
 * @returns {{
 *   state: object,
 *   report: object|null,
 *   messages: Array<{step:string, role:string, message:string}>,
 *   fixture: object
 * }}
 */
function applyGrowthInfraFixtureThroughReplyPath(
  priorState,
  fixture,
  blueprint,
  opts = {}
) {
  if (!fixture || !fixture.sample) {
    throw new Error('Refusing to apply non-sample fixture');
  }

  let state = {
    status: 'active',
    step:
      (priorState && priorState.step && priorState.step !== 'report'
        ? priorState.step
        : null) || 'website_domain',
    answers: (priorState && priorState.answers) || {},
    areas: (priorState && priorState.areas) || buildEmptyAreas(),
    startedAt: (priorState && priorState.startedAt) || new Date().toISOString(),
    context: (priorState && priorState.context) || {},
    turns: ((priorState && priorState.turns) || []).slice(),
  };

  const messages = [];
  let report = null;
  const businessName =
    opts.businessName ||
    fixture.businessName ||
    (state.context && state.context.businessName) ||
    'the business';

  for (const entry of fixture.answers || []) {
    const step = entry.step;
    const text = String(entry.message || '').trim();
    if (!text) continue;

    // Align cursor to the fixture step when replaying a full bank.
    if (step && state.step !== step && state.step !== 'report') {
      state = { ...state, step };
    }

    messages.push({ step: state.step, role: 'client', message: text });
    state.turns.push({
      speaker: 'client',
      message: text,
      at: new Date().toISOString(),
      step: state.step,
      sample: true,
      source: 'dev_fixture',
      fixtureId: fixture.id,
    });

    const reply = buildInfrastructureReadinessReply(text, state, blueprint || {}, {
      businessName,
      blueprintId: opts.blueprintId || (blueprint && blueprint.id) || null,
      blueprintVersion:
        opts.blueprintVersion || (blueprint && blueprint.version) || null,
    });

    const answers = annotateAnswersAsDevFixture(reply.answers, fixture);
    state = {
      ...state,
      status: reply.report ? 'report_ready' : 'active',
      step: reply.step,
      answers,
      areas: reply.areas,
    };

    state.turns.push({
      speaker: 'assistant',
      message: reply.message,
      at: new Date().toISOString(),
      step: reply.step,
      intent: reply.intent,
      sample: true,
      source: 'dev_fixture',
      fixtureId: fixture.id,
    });

    messages.push({
      step: reply.step,
      role: 'assistant',
      message: reply.message,
    });

    if (reply.report) {
      report = {
        ...reply.report,
        devFixture: {
          id: fixture.id,
          label: fixture.label,
          sample: true,
          note:
            'Populated from sample/dev fixture answers — not live client data. Same report builder as real answers.',
        },
      };
      break;
    }
  }

  // If the fixture ended before assets, force the real report path once.
  if (!report) {
    const reply = buildInfrastructureReadinessReply(
      'Please wrap up with the readiness report. [SAMPLE/DEV]',
      state,
      blueprint || {},
      {
        businessName,
        blueprintId: opts.blueprintId || (blueprint && blueprint.id) || null,
        blueprintVersion:
          opts.blueprintVersion || (blueprint && blueprint.version) || null,
        forceReport: true,
      }
    );
    state = {
      ...state,
      status: 'report_ready',
      step: reply.step,
      answers: annotateAnswersAsDevFixture(reply.answers, fixture),
      areas: reply.areas,
    };
    state.turns.push({
      speaker: 'assistant',
      message: reply.message,
      at: new Date().toISOString(),
      step: reply.step,
      intent: reply.intent,
      sample: true,
      source: 'dev_fixture',
      fixtureId: fixture.id,
    });
    messages.push({
      step: reply.step,
      role: 'assistant',
      message: reply.message,
    });
    report = reply.report
      ? {
          ...reply.report,
          devFixture: {
            id: fixture.id,
            label: fixture.label,
            sample: true,
            note:
              'Populated from sample/dev fixture answers — not live client data. Same report builder as real answers.',
          },
        }
      : null;
  }

  state = {
    ...state,
    status: report ? 'report_ready' : state.status,
    step: report ? 'report' : state.step,
    devFixture: {
      id: fixture.id,
      label: fixture.label,
      sample: true,
      appliedAt: new Date().toISOString(),
      disclaimer: fixture.disclaimer,
    },
  };

  return { state, report, messages, fixture };
}

module.exports = {
  isGrowthInfraDevFixturesEnabled,
  listGrowthInfraFixtures,
  getGrowthInfraFixture,
  applyGrowthInfraFixtureThroughReplyPath,
  annotateAnswersAsDevFixture,
  FIXTURE_LOADERS,
};
