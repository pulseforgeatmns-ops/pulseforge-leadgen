'use strict';

/**
 * SPEC-112 Phase 6 — Pilot success reporter.
 * Technical vs business. Never invent 50 prospects or sales.
 */

const { isQualified } = require('./Qualification');

const QUALIFIED_TARGET = 50;

function assessPilot({ aim, qualifications = [], outcomes = {} } = {}) {
  const qualified = (qualifications || []).filter(isQualified);
  const aimComplete = Boolean(
    aim &&
      aim.status === 'complete' &&
      aim.mission &&
      aim.mission.known &&
      aim.transformation &&
      aim.transformation.known &&
      aim.painOntology &&
      aim.painOntology.categories &&
      aim.painOntology.categories.length >= 3
  );
  const scoutUnderstands = Boolean(aimComplete && aim.painOntology.byId);
  const qualificationWorking = Boolean(
    aimComplete && typeof isQualified === 'function'
  );
  const outreachBegun = Boolean(outcomes.outreachBegun || outcomes.outreach_begins);

  const technical = {
    aimCompleted: { met: aimComplete, detail: aimComplete ? 'AIM is complete.' : 'AIM is not complete.' },
    scoutUnderstandsMarket: {
      met: scoutUnderstands,
      detail: scoutUnderstands
        ? 'Pain ontology is available for Scout to reason over.'
        : 'Scout does not yet have a complete AIM ontology.',
    },
    qualificationModelWorking: {
      met: qualificationWorking,
      detail: qualificationWorking
        ? 'Six-dimension qualification is available.'
        : 'Qualification model is not ready.',
    },
    qualifiedProspects: {
      met: qualified.length >= QUALIFIED_TARGET,
      count: qualified.length,
      target: QUALIFIED_TARGET,
      detail: `${qualified.length} / ${QUALIFIED_TARGET} qualified prospects.`,
    },
    outreachBegins: {
      met: outreachBegun,
      detail: outreachBegun ? 'Outreach has begun.' : 'Outreach has not begun.',
    },
  };

  const business = {
    forms: {
      met: Number(outcomes.forms || 0) > 0,
      count: Number(outcomes.forms || 0),
      detail: 'Unknown until observed.',
    },
    meetings: {
      met: Number(outcomes.meetings || 0) > 0,
      count: Number(outcomes.meetings || 0),
      detail: 'Unknown until observed.',
    },
    learning: {
      met: Number(outcomes.learning || outcomes.learnings || 0) > 0,
      count: Number(outcomes.learning || outcomes.learnings || 0),
      detail: 'Unknown until observed.',
    },
    sales: {
      met: Number(outcomes.sales || 0) > 0,
      count: Number(outcomes.sales || 0),
      detail: 'Unknown until observed.',
    },
  };

  return {
    kind: 'aim_pilot_status',
    spec: 'SPEC-112',
    clientKey: aim && aim.clientKey,
    technical,
    business,
    invented: false,
  };
}

module.exports = {
  QUALIFIED_TARGET,
  assessPilot,
};
