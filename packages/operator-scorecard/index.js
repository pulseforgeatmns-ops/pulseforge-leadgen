'use strict';

/**
 * SPEC-116 — Operator Scorecard Intelligence.
 * Max recommends. Operators approve. Drafts are never used for reporting.
 */

const types = require('./types');
const catalog = require('./Catalog');
const reasoning = require('./Reasoning');
const review = require('./Review');
const approval = require('./Approval');
const learning = require('./Learning');
const evolution = require('./Evolution');
const brief = require('./Brief');
const { createMemoryOsiStore } = require('./Store');
const { createScorecardEngine } = require('./Engine');

module.exports = {
  ...types,
  ...catalog,
  ...reasoning,
  ...review,
  ...approval,
  ...learning,
  ...evolution,
  ...brief,
  createMemoryOsiStore,
  createScorecardEngine,
};
