'use strict';

/**
 * Mail Package Generator Capability (SPEC-033).
 */

const types = require('./types');
const validate = require('./validate');
const personalize = require('./personalize');
const render = require('./render');
const exportCsv = require('./exportCsv');
const exportDocx = require('./exportDocx');
const {
  InMemoryMailPackageStore,
  createInMemoryMailPackageStore,
} = require('./MailPackageStore');
const {
  createMailPackageGeneratorCapability,
  resolveApprovedProspects,
  matchMailMergeRow,
} = require('./MailPackageGenerator');

module.exports = {
  ...types,
  ...validate,
  ...personalize,
  ...render,
  ...exportCsv,
  ...exportDocx,
  InMemoryMailPackageStore,
  createInMemoryMailPackageStore,
  createMailPackageGeneratorCapability,
  resolveApprovedProspects,
  matchMailMergeRow,
};
