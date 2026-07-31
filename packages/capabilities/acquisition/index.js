'use strict';

/**
 * Prospect Acquisition Framework (SPEC-060 / ADR-044).
 */

const types = require('./types');
const providerContract = require('./providerContract');
const {
  AcquisitionProviderRegistry,
  createDefaultAcquisitionRegistry,
} = require('./ProviderRegistry');
const {
  createPlacesAcquisitionProvider,
} = require('./providers/PlacesAcquisitionProvider');
const {
  createManualProspectProvider,
} = require('./providers/ManualProspectProvider');
const {
  createCsvImportProvider,
  parseCsvFallback,
} = require('./providers/CsvImportProvider');
const {
  createExistingProspectRepositoryProvider,
} = require('./providers/ExistingProspectRepositoryProvider');
const { verifyCandidateSet } = require('./ProspectVerification');
const {
  STRATEGY_TO_PROVIDER,
  selectAcquisitionStrategy,
  createProspectAcquisition,
  buildAcquisitionWorkspaceView,
} = require('./ProspectAcquisition');
const {
  createProspectAcquisitionCapability,
} = require('./ProspectAcquisitionCapability');

module.exports = {
  ...types,
  ...providerContract,
  AcquisitionProviderRegistry,
  createDefaultAcquisitionRegistry,
  createPlacesAcquisitionProvider,
  createManualProspectProvider,
  createCsvImportProvider,
  parseCsvFallback,
  createExistingProspectRepositoryProvider,
  verifyCandidateSet,
  STRATEGY_TO_PROVIDER,
  selectAcquisitionStrategy,
  createProspectAcquisition,
  buildAcquisitionWorkspaceView,
  createProspectAcquisitionCapability,
};
