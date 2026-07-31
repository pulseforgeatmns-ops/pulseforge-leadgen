'use strict';

/**
 * Google Places acquisition provider (SPEC-060).
 * Wraps PlacesProvider — publishes Candidates only.
 */

const {
  createPlacesProvider,
} = require('../../discovery/providers/PlacesProvider');
const { adaptSearchProvider } = require('../providerContract');
const { ACQUISITION_SOURCES } = require('../types');

/**
 * @param {object} [deps]
 */
function createPlacesAcquisitionProvider(deps = {}) {
  const places = deps.placesProvider || createPlacesProvider(deps);
  return adaptSearchProvider(places, {
    id: 'google_places',
    label: 'Google Places',
    category: 'discovery',
    acquisitionSource: ACQUISITION_SOURCES.GOOGLE_PLACES,
    status: 'existing',
    supports: ['search', 'discovery'],
  });
}

module.exports = {
  createPlacesAcquisitionProvider,
};
