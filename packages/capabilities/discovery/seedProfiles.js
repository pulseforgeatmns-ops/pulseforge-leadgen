'use strict';

/**
 * Seed Discovery Profile library (SPEC-024).
 * Versioned business assets — not application configuration.
 */

const { buildDiscoveryProfile } = require('./types');

const MANCHESTER_GEO = {
  label: 'Manchester, NH',
  cities: [
    'Manchester',
    'Bedford',
    'Goffstown',
    'Hooksett',
    'Londonderry',
    'Auburn',
  ],
  state: 'NH',
  radiusMiles: 20,
};

/**
 * @returns {object[]}
 */
function seedDiscoveryProfiles() {
  return [
    buildDiscoveryProfile({
      id: 'dp_commercial_cleaning_manchester',
      name: 'Commercial Cleaning - Manchester',
      description:
        'High-value commercial cleaning prospects for Anchor Cleaning.',
      tenantId: null,
      clientIds: [10],
      industryTargets: [
        'Commercial Property Management',
        'Law Firms',
        'CPA Firms',
        'Medical Offices',
        'Professional Offices',
      ],
      geography: MANCHESTER_GEO,
      targetCount: 50,
      requiredSignals: ['active_website', 'commercial_location', 'verified_address'],
      preferredSignals: [
        'multi_location',
        'hiring_activity',
        'professional_branding',
        'recurring_facility_ops',
      ],
      excludedSignals: [
        'residential_only',
        'existing_customer',
        'existing_prospect',
        'closed_business',
      ],
      minimumConfidence: 0.75,
      version: '1.0',
      status: 'active',
    }),
    buildDiscoveryProfile({
      id: 'dp_commercial_cleaning_providence',
      name: 'Commercial Cleaning - Providence',
      description: 'Commercial cleaning prospects in the Providence metro.',
      clientIds: [],
      industryTargets: [
        'Commercial Property Management',
        'Law Firms',
        'CPA Firms',
        'Medical Offices',
        'Professional Offices',
      ],
      geography: {
        label: 'Providence, RI',
        cities: ['Providence', 'Cranston', 'Warwick', 'Pawtucket'],
        state: 'RI',
        radiusMiles: 20,
      },
      targetCount: 50,
      requiredSignals: ['active_website', 'commercial_location', 'verified_address'],
      preferredSignals: ['multi_location', 'professional_branding'],
      excludedSignals: ['residential_only', 'existing_prospect', 'closed_business'],
      minimumConfidence: 0.75,
      version: '1.0',
    }),
    buildDiscoveryProfile({
      id: 'dp_commercial_cleaning_boston',
      name: 'Commercial Cleaning - Boston',
      description: 'Commercial cleaning prospects in Greater Boston.',
      industryTargets: [
        'Commercial Property Management',
        'Law Firms',
        'CPA Firms',
        'Medical Offices',
        'Professional Offices',
      ],
      geography: {
        label: 'Boston, MA',
        cities: ['Boston', 'Cambridge', 'Somerville', 'Brookline'],
        state: 'MA',
        radiusMiles: 15,
      },
      targetCount: 50,
      requiredSignals: ['active_website', 'commercial_location', 'verified_address'],
      preferredSignals: ['multi_location', 'professional_branding'],
      excludedSignals: ['residential_only', 'existing_prospect', 'closed_business'],
      minimumConfidence: 0.75,
      version: '1.0',
    }),
    buildDiscoveryProfile({
      id: 'dp_property_managers',
      name: 'Property Managers',
      description: 'Property management companies needing recurring facility services.',
      industryTargets: ['Property Management', 'Commercial Property Management'],
      geography: MANCHESTER_GEO,
      targetCount: 40,
      requiredSignals: ['active_website', 'verified_address'],
      preferredSignals: ['multi_location', 'recurring_facility_ops'],
      excludedSignals: ['residential_only', 'existing_prospect'],
      minimumConfidence: 0.7,
      version: '1.0',
    }),
    buildDiscoveryProfile({
      id: 'dp_overflow_cleaning_partners',
      name: 'Overflow Cleaning Partners',
      description: 'Commercial cleaning companies for overflow partnership.',
      clientIds: [10],
      industryTargets: [
        'Commercial Cleaning',
        'Janitorial Services',
        'Office Cleaning',
      ],
      geography: MANCHESTER_GEO,
      targetCount: 30,
      requiredSignals: ['active_website', 'verified_address'],
      preferredSignals: ['multi_location', 'hiring_activity'],
      excludedSignals: ['residential_only', 'existing_customer'],
      minimumConfidence: 0.7,
      version: '1.0',
    }),
    buildDiscoveryProfile({
      id: 'dp_law_firms',
      name: 'Law Firms',
      description: 'Law firm offices as commercial cleaning buyers.',
      industryTargets: ['Law Firms', 'Attorneys', 'Legal Offices'],
      geography: MANCHESTER_GEO,
      targetCount: 40,
      requiredSignals: ['active_website', 'commercial_location', 'verified_address'],
      preferredSignals: ['professional_branding', 'recurring_facility_ops'],
      excludedSignals: ['residential_only', 'existing_prospect', 'closed_business'],
      minimumConfidence: 0.75,
      version: '1.0',
    }),
    buildDiscoveryProfile({
      id: 'dp_dental_practices',
      name: 'Dental Practices',
      description: 'Dental and medical office cleaning prospects.',
      industryTargets: ['Dental Practices', 'Medical Offices'],
      geography: MANCHESTER_GEO,
      targetCount: 30,
      requiredSignals: ['active_website', 'verified_address'],
      preferredSignals: ['professional_branding', 'recurring_facility_ops'],
      excludedSignals: ['residential_only', 'existing_prospect'],
      minimumConfidence: 0.7,
      version: '1.0',
    }),
    buildDiscoveryProfile({
      id: 'dp_window_cleaning',
      name: 'Window Cleaning',
      description: 'Commercial window cleaning prospects.',
      industryTargets: ['Commercial Offices', 'Property Management', 'Retail Centers'],
      geography: MANCHESTER_GEO,
      targetCount: 30,
      requiredSignals: ['active_website', 'verified_address'],
      preferredSignals: ['multi_location', 'commercial_office'],
      excludedSignals: ['residential_only'],
      minimumConfidence: 0.7,
      version: '1.0',
    }),
  ];
}

module.exports = {
  seedDiscoveryProfiles,
  MANCHESTER_GEO,
};
