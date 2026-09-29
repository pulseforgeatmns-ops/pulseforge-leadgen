'use strict';

/**
 * Jake AO dogfood prospect book — Greater Manchester pilot cluster.
 * Distinct from Mike Campaign 001 direct-mail targets and canonical-45 AO batch names.
 *
 * Mix: 5 PM/development · 5 professional office · 4 medical/dental · 4 stretch
 */

module.exports = Object.freeze({
  CLIENT_ID: 10,
  BATCH_SLUG: 'ao-assignment-2026-09-16-jake-dogfood',
  JAKE_PRODUCTION_EMAIL: 'jzmaynard7@gmail.com',
  JAKE_LEGACY_EMAIL_CANDIDATES: Object.freeze([
    'jacob@gopulseforge.com',
    'jacob@goanchorcleaning.com',
  ]),
  PROSPECTS: Object.freeze([
    // Property management / development (5)
    {
      business_name: 'LNH Property Management',
      address: '945 Elm St, Manchester, NH',
      business_type: 'property_management',
      lane: 'property_management',
      priority: 'high',
      segment: 'property_management',
    },
    {
      business_name: 'Prider Property Management',
      address: '5 Raymond Rd, Hooksett, NH',
      business_type: 'property_management',
      lane: 'property_management',
      priority: 'high',
      segment: 'property_management',
    },
    {
      business_name: 'Chinburg Properties',
      address: '1 Washington St, Newmarket, NH',
      business_type: 'development',
      lane: 'development',
      priority: 'high',
      segment: 'property_management',
    },
    {
      business_name: 'Great Bridge Properties',
      address: '1850 Elm St, Manchester, NH',
      business_type: 'property_management',
      lane: 'property_management',
      priority: 'normal',
      segment: 'property_management',
    },
    {
      business_name: 'West Property Management',
      address: '141 South Rd, Bedford, NH',
      business_type: 'property_management',
      lane: 'property_management',
      priority: 'normal',
      segment: 'property_management',
    },

    // Professional / commercial office (5)
    {
      business_name: 'Shaheen & Gordon',
      address: '107 Storrs St, Concord, NH',
      business_type: 'law_firm',
      lane: 'commercial_office',
      priority: 'high',
      segment: 'professional_office',
    },
    {
      business_name: 'Sheehan Phinney',
      address: '1000 Elm St, Manchester, NH',
      business_type: 'law_firm',
      lane: 'commercial_office',
      priority: 'high',
      segment: 'professional_office',
    },
    {
      business_name: 'Sulloway & Hollis',
      address: '9 Capitol St, Concord, NH',
      business_type: 'law_firm',
      lane: 'commercial_office',
      priority: 'normal',
      segment: 'professional_office',
    },
    {
      business_name: 'Wadleigh Starr & Peters',
      address: '95 Market St, Manchester, NH',
      business_type: 'law_firm',
      lane: 'commercial_office',
      priority: 'normal',
      segment: 'professional_office',
    },
    {
      business_name: 'Masiello Group',
      address: '25 South Main St, Concord, NH',
      business_type: 'accounting',
      lane: 'commercial_office',
      priority: 'normal',
      segment: 'professional_office',
    },

    // Medical / dental (4)
    {
      business_name: 'Center For Dental Excellence',
      address: '27 Webster St, Manchester, NH',
      business_type: 'dental',
      lane: 'medical_dental',
      priority: 'normal',
      segment: 'medical_dental',
    },
    {
      business_name: 'Pearl Modern Dentistry',
      address: '1855 Elm St, Manchester, NH',
      business_type: 'dental',
      lane: 'medical_dental',
      priority: 'normal',
      segment: 'medical_dental',
    },
    {
      business_name: 'Bedford Dental Group',
      address: '160 S River Rd, Bedford, NH',
      business_type: 'dental',
      lane: 'medical_dental',
      priority: 'normal',
      segment: 'medical_dental',
    },
    {
      business_name: 'Hooksett Family Dental',
      address: '132 Hooksett Rd, Hooksett, NH',
      business_type: 'dental',
      lane: 'medical_dental',
      priority: 'normal',
      segment: 'medical_dental',
    },

    // High-upside / stretch (4)
    {
      business_name: 'Saint Anselm College',
      address: '100 Saint Anselm Dr, Goffstown, NH',
      business_type: 'institutional',
      lane: 'commercial_real_estate',
      priority: 'high',
      segment: 'stretch',
    },
    {
      business_name: 'Catholic Medical Center',
      address: '100 McGregor St, Manchester, NH',
      business_type: 'medical',
      lane: 'medical_dental',
      priority: 'high',
      segment: 'stretch',
    },
    {
      business_name: 'Manchester Housing & Redevelopment Authority',
      address: '749 Chestnut St, Manchester, NH',
      business_type: 'property_management',
      lane: 'property_management',
      priority: 'normal',
      segment: 'stretch',
    },
    {
      business_name: 'Tuscan Village Manchester',
      address: '777 South Willow St, Manchester, NH',
      business_type: 'development',
      lane: 'development',
      priority: 'high',
      segment: 'stretch',
    },
  ]),
});
