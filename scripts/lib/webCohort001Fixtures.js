'use strict';

const COHORT_TAG = 'WEB-COHORT-001';
const COHORT_SIZE = 25;

/** Deterministic fixture businesses for NO-SEND validation cohort. */
const FIXTURE_BUSINESSES = Object.freeze([
  { company: 'Ridgeline Law Group', domain: 'ridgeline-law-fixture.example', industry: 'legal', location: 'Austin, TX', email: 'info@ridgeline-law-fixture.example', phone: '512-555-0101', contact: 'Sarah Chen', google_rating: 4.6, google_review_count: 84, hiring_signal: true, audit_profile: 'high_deficiency_high_value' },
  { company: 'Summit Accounting Partners', domain: 'summit-acct-fixture.example', industry: 'accounting', location: 'Denver, CO', email: 'hello@summit-acct-fixture.example', phone: '303-555-0102', contact: 'Mark Rivera', google_rating: 4.8, google_review_count: 52, audit_profile: 'moderate_deficiency_strong_business' },
  { company: 'Blue Ridge Dental Care', domain: 'blueridge-dental-fixture.example', industry: 'dental', location: 'Asheville, NC', email: 'frontdesk@blueridge-dental-fixture.example', phone: '828-555-0103', contact: 'Dr. Amy Walsh', google_rating: 4.9, google_review_count: 210, recent_growth_signal: true, audit_profile: 'adequate_site_strong_business' },
  { company: 'Northstar HVAC Services', domain: 'northstar-hvac-fixture.example', industry: 'hvac', location: 'Columbus, OH', email: 'service@northstar-hvac-fixture.example', phone: '614-555-0104', contact: 'Tom Grady', google_rating: 4.4, google_review_count: 96, hiring_signal: true, audit_profile: 'high_deficiency_high_value' },
  { company: 'Harbor Home Renovations', domain: 'harbor-home-fixture.example', industry: 'home_renovation', location: 'Portland, ME', email: 'projects@harbor-home-fixture.example', phone: '207-555-0105', contact: 'Jess Lee', google_rating: 4.7, google_review_count: 38, audit_profile: 'moderate_deficiency_strong_business' },
  { company: 'Metro Fitness Collective', domain: 'metrofit-fixture.example', industry: 'fitness', location: 'Minneapolis, MN', email: 'team@metrofit-fixture.example', phone: '612-555-0106', contact: 'Chris Ortiz', google_rating: 4.5, google_review_count: 145, audit_profile: 'weak_economics' },
  { company: 'Prairie Legal Associates', domain: 'prairie-legal-fixture.example', industry: 'legal', location: 'Omaha, NE', email: 'contact@prairie-legal-fixture.example', phone: '402-555-0107', contact: 'Nina Patel', google_rating: 4.3, google_review_count: 27, audit_profile: 'high_deficiency_thin_contact' },
  { company: 'Coastal Salon Studio', domain: 'coastal-salon-fixture.example', industry: 'salon', location: 'Charleston, SC', email: 'book@coastal-salon-fixture.example', phone: '843-555-0108', contact: 'Lena Brooks', google_rating: 4.8, google_review_count: 312, audit_profile: 'adequate_site_strong_business' },
  { company: 'Ironclad Roofing', domain: 'ironclad-roof-fixture.example', industry: 'roofing', location: 'Tampa, FL', email: 'estimate@ironclad-roof-fixture.example', phone: '813-555-0109', contact: 'Dan Moore', google_rating: 4.6, google_review_count: 74, advertising_signal: true, audit_profile: 'high_deficiency_high_value' },
  { company: 'Greenway Landscaping', domain: 'greenway-land-fixture.example', industry: 'landscaping', location: 'Madison, WI', email: 'info@greenway-land-fixture.example', phone: '608-555-0110', contact: 'Paul Nguyen', google_rating: 4.2, google_review_count: 41, audit_profile: 'moderate_deficiency_strong_business' },
  { company: 'Ledger & Co CPAs', domain: 'ledger-co-fixture.example', industry: 'accounting', location: 'Raleigh, NC', email: 'team@ledger-co-fixture.example', phone: '919-555-0111', contact: 'Erica Stone', google_rating: 4.9, google_review_count: 63, audit_profile: 'high_deficiency_high_value' },
  { company: 'Urban Eats Bistro', domain: 'urban-eats-fixture.example', industry: 'restaurant', location: 'Nashville, TN', email: 'hello@urban-eats-fixture.example', phone: '615-555-0112', contact: 'Marco Silva', google_rating: 4.4, google_review_count: 520, audit_profile: 'weak_economics' },
  { company: 'Precision Plumbing Co', domain: 'precision-plumb-fixture.example', industry: 'plumbing', location: 'Phoenix, AZ', email: 'dispatch@precision-plumb-fixture.example', phone: '602-555-0113', contact: 'Rick Alvarez', google_rating: 4.7, google_review_count: 118, hiring_signal: true, audit_profile: 'high_deficiency_high_value' },
  { company: 'Atlas Property Advisors', domain: 'atlas-prop-fixture.example', industry: 'property_management', location: 'Atlanta, GA', email: 'leasing@atlas-prop-fixture.example', phone: '404-555-0114', contact: 'Monica Hayes', google_rating: 4.1, google_review_count: 33, multi_location: true, audit_profile: 'moderate_deficiency_strong_business' },
  { company: 'Brightpath Med Spa', domain: 'brightpath-med-fixture.example', industry: 'med_spa', location: 'Scottsdale, AZ', email: 'care@brightpath-med-fixture.example', phone: '480-555-0115', contact: 'Dr. Kim Park', google_rating: 4.8, google_review_count: 167, recent_growth_signal: true, audit_profile: 'adequate_site_strong_business' },
  { company: 'Cornerstone Electrical', domain: 'cornerstone-elec-fixture.example', industry: 'electrical', location: 'Salt Lake City, UT', email: 'service@cornerstone-elec-fixture.example', phone: '801-555-0116', contact: 'Joe Miller', google_rating: 4.5, google_review_count: 89, audit_profile: 'high_deficiency_high_value' },
  { company: 'Willow Creek Family Law', domain: 'willow-creek-law-fixture.example', industry: 'legal', location: 'Boise, ID', email: 'intake@willow-creek-law-fixture.example', phone: '208-555-0117', contact: 'Allison Grant', google_rating: 4.6, google_review_count: 44, audit_profile: 'moderate_deficiency_strong_business' },
  { company: 'Budget Cleaners LLC', domain: 'budget-clean-fixture.example', industry: 'cleaning', location: 'Fresno, CA', email: null, phone: '559-555-0118', contact: null, google_rating: 3.9, google_review_count: 12, audit_profile: 'weak_economics' },
  { company: 'Stonebridge Architecture', domain: 'stonebridge-arch-fixture.example', industry: 'architecture_engineering', location: 'Seattle, WA', email: 'studio@stonebridge-arch-fixture.example', phone: '206-555-0119', contact: 'Helen Wright', google_rating: 4.9, google_review_count: 28, audit_profile: 'high_deficiency_high_value' },
  { company: 'Inactive Ventures LLC', domain: 'inactive-ventures-fixture.example', industry: 'professional_services', location: 'Remote', email: null, phone: null, contact: null, operating_status: 'inactive', audit_profile: 'insufficient_evidence' },
  { company: 'Premier Auto Repair', domain: 'premier-auto-fixture.example', industry: 'auto', location: 'Kansas City, MO', email: 'service@premier-auto-fixture.example', phone: '816-555-0121', contact: 'Mike Torres', google_rating: 4.7, google_review_count: 203, audit_profile: 'moderate_deficiency_strong_business' },
  { company: 'Lakeview Event Venue', domain: 'lakeview-events-fixture.example', industry: 'restaurant', location: 'Milwaukee, WI', email: 'events@lakeview-events-fixture.example', phone: '414-555-0122', contact: 'Sasha Reed', google_rating: 4.6, google_review_count: 88, advertising_signal: true, audit_profile: 'moderate_deficiency_strong_business' },
  { company: 'Heritage Financial Planning', domain: 'heritage-fin-fixture.example', industry: 'accounting', location: 'Hartford, CT', email: 'advisors@heritage-fin-fixture.example', phone: '860-555-0123', contact: 'Robert Klein', google_rating: 4.8, google_review_count: 57, audit_profile: 'high_deficiency_high_value' },
  { company: 'Sunrise Pediatric Clinic', domain: 'sunrise-peds-fixture.example', industry: 'dental', location: 'Orlando, FL', email: 'appointments@sunrise-peds-fixture.example', phone: '407-555-0124', contact: 'Dr. Luis Morales', google_rating: 4.9, google_review_count: 176, audit_profile: 'adequate_site_strong_business' },
  { company: 'Craftsmen Builders Group', domain: 'craftsmen-build-fixture.example', industry: 'home_services', location: 'Richmond, VA', email: 'build@craftsmen-build-fixture.example', phone: '804-555-0125', contact: 'Ethan Cole', google_rating: 4.5, google_review_count: 64, hiring_signal: true, multi_location: true, audit_profile: 'high_deficiency_high_value' },
]);

function buildFixtureAudit(domain, profile) {
  const base = {
    domain,
    audited_at: new Date().toISOString(),
    findings: [],
    technical_evidence: {},
  };

  const profiles = {
    high_deficiency_high_value: () => ({
      ...base,
      findings: [
        { id: 'f1', evidence_class: 'MEASURED', category: 'performance', summary: 'Mobile performance score 38/100 (PageSpeed Insights)', measurement: { strategy: 'mobile', performance_score: 38 }, source: 'fixture', ref: 'pagespeed:performance:mobile' },
        { id: 'f2', evidence_class: 'MEASURED', category: 'performance', summary: 'Mobile LCP measured 5.4 s', measurement: { strategy: 'mobile', metric: 'lcp', numeric_value: 5400 }, source: 'fixture', ref: 'pagespeed:lcp:mobile' },
        { id: 'f3', evidence_class: 'OBSERVED', category: 'conversion_structure', summary: 'No obvious phone, email, form, or contact link detected on homepage', source: 'fixture', ref: 'conversion:none_detected' },
        { id: 'f4', evidence_class: 'OBSERVED', category: 'technical_health', summary: 'Missing viewport meta tag', source: 'fixture', ref: 'mobile:viewport' },
        { id: 'f5', evidence_class: 'MEASURED', category: 'accessibility', summary: '3 image(s) missing non-empty alt text on homepage', measurement: { missing_alt_count: 3 }, source: 'fixture', ref: 'a11y:img_alt' },
      ],
      technical_evidence: { performance: [], accessibility: [], technical_health: [], conversion_structure: [] },
    }),
    moderate_deficiency_strong_business: () => ({
      ...base,
      findings: [
        { id: 'f1', evidence_class: 'MEASURED', category: 'performance', summary: 'Mobile performance score 58/100 (PageSpeed Insights)', measurement: { performance_score: 58 }, source: 'fixture', ref: 'pagespeed:performance:mobile' },
        { id: 'f2', evidence_class: 'OBSERVED', category: 'conversion_structure', summary: 'Form element present on homepage', source: 'fixture', ref: 'conversion:form' },
        { id: 'f3', evidence_class: 'OBSERVED', category: 'technical_health', summary: 'Missing meta description', source: 'fixture', ref: 'seo:description' },
      ],
    }),
    adequate_site_strong_business: () => ({
      ...base,
      findings: [
        { id: 'f1', evidence_class: 'MEASURED', category: 'performance', summary: 'Mobile performance score 82/100 (PageSpeed Insights)', measurement: { performance_score: 82 }, source: 'fixture', ref: 'pagespeed:performance:mobile' },
        { id: 'f2', evidence_class: 'OBSERVED', category: 'conversion_structure', summary: 'Phone link present on homepage', source: 'fixture', ref: 'conversion:phone' },
        { id: 'f3', evidence_class: 'OBSERVED', category: 'conversion_structure', summary: 'Contact link present in homepage markup', source: 'fixture', ref: 'conversion:contact_link' },
      ],
    }),
    weak_economics: () => ({
      ...base,
      findings: [
        { id: 'f1', evidence_class: 'MEASURED', category: 'performance', summary: 'Mobile performance score 35/100 (PageSpeed Insights)', measurement: { performance_score: 35 }, source: 'fixture', ref: 'pagespeed:performance:mobile' },
        { id: 'f2', evidence_class: 'MEASURED', category: 'performance', summary: 'Mobile LCP measured 6.1 s', measurement: { metric: 'lcp', numeric_value: 6100 }, source: 'fixture', ref: 'pagespeed:lcp:mobile' },
        { id: 'f3', evidence_class: 'OBSERVED', category: 'conversion_structure', summary: 'No obvious phone, email, form, or contact link detected on homepage', source: 'fixture', ref: 'conversion:none_detected' },
      ],
    }),
    high_deficiency_thin_contact: () => ({
      ...base,
      findings: [
        { id: 'f1', evidence_class: 'MEASURED', category: 'performance', summary: 'Mobile performance score 41/100 (PageSpeed Insights)', measurement: { performance_score: 41 }, source: 'fixture', ref: 'pagespeed:performance:mobile' },
        { id: 'f2', evidence_class: 'OBSERVED', category: 'technical_health', summary: 'Homepage URL is not HTTPS', source: 'fixture', ref: 'technical:no_https' },
      ],
    }),
    insufficient_evidence: () => ({
      ...base,
      findings: [
        { id: 'f1', evidence_class: 'UNKNOWN', category: 'technical_health', summary: 'Homepage fetch failed during audit', source: 'fixture', ref: 'technical:fetch_failed' },
      ],
    }),
  };

  const builder = profiles[profile] || profiles.moderate_deficiency_strong_business;
  return builder();
}

function createFixtureAuditProvider() {
  const map = new Map(FIXTURE_BUSINESSES.map((b) => [b.domain.replace(/^www\./, '').toLowerCase(), b.audit_profile]));
  return (domain) => {
    const profile = map.get(domain) || 'moderate_deficiency_strong_business';
    return buildFixtureAudit(domain, profile);
  };
}

module.exports = {
  COHORT_TAG,
  COHORT_SIZE,
  FIXTURE_BUSINESSES,
  buildFixtureAudit,
  createFixtureAuditProvider,
};
