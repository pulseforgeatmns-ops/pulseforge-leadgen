'use strict';

/**
 * Seed Client Playbooks (SPEC-028 / ADR-015).
 * Strategic assets — not application configuration.
 */

const { buildClientPlaybook } = require('./types');

/**
 * @returns {object[]}
 */
function seedClientPlaybooks() {
  return [
    buildClientPlaybook({
      id: 'pb_as_cleaning_co',
      clientId: 1,
      name: 'AS Cleaning Co. — Commercial Growth',
      version: '1.0',
      status: 'active',
      targetMarkets: [
        'Medical Offices',
        'Dental Practices',
        'Property Management Companies',
      ],
      idealCustomer: {
        primaryMarkets: [
          'Medical Offices',
          'Dental Practices',
          'Property Management Companies',
        ],
        secondaryMarkets: ['Professional Offices', 'Law Firms'],
        geographicCoverage: 'Greater Toronto Area',
        minimumCompanySize: 'Single-location professional office',
        industriesToAvoid: ['Restaurants', 'Residential-only properties'],
        buyingTriggers: [
          'Staffing gaps in janitorial coverage',
          'Lease turnover / new tenant fit-out',
          'Complaint-driven vendor switch',
          'Owner wants reliable recurring service',
        ],
      },
      valuePropositions: [
        'Owner-operated quality',
        'Reliable recurring service',
        'Flexible scheduling',
        'Responsive communication',
      ],
      brandVoice: 'relationship_first',
      preferredChannels: ['direct_mail', 'phone', 'email', 'linkedin'],
      outreachSequence: [
        { day: 1, channel: 'direct_mail', action: 'Letter', notes: 'Introduce owner-operated service' },
        { day: 4, channel: 'phone', action: 'Call', notes: 'Reference the letter' },
        { day: 7, channel: 'email', action: 'Email', notes: 'Offer free walkthrough' },
        { day: 10, channel: 'linkedin', action: 'LinkedIn', notes: 'Light touch if decision-maker found' },
        { day: 14, channel: 'phone', action: 'Follow-up', notes: 'Second call / close loop' },
      ],
      offers: [
        'Free walkthrough',
        'No-obligation quote',
        'Trial cleaning',
        'Quarterly review',
      ],
      constraints: [
        {
          type: 'call_window',
          rule: 'Do not call before 9 AM',
          detail: 'Local business hours; prefer Tuesday mornings',
        },
        {
          type: 'exclude_industry',
          rule: 'Avoid restaurants',
          detail: 'Out of ICP for commercial cleaning beachhead',
        },
        {
          type: 'focus',
          rule: 'Focus on recurring contracts',
          detail: 'Prefer ongoing facility work over one-off deep cleans',
        },
        {
          type: 'exclude_crm',
          rule: 'Exclude existing CRM contacts',
          detail: 'Skip companies already in the book or active pipeline',
        },
      ],
      successMetrics: [
        'Walkthroughs booked',
        'Qualified conversations',
        'Quotes sent',
        'Closed contracts',
        'Monthly recurring revenue',
      ],
      notes:
        'Strongest referrals come from property managers. Medical offices convert well after direct mail. Phone calls perform best Tuesday mornings.',
    }),
    buildClientPlaybook({
      id: 'pb_anchor_cleaning',
      clientId: 10,
      name: 'Anchor Cleaning — Manchester Commercial',
      version: '1.0',
      status: 'active',
      targetMarkets: ['Law Firms', 'Accounting Practices', 'Medical Offices'],
      idealCustomer: {
        primaryMarkets: ['Law Firms', 'Accounting Practices'],
        secondaryMarkets: ['Medical Offices', 'Professional Offices'],
        geographicCoverage:
          'Greater Manchester NH (Manchester, Bedford, Goffstown, Hooksett, Londonderry, Auburn)',
        minimumCompanySize: 'Single-tenant professional office',
        industriesToAvoid: [
          'Multi-office national firms',
          'Residential-only',
          'Restaurants',
        ],
        buyingTriggers: [
          'Unreliable current vendor',
          'New office lease',
          'Partner wants owner-attentive service',
        ],
      },
      valuePropositions: [
        'Local owner accountability',
        'Consistent commercial standards',
        'Clear communication',
        'Recurring contract focus',
      ],
      brandVoice: 'professional',
      preferredChannels: ['direct_mail', 'phone', 'email', 'linkedin'],
      outreachSequence: [
        { day: 1, channel: 'direct_mail', action: 'Letter' },
        { day: 4, channel: 'phone', action: 'Call' },
        { day: 7, channel: 'email', action: 'Email' },
        { day: 10, channel: 'linkedin', action: 'LinkedIn' },
        { day: 14, channel: 'phone', action: 'Follow-up' },
      ],
      offers: [
        'Free walkthrough',
        'No-obligation quote',
        'Pilot cleaning week',
      ],
      constraints: [
        {
          type: 'call_window',
          rule: 'Do not call before 9 AM',
        },
        {
          type: 'exclude_industry',
          rule: 'Avoid restaurants',
        },
        {
          type: 'focus',
          rule: 'Focus on recurring contracts',
        },
        {
          type: 'exclude_crm',
          rule: 'Exclude existing CRM contacts',
        },
      ],
      successMetrics: [
        'Walkthroughs booked',
        'Quotes sent',
        'Closed contracts',
        'Monthly recurring revenue',
      ],
      notes:
        'Beachhead is single-tenant professional offices in the Manchester pilot cluster. Disqualify national multi-office firms.',
    }),
  ];
}

module.exports = {
  seedClientPlaybooks,
};
