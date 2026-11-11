'use strict';
// Six synthetic review-only prospects written the way the real LA Endeavors
// import records them: free-text fit evidence and risks, no `Label: value`
// fields. Built through the real normalizeContact/noteFor import path. Names,
// URLs and amounts are synthetic; no production identity is used.
const { normalizeContact, noteFor } = require('../../../../../services/reviewOnlyProspectImportService');

const NARRATIVE_INPUTS = [
  { name: 'Zane Example',
    fit: 'Active flipper requesting $35k for rehab on a purchased SFR. Proposed first lien on the subject property. Exit via DSCR refinance after rehab. Targeting close in ~3 weeks.',
    risks: 'Request exceeds the $5k–$20k working range. Title, lien position and DSCR eligibility unverified.' },
  { name: 'Brook Example',
    fit: 'Under contract on a duplex; needs $15,000 equity gap. Offers first-position lien. Exit: sale after 4-month rehab.',
    risks: 'Contract and ARV not verified. Closing date not recorded.' },
  { name: 'Cam Example',
    fit: 'Seeking $8k–$12k for cosmetic rehab on a property owned free and clear. Plans to refinance.',
    risks: 'Ownership not verified; timeline unknown.' },
  { name: 'Dale Example',
    fit: 'Rehab budget of $60k on a $180k purchase; ARV $260k. Exit via sale after a 5-month rehab.',
    risks: 'No specific financing request recorded.' },
  { name: 'Eli Example',
    fit: 'Two properties: requesting $12k on one and $18k on the other. First lien on one property, second lien on the other.',
    risks: 'Unclear which property the request applies to.' },
  { name: 'Fay Example',
    fit: 'Public profile describes BRRRR investing; no specific funding request or lien position recorded.',
    risks: 'No active deal recorded.' },
];

function narrativeFixture(clientId = 21, inputs = NARRATIVE_INPUTS) {
  return inputs.map((input, index) => {
    const c = normalizeContact({ ...input, reviewState: 'contact_now', market: 'Example County', category: 'borrower',
      sourceUrl: `https://example.invalid/narrative/${index + 1}` }, index);
    return { id: `00000000-0000-0000-0000-1000000000${String(index + 1).padStart(2, '0')}`, client_id: clientId,
      first_name: c.firstName, last_name: c.lastName, status: c.status, icp_score: c.icpScore,
      email: null, phone: null, source: 'review_only_import', service_area_match: c.market, notes: noteFor(c),
      do_not_contact: false, ao_outreach_review_required: true, setter_visible: false,
      acquisition_metadata: { importMode: 'review_only', outreachAuthorized: false,
        reviewState: c.reviewState, category: c.category, sourceUrls: c.sourceUrls } };
  });
}

module.exports = { NARRATIVE_INPUTS, narrativeFixture };
