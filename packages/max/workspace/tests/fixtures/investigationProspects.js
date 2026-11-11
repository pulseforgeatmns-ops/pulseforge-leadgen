'use strict';
const { normalizeContact, noteFor } = require('../../../../../services/reviewOnlyProspectImportService');
const QUESTION = 'Of these six contact_now prospects, which should Levi prioritize first, considering his working financing range, deal readiness, security position, available evidence, and likelihood of progressing to a funded transaction? Rank only where evidence supports it. Explain blockers and do not initiate outreach.';
const inputs = [
  { name: 'Alex Example', fit: 'Financing request: $12,000; Deal stage: purchase contract recorded; Security position: first lien claimed; Exit plan: sale; Closing date: 2026-12-01', risks: 'Title and borrower claims are unverified.' },
  { name: 'Blair Example', fit: 'Financing request: $30,000; Deal stage: purchase contract recorded; Security position: second lien claimed; Exit plan: refinance', risks: 'Existing senior debt needs verification.' },
  { name: 'Casey Example', fit: 'Financing request: $10,000; Deal stage: property identified; Exit plan: sale', risks: 'Lien position is unknown.' },
  { name: 'Devon Example', fit: 'Financing request: $8,000; Deal stage: early review', risks: 'Security and exit plan are unknown.' },
  { name: 'Emery Example', fit: 'Deal stage: property identified', risks: 'Amount, security and exit plan are unknown.' },
  { name: 'Frankie Example', fit: 'Public profile describes real-estate investing.', risks: 'No specific funding request is recorded.' },
];
function fixture(clientId = 21) {
  return inputs.map((input, index) => {
    const c = normalizeContact({ ...input, reviewState: 'contact_now', market: 'Example County', category: 'borrower',
      sourceUrl: `https://example.invalid/evidence/${index + 1}` }, index);
    return { id: `00000000-0000-0000-0000-${String(index + 1).padStart(12, '0')}`, client_id: clientId,
      first_name: c.firstName, last_name: c.lastName, status: c.status, icp_score: c.icpScore,
      email: null, phone: null, source: 'review_only_import', service_area_match: c.market, notes: noteFor(c),
      do_not_contact: false, ao_outreach_review_required: true, setter_visible: false,
      acquisition_metadata: { importMode: 'review_only', outreachAuthorized: false,
        reviewState: c.reviewState, category: c.category, sourceUrls: c.sourceUrls } };
  });
}
const bundle = rows => ({ tenantId: String(rows[0]?.client_id || 21), clientId: rows[0]?.client_id || 21,
  prospects: { records: { status: 'found', rows, complete: true } } });
module.exports = { fixture, bundle, QUESTION };
