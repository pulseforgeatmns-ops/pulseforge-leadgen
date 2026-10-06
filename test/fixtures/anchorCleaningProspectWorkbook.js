'use strict';

/** Production regression fixture — Anchor Cleaning Prospect List.xlsx (16 rows, Sheet1). */
function anchorCleaningProspectSheets() {
  const companies = [
    'Exeter Phillips',
    'ABC Manufacturing',
    'Granite State Daycare',
    'Manchester Legal Group',
    'Bedford CPA Partners',
    'Hooksett Office Park',
    'Londonderry Dental',
    'Auburn Property Mgmt',
    'Exeter Packaging',
    'Northside Law',
    'Valley Accounting',
    'Riverwalk Offices',
    'Summit Manufacturing',
    'Keystone Realty',
    'Pine Hill Clinics',
    'Metro Business Center',
  ];
  return [{
    sheet: 'Sheet1',
    rows: companies.map((company, idx) => ({
      rowNumber: idx + 2,
      values: {
        company,
        notes: idx % 3 === 0 ? 'Follow up this week' : '',
        status: idx === 2 ? 'Hot' : '',
      },
    })),
  }];
}

const PRODUCTION_RECONCILE_MESSAGE = 'These are my updated accounts from this week. Review all 16 rows, compare each one against what\'s already in PulseForge, and update every account, contact, note, status, and follow-up you can safely reconcile. Show me which rows changed and which rows need clarification before you save anything.';

module.exports = {
  anchorCleaningProspectSheets,
  PRODUCTION_RECONCILE_MESSAGE,
};
