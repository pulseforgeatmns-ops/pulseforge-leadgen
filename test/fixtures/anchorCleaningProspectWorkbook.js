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

const PRODUCTION_RECONCILE_MESSAGE = 'These are my updated accounts from this week. Review all 16 rows in the workbook, compare each one against what is already in PulseForge, and reconcile every account, contact, note, status, and follow-up you can safely identify. Show me the row-by-row changes, conflicts, and anything that needs clarification. Do not save anything yet.';

module.exports = {
  anchorCleaningProspectSheets,
  PRODUCTION_RECONCILE_MESSAGE,
};
