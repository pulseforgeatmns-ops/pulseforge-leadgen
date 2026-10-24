'use strict';

/** Tony-style multi-sheet operational update fixture (MAX-SPREADSHEET-002 acceptance). */
function tonyUpdatedAccountsSheets() {
  return [
    {
      sheet: 'Prospects',
      rows: [
        {
          rowNumber: 2,
          values: {
            company: 'Exeter Phillips',
            ao: 'Tony',
            notes: 'Current cleaner still missing common areas. Waiting on callback Friday.',
            next_step: 'Call Friday',
          },
        },
        {
          rowNumber: 3,
          values: {
            company: 'Exeter Phillips',
            contact: 'Lisa',
            notes: 'Dave is NOT decision maker — Lisa handles vendors',
            phone: '603-555-4444',
          },
        },
        {
          rowNumber: 4,
          values: {
            company: 'Exeter',
            notes: 'Unclear which Exeter account',
          },
        },
        {
          rowNumber: 5,
          values: {
            company: 'Granite State Manufacturing',
            contact: 'Amy',
            ao: 'Tony',
            is_new: true,
          },
        },
        {
          rowNumber: 6,
          values: {
            company: 'ABC Manufacturing',
            contact: 'Sarah Collins',
            email: 'sarah@abc.example.com',
          },
        },
        {
          rowNumber: 7,
          values: {
            company: 'ABC Manufacturing',
            contact: 'Sarah Collins',
            phone: '603-555-9999',
          },
        },
        {
          rowNumber: 8,
          values: {
            company: 'Exeter Phillips',
            status: 'Dead',
          },
        },
        {
          rowNumber: 9,
          values: {
            company: 'Exeter Phillips',
            status: 'Hot',
          },
        },
      ],
    },
    {
      sheet: 'Follow Ups',
      rows: [
        {
          rowNumber: 2,
          values: {
            company: 'Exeter Phillips',
            next_step: 'Stop back Thursday',
          },
        },
      ],
    },
    {
      sheet: 'Notes',
      rows: [
        {
          rowNumber: 2,
          values: {
            company: 'ABC Manufacturing',
            notes: 'Employee unhappy with current cleaning contractor.',
          },
        },
      ],
    },
  ];
}

module.exports = {
  tonyUpdatedAccountsSheets,
};
