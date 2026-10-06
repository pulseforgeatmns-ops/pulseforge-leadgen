'use strict';

const { normalizeText } = require('../stateIngestion/claimParser');

function synthesizeRowUnderstandingText({
  instruction = null,
  rowValues = {},
  sheetName = null,
  rowNumber = null,
  filename = null,
}) {
  const parts = [];
  if (instruction && String(instruction).trim()) {
    parts.push(String(instruction).trim());
  }
  const loc = [
    filename ? `file ${filename}` : null,
    sheetName ? `sheet ${sheetName}` : null,
    rowNumber != null ? `row ${rowNumber}` : null,
  ].filter(Boolean).join(', ');
  if (loc) parts.push(`(${loc})`);

  const company = rowValues.company || rowValues.account || rowValues.account_name;
  if (company) parts.push(`Account ${normalizeText(company)}.`);

  const contact = rowValues.contact || rowValues.contact_name || rowValues.person;
  if (contact) parts.push(`Contact ${normalizeText(contact)}.`);

  if (rowValues.notes) parts.push(String(rowValues.notes));
  if (rowValues.next_step) parts.push(`Next step: ${normalizeText(rowValues.next_step)}.`);
  if (rowValues.status) {
    parts.push(`Status field (interpret with care): ${normalizeText(rowValues.status)}.`);
  }
  if (rowValues.ao) parts.push(`AO ${normalizeText(rowValues.ao)}.`);

  for (const [key, value] of Object.entries(rowValues)) {
    if (['company', 'account', 'account_name', 'contact', 'contact_name', 'person', 'notes', 'next_step', 'status', 'ao'].includes(key)) {
      continue;
    }
    if (value != null && String(value).trim()) {
      parts.push(`${key}: ${normalizeText(value)}`);
    }
  }

  return parts.join(' ').replace(/\s+/g, ' ').trim();
}

module.exports = {
  synthesizeRowUnderstandingText,
};
