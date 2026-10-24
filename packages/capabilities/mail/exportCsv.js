'use strict';

/**
 * Mail merge + address label CSV exports (SPEC-033).
 */

/**
 * @param {object[]} packages
 * @returns {{ csv: string, rows: object[], filename: string }}
 */
function buildMailMergeCsv(packages = []) {
  const rows = packages
    .filter((p) => p.status !== 'skipped')
    .map((pkg) => {
      const letter = pkg.letter || {};
      const env = pkg.envelope || {};
      const summary = pkg.personalizationSummary || {};
      return {
        prospect_id: pkg.prospectId || '',
        status: pkg.status || '',
        recipient_name: letter.recipientName || env.recipientName || '',
        company_name: letter.companyName || env.companyName || '',
        mailing_address: env.mailingAddress || '',
        return_address: (env.returnAddress || '').replace(/\n/g, ' | '),
        personalized_opening: letter.personalizedOpening || '',
        value_proposition: letter.valueProposition || '',
        cta: letter.cta || '',
        signature: (letter.signature || '').replace(/\n/g, ' | '),
        letter_body: (letter.body || '').replace(/\n/g, ' | '),
        why_selected: summary.whySelected || '',
        letter_confidence: summary.letterConfidence != null ? summary.letterConfidence : '',
        insert_checklist: (pkg.insertChecklist || [])
          .filter((i) => i.included !== false)
          .map((i) => i.label || i.id)
          .join('; '),
        warnings: (pkg.warnings || []).join('; '),
        revision: pkg.revision != null ? pkg.revision : '',
      };
    });

  return {
    csv: toCsv(rows, MAIL_MERGE_COLUMNS),
    rows,
    filename: 'mail_merge.csv',
  };
}

/**
 * @param {object[]} packages
 * @returns {{ csv: string, rows: object[], filename: string }}
 */
function buildAddressLabelCsv(packages = []) {
  const rows = packages
    .filter((p) => p.status !== 'skipped')
    .map((pkg) => {
      const env = pkg.envelope || {};
      return {
        prospect_id: pkg.prospectId || '',
        status: pkg.status || '',
        recipient_name: env.recipientName || '',
        company_name: env.companyName || '',
        mailing_address: env.mailingAddress || '',
        return_address: (env.returnAddress || '').replace(/\n/g, ' | '),
        print_ready: pkg.status === 'ready_to_print' || pkg.status === 'approved' ? 'yes' : 'no',
      };
    });

  return {
    csv: toCsv(rows, ADDRESS_LABEL_COLUMNS),
    rows,
    filename: 'address_labels.csv',
  };
}

const MAIL_MERGE_COLUMNS = Object.freeze([
  'prospect_id',
  'status',
  'recipient_name',
  'company_name',
  'mailing_address',
  'return_address',
  'personalized_opening',
  'value_proposition',
  'cta',
  'signature',
  'letter_body',
  'why_selected',
  'letter_confidence',
  'insert_checklist',
  'warnings',
  'revision',
]);

const ADDRESS_LABEL_COLUMNS = Object.freeze([
  'prospect_id',
  'status',
  'recipient_name',
  'company_name',
  'mailing_address',
  'return_address',
  'print_ready',
]);

/**
 * @param {object[]} rows
 * @param {string[]} columns
 * @returns {string}
 */
function toCsv(rows, columns) {
  const lines = [columns.join(',')];
  for (const row of rows) {
    lines.push(columns.map((col) => csvEscape(row[col])).join(','));
  }
  return `${lines.join('\n')}\n`;
}

/**
 * @param {unknown} value
 * @returns {string}
 */
function csvEscape(value) {
  const s = value == null ? '' : String(value);
  if (/[",\n\r]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

module.exports = {
  buildMailMergeCsv,
  buildAddressLabelCsv,
  MAIL_MERGE_COLUMNS,
  ADDRESS_LABEL_COLUMNS,
  toCsv,
  csvEscape,
};
