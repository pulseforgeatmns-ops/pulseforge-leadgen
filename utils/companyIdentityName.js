'use strict';
// Division suffixes are an identity collision signal, never authority to merge
// distinct domains or move contacts between companies.
const companyIdentityNameKey = value => String(value || '').toLowerCase()
  .replace(/[^a-z0-9]+/g, ' ').trim()
  .replace(/ (commercial|residential) (division|department)$/, '')
  .replace(/ /g, '');
const COMPANY_IDENTITY_NAME_SQL = "replace(regexp_replace(trim(regexp_replace(lower(name), '[^a-z0-9]+', ' ', 'g')), ' (commercial|residential) (division|department)$', ''), ' ', '')";
module.exports = { companyIdentityNameKey, COMPANY_IDENTITY_NAME_SQL };
