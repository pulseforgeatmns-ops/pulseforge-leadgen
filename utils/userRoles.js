/** Canonical dashboard / portal roles — keep in sync with users_role_check migration. */
const USER_ROLES = [
  'admin',
  'manager',
  'setter',
  'closer',
  'sales',
  'viewer',
  'client',
  'ao',
  'cleaner',
  'facility_client',
];

const ROLE_CHECK = USER_ROLES.map(role => `'${role}'`).join(', ');

module.exports = {
  USER_ROLES,
  ROLE_CHECK,
};
