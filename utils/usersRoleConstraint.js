'use strict';

const { USER_ROLES, ROLE_CHECK } = require('./userRoles');
// Both auth and portal startup must use this key on the same transaction/client.
const USERS_ROLE_LOCK = '91720260517';

async function withSchemaTransaction(pool, work) {
  const client = await pool.connect();
  let broken;
  try {
    await client.query('BEGIN');
    await client.query("SET LOCAL lock_timeout = '10s'");
    await client.query("SET LOCAL statement_timeout = '60s'");
    await client.query('SELECT pg_advisory_xact_lock($1::bigint)', [USERS_ROLE_LOCK]);
    const result = await work(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    try { await client.query('ROLLBACK'); } catch (rollbackError) { broken = rollbackError; }
    throw error;
  } finally {
    client.release(broken);
  }
}

function isManagedRoleCheck(definition) {
  // Only the historical app-owned, role-only IN check is safe to replace.
  // Do not infer ownership from a substring, name alone, or referenced column.
  const match = /^CHECK \(\(role = ANY \(ARRAY\[(.*)\]\)\)\)$/.exec(definition);
  if (!match) return false;
  const values = match[1].split(', ');
  return values.length > 0 && values.every(value => {
    const literal = /^'([a-z_]+)'::text$/.exec(value);
    return literal && USER_ROLES.includes(literal[1]);
  });
}

async function refreshUsersRoleConstraintOnClient(client) {
  const { rows: [relation] } = await client.query(`
    SELECT c.oid, n.nspname, quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS qualified
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = to_regclass('users')
  `);
  if (!relation) throw new Error('users table is required before portal initialization');
  if (relation.nspname !== 'public') throw new Error('users must resolve to public.users; explicit operator review required');
  await client.query(`LOCK TABLE ${relation.qualified} IN ACCESS EXCLUSIVE MODE`);
  const { rows } = await client.query(`
    SELECT pg_get_constraintdef(oid) AS definition
    FROM pg_constraint WHERE conrelid = $1 AND conname = 'users_role_check'
  `, [relation.oid]);
  if (rows.length && !isManagedRoleCheck(rows[0].definition)) {
    throw new Error('Unrecognized users_role_check; explicit operator review required');
  }
  if (rows.length) await client.query(`ALTER TABLE ${relation.qualified} DROP CONSTRAINT users_role_check`);
  await client.query(`ALTER TABLE ${relation.qualified} ADD CONSTRAINT users_role_check CHECK (role IN (${ROLE_CHECK}))`);
}

async function refreshUsersRoleConstraint(pool) {
  return withSchemaTransaction(pool, refreshUsersRoleConstraintOnClient);
}

module.exports = { withSchemaTransaction, refreshUsersRoleConstraintOnClient, refreshUsersRoleConstraint, USERS_ROLE_LOCK };
