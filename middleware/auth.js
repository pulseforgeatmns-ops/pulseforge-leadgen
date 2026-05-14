const bcrypt = require('bcryptjs');
const pool = require('../db');

const ROLES = ['admin', 'manager', 'setter'];
let initPromise;

function isApiRequest(req) {
  return req.path.startsWith('/api/') || req.originalUrl.startsWith('/api/') || req.get('accept')?.includes('application/json');
}

async function ensureUsersTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL CHECK (role in ('admin', 'manager', 'setter')),
      active BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      last_login_at TIMESTAMPTZ
    )
  `);
}

async function getUserCount() {
  const { rows } = await pool.query('SELECT COUNT(*)::int AS count FROM users');
  return rows[0]?.count || 0;
}

async function initializeAuth() {
  await ensureUsersTable();
  const count = await getUserCount();
  if (count === 0 && process.env.ADMIN_EMAIL && process.env.ADMIN_PASSWORD) {
    const hash = await bcrypt.hash(process.env.ADMIN_PASSWORD, 12);
    await pool.query(`
      INSERT INTO users (name, email, password_hash, role, active)
      VALUES ($1, $2, $3, 'admin', true)
      ON CONFLICT (email) DO NOTHING
    `, [
      process.env.ADMIN_NAME || 'Pulseforge Admin',
      process.env.ADMIN_EMAIL.toLowerCase().trim(),
      hash,
    ]);
  }
}

function initAuth() {
  if (!initPromise) initPromise = initializeAuth();
  return initPromise;
}

function requireAuth(req, res, next) {
  if (req.session?.user) {
    req.user = req.session.user;
    return next();
  }

  if (req.session?.authenticated) {
    req.user = { id: null, name: 'Legacy Admin', email: 'legacy@pulseforge.local', role: 'admin' };
    req.session.user = req.user;
    return next();
  }

  if (isApiRequest(req)) return res.status(401).json({ error: 'Unauthorized' });
  return res.redirect('/login');
}

function requireRole(...roles) {
  return (req, res, next) => {
    const check = () => {
      if (roles.includes(req.user?.role)) return next();
      if (req.user?.role === 'setter' && !isApiRequest(req)) return res.redirect('/setter');
      return res.status(403).json({ error: 'Forbidden' });
    };

    if (req.user) return check();
    return requireAuth(req, res, err => (err ? next(err) : check()));
  };
}

module.exports = {
  ROLES,
  bcrypt,
  ensureUsersTable,
  getUserCount,
  initAuth,
  requireAuth,
  requireRole,
};
