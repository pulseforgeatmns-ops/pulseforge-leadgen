-- Isolated Signal web sessions and bounded login attempts. Existing users are read only.
CREATE TABLE IF NOT EXISTS signal_dashboard_session (
 sid varchar NOT NULL PRIMARY KEY,
 sess json NOT NULL,
 expire timestamp(6) NOT NULL
);
CREATE INDEX IF NOT EXISTS signal_dashboard_session_expire ON signal_dashboard_session(expire);
CREATE TABLE IF NOT EXISTS signal_dashboard_auth_attempts (
 ip_hash text NOT NULL PRIMARY KEY,
 attempts integer NOT NULL CHECK(attempts>=0),
 reset_at timestamptz NOT NULL
);
