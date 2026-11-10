'use strict';
const TARGET = Object.freeze({
  project: '5f6c50eb-6a61-4649-885e-dc3f3b80a2e5',
  environment: 'bded6fd2-b370-45be-9dd0-6a8f3683608e',
  service: 'e88561a7-a46e-4f0f-b6aa-ffa4464cf4f9',
  postgres: '5acd3fa8-5e3e-4761-bd80-60cd8a59142b',
});
function stagingConfig(env) {
  const exact = {
    RAILWAY_PROJECT_ID: TARGET.project,
    RAILWAY_ENVIRONMENT_ID: TARGET.environment,
    RAILWAY_SERVICE_ID: TARGET.service,
    MAX_CONTINUITY_DATABASE_SERVICE_ID: TARGET.postgres,
    MAX_CONTINUITY_REHEARSAL: 'staging-only',
  };
  for (const [key, expected] of Object.entries(exact)) {
    if (env[key] !== expected) throw new Error('staging_scope_rejected');
  }
  // There is deliberately no DATABASE_URL fallback or public/TCP proxy option.
  let url;
  try { url = new URL(env.MAX_CONTINUITY_DATABASE_URL); } catch { throw new Error('staging_database_rejected'); }
  if (url.protocol !== 'postgresql:' || url.hostname !== 'postgres.railway.internal' ||
      (url.port && url.port !== '5432') || url.pathname !== '/max_continuity_rehearsal' ||
      decodeURIComponent(url.username) !== 'max_continuity_rehearsal' || !url.password || url.search || url.hash) {
    throw new Error('staging_database_rejected');
  }
  return { connectionString: url.href, host: url.hostname, port: 5432, requireRestrictedRole: true };
}
function cleanEnvironment(config) {
  // No inherited keys, NODE_OPTIONS, PG*, provider credentials, routing flags or
  // dotenv file. The parent runs the child in an empty temporary directory.
  return {
    PATH: '/usr/bin:/bin',
    RAILWAY_PROJECT_ID: TARGET.project, RAILWAY_ENVIRONMENT_ID: TARGET.environment,
    RAILWAY_SERVICE_ID: TARGET.service, MAX_CONTINUITY_DATABASE_SERVICE_ID: TARGET.postgres,
    MAX_CONTINUITY_REHEARSAL: 'staging-only',
    LANG: 'C', LC_ALL: 'C',
    DATABASE_URL: 'postgresql://127.0.0.1:1/test_only', DATABASE_SSL: 'false',
    MAX_CONTINUITY_WORKER_CONFIG: JSON.stringify(config),
  };
}
module.exports = { TARGET, stagingConfig, cleanEnvironment };
