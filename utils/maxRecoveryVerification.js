'use strict';

const crypto = require('crypto');
const fs = require('fs');
const { execFileSync } = require('child_process');

const REQUIRED_OPERATIONAL_TABLES = Object.freeze([
  'clients','companies','prospects','touchpoints','agent_log','agent_actions','cal_queue','email_events',
]);

function sha256File(path) {
  const hash = crypto.createHash('sha256');
  hash.update(fs.readFileSync(path));
  return hash.digest('hex');
}

function verifyRecoveryArtifact({
  path, expectedSha256, restoreProcedurePath, durableStorageVerified = false,
  listArchive = archivePath => execFileSync('pg_restore',['--list',archivePath],{encoding:'utf8',maxBuffer:20*1024*1024}),
}) {
  const report = {
    path,
    artifact_found: false,
    restrictive_permissions: false,
    hash_verified: false,
    archive_readable: false,
    required_tables: {},
    restore_procedure_documented: false,
    durable_storage_verified: durableStorageVerified === true,
    fully_verified: false,
    errors: [],
  };
  if (!path || !fs.existsSync(path)) {
    report.errors.push('artifact_not_found');
    return report;
  }
  report.artifact_found = true;
  const stat = fs.statSync(path);
  report.mode = (stat.mode & 0o777).toString(8).padStart(3,'0');
  report.size_bytes = stat.size;
  report.restrictive_permissions = (stat.mode & 0o077) === 0;
  report.actual_sha256 = sha256File(path);
  report.expected_sha256 = expectedSha256 || null;
  report.hash_verified = Boolean(expectedSha256 && report.actual_sha256 === expectedSha256);
  try {
    const toc = listArchive(path);
    report.archive_readable = /Format: CUSTOM/i.test(toc) || /TABLE public/i.test(toc);
    for (const table of REQUIRED_OPERATIONAL_TABLES) {
      report.required_tables[table] = new RegExp(`TABLE public ${table}(?:\\s|$)`).test(toc);
    }
  } catch (error) {
    report.errors.push(`archive_unreadable:${String(error.message).slice(0,300)}`);
  }
  report.restore_procedure_documented = Boolean(restoreProcedurePath && fs.existsSync(restoreProcedurePath));
  report.fully_verified = report.artifact_found && report.restrictive_permissions && report.hash_verified
    && report.archive_readable && Object.values(report.required_tables).every(Boolean)
    && report.restore_procedure_documented && report.durable_storage_verified;
  return report;
}

module.exports = { REQUIRED_OPERATIONAL_TABLES, sha256File, verifyRecoveryArtifact };
