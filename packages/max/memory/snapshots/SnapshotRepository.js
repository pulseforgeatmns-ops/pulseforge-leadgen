'use strict';

/**
 * SnapshotRepository contract (SPEC-003).
 *
 * Append-only. No snapshot mutation.
 * Implementations must be interchangeable for repository parity tests.
 */

const SNAPSHOT_REPOSITORY_METHODS = Object.freeze([
  'append',
  'getById',
  'listByCompany',
  'latest',
  'count',
  'clear',
]);

/**
 * @typedef {object} SnapshotRepository
 * @property {(snapshot: object) => Promise<object>|object} append
 * @property {(tenantId: string, snapshotId: string) => Promise<object|null>|object|null} getById
 * @property {(tenantId: string, companyId: string, options?: object) => Promise<object[]>|object[]} listByCompany
 * @property {(tenantId: string, companyId: string) => Promise<object|null>|object|null} latest
 * @property {(tenantId?: string) => Promise<number>|number} count
 * @property {() => Promise<void>|void} clear
 */

/**
 * @param {object} repo
 * @returns {asserts repo is SnapshotRepository}
 */
function assertSnapshotRepository(repo) {
  if (!repo || typeof repo !== 'object') {
    throw new Error('SnapshotRepository is required');
  }
  for (const method of SNAPSHOT_REPOSITORY_METHODS) {
    if (typeof repo[method] !== 'function') {
      throw new Error(`SnapshotRepository missing ${method}()`);
    }
  }
}

/**
 * @param {object} repo
 */
function isSnapshotRepository(repo) {
  try {
    assertSnapshotRepository(repo);
    return true;
  } catch {
    return false;
  }
}

module.exports = {
  SNAPSHOT_REPOSITORY_METHODS,
  assertSnapshotRepository,
  isSnapshotRepository,
};
