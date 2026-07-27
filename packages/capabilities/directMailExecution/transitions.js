'use strict';

/**
 * Deterministic Direct Mail Execution state transitions (SPEC-035).
 */

const { EXECUTION_STATUS } = require('./types');

/**
 * Allowed directed edges. Delivered is optional (Mailed → Responded allowed).
 * @type {Readonly<Record<string, readonly string[]>>}
 */
const ALLOWED_TRANSITIONS = Object.freeze({
  [EXECUTION_STATUS.DRAFT]: Object.freeze([EXECUTION_STATUS.READY_TO_PRINT]),
  [EXECUTION_STATUS.READY_TO_PRINT]: Object.freeze([EXECUTION_STATUS.PRINTING]),
  [EXECUTION_STATUS.PRINTING]: Object.freeze([EXECUTION_STATUS.PRINTED]),
  [EXECUTION_STATUS.PRINTED]: Object.freeze([EXECUTION_STATUS.ASSEMBLING]),
  [EXECUTION_STATUS.ASSEMBLING]: Object.freeze([
    EXECUTION_STATUS.READY_TO_MAIL,
  ]),
  [EXECUTION_STATUS.READY_TO_MAIL]: Object.freeze([EXECUTION_STATUS.MAILED]),
  [EXECUTION_STATUS.MAILED]: Object.freeze([
    EXECUTION_STATUS.DELIVERED,
    EXECUTION_STATUS.RESPONDED,
  ]),
  [EXECUTION_STATUS.DELIVERED]: Object.freeze([EXECUTION_STATUS.RESPONDED]),
  [EXECUTION_STATUS.RESPONDED]: Object.freeze([EXECUTION_STATUS.COMPLETED]),
  [EXECUTION_STATUS.COMPLETED]: Object.freeze([]),
});

/**
 * Statuses at which campaign artifacts are locked (Printing and beyond).
 */
const LOCKED_FROM_STATUS = EXECUTION_STATUS.PRINTING;

/**
 * @param {string} from
 * @param {string} to
 * @returns {boolean}
 */
function canTransition(from, to) {
  const allowed = ALLOWED_TRANSITIONS[from];
  if (!allowed) return false;
  return allowed.includes(to);
}

/**
 * @param {string} status
 * @returns {boolean}
 */
function isLockedStatus(status) {
  const order = [
    EXECUTION_STATUS.PRINTING,
    EXECUTION_STATUS.PRINTED,
    EXECUTION_STATUS.ASSEMBLING,
    EXECUTION_STATUS.READY_TO_MAIL,
    EXECUTION_STATUS.MAILED,
    EXECUTION_STATUS.DELIVERED,
    EXECUTION_STATUS.RESPONDED,
    EXECUTION_STATUS.COMPLETED,
  ];
  return order.includes(status);
}

/**
 * Next status on the primary path (skips optional Delivered).
 * @param {string} status
 * @returns {string|null}
 */
function nextPrimaryStatus(status) {
  const primary = {
    [EXECUTION_STATUS.DRAFT]: EXECUTION_STATUS.READY_TO_PRINT,
    [EXECUTION_STATUS.READY_TO_PRINT]: EXECUTION_STATUS.PRINTING,
    [EXECUTION_STATUS.PRINTING]: EXECUTION_STATUS.PRINTED,
    [EXECUTION_STATUS.PRINTED]: EXECUTION_STATUS.ASSEMBLING,
    [EXECUTION_STATUS.ASSEMBLING]: EXECUTION_STATUS.READY_TO_MAIL,
    [EXECUTION_STATUS.READY_TO_MAIL]: EXECUTION_STATUS.MAILED,
    [EXECUTION_STATUS.MAILED]: EXECUTION_STATUS.RESPONDED,
    [EXECUTION_STATUS.DELIVERED]: EXECUTION_STATUS.RESPONDED,
    [EXECUTION_STATUS.RESPONDED]: EXECUTION_STATUS.COMPLETED,
  };
  return primary[status] || null;
}

/**
 * Assert transition or throw with a stable error code.
 * @param {string} from
 * @param {string} to
 */
function assertTransition(from, to) {
  if (!canTransition(from, to)) {
    const err = new Error(
      `Illegal execution transition: ${from} → ${to}`
    );
    err.code = 'illegal_transition';
    err.from = from;
    err.to = to;
    throw err;
  }
}

module.exports = {
  ALLOWED_TRANSITIONS,
  LOCKED_FROM_STATUS,
  canTransition,
  isLockedStatus,
  nextPrimaryStatus,
  assertTransition,
};
