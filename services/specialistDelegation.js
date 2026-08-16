'use strict';

/**
 * SPEC-098 — Max Specialist Delegation Contract (service facade).
 *
 * Canonical language for Max → specialist and specialist → Max.
 * Persistence, tenant isolation, authority, and evaluation live in
 * packages/max/specialistDelegation. This module is the app-level entry.
 */

const specialistDelegation = require('../packages/max/specialistDelegation');

module.exports = specialistDelegation;
