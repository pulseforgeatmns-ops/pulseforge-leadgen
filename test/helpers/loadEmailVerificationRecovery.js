'use strict';

const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Load the actual recovery service, but never import leadgen or its live
// verifier/DB clients. Both unit and PostgreSQL tests inject fixture verifiers.
module.exports = function loadEmailVerificationRecovery() {
  const filename = path.join(__dirname, '../../services/emailVerificationRecovery.js');
  const module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), {
    module,
    require(name) {
      if (name === '../utils/emailGuard') return require('../../utils/emailGuard');
      if (name === '../leadgen') return {
        resolveEmailVerification: async () => { throw new Error('Live verifier disabled in recovery tests'); },
      };
      throw new Error(`Unexpected recovery dependency: ${name}`);
    },
  }, { filename });
  return module.exports;
};
