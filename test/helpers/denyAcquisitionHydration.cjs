'use strict';
// Test-only fail-closed tripwire. The existing native admission import graph may
// load the runtime module, but bounded research must never enter its runtime or
// hydration APIs. Provider/admission/authority implementations stay untouched.
const runtime = require('../../services/acquisitionMissionRuntime');
let attempts = 0;
for (const [name,value] of Object.entries(runtime)) if (typeof value === 'function') {
  runtime[name] = function forbiddenAcquisitionRuntimeEntry() {
    attempts++;
    throw Object.assign(new Error(`Unexpected acquisition runtime/hydration entry: ${name}`), { code:'test_hydration_forbidden' });
  };
}
process.on('exit', () => { if (attempts) { process.stderr.write(`Blocked ${attempts} unexpected acquisition runtime/hydration entries.\n`); process.exitCode=1; } });
module.exports = { attempts:() => attempts };
