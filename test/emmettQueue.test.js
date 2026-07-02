const assert = require('assert');
const fs = require('fs');
const path = require('path');

const source = fs.readFileSync(path.join(__dirname, '..', 'emmettAgent.js'), 'utf8');
const selectionStart = source.indexOf('async function getProspectsForEmail');
const selectionEnd = source.indexOf('async function recalcStalledNoResponseProspects');
const selectionSource = source.slice(selectionStart, selectionEnd);

assert(selectionStart >= 0 && selectionEnd > selectionStart, 'candidate selection function must exist');
assert(!/LIMIT\s+100/i.test(selectionSource), 'readiness candidates must not be truncated before evaluation');

console.log('Emmett queue starvation test passed');
