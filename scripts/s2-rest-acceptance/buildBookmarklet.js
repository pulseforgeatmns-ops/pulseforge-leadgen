'use strict';
/* Collapses s2RestAcceptance.js into a javascript: bookmarklet (stdout). */
const fs = require('fs');
const path = require('path');
const src = fs.readFileSync(path.join(__dirname, 's2RestAcceptance.js'), 'utf8');
const code = src
  .replace(/\/\*[\s\S]*?\*\//g, '')
  .split('\n').map((l) => l.trim()).filter(Boolean).join(' ');
if (/(^|[^:])\/\/(?![^']*'\))/.test(code.replace(/https:\\\/\\\//g, ''))) {
  throw new Error('line comment found; bookmarklet would break');
}
new Function(code); // syntax check only, not executed
process.stdout.write('javascript:' + encodeURIComponent(code) + '\n');
