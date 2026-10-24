'use strict';

// Keep the original test:max as the full regression contract. These lanes make
// socket-bound tests explicit, never optional or silently omitted from CI.
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const root = path.resolve(__dirname, '..');
const integration = ['test/maxAttentionCronRoute.test.js', 'test/maxAttentionAuthorization.test.js'];
function manifests() {
  const command = require('../package.json').scripts['test:max'];
  if (!command.startsWith('node --test ')) throw new Error('Unsupported test:max contract; update lane manifest parser explicitly');
  const files = command.slice('node --test '.length).split(/\s+/).flatMap(pattern => {
    if (!pattern.includes('*')) return [pattern];
    if (!pattern.endsWith('/*.test.js')) throw new Error(`Unsupported test pattern: ${pattern}`);
    const dir = pattern.slice(0, -'/*.test.js'.length);
    return fs.readdirSync(path.join(root, dir)).filter(name => name.endsWith('.test.js')).sort().map(name => `${dir}/${name}`);
  });
  if (new Set(files).size !== files.length) throw new Error('Duplicate test entry');
  for (const file of integration) if (!files.includes(file)) throw new Error(`Missing integration contract: ${file}`);
  return { unit: files.filter(file => !integration.includes(file)), integration: [...integration], full: files };
}
if (require.main === module) {
  const lanes = manifests();
  if (process.argv[2] === '--list') console.log(JSON.stringify(lanes, null, 2));
  else {
    const lane = process.argv[2];
    if (!['unit', 'integration'].includes(lane)) throw new Error('Expected unit, integration, or --list');
    const args = lane === 'unit' ? ['--require', './test/helpers/denyWebsiteEvidenceNetwork.cjs', '--test'] : ['--test'];
    const env = { ...process.env };
    if (lane === 'unit') delete env.DATABASE_URL; // Unit fixtures never opt into DB integration.
    const result = spawnSync(process.execPath, [...args, ...lanes[lane]], { cwd: root, stdio: 'inherit', env });
    if (result.error) throw result.error;
    process.exitCode = result.status === null ? 1 : result.status;
  }
}
module.exports = { manifests };
