'use strict';

/**
 * SPEC-CI-LEAN-001 — deterministic PR suite selection from changed paths.
 *
 * Usage:
 *   node .github/scripts/ci-plan-pr.js changed-files.txt
 *   CHANGED_FILES="a\nb" node .github/scripts/ci-plan-pr.js
 *
 * GitHub Actions output (append to GITHUB_OUTPUT):
 *   node .github/scripts/ci-plan-pr.js changed-files.txt --github-output
 */

const fs = require('fs');
const path = require('path');

const REPO_ROOT = path.join(__dirname, '..', '..');
const DOMAIN_MAP_PATH = path.join(REPO_ROOT, '.github', 'ci', 'domains.json');

function loadDomainMap() {
  return JSON.parse(fs.readFileSync(DOMAIN_MAP_PATH, 'utf8'));
}

function patternToRegExp(pattern) {
  const normalized = pattern.replace(/\\/g, '/');
  let re = '^';
  for (let i = 0; i < normalized.length; i += 1) {
    const ch = normalized[i];
    if (ch === '*') {
      if (normalized[i + 1] === '*') {
        re += '.*';
        i += 1;
      } else {
        re += '[^/]*';
      }
    } else if (/[.+?^${}()|[\]\\]/.test(ch)) {
      re += `\\${ch}`;
    } else {
      re += ch;
    }
  }
  if (normalized.endsWith('/')) {
    re += '.*';
  }
  re += '$';
  return new RegExp(re);
}

function pathMatchesPattern(filePath, pattern) {
  const normalizedFile = filePath.replace(/\\/g, '/');
  const normalizedPattern = pattern.replace(/\\/g, '/');
  if (normalizedPattern.endsWith('/')) {
    return normalizedFile.startsWith(normalizedPattern)
      || normalizedFile === normalizedPattern.slice(0, -1);
  }
  if (normalizedPattern.includes('*')) {
    return patternToRegExp(normalizedPattern).test(normalizedFile);
  }
  return normalizedFile === normalizedPattern
    || normalizedFile.startsWith(`${normalizedPattern}/`)
    || normalizedFile.startsWith(normalizedPattern);
}

function domainsForFile(filePath, domainMap) {
  const matched = new Set();
  for (const [domain, patterns] of Object.entries(domainMap.domainPaths)) {
    for (const pattern of patterns) {
      if (pathMatchesPattern(filePath, pattern)) {
        matched.add(domain);
        break;
      }
    }
  }
  return matched;
}

function escalationSuites(changedFiles, domainMap) {
  const suites = new Map();
  for (const filePath of changedFiles) {
    for (const rule of domainMap.escalationRules || []) {
      const hit = rule.paths.some((pattern) => pathMatchesPattern(filePath, pattern));
      if (!hit) continue;
      for (const suite of rule.suites) {
        if (!suites.has(suite)) {
          suites.set(suite, []);
        }
        suites.get(suite).push(`${filePath} → ${rule.id}: ${rule.reason}`);
      }
    }
  }
  return suites;
}

function buildPlan(changedFiles, { failClosed = false } = {}) {
  const domainMap = loadDomainMap();
  const suiteReasons = new Map();
  const changedDomains = new Set();

  const addSuite = (suite, reason) => {
    if (!suiteReasons.has(suite)) suiteReasons.set(suite, new Set());
    suiteReasons.get(suite).add(reason);
  };

  addSuite('global', 'Always on pull request (GLOBAL_CRITICAL)');

  if (failClosed) {
    for (const suite of domainMap.failClosedSuites) {
      addSuite(suite, 'Fail-closed: selector error or empty diff — run safe broad set');
    }
    return finalizePlan(changedFiles, changedDomains, suiteReasons, domainMap, failClosed);
  }

  for (const filePath of changedFiles) {
    const domains = domainsForFile(filePath, domainMap);
    for (const domain of domains) {
      changedDomains.add(domain);
      addSuite(domain, `Changed path: ${filePath}`);
    }
  }

  for (const [suite, reasons] of escalationSuites(changedFiles, domainMap)) {
    for (const reason of reasons) {
      addSuite(suite, reason);
    }
  }

  return finalizePlan(changedFiles, changedDomains, suiteReasons, domainMap, failClosed);
}

function finalizePlan(changedFiles, changedDomains, suiteReasons, domainMap, failClosed) {
  const suites = [...suiteReasons.keys()].sort();
  const skipped = Object.keys(domainMap.suites)
    .filter((name) => !suites.includes(name))
    .sort();

  const lines = [];
  lines.push('## CI-LEAN-001 selection');
  lines.push('');
  lines.push(`Changed files (${changedFiles.length}):`);
  if (changedFiles.length === 0) {
    lines.push('- _(none detected — global smoke only)_');
  } else {
    for (const file of changedFiles.slice(0, 40)) {
      lines.push(`- \`${file}\``);
    }
    if (changedFiles.length > 40) {
      lines.push(`- _…and ${changedFiles.length - 40} more_`);
    }
  }
  lines.push('');
  lines.push('Changed domains:');
  if (changedDomains.size === 0) {
    lines.push('- _(no domain-specific paths — global only)_');
  } else {
    for (const domain of [...changedDomains].sort()) {
      lines.push(`- ${domain}`);
    }
  }
  lines.push('');
  lines.push('Running:');
  for (const suite of suites) {
    const meta = domainMap.suites[suite] || { label: suite };
    const reasons = [...suiteReasons.get(suite)];
    lines.push(`- **${meta.label}** (\`${suite}\`) — ${reasons[0]}`);
    for (const extra of reasons.slice(1, 3)) {
      lines.push(`  - ${extra}`);
    }
    if (reasons.length > 3) {
      lines.push(`  - _…${reasons.length - 3} more reasons_`);
    }
  }
  lines.push('');
  lines.push('Skipped:');
  for (const suite of skipped) {
    const meta = domainMap.suites[suite] || { label: suite };
    lines.push(`- ${meta.label} — no affected paths`);
  }
  if (failClosed) {
    lines.push('');
    lines.push('_Fail-closed mode: broader suites selected because planning could not trust the diff._');
  }

  return {
    suites,
    changedDomains: [...changedDomains].sort(),
    changedFiles,
    failClosed,
    summaryMarkdown: `${lines.join('\n')}\n`,
    suiteReasons: Object.fromEntries(
      [...suiteReasons.entries()].map(([k, v]) => [k, [...v]]),
    ),
  };
}

function readChangedFilesFromArgv(argv) {
  const fileArg = argv.find((arg) => !arg.startsWith('-') && arg.endsWith('.txt'));
  if (fileArg && fs.existsSync(fileArg)) {
    return fs
      .readFileSync(fileArg, 'utf8')
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean);
  }
  if (process.env.CHANGED_FILES) {
    return process.env.CHANGED_FILES.split('\n').map((l) => l.trim()).filter(Boolean);
  }
  return [];
}

function writeGitHubOutput(plan) {
  const out = process.env.GITHUB_OUTPUT;
  if (!out) {
    console.log(JSON.stringify(plan, null, 2));
    return;
  }
  fs.appendFileSync(out, `suites=${JSON.stringify(plan.suites)}\n`);
  fs.appendFileSync(out, `changed_domains=${JSON.stringify(plan.changedDomains)}\n`);
  fs.appendFileSync(out, `fail_closed=${plan.failClosed}\n`);
  const delimiter = `ci_plan_${Date.now()}`;
  fs.appendFileSync(out, `summary<<${delimiter}\n${plan.summaryMarkdown}${delimiter}\n`);
}

function main() {
  const githubOutput = process.argv.includes('--github-output');
  let failClosed = false;
  let changedFiles = [];

  try {
    changedFiles = readChangedFilesFromArgv(process.argv);
    if (changedFiles.length === 0 && process.env.GITHUB_BASE_SHA && process.env.GITHUB_SHA) {
      failClosed = true;
    }
  } catch (err) {
    failClosed = true;
    changedFiles = [];
    console.error('ci-plan-pr: failed to read changed files', err.message);
  }

  let plan;
  try {
    plan = buildPlan(changedFiles, { failClosed });
  } catch (err) {
    console.error('ci-plan-pr: planning failed', err);
    plan = buildPlan(changedFiles, { failClosed: true });
    plan.failClosed = true;
  }

  if (githubOutput) {
    writeGitHubOutput(plan);
  } else {
    console.log(JSON.stringify(plan, null, 2));
  }
}

if (require.main === module) {
  main();
}

module.exports = {
  buildPlan,
  pathMatchesPattern,
  loadDomainMap,
};
