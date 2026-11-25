#!/usr/bin/env node
'use strict';

/**
 * MAX-P0-002 — release gate for the Render production service.
 *
 * Decides whether a commit may be deployed. Refuses unless:
 *   1. the commit is the current tip of the deployment branch;
 *   2. every required workflow has a completed, successful run for that commit;
 *   3. no workflow run for that commit concluded unsuccessfully or is still running.
 *
 * Reads only the GitHub REST API (GITHUB_TOKEN, actions:read + contents:read).
 * Prints a JSON verdict; exit 0 = deploy allowed, 1 = refused, 2 = usage/API error.
 */

const DEFAULT_BRANCH = 'migration/render-current-main';
const DEFAULT_REQUIRED = ['Full platform regression (off PR path)'];
const OK_CONCLUSIONS = new Set(['success', 'skipped', 'neutral']);

/**
 * Pure decision over GitHub data.
 * @param {{sha:string, branchTip:string, runs:Array<{name:string,status:string,conclusion:string|null,event:string,html_url?:string}>, required?:string[]}} input
 */
function decide({ sha, branchTip, runs, required = DEFAULT_REQUIRED }) {
  const reasons = [];
  if (!/^[0-9a-f]{40}$/.test(String(sha || ''))) reasons.push('sha_must_be_full_40_hex');
  if (sha && branchTip && sha !== branchTip) reasons.push(`not_branch_tip:${branchTip}`);

  const latestByName = new Map();
  for (const run of runs || []) {
    const prev = latestByName.get(run.name);
    if (!prev || Number(run.run_number || 0) >= Number(prev.run_number || 0)) latestByName.set(run.name, run);
  }
  for (const [name, run] of latestByName) {
    if (run.status !== 'completed') reasons.push(`still_running:${name}`);
    else if (!OK_CONCLUSIONS.has(run.conclusion)) reasons.push(`failed:${name}:${run.conclusion}`);
  }
  for (const name of required) {
    const run = latestByName.get(name);
    if (!run) reasons.push(`required_missing:${name}`);
    else if (run.status === 'completed' && run.conclusion !== 'success') reasons.push(`required_not_success:${name}`);
  }
  return {
    allowed: reasons.length === 0,
    sha,
    branchTip,
    reasons: [...new Set(reasons)],
    runs: [...latestByName.values()].map(r => ({ name: r.name, event: r.event, status: r.status, conclusion: r.conclusion, url: r.html_url })),
  };
}

async function gh(path, token) {
  const res = await fetch(`https://api.github.com${path}`, {
    headers: { authorization: `Bearer ${token}`, accept: 'application/vnd.github+json', 'x-github-api-version': '2022-11-28' },
  });
  if (!res.ok) throw new Error(`GitHub API ${res.status} for ${path}`);
  return res.json();
}

async function main(env = process.env) {
  const repo = env.GITHUB_REPOSITORY;
  const token = env.GITHUB_TOKEN;
  const sha = String(env.CANDIDATE_SHA || '').trim();
  const branch = env.DEPLOY_BRANCH || DEFAULT_BRANCH;
  const required = env.REQUIRED_WORKFLOWS
    ? env.REQUIRED_WORKFLOWS.split(',').map(s => s.trim()).filter(Boolean)
    : DEFAULT_REQUIRED;
  if (!repo || !token || !sha) {
    console.error('GITHUB_REPOSITORY, GITHUB_TOKEN and CANDIDATE_SHA are required');
    return 2;
  }
  try {
    const ref = await gh(`/repos/${repo}/branches/${encodeURIComponent(branch)}`, token);
    const runs = [];
    for (let page = 1; page <= 5; page += 1) {
      const body = await gh(`/repos/${repo}/actions/runs?head_sha=${sha}&per_page=100&page=${page}`, token);
      runs.push(...(body.workflow_runs || []));
      if (!body.workflow_runs || body.workflow_runs.length < 100) break;
    }
    // The gate's own run is in progress by definition; exclude it.
    const selfName = env.GITHUB_WORKFLOW || '';
    const verdict = decide({ sha, branchTip: ref.commit && ref.commit.sha, runs: runs.filter(r => r.name !== selfName), required });
    console.log(JSON.stringify(verdict, null, 2));
    return verdict.allowed ? 0 : 1;
  } catch (err) {
    console.error(`release gate error: ${err.message}`);
    return 2;
  }
}

if (require.main === module) main().then(code => { process.exitCode = code; });

module.exports = { decide, DEFAULT_REQUIRED, DEFAULT_BRANCH };
