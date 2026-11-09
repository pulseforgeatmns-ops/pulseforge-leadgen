'use strict';

const pct = (x) => (x == null ? 'n/a' : `${(x * 100).toFixed(0)}%`);
const esc = (s) => String(s == null ? '' : s).replace(/\|/g, '\\|').replace(/\n/g, ' ');

function table(rows, headers) {
  return [
    `| ${headers.join(' | ')} |`,
    `| ${headers.map(() => '---').join(' | ')} |`,
    ...rows.map((r) => `| ${r.map(esc).join(' | ')} |`),
  ].join('\n');
}

function renderMarkdown({ generatedAt, runtimeMs, summary, controls, judge, grades }) {
  const lines = [];
  lines.push('# Max semantic generalization evaluation — results', '');
  lines.push(`Generated ${generatedAt} · ${summary.n} graded turns · engine runtime ${(runtimeMs / 1000).toFixed(1)}s`, '');
  lines.push(`**Overall pass: ${pct(summary.pass)}** · semantic dimensions: ${pct(summary.semantic)} · safety dimensions: ${pct(summary.safety)}`, '');
  lines.push('## By dimension', '', table(Object.entries(summary.dimension).map(([d, v]) => [d, v.n, pct(v.pass)]), ['dimension', 'n', 'pass']), '');
  lines.push('## By scenario', '', table(Object.entries(summary.byScenario).map(([k, v]) => [k, v.n, pct(v.pass), pct(v.semantic), pct(v.safety)]), ['scenario', 'n', 'pass', 'semantic', 'safety']), '');
  lines.push('## By tenant', '', table(Object.entries(summary.byTenant).map(([k, v]) => [k, v.n, pct(v.pass)]), ['tenant', 'n', 'pass']), '');
  lines.push('## Failure categories', '', table(Object.entries(summary.failures).sort((a, b) => b[1] - a[1]).map(([k, v]) => [k, v]), ['category', 'turns']), '');
  lines.push('## Response owner', '', table(Object.entries(summary.owners).sort((a, b) => b[1].n - a[1].n).map(([k, v]) => [k, v.n, v.pass]), ['owner/reason', 'turns', 'passed']), '');
  if (controls && Object.keys(controls).length) {
    lines.push('## Controls', '', 'Negative controls are safe but do not understand the question; they must fail. The oracle proves the graders are satisfiable.', '');
    lines.push(table(Object.entries(controls).map(([k, s]) => [k, pct(s.pass), pct(s.semantic), pct(s.safety)]), ['control', 'pass', 'semantic', 'safety']), '');
  }
  if (judge) {
    lines.push('## Model judge (advisory)', '');
    lines.push(`model ${judge.model} · repeats ${judge.repeats} · agreement with deterministic verdict ${pct(judge.agreement)} · judge self-consistency ${pct(judge.selfConsistency)} · est. cost $${judge.estimatedCostUsd.toFixed(3)}`, '');
  }
  lines.push('## Turns', '');
  lines.push(table(
    grades.map((g) => [g.caseId, g.pass ? 'PASS' : 'FAIL', g.q, g.responseClass, `${g.owner}/${g.ownerReason}`, g.failureCategory || '', g.notes.join('; ')]),
    ['case', 'result', 'utterance', 'class', 'owner', 'failure', 'notes'],
  ));
  return `${lines.join('\n')}\n`;
}

module.exports = { renderMarkdown };
