'use strict';

function formatUnderstandingPreview(situationModel) {
  const lines = ['I understood:'];
  const threads = situationModel.threads?.length
    ? situationModel.threads
    : [{ accountName: null, entities: situationModel.entities, events: situationModel.events, painPoints: situationModel.painPoints, commitments: situationModel.commitments, objections: situationModel.objections, corrections: situationModel.corrections }];

  for (const thread of threads) {
    const account = thread.accountName
      || thread.entities?.find(e => e.kind === 'account')?.name
      || 'Account (unresolved)';
    lines.push('');
    lines.push(String(account));

    for (const evt of thread.events || []) {
      if (evt.kind === 'conversation') lines.push('- Spoke with contact');
      if (evt.kind === 'in_person_visit') lines.push('- In-person visit');
    }
    for (const contact of (thread.entities || []).filter(e => e.kind === 'contact')) {
      lines.push(`- Contact: ${contact.name}${contact.role && contact.role !== 'unknown' ? ` (${contact.role.replace(/_/g, ' ')})` : ''}`);
    }
    for (const pain of thread.painPoints || []) {
      if (pain.current === false) {
        lines.push(`- Historical pain: ${pain.description} (not current)`);
      } else {
        lines.push(`- ${pain.description}`);
      }
    }
    for (const obj of thread.objections || []) {
      lines.push(`- Objection: ${obj.kind.replace(/_/g, ' ')}`);
    }
    for (const commit of thread.commitments || []) {
      lines.push(`- Expected ${commit.kind}: ${commit.responsible}${commit.windowPhrase ? ` (${commit.windowPhrase})` : ''}`);
    }
    for (const corr of thread.corrections || []) {
      if (corr.kind === 'temporal') {
        lines.push(`- Correction: ${corr.priorValue} → ${corr.newValue}`);
      }
      if (corr.kind === 'decision_maker_role') {
        lines.push(`- Role correction for ${corr.contactName || 'contact'}`);
      }
      if (corr.kind === 'account_identity') {
        lines.push(`- Account corrected to ${corr.newValue}`);
      }
    }
    for (const dm of thread.decisionMakerSignals || []) {
      const ep = dm.epistemic === 'uncertain' ? ' (uncertain)' : dm.epistemic === 'reported' ? ' (reported)' : '';
      lines.push(`- Decision-maker signal: ${dm.contactName}${ep}`);
    }
  }

  if (situationModel.questions?.length) {
    lines.push('');
    lines.push('Questions detected — answer conversationally before durable updates when appropriate.');
  }

  return lines.join('\n');
}

module.exports = {
  formatUnderstandingPreview,
};
