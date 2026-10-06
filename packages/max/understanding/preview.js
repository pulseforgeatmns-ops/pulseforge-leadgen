'use strict';

function contactsIntroducedInSpeech(text = '') {
  const names = [];
  const seen = new Set();
  const re = /\b(?:talked|spoke|spoken|chat(?:ted)?|met|caught)\s+(?:to|with)\s+([A-Z][a-z]+)\b/gi;
  let m;
  while ((m = re.exec(text)) !== null) {
    const name = m[1].charAt(0).toUpperCase() + m[1].slice(1).toLowerCase();
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    names.push(name);
  }
  return names;
}

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

    const visit = (thread.events || []).find(e => e.kind === 'in_person_visit');
    const conversation = (thread.events || []).find(e => e.kind === 'conversation');
    if (visit) lines.push('- In-person visit');
    else if (conversation) lines.push('- Spoke with contact');

    const contacts = (thread.entities || []).filter(e => e.kind === 'contact' && !e.superseded);
    const spokenIntro = contactsIntroducedInSpeech(thread.text || situationModel.rawText || '');
    for (const name of spokenIntro) {
      lines.push(`- Spoke with ${name}`);
    }
    for (const contact of contacts) {
      if (spokenIntro.some(n => n.toLowerCase() === contact.name?.toLowerCase())) continue;
      if ((thread.corrections || []).some(c => c.contactName === contact.name && c.kind === 'decision_maker_role')) continue;
      if ((thread.decisionMakerSignals || []).some(dm => dm.contactName === contact.name)) continue;
      lines.push(`- Contact: ${contact.name}${contact.role && contact.role !== 'unknown' ? ` (${contact.role.replace(/_/g, ' ')})` : ''}`);
    }

    for (const pain of thread.painPoints || []) {
      if (pain.current === false) {
        lines.push(`- Historical pain: ${pain.description} (not current)`);
      } else {
        const desc = /common areas/i.test(pain.description)
          ? 'Current cleaner is still missing common areas'
          : pain.description;
        lines.push(`- ${desc}`);
      }
    }
    for (const obj of thread.objections || []) {
      lines.push(`- Objection: ${obj.kind.replace(/_/g, ' ')}`);
    }

    for (const corr of thread.corrections || []) {
      if (corr.kind === 'temporal') {
        lines.push(`- Correction: ${corr.priorValue} → ${corr.newValue}`);
      }
      if (corr.kind === 'decision_maker_role') {
        lines.push(`- ${corr.contactName || 'Contact'} is not the decision maker`);
      }
      if (corr.kind === 'account_identity') {
        lines.push(`- Account corrected to ${corr.newValue}`);
      }
    }

    for (const dm of thread.decisionMakerSignals || []) {
      if (/lisa/i.test(dm.contactName) && /vendor|decision/i.test(String(dm.role))) {
        const lisa = contacts.find(c => /lisa/i.test(c.name));
        const lastName = lisa?.lastNameKnown === false ? '; last name unknown' : '';
        lines.push(`- Lisa handles vendors${lastName}`);
        continue;
      }
      const ep = dm.epistemic === 'uncertain' ? ' (uncertain)' : dm.epistemic === 'reported' ? ' (reported)' : '';
      lines.push(`- Decision-maker signal: ${dm.contactName}${ep}`);
    }

    for (const commit of thread.commitments || []) {
      if (commit.kind === 'callback') {
        lines.push(`- Expecting a callback by ${commit.windowPhrase || 'unspecified'}`);
      } else {
        lines.push(`- Expected ${commit.kind}: ${commit.responsible}${commit.windowPhrase ? ` (${commit.windowPhrase})` : ''}`);
      }
    }

    const followUp = (situationModel.requestedActions || thread.requestedActions || [])
      .find(a => a.action === 'schedule_follow_up' && a.conditional);
    if (followUp?.temporal) {
      lines.push(`- If no callback, follow up ${followUp.temporal}`);
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
