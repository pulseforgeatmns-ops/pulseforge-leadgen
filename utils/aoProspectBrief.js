'use strict';

const { formatAccountBriefing } = require('./aoAccountBriefing');

function hasValue(value) {
  if (value == null) return false;
  if (typeof value === 'string') return value.trim().length > 0;
  return true;
}

function formatTouchSummary(touchpoints = []) {
  if (!touchpoints.length) return null;
  return touchpoints.slice(0, 5).map(tp => {
    const when = tp.created_at ? new Date(tp.created_at).toISOString().slice(0, 10) : 'unknown date';
    const channel = tp.channel || 'unknown';
    const action = tp.action_type || 'touch';
    const outcome = tp.outcome ? ` — ${tp.outcome}` : '';
    return `${when}: ${channel} ${action}${outcome}`;
  }).join('\n');
}

function buildProspectBriefSections({ prospect, company, touchpoints = [], task = null }) {
  const accountName = company?.name || prospect?.company_name || task?.account_name || 'Unknown account';
  const segment = task?.segment || company?.industry || prospect?.vertical || null;
  const whyThisAccountMatters = task?.why_account_matters
    || prospect?.ao_why_account_matters
    || prospect?.ao_fit_reason
    || prospect?.ao_assignment_reason
    || (segment ? `Anchor fit in ${segment.replace(/_/g, ' ')} — worth a focused pass.` : null);

  const statusBits = [
    prospect?.ao_current_status?.replace(/_/g, ' '),
    prospect?.last_debrief_status?.replace(/_/g, ' '),
    prospect?.advisory_stage,
  ].filter(hasValue);

  const contactLine = [prospect?.name, prospect?.job_title, prospect?.email, prospect?.phone]
    .filter(hasValue)
    .join(' · ');

  const knownLines = [];
  if (segment) knownLines.push(`${segment.replace(/_/g, ' ')} in Anchor's lane.`);
  if (statusBits.length) knownLines.push(`Status: ${statusBits.join(' · ')}.`);
  if (contactLine) knownLines.push(`Contact: ${contactLine}.`);
  if (company?.location) knownLines.push(`Location: ${company.location}.`);
  const touches = formatTouchSummary(touchpoints);
  if (touches) knownLines.push(`Recent touches:\n${touches.split('\n').map(l => `  ${l}`).join('\n')}`);
  if (prospect?.help_requested && prospect?.help_reason) {
    knownLines.push(`Jake already flagged: ${prospect.help_reason}.`);
  }

  const likelyAngle = prospect?.recommended_angle || task?.recommended_angle
    || 'Lead with Diagnose — ask what\'s working and what\'s slipping before you pitch.';

  const suggestedNextMove = prospect?.ao_next_action
    || prospect?.next_action
    || task?.first_action
    || prospect?.recommended_first_action
    || 'Confirm the decision-maker and book the next touch.';

  const opener = task?.suggested_opener
    || `Hi — I'm following up on Anchor's note. Quick question: who's handling day-to-day cleaning decisions for ${accountName}?`;

  const watchLines = [];
  if (prospect?.prospect_motion === 'SUPPRESS') watchLines.push('Routing says don\'t pursue — double-check before you push.');
  if (prospect?.ao_paused) watchLines.push('Account is paused — confirm with Jake before re-engaging.');
  if (!contactLine) watchLines.push('No DM on file yet — don\'t pitch scope until you\'ve got a name.');
  if (!touches) watchLines.push('No logged touches — treat this as a cold open, not a warm follow-up.');
  if (!watchLines.length) watchLines.push('If they mention an incumbent, stay curious — map contract timing before you quote.');

  return {
    account_name: accountName,
    why_this_account_matters: whyThisAccountMatters
      || 'In your book and aligned with Anchor\'s commercial-office focus.',
    what_we_know: knownLines.length
      ? knownLines.join('\n')
      : 'Not much logged yet — start with who runs the office and who owns cleaning.',
    likely_angle: likelyAngle,
    suggested_next_move: String(suggestedNextMove).replace(/_/g, ' '),
    short_talk_track: opener,
    what_to_watch_for: watchLines.join('\n'),
    sparse: !hasValue(whyThisAccountMatters) && !contactLine && !touches && !hasValue(prospect?.recommended_angle),
  };
}

function renderCrmBriefText(sections) {
  if (sections.sparse) {
    return [
      `${sections.account_name} — light file so far`,
      '',
      'What we know',
      sections.what_we_know,
      '',
      'Suggested next move',
      sections.suggested_next_move,
      '',
      'Short talk track',
      sections.short_talk_track,
    ].join('\n');
  }

  return [
    sections.account_name,
    '',
    'Why this account matters',
    sections.why_this_account_matters,
    '',
    'What we know',
    sections.what_we_know,
    '',
    'Likely angle',
    sections.likely_angle,
    '',
    'Suggested next move',
    sections.suggested_next_move,
    '',
    'Short talk track',
    sections.short_talk_track,
    '',
    'What to watch for',
    sections.what_to_watch_for,
  ].join('\n');
}

function formatProspectBrief({ prospect, company, touchpoints = [], task = null }) {
  const sections = buildProspectBriefSections({ prospect, company, touchpoints, task });
  return renderCrmBriefText(sections);
}

function formatLeadBrief(lead) {
  return formatAccountBriefing(lead);
}

module.exports = {
  formatProspectBrief,
  formatLeadBrief,
  buildProspectBriefSections,
  renderCrmBriefText,
  hasValue,
};
