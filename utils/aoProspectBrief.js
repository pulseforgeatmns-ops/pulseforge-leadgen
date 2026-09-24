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

function formatProspectBrief({ prospect, company, touchpoints = [], task = null }) {
  const companyName = company?.name || prospect?.company_name || task?.account_name || 'Unknown';
  const segment = task?.segment || company?.industry || prospect?.vertical || null;
  const whyTheyMatter = task?.why_account_matters || prospect?.ao_fit_reason || null;
  const currentStatus = [
    prospect?.advisory_stage,
    prospect?.prospect_motion,
    prospect?.status,
  ].filter(hasValue).join(' · ') || null;

  const fields = {
    company: companyName,
    segment,
    why_they_matter: whyTheyMatter,
    current_status: currentStatus,
    recommended_angle: prospect?.recommended_angle || task?.recommended_angle || null,
    best_first_action: prospect?.recommended_first_action || task?.first_action || null,
    known_contacts: [prospect?.name, prospect?.job_title, prospect?.email, prospect?.phone]
      .filter(hasValue)
      .join(' · ') || null,
    prior_touches: formatTouchSummary(touchpoints),
    risks_avoid: prospect?.prospect_motion === 'SUPPRESS' ? 'Do not pursue — suppressed by routing.' : null,
    suggested_opener: task?.suggested_opener || null,
    next_step: prospect?.next_action || task?.desired_next_outcome || null,
    assignment_category: prospect?.ao_assignment_category || task?.assignment_category || null,
    ao_fit_score: prospect?.ao_fit_score ?? null,
    next_action_owner: prospect?.next_action_owner || null,
    next_action_due_at: prospect?.next_action_due_at || task?.deadline || null,
    last_debrief_status: prospect?.last_debrief_status || null,
    location: company?.location || task?.location || null,
    website: company?.website || null,
  };

  const requiredForFull = [
    'company',
    'segment',
    'why_they_matter',
    'current_status',
    'recommended_angle',
    'best_first_action',
  ];
  const missingRequired = requiredForFull.filter(key => !hasValue(fields[key]));
  const sparse = missingRequired.length >= 3;

  if (sparse) {
    const known = Object.entries(fields)
      .filter(([, value]) => hasValue(value))
      .map(([key, value]) => `- ${key.replace(/_/g, ' ')}: ${value}`);
    const unknown = missingRequired.map(key => `- ${key.replace(/_/g, ' ')}`);
    const research = prospect?.recommended_first_action
      || task?.first_action
      || 'Confirm decision-maker contact and current cleaning vendor status.';

    return [
      "I don't have enough data for a full brief yet.",
      '',
      'Known:',
      known.length ? known.join('\n') : '- No structured fields recorded yet',
      '',
      'Unknown:',
      unknown.length ? unknown.join('\n') : '- Core routing fields',
      '',
      'Recommended next research step:',
      research,
    ].join('\n');
  }

  const lines = [
    'Prospect Brief',
    '',
    `Company: ${fields.company}`,
    `Segment: ${fields.segment || 'Unknown'}`,
    `Why they matter: ${fields.why_they_matter || 'Not recorded'}`,
    `Current status: ${fields.current_status || 'Not recorded'}`,
    `Recommended angle: ${fields.recommended_angle || 'Not recorded'}`,
    `Best first action: ${fields.best_first_action || 'Not recorded'}`,
    `Known contacts: ${fields.known_contacts || 'Not recorded'}`,
    `Prior touches: ${fields.prior_touches || 'None recorded'}`,
    `Risks / avoid: ${fields.risks_avoid || 'None flagged'}`,
    `Suggested opener: ${fields.suggested_opener || 'Not recorded'}`,
    `Next step: ${fields.next_step || 'Not recorded'}`,
  ];

  if (hasValue(fields.assignment_category)) {
    lines.push(`Assignment category: ${fields.assignment_category}`);
  }
  if (fields.ao_fit_score != null) {
    lines.push(`AO fit score: ${fields.ao_fit_score}`);
  }
  if (hasValue(fields.next_action_owner)) {
    lines.push(`Next action owner: ${fields.next_action_owner}`);
  }
  if (hasValue(fields.next_action_due_at)) {
    lines.push(`Next action due: ${fields.next_action_due_at}`);
  }
  if (hasValue(fields.last_debrief_status)) {
    lines.push(`Last debrief: ${fields.last_debrief_status}`);
  }
  if (hasValue(fields.location)) {
    lines.push(`Location: ${fields.location}`);
  }
  if (hasValue(fields.website)) {
    lines.push(`Website: ${fields.website}`);
  }

  return lines.join('\n');
}

function formatLeadBrief(lead) {
  return formatAccountBriefing(lead);
}

module.exports = {
  formatProspectBrief,
  formatLeadBrief,
  hasValue,
};
