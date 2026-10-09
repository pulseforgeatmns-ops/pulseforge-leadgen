'use strict';

const { deriveDefaultStatus } = require('./aoCrmTypes');
const { formatFollowUpTiming, todayISOInZone } = require('./aoAccountBriefing');
const { sourceActivityDate } = require('./spreadsheetCrmEvidence');

function hasValue(value) {
  if (value == null) return false;
  if (typeof value === 'string') return value.trim().length > 0;
  return true;
}

const GENERIC_ICP_PATTERNS = [
  /is a law firm where/i,
  /professional office where/i,
  /cpa or accounting office where/i,
  /likely has recurring vendor needs/i,
  /manages short-term rentals with turnover/i,
  /in Anchor'?s (lane|service area)/i,
  /Anchor fit in/i,
  /aligned with Anchor/i,
  /worth a focused pass/i,
  /may need recurring or backup commercial cleaning/i,
  /Consistent office presentation/i,
  /Reliable local office cleaning/i,
  /without vendor churn/i,
];

function isGenericIcpText(text) {
  const value = String(text || '').trim();
  if (!value) return false;
  return GENERIC_ICP_PATTERNS.some(pattern => pattern.test(value));
}

function accountDisplayName(prospect, company) {
  return company?.name || prospect?.company_name || 'Unknown account';
}

function prospectContactName(prospect, company) {
  const parts = [prospect?.first_name, prospect?.last_name].filter(hasValue).join(' ');
  if (parts) return parts;
  const legacy = String(prospect?.name || '').trim();
  const account = accountDisplayName(prospect, company);
  if (legacy && legacy.toLowerCase() !== account.toLowerCase()) return legacy;
  return null;
}

function officePhone(prospect, company) {
  return prospect?.phone || company?.phone || null;
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

function formatActivitySummary(activity = []) {
  const withNotes = activity.filter(row => hasValue(row.notes));
  if (!withNotes.length) return null;
  return withNotes.slice(0, 3).map(row => {
    const historicalDate = sourceActivityDate(row);
    const when = historicalDate || (row.created_at ? new Date(row.created_at).toISOString().slice(0, 10) : 'unknown date');
    const recorded = historicalDate && row.created_at ? ` [recorded ${new Date(row.created_at).toISOString()}]` : '';
    const type = row.activity_type || 'note';
    return `${when} (${type})${recorded}: ${String(row.notes)}`;
  }).join('\n');
}

function noteBlob({ prospect, activity }) {
  return [
    prospect?.notes,
    ...(activity || []).map(row => row.notes),
  ].filter(hasValue).join('\n');
}

function detectSnhuUnhAmbiguity({ prospect, company, blob }) {
  const account = accountDisplayName(prospect, company).toLowerCase();
  const email = String(prospect?.email || '').toLowerCase();
  const text = String(blob || '').toLowerCase();
  const mentionsSnhu = /snhu|southern new hampshire university/.test(account) || /snhu/.test(text);
  const mentionsUnh = /@unh\.edu/.test(email) || /\bunh\b/.test(text) || /university of new hampshire/.test(text);
  return mentionsSnhu && mentionsUnh;
}

function detectInternalCleaningSignal(blob) {
  const text = String(blob || '').toLowerCase();
  return /(receptionist|staff|team|front desk).{0,48}(clean|cleaning)/.test(text)
    || /clean(s|ing)?\s+the\s+office/.test(text)
    || /in[- ]house\s+(custodial|cleaning)/.test(text)
    || /internal(ly)?\s+clean/.test(text);
}

function detectRelationshipSignal(blob) {
  const text = String(blob || '').toLowerCase();
  return /good relationship|former manager|reconnect|knows?\s+(him|her|them)\s+well/.test(text);
}

function detectPropertyMgmtLayer(blob) {
  const text = String(blob || '');
  const tagged = text.match(/building:?\s*([^\n.]+)/i);
  if (tagged) return tagged[1].trim();
  const nash = text.match(/(Nash Family Investment Properties)/i);
  return nash ? nash[1] : null;
}

function statusPlainLabel(status) {
  const map = {
    researching: 'no outreach logged yet',
    ready_to_call: 'queued for first outreach',
    call_attempted: 'call attempted — no decision-maker conversation logged yet',
    contacted: 'outreach started — log a clear next step after each touch',
    gatekeeper_reached: 'only gatekeeper contact so far — facilities owner still unknown',
    decision_maker_reached: 'decision-maker conversation started — keep diagnosing setup',
    follow_up_needed: 'active account — next touch is due',
    warm: 'warm interest — keep momentum',
    walkthrough_target: 'walkthrough target — schedule or confirm timing',
    walkthrough_booked: 'walkthrough booked — confirm details',
    proposal_needed: 'proposal stage — confirm scope and timing questions',
    proposal_sent: 'proposal sent — confirm questions and next step',
  };
  return map[status] || null;
}

function buildWhereThisStands({
  prospect,
  task,
  touchpoints,
  activity,
  contactName,
  hasConversationNotes,
  snhuAmbiguity,
}) {
  if (prospect?.help_requested) {
    const reason = prospect.help_reason ? `: ${prospect.help_reason}` : '';
    return `Waiting on Jake${reason}. Hold active outreach until Jake responds.`;
  }
  if (prospect?.ao_paused) {
    return 'Account is paused. Confirm with Jake before re-engaging.';
  }

  const status = deriveDefaultStatus(prospect || {});
  const statusText = statusPlainLabel(status);
  const hasTouches = (touchpoints?.length || 0) > 0 || (activity?.length || 0) > 0;
  const hasDm = Boolean(contactName) || Boolean(prospect?.is_decision_maker);

  if (!hasTouches && !hasConversationNotes && !hasDm) {
    let line = 'No prior conversation or decision-maker is logged yet.';
    if (task?.status === 'open' || task?.status === 'in_progress') {
      line += ' This is queued for an initial diagnostic call.';
    } else if (status === 'ready_to_call' || status === 'researching') {
      line += ' This is queued for an initial diagnostic call.';
    } else if (statusText) {
      line += ` Account state: ${statusText}.`;
    }
    return line;
  }

  const parts = [];
  if (!hasDm) parts.push('No decision-maker identified yet.');
  else parts.push(`Contact on file${prospect?.is_decision_maker ? ' (decision-maker)' : ''}.`);

  if (snhuAmbiguity) {
    parts.push('SNHU vs UNH target is unclear — confirm the correct facilities org before outreach.');
  } else if (statusText) {
    parts.push(statusText.charAt(0).toUpperCase() + statusText.slice(1));
  } else if (task?.deadline) {
    const timing = formatFollowUpTiming(task.deadline, { today: todayISOInZone() });
    if (timing.label) parts.push(timing.label.replace(/^Due /, 'Next touch '));
  }

  if (!hasTouches && !hasConversationNotes) {
    parts.push('No AO conversation notes logged yet.');
  }

  return parts.join(' ');
}

function buildWhyThisNext({
  prospect,
  task,
  contactName,
  hasConversationNotes,
  knownPain,
  snhuAmbiguity,
  internalCleaning,
  relationshipSignal,
  propertyMgmt,
}) {
  const status = deriveDefaultStatus(prospect || {});
  if (snhuAmbiguity) {
    return 'Do not pitch until you confirm whether this is SNHU facilities, UNH, or a shared vendor path — the contact email and facilities links conflict.';
  }
  if (prospect?.help_requested) {
    return 'Jake needs to weigh in before you change approach or commit to scope.';
  }
  if (status === 'walkthrough_booked' || status === 'walkthrough_target') {
    return 'Confirm walkthrough logistics and access before discussing pricing or scope changes.';
  }
  if (status === 'proposal_needed' || status === 'proposal_sent') {
    return 'Stay on the proposal thread — clarify open scope or timing questions instead of re-pitching from scratch.';
  }
  if (knownPain) {
    if (propertyMgmt) {
      return `Validate the cleaning issue with the branch contact, then map whether ${propertyMgmt} controls vendor decisions before offering scope.`;
    }
    return 'Use what they already told you — validate the pain is still true and who can act on it before offering a walkthrough or quote.';
  }
  if (internalCleaning) {
    return 'Staff are handling cleaning in-house — diagnose what they cover today and who would own a facilities conversation before suggesting outside support.';
  }
  if (relationshipSignal) {
    return 'Lead with the existing relationship — reconnect warmly and learn who handles facilities vendors today without pushing a pitch.';
  }
  if (hasConversationNotes && contactName) {
    return 'Pick up the last thread with a concrete question instead of restarting with a generic pitch.';
  }
  if (!contactName) {
    return 'Before pitching cleaning support, identify who owns cleaning/vendor decisions and whether there is any current pain with the existing setup.';
  }
  if (task?.discovery_objective && !isGenericIcpText(task.discovery_objective)) {
    return task.discovery_objective;
  }
  return 'Confirm who owns vendor decisions and how cleaning is handled today before suggesting Anchor as an option.';
}

function buildKnownContextLines({
  prospect,
  company,
  touchpoints,
  activity,
  contactName,
  task,
}) {
  const lines = [];
  const account = accountDisplayName(prospect, company);
  lines.push(account);

  const phone = officePhone(prospect, company);
  if (phone) lines.push(`Phone: ${phone}.`);

  const location = company?.location || prospect?.service_area_match || null;
  if (location) lines.push(`Location: ${location}.`);

  if (contactName) {
    const role = prospect?.job_title ? ` (${prospect.job_title})` : '';
    lines.push(`Contact: ${contactName}${role}.`);
  }
  if (prospect?.email) lines.push(`Email: ${prospect.email}.`);

  const taskWhy = task?.why_account_matters;
  const aoWhy = prospect?.ao_why_account_matters;
  for (const candidate of [taskWhy, aoWhy, prospect?.ao_assignment_reason]) {
    if (hasValue(candidate) && !isGenericIcpText(candidate)) {
      lines.push(String(candidate).trim());
      break;
    }
  }

  const activityNotes = formatActivitySummary(activity);
  if (activityNotes) {
    lines.push(`AO notes:\n${activityNotes.split('\n').map(l => `  ${l}`).join('\n')}`);
  } else {
    lines.push('No AO notes logged yet.');
  }

  const touches = formatTouchSummary(touchpoints);
  if (touches) {
    lines.push(`Prior touches:\n${touches.split('\n').map(l => `  ${l}`).join('\n')}`);
  }

  if (prospect?.help_requested && prospect?.help_reason) {
    lines.push(`Jake already flagged: ${prospect.help_reason}.`);
  }

  if (prospect?.notes && hasValue(prospect.notes)) {
    lines.push(`CRM note: ${String(prospect.notes).trim()}`);
  }

  return lines;
}

function mapNextActionLabel(raw) {
  const key = String(raw || '').trim().toLowerCase();
  const map = {
    research_contact: 'Identify the office manager or facilities contact and log name, role, and best callback path.',
    call: 'Call and ask who handles cleaning or facilities vendors for the office.',
    visit: 'Stop by, ask who handles cleaning vendors, and log the name and role you get.',
    email: 'Email to identify who handles cleaning or facilities vendors — keep it to one diagnostic question.',
    ask_jake: 'Pause outreach and ask Jake how to proceed.',
    book_walkthrough: 'Confirm walkthrough timing, access, and who will attend.',
    send_information: 'Send only what they asked for, then log what was sent and the next question to ask.',
    prepare_proposal: 'Confirm scope inputs with the decision-maker before drafting anything.',
    check_back_later: 'Log the timing they gave you and set the callback for that window.',
    disqualify: 'Log why this account is not a fit and close the task.',
    no_action: 'No outreach until account state changes — log why.',
  };
  return map[key] || null;
}

function buildNextAction({
  prospect,
  task,
  contactName,
  vertical,
  snhuAmbiguity,
  internalCleaning,
  relationshipSignal,
  propertyMgmt,
  knownPain,
}) {
  if (snhuAmbiguity) {
    return 'Confirm whether this account is SNHU or UNH facilities (check email domain, facilities page, and who owns vendor decisions) and log the correct contact path before calling.';
  }

  const fromTask = task?.first_action;
  if (hasValue(fromTask) && !isGenericIcpText(fromTask) && !/^follow[- ]?up/i.test(fromTask)) {
    return String(fromTask).trim();
  }

  const fromProspect = mapNextActionLabel(prospect?.ao_next_action)
    || mapNextActionLabel(prospect?.next_action);
  if (fromProspect) return fromProspect;

  const recommended = prospect?.recommended_first_action;
  if (hasValue(recommended) && !isGenericIcpText(recommended) && !/^follow[- ]?up/i.test(recommended)) {
    return String(recommended).trim();
  }

  if (internalCleaning && contactName) {
    const phone = officePhone(prospect, null);
    const via = phone ? ` at ${phone}` : '';
    return `Call ${contactName}${via} and ask who oversees cleaning when staff handle it in-house — confirm what's covered today and whether a short facility assessment would be useful (no pitch).`;
  }

  if (relationshipSignal && contactName) {
    const phone = officePhone(prospect, null);
    const via = phone ? ` at ${phone}` : '';
    return `Call ${contactName}${via} to reconnect, ask who handles facilities vendors today, and log the name — keep it relationship-first, not a scope pitch.`;
  }

  if (knownPain && propertyMgmt && contactName) {
    const phone = officePhone(prospect, null);
    const via = phone ? ` at ${phone}` : '';
    return `Call ${contactName}${via} and confirm how vendor issues get escalated to ${propertyMgmt} before discussing Anchor as an option.`;
  }

  if (contactName && !prospect?.is_decision_maker) {
    const phone = officePhone(prospect, null);
    const via = phone ? ` at ${phone}` : '';
    return `Call ${contactName}${via}, ask for whoever handles cleaning vendors, and log the name and role.`;
  }

  if (vertical === 'law_firm' || vertical === 'commercial_office' || vertical === 'accounting') {
    return 'Call and ask who handles cleaning or facilities vendors for the office. If you reach the office manager, ask how cleaning is currently handled and whether anything tends to get missed.';
  }

  return 'Call and ask who handles cleaning or facilities vendors for the office. If no decision-maker is available, get the right name/email and log it.';
}

function extractKnownPain({ activity, prospect }) {
  const blob = noteBlob({ prospect, activity }).toLowerCase();
  if (!blob) return null;
  if (/unhappy|dissatisfied|frustrated|missed|slipping|backup|overflow|contractor/.test(blob)) {
    return true;
  }
  return null;
}

function buildListenFor({
  prospect,
  vertical,
  knownPain,
  contactName,
  blob,
  internalCleaning,
  snhuAmbiguity,
  propertyMgmt,
}) {
  const items = [];
  if (snhuAmbiguity) {
    items.push(
      'which institution owns the facilities contact (SNHU vs UNH)',
      'whether email/domain matches the account you intend to pursue',
      'who actually controls vendor decisions',
    );
  }
  if (internalCleaning) {
    items.push(
      'what staff clean vs what is skipped',
      'who would approve outside cleaning support',
      'whether missed areas create patient or team friction',
    );
  }
  if (knownPain) {
    const text = String(blob || '').toLowerCase();
    if (/kitchen/.test(text)) items.push('kitchen or break-area misses');
    if (/under tables|restroom|bathroom/.test(text)) items.push('restroom / under-table consistency');
    items.push('whether the issue is the vendor, schedule, or scope');
    items.push('who can authorize a vendor change');
    if (propertyMgmt) items.push(`how ${propertyMgmt} routes vendor decisions`);
  }

  const v = String(vertical || prospect?.vertical || '').toLowerCase();
  if (v === 'law_firm' || v === 'accounting' || v === 'commercial_office') {
    items.push(
      'who owns vendor decisions',
      'whether cleaning is internal or outsourced',
      'whether staff ever has to clean after missed work',
      'conference room / restroom / kitchen / lobby consistency',
      'after-hours access or confidentiality concerns',
    );
  } else if (v === 'property_manager' || v === 'property_management') {
    items.push(
      'who handles janitorial vendors across properties',
      'turnover vs common-area pain',
      'backup vendor gaps when primary misses work',
    );
  } else {
    items.push(
      'who owns vendor decisions',
      'current cleaner or in-house setup',
      'what gets missed first when cleaning slips',
    );
  }

  if (!contactName) {
    items.unshift('name and role of the facilities or office decision-maker');
  }

  if (prospect?.prospect_motion === 'SUPPRESS') {
    return ['Do not pursue — confirm suppression with Jake before any outreach.'];
  }

  return [...new Set(items)];
}

function buildShortTalkTrack({ prospect, task, accountName, aoName, snhuAmbiguity, internalCleaning }) {
  if (task?.suggested_opener && hasValue(task.suggested_opener)) {
    return String(task.suggested_opener).trim();
  }
  const rep = aoName || 'Jake';
  const name = accountName || 'the office';
  if (snhuAmbiguity) {
    return `Hi, I'm ${rep} with Anchor Cleaning. Before I ask about cleaning vendors — I want to make sure I have the right facilities contact for ${name} (SNHU vs UNH). Who usually owns those decisions on your side?`;
  }
  if (internalCleaning) {
    return `Hi, I'm ${rep} with Anchor Cleaning. I heard your team may handle some cleaning in-house — who usually decides when outside help makes sense? I'm not assuming you need a vendor; I just want to understand the setup.`;
  }
  return `Hi, I'm ${rep} with Anchor Cleaning. I was hoping to ask who usually handles cleaning or facilities decisions for ${name}. We help local offices when cleaning starts creating extra work for the team, but I don't want to assume that's relevant here.`;
}

function buildProspectBriefSections({
  prospect,
  company,
  touchpoints = [],
  task = null,
  activity = [],
  aoName = null,
}) {
  const account_name = accountDisplayName(prospect, company);
  const contactName = prospectContactName(prospect, company);
  const vertical = prospect?.vertical || company?.industry || task?.segment || null;
  const activityNotes = formatActivitySummary(activity);
  const blob = noteBlob({ prospect, activity });
  const hasConversationNotes = Boolean(activityNotes)
    || (hasValue(prospect?.notes) && !isGenericIcpText(prospect.notes));
  const knownPain = extractKnownPain({ activity, prospect });
  const snhuAmbiguity = detectSnhuUnhAmbiguity({ prospect, company, blob });
  const internalCleaning = detectInternalCleaningSignal(blob);
  const relationshipSignal = detectRelationshipSignal(blob);
  const propertyMgmt = detectPropertyMgmtLayer(blob);

  const where_this_stands = buildWhereThisStands({
    prospect,
    task,
    touchpoints,
    activity,
    contactName,
    hasConversationNotes,
    snhuAmbiguity,
  });

  const why_this_next = buildWhyThisNext({
    prospect,
    task,
    contactName,
    hasConversationNotes,
    knownPain,
    snhuAmbiguity,
    internalCleaning,
    relationshipSignal,
    propertyMgmt,
  });

  const known_context = buildKnownContextLines({
    prospect,
    company,
    touchpoints,
    activity,
    contactName,
    task,
  }).join('\n');

  const next_action = buildNextAction({
    prospect,
    task,
    contactName,
    vertical,
    snhuAmbiguity,
    internalCleaning,
    relationshipSignal,
    propertyMgmt,
    knownPain,
  });

  const listenItems = buildListenFor({
    prospect,
    vertical,
    knownPain,
    contactName,
    blob,
    internalCleaning,
    snhuAmbiguity,
    propertyMgmt,
  });

  const what_to_listen_for = listenItems.join(', ');

  const short_talk_track = buildShortTalkTrack({
    prospect,
    task,
    accountName: account_name,
    aoName,
    snhuAmbiguity,
    internalCleaning,
  });

  const sparse = !contactName
    && !touchpoints.length
    && !activity.length
    && !hasValue(prospect?.recommended_angle)
    && !knownPain;

  return {
    account_name,
    where_this_stands,
    why_this_next,
    known_context,
    next_action,
    what_to_listen_for,
    short_talk_track,
    sparse,
    // Legacy keys for callers still reading old shape
    why_this_account_matters: why_this_next,
    what_we_know: known_context,
    likely_angle: why_this_next,
    suggested_next_move: next_action,
    what_to_watch_for: what_to_listen_for,
  };
}

function renderCrmBriefText(sections) {
  const blocks = [
    ['Where this stands', sections.where_this_stands],
    ['Why this is the next move', sections.why_this_next],
    ['Known context', sections.known_context],
    ['Next action', sections.next_action],
    ['What to listen for', sections.what_to_listen_for],
    ['Short talk track', sections.short_talk_track],
  ];

  const title = sections.sparse
    ? `${sections.account_name} — light file so far`
    : sections.account_name;

  const body = blocks.map(([heading, text]) => `${heading}\n${text || '—'}`).join('\n\n');
  return `${title}\n\n${body}`;
}

function formatProspectBrief(input) {
  const sections = buildProspectBriefSections(input);
  return renderCrmBriefText(sections);
}

function formatLeadBrief(lead) {
  const { formatAccountBriefing } = require('./aoAccountBriefing');
  return formatAccountBriefing(lead);
}

module.exports = {
  formatProspectBrief,
  formatLeadBrief,
  buildProspectBriefSections,
  renderCrmBriefText,
  hasValue,
  isGenericIcpText,
};
