'use strict';

const BATCH_SLUG_PREFIX = 'ao-assignment';

const LANE_INITIAL_NEXT_ACTIONS = Object.freeze({
  property_management:
    'Identify the property/facilities decision-maker and determine who manages cleaning vendors.',
  development:
    'Identify the facilities/property operations contact and determine where recurring cleaning responsibility sits.',
  commercial_office:
    'Identify the office/facilities manager and confirm whether cleaning is outsourced.',
  medical_dental:
    'Identify the practice/office manager responsible for janitorial vendors.',
  commercial_real_estate:
    'Identify the facilities/property operations contact and determine where recurring cleaning responsibility sits.',
});

const LANE_LABELS = Object.freeze({
  property_management: 'Property Management',
  development: 'Development',
  commercial_office: 'Professional Office',
  medical_dental: 'Medical/Dental',
  commercial_real_estate: 'Commercial Real Estate',
});

function normalizeAoBusinessKey(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}

function laneInitialNextAction(lane) {
  return LANE_INITIAL_NEXT_ACTIONS[lane]
    || LANE_INITIAL_NEXT_ACTIONS.commercial_office;
}

function laneLabel(lane) {
  return LANE_LABELS[lane] || 'Commercial';
}

function buildAssignmentNote({
  batchSlug,
  company,
  ownerName,
  lane,
  priority = 'High',
  pipelineStage = 'research',
  nextAction = 'research',
  initialNextAction,
  dueDate,
  aoOwnerId,
  crmProspectId = null,
  crmCompanyId = null,
  batchId = null,
  sourceConversationId = null,
}) {
  const laneText = laneLabel(lane);
  const actionText = initialNextAction || laneInitialNextAction(lane);
  const metadata = {
    batch_id: batchId,
    source_conversation_id: sourceConversationId,
    company,
    owner: ownerName,
    ao_owner_id: aoOwnerId,
    status: 'needs_follow_up',
    pipeline_stage: pipelineStage,
    next_action: nextAction,
    source: 'AO Assignment',
    mission_type: 'recurring_commercial_acquisition',
    lane: laneText,
    priority,
    initial_next_action: actionText,
    due_date: dueDate,
  };
  if (crmProspectId) metadata.crm_prospect_id = crmProspectId;
  if (crmCompanyId) metadata.crm_company_id = crmCompanyId;

  return [
    `[AO Assignment | ${batchSlug}]`,
    `${company} — ${ownerName}`,
    actionText,
    'Research → identify decision-maker → identify likely cleaning situation/gap → choose call/email/in-person approach → execute.',
    '',
    'Metadata:',
    JSON.stringify(metadata, null, 2),
  ].join('\n');
}

function isJakeAssignmentBatchNote(note, batchSlug) {
  const text = String(note || '');
  return text.includes(`[AO Assignment | ${batchSlug}]`)
    || (text.includes(BATCH_SLUG_PREFIX) && text.includes(batchSlug));
}

function addBusinessDays(isoDate, days, timeZone = 'America/New_York') {
  const [year, month, day] = isoDate.split('-').map(Number);
  let cursor = new Date(Date.UTC(year, month - 1, day, 12));
  let remaining = Math.max(0, Number(days) || 0);
  while (remaining > 0) {
    cursor.setUTCDate(cursor.getUTCDate() + 1);
    const weekday = new Intl.DateTimeFormat('en-US', { weekday: 'short', timeZone: 'UTC' }).format(cursor);
    if (weekday !== 'Sat' && weekday !== 'Sun') remaining -= 1;
  }
  return cursor.toISOString().slice(0, 10);
}

function todayISOInZone(timeZone = 'America/New_York') {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(new Date());
}

function distributeDueDates(count, { today, timeZone = 'America/New_York' } = {}) {
  const base = today || todayISOInZone(timeZone);
  const dates = [];
  for (let i = 0; i < count; i += 1) {
    if (i < 5) dates.push(base);
    else if (i < 10) dates.push(addBusinessDays(base, 1, timeZone));
    else if (i < 14) dates.push(addBusinessDays(base, 2, timeZone));
    else dates.push(addBusinessDays(base, 3 + (i - 14), timeZone));
  }
  return dates;
}

module.exports = {
  BATCH_SLUG_PREFIX,
  LANE_INITIAL_NEXT_ACTIONS,
  normalizeAoBusinessKey,
  laneInitialNextAction,
  laneLabel,
  buildAssignmentNote,
  isJakeAssignmentBatchNote,
  addBusinessDays,
  todayISOInZone,
  distributeDueDates,
};
