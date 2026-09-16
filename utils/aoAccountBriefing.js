'use strict';

const {
  deriveOperationalState,
  buildRelationshipIntel,
  parseProbeAnswers,
} = require('./aoOperationalState');
const { normalizeDueDate } = require('./aoQueueFormat');

const DEFAULT_AO_TIMEZONE = 'America/New_York';

const INTERNAL_METADATA_KEYS = Object.freeze([
  'batch_id',
  'source_conversation_id',
  'ao_owner_id',
  'crm_prospect_id',
  'crm_company_id',
  'contribution_id',
  'source_payload',
  'conversation_id',
  'mission_id',
  'orchestration_mission_id',
]);

const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;
const ASSIGNMENT_SLUG_RE = /ao-assignment-[a-z0-9-]+/i;
const ASSIGNMENT_HEADER_RE = /\[AO Assignment\s*\|[^\]]+\]/i;

const LANE_TAXONOMY = Object.freeze({
  property_management: {
    id: 'property_management',
    label: 'property-management',
    roles: [
      'property manager',
      'regional property manager',
      'facilities manager',
      'operations manager',
      'vendor manager',
    ],
    keys: [
      'property_management',
      'property management',
      'property manager',
      'pm',
      'multifamily',
    ],
  },
  development: {
    id: 'development',
    label: 'development',
    roles: [
      'facilities manager',
      'property manager',
      'operations manager',
      'construction manager',
    ],
    keys: ['development', 'developer', 'construction'],
  },
  commercial_office: {
    id: 'commercial_office',
    label: 'commercial-office',
    roles: [
      'office manager',
      'facilities manager',
      'practice manager',
      'operations manager',
    ],
    keys: [
      'commercial',
      'professional office',
      'professional_office',
      'office',
      'law_firm',
      'law firm',
      'accounting',
      'cpa',
      'practice',
      'professional_services',
    ],
  },
});

function todayISOInZone(date = new Date(), timeZone = DEFAULT_AO_TIMEZONE) {
  try {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(date);
  } catch {
    return date.toISOString().slice(0, 10);
  }
}

function extractJsonObjects(text) {
  const str = String(text || '');
  const objects = [];

  for (let i = 0; i < str.length; i += 1) {
    if (str[i] !== '{') continue;
    let depth = 0;
    let inString = false;
    let escape = false;
    for (let j = i; j < str.length; j += 1) {
      const ch = str[j];
      if (inString) {
        if (escape) escape = false;
        else if (ch === '\\') escape = true;
        else if (ch === '"') inString = false;
        continue;
      }
      if (ch === '"') inString = true;
      else if (ch === '{') depth += 1;
      else if (ch === '}') {
        depth -= 1;
        if (depth === 0) {
          const raw = str.slice(i, j + 1);
          try {
            const value = JSON.parse(raw);
            if (value && typeof value === 'object' && !Array.isArray(value)) {
              objects.push({ raw, value, start: i, end: j + 1 });
            }
          } catch {
            // Ignore non-JSON brace blocks.
          }
          i = j;
          break;
        }
      }
    }
  }

  return objects;
}

function isInternalMetadataObject(value) {
  if (!value || typeof value !== 'object') return false;
  return INTERNAL_METADATA_KEYS.some(key => Object.prototype.hasOwnProperty.call(value, key));
}

function extractAssignmentMetadata(text) {
  const harvested = {};
  for (const { value } of extractJsonObjects(text)) {
    if (!isInternalMetadataObject(value) && !value.pipeline_stage && !value.lane && !value.mission_type) {
      continue;
    }
    Object.assign(harvested, value);
  }
  return harvested;
}

function looksLikeAssignmentNote(text) {
  const value = String(text || '');
  return ASSIGNMENT_HEADER_RE.test(value)
    || ASSIGNMENT_SLUG_RE.test(value)
    || /"source"\s*:\s*"AO Assignment"/i.test(value)
    || /Metadata:\s*\{/i.test(value)
    || INTERNAL_METADATA_KEYS.some(key => value.includes(`"${key}"`));
}

function isPlaybookRecipe(text) {
  const value = String(text || '').trim();
  if (!value) return false;
  if ((value.match(/→/g) || []).length >= 1) return true;
  return /identify decision-maker/i.test(value) && /execute/i.test(value);
}

function stripInternalTokens(text) {
  return String(text || '')
    .replace(/\[AO Assignment\s*\|[^\]]+\]/gi, '')
    .replace(/ao-assignment-[a-z0-9-]+/gi, '')
    .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, '')
    .replace(/\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+\w+\s+\d{1,2}\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+GMT[^\n]*/gi, '')
    .replace(/\bMetadata:\s*/gi, '')
    .replace(/\b(batch_id|source_conversation_id|ao_owner_id|crm_prospect_id|crm_company_id)\b\s*[:=]\s*\S+/gi, '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .replace(/[ \t]{2,}/g, ' ')
    .trim();
}

function sanitizeAoFacingText(text) {
  let remaining = String(text || '');
  const objects = extractJsonObjects(remaining);
  for (let i = objects.length - 1; i >= 0; i -= 1) {
    const { start, end, value } = objects[i];
    if (isInternalMetadataObject(value) || value.pipeline_stage || value.lane) {
      remaining = remaining.slice(0, start) + remaining.slice(end);
    }
  }
  remaining = stripInternalTokens(remaining);

  const cleanedLines = remaining
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .filter(line => !isPlaybookRecipe(line))
    .filter(line => !/^Metadata:?$/i.test(line))
    .filter(line => !INTERNAL_METADATA_KEYS.some(key => line.includes(key)));

  return cleanedLines.join('\n').trim();
}

function normalizePriority(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'high' || raw === 'warm') return raw;
  if (raw === 'normal' || raw === 'medium' || raw === 'low') return raw === 'medium' ? 'normal' : raw;
  return raw || 'normal';
}

function matchLaneFromText(value) {
  const hay = String(value || '').trim().toLowerCase().replace(/[_-]+/g, ' ');
  if (!hay) return null;
  for (const lane of Object.values(LANE_TAXONOMY)) {
    if (lane.keys.some((key) => {
      if (hay === key) return true;
      const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      return new RegExp(`(?:^|\\s)${escaped}(?:\\s|$)`).test(hay);
    })) {
      return lane;
    }
  }
  return null;
}

function inferLaneFromName(businessName) {
  const name = String(businessName || '');
  if (/\bpropert(?:y|ies)\b/i.test(name) || /\bproperty management\b/i.test(name)) {
    return { lane: LANE_TAXONOMY.property_management, inferred: true, confidence: 'high' };
  }
  if (/\b(llp|law|attorney|cpa|account(?:ing|ant)?|dental|clinic|practice)\b/i.test(name)) {
    return { lane: LANE_TAXONOMY.commercial_office, inferred: true, confidence: 'high' };
  }
  if (/\bdevelopment\b/i.test(name)) {
    return { lane: LANE_TAXONOMY.development, inferred: true, confidence: 'high' };
  }
  return { lane: null, inferred: false, confidence: 'none' };
}

function classifyAccountLane(row, metadata = {}) {
  const explicit = matchLaneFromText(metadata.lane)
    || matchLaneFromText(row.business_type)
    || matchLaneFromText(row.lane);
  if (explicit) {
    return { ...explicit, inferred: false, confidence: 'high' };
  }
  const inferred = inferLaneFromName(row.business_name);
  return inferred.lane
    ? { ...inferred.lane, inferred: true, confidence: inferred.confidence }
    : { id: 'unknown', label: null, roles: ['office manager', 'facilities manager', 'owner'], inferred: false, confidence: 'none' };
}

function humanizeEnum(value) {
  return String(value || '')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function titleCase(value) {
  const text = humanizeEnum(value);
  if (!text) return '';
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function formatMonthDay(isoDate) {
  const dateOnly = normalizeDueDate(isoDate);
  if (!dateOnly) return '';
  const [year, month, day] = dateOnly.split('-').map(Number);
  const utc = new Date(Date.UTC(year, month - 1, day, 12));
  return utc.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
}

function formatFollowUpTiming(dueValue, { today, now, timeZone } = {}) {
  const due = normalizeDueDate(dueValue);
  if (!due) return { kind: 'none', label: null, iso: null };
  const todayIso = today || todayISOInZone(now || new Date(), timeZone || DEFAULT_AO_TIMEZONE);
  const monthDay = formatMonthDay(due);
  if (due < todayIso) {
    return { kind: 'overdue', label: `Overdue since ${monthDay}`, iso: due };
  }
  if (due === todayIso) {
    return { kind: 'today', label: 'Due today', iso: due };
  }
  return { kind: 'future', label: `Due ${monthDay}`, iso: due };
}

function isGenericEnumAction(value) {
  const text = String(value || '').trim().toLowerCase();
  return /^(research|follow_up|follow-up|follow up|outreach|call|visit|revisit|next)$/i.test(text);
}

function usableHumanText(value) {
  const sanitized = sanitizeAoFacingText(value);
  if (!sanitized) return null;
  if (isGenericEnumAction(sanitized)) return null;
  if (INTERNAL_METADATA_KEYS.some(key => sanitized.includes(key))) return null;
  if (/[{[]/.test(sanitized)) return null;
  if (UUID_RE.test(sanitized) || ASSIGNMENT_SLUG_RE.test(sanitized)) return null;
  return sanitized;
}

function stripOwnerAttributionLine(text, businessName, ownerName) {
  if (!text) return text;
  const lines = String(text).split('\n').map(line => line.trim()).filter(Boolean);
  const company = String(businessName || '').trim().toLowerCase();
  const owner = String(ownerName || '').trim().toLowerCase();
  return lines
    .filter(line => {
      const lower = line.toLowerCase();
      if (company && owner && lower === `${company} — ${owner}`) return false;
      if (company && owner && lower === `${company} - ${owner}`) return false;
      if (company && lower === company) return false;
      return true;
    })
    .join('\n')
    .trim();
}

function conversationNote(row, metadata) {
  const raw = row.last_interaction_summary || row.original_visit_note || '';
  if (!raw || looksLikeAssignmentNote(raw)) {
    const seedNote = String(row.original_visit_note || '').trim();
    if (seedNote && !looksLikeAssignmentNote(seedNote) && !/received direct mail before ao visit/i.test(seedNote)) {
      return usableHumanText(seedNote);
    }
    return null;
  }
  let cleaned = usableHumanText(raw);
  cleaned = stripOwnerAttributionLine(cleaned, row.business_name, metadata.owner);
  cleaned = usableHumanText(cleaned);
  if (!cleaned) return null;
  if (isPlaybookRecipe(cleaned)) return null;
  return cleaned;
}

function hasOpenEscalation(row) {
  return Boolean(row.open_escalation_id)
    && !['resolved', 'ignored'].includes(String(row.open_escalation_status || ''));
}

function hasLoggedConversation(row, note) {
  if (note) return true;
  const probes = parseProbeAnswers(row.probe_answers);
  return Object.values(probes).some(value => String(value || '').trim());
}

function conversationTopic(note, contactName) {
  let topic = firstSentence(note);
  if (!topic) return null;
  const name = String(contactName || '').trim();
  if (name) {
    const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    topic = topic.replace(new RegExp(`^${escaped}\\s+`, 'i'), '');
    const firstName = name.split(/\s+/)[0];
    if (firstName && firstName.length > 1) {
      const escapedFirst = firstName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      topic = topic.replace(new RegExp(`^${escapedFirst}\\s+`, 'i'), '');
    }
  }
  topic = topic.replace(/^(asked|mentioned|requested|said|wanted)\s+(?:about\s+|for\s+)?/i, '');
  return topic || firstSentence(note);
}

function firstSentence(text, maxLength = 90) {
  const value = String(text || '').replace(/\s+/g, ' ').trim();
  if (!value) return null;
  const sentence = value.split(/(?<=[.!?])\s+/)[0] || value;
  if (sentence.length <= maxLength) return sentence.replace(/[.!?]$/, '');
  return `${sentence.slice(0, maxLength).trim()}…`;
}

function briefingOperationalState(row) {
  const status = row.lead_status || row.status;
  const forState = {
    ...row,
    status,
    crm_prospect_id: status === 'converted_to_crm' ? row.crm_prospect_id : null,
  };
  return deriveOperationalState(forState);
}

function derivePipelineStage(row, metadata, state) {
  const explicit = String(metadata.pipeline_stage || row.pipeline_stage || '').trim().toLowerCase();
  const nextAction = String(row.next_action || row.open_next_action || metadata.next_action || '').toLowerCase();
  const status = String(row.lead_status || row.status || '').toLowerCase();

  if (['walkthrough_requested', 'walkthrough_booked', 'walkthrough_completed'].includes(status) || state === 'walkthrough_requested' || /walkthrough|tour/.test(nextAction)) {
    return 'walkthrough';
  }
  if (status === 'proposal_needed' || /proposal|quote/.test(nextAction) || /proposal/.test(explicit)) {
    return 'proposal';
  }
  if (explicit === 'research' || /\bresearch\b/.test(nextAction)) return 'research';
  if (explicit) return explicit.replace(/[_-]+/g, ' ');
  if (state === 'not_started' || state === 'no_contact') return 'research';
  if (state === 'follow_up_needed') return 'follow-up';
  return null;
}

function stageLabel(stage) {
  if (!stage) return null;
  if (stage === 'research') return 'Research stage';
  if (stage === 'walkthrough') return 'Walkthrough stage';
  if (stage === 'proposal') return 'Proposal stage';
  if (stage === 'follow-up' || stage === 'follow_up') return 'Follow-up stage';
  return `${titleCase(stage)} stage`;
}

function stateLabel(state, followUp) {
  if (state === 'jake_action_needed') return 'Waiting on Jake';
  if (state === 'walkthrough_requested') return 'Walkthrough requested';
  if (state === 'decision_maker_reached') return 'Decision-maker reached';
  if (state === 'contact_identified') return 'Contact identified';
  if (state === 'gatekeeper_reached') return 'Gatekeeper reached';
  if (state === 'not_started') return 'Not started';
  if (followUp.kind === 'overdue') return 'Follow-up overdue';
  if (followUp.kind === 'today') return 'Follow-up due today';
  if (state === 'follow_up_needed') return 'Follow-up needed';
  return titleCase(state);
}

function buildStatusLine(view) {
  const followUp = view.follow_up || { kind: 'none' };
  const state = view.operational_state;
  const waitingOnJake = view.waiting_on_jake;
  const stage = view.stage;
  const parts = [];
  const stageText = stageLabel(stage);
  if (stageText) parts.push(stageText);

  if (waitingOnJake) {
    if (!parts.includes('Waiting on Jake')) parts.push('Waiting on Jake');
    return parts.join(' · ');
  }

  if (followUp.kind === 'overdue') parts.push('follow-up overdue');
  else if (followUp.kind === 'today') parts.push('follow-up due today');
  else if (state === 'follow_up_needed' || stage === 'research') parts.push('follow-up needed');
  else {
    const label = stateLabel(state, followUp);
    if (label && !parts.includes(label) && !(stageText && label.toLowerCase().includes('follow-up'))) {
      parts.push(label);
    }
  }

  return parts.filter(Boolean).join(' · ') || 'Assigned';
}

function contactRoleGuidance(lane) {
  if (lane.id === 'property_management' || lane.id === 'development') {
    return 'Ask for the property manager, facilities manager, or person responsible for janitorial/cleaning vendors.';
  }
  if (lane.id === 'commercial_office') {
    return 'Ask for the office manager, facilities manager, or practice manager who handles cleaning vendors.';
  }
  return 'Ask for the office manager, facilities manager, or person who handles cleaning vendors.';
}

function decisionMakerRolePhrase(lane) {
  if (lane.id === 'property_management' || lane.id === 'development') {
    return 'facilities/property-management decision-maker';
  }
  if (lane.id === 'commercial_office') {
    return 'office or facilities manager who handles cleaning vendors';
  }
  return 'person who handles cleaning vendors';
}

function waitingOnOperatorAction(row) {
  const summary = usableHumanText(row.open_escalation_summary || row.open_escalation_reason);
  if (summary) return summary.replace(/\.$/, '');
  const status = String(row.lead_status || row.status || '');
  if (status === 'proposal_needed') return 'proposal follow-through';
  if (['walkthrough_requested', 'walkthrough_booked'].includes(status)) return 'walkthrough scheduling';
  return null;
}

function deriveFieldNextAction(view) {
  const contactName = view.contact_name;
  const lanePhrase = decisionMakerRolePhrase(view.lane);
  const topic = view.conversation_topic;
  const suggested = view.suggested_message;

  if (view.waiting_on_jake) {
    const operatorAction = waitingOnOperatorAction(view.source_row);
    if (operatorAction) {
      return `Waiting on Jake for ${operatorAction}. No AO action required until then.`;
    }
    return 'Waiting on Jake. No AO action required until then.';
  }

  if (view.stage === 'walkthrough' || view.operational_state === 'walkthrough_requested') {
    return 'Confirm walkthrough date/time and gather access/scope details.';
  }

  if (view.stage === 'proposal' || view.source_row.status === 'proposal_needed' || view.source_row.lead_status === 'proposal_needed') {
    return 'Follow up on the proposal and ask whether any scope or timing questions remain.';
  }

  if ((view.stage === 'research' || isGenericEnumAction(view.raw_next_action)) && !view.has_contact) {
    return `Identify the ${lanePhrase} and confirm the best contact route.`;
  }

  if (view.has_contact && !view.has_conversation) {
    const who = contactName || 'the main office';
    const via = view.contact_phone ? ` at ${view.contact_phone}` : '';
    if (contactName) {
      return `Call ${contactName}${via} and introduce Anchor as a reliable backup/overflow cleaning resource.`;
    }
    return `Call ${who}${via} and introduce Anchor as a reliable backup/overflow cleaning resource.`;
  }

  if (view.has_conversation && (view.follow_up.kind === 'overdue' || view.follow_up.kind === 'today' || view.follow_up.kind === 'future')) {
    const who = contactName || 'the account';
    if (topic) {
      return `Follow up with ${who} about ${topic} and ask for the next concrete step.`;
    }
    return `Follow up with ${who} and ask for the next concrete step.`;
  }

  if (/phone_follow_up/i.test(String(view.raw_next_action || ''))) {
    const phone = view.contact_phone ? ` at ${view.contact_phone}` : '';
    const who = contactName ? ` ${contactName}` : '';
    return `Call${who}${phone} and log the outcome with Max.`;
  }

  if (/in_person_revisit|revisit/i.test(String(view.raw_next_action || ''))) {
    const who = contactName ? ` Ask for ${contactName}.` : ` Ask for the ${lanePhrase}.`;
    return `Stop by in person and log the visit with Max.${who}`;
  }

  if (view.operational_state === 'not_started') {
    return 'Make first contact, identify the cleaning decision-maker, and log the result with Max.';
  }

  if (view.operational_state === 'gatekeeper_reached') {
    return `Ask for the ${lanePhrase} and the best time to reach them.`;
  }

  if (suggested) {
    return suggested;
  }

  if (view.raw_next_action && !isGenericEnumAction(view.raw_next_action) && usableHumanText(view.raw_next_action)) {
    return usableHumanText(view.raw_next_action);
  }

  if (!view.has_contact) {
    return `Identify the ${lanePhrase} and choose the best outreach route.`;
  }

  return 'Log the visit or follow-up outcome with Max.';
}

function buildWhyItMatters(view) {
  const priority = view.priority;
  const laneLabel = view.lane.confidence === 'high' ? view.lane.label : null;
  const priorityPrefix = (priority === 'high' || priority === 'warm')
    ? (priority === 'warm' ? 'Warm' : 'High-priority')
    : null;
  const accountNoun = laneLabel ? `${laneLabel} account` : 'account assigned to you';

  if (priorityPrefix && view.follow_up.kind === 'overdue' && !view.has_contact) {
    return `${priorityPrefix} ${accountNoun} with an overdue follow-up and no decision-maker identified yet.`;
  }
  if (priorityPrefix && view.follow_up.kind === 'overdue') {
    return `${priorityPrefix} ${accountNoun} assigned to you. Follow-up is overdue.`;
  }
  if (view.follow_up.kind === 'overdue' && !view.has_contact) {
    return `Follow-up is overdue and no decision-maker is identified yet.`;
  }
  if (view.follow_up.kind === 'overdue') {
    return 'Follow-up is overdue.';
  }
  if (view.follow_up.kind === 'today') {
    return 'Follow-up is due today.';
  }
  if (view.operational_state === 'walkthrough_requested' || view.stage === 'walkthrough') {
    return 'Walkthrough opportunity needs your follow-up.';
  }
  if (priority === 'warm') {
    return laneLabel
      ? `Warm ${laneLabel} account with active follow-up.`
      : 'Warm lead with active follow-up.';
  }
  if (view.interest_level === 'high' && view.has_conversation) {
    return 'High-interest conversation needs your next touch.';
  }
  if (priorityPrefix) {
    return `${priorityPrefix} ${accountNoun} in your assigned portfolio.`;
  }
  if (view.operational_state === 'not_started') {
    return 'Assigned account with no visit logged yet.';
  }
  if (view.follow_up.kind === 'future') {
    return `Next follow-up ${view.follow_up.label.replace(/^Due /, 'due ')}.`;
  }
  return 'Assigned account in your queue.';
}

function buildKnownFacts(view) {
  const known = [];
  if (view.lane.confidence === 'high' && view.lane.label) {
    known.push(`${titleCase(view.lane.label)} account in your assigned portfolio`);
  } else {
    known.push('Assigned account in your portfolio');
  }
  if (view.priority === 'high') known.push('High priority');
  if (view.priority === 'warm') known.push('Warm priority');
  if (view.assignment_provenance) known.push(view.assignment_provenance);
  if (!view.has_contact) known.push('No decision-maker captured yet');
  if (view.has_contact && view.contact_name) {
    const title = view.contact_title ? ` (${view.contact_title})` : '';
    known.push(`Contact on file: ${view.contact_name}${title}`);
  }
  if (!view.has_conversation) known.push('No recent conversation logged');
  if (view.conversation_note) known.push(`Last conversation: ${firstSentence(view.conversation_note, 140)}`);
  if (view.intel.current_vendor) known.push(`Current vendor: ${view.intel.current_vendor}`);
  if (view.intel.current_pain) known.push(`Pain point: ${view.intel.current_pain}`);
  if (view.address) known.push(`Address: ${view.address}`);
  if (view.interest_level === 'high' && view.has_conversation) known.push('High interest recorded');
  return known;
}

function buildUnknownFacts(view) {
  const unknown = [];
  if (!view.has_contact) unknown.push('Decision-maker name');
  if (!view.has_contact || !view.contact_phone) unknown.push('Best contact route');
  if (!view.intel.current_vendor) unknown.push('Whether cleaning is handled in-house or outsourced');
  if (!view.intel.current_pain && !view.intel.current_vendor) unknown.push('Whether they have a backup vendor need');
  if ((view.stage === 'walkthrough' || view.operational_state === 'walkthrough_requested') && !/\b\d{1,2}\/\d{1,2}\b|\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/i.test(view.conversation_note || '')) {
    unknown.push('Walkthrough date/time and access/scope details');
  }
  return unknown;
}

function whoToAskFor(view) {
  if (view.contact_name) {
    const role = view.intel.contact_role === 'decision_maker'
      ? 'decision-maker'
      : (view.contact_title || 'contact on file');
    const title = view.contact_title && view.intel.contact_role === 'decision_maker'
      ? ` (${view.contact_title})`
      : '';
    const phone = view.contact_phone ? ` Phone on file: ${view.contact_phone}.` : '';
    if (view.intel.contact_role === 'decision_maker') {
      return `${view.contact_name}${title} · ${role}. Ask for them directly.${phone}`;
    }
    return `${view.contact_name}${view.contact_title ? ` (${view.contact_title})` : ''} · ${role}. ${contactRoleGuidance(view.lane)}${phone}`;
  }
  if (view.intel.decision_maker_name) {
    return `${view.intel.decision_maker_name}. Ask for them directly.`;
  }
  return `No contact identified yet. ${contactRoleGuidance(view.lane)}`;
}

function normalizeAccountInput(row) {
  const metadata = {
    ...extractAssignmentMetadata(row.last_interaction_summary),
    ...extractAssignmentMetadata(row.original_visit_note),
  };

  return {
    business_name: row.business_name,
    address: row.address,
    business_type: row.business_type || metadata.lane || null,
    status: row.lead_status || row.status,
    lead_status: row.lead_status || row.status,
    interest_level: String(row.interest_level || metadata.interest_level || 'medium').toLowerCase(),
    priority: normalizePriority(row.priority || row.task_priority || metadata.priority),
    due_date: row.due_date || row.open_task_due || row.next_follow_up_date || metadata.due_date,
    next_action: row.next_action || row.open_next_action || metadata.next_action || metadata.initial_next_action,
    open_next_action: row.next_action || row.open_next_action || metadata.next_action,
    suggested_message: usableHumanText(row.suggested_message),
    waiting_on_jake: Boolean(row.waiting_on_jake),
    last_interaction_summary: row.last_interaction_summary,
    original_visit_note: row.original_visit_note,
    probe_answers: row.probe_answers,
    contact_name: row.contact_name || null,
    contact_title: row.contact_title || null,
    contact_phone: row.contact_phone || null,
    contact_email: row.contact_email || null,
    is_decision_maker: row.is_decision_maker,
    open_escalation_id: row.open_escalation_id,
    open_escalation_status: row.open_escalation_status,
    open_escalation_reason: row.open_escalation_reason,
    open_escalation_summary: row.open_escalation_summary,
    open_task_status: row.task_status || row.open_task_status,
    attribution_source: row.attribution_source,
    campaign_name: row.campaign_name,
    metadata,
  };
}

function buildAoAccountView(row, { today, now, timeZone } = {}) {
  const source = normalizeAccountInput(row);
  const metadata = source.metadata;
  const lane = classifyAccountLane(source, metadata);
  const followUp = formatFollowUpTiming(source.due_date, { today, now, timeZone });
  const operationalState = briefingOperationalState(source);
  const intel = buildRelationshipIntel({
    ...source,
    open_next_action: source.open_next_action,
  });
  const note = conversationNote(source, metadata);
  const hasContact = Boolean(source.contact_name || intel.decision_maker_name);
  const hasConversation = hasLoggedConversation(source, note);
  const stage = derivePipelineStage(source, metadata, operationalState);
  const waitingOnJake = Boolean(source.waiting_on_jake) || hasOpenEscalation(source);
  const assignedThroughBatch = /ao assignment/i.test(String(metadata.source || ''))
    || looksLikeAssignmentNote(source.last_interaction_summary)
    || looksLikeAssignmentNote(source.original_visit_note);
  const assignmentProvenance = assignedThroughBatch ? 'Assigned through Anchor AO batch' : null;

  const view = {
    business_name: source.business_name,
    address: source.address || null,
    lane,
    priority: source.priority,
    interest_level: source.interest_level,
    follow_up: followUp,
    operational_state: operationalState,
    stage,
    intel,
    contact_name: source.contact_name,
    contact_title: source.contact_title,
    contact_phone: source.contact_phone,
    has_contact: hasContact,
    has_conversation: hasConversation,
    conversation_note: note,
    conversation_topic: note ? conversationTopic(note, source.contact_name) : null,
    waiting_on_jake: waitingOnJake,
    raw_next_action: source.next_action,
    suggested_message: source.suggested_message,
    assignment_provenance: assignmentProvenance,
    source_row: source,
  };

  view.status_line = buildStatusLine(view);
  view.why_it_matters = buildWhyItMatters(view);
  view.who_to_ask_for = whoToAskFor(view);
  view.known = buildKnownFacts(view);
  view.unknown = buildUnknownFacts(view);
  view.next_action = deriveFieldNextAction(view);
  return view;
}

function bulletList(items) {
  return items.map(item => `- ${item}`).join('\n');
}

function formatAccountBriefing(lead, options = {}) {
  const view = buildAoAccountView(lead, options);
  const lines = [
    `Briefing — ${view.business_name}`,
    '',
    'Why it matters:',
    view.why_it_matters,
    '',
    'Status:',
    view.status_line,
    '',
    'Who to ask for:',
    view.who_to_ask_for,
    '',
    'What we know:',
    bulletList(view.known),
    '',
    'What we still need:',
    view.unknown.length ? bulletList(view.unknown) : '- No additional gaps recorded',
    '',
    'Next action:',
    view.next_action,
  ];

  if (view.follow_up.label) {
    lines.push('', 'Follow-up:', view.follow_up.label);
  }

  return lines.join('\n');
}

function formatNameList(names) {
  if (names.length === 2) return `${names[0]} and ${names[1]}`;
  const last = names[names.length - 1];
  return `${names.slice(0, -1).join(', ')}, and ${last}`;
}

function formatPrioritizationResponse(accounts, { hasOverdue = false } = {}) {
  if (!accounts.length) {
    return [
      "You don't currently have any assigned accounts in your queue.",
      '',
      'Check with Jake if you should receive new direct-mail targets, or use Log Visit to add a business you stopped by.',
    ].join('\n');
  }

  const lines = [];
  if (!hasOverdue) {
    lines.push('No overdue follow-ups — here are your highest-priority assigned accounts:');
  } else {
    lines.push('Here are your top accounts to work today:');
  }
  lines.push('');

  accounts.forEach((account, index) => {
    lines.push(`${index + 1}. ${account.business_name}`);
    lines.push(`   Why: ${account.why_now}`);
    lines.push(`   Status: ${account.status_label}`);
    lines.push(`   Next: ${account.next_step}`);
    lines.push('');
  });

  const first = accounts[0];
  const tied = first.rank_score != null
    ? accounts.filter(account => account.rank_score === first.rank_score)
    : [first];

  if (tied.length > 1) {
    lines.push(
      `${formatNameList(tied.map(account => account.business_name))} are tied on current operational priority. ${first.business_name} is listed first by due date, then name.`,
    );
  } else {
    lines.push(`Start with ${first.business_name} because ${String(first.why_now || 'it is highest on your assigned queue').replace(/\.$/, '').toLowerCase()}.`);
  }

  return lines.join('\n');
}

function formatAccountContactsReply(lead, options = {}) {
  if (!lead) {
    return 'I could not find that account in your assigned list.';
  }

  const view = buildAoAccountView(lead, options);
  const lines = [`Contacts — ${view.business_name}`, '', view.who_to_ask_for];

  if (view.has_contact && view.intel.contact_role !== 'decision_maker' && view.contact_name) {
    lines.push(`If they are not the cleaning decision-maker, ask for the ${decisionMakerRolePhrase(view.lane)}.`);
  }

  return lines.join('\n');
}

module.exports = {
  DEFAULT_AO_TIMEZONE,
  INTERNAL_METADATA_KEYS,
  todayISOInZone,
  extractAssignmentMetadata,
  sanitizeAoFacingText,
  classifyAccountLane,
  contactRoleGuidance,
  formatFollowUpTiming,
  buildAoAccountView,
  deriveFieldNextAction,
  formatAccountBriefing,
  formatPrioritizationResponse,
  formatAccountContactsReply,
};
