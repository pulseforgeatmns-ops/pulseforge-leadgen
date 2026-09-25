'use strict';

const HIGH_VALUE_CATEGORIES = new Set(['UNFAIR_ADVANTAGE', 'HIGH_VALUE_ICP']);

function startOfDay(dateStr, tz = 'America/New_York') {
  return new Date(`${dateStr}T00:00:00`);
}

function endOfDay(dateStr) {
  return new Date(`${dateStr}T23:59:59.999`);
}

function isSameDay(dueAt, dateStr) {
  if (!dueAt) return false;
  const due = new Date(dueAt);
  const day = startOfDay(dateStr);
  const end = endOfDay(dateStr);
  return due >= day && due <= end;
}

function isOverdue(dueAt, dateStr) {
  if (!dueAt) return false;
  return new Date(dueAt) < startOfDay(dateStr);
}

function computePriorityScore(row, { dateStr, now = new Date() }) {
  let score = 0;

  const dueAt = row.next_action_due_at;
  if (isOverdue(dueAt, dateStr)) score += 100;
  else if (isSameDay(dueAt, dateStr)) score += 75;

  if (['active', 'reopened'].includes(row.conversation_status)) score += 50;

  const fit = Number(row.ao_fit_score) || 0;
  if (fit >= 70) score += 30;
  else if (fit >= 50) score += Math.round((fit / 70) * 30);

  if (HIGH_VALUE_CATEGORIES.has(row.ao_assignment_category)) score += 25;

  const assignedAt = row.assigned_at || row.prospect_created_at;
  if (assignedAt) {
    const daysSince = (now - new Date(assignedAt)) / (1000 * 60 * 60 * 24);
    if (daysSince <= 7 && !row.last_touch_at) score += 15;
  }

  if (row.has_open_flag) score += 10;

  return score;
}

function rankProspectRows(rows, options) {
  return [...rows]
    .map(row => ({
      ...row,
      priority_score: computePriorityScore(row, options),
    }))
    .sort((a, b) => {
      if (b.priority_score !== a.priority_score) return b.priority_score - a.priority_score;
      const aDue = a.next_action_due_at ? new Date(a.next_action_due_at).getTime() : Infinity;
      const bDue = b.next_action_due_at ? new Date(b.next_action_due_at).getTime() : Infinity;
      if (aDue !== bDue) return aDue - bDue;
      return String(a.company_name || '').localeCompare(String(b.company_name || ''));
    });
}

function greetingForHour(hour) {
  if (hour < 12) return 'Good morning';
  if (hour < 17) return 'Good afternoon';
  return 'Good evening';
}

module.exports = {
  HIGH_VALUE_CATEGORIES,
  computePriorityScore,
  rankProspectRows,
  isOverdue,
  isSameDay,
  endOfDay,
  startOfDay,
  greetingForHour,
};
