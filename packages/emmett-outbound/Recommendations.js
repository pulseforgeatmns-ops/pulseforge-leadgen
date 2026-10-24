'use strict';

/**
 * SPEC-117 — deliverability recommendations that teach.
 */

function buildRecommendations(snapshot = {}, health = {}, capacity = {}, now = new Date()) {
  const recs = [];
  const warmup = String(snapshot.warmup?.status || '');
  const weekday = weekdayName(now, snapshot.timeZone || 'America/New_York');
  const hour = localHour(now, snapshot.timeZone || 'America/New_York');
  const replyByWeekday = snapshot.replyByWeekday || {};
  const fridayDrop = Number(replyByWeekday.Fri ?? replyByWeekday.Friday ?? 0);
  const tuesdayLift = Number(replyByWeekday.Tue ?? replyByWeekday.Tuesday ?? 0);
  const baselineReply = average(Object.values(replyByWeekday).map(Number).filter((n) => Number.isFinite(n)));

  if (warmup === 'warming') {
    recs.push({
      id: 'warmup_gradual',
      severity: 'info',
      title: 'Domain warming',
      body: 'Increase gradually Monday. No action needed beyond the recommended cap.',
    });
  }
  if (weekday === 'Fri' && hour >= 14 && baselineReply > 0 && fridayDrop > 0 && fridayDrop <= baselineReply * 0.7) {
    const dropPct = Math.round((1 - fridayDrop / baselineReply) * 100);
    recs.push({
      id: 'friday_afternoon',
      severity: 'warning',
      title: 'Pause Friday afternoon',
      body: `Historically your audience replies ${dropPct}% less after 2PM Fridays.`,
    });
  }
  if (tuesdayLift > 0 && baselineReply > 0 && tuesdayLift >= baselineReply * 1.15) {
    recs.push({
      id: 'tuesday_morning',
      severity: 'info',
      title: 'Tuesday morning lift',
      body: 'Replies increased after Tuesday morning sends.',
    });
  }
  if (capacity.outlook === 'increase') {
    recs.push({
      id: 'increase_gradually',
      severity: 'info',
      title: 'Increase gradually Monday',
      body: `If metrics hold, tomorrow's outlook is about ${capacity.tomorrow?.high || capacity.recommended} emails — not a jump to the provider ceiling.`,
    });
  }
  if (health.score < 70 && health.score >= 40) {
    recs.push({
      id: 'watch_health',
      severity: 'warning',
      title: 'Inbox on watch',
      body: (health.reasons || []).slice(0, 3).join('. ') || 'Health is below healthy. Keep volume conservative.',
    });
  }
  if (!recs.length) {
    recs.push({
      id: 'hold_course',
      severity: 'info',
      title: 'Hold course',
      body: 'Reputation is stable. Stay inside today\'s recommended capacity.',
    });
  }
  return recs;
}

function weekdayName(now, timeZone) {
  return new Intl.DateTimeFormat('en-US', { timeZone, weekday: 'short' }).format(now);
}

function localHour(now, timeZone) {
  return Number(new Intl.DateTimeFormat('en-US', { timeZone, hour: 'numeric', hourCycle: 'h23' }).format(now));
}

function average(values) {
  if (!values.length) return 0;
  return values.reduce((sum, n) => sum + n, 0) / values.length;
}

module.exports = {
  buildRecommendations,
};
