const MEANINGFUL_SIGNAL_TYPES = new Set([
  'company_signal_detected', 'icp_score_changed', 'email_human_opened', 'email_clicked',
  'email_replied', 'email_positive_reply', 'email_negative_reply', 'email_unsubscribed',
  'email_hard_bounced', 'email_soft_bounced', 'enrichment_succeeded', 'phone_found',
  'email_verified', 'operator_marked_warm', 'operator_marked_hot',
]);

const DIRECT_STATE_EVENT_PRIORITY = Object.freeze({
  operator_nulled: 100,
  contact_invalid: 100,
  email_hard_bounced_confirmed_invalid: 100,
  email_unsubscribed: 90,
  operator_disqualified: 90,
  email_positive_reply: 80,
  email_meaningful_reply: 80,
  operator_marked_hot: 70,
  operator_marked_warm: 60,
});

function asDate(value) {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function eventDate(event) {
  return asDate(event.event_timestamp || event.observed_at || event.event_at || event.created_at);
}

function withinDays(event, now, days) {
  const at = eventDate(event);
  if (!at) return false;
  const age = now.getTime() - at.getTime();
  return age >= 0 && age <= Number(days) * 86400000;
}

function eventType(event) {
  return String(event.event_type || '').trim().toLowerCase();
}

function isHumanOpen(event) {
  const type = eventType(event);
  if (type === 'email_human_opened') return true;
  return ['opened', 'open'].includes(type) && String(event.metadata?.open_source || '').toLowerCase() === 'human';
}

function isProxyOpen(event) {
  const type = eventType(event);
  return type === 'email_proxy_opened' || type === 'opened_proxy' ||
    (['opened', 'open'].includes(type) && String(event.metadata?.open_source || '').toLowerCase() === 'proxy');
}

function addComponent(components, code, points, description) {
  if (!points) return;
  components.push({ code, points: Number(points), description });
}

function latestDirectStateEvent(signals) {
  const direct = signals.filter(event => DIRECT_STATE_EVENT_PRIORITY[eventType(event)]);
  const newest = events => events.sort((a, b) => {
    const recency = (eventDate(b)?.getTime() || 0) - (eventDate(a)?.getTime() || 0);
    return recency || DIRECT_STATE_EVENT_PRIORITY[eventType(b)] - DIRECT_STATE_EVENT_PRIORITY[eventType(a)];
  })[0] || null;
  const terminalTypes = new Set([
    'operator_nulled', 'contact_invalid', 'email_hard_bounced_confirmed_invalid',
    'email_unsubscribed', 'operator_disqualified',
  ]);
  const operatorRestoreTypes = new Set(['operator_marked_hot', 'operator_marked_warm']);
  const terminal = newest(direct.filter(event => terminalTypes.has(eventType(event))));
  const restore = newest(direct.filter(event => operatorRestoreTypes.has(eventType(event))));
  if (restore && (!terminal || eventDate(restore) > eventDate(terminal))) return restore;
  return terminal || newest(direct);
}

function calculateWarmthScore({ prospect = {}, signals = [], config, now = new Date() }) {
  if (!config?.scoring || !config?.signal_windows) throw new Error('A validated Max orchestration config is required');
  const calculatedAt = asDate(now);
  if (!calculatedAt) throw new Error('now must be a valid date');
  const components = [];
  const weights = config.scoring;
  const windows = config.signal_windows;
  const icp = Number(prospect.icp_score || 0);

  if (icp >= 80) addComponent(components, 'ICP_SCORE_80_PLUS', weights.icp_80_plus, `ICP score is ${icp}`);
  else if (icp >= 65) addComponent(components, 'ICP_SCORE_65_TO_79', weights.icp_65_79, `ICP score is ${icp}`);
  else if (icp >= 50) addComponent(components, 'ICP_SCORE_50_TO_64', weights.icp_50_64, `ICP score is ${icp}`);

  if (String(prospect.vertical_tier || '').toUpperCase() === 'A') {
    addComponent(components, 'TIER_A_VERTICAL', weights.tier_a_vertical, 'Prospect belongs to a Tier A vertical');
  }

  const humanOpens = signals.filter(event => isHumanOpen(event) && withinDays(event, calculatedAt, windows.human_opens_days));
  if (humanOpens.length >= 3) addComponent(components, 'THREE_OR_MORE_HUMAN_OPENS', weights.third_human_open, `${humanOpens.length} human opens occurred within ${windows.human_opens_days} days`);
  else if (humanOpens.length === 2) addComponent(components, 'SECOND_HUMAN_OPEN', weights.second_human_open, `Two human opens occurred within ${windows.human_opens_days} days`);
  else if (humanOpens.length === 1) addComponent(components, 'FIRST_HUMAN_OPEN', weights.first_human_open, `One human open occurred within ${windows.human_opens_days} days`);

  const verifiedClicks = signals.filter(event => eventType(event) === 'email_clicked' && event.metadata?.verified !== false && withinDays(event, calculatedAt, windows.click_days));
  if (verifiedClicks.length) addComponent(components, 'VERIFIED_LINK_CLICK', weights.verified_click, `A verified link click occurred within ${windows.click_days} days`);

  const deltas = signals
    .filter(event => eventType(event) === 'icp_score_changed' && withinDays(event, calculatedAt, windows.icp_delta_days))
    .map(event => Number(event.metadata?.delta ?? Number(event.metadata?.new_score || 0) - Number(event.metadata?.old_score || 0)))
    .filter(Number.isFinite);
  const maxDelta = deltas.length ? Math.max(...deltas) : 0;
  if (maxDelta >= 40) addComponent(components, 'ICP_INCREASE_40', weights.icp_delta_40, `ICP score increased by ${maxDelta} within ${windows.icp_delta_days} days`);
  else if (maxDelta >= 15) addComponent(components, 'ICP_INCREASE_15', weights.icp_delta_15, `ICP score increased by ${maxDelta} within ${windows.icp_delta_days} days`);

  if (prospect.decision_maker === true) addComponent(components, 'DECISION_MAKER_CONFIRMED', weights.decision_maker, 'Named decision-maker is confirmed');
  if (prospect.email && prospect.email_verified === true) addComponent(components, 'VERIFIED_DIRECT_EMAIL', weights.verified_email, 'Verified direct email is available');
  if (prospect.phone) addComponent(components, 'PHONE_AVAILABLE', weights.phone_available, 'Phone number is available');

  const meaningful = signals.filter(event => MEANINGFUL_SIGNAL_TYPES.has(eventType(event)) && !isProxyOpen(event));
  const latestMeaningful = meaningful.map(eventDate).filter(Boolean).sort((a, b) => b - a)[0] || null;
  if (latestMeaningful) {
    const ageHours = (calculatedAt - latestMeaningful) / 3600000;
    if (ageHours <= 24) addComponent(components, 'SIGNAL_WITHIN_24H', weights.recent_24h, 'Most recent meaningful signal occurred within 24 hours');
    else if (ageHours <= 72) addComponent(components, 'SIGNAL_WITHIN_72H', weights.recent_72h, 'Most recent meaningful signal occurred within 72 hours');
    else if (ageHours <= Number(windows.recency_days) * 24) addComponent(components, 'SIGNAL_WITHIN_7D', weights.recent_7d, `Most recent meaningful signal occurred within ${windows.recency_days} days`);
  }

  if (prospect.email && prospect.email_verified !== true) {
    addComponent(components, 'UNVERIFIED_EMAIL_ONLY', weights.unverified_email_only, 'Available email is not verified');
  }
  const enrichmentFailures = signals.filter(event => eventType(event) === 'enrichment_failed' && withinDays(event, calculatedAt, windows.company_signal_days)).length;
  if (enrichmentFailures >= Number(weights.repeated_enrichment_failure_count)) {
    addComponent(components, 'REPEATED_ENRICHMENT_FAILURE', weights.repeated_enrichment_failure, `${enrichmentFailures} enrichment failures occurred recently`);
  }
  const softBounces = signals.filter(event => eventType(event) === 'email_soft_bounced' && withinDays(event, calculatedAt, windows.click_days));
  if (softBounces.length) addComponent(components, 'SOFT_BOUNCE', weights.soft_bounce, 'A soft bounce occurred recently');

  const rawScore = components.reduce((sum, component) => sum + component.points, 0);
  const directEvent = latestDirectStateEvent(signals);
  const latestOfTypes = types => signals
    .filter(event => types.has(eventType(event)))
    .map(eventDate).filter(Boolean).sort((a, b) => b - a)[0]?.toISOString() || null;
  return {
    score: Math.max(0, Math.min(100, Math.round(rawScore))),
    score_version: config.score_version,
    components,
    calculated_at: calculatedAt.toISOString(),
    last_meaningful_signal_at: latestMeaningful?.toISOString() || null,
    last_human_open_at: humanOpens.map(eventDate).filter(Boolean).sort((a, b) => b - a)[0]?.toISOString() || null,
    last_reply_at: latestOfTypes(new Set(['email_replied', 'email_meaningful_reply', 'email_positive_reply', 'email_negative_reply'])),
    last_positive_reply_at: latestOfTypes(new Set(['email_positive_reply'])),
    direct_state_event: directEvent ? eventType(directEvent) : null,
  };
}

module.exports = {
  DIRECT_STATE_EVENT_PRIORITY,
  MEANINGFUL_SIGNAL_TYPES,
  calculateWarmthScore,
  eventDate,
  eventType,
  isHumanOpen,
  isProxyOpen,
  latestDirectStateEvent,
};
