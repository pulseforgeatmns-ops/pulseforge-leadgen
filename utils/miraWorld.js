const LIVE_WORKSTREAMS = Object.freeze([
  Object.freeze({
    key: 'pulseforge',
    name: 'Pulseforge',
    client_id: 1,
    market: 'Providence, RI',
    focus: 'service-business outreach',
  }),
  Object.freeze({
    key: 'anchor',
    name: 'Anchor Cleaning',
    client_id: 10,
    market: 'Manchester, NH',
    focus: 'law-firm and CPA outreach',
  }),
  Object.freeze({
    key: 'upwork',
    name: 'Upwork',
    client_id: null,
    market: null,
    focus: 'freelance income',
  }),
]);

const ACTIVE_CLIENT_IDS = Object.freeze([1, 10]);
const ACTIVE_TODOIST_PROJECTS = Object.freeze(['Anchor Outreach', 'Pulseforge', 'Inbox']);
const RETIRED_CONTEXT_PATTERN = /\b(MSHI|Mountain State Home Innovations|Nashville|Brad|Dustin)\b/i;

const PROJECTS = Object.freeze(LIVE_WORKSTREAMS.map(workstream => {
  const scope = workstream.client_id ? `client_id=${workstream.client_id}` : workstream.focus;
  const detail = [workstream.focus, workstream.market].filter(Boolean).join(' in ');
  return `${workstream.name} (${scope}) — ${detail}`;
}));

const ROUTING_CONTEXT = [
  'Pulseforge/client_id=1 means Providence RI service-business outreach.',
  'Anchor Cleaning/client_id=10 means Manchester NH law-firm and CPA outreach.',
  'Do not infer client_id=1 merely from New Hampshire geography; current New Hampshire outreach belongs to Anchor/client_id=10.',
].join(' ');

const JACOB_CONTEXT =
  'Jacob is a solo founder with fragmented attention. His three active workstreams are ' +
  'Pulseforge (client_id=1: service-business outreach in Providence, Rhode Island), ' +
  'Anchor Cleaning (client_id=10: law-firm and CPA outreach in Manchester, New Hampshire), ' +
  'and Upwork (freelance income). Jacob Unbound is resting and is not active context. ' +
  'New Hampshire outreach is Anchor work, not Pulseforge Providence work. He has limited ' +
  'focused hours per day and needs the one or two highest-leverage moves surfaced, not a to-do list.';

function isActiveTodoistContextItem(project, content) {
  const text = String(content || '').trim();
  return ACTIVE_TODOIST_PROJECTS.includes(project)
    && Boolean(text)
    && !RETIRED_CONTEXT_PATTERN.test(text)
    && !/^Alert:/i.test(text);
}

module.exports = {
  LIVE_WORKSTREAMS,
  ACTIVE_CLIENT_IDS,
  ACTIVE_TODOIST_PROJECTS,
  PROJECTS,
  ROUTING_CONTEXT,
  JACOB_CONTEXT,
  isActiveTodoistContextItem,
};
