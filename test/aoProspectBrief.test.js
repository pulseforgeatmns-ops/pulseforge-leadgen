'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  buildProspectBriefSections,
  formatProspectBrief,
  isGenericIcpText,
} = require('../utils/aoProspectBrief');

test('filters generic ICP why-account copy from brief context', () => {
  const generic = 'Curtin Law Office is a law firm where a consistent office presentation matters and vendor decisions usually sit with office management.';
  assert.equal(isGenericIcpText(generic), true);

  const sections = buildProspectBriefSections({
    prospect: {
      vertical: 'law_firm',
      phone: '(603) 669-7700',
      assigned_ao_id: 26,
      ao_why_account_matters: generic,
    },
    company: {
      name: 'Curtin Law Office',
      location: '40 Bay St, Manchester, NH 03104',
    },
    touchpoints: [],
    activity: [],
    task: {
      status: 'open',
      why_account_matters: generic,
      first_action: 'Call and ask who handles cleaning or facilities vendors for the office.',
      suggested_opener: 'Hi, I\'m Jake with Anchor Cleaning. I was hoping to ask who usually handles cleaning or facilities decisions for the office. We help local offices when cleaning starts creating extra work for the team, but I don\'t want to assume that\'s relevant here.',
    },
    aoName: 'Jake',
  });

  assert.match(sections.where_this_stands, /No prior conversation or decision-maker is logged yet/i);
  assert.match(sections.why_this_next, /identify who owns cleaning\/vendor decisions/i);
  assert.doesNotMatch(sections.known_context, /consistent office presentation/i);
  assert.doesNotMatch(sections.known_context, /Anchor'?s lane/i);
  assert.match(sections.known_context, /Curtin Law Office/);
  assert.match(sections.known_context, /\(603\) 669-7700/);
  assert.match(sections.known_context, /40 Bay St, Manchester/);
  assert.match(sections.next_action, /Call and ask who handles cleaning/i);
  assert.doesNotMatch(sections.next_action, /follow up/i);
  assert.match(sections.what_to_listen_for, /vendor decisions/i);
  assert.match(sections.short_talk_track, /Jake with Anchor Cleaning/);
});

test('formatProspectBrief uses account-state section headings', () => {
  const brief = formatProspectBrief({
    prospect: { vertical: 'law_firm', assigned_ao_id: 1 },
    company: { name: 'Sparse Co', location: 'Manchester NH' },
    touchpoints: [],
    activity: [],
  });
  assert.match(brief, /Where this stands/);
  assert.match(brief, /Why this is the next move/);
  assert.match(brief, /Known context/);
  assert.match(brief, /Next action/);
  assert.match(brief, /What to listen for/);
  assert.match(brief, /Short talk track/);
  assert.doesNotMatch(brief, /Why this account matters/);
});
