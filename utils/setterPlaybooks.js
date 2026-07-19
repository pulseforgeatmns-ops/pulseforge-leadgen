'use strict';

const ANCHOR_QUESTIONS = Object.freeze({
  cleaning_company_overflow: [
    'Do you ever turn away jobs because your team is full or short-staffed?',
    'Which job types or service areas are hardest to cover?',
    'What would you need from an overflow partner before trusting them with a client?',
  ],
  str_manager: [
    'How many units or turnovers are you coordinating in a typical week?',
    'Where do last-minute schedule changes create the most pressure?',
    'What matters most in a backup turnover-cleaning partner?',
  ],
  property_manager: [
    'What kinds of turns, move-outs, or common-area jobs do you coordinate?',
    'Where do coverage gaps or vendor delays happen most often?',
    'How do you approve a new cleaning vendor?',
  ],
  realtor: [
    'How often do clients need pre-listing, move-in, or move-out cleaning?',
    'Do you already have a reliable partner for short-notice work?',
    'What would make a cleaning referral partner easy for you to use?',
  ],
  restoration_remodeling_partner: [
    'How often do projects need post-construction or final-detail cleaning?',
    'What timing or quality issues do you run into with cleanup?',
    'Who decides when a cleaning subcontractor is added to a job?',
  ],
  commercial_office: [
    'How is cleaning coverage handled today?',
    'Do absences, special projects, or schedule changes create gaps?',
    'When do you review or add cleaning vendors?',
  ],
});

function humanSetterPlaybook({ clientId, clientName = 'the client', vertical = 'general' }) {
  const anchor = Number(clientId) === 10;
  const questions = anchor && ANCHOR_QUESTIONS[vertical]
    ? ANCHOR_QUESTIONS[vertical]
    : [
        'How are you handling this need today?',
        'Where is the current process creating the most friction?',
        'Who else is involved in deciding the next step?',
      ];
  return {
    mode: 'human_only',
    title: anchor ? 'Anchor partner outreach' : `${clientName} outreach`,
    objective: anchor
      ? 'Confirm a real cleaning need and agree on a specific human follow-up.'
      : 'Understand the need, identify the decision maker, and agree on a specific next action.',
    opener: anchor
      ? 'Hi, is this [name]? I’m [operator] calling with Anchor Cleaning Services. We help local businesses cover overflow and time-sensitive cleaning work. I wanted to ask whether you ever need a reliable local cleaning partner when your regular coverage is stretched.'
      : `Hi, is this [name]? I’m [operator], calling on behalf of ${clientName}. I’ll keep it brief—do you have a minute for one quick question?`,
    qualification_questions: questions,
    objection_prompts: [
      { objection: 'We already have someone', response: 'That makes sense. Is there ever a need for backup coverage, overflow, or short-notice work?' },
      { objection: 'Send information', response: 'Absolutely. What would be most useful to include, and when should I follow up after you review it?' },
      { objection: 'Not a good time', response: 'No problem. What day and time would be better for a short follow-up?' },
      { objection: 'Not interested', response: 'Understood. I’ll record that so the team handles it correctly. Thank you for your time.' },
    ],
    required_close: 'Before saving, record the decision maker, interest level, disposition, and either a dated next action or a clear no-follow-up outcome.',
    safety: 'The operator places every call and chooses every outcome. Pulseforge does not initiate calls or send follow-ups from this workflow.',
  };
}

module.exports = { ANCHOR_QUESTIONS, humanSetterPlaybook };
