'use strict';

/**
 * Personalization engine (SPEC-027B / ADR-014).
 * Deterministic composition from discovery evidence — not a template engine.
 * Never invents markets, goals, or challenges. Never copies notes verbatim as body.
 */

const {
  buildSection,
  buildProposalDocument,
  LONG_TERM_ADVANTAGE_BLOCK,
  assertPersonalized,
} = require('./types');
const {
  evidenceRef,
  inventoryEvidence,
  resolveMarkets,
  resolveStrategyContext,
} = require('./evidence');
const { resolvePricingPackage } = require('./pricing');
const { brandVoiceLabel } = require('../playbook/types');

/**
 * @param {object} summary - DiscoverySummary
 * @param {object} [opts]
 * @returns {object} ProposalDocument
 */
function composeProposal(summary, opts = {}) {
  const profile = opts.profile || null;
  const playbook = opts.playbook || null;
  const strategy = opts.strategy || resolveStrategyContext(opts.inputs || {});
  if (playbook && !strategy.playbook) strategy.playbook = playbook;
  const pricing = resolvePricingPackage(
    opts.pricingPackageId || opts.pricingPackage,
    opts.pricingOverrides || {}
  );
  const inventory = inventoryEvidence(summary, profile, strategy, playbook);
  const markets = resolveMarkets(summary, profile, strategy, playbook);
  const warnings = [];

  if (!summary.companyName) {
    throw new Error('Discovery Summary requires companyName');
  }

  const thin =
    inventory.present.filter((k) => k.startsWith('discovery.')).length < 5;
  if (thin) {
    warnings.push(
      'Discovery Summary is thin — several sections state uncertainty rather than invent detail.'
    );
  }
  if (markets.uncertain) {
    warnings.push('Target markets not evidenced; strategy section marks uncertainty.');
  }
  if (!playbook) {
    warnings.push(
      'No Client Playbook attached — proposal lacks playbook voice, offers, and success metrics (ADR-015).'
    );
  }

  const sections = [
    sectionCover(summary),
    sectionExecutiveSummary(summary, playbook),
    sectionUnderstanding(summary),
    sectionWhyPulseforge(summary, playbook),
    sectionRecommendedStrategy(summary, profile, markets, strategy, playbook),
    sectionWhatWeHandle(summary, markets, playbook),
    sectionYourRole(summary, playbook),
    sectionFirst90Days(summary, markets, opts.timelineOverrides, playbook),
    sectionLongTermAdvantage(summary, playbook),
    sectionInvestment(summary, pricing),
    sectionNextSteps(summary, playbook),
  ];

  const nextStepsFlow = [
    'Proposal Approval',
    'Kickoff Meeting',
    'Discovery Profile Approval',
    'Client Playbook Confirmation',
    'Campaign Launch',
  ];

  const evidenceCount = sections.reduce(
    (n, s) => n + (s.evidenceRefs ? s.evidenceRefs.length : 0),
    0
  );

  const document = buildProposalDocument({
    title: 'Commercial Growth Proposal',
    preparedFor: summary.companyName,
    preparedBy: 'Pulseforge',
    contactName: summary.contactName,
    sections,
    pricing,
    nextStepsFlow,
    personalizationScore: scorePersonalization(summary, sections, playbook),
    evidenceCount,
    warnings,
    playbookId: playbook ? playbook.id : null,
    playbookVersion: playbook ? playbook.version : null,
  });

  const check = assertPersonalized(document, summary);
  if (!check.ok) {
    warnings.push('Personalization check soft-fail: ' + check.reasons.join(', '));
    document.warnings = warnings;
    document.personalizationCheck = check;
  } else {
    document.personalizationCheck = check;
  }

  return document;
}

function scorePersonalization(summary, sections, playbook) {
  let score = 0;
  if (summary.companyName) score += 15;
  if (summary.companyStage) score += 10;
  if (summary.geography) score += 10;
  if (summary.goals && summary.goals.length) score += 15;
  if (summary.challenges && summary.challenges.length) score += 15;
  if (summary.icp && summary.icp.length) score += 10;
  if (summary.growthVision) score += 10;
  if (summary.currentProcess) score += 5;
  if (playbook && playbook.valuePropositions && playbook.valuePropositions.length) {
    score += 5;
  }
  if (playbook && playbook.offers && playbook.offers.length) score += 5;
  const refs = sections.reduce(
    (n, s) => n + (s.evidenceRefs && s.evidenceRefs.length ? s.evidenceRefs.length : 0),
    0
  );
  score += Math.min(10, refs);
  return Math.min(100, score);
}

function joinList(items, fallback) {
  if (!items || !items.length) return fallback;
  if (items.length === 1) return items[0];
  if (items.length === 2) return items[0] + ' and ' + items[1];
  return items.slice(0, -1).join(', ') + ', and ' + items[items.length - 1];
}

function weaveNotesHint(summary) {
  if (!summary.notes) return null;
  return summary.notes.length > 180 ? summary.notes.slice(0, 177) + '…' : summary.notes;
}

function sectionCover(summary) {
  return buildSection({
    id: 'cover',
    title: 'Commercial Growth Proposal',
    body:
      'Prepared for:\n' +
      summary.companyName +
      '\n\nPrepared by:\nPulseforge',
    bullets: summary.contactName ? ['Primary contact: ' + summary.contactName] : [],
    evidenceRefs: [
      evidenceRef('discovery', 'companyName', summary.companyName),
    ].concat(
      summary.contactName
        ? [evidenceRef('discovery', 'contactName', summary.contactName)]
        : []
    ),
    editable: false,
  });
}

function sectionExecutiveSummary(summary, playbook) {
  const parts = [];
  parts.push(
    'This proposal is written specifically for ' +
      summary.companyName +
      (summary.geography ? ' in ' + summary.geography : '') +
      (summary.industry ? ', operating in ' + summary.industry : '') +
      '.'
  );

  if (summary.companyStage) {
    parts.push(
      'During discovery, you described the business as ' +
        summary.companyStage +
        ' — that stage shapes both the pace and the first markets we recommend.'
    );
  }

  if (summary.currentClients && summary.currentClients.length) {
    parts.push(
      'You already have footing with ' +
        joinList(summary.currentClients) +
        ' — the work ahead is commercial growth that builds on that base, not a restart.'
    );
  }

  if (summary.goals && summary.goals.length) {
    parts.push('What you asked for most clearly: ' + joinList(summary.goals) + '.');
  }

  if (summary.challenges && summary.challenges.length) {
    parts.push('The friction we heard: ' + joinList(summary.challenges) + '.');
  }

  if (summary.currentProcess) {
    parts.push(
      "Today's process — " +
        summary.currentProcess +
        ' — is workable for early stage, but it will not produce predictable commercial acquisition on its own.'
    );
  }

  if (playbook && playbook.valuePropositions && playbook.valuePropositions.length) {
    parts.push(
      'Your Client Playbook emphasizes ' +
        joinList(playbook.valuePropositions) +
        ' — this proposal and the campaigns that follow will use that language.'
    );
  }

  if (parts.length < 3) {
    parts.push(
      'We have limited structured discovery detail so far; the sections below state what we know and where we still need confirmation with you.'
    );
  }

  const refs = [evidenceRef('discovery', 'companyName', summary.companyName)];
  for (const f of ['companyStage', 'geography', 'industry', 'currentProcess']) {
    if (summary[f]) refs.push(evidenceRef('discovery', f, summary[f]));
  }
  for (const f of ['goals', 'challenges', 'currentClients']) {
    if (summary[f] && summary[f].length) {
      refs.push(evidenceRef('discovery', f, joinList(summary[f])));
    }
  }
  if (playbook && playbook.valuePropositions && playbook.valuePropositions.length) {
    refs.push(
      evidenceRef(
        'client_playbook',
        'valuePropositions',
        joinList(playbook.valuePropositions)
      )
    );
  }

  return buildSection({
    id: 'executive_summary',
    title: 'Executive Summary',
    body: parts.join(' '),
    evidenceRefs: refs,
    uncertain: parts.some((p) => /limited structured discovery/i.test(p)),
  });
}

function sectionUnderstanding(summary) {
  const bullets = [];
  const refs = [];

  if (summary.companyStage || summary.currentClients.length || summary.currentProcess) {
    const stateBits = [];
    if (summary.companyStage) stateBits.push(summary.companyStage);
    if (summary.currentClients.length) {
      stateBits.push('current clients include ' + joinList(summary.currentClients));
    }
    if (summary.currentProcess) {
      stateBits.push('follow-up and pipeline today: ' + summary.currentProcess);
    }
    if (summary.currentMarketingChannels.length) {
      stateBits.push(
        'marketing channels in use: ' + joinList(summary.currentMarketingChannels)
      );
    }
    bullets.push('Current state — ' + stateBits.join('; ') + '.');
    refs.push(evidenceRef('discovery', 'current_state', stateBits.join('; ')));
  } else {
    bullets.push(
      'Current state — not fully captured in discovery notes yet; we will confirm operating baseline at kickoff.'
    );
    refs.push(evidenceRef('uncertainty', 'current_state', 'missing stage/clients/process'));
  }

  if (summary.goals.length) {
    bullets.push('Goals — ' + joinList(summary.goals) + '.');
    refs.push(evidenceRef('discovery', 'goals', joinList(summary.goals)));
  } else {
    bullets.push(
      'Goals — not yet documented; we will lock measurable goals before campaign launch.'
    );
    refs.push(evidenceRef('uncertainty', 'goals', 'missing'));
  }

  if (summary.challenges.length) {
    bullets.push('Challenges — ' + joinList(summary.challenges) + '.');
    refs.push(evidenceRef('discovery', 'challenges', joinList(summary.challenges)));
  } else {
    bullets.push(
      'Challenges — not explicitly listed; we will not invent them. Confirm constraints at kickoff.'
    );
    refs.push(evidenceRef('uncertainty', 'challenges', 'missing'));
  }

  if (summary.growthVision) {
    bullets.push('Growth vision — ' + summary.growthVision + '.');
    refs.push(evidenceRef('discovery', 'growthVision', summary.growthVision));
  } else {
    bullets.push(
      'Growth vision — not yet stated in structured form; strategy stays provisional until you confirm direction.'
    );
    refs.push(evidenceRef('uncertainty', 'growthVision', 'missing'));
  }

  const noteHint = weaveNotesHint(summary);
  let body = "Here's what we heard from " + summary.companyName + '.';
  if (noteHint) {
    body +=
      ' Additional context from the conversation (paraphrased for this proposal, not pasted as notes): ' +
      noteHint;
  }

  if (summary.notes) refs.push(evidenceRef('discovery', 'notes', 'paraphrased'));

  return buildSection({
    id: 'understanding',
    title: 'Understanding Your Business',
    body,
    bullets,
    evidenceRefs: refs,
    uncertain: bullets.some((b) => /not (yet|fully|explicitly)/i.test(b)),
  });
}

function sectionWhyPulseforge(summary, playbook) {
  const goals = summary.goals.length
    ? joinList(summary.goals)
    : 'predictable commercial growth';
  const refs = [evidenceRef('discovery', 'companyName', summary.companyName)];
  if (summary.goals.length) {
    refs.push(evidenceRef('discovery', 'goals', goals));
  } else {
    refs.push(evidenceRef('uncertainty', 'goals', 'using conservative default framing'));
  }

  const bullets = [
    'Predictable acquisition — research and outreach on a cadence, not when someone remembers',
    'Consistent outreach — personalized touches that still scale',
    'Commercial specialization — markets chosen with you, not generic SMB spray',
    'Continuous refinement — every campaign improves the next from evidence',
  ];

  if (playbook && playbook.valuePropositions && playbook.valuePropositions.length) {
    bullets.unshift(
      'Your differentiators in market: ' + joinList(playbook.valuePropositions)
    );
    refs.push(
      evidenceRef(
        'client_playbook',
        'valuePropositions',
        joinList(playbook.valuePropositions)
      )
    );
  }
  if (playbook && playbook.brandVoice) {
    bullets.push(
      'Brand voice locked to ' +
        brandVoiceLabel(playbook.brandVoice) +
        ' across outreach and proposals'
    );
    refs.push(evidenceRef('client_playbook', 'brandVoice', playbook.brandVoice));
  }

  const body =
    'Pulseforge is built to give ' +
    summary.companyName +
    ' a predictable way to acquire commercial work — consistent outreach, commercial specialization, and continuous refinement against real market evidence. We frame the system around what you said matters: ' +
    goals +
    (playbook && playbook.valuePropositions && playbook.valuePropositions.length
      ? ', expressed through your playbook strengths (' +
        joinList(playbook.valuePropositions.slice(0, 2)) +
        ')'
      : '') +
    '. That means less ad-hoc follow-up and more of a repeatable engine you can hire and deliver against.';

  return buildSection({
    id: 'why_pulseforge',
    title: 'Why Pulseforge',
    body,
    bullets,
    evidenceRefs: refs,
    uncertain: !summary.goals.length,
  });
}

function sectionRecommendedStrategy(summary, profile, markets, strategy, playbook) {
  const profileLabel =
    (playbook && playbook.name) ||
    (profile && profile.name) ||
    [summary.industry, summary.geography].filter(Boolean).join(' — ') ||
    null;

  const bullets = [];
  if (markets.markets.length) {
    bullets.push('Initial focus:');
    for (const m of markets.markets) bullets.push(m);
  }

  if (playbook && playbook.offers && playbook.offers.length) {
    bullets.push('Offers from your playbook: ' + joinList(playbook.offers) + '.');
  }

  if (playbook && playbook.preferredChannels && playbook.preferredChannels.length) {
    bullets.push(
      'Preferred channels (ranked): ' + joinList(playbook.preferredChannels) + '.'
    );
  }

  if (playbook && playbook.successMetrics && playbook.successMetrics.length) {
    bullets.push(
      'Success measured by: ' + joinList(playbook.successMetrics) + '.'
    );
  }

  if (strategy && strategy.campaignName) {
    bullets.push(
      'Campaign draft in progress: ' +
        strategy.campaignName +
        (strategy.prospectCount
          ? ' (' + strategy.prospectCount + ' ranked prospects available for first wave)'
          : '')
    );
  }

  if (strategy && strategy.topProspects && strategy.topProspects.length) {
    const names = strategy.topProspects
      .map((p) => p.companyName)
      .filter(Boolean)
      .slice(0, 3);
    if (names.length) {
      bullets.push('Early opportunity examples from ranking: ' + joinList(names) + '.');
    }
  }

  let body;
  if (markets.markets.length && !markets.uncertain) {
    body =
      'During our conversation, you shared that ' +
      summary.companyName +
      ' is intentionally focusing on ' +
      joinList(markets.markets) +
      (summary.growthVision
        ? ' because those clients align with your long-term vision (' +
          summary.growthVision +
          ')'
        : '') +
      '. This proposal is built around that strategy' +
      (profileLabel ? ' using "' + profileLabel + '"' : '') +
      '. ' +
      markets.why;
  } else {
    body =
      markets.why +
      ' For ' +
      summary.companyName +
      ', we will not invent a beachhead market in this document.';
  }

  if (strategy && strategy.narrative) {
    body += ' ' + strategy.narrative;
  }

  const refs = [...markets.refs];
  if (playbook) {
    if (playbook.offers && playbook.offers.length) {
      refs.push(evidenceRef('client_playbook', 'offers', joinList(playbook.offers)));
    }
    if (playbook.successMetrics && playbook.successMetrics.length) {
      refs.push(
        evidenceRef(
          'client_playbook',
          'successMetrics',
          joinList(playbook.successMetrics)
        )
      );
    }
  }

  return buildSection({
    id: 'recommended_strategy',
    title: 'Recommended Strategy',
    body,
    bullets,
    evidenceRefs: refs,
    uncertain: markets.uncertain,
  });
}

function sectionWhatWeHandle(summary, markets, playbook) {
  const bullets = [
    'Prospect research against the approved Discovery Profile',
    'Personalized outreach into the agreed commercial segments',
    'Follow-up cadence so interested replies do not stall',
    'Pipeline visibility for your team',
    'Campaign refinement from evidence each cycle',
  ];
  if (markets.markets.length) {
    bullets[1] =
      'Personalized outreach into ' +
      joinList(markets.markets) +
      (summary.geography ? ' across ' + summary.geography : '');
  }
  if (playbook && playbook.outreachSequence && playbook.outreachSequence.length) {
    const seq = playbook.outreachSequence
      .map((s) => 'Day ' + s.day + ' ' + (s.action || s.channel))
      .join(' → ');
    bullets.push('Outreach sequence from your playbook: ' + seq);
  }
  if (playbook && playbook.offers && playbook.offers.length) {
    bullets.push('Lead offers: ' + joinList(playbook.offers));
  }

  const refs = [
    evidenceRef('discovery', 'companyName', summary.companyName),
  ].concat(
    markets.markets.length
      ? [evidenceRef('strategy', 'markets', joinList(markets.markets))]
      : [evidenceRef('uncertainty', 'markets', 'generic scope until markets lock')]
  );
  if (playbook && playbook.outreachSequence && playbook.outreachSequence.length) {
    refs.push(evidenceRef('client_playbook', 'outreachSequence', 'pinned'));
  }

  return buildSection({
    id: 'what_we_handle',
    title: 'What We Handle',
    body:
      'Pulseforge owns the acquisition system for ' +
      summary.companyName +
      ' so your team can stay on delivery and close.',
    bullets,
    evidenceRefs: refs,
    uncertain: markets.uncertain,
  });
}

function sectionYourRole(summary, playbook) {
  const bullets = [
    'Respond to interested prospects promptly',
    'Attend walkthroughs and site visits',
    'Close work and set commercial terms',
    'Deliver excellent service so referrals compound',
  ];
  if (summary.goals.some((g) => /subcontract|hire|crew|team/i.test(g))) {
    bullets.push(
      'Build delivery capacity (including subcontractors if that remains your plan) as pipeline grows'
    );
  }
  if (playbook && playbook.offers && playbook.offers.some((o) => /walkthrough/i.test(o))) {
    bullets[1] = 'Attend walkthroughs booked from your free-walkthrough offer';
  }

  return buildSection({
    id: 'your_role',
    title: 'Your Role',
    body:
      summary.companyName +
      ' remains the operator of the business. Pulseforge fills the top of funnel; you own relationship, walkthrough, close, and delivery — the work only you can do well.',
    bullets,
    evidenceRefs: [
      evidenceRef('discovery', 'companyName', summary.companyName),
    ].concat(
      summary.goals.length
        ? [evidenceRef('discovery', 'goals', joinList(summary.goals))]
        : []
    ).concat(
      playbook && playbook.offers && playbook.offers.length
        ? [evidenceRef('client_playbook', 'offers', joinList(playbook.offers))]
        : []
    ),
  });
}

function sectionFirst90Days(summary, markets, timelineOverrides, playbook) {
  const month1 =
    (timelineOverrides && timelineOverrides.month1) ||
    'Foundation — lock Discovery Profile' +
      (markets.markets.length ? ' for ' + joinList(markets.markets.slice(0, 3)) : '') +
      (playbook ? ' and confirm Client Playbook “' + playbook.name + '”' : '') +
      ', stand up research and outreach, align response ownership with ' +
      (summary.contactName || summary.companyName) +
      '.';
  const month2 =
    (timelineOverrides && timelineOverrides.month2) ||
    'Optimization — tighten messaging from early replies, drop weak segments, double down on what earns conversations' +
      (summary.challenges.length
        ? ' (explicitly reducing friction around ' +
          joinList(summary.challenges.slice(0, 2)) +
          ')'
        : '') +
      (playbook && playbook.successMetrics && playbook.successMetrics.length
        ? ' against ' + joinList(playbook.successMetrics.slice(0, 2))
        : '') +
      '.';
  const month3 =
    (timelineOverrides && timelineOverrides.month3) ||
    'Scale — expand volume inside proven segments' +
      (summary.growthVision ? ' toward ' + summary.growthVision : '') +
      ', and formalize the weekly operating cadence.';

  return buildSection({
    id: 'first_90_days',
    title: 'First 90 Days',
    body: 'A practical implementation arc for ' + summary.companyName + ':',
    bullets: ['Month 1 — ' + month1, 'Month 2 — ' + month2, 'Month 3 — ' + month3],
    evidenceRefs: [
      evidenceRef('discovery', 'companyName', summary.companyName),
    ].concat(
      markets.markets.length
        ? [evidenceRef('strategy', 'markets', joinList(markets.markets))]
        : [evidenceRef('uncertainty', 'markets', 'timeline uses provisional markets')]
    ).concat(
      playbook
        ? [evidenceRef('client_playbook', 'id', playbook.id + '@' + playbook.version)]
        : []
    ),
    editable: true,
  });
}

function sectionLongTermAdvantage(summary, playbook) {
  const metricHint =
    playbook && playbook.successMetrics && playbook.successMetrics.length
      ? ' Progress is judged by your metrics: ' +
        joinList(playbook.successMetrics) +
        '.'
      : '';
  return buildSection({
    id: 'long_term_advantage',
    title: 'Long-Term Advantage',
    body:
      LONG_TERM_ADVANTAGE_BLOCK +
      ' For ' +
      summary.companyName +
      ', that means the system gets sharper in your geography and ICP instead of resetting every quarter.' +
      metricHint,
    evidenceRefs: [
      evidenceRef('messaging', 'long_term_advantage_block', 'approved'),
      evidenceRef('discovery', 'companyName', summary.companyName),
    ].concat(
      playbook && playbook.successMetrics && playbook.successMetrics.length
        ? [
            evidenceRef(
              'client_playbook',
              'successMetrics',
              joinList(playbook.successMetrics)
            ),
          ]
        : []
    ),
  });
}

function sectionInvestment(summary, pricing) {
  const bullets = [
    'Package: ' + pricing.label,
    'Setup: ' + pricing.setupFee,
    'Monthly / retainer: ' + pricing.monthly,
    "What's included:",
  ];
  for (const x of pricing.included) bullets.push('• ' + x);
  bullets.push('Payment schedule:');
  for (const x of pricing.paymentSchedule) bullets.push('• ' + x);
  if (pricing.optionalTerms.length) {
    bullets.push('Optional terms:');
    for (const t of pricing.optionalTerms) bullets.push('• ' + t);
  }
  if (pricing.operatorNotes) {
    bullets.push('Operator notes: ' + pricing.operatorNotes);
  }

  return buildSection({
    id: 'investment',
    title: 'Investment',
    body:
      'Investment for ' +
      summary.companyName +
      ' uses the ' +
      pricing.label +
      ' package. ' +
      pricing.description +
      ' Final amounts are confirmed by the operator before this proposal is sent.',
    bullets,
    evidenceRefs: [
      evidenceRef('pricing', 'package', pricing.id),
      evidenceRef('discovery', 'companyName', summary.companyName),
    ],
    editable: true,
  });
}

function sectionNextSteps(summary, playbook) {
  const bullets = [
    'Proposal Approval',
    'Kickoff Meeting',
    'Discovery Profile Approval',
    'Client Playbook Confirmation',
    'Campaign Launch',
  ];
  if (playbook && playbook.offers && playbook.offers[0]) {
    bullets.push('First client-facing offer in market: ' + playbook.offers[0]);
  }

  return buildSection({
    id: 'next_steps',
    title: 'Next Steps',
    body:
      'When ' +
      summary.companyName +
      ' is ready to proceed, we move in a short, deliberate sequence — no busywork, no vague “next quarter” plans.',
    bullets,
    evidenceRefs: [evidenceRef('discovery', 'companyName', summary.companyName)].concat(
      playbook
        ? [evidenceRef('client_playbook', 'id', playbook.id + '@' + playbook.version)]
        : []
    ),
    editable: true,
  });
}

module.exports = {
  composeProposal,
  scorePersonalization,
  joinList,
};
