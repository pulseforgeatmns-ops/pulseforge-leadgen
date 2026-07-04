require('dotenv').config();
const axios = require('axios');
const { randomUUID } = require('crypto');
const nodemailer = require('nodemailer');
const db = require('./dbClient');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');
const { recalculateICP } = require('./utils/icpScoring');
const { invalidOutreachEmailReason } = require('./utils/emailGuard');
const { normalizeRootDomain, rootDomainFromEmail } = require('./utils/brevoEvents');
const { AI_TELL_PHRASES } = require('./utils/voiceRules');
const { renderTemplate, withHouseGreetingFallback } = require('./utils/templateMerge');
const { ANCHOR_DRAFT_SEQUENCES } = require('./utils/anchorEmailTemplates');
const { getWarmupProgress, resolveWarmupDailyCap } = require('./utils/sendWarmup');
const { reportAgentRun } = require('./utils/agentObservability');
const {
  evaluateSendingReadiness,
  exactSequenceName,
  getBrevoState,
} = require('./utils/sendingReadiness');

const AGENT_NAME = 'emmett';
let FROM_EMAIL = null;
let FROM_NAME = null;
const CLIENT_ID = getRuntimeClientId();
let CLIENT_CONFIG = null;
let MAIL_TRANSPORTER = null;

function makeRunId() {
  return `${AGENT_NAME}-${CLIENT_ID || 'none'}-${new Date().toISOString()}-${randomUUID()}`;
}

function sendErrorSample(result, prospect = null) {
  if (!result?.error) return null;
  return {
    prospect_id: prospect?.id || null,
    email: prospect?.email || null,
    error: result.error,
  };
}

async function reportEmmettRun({ runId, attempts, successes, skipped = 0, errorSample = null }) {
  try {
    return await reportAgentRun({
      agent: AGENT_NAME,
      clientId: CLIENT_ID,
      runId,
      attempts,
      successes,
      skipped,
      errorSample,
    });
  } catch (err) {
    console.error('[Emmett] Observability report failed:', err.message);
    return null;
  }
}

const clientConfig = {
  1: {
    dailyCap: 100,
    verticalCap: 15,
    warmup: {
      resetAfterDays: 7,
      stages: [
        { afterSendDays: 0, dailyCap: 10 },
        { afterSendDays: 2, dailyCap: 15 },
        { afterSendDays: 4, dailyCap: 25 },
        { afterSendDays: 7, dailyCap: 40 },
        { afterSendDays: 11, dailyCap: 60 },
        { afterSendDays: 16, dailyCap: 80 },
        { afterSendDays: 22, dailyCap: 100 },
      ],
    },
  },
  2: { dailyCap: 40, verticalCap: 10 },
  5: { dailyCap: 30, verticalCap: 8, ramp: { afterDays: 14, bounceCeiling: 0.03, newDailyCap: 50 } },
  10: {
    dailyCap: 50,
    verticalCap: 10,
    warmup: {
      resetAfterDays: 7,
      stages: [
        { afterSendDays: 0, dailyCap: 5 },
        { afterSendDays: 3, dailyCap: 8 },
        { afterSendDays: 6, dailyCap: 12 },
        { afterSendDays: 10, dailyCap: 18 },
        { afterSendDays: 15, dailyCap: 25 },
        { afterSendDays: 21, dailyCap: 35 },
        { afterSendDays: 28, dailyCap: 50 },
      ],
    },
  },
};

function getEmmettClientConfig(clientId = CLIENT_ID) {
  return clientConfig[clientId] || clientConfig[1];
}

function normalizeSendingDomain(value) {
  return normalizeRootDomain(value) || rootDomainFromEmail(value) || 'unknown.local';
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function findAiTellPhrases(text) {
  const value = String(text || '');
  return AI_TELL_PHRASES.filter(phrase => {
    const escaped = escapeRegex(phrase);
    const isSingleWord = /^[a-z']+$/i.test(phrase);
    const pattern = isSingleWord ? `\\b${escaped}\\b` : escaped;
    return new RegExp(pattern, 'i').test(value);
  });
}

async function activeGlobalEmailHalt() {
  const pool = require('./db');
  try {
    const res = await pool.query(`
      SELECT id
      FROM blockers
      WHERE resolved = false
        AND blocking = 'emmett_global_halt'
      ORDER BY created_at DESC
      LIMIT 1
    `);
    return res.rows[0] || null;
  } catch (err) {
    if (err.code !== '42P01') console.error('[Emmett] global halt check failed:', err.message);
    return null;
  }
}

async function writeEmailBlocker(kind, sendingDomain, health) {
  const pool = require('./db');
  const isHalt = kind === 'halted';
  const blocking = isHalt ? 'emmett_global_halt' : `emmett_domain_pause:${sendingDomain}`;
  const content = isHalt
    ? `Emmett halted because ${sendingDomain} reached ${health.bouncePct}% bounces over ${health.sends} sends in 7 days.`
    : `Emmett paused ${sendingDomain} because it reached ${health.bouncePct}% bounces over ${health.sends} sends in 7 days.`;

  try {
    await pool.query(`
      INSERT INTO blockers (client_id, content, blocking)
      SELECT $1, $2, $3
      WHERE NOT EXISTS (
        SELECT 1
        FROM blockers
        WHERE resolved = false
          AND blocking = $3
      )
    `, [isHalt ? null : CLIENT_ID, content, blocking]);
  } catch (err) {
    if (err.code !== '42P01') console.error('[Emmett] blocker write failed:', err.message);
  }
}

async function logDomainHealthAlert(action, health, status = 'skipped') {
  await db.logAgentAction(
    AGENT_NAME,
    action,
    null,
    null,
    {
      client_id: CLIENT_ID,
      sending_domain: health.sendingDomain,
      bounce_pct: health.bouncePct,
      sends: health.sends,
      bounces: health.bounces,
    },
    status
  );
}

async function checkSendingDomainHealth(sendingDomain) {
  const normalizedDomain = normalizeSendingDomain(sendingDomain);
  const activeHalt = await activeGlobalEmailHalt();
  if (activeHalt) {
    return { status: 'halted', bouncePct: 0, sends: 0, bounces: 0, sendingDomain: normalizedDomain };
  }

  const pool = require('./db');
  const res = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE event_type IN ('hard_bounce','blocked')) AS bounces,
      COUNT(*) FILTER (WHERE event_type = 'sent') AS sends,
      ROUND(100.0 * COUNT(*) FILTER (WHERE event_type IN ('hard_bounce','blocked'))
            / NULLIF(COUNT(*) FILTER (WHERE event_type = 'sent'), 0), 2) AS bounce_pct
    FROM email_events
    WHERE event_at >= NOW() - INTERVAL '7 days'
      AND sending_domain = $1
  `, [normalizedDomain]);

  const row = res.rows[0] || {};
  const health = {
    status: 'ok',
    bouncePct: Number(row.bounce_pct || 0),
    sends: Number(row.sends || 0),
    bounces: Number(row.bounces || 0),
    sendingDomain: normalizedDomain,
  };

  if (health.sends < 20) return health;

  if (health.bouncePct >= 4.0) {
    health.status = 'halted';
    await writeEmailBlocker('halted', normalizedDomain, health);
    await logDomainHealthAlert('sending_domain_halted', health, 'failed');
    console.error(`[Emmett] Critical bounce alert for ${normalizedDomain}: ${health.bouncePct}% over ${health.sends} sends`);
    return health;
  }

  if (health.bouncePct >= 2.0) {
    health.status = 'paused';
    await writeEmailBlocker('paused', normalizedDomain, health);
    await logDomainHealthAlert('sending_domain_paused', health, 'skipped');
    console.warn(`[Emmett] Bounce warning for ${normalizedDomain}: ${health.bouncePct}% over ${health.sends} sends`);
  }

  return health;
}

// Email sequence definitions — reply-based CTAs only (no external or Calendly links in bodies)
const SEQUENCES = {
  ...ANCHOR_DRAFT_SEQUENCES,
  mshi_property_management: [
    {
      day: 0,
      subject: "one crew for your turns and repairs",
      body: `Brad here from Mountain State Home Innovations. I know managing units means a steady stream of make-readies and repairs between tenants, and chasing reliable contractors for it is its own headache.

We handle turn work and renovations for property managers in the valley, one point of contact, fast turns to cut your vacancy days.

Want me to send a couple recent before-and-afters? If it looks useful I can be your backup crew for the next turn and earn the rest from there.

Brad Hudson
Mountain State Home Innovations`
    },
    {
      day: 4,
      subject: "Re: one crew for your turns and repairs",
      body: `Hi {{first_name}},

Just following up in case my last note got buried.

One thing that sets us apart — most contractors go quiet after the estimate. We don't. Every client gets direct access to Brad or Dustin throughout the whole project. For property managers who need fast turnaround on damage or wear, that matters.

We're happy to do a free walkthrough of any properties you manage in Kanawha, Putnam, or Cabell County — just reply here and we'll set it up.

Brad
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 8,
      subject: "What we've done for other property managers in WV",
      body: `Hi {{first_name}},

We've done subcontract work with some of the larger WV firms — Tri-State Exterior Solutions, St Albans Windows, Secure Construction — so we know what quality at scale looks like.

Decks and siding are our highest volume work. If {{business_name}} has properties that need attention, we'd love to put together a free estimate.

You can also see our Google reviews here: https://share.google/KeVYcU4QxVwfur0cN

Brad
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 13,
      subject: "Closing the loop",
      body: `Hi {{first_name}},

I don't want to keep filling up your inbox so I'll leave it here.

If the timing ever works out, give us a call — Brad or Dustin will pick up.

304-483-3655

No obligation, free estimate, local crew. We'll be here.

Brad
Mountain State Home Innovations`
    }
  ],
  mshi_probate_attorney: [
    {
      day: 0,
      subject: "when estate repairs delay the case",
      body: `Hi {{first_name}},

Unresolved water damage or deferred maintenance can add weeks before an estate property is ready to list or transfer. Mountain State Home Innovations handles documented repairs in WV, keeps out-of-state heirs updated, and gives the attorney one point of contact.

Worth a brief call this week to see if we'd fit any properties currently in your case load? Or if an estate property needs contractor coordination now, I can review the details and send back a one-page assessment either way.

Brad Hudson
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 4,
      subject: "Re: when estate repairs delay the case",
      body: `Hi {{first_name}},

When heirs live out of state, contractor coordination often lands with the attorney even when it does not fit neatly into billable casework. Mountain State Home Innovations can absorb the heir communication, repair scheduling, and written project documentation through one WV point of contact.

Worth a brief conversation about any current cases? Or send me details on a case where the property is holding things up and I'll send back what we would handle.

Brad Hudson
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 8,
      subject: "how are estate repairs handled now?",
      body: `Hi {{first_name}},

When a probate case stalls because the property is not sale-ready, are you coordinating repairs directly or routing that work to the heirs? In either case, Mountain State Home Innovations can manage the WV repair scope, document progress, and keep everyone informed without adding another project to your case load.

I can sketch what handing that off would look like for one of your current cases.

Brad Hudson
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 13,
      subject: "before the next probate filing rush",
      body: `Hi {{first_name}},

Unresolved estate repairs will keep stretching probate timelines until there is a reliable contractor handoff. Okay to circle back at the end of the quarter, before the year-end probate filing rush?

Brad Hudson
Mountain State Home Innovations
304-483-3655`
    }
  ],
  mshi_investor_flipper: [
    {
      day: 0,
      subject: "protecting margin on the next rehab",
      body: `Hi {{first_name}},

Each week a rehab runs long adds interest, taxes, insurance, and utilities directly against the deal's margin. Mountain State Home Innovations scopes WV rehabs for realistic costs and timelines, then provides one point of contact across the project.

Worth a quick call this week to talk through your next acquisition? Or send me the address of a property you're working through and I'll send back our rehab estimate, free and useful as a second opinion either way.

Brad Hudson
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 4,
      subject: "Re: protecting margin on the next rehab",
      body: `Hi {{first_name}},

When a rehab budget jumps after demolition, the original scope usually missed conditions behind the walls or local code requirements. Mountain State Home Innovations builds those WV details into the estimate so the projected number has a better chance of holding through the work.

Worth a brief conversation about an upcoming project? Or send me a quote you're sitting on and I'll send back what we would say about the scope.

Brad Hudson
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 8,
      subject: "who is carrying the GC role?",
      body: `Hi {{first_name}},

Are you GC-ing your flips yourself, or do you have a contractor partner handling the full rehab scope? Whether the priority is acquisition cadence, a shorter holding period, or a clean exit, Mountain State Home Innovations can take ownership of the WV scope through one point of contact.

I can map out what bringing one of your properties through our scope would look like.

Brad Hudson
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 13,
      subject: "before the next acquisition closes",
      body: `Hi {{first_name}},

Carrying costs on the next deal will keep compounding until the contractor relationship is locked in. Okay to circle back before your next acquisition closes?

Brad Hudson
Mountain State Home Innovations
304-483-3655`
    }
  ],
  mshi: [
    {
      day: 0,
      subject: "Quick question about {{business_name}}",
      body: `Hi {{first_name}},

My name is Brad — I run Mountain State Home Innovations out of Charleston with my partner Dustin.

We specialize in decks, siding, and exterior work across Kanawha, Putnam, and Cabell County. Licensed WV065578.

I came across {{business_name}} and wanted to reach out directly. We do a lot of work for property managers and HOAs who need reliable contractors they can call without the runaround.

Would it be worth a quick conversation?

Brad
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 4,
      subject: "Re: Quick question about {{business_name}}",
      body: `Hi {{first_name}},

Just following up in case my last note got buried.

One thing that sets us apart — most contractors go quiet after the estimate. We don't. Every client gets direct access to Brad or Dustin throughout the whole project. For property managers who need fast turnaround on damage or wear, that matters.

We're happy to do a free walkthrough of any properties you manage in Kanawha, Putnam, or Cabell County — just reply here and we'll set it up.

Brad
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 8,
      subject: "What we've done for other property managers in WV",
      body: `Hi {{first_name}},

We've done subcontract work with some of the larger WV firms — Tri-State Exterior Solutions, St Albans Windows, Secure Construction — so we know what quality at scale looks like.

Decks and siding are our highest volume work. If {{business_name}} has properties that need attention, we'd love to put together a free estimate.

You can also see our Google reviews here: https://share.google/KeVYcU4QxVwfur0cN

Brad
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 13,
      subject: "Closing the loop",
      body: `Hi {{first_name}},

I don't want to keep filling up your inbox so I'll leave it here.

If the timing ever works out, give us a call — Brad or Dustin will pick up.

304-483-3655

No obligation, free estimate, local crew. We'll be here.

Brad
Mountain State Home Innovations`
    }
  ],
  home_renovation: [
    {
      day: 0,
      subject: "Quick question about {{business_name}}",
      body: `Hi {{first_name|}},

We handle exterior and interior renovations for property managers, HOAs, landlords, and banks across the Charleston area.

The thing our clients usually notice first is communication. We show up, keep you updated at every step, and Brad or Dustin are the ones doing the work instead of handing it off to a crew you have never met.

We offer free estimates and are licensed in WV under WV065578.

Are you planning any exterior or interior work on your properties this year?

Brad & Dustin
Mountain State Home Innovations`
    },
    {
      day: 4,
      subject: "Re: Quick question about {{business_name}}",
      body: `Hi {{first_name|}},

Most contractors go quiet after the estimate. We do not.

Every client gets direct access to Brad or Dustin throughout the project, which matters when you are managing properties, board expectations, repairs, weather delays, or emergency damage.

If you want to see a free mockup of what that could look like for {{business_name}}, just reply here and I'll have something over to you same day.

Brad & Dustin`
    },
    {
      day: 8,
      subject: "What we've done for other property teams",
      body: `Hi {{first_name|}},

Before Mountain State Home Innovations, we subcontracted for larger WV firms including Tri-State Exterior Solutions, St Albans Windows, and Secure Construction. That gave us a lot of experience doing quality work at scale while still caring about the details.

Decks and siding are two of our highest priority services right now, along with windows, interior renovations, and repair work when something needs attention quickly.

Are you currently working with a contractor you trust for this kind of work, or is it more as-needed when something comes up?

Brad & Dustin`
    },
    {
      day: 13,
      subject: "Closing the loop",
      body: `Hi {{first_name|}},

Last note from us. We know you are busy, so we will keep it simple.

Our number is 304-483-3655. Brad or Dustin will pick up.

If the timing ever works out, just reply to this — I'll put something together for {{business_name}} same day. No forms, no pressure.

Brad & Dustin
Mountain State Home Innovations`
    }
  ],
  cleaning: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name|}},

A cleaning quote that waits until the end of the workday is usually calling the next company by then.

When the owner is on a job, fast follow-up is hard to do consistently. Are new inquiries at {{business_name}} getting a reply within a few minutes, or only when someone gets free?

Jacob Maynard
Pulseforge
(603) 293-5816`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name|}},

Missed calls and web forms cool off fastest while the crew is still on-site.

A two-minute text and automatic follow-up can keep those quotes alive without pulling anyone off a job.

Want me to mock up that workflow for {{business_name}}?

Jacob`
    },
    {
      day: 8,
      subject: "quick question about {{business_name}}",
      body: `Hi {{first_name|}},

Are you currently following up with new cleaning leads automatically, or is it still mostly manual when someone gets a chance?

If manual follow-up is the bottleneck, I can sketch the two-minute response flow for {{business_name}}.

Jacob`
    },
    {
      day: 13,
      subject: "before the next cleaning rush",
      body: `Hi {{first_name|}},

Unanswered cleaning quotes at {{business_name}} will keep cooling off while the crew is busy.

Okay to circle back at the start of next quarter, before spring deep-clean demand picks up?

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],
  restaurant: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name|}},

A guest who visits {{business_name}} once and never hears from you again has little reason to choose you over the next new spot.

That gap leaves slower nights dependent on walk-ins and social posts. Are you doing anything now to bring first-time guests back for a second visit?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name|}},

A slow Tuesday stays slow when last month's guests are not given a reason to return.

Automated reminders and local outreach can fill that gap between rushes without adding another task for the floor team.

Want a quick mockup built around {{business_name}}?

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working locally right now",
      body: `Hi {{first_name|}},

Are you currently bringing past guests back between visits, or is {{business_name}} still relying mostly on word of mouth and social posts?

If repeat visits are the gap, I can map out a simple return-guest campaign.

Jacob`
    },
    {
      day: 13,
      subject: "before the next busy season",
      body: `Hi {{first_name|}},

Uneven weekday covers at {{business_name}} will stay dependent on chance until past guests have a reason to return.

Okay to circle back before holiday planning starts?

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],
  salon: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name|}},

Every client who leaves {{business_name}} without a future appointment is a chair you have to refill later.

That turns retention into a constant acquisition job. Are most clients rebooking before they leave, or does the calendar depend on reminders and referrals?

Jacob Maynard
Pulseforge
(603) 293-5816`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name|}},

Gaps in next month's calendar start with clients who leave this month without rebooking.

Automatic reminders can bring lapsed clients back and respond to new Google leads while the team stays with clients.

Want me to sketch that flow for {{business_name}}?

Jacob`
    },
    {
      day: 8,
      subject: "quick question about {{business_name}}",
      body: `Hi {{first_name|}},

Are you currently bringing in new clients outside referrals and social media, or are those still the main sources?

If that mix feels narrow, I can map out another acquisition path for {{business_name}}.

Jacob`
    },
    {
      day: 13,
      subject: "before peak booking season",
      body: `Hi {{first_name|}},

A client overdue for a cut, color, or treatment is a booking opportunity sitting untouched in {{business_name}}'s list.

Okay to circle back a month before prom and wedding season?

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  fitness: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name|}},

Every trial guest who leaves {{business_name}} without a same-day response is more likely to join the studio that texts first.

That makes membership growth depend on front-desk bandwidth between classes. Are trial leads getting an immediate reply now?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name|}},

Empty spots in next week's classes often trace back to trial leads that never got a second touch.

A short automated sequence can answer, remind, and invite each lead without adding front-desk work. Want a mockup for {{business_name}}?

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for fitness studios in Southern NH",
      body: `Hi {{first_name|}},

Are trial leads at {{business_name}} getting a same-day text and a second invitation, or is follow-up still handled between classes?

If the process is manual, I can sketch a simple trial-to-member flow.

Jacob`
    },
    {
      day: 13,
      subject: "before the January rush",
      body: `Hi {{first_name|}},

Unconverted trial leads leave empty spots in classes that could be full at {{business_name}}.

Okay to circle back in November, before January resolution demand begins?

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  property: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name|}},

Each extra day a unit sits vacant costs rent while its listing competes beside dozens of similar units.

Fast inquiry response matters most when the team is already handling tenants and owners. Are prospects at {{business_name}} getting an immediate reply?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name|}},

Leasing inquiries that wait until tenant issues settle are often touring another unit first.

An automated first response can protect the lead until someone is free. Want a mockup for {{business_name}}?

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for property managers in Southern NH",
      body: `Hi {{first_name|}},

Are leasing inquiries at {{business_name}} getting an immediate response and scheduled follow-up, or is the process still manual between tenant issues?

If manual response is stretching vacancy time, I can map out a lighter workflow.

Jacob`
    },
    {
      day: 13,
      subject: "before the next turn season",
      body: `Hi {{first_name|}},

Unanswered leasing inquiries at {{business_name}} turn into vacancy days while prospects tour other units.

Okay to circle back 60 days before your next turn season?

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  landscaping: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name|}},

When spring quote requests pile up faster than the crew can answer them, summer maintenance slots go to the company that replies first.

Is {{business_name}} responding while the crew is on-site, or are estimates waiting until the workday ends?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name|}},

A landscaping estimate that waits until the crew gets off-site is often booked elsewhere before dinner.

An immediate text can hold the conversation until someone can quote the work. Want a mockup for {{business_name}}?

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for landscapers in Southern NH",
      body: `Hi {{first_name|}},

Are new quote requests at {{business_name}} getting a same-day response and a second touch, or is follow-up still handled after the crew gets back?

If the process is manual, I can sketch a simple estimate follow-up flow.

Jacob`
    },
    {
      day: 13,
      subject: "before spring quote season",
      body: `Hi {{first_name|}},

Estimates that never get a second touch leave summer work sitting in {{business_name}}'s lead list.

Okay to circle back in late winter, before spring quote season starts?

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  home_services: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name|}},

A homeowner who calls {{business_name}} during a job and reaches voicemail usually calls the next contractor before the crew is free.

Are missed calls getting an immediate text now, or does follow-up wait until the workday ends?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name|}},

The highest-intent home service leads often arrive while every tech is busy.

An immediate text can keep a missed caller from booking the next contractor. Want a mockup for {{business_name}}?

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for home services in Southern NH",
      body: `Hi {{first_name|}},

Are missed calls and web forms at {{business_name}} getting an immediate response, or is someone calling back after the crew finishes the job?

If callbacks are delayed, I can map out a simple lead-capture flow.

Jacob`
    },
    {
      day: 13,
      subject: "before the next project cycle",
      body: `Hi {{first_name|}},

Missed calls that never get a second touch are jobs another contractor can win from {{business_name}}.

Okay to circle back at the start of next quarter, after the current project cycle clears?

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  auto: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name|}},

Every first-time customer who leaves {{business_name}} without a service reminder is one oil change away from becoming another shop's regular.

Are reminders going out before the next service interval, or does repeat business depend on the customer remembering?

Jacob Maynard
Pulseforge
(603) 293-5816`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name|}},

An estimate that leaves without follow-up usually means the customer found a price comparison somewhere else.

Automatic reminders can bring those customers back without adding calls to the service desk.

Want me to mock up that flow for {{business_name}}?

Jacob`
    },
    {
      day: 8,
      subject: "quick question about {{business_name}}",
      body: `Hi {{first_name|}},

Are you currently following up with customers after a job, or is repeat service still mostly left to memory and referrals?

I can sketch a service-reminder flow for {{business_name}} either way.

Jacob`
    },
    {
      day: 13,
      subject: "{{business_name}} — keeping it brief",
      body: `Hi {{first_name|}},

Customers due for service who have not heard from {{business_name}} are revenue sitting in the repair history.

Okay to circle back before winter service prep begins?

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  med_spa: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name|}},

I work with local med spas and aesthetic practices in Southern NH on one specific problem: consistent new client acquisition without depending entirely on word of mouth and Instagram.

Most owners I talk to have strong retention with existing clients. The challenge is staying visible to people who are actively searching for aesthetic services but have not found {{business_name}} yet.

I built a system that handles outreach automatically. It finds and reaches out to local prospects on your behalf, keeps your name visible to people searching in your area, and runs in the background while you focus on clients.

Is bringing in new clients more consistently something you are actively working on?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name|}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

The practices I work with are not struggling. They have skilled providers and clients who love them. The gap is always the same: not enough new people finding them consistently outside of referrals.

If you want to see a free mockup of what that could look like for {{business_name}}, just reply here and I'll have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for med spas in Southern NH",
      body: `Hi {{first_name|}},

One thing I am seeing across local aesthetic practices right now. The ones growing consistently are not just posting more on Instagram. They are staying in front of local prospects who are actively looking but have not booked yet.

Most practices rely on existing clients to spread the word. The ones with waitlists do more than that.

I help practices like {{business_name}} stay visible to new clients automatically. No extra time required on your end.

Are you currently doing anything to reach new clients consistently outside of social media and referrals?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name|}},

Last note from me. I do not want to clutter your inbox.

If the timing ever works out, just reply to this — I'll put something together for {{business_name}} same day. No forms, no pressure.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  re_engagement: [
    {
      day: 0,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name|}},

New inquiries that wait hours for a reply are usually contacting another local business before anyone calls back.

Is that response gap still showing up at {{business_name}}, or has the follow-up process changed?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "one thing I'm seeing locally right now",
      body: `Hi {{first_name|}},

Forms, calls, and DMs become expensive leaks when they depend on someone remembering to respond between jobs.

A lightweight response flow can keep those inquiries alive. Want me to sketch one for {{business_name}}?

Jacob`
    },
    {
      day: 8,
      subject: "another look at {{business_name}} in 90 days",
      body: `Hi {{first_name|}},

Unanswered inquiries at {{business_name}} will keep getting colder until the response process changes.

Okay to circle back in 90 days to see whether follow-up is still a bottleneck?

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ]
};

// Pulseforge and Anchor both use the house greeting: a named contact gets
// "Hi Avery," while a business inbox with no contact name gets "Hi,".
// Keep this list explicit so MSHI's separately approved copy is unchanged.
const HOUSE_GREETING_SEQUENCE_NAMES = [
  'anchor_law_firm_draft',
  'anchor_accounting_draft',
  'home_renovation',
  'cleaning',
  'restaurant',
  'salon',
  'fitness',
  'property',
  'landscaping',
  'home_services',
  'auto',
  'med_spa',
  're_engagement',
];
for (const sequenceName of HOUSE_GREETING_SEQUENCE_NAMES) {
  if (SEQUENCES[sequenceName]) {
    SEQUENCES[sequenceName] = withHouseGreetingFallback(SEQUENCES[sequenceName]);
  }
}

// Fallback step used when a clicked/warm prospect is selected. Defaults to the
// cleaning Day 4 step until a dedicated warm sequence is defined.
const WARM_STEP = SEQUENCES.cleaning.find(step => step.day === 4);

function fillTemplate(template, prospect) {
  return renderTemplate(template, prospect, prospect.company_fields);
}

function getDefaultSmtpConfig() {
  return {
    host: process.env.BREVO_SMTP_HOST || process.env.SMTP_HOST || 'smtp-relay.brevo.com',
    port: Number(process.env.BREVO_SMTP_PORT || process.env.SMTP_PORT || 587),
    user: process.env.BREVO_SMTP_USER || process.env.BREVO_SMTP_LOGIN || process.env.SMTP_USER,
    pass: process.env.BREVO_SMTP_PASS || process.env.BREVO_SMTP_PASSWORD || process.env.SMTP_PASS || process.env.SMTP_PASSWORD || process.env.BREVO_API_KEY,
  };
}

function getClientSmtpConfig() {
  if (CLIENT_CONFIG?.smtp_host && CLIENT_CONFIG?.smtp_user && CLIENT_CONFIG?.smtp_pass) {
    return {
      host: CLIENT_CONFIG.smtp_host,
      port: Number(CLIENT_CONFIG.smtp_port || 587),
      user: CLIENT_CONFIG.smtp_user,
      pass: CLIENT_CONFIG.smtp_pass,
    };
  }
  return getDefaultSmtpConfig();
}

function createMailTransporter() {
  const smtpConfig = getClientSmtpConfig();
  if (!smtpConfig.host || !smtpConfig.user || !smtpConfig.pass) {
    console.warn('SMTP credentials missing; continuing with Brevo API send path');
    return null;
  }
  return nodemailer.createTransport({
    host: smtpConfig.host,
    port: smtpConfig.port,
    secure: smtpConfig.port === 465,
    auth: {
      user: smtpConfig.user,
      pass: smtpConfig.pass,
    },
  });
}

async function sendEmail(toEmail, toName, subject, body, tags) {
  try {
    if (!process.env.BREVO_API_KEY) {
      return { success: false, error: 'BREVO_API_KEY not set' };
    }

    const payload = {
      sender: { name: FROM_NAME, email: FROM_EMAIL },
      to: [{ email: toEmail, name: toName }],
      subject,
      htmlContent: '<html><body style="font-family:Georgia,serif;font-size:16px;line-height:1.6;color:#1a1a1a;max-width:560px;margin:0 auto;padding:20px;">' + body.replace(/\n/g, '<br>') + '</body></html>',
      textContent: body,
    };
    if (Array.isArray(tags) && tags.length) {
      payload.tags = tags.map(String);
    }

    const res = await axios.post('https://api.brevo.com/v3/smtp/email', payload, {
      headers: { 'api-key': process.env.BREVO_API_KEY, 'Content-Type': 'application/json' },
      timeout: 15000,
    });

    const messageId =
      res.data?.messageId ||
      res.data?.messageID ||
      res.data?.message_id ||
      res.data?.messageIds?.[0] ||
      res.headers?.['message-id'] ||
      res.headers?.['x-message-id'] ||
      null;
    console.log(`Email sent to ${toEmail} — Message ID: ${messageId || 'not returned'}`);
    return { success: true, messageId, brevoResponse: res.data || null };
  } catch (err) {
    const errorDetail = err.response?.data || err.message;
    console.error(`Failed to send to ${toEmail}:`, errorDetail);
    return {
      success: false,
      error: typeof errorDetail === 'string' ? errorDetail : JSON.stringify(errorDetail),
    };
  }
}

function stripForbiddenMshiCopy(body, prospect) {
  if (Number(prospect.client_id) !== 2) return body;

  const forbidden = [
    /\bPulseforge\b/gi,
    /\bAI\b/gi,
    /\bautomation\b/gi,
    /\bmarketing agency\b/gi,
  ];
  let cleaned = body;
  let stripped = false;

  for (const pattern of forbidden) {
    pattern.lastIndex = 0;
    if (pattern.test(cleaned)) {
      stripped = true;
      pattern.lastIndex = 0;
      cleaned = cleaned.replace(pattern, '').replace(/[ \t]{2,}/g, ' ');
    }
  }

  if (stripped) {
    console.warn(`Warning: stripped forbidden MSHI copy before sending to ${prospect.email}`);
  }

  return cleaned;
}

async function getEmailsSentToday() {
  const pool = require('./db');
  const res = await pool.query(`
    SELECT COUNT(*)::int AS count
    FROM agent_log
    WHERE action = 'email_sent'
      AND client_id = $1
      AND DATE(ran_at) = CURRENT_DATE
  `, [CLIENT_ID]);
  return res.rows[0]?.count || 0;
}

async function getEffectiveSendConfig(baseConfig) {
  const pool = require('./db');
  if (baseConfig.warmup) {
    const progress = await getWarmupProgress(
      pool,
      CLIENT_ID,
      baseConfig.warmup.resetAfterDays
    );
    const warmupCap = resolveWarmupDailyCap(
      baseConfig.warmup.stages,
      progress.activeSendDays
    );
    const dailyCap = Math.min(baseConfig.dailyCap, warmupCap || baseConfig.dailyCap);
    return {
      ...baseConfig,
      dailyCap,
      ramped: dailyCap < baseConfig.dailyCap,
      warmupProgress: progress,
    };
  }

  if (!baseConfig.ramp || CLIENT_ID !== 5) return { ...baseConfig, ramped: false };

  const stats = await pool.query(`
    SELECT
      MIN(ran_at) AS first_sent_at,
      COUNT(*)::int AS total_sent
    FROM agent_log
    WHERE action = 'email_sent'
      AND client_id = $1
  `, [CLIENT_ID]);
  const firstSentAt = stats.rows[0]?.first_sent_at;
  const totalSent = Number(stats.rows[0]?.total_sent || 0);
  if (!firstSentAt || totalSent === 0) return { ...baseConfig, ramped: false };

  const bounceStats = await pool.query(`
    SELECT COUNT(*)::int AS bounced
    FROM touchpoints
    WHERE client_id = $1
      AND channel = 'email'
      AND action_type IN ('email_bounced', 'email_soft_bounce')
  `, [CLIENT_ID]);

  const bounced = Number(bounceStats.rows[0]?.bounced || 0);
  const bounceRate = totalSent ? bounced / totalSent : 0;
  const daysSinceFirstSend = (Date.now() - new Date(firstSentAt).getTime()) / (1000 * 60 * 60 * 24);
  const shouldRamp =
    daysSinceFirstSend >= baseConfig.ramp.afterDays &&
    bounceRate < baseConfig.ramp.bounceCeiling;

  if (!shouldRamp) return { ...baseConfig, ramped: false };

  const existingRampLog = await pool.query(`
    SELECT 1
    FROM agent_log
    WHERE agent_name = $1
      AND action = 'cap_ramped'
      AND client_id = $2
    LIMIT 1
  `, [AGENT_NAME, CLIENT_ID]);

  if (!existingRampLog.rows.length) {
    await db.logAgentAction(
      AGENT_NAME,
      'cap_ramped',
      null,
      null,
      {
        client_id: CLIENT_ID,
        previous_daily_cap: baseConfig.dailyCap,
        new_daily_cap: baseConfig.ramp.newDailyCap,
        days_since_first_send: Number(daysSinceFirstSend.toFixed(1)),
        bounce_rate: Number(bounceRate.toFixed(4)),
        bounced,
        total_sent: totalSent,
      },
      'success'
    );
  }

  return { ...baseConfig, dailyCap: baseConfig.ramp.newDailyCap, ramped: true };
}

function sendingWindowEndHour(clientId = CLIENT_ID) {
  return 16.5;
}

function sendingWindowLabel(clientId = CLIENT_ID) {
  return 'Monday-Friday 9am-4:30pm ET';
}

function isWithinSendingWindow(date = new Date(), clientId = CLIENT_ID) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    weekday: 'short',
    hour: 'numeric',
    minute: 'numeric',
    hour12: false,
  }).formatToParts(date);
  const weekday = parts.find(p => p.type === 'weekday')?.value;
  const hour = Number(parts.find(p => p.type === 'hour')?.value);
  const minute = Number(parts.find(p => p.type === 'minute')?.value);
  const time = hour + (minute / 60);
  return ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(weekday) && time >= 9 && time < sendingWindowEndHour(clientId);
}

async function logSkippedOutsideWindow() {
  await db.logAgentAction(
    AGENT_NAME,
    'skipped_outside_window',
    null,
    null,
    {
      client_id: CLIENT_ID,
      window: sendingWindowLabel(CLIENT_ID),
      checked_at: new Date().toISOString(),
    },
    'success'
  );
}

async function createEmailSendLog(prospect, payload) {
  const pool = require('./db');
  const res = await pool.query(`
    INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
    VALUES ($1, 'email_pending', $2, $3, 'pending', NOW(), $4)
    RETURNING id
  `, [AGENT_NAME, prospect.id, JSON.stringify(payload), CLIENT_ID]);
  return res.rows[0].id;
}

async function completeEmailSendLog(logId, action, payload, status = 'completed') {
  const pool = require('./db');
  await pool.query(`
    UPDATE agent_log
    SET action = $1,
        status = $2,
        payload = $3
    WHERE id = $4
      AND client_id = $5
  `, [action, status, JSON.stringify(payload), logId, CLIENT_ID]);
}

async function logSendingReadinessBlocked(readiness, stage) {
  const failureCodes = readiness.failures.map(failure => failure.code);
  console.error(
    `[Emmett] SEND BLOCKED at ${stage} for prospect ${readiness.prospect_id}: ${failureCodes.join(', ')}`
  );
  await db.logAgentAction(
    AGENT_NAME,
    'sending_readiness_blocked',
    readiness.prospect_id,
    null,
    { ...readiness, stage },
    'failed',
    readiness.failures.map(failure => failure.message).join(' | ')
  );
}

async function getProspectsForEmail(options = {}) {
  const pool = require('./db');
  const targetVertical = options.targetVertical || options.target_vertical || null;
  const firstTouchOnly = options.firstTouchOnly || options.first_touch_only || false;
  const excludeActionTypes = Array.isArray(options.excludeActionTypes)
    ? options.excludeActionTypes.filter(Boolean).map(String)
    : [];
  const targetEmails = Array.isArray(options.targetEmails)
    ? options.targetEmails.filter(Boolean).map(email => String(email).toLowerCase())
    : [];

  const res = await pool.query(`
    SELECT
      p.*,
      c.name as company,
      row_to_json(c) AS company_fields,
      COALESCE(email_stats.outbound_email_count, 0)::int AS outbound_email_count,
      email_stats.last_touchpoint_at
    FROM prospects p
    LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) FILTER (WHERE t.channel = 'email' AND t.action_type = 'outbound')::int AS outbound_email_count,
        COUNT(*) FILTER (WHERE t.channel = 'email')::int AS email_touchpoint_count,
        MAX(t.created_at) AS last_touchpoint_at
      FROM touchpoints t
      WHERE t.prospect_id = p.id AND t.client_id = p.client_id
    ) email_stats ON true
    WHERE (p.status IN ('cold', 'contacted') OR (
      p.status = 'warm'
      AND COALESCE(email_stats.outbound_email_count, 0) > 0
      AND email_stats.last_touchpoint_at <= NOW() - INTERVAL '14 days'
    ))
    AND p.client_id = $1
    AND p.email IS NOT NULL
    AND p.email != ''
    AND p.email NOT LIKE '%@domain.com'
    AND p.email NOT LIKE '%@example.com'
    AND p.do_not_contact IS NOT TRUE
    AND (
      p.email_status IN ('valid', 'verified', 'role')
      OR (p.email_status = 'unverified_legacy' AND p.status = 'contacted')
    )
    AND ($2::text IS NULL OR LOWER(COALESCE(p.vertical, '')) = LOWER($2::text))
    AND (
      cardinality($7::text[]) = 0
      OR LOWER(p.email) = ANY($7::text[])
    )
    AND (
      cardinality($6::text[]) = 0
      OR NOT EXISTS (
        SELECT 1
        FROM touchpoints tx
        WHERE tx.prospect_id = p.id
          AND tx.client_id = p.client_id
          AND tx.action_type = ANY($6::text[])
      )
    )
    AND (
      $3::boolean IS NOT TRUE
      OR (
        p.status = 'cold'
        AND NOT EXISTS (
          SELECT 1
          FROM agent_log al
          WHERE al.agent_name = $4
            AND al.action = 'email_sent'
            AND al.prospect_id = p.id
            AND al.client_id = p.client_id
            AND COALESCE(al.payload->>'from_email', '') = $5
        )
      )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM touchpoints tb
      JOIN prospects pb ON pb.id = tb.prospect_id AND pb.client_id = tb.client_id
      WHERE LOWER(pb.email) = LOWER(p.email)
        AND tb.channel = 'email'
        AND tb.action_type = 'email_bounced'
    )
    AND (
      p.status = 'warm'
      OR COALESCE(email_stats.email_touchpoint_count, 0) < 4
    )
    ORDER BY
      CASE WHEN p.status = 'warm' THEN 0 ELSE 1 END ASC,
      -- icp_score carries the dynamic engagement bonus (opens, clicks, replies,
      -- calls), so highest-scoring prospects get contacted first within each tier.
      p.icp_score DESC NULLS LAST,
      p.last_contacted_at ASC NULLS FIRST
  `, [CLIENT_ID, targetVertical, firstTouchOnly, AGENT_NAME, FROM_EMAIL, excludeActionTypes, targetEmails]);

  return res.rows;
}

// Recompute ICP for prospects that have accumulated 5+ touches in the last 30
// days without ever replying. recalculateICP applies the -10 no-response
// penalty, demoting stalled prospects so engaged ones sort ahead of them on
// the next run. Best-effort — never blocks the send loop.
async function recalcStalledNoResponseProspects() {
  const pool = require('./db');
  try {
    const res = await pool.query(`
      SELECT p.id
      FROM prospects p
      WHERE p.client_id = $1
        AND COALESCE(p.do_not_contact, false) = false
        AND (
          SELECT COUNT(*) FROM touchpoints t
          WHERE t.prospect_id = p.id
            AND t.client_id = p.client_id
            AND t.created_at > NOW() - INTERVAL '30 days'
        ) >= 5
        AND NOT EXISTS (
          SELECT 1 FROM touchpoints t
          WHERE t.prospect_id = p.id
            AND t.client_id = p.client_id
            AND t.action_type IN ('inbound_reply', 'inbound', 'reply', 'email_reply')
        )
    `, [CLIENT_ID]);

    for (const row of res.rows) {
      await recalculateICP(row.id, { clientId: CLIENT_ID, reason: 'emmett:5_touch_no_response_30d' })
        .catch(err => console.error(`[Emmett] recalculateICP failed for ${row.id}: ${err.message}`));
    }
    if (res.rows.length) {
      console.log(`[Emmett] Recalculated ICP for ${res.rows.length} stalled (5+ touch, no-response) prospect(s)`);
    }
    return res.rows.length;
  } catch (err) {
    console.error('[Emmett] recalcStalledNoResponseProspects error:', err.message);
    return 0;
  }
}

function getSequenceForProspect(prospect) {
  const exactSequence = exactSequenceName(CLIENT_CONFIG || { id: CLIENT_ID }, prospect, SEQUENCES);
  if (!exactSequence) return null;

  const lastTouchpointAt = prospect.last_touchpoint_at ? new Date(prospect.last_touchpoint_at) : null;
  const daysSinceLastTouchpoint = lastTouchpointAt
    ? (Date.now() - lastTouchpointAt.getTime()) / (1000 * 60 * 60 * 24)
    : null;
  if (
    prospect.status === 'warm' &&
    Number(prospect.outbound_email_count || 0) > 0 &&
    daysSinceLastTouchpoint !== null &&
    daysSinceLastTouchpoint >= 14
  ) {
    return 're_engagement';
  }
  return exactSequence;
}

async function getNextSequenceStep(prospect) {
  const pool = require('./db');
  const sequenceName = getSequenceForProspect(prospect);
  const isReEngagement = sequenceName === 're_engagement';
  const isMshiSequence = [
    'mshi',
    'mshi_property_management',
    'mshi_probate_attorney',
    'mshi_investor_flipper',
  ].includes(sequenceName);

  const res = isMshiSequence
    ? await pool.query(`
        SELECT ran_at AS created_at
        FROM agent_log
        WHERE agent_name = $1
          AND action = 'email_sent'
          AND prospect_id = $2
          AND client_id = $3
          AND payload->>'sequence' = $4
          AND COALESCE(payload->>'from_email', '') = $5
        ORDER BY ran_at ASC
      `, [AGENT_NAME, prospect.id, CLIENT_ID, sequenceName, FROM_EMAIL])
    : await pool.query(`
        SELECT * FROM touchpoints
        WHERE prospect_id = $1
        AND client_id = $2
        AND channel = 'email'
        AND ${
          isReEngagement
            ? "action_type = 'outbound' AND COALESCE(outcome, '') LIKE '%re_engagement%'"
            : "action_type IN ('outbound', 'email_warm')"
        }
        ORDER BY created_at ASC
      `, [prospect.id, CLIENT_ID]);

  const emailsSent = res.rows.length;
  const sequence = SEQUENCES[sequenceName];
  if (!sequence) return null;

  if (emailsSent >= sequence.length) {
    console.log('Sequence complete');
    return null;
  }

  const nextStep = sequence[emailsSent];
  console.log('nextStep:', nextStep?.day);

  if (emailsSent > 0) {
    const lastEmail = res.rows[res.rows.length - 1];
    const daysSinceLast = (Date.now() - new Date(lastEmail.created_at)) / (1000 * 60 * 60 * 24);
    const daysRequired = nextStep.day - sequence[emailsSent - 1].day;
    console.log('daysSinceLast:', daysSinceLast, 'daysRequired:', daysRequired);

    if (daysSinceLast < daysRequired) {
      console.log('Too soon');
      return null;
    }
  }

  return nextStep;
}

async function hasClickedEmail(prospectId) {
  const pool = require('./db');
  const res = await pool.query(`
    SELECT 1 FROM touchpoints
    WHERE prospect_id = $1 AND client_id = $2 AND channel = 'email' AND action_type = 'email_clicked'
    LIMIT 1
  `, [prospectId, CLIENT_ID]);
  return res.rows.length > 0;
}

async function hasSentWarmEmail(prospectId) {
  const pool = require('./db');
  const res = await pool.query(`
    SELECT 1 FROM touchpoints
    WHERE prospect_id = $1 AND client_id = $2 AND channel = 'email' AND action_type = 'email_warm'
    LIMIT 1
  `, [prospectId, CLIENT_ID]);
  return res.rows.length > 0;
}

async function hasPriorStepSend(prospectId, stepDay, sequenceName) {
  const pool = require('./db');
  const res = await pool.query(`
    SELECT 1
    FROM agent_log
    WHERE agent_name = $1
      AND action = 'email_sent'
      AND prospect_id = $2
      AND client_id = $3
      AND payload->>'step' = $4
      AND payload->>'sequence' = $5
      AND COALESCE(payload->>'from_email', '') = $6
    LIMIT 1
  `, [AGENT_NAME, prospectId, CLIENT_ID, String(stepDay), sequenceName, FROM_EMAIL]);
  return res.rows.length > 0;
}

function humanDelay() {
  const ms = (45 + Math.random() * 45) * 1000; // 45–90 seconds
  console.log(`Waiting ${Math.round(ms / 1000)}s before next send...`);
  return new Promise(r => setTimeout(r, ms));
}

function isDashboardTrigger(context = {}) {
  return context?.triggered_by === 'dashboard' ||
    context?.triggeredBy === 'dashboard' ||
    context?.source === 'dashboard';
}

async function run(context = {}) {
  const runId = makeRunId();
  let attempts = 0;
  let successes = 0;
  let skipped = 0;
  let errorSample = null;
  const finish = async (extra = {}) => {
    const result = { attempts, successes, skipped, errorSample, ...extra };
    await reportEmmettRun({ runId, ...result });
    return result;
  };

  try {
  const dashboardOverride = isDashboardTrigger(context);
  if (!dashboardOverride && !isWithinSendingWindow()) {
    console.log(`Outside Emmett sending window (${sendingWindowLabel(CLIENT_ID)}) — skipping run`);
    await logSkippedOutsideWindow();
    return finish({ idle: true, reason: 'outside_sending_window' });
  }
  if (dashboardOverride) {
    console.log('Dashboard-triggered Emmett run — bypassing sending window check');
  }

  const HOLIDAYS_2026 = [
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-05-25',
    '2026-07-04', '2026-09-07', '2026-11-11', '2026-11-26', '2026-12-25'
  ];
  const today = new Date().toISOString().split('T')[0];
  if (HOLIDAYS_2026.includes(today)) {
    console.log(`Holiday detected (${today}) — skipping run`);
    return finish({ idle: true, reason: 'holiday' });
  }

  console.log('\nEmmett agent running...\n');
  CLIENT_CONFIG = await getClientConfig(CLIENT_ID);
  if (!CLIENT_CONFIG) throw new Error(`Active client not found: ${CLIENT_ID}`);
  FROM_EMAIL = CLIENT_CONFIG.sender_email;
  FROM_NAME = CLIENT_CONFIG.sender_name;
  MAIL_TRANSPORTER = createMailTransporter();
  console.log(`Configured sender: ${FROM_NAME || '(missing)'} <${FROM_EMAIL || '(missing)'}>`);

  const sendingDomain = normalizeSendingDomain(CLIENT_CONFIG.sending_domain);
  const brevoState = await getBrevoState(CLIENT_CONFIG);
  const domainHealth = await checkSendingDomainHealth(sendingDomain);
  if (domainHealth.status === 'halted') {
    console.error(`[Emmett] Sends halted. Resolve the Emmett blocker before restarting sends.`);
    await db.logAgentAction(
      AGENT_NAME,
      'cron_run',
      null,
      null,
      {
        sent: 0,
        prospects_evaluated: 0,
        client_id: CLIENT_ID,
        sending_domain: sendingDomain,
        domain_health: domainHealth,
        reason: 'domain_halted',
      },
      'failed'
    );
    return finish({ idle: true, reason: 'domain_halted' });
  }
  if (domainHealth.status === 'paused') {
    console.warn(`[Emmett] Sends paused for ${sendingDomain}. Bounce rate is ${domainHealth.bouncePct}% over ${domainHealth.sends} sends.`);
    await db.logAgentAction(
      AGENT_NAME,
      'cron_run',
      null,
      null,
      {
        sent: 0,
        prospects_evaluated: 0,
        client_id: CLIENT_ID,
        sending_domain: sendingDomain,
        domain_health: domainHealth,
        reason: 'domain_paused',
      },
      'skipped'
    );
    return finish({ idle: true, reason: 'domain_paused' });
  }

  const sendConfig = await getEffectiveSendConfig(getEmmettClientConfig(CLIENT_ID));
  const alreadySentToday = await getEmailsSentToday();
  const remainingCapacity = Math.max(0, sendConfig.dailyCap - alreadySentToday);
  const warmupLabel = sendConfig.warmupProgress
    ? ` (warmup send-day ${sendConfig.warmupProgress.activeSendDays},${sendConfig.warmupProgress.reset ? ' reset,' : ''} ceiling ${getEmmettClientConfig(CLIENT_ID).dailyCap})`
    : sendConfig.ramped ? ' (ramped)' : '';
  console.log(`Daily cap: ${sendConfig.dailyCap}${warmupLabel}; already sent today: ${alreadySentToday}; remaining capacity: ${remainingCapacity}`);

  if (remainingCapacity <= 0) {
    console.log('Daily send limit already reached from database count.');
    await db.logAgentAction(
      AGENT_NAME,
      'cron_run',
      null,
      null,
      { sent: 0, prospects_evaluated: 0, daily_cap: sendConfig.dailyCap, already_sent_today: alreadySentToday, client_id: CLIENT_ID },
      'success'
    );
    return finish({ idle: true, reason: 'daily_cap' });
  }

  const prospects = await getProspectsForEmail(context);
  console.log(`Found ${prospects.length} prospects to contact\n`);

  let sent = 0;
  const requestedMaxSends = Number(context.max_sends || context.maxSends || 0);
  const dailyLimit = requestedMaxSends > 0 ? Math.min(remainingCapacity, requestedMaxSends) : remainingCapacity;
  const verticalCap = sendConfig.verticalCap; // max sends per vertical per run
  const verticalCounts = {};

  for (const prospect of prospects) {
    const stillActive = await getClientConfig(CLIENT_ID);
    if (!stillActive) {
      throw new Error(`[Emmett] Client ${CLIENT_ID} deactivated mid-run — aborting after ${sent} sends`);
    }

    if (sent >= dailyLimit) {
      console.log('Daily send limit reached.');
      break;
    }

    // Vertical cap — prevent blasting a single vertical in one run
    const vertical = (prospect.vertical || 'unknown').toLowerCase();
    verticalCounts[vertical] = (verticalCounts[vertical] || 0);
    if (verticalCounts[vertical] >= verticalCap) {
      console.log(`Skipping ${prospect.email} — vertical cap reached for "${vertical}"`);
      continue;
    }

    console.log('Processing prospect:', prospect.first_name, prospect.email);

    const readiness = await evaluateSendingReadiness({
      client: CLIENT_CONFIG,
      prospect,
      sequenceCatalog: SEQUENCES,
      pool: require('./db'),
      brevoState,
      assignedSequenceName: getSequenceForProspect(prospect),
    });
    if (!readiness.sendable) {
      skipped++;
      await logSendingReadinessBlocked(readiness, 'prospect_evaluation');
      continue;
    }

    let step;
    try {
      step = await getNextSequenceStep(prospect);
      console.log('Step result:', step);
    } catch (err) {
      console.error('getNextSequenceStep error:', err.message);
      continue;
    }
    if (!step) continue;

    const sequenceName = getSequenceForProspect(prospect);

    const invalidEmailReason = invalidOutreachEmailReason(prospect.email);
    if (invalidEmailReason) {
      console.log(`Skipping ${prospect.email} — invalid email (${invalidEmailReason})`);
      await db.logAgentAction(
        AGENT_NAME,
        'email_skipped',
        prospect.id,
        null,
        { reason: invalidEmailReason, prospect_id: prospect.id, email: prospect.email, client_id: prospect.client_id },
        'success'
      );
      continue;
    }

    if (await hasPriorStepSend(prospect.id, step.day, sequenceName)) {
      console.log(`Skipping ${prospect.email} — prior step ${step.day} send already exists`);
      await db.logAgentAction(
        AGENT_NAME,
        'email_skipped',
        prospect.id,
        null,
        { reason: 'prior_step_send', prospect_id: prospect.id, step: step.day, client_id: prospect.client_id },
        'success'
      );
      continue;
    }

    // Check for warm email substitution on Day 4+ follow-ups
    let useWarm = false;
    const usesMshiVerticalFollowups = [
      'mshi',
      'mshi_probate_attorney',
      'mshi_investor_flipper',
    ].includes(sequenceName);
    const hasProtectedSegments = Array.isArray(step.protectedSegments) && step.protectedSegments.length > 0;
    if (!usesMshiVerticalFollowups && sequenceName !== 're_engagement' && step.day > 0 && !hasProtectedSegments) {
      const clicked  = await hasClickedEmail(prospect.id);
      const warmSent = await hasSentWarmEmail(prospect.id);
      if (clicked && warmSent) {
        console.log(`${prospect.email} already received warm email — skipping`);
        continue;
      }
      if (clicked && !warmSent) {
        useWarm = true;
        console.log(`Emmett: ${prospect.email} has clicked — sending warm sequence instead of standard follow-up`);
      }
    }

    const activeStep = useWarm ? WARM_STEP : step;
    const renderedSubject = fillTemplate(activeStep.subject, prospect);
    const renderedBody = fillTemplate(activeStep.body, prospect);
    if (!renderedSubject.ok || !renderedBody.ok) {
      const unknownTokens = [...new Set([
        ...renderedSubject.unknownTokens,
        ...renderedBody.unknownTokens,
      ])];
      const missingTokens = [...new Set([
        ...renderedSubject.missingRequiredTokens,
        ...renderedBody.missingRequiredTokens,
      ])];
      await logSendingReadinessBlocked({
        sendable: false,
        client_id: CLIENT_ID,
        prospect_id: prospect.id,
        sequence: sequenceName,
        checks: [],
        failures: [
          ...(unknownTokens.length ? [{
            code: 'template_tokens_known',
            message: `Template references unknown token(s): ${unknownTokens.join(', ')}.`,
            details: { unknown_tokens: unknownTokens },
          }] : []),
          ...(missingTokens.length ? [{
            code: 'template_required_tokens_present',
            message: `Required template token(s) are empty: ${missingTokens.join(', ')}.`,
            details: { missing_tokens: missingTokens },
          }] : []),
        ],
      }, 'template_render');
      skipped++;
      continue;
    }
    const subject = renderedSubject.output;
    let body = renderedBody.output;
    body = stripForbiddenMshiCopy(body, prospect);
    const aiTellPhrases = findAiTellPhrases(`${subject}\n${body}`);
    if (aiTellPhrases.length) {
      console.warn(`Skipping ${prospect.email} because AI-tell phrase is present in outbound copy: ${aiTellPhrases.join(', ')}`);
      await db.logAgentAction(
        AGENT_NAME,
        'email_skipped',
        prospect.id,
        null,
        {
          reason: 'ai_tell_phrase',
          phrases: aiTellPhrases,
          prospect_id: prospect.id,
          step: step.day,
          sequence: sequenceName,
          client_id: prospect.client_id,
        },
        'success'
      );
      continue;
    }

    console.log(`Sending to: ${prospect.email} (${prospect.name})`);
    console.log(`Subject: ${subject}`);
    if (sent === 0) {
      console.log(`From header: ${FROM_NAME} <${FROM_EMAIL}>`);
    }

    const tags = [sequenceName, `step_${step.day}`, prospect.vertical].filter(Boolean);
    const logPayload = {
      sequence: sequenceName,
      step: step.day,
      vertical: prospect.vertical,
      subject,
      client_id: CLIENT_ID,
      email: prospect.email,
      from_email: FROM_EMAIL,
      from_name: FROM_NAME,
    };
    // Re-check live database and Brevo state at the final boundary. Nothing
    // capable of sending runs after a failed result.
    const finalReadiness = await evaluateSendingReadiness({
      client: CLIENT_CONFIG,
      prospect,
      sequenceCatalog: SEQUENCES,
      pool: require('./db'),
      assignedSequenceName: sequenceName,
    });
    if (!finalReadiness.sendable) {
      skipped++;
      await logSendingReadinessBlocked(finalReadiness, 'pre_send');
      continue;
    }
    const sendLogId = await createEmailSendLog(prospect, logPayload);
    attempts++;
    const result = await sendEmail(
      prospect.email,
      `${prospect.first_name} ${prospect.last_name}`,
      subject,
      body,
      tags
    );

    if (result.success) {
      await completeEmailSendLog(sendLogId, 'email_sent', {
        ...logPayload,
        message_id: result.messageId,
        brevo_response: result.brevoResponse || null,
      }, 'completed');
      await db.logTouchpoint(
        prospect.id,
        'email',
        useWarm ? 'email_warm' : 'outbound',
        subject,
        useWarm ? { sequence: 'warm_outreach' } : { step: step.day, sequence: sequenceName === 're_engagement' ? 're_engagement' : 'cold_outreach' },
        'neutral'
      );
      if (step.day === 0 && !useWarm) {
        const pool = require('./db');
        await pool.query(
          `UPDATE prospects SET status = 'contacted', updated_at = NOW()
           WHERE id = $1 AND client_id = $2 AND status = 'cold'`,
          [prospect.id, CLIENT_ID]
        );
      }
      verticalCounts[vertical]++;
      sent++;
      successes++;
      console.log('Touchpoint logged.\n');
    } else {
      errorSample = errorSample || sendErrorSample(result, prospect);
      await completeEmailSendLog(sendLogId, 'email_failed', { ...logPayload, error: result.error }, 'failed');
      if (context.stopOnSendError) {
        throw new Error(`Brevo send failed for prospect ${prospect.id}: ${result.error}`);
      }
    }

    if (sent < dailyLimit) await humanDelay();
  }

  // After outreach, demote stalled no-response prospects via the dynamic ICP
  // penalty so they fall below engaged prospects on the next run.
  await recalcStalledNoResponseProspects();

  console.log(`\nEmmett complete. Emails sent: ${sent}`);
  await db.logAgentAction(
    AGENT_NAME,
    'cron_run',
    null,
    null,
    {
      sent,
      prospects_evaluated: prospects.length,
      daily_cap: sendConfig.dailyCap,
      already_sent_today: alreadySentToday,
      remaining_capacity: remainingCapacity,
      warmup_active_send_days: sendConfig.warmupProgress?.activeSendDays ?? null,
      warmup_reset: sendConfig.warmupProgress?.reset ?? null,
      vertical_cap: verticalCap,
      client_id: CLIENT_ID,
      attempts,
      successes,
    },
    'success'
  );
  return finish();
  } catch (err) {
    errorSample = errorSample || { error: err.message };
    return finish({ failed: true });
  }
}

module.exports = { run, checkSendingDomainHealth };

if (require.main === module) {
  run().catch(async (err) => {
    try {
      await db.logAgentAction(
        AGENT_NAME,
        'cron_run',
        null,
        null,
        { client_id: CLIENT_ID },
        'failed',
        err.message
      );
    } catch (logErr) {
      console.error('Failed to log Emmett fatal error:', logErr.message);
    }
    console.error(err);
    process.exit(1);
  });
}
