require('dotenv').config();
const axios = require('axios');
const db = require('./dbClient');

const AGENT_NAME = 'emmett';
const BREVO_API_KEY = process.env.BREVO_API_KEY;
const FROM_EMAIL = 'jacob@gopulseforge.com';
const FROM_NAME = 'Jacob Maynard';

// Email sequence definitions
const SEQUENCES = {
  cold_outreach: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I spent years running restaurants and a cleaning company in New England. The hardest part wasn't the work — it was staying visible when I was too busy doing the work to market it.

Most owners I talk to are in the same spot. Great business, not enough time to stay in front of new customers consistently.

I built a system that handles that automatically — finds local prospects, reaches out on your behalf, keeps your name visible between jobs. It runs in the background while you run the business.

I'd love to put together a free mockup showing what this could look like specifically for {{business_name}}.

Worth a look?

Jacob Maynard
gopulseforge.com`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago — wanted to follow up once before moving on.

I know you're busy. That's kind of the whole point.

The businesses I work with aren't struggling — they're good at what they do. They just don't have time to chase new customers on top of everything else. That's the gap I fill.

If you want to see a free mockup of what consistent outreach could look like for {{business_name}}, just reply and I'll have something over to you same day.

Jake`
    },
    {
      day: 8,
      subject: "what's actually working in Manchester right now",
      body: `Hi {{first_name}},

One thing I'm seeing across local businesses right now — the ones growing consistently aren't spending more on ads. They're just staying in front of people longer than their competition.

Most owners go quiet between jobs. The ones winning don't.

I help businesses like {{business_name}} stay visible automatically — no extra time required on your end.

Still happy to put together something specific for you if you want to see it in action.

Jake
gopulseforge.com`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me — I don't want to clutter your inbox.

If the timing is ever right and you want to see what automated outreach could do for {{business_name}}, just reply to this and I'll put something together.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ]
};

const WARM_STEP = {
  subject: "saw you checked us out — quick question",
  body: `Hi {{first_name}},

Noticed you clicked through on one of my earlier notes — wanted to reach out directly.

The fact that you looked means something. I know the inbox is noisy.

I'd love to show you what consistent automated outreach could look like specifically for {{business_name}} — I can have something over to you same day.

Worth a quick conversation this week?

Jacob Maynard
gopulseforge.com`
};

function fillTemplate(template, prospect) {
  const rawName = prospect.first_name || prospect.name?.split(' ')[0] || '';
  const firstName = (rawName && rawName !== '—') ? rawName : 'there';
  const businessName = prospect.company || prospect.notes?.split('—')[0]?.trim() || 'your business';

  return template
    .replace(/{{first_name}}/g, firstName)
    .replace(/{{business_name}}/g, businessName);
}

async function sendEmail(toEmail, toName, subject, body) {
  try {
    const response = await axios.post('https://api.brevo.com/v3/smtp/email', {
      sender: { name: FROM_NAME, email: FROM_EMAIL },
      to: [{ email: toEmail, name: toName }],
      subject,
      htmlContent: '<html><body style="font-family:Georgia,serif;font-size:16px;line-height:1.6;color:#1a1a1a;max-width:560px;margin:0 auto;padding:20px;">' + body.replace(/\n/g, '<br>') + '</body></html>',
      textContent: body
    }, {
      headers: {
        'api-key': BREVO_API_KEY,
        'Content-Type': 'application/json'
      }
    });

    console.log(`Email sent to ${toEmail} — Message ID: ${response.data.messageId}`);
    return true;
  } catch (err) {
    console.error(`Failed to send to ${toEmail}:`, err.response?.data || err.message);
    return false;
  }
}

async function getProspectsForEmail() {
  const pool = require('./db');

  const res = await pool.query(`
    SELECT p.*, c.name as company
    FROM prospects p
    LEFT JOIN companies c ON p.company_id = c.id
    WHERE (p.status = 'cold' OR (
      p.status = 'warm'
      AND EXISTS (
        SELECT 1 FROM touchpoints t2
        WHERE t2.prospect_id = p.id AND t2.channel = 'email'
      )
    ))
    AND p.email IS NOT NULL
    AND p.email != ''
    AND p.email NOT LIKE '%@domain.com'
    AND p.email NOT LIKE '%@example.com'
    AND p.do_not_contact IS NOT TRUE
    AND (
      SELECT COUNT(*) FROM touchpoints t
      WHERE t.prospect_id = p.id AND t.channel = 'email'
    ) < 4
    LIMIT 20
  `);

  return res.rows;
}

async function getNextSequenceStep(prospectId) {
  const pool = require('./db');

  const res = await pool.query(`
    SELECT * FROM touchpoints
    WHERE prospect_id = $1
    AND channel = 'email'
    AND action_type IN ('outbound', 'email_warm')
    ORDER BY created_at ASC
  `, [prospectId]);

  const emailsSent = res.rows.length;
  const sequence = SEQUENCES.cold_outreach;


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
    WHERE prospect_id = $1 AND channel = 'email' AND action_type = 'email_clicked'
    LIMIT 1
  `, [prospectId]);
  return res.rows.length > 0;
}

async function hasSentWarmEmail(prospectId) {
  const pool = require('./db');
  const res = await pool.query(`
    SELECT 1 FROM touchpoints
    WHERE prospect_id = $1 AND channel = 'email' AND action_type = 'email_warm'
    LIMIT 1
  `, [prospectId]);
  return res.rows.length > 0;
}

function humanDelay() {
  const ms = (45 + Math.random() * 45) * 1000; // 45–90 seconds
  console.log(`Waiting ${Math.round(ms / 1000)}s before next send...`);
  return new Promise(r => setTimeout(r, ms));
}

async function run() {
  console.log('\nEmmett agent running...\n');

  const prospects = await getProspectsForEmail();
  console.log(`Found ${prospects.length} prospects to contact\n`);

  console.log('Prospects found:', JSON.stringify(prospects, null, 2));

  let sent = 0;
  const dailyLimit = 40;
  const industryCap = 5; // max sends per industry per run
  const industryCounts = {};

  for (const prospect of prospects) {
    if (sent >= dailyLimit) {
      console.log('Daily send limit reached.');
      break;
    }

    // Industry cap — prevent blasting a single vertical in one run
    const industry = (prospect.industry || 'unknown').toLowerCase();
    industryCounts[industry] = (industryCounts[industry] || 0);
    if (industryCounts[industry] >= industryCap) {
      console.log(`Skipping ${prospect.email} — industry cap reached for "${industry}"`);
      continue;
    }

    console.log('Processing prospect:', prospect.first_name, prospect.email);

    // Per-prospect DNC check (safety net in case status changed since query)
    const pool2 = require('./db');
    const dncCheck = await pool2.query(
      'SELECT do_not_contact FROM prospects WHERE id = $1', [prospect.id]
    );
    if (dncCheck.rows[0]?.do_not_contact) {
      console.log(`Skipping ${prospect.email} — do_not_contact`);
      continue;
    }

    let step;
    try {
      step = await getNextSequenceStep(prospect.id);
      console.log('Step result:', step);
    } catch (err) {
      console.error('getNextSequenceStep error:', err.message);
      continue;
    }
    if (!step) continue;

    // Check for warm email substitution on Day 4+ follow-ups
    let useWarm = false;
    if (step.day > 0) {
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
    const subject = fillTemplate(activeStep.subject, prospect);
    const body    = fillTemplate(activeStep.body,    prospect);

    console.log(`Sending to: ${prospect.email} (${prospect.name})`);
    console.log(`Subject: ${subject}`);

    const success = await sendEmail(
      prospect.email,
      `${prospect.first_name} ${prospect.last_name}`,
      subject,
      body
    );

    if (success) {
      await db.logTouchpoint(
        prospect.id,
        'email',
        useWarm ? 'email_warm' : 'outbound',
        subject,
        useWarm ? { sequence: 'warm_outreach' } : { step: step.day, sequence: 'cold_outreach' },
        'neutral'
      );
      industryCounts[industry]++;
      sent++;
      console.log('Touchpoint logged.\n');
    }

    if (sent < dailyLimit) await humanDelay();
  }

  console.log(`\nEmmett complete. Emails sent: ${sent}`);
}

run().catch(console.error);