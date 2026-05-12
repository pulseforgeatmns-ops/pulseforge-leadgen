require('dotenv').config();
const axios = require('axios');
const db = require('./dbClient');

const AGENT_NAME = 'emmett';
const BREVO_API_KEY = process.env.BREVO_API_KEY;
const FROM_EMAIL = 'jacob@gopulseforge.com';
const FROM_NAME = 'Jacob Maynard';

// Email sequence definitions
const DEMO_URL = 'https://pulseforge-leadgen-production.up.railway.app/demo';
const CALENDLY_URL = 'https://calendly.com/jacob-gopulseforge/20min';

const SEQUENCES = {
  cleaning: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I spent years running restaurants and a cleaning company in New England. The hardest part wasn't the work — it was staying visible when I was too busy doing the work to market it.

Most owners I talk to are in the same spot. Great business, not enough time to stay in front of new customers consistently.

I built a system that handles that automatically — finds local prospects, reaches out on your behalf, keeps your name visible between jobs. It runs in the background while you run the business.

Reply and I'll send over a quick overview of what this looks like for a business like {{business_name}}.

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

If you want to see what the system actually looks like running:
${DEMO_URL}

Jake`
    },
    {
      day: 8,
      subject: "what's actually working in Manchester right now",
      body: `Hi {{first_name}},

One thing I'm seeing across local businesses right now — the ones growing consistently aren't spending more on ads. They're just staying in front of people longer than their competition.

Most owners go quiet between jobs. The ones winning don't.

I help businesses like {{business_name}} stay visible automatically — no extra time required on your end.

If this is relevant for {{business_name}}, here's a link to grab 20 minutes with me:
${CALENDLY_URL}

Jake`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me — I don't want to clutter your inbox.

If the timing is ever right, the demo is here: ${DEMO_URL}

Or just reply anytime — I'll put something together for {{business_name}} same day.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],
  restaurant: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I spent years running restaurants in New England. The hardest part was not the service or the food. It was staying visible when I was too busy running the floor to think about marketing.

Most owners I talk to are in the same spot. Great restaurant, not enough time to stay in front of new customers consistently.

I built a system that handles that automatically. It finds local prospects, reaches out on your behalf, and keeps your name visible between rushes. It runs in the background while you run the restaurant.

One question: is getting more new customers through the door something you are actively working on right now?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

I know you are busy. That is kind of the whole point.

The restaurants I work with are not struggling. They are good at what they do. They just do not have time to chase new customers on top of running the kitchen, managing staff, and everything else. That is the gap I fill.

If you want to see a free mockup of what consistent outreach could look like for {{business_name}}, just reply and I will have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working in Manchester right now",
      body: `Hi {{first_name}},

One thing I am seeing across local restaurants right now. The ones growing consistently are not spending more on ads. They are just staying in front of people longer than their competition.

Most owners go quiet between services. The ones winning do not.

I help restaurants like {{business_name}} stay visible automatically. No extra time required on your end.

Quick question before I move on. Are you currently doing anything to stay in front of customers between visits, or is it mostly word of mouth at this point?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing is ever right and you want to see what automated outreach could do for {{business_name}}, just reply to this and I will put something together.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],
  salon: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I work with local service businesses in Southern NH on one specific problem: staying visible to new customers without adding more to your plate.

Most salon owners I talk to are fully booked with existing clients but not consistently bringing in new ones. The referral pipeline is unpredictable and there is never enough time to market between appointments.

I built a system that handles outreach automatically. It finds local prospects, reaches out on your behalf, and keeps your name in front of people who are actively looking for a stylist. It runs in the background while you focus on clients.

Is bringing in new clients consistently something you are actively working on right now?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

The salons I work with are not struggling. They are talented and their existing clients love them. The challenge is always the same: not enough time between appointments to market consistently.

If you want to see a free mockup of what automated outreach could look like for {{business_name}}, just reply and I will have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working in Southern NH right now",
      body: `Hi {{first_name}},

One thing I am seeing across local salons right now. The ones growing consistently are not running more ads. They are just staying in front of new clients longer than their competition.

Most stylists go quiet between bookings. The ones building a waitlist do not.

I help salons like {{business_name}} stay visible automatically. No extra time required on your end.

Are you currently doing anything to bring in new clients consistently, or is it mostly referrals and repeat bookings?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing is ever right and you want to see what automated outreach could do for {{business_name}}, just reply and I will put something together.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  fitness: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I work with local fitness studios and gyms in Southern NH on one specific problem: keeping your schedule full without spending your time chasing new members.

Most studio owners I talk to are great at what they do but inconsistent at bringing in new clients. Word of mouth works until it does not. And there is never enough time between classes to market consistently.

I built a system that handles outreach automatically. It finds local prospects, reaches out on your behalf, and keeps your name in front of people who are actively looking for a gym or studio. It runs in the background while you focus on your members.

Is growing your membership consistently something you are actively working on right now?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

The studios I work with are not struggling. They have great instructors and loyal members. The challenge is always the same: memberships plateau because there is no consistent way to reach new people between managing classes and running the business.

If you want to see a free mockup of what automated outreach could look like for {{business_name}}, just reply and I will have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for fitness studios in Southern NH",
      body: `Hi {{first_name}},

One thing I am seeing across local studios right now. The ones growing their membership are not running more ads. They are just staying visible to new people longer than their competition.

Most owners go quiet between sessions. The ones building waitlists do not.

I help studios like {{business_name}} stay visible automatically. No extra time required on your end.

Are you currently doing anything to bring in new members consistently, or is it mostly referrals and word of mouth?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing is ever right and you want to see what automated outreach could do for {{business_name}}, just reply to this and I will put something together.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  property: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I work with local property management companies in Southern NH on one specific problem: keeping units filled without relying entirely on Zillow and word of mouth.

Most property managers I talk to spend more time than they should chasing leads manually. The pipeline is inconsistent and there is never enough time between managing tenants and owners to market the portfolio properly.

I built a system that handles outreach automatically. It identifies local prospects, reaches out on your behalf, and keeps your properties in front of people who are actively looking. It runs in the background while you manage the day to day.

Is filling vacancies faster and more consistently something you are actively working on right now?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

The property management companies I work with are not struggling. They have solid portfolios and good relationships with their owners. The challenge is always the same: vacancy periods stretch longer than they should because outreach is inconsistent or manual.

If you want to see a free mockup of what automated outreach could look like for {{business_name}}, just reply and I will have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for property managers in Southern NH",
      body: `Hi {{first_name}},

One thing I am seeing across local property management companies right now. The ones keeping vacancy rates low are not spending more on listing sites. They are just staying in front of prospective tenants and owner referrals longer than their competition.

Most companies go quiet between leases. The ones with full portfolios do not.

I help companies like {{business_name}} stay visible automatically. No extra time required on your end.

Are you currently doing anything to generate consistent leads for vacancies, or is it mostly listing sites and referrals?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing is ever right and you want to see what automated outreach could do for {{business_name}}, just reply to this and I will put something together.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  landscaping: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I work with local lawn care and landscaping companies in Southern NH on one specific problem: keeping the schedule full year round without relying on door knocking and referrals alone.

Most owners I talk to are great at the work but inconsistent at bringing in new clients. Spring fills up fast but the pipeline for summer and fall maintenance is always thinner than it should be.

I built a system that handles outreach automatically. It finds local homeowners and commercial properties in your area, reaches out on your behalf, and keeps your name visible between jobs. It runs in the background while you run the crew.

Is keeping the schedule consistently full something you are actively working on right now?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

The landscaping companies I work with are not struggling. They do great work and their existing clients love them. The challenge is always the same: the off-season pipeline dries up and spring scramble is stressful every year.

If you want to see a free mockup of what consistent outreach could look like for {{business_name}}, just reply and I will have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for landscapers in Southern NH",
      body: `Hi {{first_name}},

One thing I am seeing across local landscaping companies right now. The ones with full schedules are not spending more on ads. They are just staying in front of new clients longer than their competition.

Most owners go quiet between jobs. The ones with waitlists do not.

I help companies like {{business_name}} stay visible automatically. No extra time required on your end.

Are you currently doing anything to bring in new clients consistently, or is it mostly referrals and repeat customers?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing is ever right and you want to see what automated outreach could do for {{business_name}}, just reply to this and I will put something together.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  home_services: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I work with local home service companies in Southern NH on one specific problem: staying visible to homeowners between jobs so the pipeline does not go quiet.

Most owners I talk to get plenty of repeat business from happy customers but are not consistently reaching new homeowners. Referrals are great until they are not enough.

I built a system that handles outreach automatically. It finds local homeowners in your area, reaches out on your behalf, and keeps {{business_name}} visible year round. It runs in the background while you run the jobs.

Is building a more consistent pipeline of new homeowners something you are actively working on?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

The home service companies I work with are not struggling. They do great work. The challenge is always the same: the work fills the schedule but not enough new homeowners are finding them consistently.

If you want to see a free mockup of what that outreach could look like for {{business_name}}, just reply and I will have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for home services in Southern NH",
      body: `Hi {{first_name}},

One thing I am seeing across local home service companies right now. The ones with full schedules are not relying on referrals alone. They are staying in front of new homeowners consistently.

Most owners go quiet between jobs. The ones with waitlists do not.

I help companies like {{business_name}} stay visible automatically. No extra time required on your end.

Are you currently doing anything to reach new homeowners consistently, or is it mostly word of mouth?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing is ever right and you want to see what automated outreach could do for {{business_name}}, just reply and I will put something together.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  auto: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I work with local auto repair shops in Southern NH on one specific problem: keeping the service bay full without waiting for walk-ins and hoping word of mouth is enough.

Most shop owners I talk to are excellent mechanics with loyal repeat customers. The challenge is not the quality of work. It is staying consistently in front of local car owners who have not found them yet.

I built a system that handles that automatically. It finds local car owners in your area, reaches out on your behalf, and keeps {{business_name}} visible between service intervals. It runs in the background while you run the shop.

Is building a steadier flow of new customers something you are working on right now?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

The shops I work with are not struggling. They do honest work and their regulars keep coming back. The gap is always the same: not enough new customers finding them between the ones who already know them.

If you want to see a free mockup of what consistent outreach could look like for {{business_name}}, just reply and I will have something over same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for auto shops in Southern NH",
      body: `Hi {{first_name}},

One pattern I keep seeing across local auto shops right now. The ones with bays consistently full are not spending more on ads. They are just staying in front of local car owners longer than the shop down the street.

Most shops wait for the phone to ring. The ones with no open slots do not.

I help shops like {{business_name}} stay visible automatically. No extra time on your end.

Are you currently doing anything to reach new customers consistently, or is it mostly repeat and referral?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing is ever right and you want to see what automated outreach could do for {{business_name}}, just reply and I will put something together.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  med_spa: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

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
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

The practices I work with are not struggling. They have skilled providers and clients who love them. The gap is always the same: not enough new people finding them consistently outside of referrals.

If you want to see a free mockup of what that outreach could look like for {{business_name}}, just reply and I will have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for med spas in Southern NH",
      body: `Hi {{first_name}},

One thing I am seeing across local aesthetic practices right now. The ones growing consistently are not just posting more on Instagram. They are staying in front of local prospects who are actively looking but have not booked yet.

Most practices rely on existing clients to spread the word. The ones with waitlists do more than that.

I help practices like {{business_name}} stay visible to new clients automatically. No extra time required on your end.

Are you currently doing anything to reach new clients consistently outside of social media and referrals?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing is ever right and you want to see what automated outreach could do for {{business_name}}, just reply and I will put something together.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ]
};

function fillTemplate(template, prospect) {
  const rawName = prospect.first_name || prospect.name?.split(' ')[0] || '';
  const firstName = (rawName && rawName !== '—') ? rawName : 'there';

  let businessName = prospect.company || '';
  if (!businessName) {
    const fromNotes = prospect.notes?.split('—')[0]?.trim() || '';
    // Clean domain-style names
    businessName = fromNotes
      .replace(/\.(com|net|org|io|us)$/i, '')
      .replace(/^(www\.)/i, '')
      .trim();
  }
  if (!businessName || businessName.length < 4) businessName = 'your business';

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
    LIMIT 100
  `);

  return res.rows;
}

function getSequenceForProspect(prospect) {
  const vertical = (prospect.vertical || prospect.industry || '').toLowerCase();
  if (vertical.includes('salon') || vertical.includes('hair') || vertical.includes('spa') || vertical.includes('barber')) {
    return 'salon';
  }
  if (vertical.includes('restaurant') || vertical.includes('cafe') || vertical.includes('diner')) {
    return 'restaurant';
  }
  if (vertical.includes('fitness') || vertical.includes('gym') || vertical.includes('yoga') || vertical.includes('pilates') || vertical.includes('studio') || vertical.includes('barre')) {
    return 'fitness';
  }
  if (vertical.includes('property') || vertical.includes('management') || vertical.includes('millcity')) {
    return 'property';
  }
  if (vertical.includes('landscap') || vertical.includes('lawn')) {
    return 'landscaping';
  }
  if (vertical.includes('home') || vertical.includes('services') || vertical.includes('hvac') || vertical.includes('plumb') || vertical.includes('electric') || vertical.includes('handyman')) {
    return 'home_services';
  }
  if (vertical.includes('auto') || vertical.includes('repair') || vertical.includes('mechanic') || vertical.includes('car')) {
    return 'auto';
  }
  if (vertical.includes('med') || vertical.includes('aesthetic') || vertical.includes('botox') || vertical.includes('laser')) {
    return 'med_spa';
  }
  return 'cleaning';
}

async function getNextSequenceStep(prospect) {
  const pool = require('./db');

  const res = await pool.query(`
    SELECT * FROM touchpoints
    WHERE prospect_id = $1
    AND channel = 'email'
    AND action_type IN ('outbound', 'email_warm')
    ORDER BY created_at ASC
  `, [prospect.id]);

  const emailsSent = res.rows.length;
  const sequence = SEQUENCES[getSequenceForProspect(prospect)];


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
  const dailyLimit = 100;
  const industryCap = 15; // max sends per industry per run
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
      step = await getNextSequenceStep(prospect);
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
