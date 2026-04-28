require('dotenv').config();
const axios = require('axios');
const db = require('./dbClient');

const AGENT_NAME = 'emmett_agent';
const BREVO_API_KEY = process.env.BREVO_API_KEY;
const FROM_EMAIL = 'jacob@gopulseforge.com';
const FROM_NAME = 'Jacob Maynard';

// Email sequence definitions
const SEQUENCES = {
  cold_outreach: [
    {
      day: 0,
      subject: "Quick question about {{business_name}}",
      body: `Hi {{first_name}},

I came across {{business_name}} and wanted to reach out directly.

Most local businesses I talk to are doing great work but struggle to stay visible online consistently — not because they don't want to, but because there's simply no time.

I run Pulseforge, an AI marketing system that handles your online presence automatically. It finds potential customers, engages with them on LinkedIn and via email, and keeps your name in front of the right people — without you having to lift a finger.

I'd love to put together a quick mockup showing what this could look like for {{business_name}} specifically. No cost, no commitment.

Worth a 10-minute look?

Jake Maynard
Pulseforge
gopulseforge.com`
    },
    {
      day: 4,
      subject: "Re: Quick question about {{business_name}}",
      body: `Hi {{first_name}},

Just following up on my note from a few days ago.

I know inboxes get busy. I put together a quick example of what an automated outreach system could look like for a business like {{business_name}} — took me about 20 minutes to build.

Happy to send it over if you're curious. No strings attached.

Jake
gopulseforge.com`
    },
    {
      day: 8,
      subject: "One thing that's working for service businesses in Manchester",
      body: `Hi {{first_name}},

I'll keep this short.

The businesses I'm seeing grow consistently right now are the ones that stay visible between jobs — not just when they're actively looking for work.

Pulseforge automates that visibility. It's running for a few local businesses already and the feedback has been strong.

If timing is ever right for {{business_name}}, I'm here.

Jake
gopulseforge.com`
    },
    {
      day: 13,
      subject: "Last note from me",
      body: `Hi {{first_name}},

This is my last follow-up — I don't want to clog your inbox.

If you ever want to see what consistent automated outreach could do for {{business_name}}, just reply to this email and I'll put something together for you.

Wishing you a strong season either way.

Jake Maynard
Pulseforge
gopulseforge.com`
    }
  ]
};

function fillTemplate(template, prospect) {
  const firstName = prospect.first_name || prospect.name?.split(' ')[0] || 'there';
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

  // Get prospects that are cold and have an email
  const res = await pool.query(`
    SELECT p.*, c.name as company
    FROM prospects p
    LEFT JOIN companies c ON p.company_id = c.id
    WHERE p.status = 'cold'
    AND p.email IS NOT NULL
    AND p.email != ''
    AND NOT EXISTS (
      SELECT 1 FROM touchpoints t
      WHERE t.prospect_id = p.id
      AND t.channel = 'email'
      AND t.created_at > NOW() - INTERVAL '14 days'
    )
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

async function run() {
  console.log('\nEmmett agent running...\n');

  const prospects = await getProspectsForEmail();
  console.log(`Found ${prospects.length} prospects to contact\n`);

  console.log('Prospects found:', JSON.stringify(prospects, null, 2));

  let sent = 0;
  const dailyLimit = 40;

  for (const prospect of prospects) {
    if (sent >= dailyLimit) {
      console.log('Daily send limit reached.');
      break;
    }

    console.log('Processing prospect:', prospect.first_name, prospect.email);

  let step;
try {
  step = await getNextSequenceStep(prospect.id);
  console.log('Step result:', step);
} catch (err) {
  console.error('getNextSequenceStep error:', err.message);
  continue;
}
if (!step) continue;

    const subject = fillTemplate(step.subject, prospect);
    const body = fillTemplate(step.body, prospect);

    console.log(`Sending to: ${prospect.email} (${prospect.name})`);
    console.log(`Subject: ${subject}`);

   const success = await sendEmail(prospect.email, `${prospect.first_name} ${prospect.last_name}`, subject, body);

    if (success) {
      await db.logTouchpoint(
        prospect.id,
        'email',
        'outbound',
        subject,
        { step: step.day, sequence: 'cold_outreach' },
        'neutral'
      );
      sent++;
      console.log('Touchpoint logged.\n');
    }

    // Small delay between sends
    await new Promise(r => setTimeout(r, 2000));
  }

  console.log(`\nEmmett complete. Emails sent: ${sent}`);
}

run().catch(console.error);