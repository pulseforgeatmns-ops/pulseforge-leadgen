require('dotenv').config();
const pool = require('./db');
const Anthropic = require('@anthropic-ai/sdk');

const client = new Anthropic();
const AGENT_NAME = 'paige_agent';

const CONTENT_TYPES = ['promotional', 'educational', 'seasonal', 'behind-the-scenes', 'community'];
const CHANNELS = ['facebook_page', 'google_business'];

// Vertical-specific context fed into the prompt so Claude sounds right for each business type
const VERTICAL_PROMPTS = {
  cleaning:    'spotless results, reliable local team, before-and-after transformations, booking convenience, trust and consistency',
  restaurant:  'fresh food, daily specials, family-friendly atmosphere, local ingredients, dine-in and takeout',
  hvac:        'heating and cooling, seasonal tune-ups, home comfort, licensed and insured technicians, fast response',
  salon:       'haircuts, color, styling, making clients look and feel their best, welcoming environment, skilled stylists',
  fitness:     'workouts, personal training, community classes, achieving health goals, supportive welcoming atmosphere',
  landscaping: 'lawn care, curb appeal, seasonal cleanups, dependable crew, beautiful outdoor spaces year-round',
  plumbing:    'fast response, licensed plumbers, repairs and installs, upfront pricing, no surprises',
  auto:        'honest repairs, skilled mechanics, routine maintenance, transparent pricing, keeping you safely on the road',
};

function getVerticalContext(industry) {
  const lower = (industry || '').toLowerCase();
  const match = Object.entries(VERTICAL_PROMPTS).find(([k]) => lower.includes(k));
  return match ? match[1] : `local ${industry || 'business'} services`;
}

async function getActiveClients() {
  const res = await pool.query(`
    SELECT id, name, industry, location, website, notes
    FROM companies
    WHERE name IS NOT NULL AND name != ''
    ORDER BY created_at DESC
    LIMIT 20
  `);
  return res.rows;
}

// Pick a content type we haven't used recently for this company so posts stay varied
async function pickContentType(companyName) {
  const res = await pool.query(`
    SELECT post_content
    FROM pending_comments
    WHERE author_name = $1
      AND channel IN ('facebook_page', 'google_business')
      AND created_at > NOW() - INTERVAL '30 days'
    ORDER BY created_at DESC
    LIMIT 10
  `, [companyName]);

  const recentTypes = res.rows
    .map(r => r.post_content?.split('·')[1]?.trim().toLowerCase())
    .filter(Boolean);

  const unused = CONTENT_TYPES.filter(t => !recentTypes.some(r => r.includes(t)));
  return unused.length > 0 ? unused[0] : CONTENT_TYPES[0];
}

async function generatePost(company, contentType, channel) {
  const verticalCtx = getVerticalContext(company.industry);
  const location = company.location || 'Manchester, NH';

  const channelNote = channel === 'google_business'
    ? 'Google Business Profile update — professional tone, warm but concise, no hashtags, no emojis'
    : 'Facebook page post — conversational, include a question or CTA to spark engagement, 2-3 relevant hashtags at the end are fine';

  const message = await client.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 300,
    messages: [{
      role: 'user',
      content: `You are writing a ${contentType} social media post for ${company.name}, a local business in ${location}.

Business focus: ${verticalCtx}
Post format: ${channelNote}
Content type: ${contentType}

Write 2-4 sentences. Sound like the actual owner wrote it — friendly, local, genuine. Never corporate or salesy. Be specific to ${location} and what this business does. Do not mention competitors.

Return only the post text. No quotes, no labels, no explanation.`
    }]
  });

  return message.content[0].text.trim();
}

async function saveToPendingApprovals(company, content, contentType, channel) {
  const channelLabel = channel === 'facebook_page' ? 'Facebook Page' : 'Google Business';
  const label = `${channelLabel} · ${contentType.charAt(0).toUpperCase() + contentType.slice(1)}`;

  const res = await pool.query(`
    INSERT INTO pending_comments
      (author_name, author_title, post_content, comment, post_url, channel, status)
    VALUES ($1, $2, $3, $4, NULL, $5, 'pending')
    RETURNING id
  `, [
    company.name,
    company.industry || 'Local Business',
    label,
    content,
    channel
  ]);

  return res.rows[0].id;
}

async function logRun(status, payload) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at)
    VALUES ($1, $2, $3, $4, NOW())
  `, [AGENT_NAME, 'generate_content', JSON.stringify(payload), status]);
}

async function run() {
  console.log('\nPaige agent running...\n');

  try {
    const clients = await getActiveClients();
    console.log(`Found ${clients.length} client${clients.length !== 1 ? 's' : ''}.\n`);

    if (!clients.length) {
      console.log('No clients in the companies table. Add companies to get started.');
      await logRun('success', { clients_processed: 0, posts_generated: 0 });
      return;
    }

    let generated = 0;

    for (const company of clients) {
      for (const channel of CHANNELS) {
        const contentType = await pickContentType(company.name);
        console.log(`${company.name} — ${channel} — ${contentType}`);

        try {
          const content = await generatePost(company, contentType, channel);
          const id = await saveToPendingApprovals(company, content, contentType, channel);
          console.log(`  ✓ queued (${id.slice(0, 8)})\n`);
          generated++;
        } catch (err) {
          console.error(`  ✗ ${company.name}/${channel}: ${err.message}`);
        }

        await new Promise(r => setTimeout(r, 1500));
      }
    }

    await logRun('success', { clients_processed: clients.length, posts_generated: generated });
    console.log(`\nPaige complete — ${generated} post${generated !== 1 ? 's' : ''} queued for approval.`);
  } catch (err) {
    console.error('Paige error:', err.message);
    await logRun('error', { error: err.message }).catch(() => {});
  }
}

run().catch(console.error);
