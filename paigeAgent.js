require('dotenv').config();
const pool = require('./db');
const Anthropic = require('@anthropic-ai/sdk');
const { sendTelegramNotification } = require('./utils/telegram');

const client = new Anthropic();
const AGENT_NAME = 'paige';

const CONTENT_TYPES = ['promotional', 'educational', 'seasonal', 'behind-the-scenes', 'community'];
const BLOG_CONTENT_TYPES = ['educational', 'behind-the-scenes', 'community', 'seasonal'];
const CHANNELS = ['facebook_page', 'google_business', 'blog'];

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
async function pickContentType(companyName, channel = null) {
  const res = await pool.query(`
    SELECT post_content
    FROM pending_comments
    WHERE author_name = $1
      AND channel IN ('facebook_page', 'google_business', 'blog')
      AND created_at > NOW() - INTERVAL '30 days'
    ORDER BY created_at DESC
    LIMIT 10
  `, [companyName]);

  const recentTypes = res.rows
    .map(r => r.post_content?.split('·')[1]?.trim().toLowerCase())
    .filter(Boolean);

  const types = channel === 'blog' ? BLOG_CONTENT_TYPES : CONTENT_TYPES;
  const unused = types.filter(t => !recentTypes.some(r => r.includes(t)));
  return unused.length > 0 ? unused[0] : types[0];
}

function buildFacebookPrompt(company, contentType, verticalCtx, location) {
  const isPulseforge = company.name.toLowerCase().includes('pulseforge');

  const audienceNote = isPulseforge
    ? `IMPORTANT: Pulseforge's audience is small business owners considering marketing automation — not end customers of another business. Write directly to an owner who is tired of doing repetitive marketing tasks themselves and wants a system that runs in the background.`
    : `Write as the actual owner of ${company.name} talking to their local community in ${location}.`;

  return `You are writing a Facebook post for ${company.name}'s business page.
${company.name} is a local ${company.industry || 'business'} in ${location}${isPulseforge ? ' that automates marketing and outreach for small business owners using AI' : ` that uses Pulseforge — an AI system that automates their marketing so the owner can focus on the actual work`}.

Content type: ${contentType}
Business context: ${verticalCtx}

${audienceNote}

Write a Facebook post (2-4 sentences) that:
- Sounds like a real person talking, not a brand account — no "excited to announce," no "we pride ourselves"
- Is specific to ${location} — reference the area naturally
- For "promotional": leads with a concrete result or offer, ends with a soft CTA
- For "educational": shares one genuinely useful tip the reader can act on
- For "seasonal": ties to what's actually happening in ${location} right now
- For "behind-the-scenes": gives a glimpse of the people or process behind the business
- For "community": mentions something local — a neighborhood, event, or shared experience
- May end with 1-2 hashtags only if they feel natural — skip if they don't

No buzzwords. No corporate tone. Write like a person.

Return only the post text.`;
}

function buildGooglePrompt(company, contentType, verticalCtx, location) {
  const isPulseforge = company.name.toLowerCase().includes('pulseforge');

  const audienceNote = isPulseforge
    ? `IMPORTANT: Pulseforge's audience is small business owners searching for ways to automate their marketing — not end customers of another business. Write to an owner who wants leads and visibility without doing the manual work themselves.`
    : `Write for a potential customer in ${location} who is searching for ${company.industry || 'this type of service'} and deciding whether to call.`;

  return `You are writing a Google Business Profile update for ${company.name}, a local ${company.industry || 'business'} in ${location}${isPulseforge ? ' that helps small business owners automate their marketing using AI' : ''}.

Content type: ${contentType}
Business context: ${verticalCtx}

${audienceNote}

Write a Google Business update (3-5 sentences) that:
- Reads like something genuinely useful to someone searching "${company.industry || 'local services'} near me" — not promotional fluff
- Mentions ${location} or the surrounding area naturally
- For "promotional": states the offer clearly and why it matters to a new customer
- For "educational": answers a real question people ask before booking or buying
- For "seasonal": explains what customers should be thinking about this time of year
- For "behind-the-scenes": builds trust by describing the team, process, or standards
- For "community": connects the business to the local area in a credible way
- NO hashtags, NO emojis — professional but human, not corporate

Return only the post text.`;
}

function buildBlogPrompt(company, contentType, verticalCtx) {
  const isPulseforge = company.name.toLowerCase().includes('pulseforge');
  const location = company.location || 'Manchester, NH';

  const audienceNote = isPulseforge
    ? `IMPORTANT: Pulseforge's audience is small business owners considering marketing automation — not end customers of another business. Write to an owner who is tired of doing repetitive marketing tasks themselves and wants a system that runs in the background.`
    : `Write as the owner of ${company.name} — personal, grounded, and expert. This is their business blog, not a corporate content farm.`;

  return `You are writing a blog post for ${company.name}, a local ${company.industry || 'business'} in ${location}${isPulseforge ? ' that automates marketing and outreach for small business owners using AI' : ''}.

Content type: ${contentType}
Business context: ${verticalCtx}

${audienceNote}

Write a blog post (350-500 words) using this structure:
# [Title]

[Intro paragraph — mention the business name and what they do, include ${location} naturally]

## [Subheading]
[Body section]

## [Subheading]
[Body section]

[Optional third ## section if it fits naturally — skip if it would feel padded]

[Closing paragraph with a soft, non-pushy call to action]

Requirements:
- Use # for the title and ## for subheadings — no other markdown
- Mention ${location} or the surrounding area 2-3 times naturally, never forced
- Sounds like the business owner wrote it — specific, personal, expert
- For "educational": practical how-to or tips the reader can actually use
- For "behind-the-scenes": a window into the people, process, or story behind the business
- For "community": local focus — neighborhoods, events, what it means to serve this area
- For "seasonal": what customers should know or do this time of year, from an expert's perspective
- Never open with "In today's digital world" or any generic throat-clearing — start specific
- No keyword stuffing — mention the business and location where they fit naturally
- No corporate tone, no "we pride ourselves," no "cutting-edge solutions"

Return only the blog post text with markdown formatting.`;
}

async function generatePost(company, contentType, channel) {
  const verticalCtx = getVerticalContext(company.industry);
  const location = company.location || 'Manchester, NH';

  const prompt = channel === 'google_business'
    ? buildGooglePrompt(company, contentType, verticalCtx, location)
    : channel === 'blog'
      ? buildBlogPrompt(company, contentType, verticalCtx)
      : buildFacebookPrompt(company, contentType, verticalCtx, location);

  const message = await client.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: channel === 'blog' ? 800 : 300,
    messages: [{ role: 'user', content: prompt }]
  });

  return message.content[0].text.trim();
}

async function saveToPendingApprovals(company, content, contentType, channel) {
  const channelLabel = { facebook_page: 'Facebook Page', google_business: 'Google Business', blog: 'Blog' }[channel] || channel;
  const label = `${channelLabel} · ${contentType.charAt(0).toUpperCase() + contentType.slice(1)}`;

  const existing = await pool.query(`
    SELECT id FROM pending_comments
    WHERE channel = $1
      AND post_content = $2
      AND author_name = $3
      AND status = 'pending'
    LIMIT 1
  `, [channel, label, company.name]);

  if (existing.rows.length > 0) {
    console.log(`  ↷ Skipping duplicate: ${channel} · ${contentType} for ${company.name}`);
    return null;
  }

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
  console.log('-- CLEANUP QUERY (run manually in psql to remove existing duplicates) --');
  console.log(`DELETE FROM pending_comments
WHERE id NOT IN (
  SELECT DISTINCT ON (channel, post_content, author_name) id
  FROM pending_comments
  WHERE status = 'pending'
  ORDER BY channel, post_content, author_name, created_at DESC
)
AND status = 'pending';`);
  console.log('------------------------------------------------------------------------\n');

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
        const contentType = await pickContentType(company.name, channel);
        console.log(`${company.name} — ${channel} — ${contentType}`);

        try {
          const content = await generatePost(company, contentType, channel);
          const id = await saveToPendingApprovals(company, content, contentType, channel);
          if (id) {
            console.log(`  ✓ queued (${id.slice(0, 8)})\n`);
            generated++;
            const channelLabel = { facebook_page: 'Facebook Page', google_business: 'Google Business', blog: 'Blog' }[channel] || channel;
            await sendTelegramNotification({
              channel,
              post_content: `${channelLabel} · ${contentType.charAt(0).toUpperCase() + contentType.slice(1)}`,
              comment: content,
            });
          }
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
