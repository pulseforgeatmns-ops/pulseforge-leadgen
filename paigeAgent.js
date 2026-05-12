require('dotenv').config();
const pool = require('./db');
const Anthropic = require('@anthropic-ai/sdk');

const client = new Anthropic();
const AGENT_NAME = 'paige';

const CONTENT_TYPES = ['promotional', 'educational', 'seasonal', 'behind-the-scenes', 'community'];
const BLOG_CONTENT_TYPES = ['educational', 'behind-the-scenes', 'community', 'seasonal'];
const LINKEDIN_CONTENT_TYPES = ['educational', 'behind-the-scenes', 'results', 'community'];
const CHANNELS = ['facebook_page', 'google_business', 'blog', 'linkedin_page'];

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

// Pick a content type — uses performance weighting when ≥4 posts have data, else round-robin
async function pickContentType(companyName, channel = null, companyId = null) {
  const types = channel === 'blog'
    ? BLOG_CONTENT_TYPES
    : channel === 'linkedin_page'
      ? LINKEDIN_CONTENT_TYPES
      : CONTENT_TYPES;

  // Try performance-weighted selection if we have enough data
  if (companyId && channel) {
    try {
      const perfRes = await pool.query(`
        SELECT content_type, avg_engagement_rate, post_count
        FROM content_performance_summary
        WHERE company_id = $1 AND channel = $2
          AND content_type = ANY($3)
        ORDER BY avg_engagement_rate DESC
      `, [companyId, channel, types]);

      const rows = perfRes.rows.filter(r => r.post_count >= 4);
      if (rows.length >= 2) {
        // Weighted random: top third 50%, middle 30%, bottom 20%
        const third = Math.ceil(rows.length / 3);
        const top    = rows.slice(0, third);
        const mid    = rows.slice(third, third * 2);
        const bot    = rows.slice(third * 2);

        const rand = Math.random();
        let pool_choice;
        if (rand < 0.50 && top.length)           pool_choice = top;
        else if (rand < 0.80 && mid.length)      pool_choice = mid;
        else if (bot.length)                      pool_choice = bot;
        else                                      pool_choice = top;

        const pick = pool_choice[Math.floor(Math.random() * pool_choice.length)];
        return pick.content_type;
      }
    } catch (_) {}
  }

  // Fallback: round-robin based on recent pending_comments history
  const channelClause = channel ? `AND channel = $2` : `AND channel IN ('facebook_page', 'google_business', 'blog')`;
  const params = channel ? [companyName, channel] : [companyName];

  const res = await pool.query(`
    SELECT post_content
    FROM pending_comments
    WHERE author_name = $1
      ${channelClause}
      AND created_at > NOW() - INTERVAL '60 days'
    ORDER BY created_at DESC
    LIMIT 20
  `, params);

  const recentTypes = res.rows
    .map(r => r.post_content?.split('·').pop()?.trim().toLowerCase())
    .filter(Boolean);

  const unused = types.filter(t => !recentTypes.includes(t));
  if (unused.length > 0) return unused[0];

  for (let i = recentTypes.length - 1; i >= 0; i--) {
    if (types.includes(recentTypes[i])) return recentTypes[i];
  }
  return types[0];
}

function buildFacebookPrompt(company, contentType, verticalCtx, location, lastContentType) {
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
- Never use dashes or hyphens in any content you write — not as punctuation, not as separators, not in any context.

No buzzwords. No corporate tone. Write like a person.

VARIETY RULES — enforce these on every post:
- Never start a post with "Most small business owners" or any variation of that phrase
- Never start consecutive posts with the same opening word or phrase
- Rotate the angle on every post — do not repeat the same core message within 7 days
- Avoid these overused openers: "Most", "Many", "As a", "If you're a", "Running a small business"
- Each post must make ONE specific point — not a general observation about small business
- Use concrete specifics — a season, a day of week, a specific service, a local reference — rather than broad statements
- If the previous post was educational, this one should be promotional or behind-the-scenes, and vice versa
${lastContentType ? `\nThe last post for this channel was: ${lastContentType}. This post must feel distinct from that — different angle, different opening, different structure.` : ''}

Return only the post text.`;
}

function buildGooglePrompt(company, contentType, verticalCtx, location, lastContentType) {
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
- Never use dashes or hyphens in any content you write — not as punctuation, not as separators, not in any context.

VARIETY RULES — enforce these on every post:
- Never start a post with "Most small business owners" or any variation of that phrase
- Never start consecutive posts with the same opening word or phrase
- Rotate the angle on every post — do not repeat the same core message within 7 days
- Avoid these overused openers: "Most", "Many", "As a", "If you're a", "Running a small business"
- Each post must make ONE specific point — not a general observation about small business
- Use concrete specifics — a season, a day of week, a specific service, a local reference — rather than broad statements
- If the previous post was educational, this one should be promotional or behind-the-scenes, and vice versa
${lastContentType ? `\nThe last post for this channel was: ${lastContentType}. This post must feel distinct from that — different angle, different opening, different structure.` : ''}

Return only the post text.`;
}

function buildBlogPrompt(company, contentType, verticalCtx, lastContentType) {
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
- Never use dashes or hyphens in any content you write — not as punctuation, not as separators, not in any context.

VARIETY RULES — enforce these on every post:
- Never start a post with "Most small business owners" or any variation of that phrase
- Never start consecutive posts with the same opening word or phrase
- Rotate the angle on every post — do not repeat the same core message within 7 days
- Avoid these overused openers: "Most", "Many", "As a", "If you're a", "Running a small business"
- Each post must make ONE specific point — not a general observation about small business
- Use concrete specifics — a season, a day of week, a specific service, a local reference — rather than broad statements
- If the previous post was educational, this one should be promotional or behind-the-scenes, and vice versa
${lastContentType ? `\nThe last post for this channel was: ${lastContentType}. This post must feel distinct from that — different angle, different opening, different structure.` : ''}

Return only the blog post text with markdown formatting.`;
}

function buildLinkedInPrompt(company, contentType, verticalCtx, lastContentType) {
  const isPulseforge = company.name.toLowerCase().includes('pulseforge');
  const location = company.location || 'Manchester, NH';

  const audienceNote = isPulseforge
    ? `IMPORTANT: Pulseforge's audience is small business owners considering marketing automation. Write to an owner who is tired of doing repetitive marketing tasks and wants a system that runs in the background. Speak to pain points: time wasted on marketing, inconsistent social presence, missed leads.`
    : `Write as the brand voice of ${company.name} — use "we" and "${company.name}", not "I". Speak to local customers and business owners in ${location}.`;

  return `You are writing a LinkedIn page post for ${company.name}, a local ${company.industry || 'business'} in ${location}${isPulseforge ? ' that automates marketing and outreach for small business owners using AI' : ''}.

Content type: ${contentType}
Business context: ${verticalCtx}

${audienceNote}

Write a LinkedIn post (150-250 words) with this structure:
- First line: a specific hook — no fluff openers like "excited to share" or "we're thrilled"
- 2-3 short paragraphs or punchy line breaks
- Final line: a question or soft CTA to drive comments
- Last line: 3-5 relevant hashtags

Voice and tone:
- Brand voice — use "we" and "${company.name}", not "I"
- Confident local expert, never corporate, never salesy
- Mention Manchester NH or New Hampshire naturally where it fits — don't force it
- NO URLs in the post text
- For "educational": one specific insight or actionable tip — not generic advice
- For "behind-the-scenes": a genuine look at how ${isPulseforge ? 'the automation system works or how we build it' : `${company.name} operates day to day`}
- For "results": a concrete outcome — time saved, leads generated, or a client win (keep clients anonymous)
- For "community": connect to the local small business ecosystem in New Hampshire
- Never use dashes or hyphens in any content you write — not as punctuation, not as separators, not in any context.

No buzzwords. No "we're excited to announce." Write like a knowledgeable local operator who has been in the trenches.

VARIETY RULES — enforce these on every post:
- Never start a post with "Most small business owners" or any variation of that phrase
- Never start consecutive posts with the same opening word or phrase
- Rotate the angle on every post — do not repeat the same core message within 7 days
- Avoid these overused openers: "Most", "Many", "As a", "If you're a", "Running a small business"
- Each post must make ONE specific point — not a general observation about small business
- Use concrete specifics — a season, a day of week, a specific service, a local reference — rather than broad statements
- If the previous post was educational, this one should be promotional or behind-the-scenes, and vice versa
${lastContentType ? `\nThe last post for this channel was: ${lastContentType}. This post must feel distinct from that — different angle, different opening, different structure.` : ''}

Return only the post text.`;
}

async function getLastContentType(companyName, channel) {
  const res = await pool.query(`
    SELECT post_content FROM pending_comments
    WHERE author_name = $1 AND channel = $2
    ORDER BY created_at DESC LIMIT 1
  `, [companyName, channel]);
  if (!res.rows.length) return null;
  const parts = res.rows[0].post_content?.split('·');
  return parts?.length > 1 ? parts[1].trim().toLowerCase() : null;
}

async function generatePost(company, contentType, channel) {
  const verticalCtx = getVerticalContext(company.industry);
  const location = company.location || 'Manchester, NH';
  const lastContentType = await getLastContentType(company.name, channel);

  if (lastContentType) {
    console.log(`  [variety] Last ${channel} post type: ${lastContentType} → generating: ${contentType}`);
  }

  const prompt = channel === 'google_business'
    ? buildGooglePrompt(company, contentType, verticalCtx, location, lastContentType)
    : channel === 'blog'
      ? buildBlogPrompt(company, contentType, verticalCtx, lastContentType)
      : channel === 'linkedin_page'
        ? buildLinkedInPrompt(company, contentType, verticalCtx, lastContentType)
        : buildFacebookPrompt(company, contentType, verticalCtx, location, lastContentType);

  const message = await client.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: channel === 'blog' ? 800 : channel === 'linkedin_page' ? 450 : 300,
    messages: [{ role: 'user', content: prompt }]
  });

  return message.content[0].text.trim();
}

async function saveToPendingApprovals(company, content, contentType, channel) {
  const channelLabel = {
    facebook_page:   'Facebook Page',
    google_business: 'Google Business',
    blog:            'Blog',
    linkedin_page:   'LinkedIn Page',
  }[channel] || channel;
  const label = `${channelLabel} · ${contentType.charAt(0).toUpperCase() + contentType.slice(1)}`;

  // LinkedIn Page posts carry a first-comment URL posted via Buffer after approval
  const storedContent = channel === 'linkedin_page'
    ? `POST: ${content}\nFIRST_COMMENT: https://gopulseforge.com`
    : content;

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
    storedContent,
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
        const contentType = await pickContentType(company.name, channel, company.id);
        console.log(`${company.name} — ${channel} — ${contentType}`);

        try {
          const content = await generatePost(company, contentType, channel);
          const id = await saveToPendingApprovals(company, content, contentType, channel);
          if (id) {
            console.log(`  ✓ queued (${id.slice(0, 8)})\n`);
            generated++;
            const channelLabel = { facebook_page: 'Facebook Page', google_business: 'Google Business', blog: 'Blog', linkedin_page: 'LinkedIn Page' }[channel] || channel;
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
