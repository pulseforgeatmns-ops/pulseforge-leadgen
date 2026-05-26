require('dotenv').config();
const pool = require('./db');
const Anthropic = require('@anthropic-ai/sdk');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');

const client = new Anthropic();
const AGENT_NAME = 'paige';

const CONTENT_TYPES = ['promotional', 'educational', 'seasonal', 'behind-the-scenes', 'community'];
const BLOG_CONTENT_TYPES = ['educational', 'behind-the-scenes', 'community', 'seasonal'];
const LINKEDIN_CONTENT_TYPES = ['educational', 'behind-the-scenes', 'results', 'community'];
const CHANNELS = ['facebook_page', 'google_business', 'blog', 'linkedin_page'];
const MIN_QUALITY_SCORE = 24;
const MIN_HOOK_SCORE = 8;
const CLIENT_ID = getRuntimeClientId();
let CLIENT_CONFIG = null;

const PULSEFORGE_TOPIC_BANK = [
  {
    label: 'Behind the scenes',
    guidance: "what actually runs when a client's phone is quiet",
  },
  {
    label: 'Client result story',
    guidance: 'a specific, local, anonymized client result story',
  },
  {
    label: 'Common follow-up mistake',
    guidance: 'a common mistake local businesses make with follow-up',
  },
  {
    label: 'Tool and system explainer',
    guidance: 'what n8n, automation, or an AI workflow actually does in plain English',
  },
  {
    label: 'Contrarian take',
    guidance: "why more leads isn't always the answer",
  },
  {
    label: 'Human side',
    guidance: 'the bartender builds an AI agency story, grounded and practical',
  },
  {
    label: 'Before and after workflow',
    guidance: 'a workflow transformation from manual chaos to automatic follow-through',
  },
  {
    label: 'Industry-specific pain point',
    guidance: 'one pain point for a restaurant, cleaner, contractor, salon, gym, or local service owner',
  },
  {
    label: 'AI agent FAQ',
    guidance: 'what an AI agent actually does all day',
  },
  {
    label: 'Local market observation',
    guidance: 'a Manchester NH small business observation',
  },
  {
    label: 'Cost of doing nothing',
    guidance: 'what no automation quietly costs a local business',
  },
  {
    label: 'Day in the life',
    guidance: 'a day in the life of an automated system',
  },
];

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

function uniqueList(values, limit = 12) {
  return [...new Set(values.filter(Boolean).map(v => String(v).trim()).filter(Boolean))].slice(0, limit);
}

function firstWords(text, count = 8) {
  return String(text || '')
    .replace(/^POST:\s*/i, '')
    .replace(/[#*_`]/g, '')
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, count)
    .join(' ');
}

function cleanPostText(text) {
  return String(text || '')
    .replace(/^POST:\s*/i, '')
    .replace(/\nFIRST_COMMENT:[\s\S]*$/i, '')
    .replace(/[#*_`]/g, '')
    .trim();
}

function extractCoreAngle(text) {
  const cleaned = cleanPostText(text);
  const firstLine = cleaned
    .split(/\n+/)
    .map(line => line.trim())
    .find(Boolean) || '';
  const hook = firstLine || firstWords(cleaned, 14);
  return hook
    .replace(/\s+/g, ' ')
    .replace(/[.!?]+$/, '')
    .slice(0, 160)
    .trim();
}

function getPulseforgeTopicAngle(date = new Date()) {
  const day = Number(new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    day: 'numeric',
  }).format(date));
  return PULSEFORGE_TOPIC_BANK[(day - 1) % PULSEFORGE_TOPIC_BANK.length];
}

function formatUsedAngles(angles) {
  return angles?.length
    ? angles.map(angle => `- ${angle}`).join('\n')
    : '- none found in the last 14 days';
}

async function getRecentPublishedAngles() {
  const res = await pool.query(`
    SELECT
      al.payload,
      al.ran_at,
      pc.comment,
      pc.post_content
    FROM agent_log al
    LEFT JOIN pending_comments pc
      ON pc.id::text = al.payload->>'id'
      AND pc.client_id = al.client_id
    WHERE al.client_id = $1
      AND al.ran_at >= NOW() - INTERVAL '14 days'
      AND (
        al.action IN ('post_published', 'published_post', 'publish_post', 'content_published', 'blog_published')
        OR al.action ILIKE '%publish%'
      )
      AND al.status IN ('success', 'completed', 'posted')
    ORDER BY al.ran_at DESC
    LIMIT 40
  `, [CLIENT_ID]);

  return uniqueList(res.rows.map(row => {
    const payload = parseLogPayload(row.payload);
    return extractCoreAngle(
      row.comment ||
      payload.post ||
      payload.content ||
      payload.post_content ||
      payload.comment ||
      row.post_content
    );
  }), 20);
}

function extractRecentThemes(rows) {
  const seasonalTerms = ['summer', 'winter', 'spring', 'fall', 'autumn', 'holiday', 'holidays', 'christmas', 'thanksgiving', 'new year', 'memorial day', 'labor day'];
  const locationTerms = ['manchester', 'nh', 'new hampshire', 'local'];
  const serviceTerms = ['cleaning', 'cleaners', 'salon', 'restaurant', 'fitness', 'gym', 'landscaping', 'lawn', 'plumbing', 'hvac', 'auto', 'marketing', 'automation'];
  const patternTerms = [
    'did you know',
    'are you looking',
    'as a local business owner',
    'most small business owners',
    'many business owners',
    'if you are',
    'if you’re',
    'running a small business',
    'when it comes to',
    'in today',
    'we know',
    'we understand'
  ];

  const texts = rows.map(row => row.comment || row.post_content || '').filter(Boolean);
  const lowerTexts = texts.map(text => text.toLowerCase());

  return {
    seasonal: uniqueList(seasonalTerms.filter(term => lowerTexts.some(text => text.includes(term)))),
    location: uniqueList(locationTerms.filter(term => lowerTexts.some(text => text.includes(term)))),
    openings: uniqueList(texts.map(text => firstWords(text)), 30),
    services: uniqueList(serviceTerms.filter(term => lowerTexts.some(text => text.includes(term)))),
    patterns: uniqueList(patternTerms.filter(term => lowerTexts.some(text => text.includes(term)))),
  };
}

async function getRecentThemes(channel) {
  const res = await pool.query(`
    SELECT post_content, comment, created_at
    FROM pending_comments
    WHERE channel = $1
      AND client_id = $2
    ORDER BY created_at DESC
    LIMIT 30
  `, [channel, CLIENT_ID]);
  return extractRecentThemes(res.rows);
}

function formatThemeList(values) {
  return values?.length ? values.join('; ') : 'none found';
}

function buildContentRules(recentThemes, recentPublishedAngles = [], topicAngle = null) {
  const topicBlock = topicAngle ? `
TODAY'S PULSEFORGE TOPIC BUCKET:
- ${topicAngle.label}: ${topicAngle.guidance}
- If writing for Pulseforge, make this the core angle for today's posts. Do not drift into a different bucket unless the channel absolutely requires it.` : '';

  return `CONTENT RULES — YOU MUST FOLLOW THESE:
- Do NOT use any seasonal reference (summer, winter, spring, fall, holidays) as the hook or opening angle
- Do NOT open with a location name (Manchester, NH, New Hampshire, local)
- Do NOT reuse any of these recent opening phrases: ${formatThemeList(recentThemes.openings)}
- Do NOT use these structural patterns: ${formatThemeList(recentThemes.patterns)}
- Avoid repeating these recent service angles unless the post has a clearly different hook: ${formatThemeList(recentThemes.services)}
- Lead with a specific result, outcome, stat, or question that creates genuine curiosity — not a seasonal or geographic observation
- Every post must have a concrete hook in the first line that works without knowing the location or time of year
- Write in a natural, conversational tone. Always use contractions — it's, don't, that's, you're, we're, isn't, hasn't, won't, can't. Never write "it is", "do not", "that is", "you are", "we are" when a contraction would sound more natural. The content should sound like a sharp business owner wrote it, not a marketing agency.
${topicBlock}

USED-ANGLES MEMORY — DO NOT REUSE:
These hooks or angles appeared in published posts from the last 14 days:
${formatUsedAngles(recentPublishedAngles)}

If any of these ideas appear above, do not use competitor comparison hooks, do not open with statistics about lead response rates, and do not use "your competitor just", "40% of leads", or "follow-up speed" angles. Even if they are not listed, avoid those angles for Pulseforge unless a human explicitly asks for them.

HOOK WRITING — NON-NEGOTIABLE:
Every post must open with a strong hook in the first line. Strong hooks include: a counterintuitive statement, a specific operational detail, a direct question that creates curiosity, or a bold claim. Never start a post with "I", "We", "At [company]", or a generic greeting. The first line must stop the scroll. Examples of weak hooks: "We help local businesses grow." Examples of strong hooks: "The quietest part of a local business is usually where the leak is." or "A booking request should not depend on whether the owner checked their inbox at the right minute."

ORIGINALITY — NON-NEGOTIABLE:
Every piece of content must feel like it was written for the first time. Never reuse the same opening premise, stat, or scenario you've used before. If you catch yourself writing something that sounds like something Pulseforge has said before, stop and pick a different angle from today's topic bucket.

CTA — NON-NEGOTIABLE:
Every post must end with exactly one of these CTAs — rotate them, never use the same one twice in a row: (1) 'Reply and I'll show you what this looks like for [business type] in Manchester.' (2) 'Drop a comment — what's the one part of your follow-up process you wish ran itself?' (3) 'DM us and we'll put together a free mockup for your business this week.' (4) 'Curious what this would cost for a business your size? Reply and I'll break it down.' Never end with a generic 'reach out' or 'contact us' line.`;
}

async function getActiveClients() {
  const res = await pool.query(`
    SELECT id, name, industry, location, website, notes
    FROM companies
    WHERE name IS NOT NULL AND name != ''
      AND client_id = $1
    ORDER BY created_at DESC
    LIMIT 20
  `, [CLIENT_ID]);
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
      AND client_id = $${channel ? 3 : 2}
      AND created_at > NOW() - INTERVAL '60 days'
    ORDER BY created_at DESC
    LIMIT 20
  `, [...params, CLIENT_ID]);

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
    WHERE author_name = $1 AND channel = $2 AND client_id = $3
    ORDER BY created_at DESC LIMIT 1
  `, [companyName, channel, CLIENT_ID]);
  if (!res.rows.length) return null;
  const parts = res.rows[0].post_content?.split('·');
  return parts?.length > 1 ? parts[1].trim().toLowerCase() : null;
}

async function createDraft(prompt, systemPrompt, channel) {
  const message = await client.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: channel === 'blog' ? 800 : channel === 'linkedin_page' ? 450 : 300,
    system: systemPrompt,
    messages: [{ role: 'user', content: prompt }]
  });

  return message.content[0].text.trim();
}

function parseScoreJson(text) {
  const raw = String(text || '').trim();
  // Strip ```json / ``` code fences if present, then locate the JSON object.
  const defenced = raw.replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```\s*$/i, '').trim();
  const match = defenced.match(/\{[\s\S]*\}/);
  if (!match) throw new Error('No JSON object returned from quality scoring');

  // Prefer strict JSON (the prompt now demands double quotes). Only if that
  // fails do we attempt the single→double quote swap as a last-resort
  // recovery. The blanket replace can corrupt valid JSON when a string value
  // contains an apostrophe, so it must be the fallback, not the primary path.
  let parsed;
  try {
    parsed = JSON.parse(match[0]);
  } catch (_) {
    parsed = JSON.parse(match[0].replace(/'/g, '"'));
  }

  const specificity = Number(parsed.specificity || 0);
  const originality = Number(parsed.originality || 0);
  const hookStrength = Number(parsed.hook_strength || 0);
  return {
    specificity,
    originality,
    hook_strength: hookStrength,
    total: Number(parsed.total || specificity + originality + hookStrength),
    weak_dimension: parsed.weak_dimension || 'none',
    reason: parsed.reason || '',
  };
}

async function scoreDraft(draft, recentPublishedAngles = []) {
  const message = await client.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 220,
    messages: [{
      role: 'user',
      content: `Score this social media post on three dimensions (1-10 each):

1. SPECIFICITY — Does it mention a concrete result, number, outcome,
   or specific scenario? (10 = very specific, 1 = vague platitudes)

2. ORIGINALITY — Does it avoid clichés, seasonal hooks, and location
   name-drops as the main angle? (10 = fresh angle, 1 = recycled formula)
   Use this recent 14-day published angle list as the main originality reference:
${formatUsedAngles(recentPublishedAngles)}
   Originality scores below 7 should only occur if the post reuses a hook
   or angle seen in the last 14 days. A fresh angle on a familiar topic can
   still score 8+.

3. HOOK STRENGTH — Does the opening line give someone a reason to stop
   scrolling and keep reading? (10 = compelling, 1 = forgettable)

Return ONLY a strict JSON object with double-quoted keys and string values.
Do not wrap in code fences. Do not include any prose before or after.
Use this exact shape:
{ "specificity": X, "originality": X, "hook_strength": X, "total": X,
  "weak_dimension": "specificity|originality|hook_strength|none",
  "reason": "one sentence explanation of the lowest score (avoid quotation marks inside)" }

Post to score:
${draft}`
    }]
  });

  return parseScoreJson(message.content[0].text);
}

async function generatePost(company, contentType, channel) {
  const verticalCtx = getVerticalContext(company.industry);
  const location = company.location || 'Manchester, NH';
  const lastContentType = await getLastContentType(company.name, channel);
  const recentThemes = await getRecentThemes(channel);
  const isPulseforge = company.name.toLowerCase().includes('pulseforge');
  const recentPublishedAngles = await getRecentPublishedAngles();
  const topicAngle = isPulseforge ? getPulseforgeTopicAngle() : null;
  const clientContext = CLIENT_ID === 2 ? `

MSHI CLIENT CONTEXT:
- Client: Mountain State Home Innovations, locally owned WV contractor run by Brad and Dustin
- Tone: energetic, professional, personable, not corporate
- Themes: ${CLIENT_CONFIG?.paige_themes || 'seasonal exterior tips, project spotlights, before/after features, technical tips, emergency repair awareness'}
- Lead with specific tips, outcomes, project scenarios, communication, reliability, and owner-done work
- Reference WV locations naturally only when relevant
- Never attack competitors or mention negative customer experiences
- License WV065578 may be used in trust-building posts` : '';
  const systemPrompt = `${buildContentRules(recentThemes, recentPublishedAngles, topicAngle)}${clientContext}`;

  if (lastContentType) {
    console.log(`  [variety] Last ${channel} post type: ${lastContentType} → generating: ${contentType}`);
  }
  console.log(`  [themes] Recent openings checked: ${recentThemes.openings.length}; patterns: ${recentThemes.patterns.length}`);
  if (isPulseforge && topicAngle) {
    console.log(`  [topic] ${topicAngle.label}`);
  }
  console.log(`  [memory] Recent published angles checked: ${recentPublishedAngles.length}`);

  const prompt = channel === 'google_business'
    ? buildGooglePrompt(company, contentType, verticalCtx, location, lastContentType)
    : channel === 'blog'
      ? buildBlogPrompt(company, contentType, verticalCtx, lastContentType)
      : channel === 'linkedin_page'
        ? buildLinkedInPrompt(company, contentType, verticalCtx, lastContentType)
        : buildFacebookPrompt(company, contentType, verticalCtx, location, lastContentType);

  const firstDraft = await createDraft(prompt, systemPrompt, channel);
  const firstScore = await scoreDraft(firstDraft, recentPublishedAngles);
  let finalDraft = firstDraft;
  let finalScore = firstScore;
  let regenerated = false;

  if (firstScore.total < MIN_QUALITY_SCORE || firstScore.hook_strength < MIN_HOOK_SCORE) {
    regenerated = true;
    console.log(`  [quality] Score ${firstScore.total}/30, regenerating for ${firstScore.weak_dimension}`);
    const regenPrompt = `${prompt}

Your previous draft scored ${firstScore.hook_strength}/10 on hook strength. Reason: ${firstScore.reason}.

A strong hook is the ONLY thing that matters in the first line. Here are examples of strong vs weak:

WEAK: "We help local businesses stay on top of their marketing."
WEAK: "Running a small business is tough, especially when it comes to marketing."
WEAK: "Most business owners don't realize how much time they spend on marketing."

STRONG: "A cleaning company owner can miss three booking requests before lunch without ever noticing the pattern."
STRONG: "The phone going quiet is not always a demand problem."
STRONG: "One tiny handoff between inbox and calendar can decide whether a lead turns into revenue."

The first line must create a reason to keep reading. It should be specific, surprising, or create a gap the reader wants to close. No generic observations. No "most business owners." No "running a small business."
Do not use competitor comparison hooks, lead response rate statistics, "your competitor just", "40% of leads", or "follow-up speed" angles. Use today's topic bucket instead: ${topicAngle ? `${topicAngle.label}: ${topicAngle.guidance}` : 'the most distinct available angle'}.

Rewrite the post with a completely different opening line that hits one of these patterns:
- A specific operational detail ("the missed handoff between inbox and calendar")
- A quiet business problem ("the phone is quiet but the system is still working")
- A counterintuitive claim ("the businesses winning aren't spending more on ads")
- A direct question ("which part of the follow-up process should not depend on you?")

Return only the rewritten post text.`;
    const secondDraft = await createDraft(regenPrompt, systemPrompt, channel);
    const secondScore = await scoreDraft(secondDraft, recentPublishedAngles);
    if (secondScore.total > firstScore.total) {
      finalDraft = secondDraft;
      finalScore = secondScore;
    }
  } else {
    console.log(`  [quality] Score ${firstScore.total}/30`);
  }

  return { content: finalDraft, quality: finalScore, regenerated };
}

async function logQualityScore(channel, quality, regenerated, post) {
  const payload = {
    channel,
    scores: {
      specificity: quality.specificity,
      originality: quality.originality,
      hook_strength: quality.hook_strength,
      total: quality.total,
    },
    specificity: quality.specificity,
    originality: quality.originality,
    hook_strength: quality.hook_strength,
    total: quality.total,
    regenerated,
    weak_dimension: quality.weak_dimension === 'none' ? null : quality.weak_dimension,
    post_preview: String(post || '').slice(0, 80),
  };
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
    VALUES ($1, $2, $3, $4, NOW(), $5)
  `, [
    AGENT_NAME,
    'content_scored',
    JSON.stringify(payload),
    'success',
    CLIENT_ID,
  ]);
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
      AND client_id = $4
    LIMIT 1
  `, [channel, label, company.name, CLIENT_ID]);

  if (existing.rows.length > 0) {
    console.log(`  ↷ Skipping duplicate: ${channel} · ${contentType} for ${company.name}`);
    return null;
  }

  const res = await pool.query(`
    INSERT INTO pending_comments
      (author_name, author_title, post_content, comment, post_url, channel, status, client_id)
    VALUES ($1, $2, $3, $4, NULL, $5, 'pending', $6)
    RETURNING id
  `, [
    company.name,
    company.industry || 'Local Business',
    label,
    storedContent,
    channel,
    CLIENT_ID
  ]);

  return res.rows[0].id;
}

async function logRun(status, payload) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
    VALUES ($1, $2, $3, $4, NOW(), $5)
  `, [AGENT_NAME, 'generate_content', JSON.stringify(payload), status, CLIENT_ID]);
}

function parseLogPayload(payload) {
  if (!payload) return {};
  if (typeof payload !== 'string') return payload;
  try {
    return JSON.parse(payload);
  } catch (_) {
    return {};
  }
}

// agent_log_status_check only allows: success | failed | skipped | pending.
// Default to 'success' (the codebase convention for "done OK") so this UPDATE
// never throws and bypasses the main channel loop in run().
async function completeRegenerateTrigger(triggerId, status = 'success') {
  await pool.query(
    `UPDATE agent_log SET status = $1 WHERE id = $2 AND agent_name = 'max' AND action = 'paige_regenerate_trigger'`,
    [status, triggerId]
  );
}

// Max quality-gate triggers — run before normal generation
async function processRegenerateTriggers() {
  const triggers = await pool.query(`
    SELECT id, payload
    FROM agent_log
    WHERE agent_name = 'max'
      AND action = 'paige_regenerate_trigger'
      AND status = 'pending'
      AND client_id = $1
      AND ran_at >= NOW() - INTERVAL '24 hours'
    ORDER BY ran_at ASC
  `, [CLIENT_ID]);

  if (!triggers.rows.length) return { triggers: 0, regenerated: 0 };

  const clients = await getActiveClients();
  if (!clients.length) {
    for (const row of triggers.rows) {
      await completeRegenerateTrigger(row.id, 'success');
    }
    return { triggers: triggers.rows.length, regenerated: 0 };
  }

  let regenerated = 0;
  console.log(`[Paige] Processing ${triggers.rows.length} Max regenerate trigger(s)...`);

  for (const row of triggers.rows) {
    const payload = parseLogPayload(row.payload);
    const channel = payload.channel;
    if (!channel || !CHANNELS.includes(channel)) {
      await completeRegenerateTrigger(row.id);
      continue;
    }

    for (const company of clients) {
      const contentType = await pickContentType(company.name, channel, company.id);
      console.log(`  [regenerate] ${company.name} — ${channel} — ${contentType}`);
      try {
        const postResult = await generatePost(company, contentType, channel);
        const id = await saveToPendingApprovals(company, postResult.content, contentType, channel);
        if (id) {
          await logQualityScore(channel, postResult.quality, true, postResult.content);
          console.log(`  ✓ regenerated (${id.slice(0, 8)})`);
          regenerated++;
        }
      } catch (err) {
        console.error(`  ✗ regenerate ${company.name}/${channel}: ${err.message}`);
        await logChannelError(company, channel, err);
      }
      await new Promise(r => setTimeout(r, 1500));
    }

    await completeRegenerateTrigger(row.id);
  }

  return { triggers: triggers.rows.length, regenerated };
}

// Records a per-channel generation failure to agent_log so we can query
// `SELECT * FROM agent_log WHERE action = 'generate_content_error'` and
// see exactly which channel/company blew up and why. Best-effort — never
// throws so it can't mask the original error.
async function logChannelError(company, channel, err) {
  const payload = {
    channel,
    company: company?.name || null,
    error: err?.message || String(err),
    stack_preview: String(err?.stack || '').split('\n').slice(0, 4).join('\n'),
  };
  try {
    await pool.query(`
      INSERT INTO agent_log (agent_name, action, payload, status, error_msg, ran_at, client_id)
      VALUES ($1, $2, $3, $4, $5, NOW(), $6)
    `, [AGENT_NAME, 'generate_content_error', JSON.stringify(payload), 'failed', payload.error, CLIENT_ID]);
  } catch (logErr) {
    console.error(`  [logChannelError] failed to write: ${logErr.message}`);
  }
}

async function run() {
  console.log('\nPaige agent running...\n');
  CLIENT_CONFIG = await getClientConfig(CLIENT_ID);
  if (!CLIENT_CONFIG) throw new Error(`Active client not found: ${CLIENT_ID}`);
  if (CLIENT_ID === 2 && !CLIENT_CONFIG.facebook_url) {
    console.log('MSHI Paige disabled until Facebook page is created and connected.');
    await logRun('success', { client_id: CLIENT_ID, skipped: 'facebook_page_missing' });
    return;
  }
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

    const regenerateResult = await processRegenerateTriggers();
    if (regenerateResult.triggers) {
      console.log(`[Paige] Regenerate pass: ${regenerateResult.regenerated} post(s) from ${regenerateResult.triggers} trigger(s)\n`);
    }

    if (!clients.length) {
      console.log('No clients in the companies table. Add companies to get started.');
      await logRun('success', {
        clients_processed: 0,
        posts_generated: 0,
        max_regenerate_triggers: regenerateResult.triggers,
        max_regenerated: regenerateResult.regenerated,
      });
      return;
    }

    let generated = 0;
    const channelsFailed = [];
    const channelsQueued = [];

    for (const company of clients) {
      for (const channel of CHANNELS) {
        const contentType = await pickContentType(company.name, channel, company.id);
        console.log(`${company.name} — ${channel} — ${contentType}`);

        try {
          const postResult = await generatePost(company, contentType, channel);
          const content = postResult.content;
          const id = await saveToPendingApprovals(company, content, contentType, channel);
          if (id) {
            await logQualityScore(channel, postResult.quality, postResult.regenerated, content);
            console.log(`  ✓ queued (${id.slice(0, 8)})\n`);
            generated++;
            channelsQueued.push(`${company.name}/${channel}`);
          }
        } catch (err) {
          console.error(`  ✗ ${company.name}/${channel}: ${err.message}`);
          channelsFailed.push(`${company.name}/${channel}`);
          await logChannelError(company, channel, err);
        }

        await new Promise(r => setTimeout(r, 1500));
      }
    }

    await logRun('success', {
      clients_processed: clients.length,
      posts_generated: generated,
      channels_queued: channelsQueued,
      channels_failed: channelsFailed,
      max_regenerate_triggers: regenerateResult.triggers,
      max_regenerated: regenerateResult.regenerated,
    });
    console.log(`\nPaige complete — ${generated} post${generated !== 1 ? 's' : ''} queued, ${channelsFailed.length} channel${channelsFailed.length !== 1 ? 's' : ''} failed.`);
    if (channelsFailed.length) console.log(`  Failed: ${channelsFailed.join(', ')}`);
  } catch (err) {
    console.error('Paige error:', err.message);
    await logRun('failed', { error: err.message }).catch(() => {});
  }
}

module.exports = { run };

if (require.main === module) {
  run().catch(err => {
    console.error('[Paige] Fatal error:', err.message);
    process.exit(1);
  });
}
