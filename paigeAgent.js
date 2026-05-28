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
const CLIENT_1_CHANNELS = ['facebook_page', 'google_business', 'linkedin_page', 'linkedin_personal', 'blog'];
const MSHI_CHANNELS = ['facebook_page', 'google_business', 'blog'];
const ALL_CHANNELS = [...new Set([...CHANNELS, ...CLIENT_1_CHANNELS, ...MSHI_CHANNELS])];
const MIN_QUALITY_SCORE = 24;
const MIN_DIMENSION_SCORE = 7;
const MIN_HOOK_SCORE = 7;
const MAX_REGENERATION_ATTEMPTS = 4;
const CLIENT_ID = getRuntimeClientId();
const CLEAR_PENDING_PULSEFORGE_APPROVALS = process.env.PAIGE_CLEAR_PENDING_PULSEFORGE !== '0';
let CLIENT_CONFIG = null;

const PULSEFORGE_POV_RULE = `PULSEFORGE POV — NON-NEGOTIABLE:
You are writing content FOR Pulseforge, an AI marketing and automation agency. Write from Pulseforge's point of view. Reference client verticals as examples of businesses we help — never write as if Pulseforge IS a lawn care company, auto shop, or any other business. Wrong: 'A client called us about her yard...' written as a lawn care company. Correct: 'A lawn care company in Southern NH came to us because...' written as Pulseforge telling the story.`;

const PROFESSIONAL_COPYWRITER_QUALITY_RULE = `PROFESSIONAL COPYWRITER QUALITY BAR — NON-NEGOTIABLE:
Write at a professional copywriter level. Every post must have:

- A specific, unexpected opening line that earns attention — not a statistic, not 'Most businesses...', not 'Did you know'
- A concrete detail that makes it feel real — a number, a timeline, a specific vertical, a location, a named outcome
- A clear point of view — Pulseforge has an opinion, not just information
- A closer that creates forward motion — not engagement bait, not a question for comments

Scoring targets: hook_strength 8+, specificity 8+, originality 8+. A score below 7 on any dimension means the post failed. Regenerate until all three dimensions hit 7 or above before passing to publish.`;

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

const BLOG_CLOSER_BANK = [
  "If any of this sounds familiar, visit https://gopulseforge.com/contact and we'll map out what your first automated sequence would look like.",
  "The businesses getting ahead right now aren't working harder. They built something that works without them. We build that.",
  'Want to see what this looks like for your specific business? We do free workflow audits for Manchester-area businesses.',
  "This is exactly what we set up for clients in the first 30 days. If you're curious what that looks like, reach out.",
  'Most of our clients are surprised how little it takes to get the first sequence live. Happy to walk you through it.',
  "We've built this for restaurants, cleaners, contractors, and salons across NH. The playbook is the same. The results aren't.",
  "If you're still following up manually, you're leaving jobs on the table. Let's fix that.",
  'The setup takes less than a week. The follow-up runs forever.',
];

const FACEBOOK_CTA_BANK = [
  "If this sounds familiar, we'll show you what the first sequence could look like.",
  'Send us a message and we can map the handoff that should be automated first.',
  'A simple workflow audit is usually enough to find the first fix.',
  'We can sketch the first automation for your business this week.',
  'The first step is usually smaller than owners think.',
  'If the manual follow-up is getting old, we can help tighten it up.',
  'We build the quiet systems that keep the next step moving.',
  'Reach out when you want the follow-up to stop living in your head.',
];

const MSHI_TOPIC_BANK = [
  { label: 'Before/after project reveal', guidance: 'show the visible change and what Brad and Dustin fixed for the homeowner' },
  { label: 'Seasonal maintenance tip', guidance: 'practical advice for decks, siding, gutters, and exterior upkeep in West Virginia weather' },
  { label: 'Why hire a local contractor vs national chain', guidance: 'explain the value of direct access, local accountability, and owner-done work without attacking competitors' },
  { label: 'Project spotlight: deck build', guidance: 'break down a deck build from walkthrough to finished outdoor space' },
  { label: 'Project spotlight: siding installation', guidance: 'explain what siding work protects and why details matter' },
  { label: 'Emergency repair story', guidance: 'tell a practical repair story focused on fast response, safety, and protecting the home' },
  { label: 'What to look for when hiring a contractor in WV', guidance: 'give homeowner-friendly checks like license, communication, references, and clear scope' },
  { label: 'Meet Brad and Dustin', guidance: 'introduce the owners as local tradespeople who personally stay involved in the work' },
  { label: 'Client testimonial highlight', guidance: 'turn a homeowner compliment into a specific proof point without sounding polished or corporate' },
  { label: 'How long does X project take?', guidance: 'explain realistic project timelines at a high level without quoting pricing or guarantees' },
  { label: 'WV weather and what it does to your home exterior', guidance: 'connect rain, humidity, storms, and seasonal swings to decks, siding, windows, and repairs' },
  { label: 'Free estimate — what to expect', guidance: 'walk homeowners through a no-pressure property walkthrough and direct conversation with Brad or Dustin' },
];

const MSHI_CTA_BANK = [
  'Call Brad or Dustin directly at 304-483-3655 for a free estimate.',
  "We serve Kanawha, Putnam, and Cabell County. Reach out and we'll come to you.",
  'Licensed, local, and we pick up the phone. 304-483-3655.',
  'Free walkthrough of your property — no obligation, no pressure.',
  'Every estimate is free. Every job is done by Brad and Dustin personally.',
];

const MSHI_SERVICE_AREAS = 'Kanawha, Putnam, and Cabell County';
const MSHI_CORE_SERVICES = 'decks, siding, exterior remodeling, windows, and emergency repair';

const CHANNEL_TOPIC_LENSES = {
  linkedin_page: {
    label: 'POV angle',
    guidance: 'Use the topic as a first-person industry observation or contrarian operator take. Build credibility. No overt sales pitch.',
  },
  linkedin_personal: {
    label: 'founder journey angle',
    guidance: 'Use the topic through Jacob Maynard personally: behind the scenes, bartending, the agency grind, what he built, or a specific client win. It must not use the same narrative arc as the Pulseforge LinkedIn page.',
  },
  google_business: {
    label: 'local proof angle',
    guidance: 'Use the topic to prove Pulseforge is legitimate, local to Manchester NH, and useful to someone who just found the business on Google.',
  },
  facebook_page: {
    label: 'client story angle',
    guidance: 'Use the topic as a relatable client scenario or human moment. Warm, conversational, story-first.',
  },
  blog: {
    label: 'full breakdown angle',
    guidance: 'Use the topic as a useful educational breakdown with practical steps, then position Pulseforge naturally as the solution.',
  },
};

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
  if (CLIENT_ID === 2) {
    return `local WV exterior contracting: ${MSHI_CORE_SERVICES}; locally owned, licensed WV065578, Brad and Dustin do the work themselves, direct owner access throughout the project`;
  }
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

function getPulseforgeTopicAngle(date = new Date(), channel = null) {
  const day = Number(new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    day: 'numeric',
  }).format(date));
  const offset = channel === 'linkedin_personal' ? 5 : 0;
  return PULSEFORGE_TOPIC_BANK[(day - 1 + offset) % PULSEFORGE_TOPIC_BANK.length];
}


function getMshiTopicAngle(date = new Date(), channel = null) {
  const day = Number(new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    day: 'numeric',
  }).format(date));
  const channelOffsets = { facebook_page: 0, google_business: 4, blog: 8 };
  const offset = channelOffsets[channel] || 0;
  return MSHI_TOPIC_BANK[(day - 1 + offset) % MSHI_TOPIC_BANK.length];
}

function seededIndex(seed, size) {
  let hash = 0;
  for (let i = 0; i < seed.length; i++) {
    hash = ((hash << 5) - hash) + seed.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash) % size;
}

function getBlogCloser(channel, date = new Date()) {
  const dateKey = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
  return BLOG_CLOSER_BANK[seededIndex(`${dateKey}:${channel}`, BLOG_CLOSER_BANK.length)];
}

function getFacebookCta(channel, date = new Date()) {
  const dateKey = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
  return FACEBOOK_CTA_BANK[seededIndex(`${dateKey}:${channel}`, FACEBOOK_CTA_BANK.length)];
}


function getMshiCta(channel, date = new Date()) {
  const dateKey = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
  return MSHI_CTA_BANK[seededIndex(`${dateKey}:${channel}`, MSHI_CTA_BANK.length)];
}

function buildChannelStrategyBlock(channel, topicAngle = null) {
  const lens = CHANNEL_TOPIC_LENSES[channel];
  const topicLine = topicAngle
    ? `Today's shared topic bucket is "${topicAngle.label}: ${topicAngle.guidance}".`
    : 'No Pulseforge topic bucket is active for this company.';

  if (!lens) return '';
  const exampleLine = topicAngle?.label === 'Behind the scenes'
    ? 'Example: if the bucket is behind the scenes about what runs while the owner sleeps, LinkedIn gets the POV angle, Google Business gets the local proof angle, Facebook gets the client story angle, and the blog gets the full breakdown.'
    : '';
  return `CHANNEL STRATEGY — DO NOT BLEND FORMATS:
${topicLine}
This channel must use the ${lens.label}: ${lens.guidance}
Do not reuse the same core angle or narrative arc across channels on the same day. Same topic bucket, different channel lens.
${exampleLine}`;
}


function buildMshiChannelStrategyBlock(channel, topicAngle) {
  const strategies = {
    facebook_page: { format: 'Project stories, before/after, community presence', tone: 'Warm, personal, local', length: '100-150 words', guidance: 'Use the topic as a homeowner-facing story from Brad and Dustin. It should feel like a real update from a local contractor, not an ad.' },
    google_business: { format: 'Search-intent, local proof', tone: 'Short, specific to Charleston WV', length: '75-100 words', guidance: 'Use the topic to help someone searching for a local WV contractor understand what MSHI does and why they can trust Brad and Dustin.' },
    blog: { format: 'Project breakdowns, homeowner tips, seasonal advice', tone: 'Educational, practical', length: '400-600 words', guidance: 'Use the topic as a useful article for homeowners, property managers, or HOAs deciding how to handle exterior work.' },
  };
  const strategy = strategies[channel];
  if (!strategy) return '';

  return `MSHI CHANNEL STRATEGY — DO NOT BLEND FORMATS:
Today's MSHI topic bucket is "${topicAngle.label}: ${topicAngle.guidance}".
Channel: ${channel}
Format: ${strategy.format}
Tone: ${strategy.tone}
Length: ${strategy.length}
Execution: ${strategy.guidance}
Do not reuse the same core angle or narrative arc across channels on the same day. Same topic bucket, different channel lens.`;
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

function buildContentRules(recentThemes, recentPublishedAngles = [], topicAngle = null, isPulseforge = false) {
  const topicBlock = topicAngle ? `
TODAY'S PULSEFORGE TOPIC BUCKET:
- ${topicAngle.label}: ${topicAngle.guidance}
- If writing for Pulseforge, make this the core angle for today's posts. Do not drift into a different bucket unless the channel absolutely requires it.` : '';
  const pulseforgeRules = isPulseforge ? `
${PULSEFORGE_POV_RULE}

${PROFESSIONAL_COPYWRITER_QUALITY_RULE}
` : '';

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
${pulseforgeRules}

USED-ANGLES MEMORY — DO NOT REUSE:
These hooks or angles appeared in published posts from the last 14 days:
${formatUsedAngles(recentPublishedAngles)}

If any of these ideas appear above, do not use competitor comparison hooks, do not open with statistics about lead response rates, and do not use "your competitor just", "40% of leads", or "follow-up speed" angles. Even if they are not listed, avoid those angles for Pulseforge unless a human explicitly asks for them.

HOOK WRITING — NON-NEGOTIABLE:
Every post must open with a strong hook in the first line. Strong hooks include: a counterintuitive statement, a specific operational detail, a direct question that creates curiosity, or a bold claim. Never start a post with "I", "We", "At [company]", or a generic greeting. The first line must stop the scroll. Examples of weak hooks: "We help local businesses grow." Examples of strong hooks: "The quietest part of a local business is usually where the leak is." or "A booking request should not depend on whether the owner checked their inbox at the right minute."

ORIGINALITY — NON-NEGOTIABLE:
Every piece of content must feel like it was written for the first time. Never reuse the same opening premise, stat, or scenario you've used before. If you catch yourself writing something that sounds like something Pulseforge has said before, stop and pick a different angle from today's topic bucket.

CONCRETE ANCHOR — NON-NEGOTIABLE:
Every post must include at least ONE of the following concrete anchors:

A specific number or metric (response time, percentage, dollar amount, number of leads)
A named outcome ('booked 3 jobs', 'cut follow-up time by 80%', 'recovered 12 dead leads')
A specific timeline ('in the first 30 days', 'within 90 seconds', 'before the owner woke up')

Without a concrete anchor the post will score below 24. Generic claims like 'businesses save time' or 'leads don't go cold' are not enough. Always ground the post in something specific and real.`;
}


function buildMshiContentRules(recentThemes, recentPublishedAngles = [], topicAngle = null) {
  const topicBlock = topicAngle ? `
TODAY'S MSHI TOPIC BUCKET:
- ${topicAngle.label}: ${topicAngle.guidance}
- Make this the core angle for today's content. Keep the same topic bucket but change the channel lens.` : '';

  return `CONTENT RULES — YOU MUST FOLLOW THESE:
- Write as Brad and Dustin from Mountain State Home Innovations, not as a marketing agency and not as a polished brand voice
- Voice: local, personal, West Virginia proud, and plain spoken. These are tradespeople, not marketers
- Audience: homeowners, property managers, and HOAs in ${MSHI_SERVICE_AREAS}
- Reference real service areas naturally: Kanawha County, Putnam County, Cabell County, Charleston WV, Huntington WV, Hurricane WV, and nearby communities
- Highlight trust points when they fit: locally owned, licensed WV065578, Brad and Dustin do the work themselves, and customers have direct access throughout the project
- Core services to feature: ${MSHI_CORE_SERVICES}
- Do NOT generate LinkedIn content for MSHI
- Never mention Pulseforge, AI, automation, or a marketing agency
- Never include pricing specifics, dollar amounts, percentages, or package prices
- Never make negative references to competitors. If comparing local vs national chains, keep it positive and focused on accountability and direct access
- Do NOT reuse any of these recent opening phrases: ${formatThemeList(recentThemes.openings)}
- Do NOT use these structural patterns: ${formatThemeList(recentThemes.patterns)}
- Avoid repeating these recent service angles unless the post has a clearly different hook: ${formatThemeList(recentThemes.services)}
- Lead with a concrete project detail, homeowner concern, practical tip, or local proof point
- Every post must have a clear first line that makes a homeowner want to keep reading
- Use contractions naturally. Keep the language plain and direct
${topicBlock}

USED-ANGLES MEMORY — DO NOT REUSE:
These hooks or angles appeared in published posts from the last 14 days:
${formatUsedAngles(recentPublishedAngles)}

HOOK WRITING — NON-NEGOTIABLE:
Every post must open with a strong hook in the first line. Strong hooks include a visible project detail, a homeowner problem, a practical warning, or a direct question. Never start with "At Mountain State Home Innovations," "We're excited," "Spring is here," or generic contractor language.

ORIGINALITY — NON-NEGOTIABLE:
Every piece of content must feel specific to Brad, Dustin, the homeowner, and the job. If the draft sounds like any contractor in any state could have written it, rewrite it with a real WV service area, service, or job detail.`;
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

function isPulseforgeCompany(company) {
  return String(company?.name || '').toLowerCase().includes('pulseforge');
}

function getPulseforgeCompanyFromConfig() {
  if (CLIENT_ID !== 1 || !CLIENT_CONFIG) return null;
  const cityState = [CLIENT_CONFIG.city, CLIENT_CONFIG.state].filter(Boolean).join(', ');
  return {
    id: null,
    name: CLIENT_CONFIG.business_name || CLIENT_CONFIG.name || 'Pulseforge',
    industry: CLIENT_CONFIG.vertical || 'AI marketing and automation agency',
    location: cityState || 'Manchester, NH',
    website: CLIENT_CONFIG.website || 'https://gopulseforge.com',
    notes: CLIENT_CONFIG.differentiators || CLIENT_CONFIG.brand_voice || null,
  };
}

function getGenerationClients(companies) {
  if (CLIENT_ID !== 1) return companies;
  const pulseforgeCompanies = companies.filter(isPulseforgeCompany);
  if (pulseforgeCompanies.length) return pulseforgeCompanies.slice(0, 1);
  const pulseforgeCompany = getPulseforgeCompanyFromConfig();
  return pulseforgeCompany ? [pulseforgeCompany] : [];
}

function getChannelsForCompany(company) {
  if (CLIENT_ID === 2) return MSHI_CHANNELS;
  if (CLIENT_ID !== 1 || !isPulseforgeCompany(company)) return CHANNELS;
  return CLIENT_1_CHANNELS;
}

async function rejectPendingPulseforgeApprovals(companies) {
  if (CLIENT_ID !== 1 || !CLEAR_PENDING_PULSEFORGE_APPROVALS) return 0;

  const pulseforgeNames = getGenerationClients(companies).map(company => company.name);
  if (!pulseforgeNames.length) return 0;

  const res = await pool.query(`
    UPDATE pending_comments
    SET status = 'rejected'
    WHERE client_id = $1
      AND status = 'pending'
      AND author_name = ANY($2)
      AND channel = ANY($3)
  `, [CLIENT_ID, pulseforgeNames, CLIENT_1_CHANNELS]);

  const rejected = res.rowCount || 0;
  if (rejected) {
    await pool.query(`
      INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
      VALUES ($1, $2, $3, $4, NOW(), $5)
    `, [
      AGENT_NAME,
      'pulseforge_pending_approvals_cleared',
      JSON.stringify({ rejected, channels: CLIENT_1_CHANNELS, author_names: pulseforgeNames }),
      'success',
      CLIENT_ID,
    ]);
  }
  return rejected;
}

// Pick a content type — uses performance weighting when ≥4 posts have data, else round-robin
async function pickContentType(companyName, channel = null, companyId = null) {
  const types = channel === 'blog'
    ? BLOG_CONTENT_TYPES
    : channel === 'linkedin_page' || channel === 'linkedin_personal'
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

function buildFacebookPrompt(company, contentType, verticalCtx, location, lastContentType, channelStrategy, facebookCta) {
  const isPulseforge = company.name.toLowerCase().includes('pulseforge');

  const audienceNote = isPulseforge
    ? `IMPORTANT: Pulseforge's audience is small business owners considering marketing automation — not end customers of another business. Write directly to an owner who is tired of doing repetitive marketing tasks themselves and wants a system that runs in the background.`
    : `Write as the actual owner of ${company.name} talking to their local community in ${location}.`;

  return `You are writing a Facebook post for ${company.name}'s business page.
${company.name} is a local ${company.industry || 'business'} in ${location}${isPulseforge ? ' that automates marketing and outreach for small business owners using AI' : ` that uses Pulseforge — an AI system that automates their marketing so the owner can focus on the actual work`}.

Content type: ${contentType}
Business context: ${verticalCtx}

${audienceNote}
${channelStrategy}

FACEBOOK STRATEGY:
- Purpose: story-first, conversational, human content that makes the business feel real
- Format: 100-175 words, warm tone, short paragraphs
- Lead with a relatable moment, specific client scenario, or small business owner situation
- Make it feel like a human story, not a thought leadership post and not a search listing
- If today's topic appears on other channels, Facebook must use the client story angle only
- End with this CTA or a natural close variation: "${facebookCta}"
- Sounds like a real person talking, not a brand account — no "excited to announce," no "we pride ourselves"
- Is specific to ${location} — reference the area naturally
- For "promotional": leads with a concrete result or offer, ends with the CTA above
- For "educational": teaches through a short scenario, not an abstract tip list
- For "seasonal": ties to what's actually happening in ${location} right now through a human moment
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

function buildGooglePrompt(company, contentType, verticalCtx, location, lastContentType, channelStrategy) {
  const isPulseforge = company.name.toLowerCase().includes('pulseforge');

  const audienceNote = isPulseforge
    ? `IMPORTANT: Pulseforge's audience is small business owners searching for ways to automate their marketing — not end customers of another business. Write to an owner who wants leads and visibility without doing the manual work themselves.`
    : `Write for a potential customer in ${location} who is searching for ${company.industry || 'this type of service'} and deciding whether to call.`;

  return `You are writing a Google Business Profile update for ${company.name}, a local ${company.industry || 'business'} in ${location}${isPulseforge ? ' that helps small business owners automate their marketing using AI' : ''}.

Content type: ${contentType}
Business context: ${verticalCtx}

${audienceNote}
${channelStrategy}

GOOGLE BUSINESS STRATEGY:
- Purpose: short, local, search-intent proof for someone who just found Pulseforge on Google and wants to know if we're legit
- Format: 75-100 words max, no hashtags, no emojis, no long story arc
- Tone: practical, clear, credible, locally grounded in Manchester NH
- Mention Manchester NH or the surrounding area naturally
- If today's topic appears on other channels, Google Business must use the local proof angle only
- End with a soft CTA, not a comment prompt
- Reads like something genuinely useful to someone searching "${company.industry || 'local services'} near me" — not promotional fluff
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

function buildBlogPrompt(company, contentType, verticalCtx, lastContentType, blogCloser, channelStrategy) {
  const isPulseforge = company.name.toLowerCase().includes('pulseforge');
  const location = company.location || 'Manchester, NH';

  const audienceNote = isPulseforge
    ? `IMPORTANT: Pulseforge's audience is small business owners considering marketing automation — not end customers of another business. Write to an owner who is tired of doing repetitive marketing tasks themselves and wants a system that runs in the background.`
    : `Write as the owner of ${company.name} — personal, grounded, and expert. This is their business blog, not a corporate content farm.`;

  return `You are writing a blog post for ${company.name}, a local ${company.industry || 'business'} in ${location}${isPulseforge ? ' that automates marketing and outreach for small business owners using AI' : ''}.

Content type: ${contentType}
Business context: ${verticalCtx}

${audienceNote}
${channelStrategy}

BLOG STRATEGY:
- Purpose: long-form educational content that teaches something useful and positions Pulseforge as the solution naturally
- Format: 400-600 words
- Tone: clear, useful, grounded, not salesy
- If today's topic appears on other channels, the blog must use the full breakdown angle only
- Build the reader's understanding before mentioning the offer

Write a blog post (400-600 words) using this structure:
# [Title]

[Intro paragraph — mention the business name and what they do, include ${location} naturally]

## [Subheading]
[Body section]

## [Subheading]
[Body section]

[Optional third ## section if it fits naturally — skip if it would feel padded]

[Closing paragraph using the required closer below]

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
- End the post with this exact closer, adjusted only if needed for grammar: "${blogCloser}"
- Never use engagement-bait questions as blog closers. Do not end with "drop a comment", "what do you think", "let us know below", or any request for comments.

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


function buildMshiFacebookPrompt(company, contentType, verticalCtx, location, lastContentType, channelStrategy, cta) {
  return `You are writing a Facebook post for ${company.name}'s business page.
${company.name} is Mountain State Home Innovations, a locally owned West Virginia contractor run by Brad and Dustin.

Content type: ${contentType}
Business context: ${verticalCtx}

${channelStrategy}

MSHI FACEBOOK STRATEGY:
- Channel purpose: primary local contractor channel for project stories, before/after photos, and community presence
- Audience: homeowners, property managers, and HOAs in ${MSHI_SERVICE_AREAS}
- Format: 100-150 words, short paragraphs, no corporate polish
- Tone: warm, personal, local, West Virginia proud, and plain spoken
- Write as Brad and Dustin. Use "we" naturally, but make it sound like tradespeople talking to neighbors
- Feature real services when relevant: ${MSHI_CORE_SERVICES}
- Reference Charleston WV, Kanawha County, Putnam County, or Cabell County naturally when it fits
- Highlight locally owned, licensed WV065578, owner-done work, and direct access during the project when relevant
- End with this CTA or a close variation that keeps the meaning: "${cta}"
- Never mention Pulseforge, AI, automation, marketing, pricing, or competitors

VARIETY RULES — enforce these on every post:
- Never start consecutive posts with the same opening word or phrase
- Rotate the angle on every post — do not repeat the same core message within 7 days
- Avoid generic contractor openers like "Looking to upgrade your home" or "Your home deserves the best"
- Each post must make ONE specific point: a project detail, homeowner concern, service tip, or local proof point
${lastContentType ? `\nThe last post for this channel was: ${lastContentType}. This post must feel distinct from that — different angle, different opening, different structure.` : ''}

Return only the post text.`;
}

function buildMshiGooglePrompt(company, contentType, verticalCtx, location, lastContentType, channelStrategy, cta) {
  return `You are writing a Google Business Profile update for ${company.name}.
${company.name} is Mountain State Home Innovations, a licensed WV contractor run by Brad and Dustin.

Content type: ${contentType}
Business context: ${verticalCtx}

${channelStrategy}

MSHI GOOGLE BUSINESS STRATEGY:
- Channel purpose: search-intent, local proof for someone deciding whether to call a Charleston WV contractor
- Audience: homeowners, property managers, and HOAs in ${MSHI_SERVICE_AREAS}
- Format: 75-100 words, short and specific
- Tone: clear, practical, credible, and locally grounded
- Mention Charleston WV or the surrounding WV service area naturally
- Feature one concrete service or trust point: ${MSHI_CORE_SERVICES}, licensed WV065578, Brad and Dustin do the work themselves, or direct access throughout the project
- End with this CTA or a close variation that keeps the meaning: "${cta}"
- No hashtags and no emojis
- Never mention Pulseforge, AI, automation, marketing, pricing, or competitors

VARIETY RULES — enforce these on every post:
- Never start consecutive posts with the same opening word or phrase
- Rotate the angle on every post — do not repeat the same core message within 7 days
- Avoid generic contractor openers like "Looking to upgrade your home" or "Your home deserves the best"
- Each post must answer one practical question or prove one reason to trust MSHI
${lastContentType ? `\nThe last post for this channel was: ${lastContentType}. This post must feel distinct from that — different angle, different opening, different structure.` : ''}

Return only the post text.`;
}

function buildMshiBlogPrompt(company, contentType, verticalCtx, lastContentType, blogCloser, channelStrategy) {
  const location = company.location || 'Charleston WV';

  return `You are writing a blog post for ${company.name}.
${company.name} is Mountain State Home Innovations, a locally owned West Virginia contractor run by Brad and Dustin.

Content type: ${contentType}
Business context: ${verticalCtx}

${channelStrategy}

MSHI BLOG STRATEGY:
- Channel purpose: SEO, long-form project stories, and helpful content for homeowners
- Audience: homeowners, property managers, and HOAs in ${MSHI_SERVICE_AREAS}
- Format: 400-600 words
- Tone: educational, practical, local, and plain spoken
- Write as Brad and Dustin, not as a marketing agency
- Build the reader's understanding before mentioning the estimate
- Feature one or more core services when relevant: ${MSHI_CORE_SERVICES}
- Reference ${location}, Charleston WV, Kanawha County, Putnam County, or Cabell County naturally
- Highlight locally owned, licensed WV065578, Brad and Dustin doing the work themselves, and direct access throughout the project when relevant
- Never mention Pulseforge, AI, automation, marketing, pricing, or competitors

Write a blog post (400-600 words) using this structure:
# [Title]

[Intro paragraph with a specific homeowner problem, project detail, or WV weather issue]

## [Subheading]
[Body section]

## [Subheading]
[Body section]

[Optional third ## section if it fits naturally — skip if it would feel padded]

[Closing paragraph using the required closer below]

Requirements:
- Use # for the title and ## for subheadings — no other markdown
- No keyword stuffing
- No corporate tone, no "we pride ourselves," no "industry-leading"
- End the post with this exact closer, adjusted only if needed for grammar: "${blogCloser}"
- Never use engagement-bait questions as blog closers. Do not end with "drop a comment", "what do you think", "let us know below", or any request for comments

VARIETY RULES — enforce these on every post:
- Never start consecutive posts with the same opening word or phrase
- Rotate the angle on every post — do not repeat the same core message within 7 days
- Avoid generic contractor openers like "Looking to upgrade your home" or "Your home deserves the best"
- Each post must make ONE specific point: a project breakdown, homeowner tip, seasonal advice, or service explanation
${lastContentType ? `\nThe last post for this channel was: ${lastContentType}. This post must feel distinct from that — different angle, different opening, different structure.` : ''}

Return only the blog post text with markdown formatting.`;
}

function buildLinkedInPrompt(company, contentType, verticalCtx, lastContentType, channelStrategy) {
  const isPulseforge = company.name.toLowerCase().includes('pulseforge');
  const location = company.location || 'Manchester, NH';

  const audienceNote = isPulseforge
    ? `IMPORTANT: Pulseforge's audience is small business owners considering marketing automation. Write from a first-person operator POV to owners who are tired of repetitive marketing tasks and want a system that runs in the background.`
    : `Write as the brand voice of ${company.name} — use "we" and "${company.name}", not "I". Speak to local customers and business owners in ${location}.`;

  return `You are writing a LinkedIn page post for ${company.name}, a local ${company.industry || 'business'} in ${location}${isPulseforge ? ' that automates marketing and outreach for small business owners using AI' : ''}.

Content type: ${contentType}
Business context: ${verticalCtx}

${audienceNote}
${channelStrategy}

LINKEDIN STRATEGY:
- Purpose: thought leadership and credibility
- Format: 150-250 words
- Tone: first-person POV, industry observation or contrarian take, no overt sales pitch
- If today's topic appears on other channels, LinkedIn must use the POV angle only
- No hard sell, no product walkthrough pitch, no "book a call" style ending
- Use no more than 2-3 hashtags

Write a LinkedIn post (150-250 words) with this structure:
- First line: a specific industry observation or contrarian hook — no fluff openers like "excited to share" or "we're thrilled"
- 2-3 short paragraphs or punchy line breaks
- Final line: a thoughtful forward-moving statement, not a sales pitch and not a question for comments
- Last line: 2-3 relevant hashtags max

Voice and tone:
- First-person POV for Pulseforge — use "I" when writing as Pulseforge; for non-Pulseforge clients, use "we" and "${company.name}"
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

function buildLinkedInPersonalPrompt(company, contentType, verticalCtx, lastContentType, channelStrategy) {
  const location = company.location || 'Manchester, NH';

  return `You are writing a LinkedIn personal profile post for Jacob Maynard, founder of Pulseforge in ${location}.

Content type: ${contentType}
Business context: ${verticalCtx}

${channelStrategy}

LINKEDIN PERSONAL PROFILE STRATEGY:
- Purpose: personal brand, founder journey, behind the scenes, and credibility through lived experience
- Format: 150-250 words
- Voice: written as Jacob Maynard personally, never as Pulseforge
- Use "I built this", "I noticed", "I learned", and "my clients" where natural
- Never use "we built this" or sound like a company page
- More personal than the Pulseforge LinkedIn page: bartending, the agency grind, specific client wins, building systems late after service shifts, or lessons from local business owners
- If today's topic appears on other channels, this profile must use the founder journey angle only and must not reuse the Pulseforge LinkedIn page's core narrative arc
- No hard sell, no product walkthrough pitch, no "book a call" style ending
- Use no more than 2-3 hashtags

Write a LinkedIn personal profile post (150-250 words) with this structure:
- First line: a personal, specific, scroll-stopping hook. It may start with "I" when the line is concrete and personal.
- 2-4 short paragraphs or punchy line breaks
- Include one grounded detail from Jacob's perspective: bartending, building Pulseforge, a real client pattern, or the daily agency grind
- Final line: a human forward-moving reflection, not a sales pitch and not a question for comments
- Last line: 2-3 relevant hashtags max

Voice and tone:
- Sounds like a person, not a brand account
- Founder/operator perspective, direct and honest
- Mention Pulseforge only if it feels natural; the post is about Jacob's lens, not company promotion
- For "behind-the-scenes": show what Jacob is building or learning
- For "results": describe a specific anonymized client win from Jacob's perspective
- For "educational": teach through a personal story, not generic advice
- For "community": connect to Manchester NH, bartending, or local small business owners
- Never use dashes or hyphens in any content you write — not as punctuation, not as separators, not in any context.

VARIETY RULES — enforce these on every post:
- Never start a post with "Most small business owners" or any variation of that phrase
- Never start consecutive posts with the same opening word or phrase
- Rotate the angle on every post — do not repeat the same core message within 7 days
- Avoid these overused openers: "Most", "Many", "As a", "If you're a", "Running a small business"
- Each post must make ONE specific point — not a general observation about small business
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
    max_tokens: channel === 'blog' ? 1000 : channel === 'linkedin_page' || channel === 'linkedin_personal' ? 450 : 300,
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
   A specificity score of 7 means the post is concrete enough to pass. Do NOT reject a post scoring 7 on specificity because it lacks a revenue figure. Concrete details, timelines, and named scenarios count as specificity. Only flag specificity as the weak dimension if it scores 6 or below.

2. ORIGINALITY — Does it avoid clichés, seasonal hooks, and location
   name-drops as the main angle? (10 = fresh angle, 1 = recycled formula)
   Use this recent 14-day published angle list as the main originality reference:
${formatUsedAngles(recentPublishedAngles)}
   Originality scores below 7 should only occur if the post reuses a hook
   or angle seen in the last 14 days. A fresh angle on a familiar topic can
   still score 8+.

3. HOOK STRENGTH — Does the opening line give someone a reason to stop
   scrolling and keep reading? (10 = compelling, 1 = forgettable)
   A hook_strength score of 7 means the opening is good but not exceptional. This is a passing score. Do NOT fail a post for hook_strength = 7 by adding qualitative commentary that contradicts the numeric score. If you scored it 7, it passed the dimension floor. Only flag hook_strength as the weak dimension if it scores 6 or below.

Passing requires total >= ${MIN_QUALITY_SCORE}, hook_strength >= ${MIN_HOOK_SCORE},
specificity >= ${MIN_DIMENSION_SCORE}, and originality >= ${MIN_DIMENSION_SCORE}.
A score below 7 on any dimension means the post failed.

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

function validateDraftForClient(draft, channel) {
  if (CLIENT_ID !== 2) return [];

  const text = String(draft || '');
  const checks = [
    { label: 'mentions Pulseforge', pattern: /\bpulseforge\b/i },
    { label: 'mentions AI', pattern: /\bAI\b/i },
    { label: 'mentions automation', pattern: /\bautomation\b|\bautomated\b|\bautomate\b/i },
    { label: 'mentions marketing agency', pattern: /\bmarketing agency\b|\bmarketers?\b/i },
    { label: 'includes pricing specifics', pattern: /\$\s*\d|\b\d+(?:,\d{3})*(?:\.\d{2})?\s*(?:dollars|bucks)\b|\b\d+%\b/i },
    { label: 'uses negative competitor framing', pattern: /\bcompetitors?\b|\bcut corners\b|\brip(?:s|ped)? off\b|\bbad contractors?\b|\bcheap contractors?\b/i },
  ];

  const issues = checks
    .filter(check => check.pattern.test(text))
    .map(check => check.label);

  if (channel === 'linkedin_page' || channel === 'linkedin_personal') {
    issues.push('LinkedIn content is disabled for MSHI');
  }

  return issues;
}

function passesQualityGate(score) {
  return score.total >= MIN_QUALITY_SCORE &&
    score.hook_strength >= MIN_HOOK_SCORE &&
    score.specificity >= MIN_DIMENSION_SCORE &&
    score.originality >= MIN_DIMENSION_SCORE;
}

function getQualityGateSnapshot(score) {
  const total = Number(score?.total || 0);
  const hook = Number(score?.hook_strength || 0);
  const spec = Number(score?.specificity || 0);
  const orig = Number(score?.originality || 0);

  const snapshot = {
    thresholds: {
      total: MIN_QUALITY_SCORE,
      hook_strength: MIN_HOOK_SCORE,
      specificity: MIN_DIMENSION_SCORE,
      originality: MIN_DIMENSION_SCORE,
    },
    scores: {
      total,
      hook_strength: hook,
      specificity: spec,
      originality: orig,
    },
  };

  const checks = {
    total: total >= snapshot.thresholds.total,
    hook_strength: hook >= snapshot.thresholds.hook_strength,
    specificity: spec >= snapshot.thresholds.specificity,
    originality: orig >= snapshot.thresholds.originality,
  };

  const failed = Object.entries(checks)
    .filter(([, ok]) => !ok)
    .map(([k]) => k);

  return { ...snapshot, checks, failed, passes: failed.length === 0 };
}

function logQualityGateComparison(contextLabel, score) {
  const snap = getQualityGateSnapshot(score);
  const failed = snap.failed.length ? `failed=[${snap.failed.join(', ')}]` : 'failed=[]';
  console.log(
    `  [quality_gate] ${contextLabel} ` +
    `scores(total=${snap.scores.total}, hook=${snap.scores.hook_strength}, spec=${snap.scores.specificity}, orig=${snap.scores.originality}) ` +
    `thresholds(total=${snap.thresholds.total}, hook=${snap.thresholds.hook_strength}, spec=${snap.thresholds.specificity}, orig=${snap.thresholds.originality}) ` +
    `${failed} passes=${snap.passes}`
  );
}

async function generatePost(company, contentType, channel) {
  const verticalCtx = getVerticalContext(company.industry);
  const isMshi = CLIENT_ID === 2;
  const location = company.location || (isMshi ? 'Charleston WV' : 'Manchester, NH');
  const lastContentType = await getLastContentType(company.name, channel);
  const recentThemes = await getRecentThemes(channel);
  const isPulseforge = isPulseforgeCompany(company);
  const recentPublishedAngles = await getRecentPublishedAngles();
  const topicAngle = isMshi
    ? getMshiTopicAngle(new Date(), channel)
    : isPulseforge
      ? getPulseforgeTopicAngle(new Date(), channel)
      : null;
  const blogCloser = channel === 'blog'
    ? (isMshi ? getMshiCta(channel) : getBlogCloser(channel))
    : null;
  const facebookCta = channel === 'facebook_page'
    ? (isMshi ? getMshiCta(channel) : getFacebookCta(channel))
    : null;
  const mshiCta = isMshi ? getMshiCta(channel) : null;
  const channelStrategy = isMshi
    ? buildMshiChannelStrategyBlock(channel, topicAngle)
    : buildChannelStrategyBlock(channel, topicAngle);
  const systemPrompt = isMshi
    ? buildMshiContentRules(recentThemes, recentPublishedAngles, topicAngle)
    : buildContentRules(recentThemes, recentPublishedAngles, topicAngle, isPulseforge);

  if (lastContentType) {
    console.log(`  [variety] Last ${channel} post type: ${lastContentType} → generating: ${contentType}`);
  }
  console.log(`  [themes] Recent openings checked: ${recentThemes.openings.length}; patterns: ${recentThemes.patterns.length}`);
  if ((isPulseforge || isMshi) && topicAngle) {
    console.log(`  [topic] ${topicAngle.label}`);
  }
  if (blogCloser) {
    console.log(`  [blog closer] ${blogCloser}`);
  }
  if (facebookCta) {
    console.log(`  [facebook cta] ${facebookCta}`);
  }
  console.log(`  [memory] Recent published angles checked: ${recentPublishedAngles.length}`);

  if (isMshi && !MSHI_CHANNELS.includes(channel)) {
    throw new Error(`MSHI does not generate ${channel} content`);
  }

  const prompt = isMshi
    ? channel === 'google_business'
      ? buildMshiGooglePrompt(company, contentType, verticalCtx, location, lastContentType, channelStrategy, mshiCta)
      : channel === 'blog'
        ? buildMshiBlogPrompt(company, contentType, verticalCtx, lastContentType, blogCloser, channelStrategy)
        : buildMshiFacebookPrompt(company, contentType, verticalCtx, location, lastContentType, channelStrategy, facebookCta)
    : channel === 'google_business'
      ? buildGooglePrompt(company, contentType, verticalCtx, location, lastContentType, channelStrategy)
      : channel === 'blog'
        ? buildBlogPrompt(company, contentType, verticalCtx, lastContentType, blogCloser, channelStrategy)
        : channel === 'linkedin_personal'
          ? buildLinkedInPersonalPrompt(company, contentType, verticalCtx, lastContentType, channelStrategy)
        : channel === 'linkedin_page'
          ? buildLinkedInPrompt(company, contentType, verticalCtx, lastContentType, channelStrategy)
          : buildFacebookPrompt(company, contentType, verticalCtx, location, lastContentType, channelStrategy, facebookCta);

  let draft = null;
  let score = null;
  let validationIssues = [];
  let finalDraft = null;
  let finalScore = null;
  let finalValidationIssues = [];
  let regenerated = false;
  let regenerationAttempts = 0;

  try {
    draft = await createDraft(prompt, systemPrompt, channel);
    score = await scoreDraft(draft, recentPublishedAngles);
    validationIssues = validateDraftForClient(draft, channel);
    finalDraft = draft;
    finalScore = score;
    finalValidationIssues = validationIssues;

    logQualityGateComparison('initial', score);

    while ((validationIssues.length || !passesQualityGate(score)) && regenerationAttempts < MAX_REGENERATION_ATTEMPTS) {
      regenerated = true;
      regenerationAttempts++;
      if (validationIssues.length) {
        console.log(`  [validation] Regenerating attempt ${regenerationAttempts}/${MAX_REGENERATION_ATTEMPTS} for client rules: ${validationIssues.join(', ')}`);
      } else {
        console.log(`  [quality] Score ${score.total}/30, regenerating attempt ${regenerationAttempts}/${MAX_REGENERATION_ATTEMPTS} for ${score.weak_dimension}`);
      }

      const regenPrompt = isMshi
        ? [
            prompt,
            '',
            `Your previous draft failed these MSHI validation rules: ${validationIssues.join(', ') || 'quality threshold'}.`,
            `Your previous draft scored ${score.total}/30 overall, with specificity ${score.specificity}/10, originality ${score.originality}/10, and hook strength ${score.hook_strength}/10. Reason: ${score.reason}.`,
            '',
            'Rewrite it with a completely different opening line and keep it specific to Brad, Dustin, Mountain State Home Innovations, and West Virginia homeowners.',
            'Do not mention Pulseforge, AI, automation, marketing, pricing, competitors, or LinkedIn.',
            'Do not include dollar amounts, percentages, package prices, or negative comparisons.',
            `The final score must be ${MIN_QUALITY_SCORE}/30 or higher, and specificity, originality, and hook_strength must each be at least ${MIN_DIMENSION_SCORE}/10.`,
            `Use today's MSHI topic bucket: ${topicAngle ? topicAngle.label + ': ' + topicAngle.guidance : 'the most distinct available MSHI angle'}.`,
            '',
            'Return only the rewritten content.',
          ].join('\n')
        : [
            prompt,
            '',
            `Your previous draft scored ${score.total}/30 total, with specificity ${score.specificity}/10, originality ${score.originality}/10, and hook strength ${score.hook_strength}/10. Reason: ${score.reason}.`,
            '',
            'A strong hook is the ONLY thing that matters in the first line. Here are examples of strong vs weak:',
            '',
            'WEAK: "We help local businesses stay on top of their marketing."',
            'WEAK: "Running a small business is tough, especially when it comes to marketing."',
            'WEAK: "Most business owners don\'t realize how much time they spend on marketing."',
            '',
            'STRONG: "A cleaning company owner can miss three booking requests before lunch without ever noticing the pattern."',
            'STRONG: "The phone going quiet is not always a demand problem."',
            'STRONG: "One tiny handoff between inbox and calendar can decide whether a lead turns into revenue."',
            '',
            'The first line must create a reason to keep reading. It should be specific, surprising, or create a gap the reader wants to close.',
            'No generic observations. No "most business owners." No "running a small business."',
            'Do not use competitor comparison hooks, lead response rate statistics, "your competitor just", "40% of leads", or "follow-up speed" angles.',
            `Use today's topic bucket instead: ${topicAngle ? topicAngle.label + ': ' + topicAngle.guidance : 'the most distinct available angle'}.`,
            `The final score must be ${MIN_QUALITY_SCORE}/30 or higher, and specificity, originality, and hook_strength must each be at least ${MIN_DIMENSION_SCORE}/10. Do not pass along a merely acceptable post.`,
            '',
            'Return only the rewritten post text.',
          ].join('\n');

      draft = await createDraft(regenPrompt, systemPrompt, channel);
      score = await scoreDraft(draft, recentPublishedAngles);
      validationIssues = validateDraftForClient(draft, channel);

      logQualityGateComparison(`attempt_${regenerationAttempts}`, score);

      if (!validationIssues.length && (finalValidationIssues.length || score.total > finalScore.total)) {
        finalDraft = draft;
        finalScore = score;
        finalValidationIssues = validationIssues;
      }
    }
  } catch (err) {
    // If scoring/generation breaks mid-loop, this channel currently fails via logChannelError.
    // Make a best-effort attempt to log a content_failed row as well so the failure isn't silent.
    const fallbackScore = finalScore || score || { specificity: 0, originality: 0, hook_strength: 0, total: 0, weak_dimension: 'none', reason: `error: ${err.message}` };
    try {
      await logContentFailed(company, channel, contentType, fallbackScore, regenerationAttempts, finalDraft || draft || '');
    } catch (logErr) {
      console.error(`  [content_failed] log write failed: ${logErr.message}`);
    }
    throw err;
  }

  if (finalValidationIssues.length) {
    await logContentFailed(company, channel, contentType, finalScore, regenerationAttempts, finalDraft);
    console.log(`  [validation] Failed after ${regenerationAttempts} regeneration attempt(s): ${finalValidationIssues.join(', ')}; skipping ${channel}`);
    return { content: null, quality: finalScore, regenerated, failed: true, regenerationAttempts };
  }

  if (!passesQualityGate(finalScore)) {
    logQualityGateComparison('final_compare', finalScore);
    await logContentFailed(company, channel, contentType, finalScore, regenerationAttempts, finalDraft);
    console.log(`  [quality] Failed after ${regenerationAttempts} regeneration attempt(s), best score ${finalScore.total}/30; skipping ${channel}`);
    return { content: null, quality: finalScore, regenerated, failed: true, regenerationAttempts };
  }

  console.log(`  [quality] Score ${finalScore.total}/30 after ${regenerationAttempts} regeneration attempt(s)`);
  return { content: finalDraft, quality: finalScore, regenerated, regenerationAttempts };
}

async function logQualityScore(channel, quality, regenerated, post, attemptCount = 0) {
  console.log(`  [logQualityScore] ${channel} attempt_count being passed: ${attemptCount}`);
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
    attempt_count: attemptCount,
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

async function logContentFailed(company, channel, contentType, quality, regenerationAttempts, post) {
  const payload = {
    company: company?.name || null,
    channel,
    content_type: contentType,
    best_score: quality.total,
    scores: {
      specificity: quality.specificity,
      originality: quality.originality,
      hook_strength: quality.hook_strength,
      total: quality.total,
    },
    regeneration_attempts: regenerationAttempts,
    minimum_required_score: MIN_QUALITY_SCORE,
    minimum_dimension_score: MIN_DIMENSION_SCORE,
    weak_dimension: quality.weak_dimension === 'none' ? null : quality.weak_dimension,
    reason: quality.reason || '',
    post_preview: String(post || '').slice(0, 160),
  };
  try {
    await pool.query(`
      INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
      VALUES ($1, $2, $3, $4, NOW(), $5)
    `, [
      AGENT_NAME,
      'content_failed',
      JSON.stringify(payload),
      'failed',
      CLIENT_ID,
    ]);
  } catch (err) {
    console.error(`  [content_failed] Failed to write agent_log row: ${err.message}`);
    throw err;
  }
}

async function saveToPendingApprovals(company, content, contentType, channel) {
  const channelLabel = {
    facebook_page:   'Facebook Page',
    google_business: 'Google Business',
    blog:            'Blog',
    linkedin_page:   'LinkedIn Page',
    linkedin_personal:'LinkedIn Personal',
  }[channel] || channel;
  const label = `${channelLabel} · ${contentType.charAt(0).toUpperCase() + contentType.slice(1)}`;

  // LinkedIn Page posts carry a first-comment URL posted via Buffer after approval
  const storedContent = channel === 'linkedin_page'
    ? `POST: ${content}\nFIRST_COMMENT: https://gopulseforge.com`
    : content;

  if (CLIENT_ID === 1 && isPulseforgeCompany(company)) {
    const channelExisting = await pool.query(`
      SELECT id FROM pending_comments
      WHERE channel = $1
        AND author_name = $2
        AND status = 'pending'
        AND client_id = $3
      LIMIT 1
    `, [channel, company.name, CLIENT_ID]);

    if (channelExisting.rows.length > 0) {
      console.log(`  ↷ Skipping duplicate pending Pulseforge channel: ${channel}`);
      return null;
    }
  }

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

  const clients = getGenerationClients(await getActiveClients());
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
    if (!channel || !ALL_CHANNELS.includes(channel)) {
      await completeRegenerateTrigger(row.id);
      continue;
    }

    for (const company of clients) {
      const allowedChannels = getChannelsForCompany(company);
      if (!allowedChannels.includes(channel)) {
        console.log(`  [regenerate] skipping ${company.name}/${channel}: channel disabled for client ${CLIENT_ID}`);
        continue;
      }
      const contentType = await pickContentType(company.name, channel, company.id);
      console.log(`  [regenerate] ${company.name} — ${channel} — ${contentType}`);
      try {
        const postResult = await generatePost(company, contentType, channel);
        if (postResult.failed) continue;
        const id = await saveToPendingApprovals(company, postResult.content, contentType, channel);
        if (id) {
          await logQualityScore(channel, postResult.quality, true, postResult.content, postResult.regenerationAttempts);
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
    console.log('MSHI Facebook page is not connected yet; Paige will still queue Facebook, Google Business, and blog drafts for approval.');
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
    const allClients = await getActiveClients();
    const rejectedPulseforgePending = await rejectPendingPulseforgeApprovals(allClients);
    if (rejectedPulseforgePending) {
      console.log(`[Paige] Rejected ${rejectedPulseforgePending} pending Pulseforge approval(s) before regenerating.`);
    }

    const clients = getGenerationClients(allClients);
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
        pulseforge_pending_rejected: rejectedPulseforgePending,
        max_regenerate_triggers: regenerateResult.triggers,
        max_regenerated: regenerateResult.regenerated,
      });
      return;
    }

    let generated = 0;
    const channelsFailed = [];
    const channelsQueued = [];

    for (const company of clients) {
      for (const channel of getChannelsForCompany(company)) {
        const contentType = await pickContentType(company.name, channel, company.id);
        console.log(`${company.name} — ${channel} — ${contentType}`);

        try {
          const postResult = await generatePost(company, contentType, channel);
          if (postResult.failed) {
            channelsFailed.push(`${company.name}/${channel}`);
            continue;
          }
          const content = postResult.content;
          const id = await saveToPendingApprovals(company, content, contentType, channel);
          if (id) {
            await logQualityScore(channel, postResult.quality, postResult.regenerated, content, postResult.regenerationAttempts);
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
      pulseforge_pending_rejected: rejectedPulseforgePending,
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
