require('dotenv').config();
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const Anthropic = require('@anthropic-ai/sdk');
const db = require('./dbClient');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');
const { buildVoiceConstraintBlock } = require('./utils/voiceRules');

puppeteer.use(StealthPlugin());

const AGENT_NAME = 'link';
const SESSION_FILE = './linkedin_session.json';
const CLIENT_ID = getRuntimeClientId();

const client = new Anthropic();

const agentPersona = {
  name: 'Jake Maynard',
  title: 'Founder, Pulseforge'
};

// Topics we want to engage on — posts about running local businesses
const RELEVANT_KEYWORDS = [
  'small business', 'local business', 'marketing', 'customers', 'clients',
  'restaurant', 'cleaning', 'hvac', 'plumbing', 'salon', 'landscaping',
  'owner', 'entrepreneur', 'leads', 'growth', 'slow season', 'busy season',
  'referrals', 'reviews', 'google', 'visibility', 'social media'
];

function randomDelay(min, max) {
  return new Promise(resolve =>
    setTimeout(resolve, Math.floor(Math.random() * (max - min + 1)) + min)
  );
}

async function saveSession(page) {
  const cookies = await page.cookies();
  fs.writeFileSync(SESSION_FILE, JSON.stringify(cookies, null, 2));
  console.log('LinkedIn session saved');
}

async function loadSession(page) {
  if (process.env.LINKEDIN_SESSION) {
    const cookies = JSON.parse(Buffer.from(process.env.LINKEDIN_SESSION, 'base64').toString('utf8'));
    await page.setCookie(...cookies);
    console.log('LinkedIn session loaded from env');
    return true;
  }
  if (!fs.existsSync(SESSION_FILE)) return false;
  try {
    const cookies = JSON.parse(fs.readFileSync(SESSION_FILE));
    await page.setCookie(...cookies);
    console.log('LinkedIn session loaded');
    return true;
  } catch {
    return false;
  }
}

async function isLoggedIn(page) {
  try {
    await page.goto('https://www.linkedin.com/feed/', { waitUntil: 'domcontentloaded', timeout: 60000 });
    await randomDelay(3000, 5000);
    const url = page.url();
    return url.includes('/feed') || url.includes('/in/');
  } catch {
    return false;
  }
}

function isRelevant(text) {
  const lower = text.toLowerCase();
  return RELEVANT_KEYWORDS.some(kw => lower.includes(kw));
}

async function extractPosts(page) {
  await page.evaluate(() => window.scrollBy(0, 2000));
  await randomDelay(2000, 3000);
  await page.evaluate(() => window.scrollBy(0, 2000));
  await randomDelay(1500, 2500);

  return page.evaluate(() => {
    const posts = [];
    const seen = new Set();
    let malformedCount = 0;

    const AUTHOR_SELECTORS = [
      '.update-components-actor__title span[aria-hidden="true"]',
      '.update-components-actor__name span[aria-hidden="true"]',
      '.update-components-actor__title',
      '.feed-shared-actor__name span[aria-hidden]',
    ];

    // Try multiple selector patterns across LinkedIn DOM versions
    const containers = document.querySelectorAll(
      '[data-urn*="activity"], .feed-shared-update-v2, .occludable-update'
    );

    for (const el of containers) {
      try {
        // Post text
        const textEl = el.querySelector(
          '.feed-shared-update-v2__description .break-words span[dir], ' +
          '.feed-shared-text .break-words span[dir], ' +
          '.update-components-text .break-words span[dir], ' +
          '.feed-shared-inline-show-more-text span[dir]'
        );
        const content = textEl?.innerText?.trim() || '';
        if (!content || content.length < 40) continue;
        if (seen.has(content.slice(0, 60))) continue;
        seen.add(content.slice(0, 60));

        // Author name
        let authorName = 'Unknown';
        for (const sel of AUTHOR_SELECTORS) {
          const text = el.querySelector(sel)?.innerText?.trim();
          if (text) {
            authorName = text.split('\n')[0].trim();
            break;
          }
        }

        // Post URL — look for permalink anchor
        const linkEl = el.querySelector('a[href*="/posts/"], a[href*="/activity-"]');
        const postUrl = linkEl?.href || null;

        posts.push({ authorName, content, postUrl });
        if (posts.length >= 15) break;
      } catch {
        // skip malformed post
        malformedCount++;
      }
    }

    return { posts, malformedCount };
  });
}

async function generateComment(postContent, authorName) {
  const message = await client.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 150,
    system: buildVoiceConstraintBlock(),
    messages: [{
      role: 'user',
      content: `You are ${agentPersona.name}, ${agentPersona.title}. You are commenting on a LinkedIn post as yourself, not as a marketer.

Primary mode: Lead with substance. The comment should be a sharp observation, a pushback, or a reframe of what the post claims. Relevance to the specific post content is the highest priority. The reader should feel the comment engaged with the actual argument, not delivered a template.

Personal lens: Available but secondary. Jacob's background (10+ years operating restaurants and a cleaning company, bartending while bootstrapping Pulseforge solo, working with local service businesses like home renovation contractors in Charleston WV right now, single dad) is a tool to deploy when it materially sharpens the point. Never as the opening move. Most comments should have no autobiographical anchor at all.

Post by ${authorName}:
"${postContent.slice(0, 600)}"

Write a LinkedIn comment (2 to 3 sentences max) that:
- Responds directly to something specific in the post. Reference a specific claim, premise, or detail.
- Leads with the point, not credentials or backstory.
- Uses background only when it materially sharpens the argument.
- Reads like you typed it on your phone between two things. Short sentences. Direct. No corporate cadence.

Hard rules (do not violate):
- Never use em dashes. Use periods or commas. Two short sentences instead of one long one joined by an em dash.
- Do not open with "When I ran restaurants...", "In the cleaning business...", "Running restaurants taught me...", "I've opened restaurants...", "When I was scaling...", or any opener that leads with "I" plus a past tense operator verb.
- No "great post", no "love this", no "thanks for sharing", no thought-leader phrasing like "at the end of the day", "the truth is", "double-tap", "this hits", "the messy middle", or "embracing the journey".
- No self-promotion. No product pitch. If referencing your work, say "my agency" or "operating restaurants".
- No moderator-style questions. If you ask a question, it should be a real one a peer would ask.

Variety:
- When background comes in, rotate the thread. Do not pull from restaurants every time. Bartending now, the current Pulseforge grind, doing outbound yourself, working with home services clients, and parenting are all available.
- Vary the structure across comments: pure pushback on a specific claim, sharper reframe of the premise, second-order implication the post missed, counter-example from a different domain, or an observation about who the advice does or does not apply to.

Examples of range (study the cadence, do not copy phrases):

Example 1 (pure insight, no anchor):
"The hardest part isn't picking the channel. It's resisting the urge to bail two weeks in when nothing's working yet. Most of what looks like wrong-channel is actually right-channel-too-soon."

Example 2 (counter-take, no anchor):
"This is the right answer for a team of 50. At 5 the bottleneck isn't process, it's whoever stops sleeping first. Different problem, different fix."

Example 3 (pushback with background woven in mid-comment, not as opener):
"The frame assumes you know what your offer is. Most early stage operators don't, and pretending you do because someone said clarity matters is how you end up rebuilding the funnel three times. Faster path is shipping the messy version and letting prospects tell you what stuck. Did this with my own client work and saved months of guessing."

Return only the comment text. No preamble, no quotes around it, no "POST:" prefix.`
    }]
  });

  const commentText = message.content[0].text.trim();
  return commentText.trim();
}

async function run() {
  const clientConfig = await getClientConfig(CLIENT_ID);
  if (!clientConfig) throw new Error(`Active client not found: ${CLIENT_ID}`);
  if (CLIENT_ID !== 1) {
    console.log('Link engagement is enabled only for Pulseforge client_id=1.');
    return { skipped: true, reason: 'client_not_enabled', client_id: CLIENT_ID };
  }
  let browser;
  let drafted = 0;
  let unknownCount = 0;
  let totalPosts = 0;
  let malformedPosts = 0;
  const limit = 10;
  try {
    browser = await puppeteer.launch({
      headless: false,
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 800 });

    const sessionLoaded = await loadSession(page);

    if (sessionLoaded) {
      const loggedIn = await isLoggedIn(page);
      if (!loggedIn) {
        console.log('Session expired — please log in manually');
        await page.goto('https://www.linkedin.com/login', { waitUntil: 'domcontentloaded' });
        console.log('Waiting 90 seconds for manual login...');
        await randomDelay(90000, 90000);
        await saveSession(page);
      }
    } else {
      console.log('No session — please log in manually');
      await page.goto('https://www.linkedin.com/login', { waitUntil: 'domcontentloaded' });
      console.log('Waiting 90 seconds for manual login...');
      await randomDelay(90000, 90000);
      await saveSession(page);
    }

    console.log('\nLinkedIn agent running...\n');

    // Search for small business content
    const searchUrls = [
      'https://www.linkedin.com/in/retentionadam/recent-activity/all/',
      'https://www.linkedin.com/in/justinwelsh/recent-activity/all/',
      'https://www.linkedin.com/in/coldemailwizard/recent-activity/all/',
      'https://www.linkedin.com/in/jasondbay/recent-activity/all/',
      'https://www.linkedin.com/in/anthony-natoli/recent-activity/all/',
      'https://www.linkedin.com/in/outboundsales/recent-activity/all/',
      'https://www.linkedin.com/in/morganjingramamp/recent-activity/all/',
    ];

    for (const url of searchUrls) {
      const stillActive = await getClientConfig(CLIENT_ID);
      if (!stillActive) {
        throw new Error(`[LinkedIn] Client ${CLIENT_ID} deactivated mid-run — aborting`);
      }

      if (drafted >= limit) break;

      console.log(`Scanning: ${url}`);
      try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
        try {
          await page.waitForSelector('[data-urn*="urn:li:activity"]', { timeout: 30000 });
        } catch (e) {
          console.log(`No posts rendered on ${url} after 30s wait, skipping`);
          continue;
        }
        await randomDelay(4000, 6000);
      } catch (err) {
        console.error(`Navigation failed: ${err.message}`);
        continue;
      }

      const { posts, malformedCount } = await extractPosts(page);
      malformedPosts += malformedCount;
      console.log(`Found ${posts.length} posts`);

      for (const post of posts) {
        if (drafted >= limit) break;
        totalPosts++;
        if (post.authorName === 'Unknown') unknownCount++;

        if (!isRelevant(post.content)) {
          console.log(`  Skipping (off-topic): ${post.authorName}`);
          continue;
        }

        console.log(`  Drafting comment for: ${post.authorName}`);

        try {
          const comment = await generateComment(post.content, post.authorName);

          await db.savePendingComment({
            authorName: post.authorName,
            authorTitle: 'LinkedIn',
            postContent: post.content.substring(0, 500),
            comment,
            postUrl: post.postUrl,
            channel: 'linkedin'
          });


          await db.logAgentAction(
            AGENT_NAME,
            'generate_comment',
            null,
            post.postUrl,
            { comment, authorName: post.authorName },
            'success'
          );

          console.log(`  ✓ queued — "${comment.slice(0, 60)}..."`);
          drafted++;
        } catch (err) {
          console.error(`  ✗ ${post.authorName}: ${err.message}`);
        }

        await randomDelay(3000, 6000);
      }

      await randomDelay(10000, 20000);
    }

    console.log(
      `\nLinkedIn agent complete — ${drafted} comment${drafted !== 1 ? 's' : ''} queued for approval. ` +
      `${malformedPosts} malformed post container${malformedPosts !== 1 ? 's' : ''} skipped.`
    );

    const failRate = totalPosts ? (unknownCount / totalPosts) : 0;
    if (failRate > 0.3) {
      console.error(
        `⚠️ AUTHOR EXTRACTION DEGRADED: ${unknownCount}/${totalPosts} posts (${Math.round(failRate * 100)}%) resolved to Unknown. ` +
        `LinkedIn likely changed the actor DOM — author selectors need updating in linkedinAgent.js.`
      );
    }

    return { drafted, limit, unknownCount, totalPosts, malformedPosts, client_id: CLIENT_ID };
  } finally {
    if (browser) await browser.close().catch(() => {});
  }
}

module.exports = { run };

if (require.main === module) {
  run().catch(err => {
    console.error('[LinkedIn] Fatal error:', err.message);
    process.exit(1);
  });
}
