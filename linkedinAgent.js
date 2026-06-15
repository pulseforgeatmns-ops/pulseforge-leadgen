require('dotenv').config();
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const Anthropic = require('@anthropic-ai/sdk');
const db = require('./dbClient');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');

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
        const authorEl = el.querySelector(
          '.update-components-actor__name span[aria-hidden], ' +
          '.feed-shared-actor__name span[aria-hidden], ' +
          '.update-components-actor__name'
        );
        const authorName = authorEl?.innerText?.trim() || 'Unknown';

        // Post URL — look for permalink anchor
        const linkEl = el.querySelector('a[href*="/posts/"], a[href*="/activity-"]');
        const postUrl = linkEl?.href || null;

        posts.push({ authorName, content, postUrl });
        if (posts.length >= 15) break;
      } catch {
        // skip malformed post
      }
    }

    return posts;
  });
}

async function generateComment(postContent, authorName) {
  const message = await client.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 150,
    messages: [{
      role: 'user',
      content: `You are ${agentPersona.name}, ${agentPersona.title}. You are commenting on a LinkedIn post as yourself, not as a marketer.

Your background: 10+ years running restaurants and a cleaning company in NH. Currently bootstrapping Pulseforge (a multi-agent lead gen system for local small businesses) while bartending for runway. Real operator who has shipped real things.

Post by ${authorName}:
"${postContent.slice(0, 600)}"

Write a LinkedIn comment (2 to 3 sentences max) that:
- Responds directly to something specific in the post. Reference a specific claim or detail.
- Brings your operator lens. Reference concrete experience from restaurants, the cleaning company, bartending, or running your agency when it actually fits. Specific beats abstract.
- Reads like you typed it on your phone between two things. Short sentences. Direct. No corporate cadence.

Hard rules (do not violate):
- Never use em dashes. Use periods or commas. Two short sentences instead of one long one joined by an em dash.
- No "great post", no "love this", no "thanks for sharing", no thought-leader phrasing like "the messy middle" or "embracing the journey".
- No self-promotion. No product pitch. If referencing your work, say "my agency" or "operating restaurants".
- No moderator-style questions. If you ask a question, it should be a real one a peer would ask.

Examples of your voice (study the cadence, do not copy phrases):

Example 1 (on an SEO post):
"Running a multi-agent lead gen system and wrestling with this exact call. 5-10 high-intent buyer keywords with deep pages, or topic clusters around the awareness searches happening before buyers know the category exists. Volume says cluster. Conversion says high-intent. Curious what you picked and why."

Example 2 (on a post claiming you can have fun, help, and money all together):
"True at the destination, harder at the start. Trio stacks once you have runway. Right now I'm bootstrapping an agency while bartending for runway after a decade running restaurants and a cleaning company. Fun is the lagging indicator, not the current state. Money first. Help next. Fun shows up last."

Notice the patterns: short sentences, periods between thoughts, specific operator details, no em dashes anywhere, contrast or contrarian framing wrapped naturally.

Return only the comment text. No preamble, no quotes around it, no "POST:" prefix.`
    }]
  });

  const commentText = message.content[0].text.trim();
  // Store with delimiter so the approval flow can post the URL as a first reply
  return `POST: ${commentText}\nFIRST_COMMENT: https://gopulseforge.com`;
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
  const limit = 10;
  try {
    browser = await puppeteer.launch({
      headless: true,
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
        await randomDelay(4000, 6000);
      } catch (err) {
        console.error(`Navigation failed: ${err.message}`);
        continue;
      }

      const posts = await extractPosts(page);
      console.log(`Found ${posts.length} posts`);

      for (const post of posts) {
        if (drafted >= limit) break;
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

    console.log(`\nLinkedIn agent complete — ${drafted} comment${drafted !== 1 ? 's' : ''} queued for approval.`);
    return { drafted, limit, client_id: CLIENT_ID };
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
