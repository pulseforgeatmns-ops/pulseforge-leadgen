require('dotenv').config();
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const Anthropic = require('@anthropic-ai/sdk');
const db = require('./dbClient');
const { sendTelegramNotification } = require('./utils/telegram');

puppeteer.use(StealthPlugin());

const AGENT_NAME = 'linkedin_agent';
const SESSION_FILE = './linkedin_session.json';

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
      content: `You are ${agentPersona.name}, ${agentPersona.title}. You're commenting on a LinkedIn post as yourself — a real person who runs a marketing automation company for local small businesses.

Post by ${authorName}:
"${postContent.slice(0, 600)}"

Write a LinkedIn comment (2-3 sentences) that:
- Responds directly to something specific in the post — show you actually read it
- Adds a real observation or short relevant experience from running your own businesses
- Sounds like a real person, not a marketer — no "great post!", no self-promotion, no product pitch
- Ends naturally — can be a thought, a question, or just a statement
- Keeps a professional but human tone

Return only the comment text.`
    }]
  });

  return message.content[0].text.trim();
}

async function run() {
  const browser = await puppeteer.launch({
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
    'https://www.linkedin.com/search/results/content/?keywords=small%20business%20owner%20marketing&datePosted=past-week&sortBy=relevance',
    'https://www.linkedin.com/search/results/content/?keywords=local%20business%20growth%20customers&datePosted=past-week&sortBy=relevance',
    'https://www.linkedin.com/feed/',
  ];

  let drafted = 0;
  const limit = 10;

  for (const url of searchUrls) {
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

        await sendTelegramNotification({
          channel: 'linkedin',
          post_content: post.content.substring(0, 500),
          comment,
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
  await browser.close();
}

run().catch(console.error);
