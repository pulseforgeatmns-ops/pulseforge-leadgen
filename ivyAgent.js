require('dotenv').config();
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const Anthropic = require('@anthropic-ai/sdk');
const db = require('./dbClient');
const { sendTelegramNotification } = require('./utils/telegram');

puppeteer.use(StealthPlugin());

const AGENT_NAME = 'ivy';
const SESSION_FILE = './instagram_session.json';
const MAX_DRAFTS = 8;

const client = new Anthropic();

// Business-signal keywords — used to filter captions to actual business content
const BUSINESS_KEYWORDS = [
  'before', 'after', 'team', 'crew', 'staff', 'shop', 'store', 'open',
  'service', 'customer', 'client', 'business', 'owner', 'restaurant',
  'cleaning', 'salon', 'hvac', 'retail', 'local', ' nh ', 'manchester',
  'grand open', 'new location', 'hiring', 'workspace', 'office', 'job done',
  'transformation', 'result', 'project', 'install', 'repair', 'renovati'
];

// Hashtag pages to scan — local + vertical-specific
const TARGET_HASHTAGS = [
  'manchesternh',
  'nhsmallbusiness',
  'manchesternhbusiness',
  'nhrestaurant',
  'nhcleaning',
  'nhsalon',
  'nhhvac',
  'nhretail',
];

function randomDelay(min, max) {
  return new Promise(resolve =>
    setTimeout(resolve, Math.floor(Math.random() * (max - min + 1)) + min)
  );
}

function isBusinessPost(caption) {
  if (!caption || caption.length < 20) return false;
  const lower = caption.toLowerCase();
  return BUSINESS_KEYWORDS.some(kw => lower.includes(kw));
}

// ── SESSION ────────────────────────────────────────────────────────────────
async function saveSession(page) {
  const cookies = await page.cookies();
  fs.writeFileSync(SESSION_FILE, JSON.stringify(cookies, null, 2));
  console.log('Instagram session saved');
}

async function loadSession(page) {
  if (!fs.existsSync(SESSION_FILE)) return false;
  try {
    const cookies = JSON.parse(fs.readFileSync(SESSION_FILE));
    await page.setCookie(...cookies);
    console.log('Instagram session loaded');
    return true;
  } catch {
    return false;
  }
}

async function isLoggedIn(page) {
  try {
    await page.goto('https://www.instagram.com/', { waitUntil: 'domcontentloaded', timeout: 60000 });
    await randomDelay(3000, 5000);
    const url = page.url();
    return !url.includes('/accounts/login') && !url.includes('/login');
  } catch {
    return false;
  }
}

// ── SCRAPE ─────────────────────────────────────────────────────────────────
async function findPostsByHashtag(page, hashtag) {
  const posts = [];

  try {
    await page.goto(`https://www.instagram.com/explore/tags/${hashtag}/`, {
      waitUntil: 'domcontentloaded',
      timeout: 60000,
    });
    await randomDelay(3000, 5000);

    const currentUrl = page.url();
    if (currentUrl.includes('challenge') || currentUrl.includes('login')) {
      console.warn(`  Instagram challenge/redirect on #${hashtag} — skipping`);
      return posts;
    }

    await page.evaluate(() => window.scrollBy(0, 1500));
    await randomDelay(2000, 3000);

    const html = await page.content();

    // Caption text embedded in the page JSON
    const captionMatches = [
      ...html.matchAll(/"edge_media_to_caption":\{"edges":\[\{"node":\{"text":"([^"]{20,500})"/g),
      ...html.matchAll(/"accessibility_caption":"([^"]{20,400})"/g),
    ];
    const shortcodeMatches = [...html.matchAll(/"shortcode":"([A-Za-z0-9_-]{8,12})"/g)];

    const seen = new Set();
    for (let i = 0; i < Math.min(captionMatches.length, 20); i++) {
      const caption = captionMatches[i][1]
        .replace(/\\n/g, ' ')
        .replace(/\\u[0-9a-fA-F]{4}/g, '')
        .replace(/\\/g, '')
        .trim();

      if (!caption || caption.length < 20) continue;
      if (seen.has(caption.slice(0, 40))) continue;
      seen.add(caption.slice(0, 40));

      const shortcode = shortcodeMatches[i]?.[1] || null;
      const postUrl = shortcode ? `https://www.instagram.com/p/${shortcode}/` : null;

      posts.push({ caption, postUrl, hashtag });
      if (posts.length >= 10) break;
    }

    console.log(`  #${hashtag}: ${posts.length} posts found`);
  } catch (err) {
    console.error(`  Error scanning #${hashtag}:`, err.message);
  }

  return posts;
}

// ── COMMENT GENERATION ─────────────────────────────────────────────────────
async function generateComment(caption) {
  const message = await client.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 120,
    messages: [{
      role: 'user',
      content: `You're a local person in New Hampshire commenting on a local business Instagram post.

Post caption:
"${caption.slice(0, 500)}"

Write a 1-2 sentence Instagram comment that:
- References something specific from this caption — the location, the work shown, the team, a real detail
- Sounds like a genuine neighbor engaging with local business content — warm, community-minded
- Never uses filler phrases like "Great post!", "Love this!", "So inspiring!", "Awesome!"
- Never mentions Pulseforge, marketing, automation, or anything promotional
- Never includes hashtags
- Reads like it came from a real person who actually read the post, not a bot

Return only the comment text.`
    }]
  });

  return message.content[0].text.trim();
}

// ── MAIN ───────────────────────────────────────────────────────────────────
async function run() {
  const browser = await puppeteer.launch({
    headless: false,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 800 });

  // Instagram requires a live browser session — exit cleanly if missing or expired
  const sessionLoaded = await loadSession(page);
  if (!sessionLoaded) {
    console.warn('Ivy: No instagram_session.json found.');
    console.warn('Log in to Instagram in a real browser, export cookies to JSON,');
    console.warn('and save to instagram_session.json in the project root.');
    await browser.close();
    return;
  }

  const loggedIn = await isLoggedIn(page);
  if (!loggedIn) {
    console.warn('Ivy: Instagram session expired or showing a challenge.');
    console.warn('Delete instagram_session.json and re-export fresh cookies.');
    await browser.close();
    return;
  }

  console.log('\nIvy agent running...\n');

  let drafted = 0;

  for (const hashtag of TARGET_HASHTAGS) {
    if (drafted >= MAX_DRAFTS) break;

    console.log(`\nScanning #${hashtag}...`);
    const posts = await findPostsByHashtag(page, hashtag);

    for (const post of posts) {
      if (drafted >= MAX_DRAFTS) break;

      if (!isBusinessPost(post.caption)) {
        continue;
      }

      try {
        const comment = await generateComment(post.caption);

        await db.savePendingComment({
          authorName:   `#${post.hashtag}`,
          authorTitle:  'Instagram',
          postContent:  post.caption.substring(0, 500),
          comment,
          postUrl:      post.postUrl,
          channel:      'instagram'
        });

        await sendTelegramNotification({
          channel:      'instagram',
          post_content: post.caption.substring(0, 500),
          comment,
        });

        await db.logAgentAction(
          AGENT_NAME,
          'generate_comment',
          null,
          post.postUrl,
          { comment, hashtag: post.hashtag },
          'success'
        );

        console.log(`  ✓ queued — "${comment.slice(0, 60)}..."`);
        drafted++;
      } catch (err) {
        console.error(`  ✗ ${err.message}`);
      }

      await randomDelay(2000, 4000);
    }

    await randomDelay(8000, 15000);
  }

  if (drafted > 0) {
    const axios = require('axios');
    const BOT = process.env.TELEGRAM_BOT_TOKEN;
    const CHAT = process.env.TELEGRAM_CHAT_ID;
    if (BOT && CHAT) {
      await axios.post(`https://api.telegram.org/bot${BOT}/sendMessage`, {
        chat_id: CHAT,
        text: `📸 Ivy complete — ${drafted} Instagram comment draft${drafted !== 1 ? 's' : ''} queued for approval.`,
      }).catch(() => {});
    }
  }

  console.log(`\nIvy complete — ${drafted} comment${drafted !== 1 ? 's' : ''} queued for approval.`);
  await browser.close();
}

run().catch(console.error);
