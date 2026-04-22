require('dotenv').config();
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const { generateComment } = require('./commentGenerator');
const db = require('./dbClient');

puppeteer.use(StealthPlugin());

const AGENT_NAME = 'facebook_agent';
const SESSION_FILE = './facebook_session.json';

const agentPersona = {
  name: 'Jake Morrison',
  title: 'Marketing Consultant'
};

const TARGET_GROUPS = [
  'https://www.facebook.com/groups/783332741800570',
  'https://www.facebook.com/groups/520086383028477',
  'https://www.facebook.com/groups/131968984142922',
  'https://www.facebook.com/groups/286403928622534',
  'https://www.facebook.com/groups/528019700581656',
  'https://www.facebook.com/groups/217977851728936',
  'https://www.facebook.com/groups/newenglandbusinesscommunity',
  'https://www.facebook.com/groups/1538960372868075',
  'https://www.facebook.com/groups/301392468019063',
];

function randomDelay(min, max) {
  return new Promise(resolve =>
    setTimeout(resolve, Math.floor(Math.random() * (max - min + 1)) + min)
  );
}

function isEnglish(text) {
  const englishChars = text.match(/[a-zA-Z]/g) || [];
  const totalChars = text.replace(/\s/g, '').length;
  if (totalChars === 0) return false;
  return (englishChars.length / totalChars) > 0.6;
}

async function saveSession(page) {
  const cookies = await page.cookies();
  fs.writeFileSync(SESSION_FILE, JSON.stringify(cookies, null, 2));
  console.log('Facebook session saved');
}

async function loadSession(page) {
  if (!fs.existsSync(SESSION_FILE)) return false;
  const cookies = JSON.parse(fs.readFileSync(SESSION_FILE));
  await page.setCookie(...cookies);
  console.log('Facebook session loaded');
  return true;
}

async function isLoggedIn(page) {
  try {
    await page.goto('https://www.facebook.com', { waitUntil: 'domcontentloaded', timeout: 60000 });
    await randomDelay(2000, 4000);
    const url = page.url();
    return !url.includes('login');
  } catch (err) {
    return false;
  }
}

async function findPosts(page, groupUrl, maxPosts = 5) {
  const posts = [];

  try {
    console.log(`Navigating to: ${groupUrl}`);
    await page.goto(groupUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await randomDelay(4000, 6000);

    await page.evaluate(() => window.scrollBy(0, 2000));
    await randomDelay(2000, 3000);

    const html = await page.content();

    const storyMatches = [...html.matchAll(/"__typename":"Story".*?"message":\{"text":"([^"]{20,500})"/g)];
    const authorMatches = [...html.matchAll(/"owning_profile":\{"__typename":"User","name":"([^"]+)"/g)];
    const urlMatches = [...html.matchAll(/"wwwURL":"(https:[^"]+permalink[^"]+)"/g)];

    const seen = new Set();
    for (let i = 0; i < Math.min(storyMatches.length, maxPosts * 2); i++) {
      const content = storyMatches[i][1].replace(/\\n/g, ' ').replace(/\\u[0-9a-fA-F]{4}/g, '').trim();
      if (seen.has(content.slice(0, 50))) continue;
      seen.add(content.slice(0, 50));
      const authorName = authorMatches[i] ? authorMatches[i][1] : 'Unknown';
      const postUrl = urlMatches[i] ? urlMatches[i][1].replace(/\\\//g, '/') : null;
      if (content.length > 20) posts.push({ authorName, content, postUrl });
      if (posts.length >= maxPosts) break;
    }

    console.log(`Found ${posts.length} posts in group`);

  } catch (err) {
    console.error('Error finding posts:', err.message);
  }

  return posts;
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
      await page.goto('https://www.facebook.com/login', { waitUntil: 'domcontentloaded' });
      console.log('Waiting 60 seconds for manual login...');
      await randomDelay(60000, 60000);
      await saveSession(page);
    }
  } else {
    console.log('No session found — please log in manually');
    await page.goto('https://www.facebook.com/login', { waitUntil: 'domcontentloaded' });
    console.log('Waiting 60 seconds for manual login...');
    await randomDelay(60000, 60000);
    await saveSession(page);
  }

  console.log('\nFacebook agent running...\n');

  for (const groupUrl of TARGET_GROUPS) {
    const posts = await findPosts(page, groupUrl, 3);

    for (const post of posts) {
        if (!isEnglish(post.content)) {
        console.log(`Skipping non-English post by: ${post.authorName}`);
        continue;
      }

      const comment = await generateComment(
        post.content,
        post.authorName,
        '',
        agentPersona
      );

      await db.savePendingComment({
        authorName: post.authorName,
        authorTitle: 'Facebook Group Member',
        postContent: post.content.substring(0, 500),
        comment,
        postUrl: post.postUrl,
        channel: 'facebook'
        });

      await db.logAgentAction(
        AGENT_NAME,
        'generate_comment',
        null,
        post.postUrl,
        { comment, authorName: post.authorName },
        'success'
      );

      console.log(`\nPost by: ${post.authorName}`);
      console.log(`Comment: ${comment}`);
      console.log(`URL: ${post.postUrl}`);
      console.log('Draft saved.');

      await randomDelay(5000, 10000);
    }

    await randomDelay(15000, 25000);
  }

  console.log('\nFacebook agent complete.');
  await browser.close();
}

run().catch(console.error);