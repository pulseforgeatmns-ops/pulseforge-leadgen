'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const puppeteer = require('puppeteer');
const root = path.join(__dirname, '../..');

function model(clientId = 10) {
  return {
    meta: { tenantId: String(clientId), generatedAt: '2026-10-19T12:00:00Z' },
    spatialOverview: {
      generatedAt: '2026-10-19T12:00:00Z', maxAnchor: {},
      listFallback: ['content', 'acquisition', 'clients', 'campaigns'].map(id => ({ id, label: id[0].toUpperCase() + id.slice(1), priority: 'normal', summary: 'Monitoring activity', intelligence: false })),
      domains: ['content', 'acquisition', 'clients', 'campaigns'].map((id, i) => ({
        id, label: id[0].toUpperCase() + id.slice(1), priority: 'normal',
        position: { slot: ['top', 'left', 'right', 'bottom'][i] },
        summary: { lines: ['Monitoring activity'], compressed: 'Monitoring activity' },
        intelligence: { active: false }, actions: [], drawer: { sections: [] },
      })),
    },
    livingBrief: {
      clientId, generatedAt: '2026-10-19T12:00:00Z', pipeline: { total: 17, new24h: 2, warm: 3, booked: 1 }, approvals: { pending: 4 },
      googleAds: { available: true, currency: 'USD', window: { start: '2026-10-13', end: '2026-10-19' }, campaigns: [
        { name: 'Search <img src=x onerror="window.injected=true">', status: 'ENABLED', spend: 20.42, clicks: 3, conversions: null },
        { name: 'PMax', status: 'PAUSED', spend: 300, clicks: 40, conversions: 10 },
      ] },
    },
  };
}

function reviewModel() {
  return { clientId: 10, pendingCount: 3, warnings: [], items: [
    { id: 'social:post-a', artifactId: 'post-a', source: 'canonical', channel: 'facebook_page', title: 'Ready for the week ahead', body: 'A prepared workspace makes Monday easier.\n\nOur team handles the details so yours can focus.', media: [{ url: '/fixture-content.svg', alt: 'A clean workspace', type: 'image' }], canReview: true, decisionMode: 'exact_preview' },
    { id: 'pending:old', pendingCommentId: 'old', source: 'legacy', channel: 'google_business', title: 'Older social draft', body: 'A text-only post waiting for verification.', media: [], canReview: false, decisionMode: 'binding_required', blocker: 'This older draft needs a verified post version and destination before it can be approved.' },
    { id: 'pending:comment', pendingCommentId: 'comment', source: 'legacy', channel: 'linkedin', title: 'A prospect', body: '<img src=x onerror="window.injected=true"> is displayed as draft text.', originalPost: 'A prospect’s original post.', media: [], canReview: true, decisionMode: 'legacy' },
  ] };
}

async function setup(t, { role = 'admin', width = 1440, motion = 'no-preference', askReview = false, previewError = 0, decisionError = 0, reviewDelay = 0 } = {}) {
  const browser = await puppeteer.launch({ executablePath: process.env.CHROMIUM_PATH || undefined, headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.setViewport({ width, height: 1000 });
  await page.emulateMediaFeatures([{ name: 'prefers-reduced-motion', value: motion }]);
  const errors = [], writes = [], reads = [];
  const review = reviewModel();
  page.on('pageerror', error => errors.push(error.message));
  await page.setRequestInterception(true);
  page.on('request', async request => {
    const url = new URL(request.url());
    if (url.origin !== 'http://localhost') return request.abort();
    if (url.pathname.startsWith('/api/')) {
      let data = {}, status = 200;
      if (request.method() === 'GET') reads.push(url.pathname);
      if (url.pathname === '/api/me') data = { user: { name: 'Jacob', role }, active_client_id: 10, client: { display_name: 'Test workspace' } };
      else if (url.pathname === '/api/clients') data = { active_client_id: 10, clients: [{ id: 10, name: 'Test workspace' }, { id: 11, name: 'Other workspace' }] };
      else if (url.pathname === '/api/v1/command-deck') data = model();
      else if (url.pathname === '/api/v1/max/workspace/open') data = { sessionId: 'fixture-session', opening: { fullText: 'Test opening' }, suggestions: [] };
      else if (url.pathname === '/api/v1/max/workspace/ask') data = { sessionId: 'fixture-session', prose: 'Test answer', suggestions: [], ...(askReview ? { uiAction: { type: 'open_content_review' } } : {}) };
      else if (url.pathname === '/api/content-review') { if (reviewDelay) await new Promise(resolve => setTimeout(resolve, reviewDelay)); data = { ...review, pendingCount: review.items.length }; }
      else if (url.pathname === '/api/paige/social/post-a/preview') {
        status = previewError || 200;
        data = previewError ? { error: 'social_account_invalid' } : { artifact: { body: review.items[0].body, mediaRefs: ['/fixture-content.svg'] }, account: { id: 'account-10', platform: 'facebook_page', displayName: 'Test business page' }, approvalHash: 'exact-preview-hash' };
      } else if (url.pathname === '/api/paige/social/post-a/decision') {
        status = decisionError || 200;
        data = decisionError ? { error: 'approval_preview_changed' } : { ok: true };
        if (!decisionError) review.items = review.items.filter(item => item.id !== 'social:post-a');
      }
      if (request.method() !== 'GET') writes.push({ path: url.pathname, body: JSON.parse(request.postData() || '{}') });
      return request.respond({ status, contentType: 'application/json', body: JSON.stringify(data) });
    }
    if (url.pathname === '/fixture-content.svg') return request.respond({ status: 200, contentType: 'image/svg+xml', body: '<svg xmlns="http://www.w3.org/2000/svg" width="700" height="280"><rect width="700" height="280" fill="#182743"/><path d="M0 220L700 110V280H0Z" fill="#40527a"/><text x="44" y="100" font-family="sans-serif" font-size="28" fill="#f3efe4">A fresh start to the week.</text><text x="44" y="145" font-family="sans-serif" font-size="16" fill="#d4af5a">CONTENT PREVIEW · TEST FIXTURE</text></svg>' });
    const file = path.join(root, 'public', url.pathname === '/command-deck' ? 'command-deck.html' : url.pathname);
    if (!fs.existsSync(file) || !fs.statSync(file).isFile()) return request.respond({ status: 404, body: '' });
    const extension = path.extname(file);
    return request.respond({ status: 200, contentType: extension === '.css' ? 'text/css' : extension === '.js' ? 'application/javascript' : 'text/html', body: fs.readFileSync(file) });
  });
  await page.goto('http://localhost/command-deck');
  await page.waitForSelector('.lc-brief-line');
  await page.waitForSelector('.pf-living-shell');
  return { page, errors, writes, reads };
}

test('desktop opening brief, scoped Ads facts and real Max conversation path', async t => {
  const { page, errors, writes } = await setup(t);
  assert.deepEqual(await page.$$eval('.pf-nav-links a', links => links.map(link => link.textContent)), ['Command', 'Pipeline', 'Clients']);
  const ads = await page.$eval('.lc-brief-line:nth-child(2)', element => element.textContent);
  assert.match(ads, /20.42/);
  assert.match(ads, /conversions unavailable/);
  assert.doesNotMatch(ads, /300|40 clicks|10 platform/);
  assert.equal(await page.evaluate(() => window.injected), undefined);
  assert.match(await page.$eval('#lcBriefTitle', element => element.textContent), /Jacob/);
  assert.equal(await page.$eval('#pfNeedsCount', element => element.textContent), '4');
  const before = await page.$eval('.lc-ring-one', element => getComputedStyle(element).transform);
  await new Promise(resolve => setTimeout(resolve, 140));
  assert.notEqual(await page.$eval('.lc-ring-one', element => getComputedStyle(element).transform), before);
  if (process.env.LIVING_COMMAND_QA_DIR) await page.screenshot({ path: path.join(process.env.LIVING_COMMAND_QA_DIR, 'living-command-desktop.png'), fullPage: true });
  const answerResponse = page.waitForResponse(response => response.url().endsWith('/api/v1/max/workspace/ask'));
  await page.click('.lc-brief-line:nth-child(2)');
  await answerResponse;
  await page.waitForFunction(() => !document.getElementById('maxWorkspace').hidden);
  await page.waitForFunction(() => !document.body.classList.contains('lc-thinking'));
  assert.ok(writes.find(write => write.path === '/api/v1/max/workspace/open'));
  const question = writes.find(write => write.path.endsWith('/ask'));
  assert.match(question.body.question, /Penny/);
  assert.equal(question.body.context.tenantId, '10');
  await page.click('button[data-mx-close]');
  const openedAgain = page.waitForResponse(response => response.url().endsWith('/api/v1/max/workspace/open'));
  await page.focus('#cdSpatialMax');
  await page.keyboard.press('Enter');
  await openedAgain;
  assert.equal(writes.filter(write => write.path.endsWith('/ask')).length, 1);
  assert.deepEqual(errors, []);
});

test('mobile and reduced motion remain usable; tenant transition clears prior facts', async t => {
  const { page, errors } = await setup(t, { role: 'client', width: 390, motion: 'reduce' });
  assert.deepEqual(await page.$$eval('.pf-nav-links a', links => links.map(link => link.textContent)), ['Command', 'Pipeline', 'My Business']);
  assert.equal(await page.$('#pfTenantSelect'), null);
  assert.equal(await page.$eval('.lc-ring-one', element => getComputedStyle(element).animationName), 'none');
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  if (process.env.LIVING_COMMAND_QA_DIR) await page.screenshot({ path: path.join(process.env.LIVING_COMMAND_QA_DIR, 'living-command-mobile.png'), fullPage: true });
  await page.evaluate(() => window.LivingCommand.clear());
  assert.equal(await page.$eval('#lcBriefLines', element => element.children.length), 0);
  assert.equal(await page.$eval('[data-lc-ask]', element => element.disabled), true);
  await page.evaluate(() => window.LivingCommand.unavailable());
  assert.equal(await page.$eval('#lcRefresh', element => element.disabled), false);
  assert.match(await page.$eval('#lcBriefIntro', element => element.textContent), /couldn’t load/);
  assert.deepEqual(errors, []);
});

test('content review opens visual drafts directly, without chat or approval side effects', async t => {
  const { page, errors, writes } = await setup(t);
  await page.click('.lc-brief-line:nth-child(3)');
  await page.waitForFunction(() => !document.getElementById('contentReview').hidden && document.querySelector('.cr-primary')?.disabled === false);
  assert.equal(await page.$eval('#maxWorkspace', el => el.hidden), true);
  assert.equal(writes.length, 0);
  assert.equal(await page.$eval('.cr-media', el => el.getAttribute('src')), '/fixture-content.svg');
  assert.match(await page.$eval('.cr-destination', el => el.textContent), /Test business page/);
  if (process.env.LIVING_COMMAND_QA_DIR) await page.screenshot({ path: path.join(process.env.LIVING_COMMAND_QA_DIR, 'living-command-content-review.png'), fullPage: true });
  await page.select('#crChannel', 'google_business');
  assert.match(await page.$eval('.cr-text-preview', el => el.textContent), /No image attached/);
  assert.equal(await page.$eval('.cr-primary', el => el.disabled), true);
  assert.match(await page.$eval('.cr-blocker', el => el.textContent), /verified post version/);
  await page.select('#crChannel', 'linkedin');
  assert.equal(await page.$('.cr-post-body img'), null);
  assert.equal(await page.evaluate(() => window.injected), undefined);
  assert.equal(await page.$eval('.cr-primary', el => el.textContent), 'Approve & post');
  await page.focus('#crChannel'); await page.keyboard.press('Escape');
  assert.equal(await page.$eval('#contentReview', el => el.hidden), true);
  await page.click('a[href="/command-deck#content-review"]');
  await page.waitForSelector('.cr-draft');
  assert.equal(writes.length, 0);
  assert.deepEqual(errors, []);
});

test('typing a review request to Max follows the review directive and closes chat', async t => {
  const { page, errors, writes } = await setup(t, { askReview: true });
  await page.click('#cdSpatialMax');
  await page.waitForFunction(() => !document.getElementById('maxWorkspace').hidden);
  await page.type('#mxAskInput', 'Show me the content waiting for my approval');
  await page.click('#mxAskSend');
  await page.waitForFunction(() => !document.getElementById('contentReview').hidden && document.querySelector('.cr-primary')?.disabled === false);
  assert.equal(await page.$eval('#maxWorkspace', el => el.hidden), true);
  assert.equal(writes.filter(write => write.path.endsWith('/ask')).length, 1);
  assert.equal(writes.filter(write => /decision|publish|approvals/.test(write.path)).length, 0);
  assert.deepEqual(errors, []);
});

test('approval records only the exact displayed version and keeps publishing separate', async t => {
  const { page, errors, writes } = await setup(t);
  await page.click('.lc-brief-line:nth-child(3)');
  await page.waitForFunction(() => document.querySelector('.cr-primary')?.disabled === false);
  await page.click('.cr-primary');
  await page.waitForFunction(() => document.getElementById('crNotice').textContent === 'Approval recorded.');
  const decisions = writes.filter(write => write.path.endsWith('/decision'));
  assert.equal(decisions.length, 1);
  assert.deepEqual(decisions[0].body, { decision: 'approve', accountId: 'account-10', expectedApprovalHash: 'exact-preview-hash' });
  assert.equal(writes.filter(write => /publish|request|maintain|cycle/.test(write.path)).length, 0);
  assert.equal(await page.$('[data-content-id="social:post-a"]'), null);
  assert.deepEqual(errors, []);
});

test('changed drafts and missing connections cannot be silently approved', async t => {
  const stale = await setup(t, { decisionError: 409 });
  await stale.page.click('.lc-brief-line:nth-child(3)');
  await stale.page.waitForFunction(() => document.querySelector('.cr-primary')?.disabled === false);
  await stale.page.click('.cr-primary');
  await stale.page.waitForFunction(() => document.getElementById('crNotice').textContent.includes('changed'));
  assert.ok(await stale.page.$('[data-content-id="social:post-a"]'));
  assert.equal(stale.writes.filter(write => write.path.endsWith('/publish')).length, 0);
  const blocked = await setup(t, { previewError: 409 });
  await blocked.page.click('.lc-brief-line:nth-child(3)');
  await blocked.page.waitForFunction(() => document.querySelector('.cr-blocker')?.textContent.includes('connection'));
  assert.equal(await blocked.page.$eval('.cr-primary', el => el.disabled), true);
  assert.equal(blocked.writes.length, 0);
  assert.deepEqual([...stale.errors, ...blocked.errors], []);
});

test('mobile review fits the viewport and tenant changes discard late draft responses', async t => {
  const { page, errors } = await setup(t, { width: 390, motion: 'reduce' });
  await page.click('.lc-brief-line:nth-child(3)');
  await page.waitForFunction(() => document.querySelector('.cr-primary')?.disabled === false);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  assert.equal(await page.$eval('#contentReview', el => el.contains(document.activeElement)), true);
  if (process.env.LIVING_COMMAND_QA_DIR) await page.screenshot({ path: path.join(process.env.LIVING_COMMAND_QA_DIR, 'living-command-content-review-mobile.png'), fullPage: true });
  await page.evaluate(() => document.dispatchEvent(new CustomEvent('pulseforge:tenant-changed')));
  assert.equal(await page.$eval('#contentReview', el => el.hidden), true);
  assert.equal(await page.$eval('#crDetail', el => el.textContent), '');
  const delayed = await setup(t, { reviewDelay: 300 });
  await delayed.page.click('.lc-brief-line:nth-child(3)');
  const response = delayed.page.waitForResponse(res => res.url().endsWith('/api/content-review'));
  await delayed.page.evaluate(() => document.dispatchEvent(new CustomEvent('pulseforge:tenant-changed')));
  await response;
  assert.equal(await delayed.page.$eval('#contentReview', el => el.hidden), true);
  assert.equal(await delayed.page.$eval('#crDetail', el => el.textContent), '');
  assert.deepEqual([...errors, ...delayed.errors], []);
});
