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
    { id: 'pending:report', pendingCommentId: 'report', source: 'legacy', channel: 'ads_report', kind: 'report', title: 'Google Ads report', body: '# Google Ads evidence\n\n**CURRENTLY ENABLED CAMPAIGNS**\n\n- **Search**: 3 clicks.\n- <img src=x onerror="window.injected=true">\n\n---\n\nPrepared by Penny.', media: [], canReview: true, decisionMode: 'legacy', createdAt: '2026-10-19T17:20:00Z' },
  ] };
}

async function setup(t, { role = 'admin', width = 1440, motion = 'no-preference', askReview = false, previewError = 0, decisionError = 0, reviewDelay = 0, accountDelay = 0, accountError = 0 } = {}) {
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
      else if (url.pathname === '/api/v1/max/spreadsheet/scope') data = { tenant_id: 10, actor_id: 1, ao_id: null, can_approve: false, aos: [{ id: 24, name: 'Tony' }, { id: 20, name: 'Mike' }] };
      else if (url.pathname === '/api/v1/max/spreadsheet/proposals') { status = 503; data = { error: '42P01' }; }
      else if (url.pathname === '/api/v1/max/account-review') {
        if (accountDelay) await new Promise(resolve => setTimeout(resolve, accountDelay));
        status = accountError || 200;
        data = accountError ? { error: 'account_review_unavailable' } : { tenant_id: 10, ao_id: 24, ao_name: 'Tony', summary: { total: 2, overdue: 1, help_requested: 1 }, accounts: [
          { prospect_id: 'account-a', company_name: 'North River Properties', assigned_ao_id: '24', current_status: 'decision_maker_reached', next_action: 'follow_up', next_action_date: '2026-10-20', last_touch_at: '2026-10-19T15:00:00Z', priority: 'high', help_requested: true, help_reason: 'Review the facilities proposal.', latest_note_preview: 'Operations contact requested a follow-up.' },
          { prospect_id: 'account-b', company_name: 'Elm Street Offices', assigned_ao_id: '24', current_status: 'call_attempted', next_action: 'call', overdue: true, last_touch_at: '2026-10-16T15:00:00Z' },
        ] };
      } else if (url.pathname.startsWith('/api/v1/max/account-review/')) data = { tenant_id: 10, ao_id: 24, account: { summary: { assigned_ao_id: 24, why_account_matters: 'Recurring facility cleaning opportunity.' }, contact: { name: 'Alex Morgan', role: 'Operations manager', email: 'operations@example.test', phone: '603-555-0100' }, history: [{ created_at: '2026-10-19T15:00:00Z', activity_type: 'call', outcome: 'decision_maker_reached', notes: '<img src=x onerror="window.injected=true"> is a literal source note.' }] } };
      else if (url.pathname === '/api/v1/max/workspace/open') data = { sessionId: 'fixture-session', opening: { fullText: 'Test opening' }, suggestions: [] };
      else if (url.pathname === '/api/v1/max/workspace/review-navigation' || url.pathname === '/api/v1/max/workspace/ask') {
        const body = JSON.parse(request.postData() || '{}');
        const { parseReviewNavigation, resolveReviewNavigation } = require('../../packages/max/workspace/ReviewNavigation');
        const intent = parseReviewNavigation(body.question, body.reviewNavigation);
        data = intent ? resolveReviewNavigation(intent, { tenantId: 10, aos: [{ id: 24, name: 'Tony' }, { id: 20, name: 'Mike' }] }) : { sessionId: 'fixture-session', prose: 'Test answer', suggestions: [], ...(askReview ? { uiAction: { type: 'open_content_review', tenantId: '10' } } : {}) };
      }
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
  assert.equal(writes.filter(write => !write.path.endsWith('/review-navigation')).length, 0);
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
  await page.select('#crChannel', 'ads_report');
  assert.match(await page.$eval('.cr-text-preview', el => el.textContent), /Saved report/);
  assert.equal(await page.$eval('.cr-primary', el => el.textContent), 'Approve report');
  assert.equal(await page.$$('.cr-report-body h4').then(nodes => nodes.length), 2);
  assert.equal(await page.$$('.cr-report-body li').then(nodes => nodes.length), 2);
  assert.equal(await page.$('.cr-report-body img'), null);
  assert.equal(await page.evaluate(() => window.injected), undefined);
  assert.match(await page.$eval('.cr-created', el => el.textContent), /2026/);
  await page.focus('#crChannel'); await page.keyboard.press('Escape');
  assert.equal(await page.$eval('#contentReview', el => el.hidden), true);
  await page.click('a[href="/command-deck#content-review"]');
  await page.waitForSelector('.cr-draft');
  assert.equal(writes.filter(write => !write.path.endsWith('/review-navigation')).length, 0);
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
  assert.equal(blocked.writes.filter(write => !write.path.endsWith('/review-navigation')).length, 0);
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
  const response = delayed.page.waitForResponse(res => new URL(res.url()).pathname === '/api/content-review');
  await delayed.page.evaluate(() => document.dispatchEvent(new CustomEvent('pulseforge:tenant-changed')));
  await response;
  assert.equal(await delayed.page.$eval('#contentReview', el => el.hidden), true);
  assert.equal(await delayed.page.$eval('#crDetail', el => el.textContent), '');
  assert.deepEqual([...errors, ...delayed.errors], []);
});

test('Tony account picker opens real account review independently of unavailable spreadsheet previews', async t => {
  const { page, errors, writes, reads } = await setup(t);
  await page.click('#cdSpatialMax');
  await page.waitForSelector('[data-spreadsheet-ao]');
  await page.select('[data-spreadsheet-ao]', '24');
  await page.click('[data-spreadsheet-refresh]');
  await page.waitForFunction(() => document.querySelector('[data-spreadsheet-feedback]')?.textContent.includes('still review assigned accounts'));
  assert.doesNotMatch(await page.$eval('#mxThread', el => el.textContent), /42P01/);
  await page.click('[data-account-review]');
  await page.waitForSelector('#arDetail .ar-event');
  assert.equal(await page.$eval('#maxWorkspace', el => el.hidden), true);
  assert.equal(await page.$eval('#arTitle', el => el.textContent), 'Tony’s accounts');
  assert.match(await page.$eval('#arSubtitle', el => el.textContent), /2 assigned/);
  assert.match(await page.$eval('#arDetail', el => el.textContent), /North River Properties|Alex Morgan|Operations manager|Review the facilities proposal/);
  assert.equal(await page.$('#arDetail img'), null);
  assert.equal(await page.evaluate(() => window.injected), undefined);
  assert.equal(writes.filter(write => !write.path.endsWith('/workspace/open') && !write.path.endsWith('/review-navigation')).length, 0);
  assert.ok(reads.includes('/api/v1/max/account-review/account-a'));
  if (process.env.LIVING_COMMAND_QA_DIR) await page.screenshot({ path: path.join(process.env.LIVING_COMMAND_QA_DIR, 'living-command-account-review.png'), fullPage: true });
  await page.select('#arFilter', 'overdue');
  assert.equal(await page.$eval('#arDetail h3', el => el.textContent), 'Elm Street Offices');
  await page.focus('#arSearch'); await page.keyboard.press('Escape');
  assert.equal(await page.$eval('#accountReview', el => el.hidden), true);
  assert.deepEqual(errors, []);
});

test('typing review Tony’s accounts routes to verified account data before generic Max chat', async t => {
  const { page, errors, writes, reads } = await setup(t);
  await page.click('#cdSpatialMax');
  await page.waitForSelector('[data-spreadsheet-ao]');
  await page.type('#mxAskInput', 'Review Tony’s accounts');
  await page.click('#mxAskSend');
  await page.waitForSelector('#arDetail .ar-event');
  assert.equal(await page.$eval('#arTitle', el => el.textContent), 'Tony’s accounts');
  assert.equal(writes.filter(write => write.path.endsWith('/ask')).length, 1);
  assert.equal(reads.filter(path => path.includes('/spreadsheet/proposals')).length, 0);
  assert.deepEqual(errors, []);
});

test('mobile account review fits the viewport and workspace changes clear late responses', async t => {
  const { page, errors, writes } = await setup(t, { width: 390, motion: 'reduce' });
  await page.click('#cdAskForm button[type="submit"]'); await page.waitForSelector('[data-spreadsheet-ao]');
  await page.select('[data-spreadsheet-ao]', '24'); await page.click('[data-account-review]');
  await page.waitForSelector('#arDetail .ar-event');
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  assert.equal(await page.$eval('#accountReview', el => el.contains(document.activeElement)), true);
  if (process.env.LIVING_COMMAND_QA_DIR) await page.screenshot({ path: path.join(process.env.LIVING_COMMAND_QA_DIR, 'living-command-account-review-mobile.png'), fullPage: true });
  await page.evaluate(() => document.dispatchEvent(new CustomEvent('pulseforge:tenant-changed')));
  assert.equal(await page.$eval('#accountReview', el => el.hidden), true);
  assert.equal(await page.$eval('#arDetail', el => el.textContent), '');
  assert.equal(writes.filter(write => !write.path.endsWith('/workspace/open') && !write.path.endsWith('/review-navigation')).length, 0);
  const delayed = await setup(t, { accountDelay: 300 });
  await delayed.page.click('#cdSpatialMax'); await delayed.page.waitForSelector('[data-spreadsheet-ao]');
  await delayed.page.select('[data-spreadsheet-ao]', '24'); await delayed.page.click('[data-account-review]');
  await delayed.page.waitForFunction(() => !document.getElementById('accountReview').hidden);
  const response = delayed.page.waitForResponse(res => new URL(res.url()).pathname === '/api/v1/max/account-review');
  await delayed.page.evaluate(() => document.dispatchEvent(new CustomEvent('pulseforge:tenant-changed'))); await response;
  assert.equal(await delayed.page.$eval('#accountReview', el => el.hidden), true);
  assert.equal(await delayed.page.$eval('#arDetail', el => el.textContent), '');
  assert.deepEqual([...errors, ...delayed.errors], []);
});
