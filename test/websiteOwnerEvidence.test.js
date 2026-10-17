'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const { extractWebsiteOwnerCandidates, LIMITS } = require('../utils/websiteOwnerEvidence');
const { applyScoutEnrichment } = require('../utils/babrunScoutContactEvidence');
const { governedContactReason } = require('../utils/governedContactEligibility');
const { runScoutWaterfall, loadScoutSave, recordingDb, verificationReceipt, leadFixture } = require('./helpers/babrunScoutIntake');

const domain = 'fixturepainting.example';
const ownerEmail = `ada@${domain}`;
const sourceUrl = `https://${domain}/about`;
const page = text => ({ url: sourceUrl, text });
const card = (name = 'Ada Lovelace', title = 'Owner', email = ownerEmail, extra = '') =>
  `<div class="team-member"><h3>${name}</h3><p class="role">${title}</p><a href="mailto:${email}">${email}</a>${extra}</div>`;
const json = value => `<script type="application/ld+json">${JSON.stringify(value)}</script>`;
const person = (extra = {}) => ({ '@type': 'Person', name: 'Ada Lovelace', jobTitle: 'Founder', email: ownerEmail,
  worksFor: { '@type': 'Organization', url: `https://${domain}` }, ...extra });
const extract = html => extractWebsiteOwnerCandidates([page(`<html><body>${html}</body></html>`)], domain);

// Actual scrape function and actual crawler, with every HTTP boundary replaced.
// An optional source file allows reproducing the regression on the base commit.
function scrapeFixture({ pagesByPath = {}, redirects = {}, sourceFile } = {}) {
  const filename = path.join(__dirname, '../leadgen.js');
  const source = fs.readFileSync(sourceFile || process.env.SCOUT_WEBSITE_SOURCE || filename, 'utf8');
  const start = source.indexOf('function filterScrapedWebsiteEmails(');
  const end = source.indexOf('\nasync function fetchProbateWebsiteSignals(', start);
  assert.ok(start >= 0 && end > start);
  const calls = [];
  const axios = { get: async (url, options) => {
    assert.equal(new URL(url).hostname.replace(/^www\./, ''), domain, 'no provider or foreign host calls');
    calls.push({ url, options });
    const data = pagesByPath[new URL(url).pathname] ?? '<html>No contact listed</html>';
    return { status: 200, data, request: { res: { responseURL: redirects[new URL(url).pathname] || url } } };
  } };
  const context = { require: createRequire(filename), axios, module: { exports: {} } };
  vm.runInNewContext(source.slice(start, end) + '\nmodule.exports = { scrapeWebsiteEmail, filterScrapedWebsiteEmails };', context);
  return { scrape: (options = { ownerEvidence: true }) => context.module.exports.scrapeWebsiteEmail(domain, options),
    scrapeDefault: () => context.module.exports.scrapeWebsiteEmail(domain), calls };
}

const roleFallback = { email: `info@${domain}`, contact: '', title: '', sourceUrl: `https://${domain}/` };
function withRoleHomepage(html, extra = {}) {
  return scrapeFixture({ pagesByPath: { '/': `<html>${roleFallback.email}</html>`, '/about': `<html>${html}</html>` }, ...extra });
}

test('owner outranks the first role email using the same eight crawl requests', async () => {
  const fixture = withRoleHomepage(card());
  const result = await fixture.scrape();
  assert.equal(result.email, ownerEmail);
  assert.equal(result.contact, 'Ada Lovelace');
  assert.equal(result.title, 'Owner');
  assert.equal(result.sourceUrl, sourceUrl);
  assert.equal(result.websiteOwnerEvidence.record, card());
  assert.deepEqual(fixture.calls.map(call => new URL(call.url).pathname),
    ['/', '/contact', '/contact-us', '/about', '/about-us', '/team', '/staff', '/attorneys']);
  assert.equal(fixture.calls.length, 8);
  for (const { options } of fixture.calls) assert.equal(options.timeout, 5000);
});

test('owner ranking is explicit opt-in and cannot leak into default or disabled calls', async () => {
  const fixture = withRoleHomepage(card());
  for (const options of [{}, { ownerEvidence: false }, { ownerEvidence: 'true' }]) {
    assert.deepEqual(JSON.parse(JSON.stringify(await fixture.scrape(options))), roleFallback);
  }
  assert.equal((await fixture.scrape({ ownerEvidence: true })).email, ownerEmail);
  assert.deepEqual(JSON.parse(JSON.stringify(await fixture.scrape({}))), roleFallback);
  assert.deepEqual(JSON.parse(JSON.stringify(await fixture.scrapeDefault())), roleFallback);
  assert.equal(fixture.calls.length, 8 * 6);
});

test('standard intake opts in only for tenant 13 and chain callers must opt in independently', async () => {
  for (const clientId of [1, 10, 13, 17]) {
    const { scrapeOptions } = await runScoutWaterfall(leadFixture(), { clientId, hunter: null });
    assert.equal(scrapeOptions.length, 1);
    assert.equal(scrapeOptions[0].ownerEvidence, clientId === 13);
  }
  const source = fs.readFileSync(path.join(__dirname, '../leadgen.js'), 'utf8');
  const body = source.slice(source.indexOf('async function runEnrichmentChain('), source.indexOf('\nmodule.exports = {'));
  const calls = [];
  const context = { module: { exports: {} }, CONFIG: { clientId: 13, jobTitle: 'original' },
    enrichWithProspeo: async () => { calls.push('prospeo'); return null; },
    enrichWithHunter: async () => { calls.push('hunter'); return null; },
    scrapeWebsiteEmail: async (d, options) => { calls.push({ domain: d, options }); return null; },
    recordEmailEnrichmentMethod: async () => {},
  };
  vm.runInNewContext(body + '\nmodule.exports = runEnrichmentChain;', context);
  await context.module.exports(domain);
  assert.equal(calls[2].options.ownerEvidence, undefined, 'mutable CONFIG tenant does not opt in a generic caller');
  await context.module.exports(domain, 'owner', { ownerEvidence: true });
  assert.equal(calls[5].options.ownerEvidence, true);
  await context.module.exports(domain);
  assert.equal(calls[8].options.ownerEvidence, undefined);
  assert.equal(context.CONFIG.jobTitle, 'original');
  assert.deepEqual(calls.filter(x => typeof x === 'string'), ['prospeo', 'hunter', 'prospeo', 'hunter', 'prospeo', 'hunter']);
});

test('actual promotion opts in only for Babrun, independently of shared Scout CONFIG', async t => {
  const filename = path.join(__dirname, '../scripts/promoteUnenriched.js');
  const source = fs.readFileSync(filename, 'utf8');
  const body = source.slice(source.indexOf('async function promoteRecord('), source.indexOf('\nasync function run()'));
  const context = { module: { exports: {} }, console: { log() {} }, require: createRequire(filename),
    configuredServiceAreas: () => [], promotionServiceAreaMatch: () => null };
  vm.runInNewContext(body + '\nmodule.exports = promoteRecord;', context);
  for (const clientId of [10, 13, 17]) await t.test(`tenant ${clientId}`, async () => {
    const calls = [];
    const result = await context.module.exports({ id: 1, client_id: clientId, domain }, {
      loadClientConfig: async () => ({}), verify: async () => { throw new Error('unexpected verification'); },
      enrich: async (...args) => { calls.push(args); return null; },
      db: { query: async sql => { assert.match(sql, /UPDATE scout_unenriched/); return { rows: [] }; } },
    });
    assert.equal(result.reason, 'email_unresolved');
    assert.equal(calls.length, 1); assert.equal(calls[0][0], domain); assert.equal(calls[0][1], 'owner');
    assert.equal(calls[0][2].ownerEvidence, clientId === 13);
  });
});

test('explicit HTML and microdata preserve exact source slices with decoded identity', () => {
  for (const record of [card('Ada Lovelace', 'Founder &amp; Owner'),
    `<article itemscope itemtype="https://schema.org/Person"><span itemprop="name">Ada Lovelace</span><meta itemprop="jobTitle" content="Co-Founder"><link itemprop="worksFor" href="https://${domain}"><a itemprop="email" href="mailto:${ownerEmail}">Email Ada</a></article>`]) {
    const html = `<html>prefix${record}suffix</html>`;
    const [result] = extractWebsiteOwnerCandidates([page(html)], domain);
    assert.ok(result);
    const evidence = result.websiteOwnerEvidence;
    assert.equal(evidence.record, record);
    assert.equal(html.slice(evidence.start, evidence.end), record);
    assert.equal(evidence.sourceUrl, sourceUrl);
    assert.equal(evidence.contactName, 'Ada Lovelace');
    assert.equal(Object.hasOwn(result, 'verified'), false);
  }
});

test('multiple separate staff records select only a bound owner and ignore founder mentions in employee bios', () => {
  const html = card('Grace Hopper', 'Office Manager', `grace@${domain}`, '<p>Works with our founder Ada Lovelace.</p>') + card();
  assert.equal(extract(html)[0].email, ownerEmail);
  assert.equal(extract(card('Grace Hopper', 'Office Manager', `grace@${domain}`, '<p>Founder Ada Lovelace built the company.</p>')).length, 0);
});

test('JSON-LD binds fields within one Person and retains exact script plus pointer', () => {
  const record = json({ '@context': 'https://schema.org', '@graph': [person({ name: 'Grace Hopper', jobTitle: 'Office Manager', email: `grace@${domain}` }), person()] });
  const [result] = extract(record);
  assert.equal(result.email, ownerEmail);
  assert.equal(result.websiteOwnerEvidence.record, record);
  assert.equal(result.websiteOwnerEvidence.jsonPointer, '/@graph/1');
  assert.equal(extract(json({ '@type': 'Organization', url: `https://${domain}`, founder: person({ worksFor: undefined }) }))[0].email, ownerEmail);
  assert.equal(extract(json({ '@type': 'Organization', url: `https://${domain}`, employee: [person({ worksFor: undefined })] }))[0].email, ownerEmail);
});

test('explicitly observed personal-provider email remains subject to the existing local-part check', () => {
  assert.equal(extract(card('Ada Lovelace', 'Founder', 'ada.lovelace@gmail.com'))[0].email, 'ada.lovelace@gmail.com');
  assert.equal(extract(card('Ada Lovelace', 'Founder', 'randomname@gmail.com')).length, 0);
});

const rejectedRecords = [
  ['page-wide name and title with unrelated footer email', '<h2>Ada Lovelace</h2><p>Founder</p>' + `<footer><a href="mailto:${ownerEmail}">${ownerEmail}</a></footer>`],
  ['two staff names', card('Ada Lovelace', 'Owner', ownerEmail, '<h3>Ada Byron</h3>')],
  ['two name fields', card('Ada Lovelace', 'Owner', ownerEmail, '<span class="person-name">Ada Byron</span>')],
  ['multiple addresses', card('Ada Lovelace', 'Owner', ownerEmail, `<a href="mailto:grace@${domain}">Grace</a>`)],
  ['foreign address is still ambiguity', card('Ada Lovelace', 'Owner', ownerEmail, '<a href="mailto:staff@other.example">Staff</a>')],
  ['role address is not personal evidence', card('Ada Lovelace', 'Owner', `info@${domain}`)],
  ['untrusted email domain', card('Ada Lovelace', 'Owner', 'ada@other.example')],
  ['directory email domain', card('Ada Lovelace', 'Owner', 'ada@linkedin.com')],
  ['employee address in owner record', card('Ada Lovelace', 'Owner', `grace@${domain}`)],
  ['assistant title', card('Ada Lovelace', 'Assistant to Owner')],
  ['former owner', card('Ada Lovelace', 'Former Owner')],
  ['vice-president title', card('Ada Lovelace', 'Vice President')],
  ['owner keyword in sentence', card('Ada Lovelace', 'Works with the founder')],
  ['unlabeled nested owner refers to someone else', `<div class="team-member"><h3>Ada Lovelace</h3><p>Office Manager</p><div><span>Owner</span><span>Grace Hopper</span></div><a href="mailto:${ownerEmail}">${ownerEmail}</a></div>`],
  ['labeled nested owner refers to someone else', `<div class="team-member"><h3>Ada Lovelace</h3><p>Office Manager</p><section><span class="role">Owner</span><span>Grace Hopper</span></section><a href="mailto:${ownerEmail}">${ownerEmail}</a></div>`],
  ['missing explicit title', card('Ada Lovelace', '')],
  ['generic name', card('Our Owner', 'Owner')],
  ['email in arbitrary attribute only', `<div class="team-member" data-email="${ownerEmail}"><h3>Ada Lovelace</h3><p>Owner</p></div>`],
  ['page text email without explicit email field', `<div class="team-member"><h3>Ada Lovelace</h3><p>Owner</p>${ownerEmail}</div>`],
  ['multi-recipient mailto', card().replace(`mailto:${ownerEmail}`, `mailto:${ownerEmail},grace@${domain}`)],
  ['neighbor email must not fill incomplete owner', `<div class="team-member"><h3>Ada Lovelace</h3><p>Owner</p></div><div class="team-member"><a href="mailto:${ownerEmail}">Email</a></div>`],
  ['nested employee cannot supply owner email', `<div class="team-member"><h3>Ada Lovelace</h3><p>Owner</p><div class="team-member"><h3>Grace Hopper</h3><p>Office Manager</p><a href="mailto:${ownerEmail}">Email</a></div></div>`],
  ['nested owner label cannot supply employee title', `<div class="team-member"><h3>Ada Lovelace</h3><a href="mailto:${ownerEmail}">Email</a><div class="team-member"><p>Owner</p></div></div>`],
  ['hidden entire card', `<div hidden>${card()}</div>`],
  ['aria-hidden entire card', `<div aria-hidden="true">${card()}</div>`],
  ['display-none entire card', `<div style="display: none">${card()}</div>`],
  ['hidden class entire card', `<div class="d-none">${card()}</div>`],
  ['hidden owner title', card().replace('<p class="role">', '<p hidden class="role">')],
  ['template card', `<template>${card()}</template>`],
  ['noscript card', `<noscript>${card()}</noscript>`],
  ['executable script string', `<script>const owner = '${card()}';</script>`],
  ['script inside card', card('Ada Lovelace', 'Owner', ownerEmail, '<script>"Founder"</script>')],
  ['testimonial card', `<section class="customer-testimonials">${card()}</section>`],
  ['guest speaker card', `<section class="guest-speakers">${card()}</section>`],
  ['bare generic person card', card().replace('team-member', 'person-card')],
  ['bare Person without company association', card().replace('class="team-member"', 'itemscope itemtype="https://schema.org/Person"')],
  ['foreign HTML Organization ancestor', `<section itemscope itemtype="https://schema.org/Organization" itemid="https://other.example">${card()}</section>`],
  ['unclosed card boundary', card().replace('</div>', '')],
  ['oversized card', card('Ada Lovelace', 'Owner', ownerEmail, 'x'.repeat(LIMITS.recordChars))],
  ['JSON-LD separate records cannot supply missing fields', json({ '@graph': [person({ email: undefined }), { '@type': 'Person', email: ownerEmail }] })],
  ['JSON-LD nested employee cannot supply owner email', json(person({ email: undefined, employee: person({ name: 'Grace Hopper', jobTitle: 'Office Manager' }) }))],
  ['JSON-LD email array ambiguous', json(person({ email: [ownerEmail, `ada.lovelace@${domain}`] }))],
  ['JSON-LD foreign worksFor', json(person({ worksFor: { '@type': 'Organization', url: 'https://other.example/' } }))],
  ['JSON-LD mixed worksFor URL array', json(person({ worksFor: { url: [`https://${domain}/about`, 'https://other.example/'] } }))],
  ['JSON-LD mixed Organization URL array', json({ '@type': 'Organization', url: [`https://${domain}/about`, 'https://other.example/'], founder: person({ worksFor: undefined }) })],
  ['JSON-LD foreign Organization id conflicts with matching URL', json({ '@type': 'Organization', url: `https://${domain}`, '@id': 'https://other.example/#company', founder: person() })],
  ['JSON-LD unaffiliated standalone Person', json(person({ worksFor: undefined }))],
  ['JSON-LD foreign Organization ancestor', json({ '@type': 'Organization', url: 'https://other.example', founder: person({ email: 'ada.lovelace@gmail.com' }) })],
  ['JSON-LD missing Organization identity', json({ '@type': 'Organization', founder: person({ worksFor: undefined }) })],
  ['JSON-LD unrelated Organization relationship', json({ '@type': 'Organization', url: `https://${domain}`, knows: person({ worksFor: undefined }) })],
  ['JSON-LD Organization author is not employee', json({ '@type': 'Organization', url: `https://${domain}`, author: person({ worksFor: undefined }) })],
  ['JSON-LD association does not traverse arbitrary descendants', json({ '@type': 'Organization', url: `https://${domain}`, employee: { knows: person({ worksFor: undefined }) } })],
  ['JSON-LD duplicate name key', '<script type="application/ld+json">' + JSON.stringify(person()).replace('"name":"Ada Lovelace"', '"name":"Grace Hopper","name":"Ada Lovelace"') + '</script>'],
  ['JSON-LD duplicate title key', '<script type="application/ld+json">' + JSON.stringify(person()).replace('"jobTitle":"Founder"', '"jobTitle":"Receptionist","jobTitle":"Founder"') + '</script>'],
  ['JSON-LD testimonial person', json({ '@type': 'Review', author: person() })],
  ['JSON-LD hidden script', `<div hidden>${json(person())}</div>`],
  ['JSON-LD wrong script type', json(person()).replace('application/ld+json', 'application/json')],
  ['malformed JSON-LD', '<script type="application/ld+json">{bad JSON}</script>'],
];

test('ambiguous, unrelated, untrusted and hidden records never outrank the exact fallback', async t => {
  for (const [name, html] of rejectedRecords) await t.test(name, async () => {
    assert.deepEqual(extract(html), []);
    const fixture = withRoleHomepage(html);
    assert.deepEqual(JSON.parse(JSON.stringify(await fixture.scrape())), roleFallback);
    assert.equal(fixture.calls.length, 8);
  });
});

test('conflicting explicit records are held while exact duplicates are harmless', () => {
  assert.equal(extract(card() + card('Ada Byron', 'Founder')).length, 0);
  assert.equal(extract(card() + card('Ada Lovelace', 'Founder', `ada.lovelace@${domain}`)).length, 0);
  assert.equal(extract(card() + card('Grace Hopper', 'Office Manager', ownerEmail)).length, 0);
  for (const title of ['Office Manager', 'Former Owner', 'Receptionist']) {
    assert.equal(extract(card() + card('Ada Lovelace', title, ownerEmail)).length, 0);
    assert.equal(extract(json({ '@graph': [person(), person({ jobTitle: title })] })).length, 0);
  }
  assert.equal(extract(card() + `<section class="former">${card('Ada Lovelace', 'Former Owner')}</section>`).length, 0);
  assert.equal(extract(card() + card().replace('team-member', 'team-member former')).length, 0);
  const duplicate = extractWebsiteOwnerCandidates([page(card()), { url: `https://${domain}/team`, text: card() }], domain);
  assert.equal(duplicate.length, 2);
  assert.equal(duplicate[0].sourceUrl, sourceUrl);
});

test('complete nested Person remains independently bound; outer fields cannot bleed in', () => {
  const result = extract(`<div class="team-member"><h3>Grace Hopper</h3><p>Office Manager</p>${card()}</div>`);
  assert.equal(result.length, 1);
  assert.equal(result[0].contact, 'Ada Lovelace');
});

test('untrusted sources and off-domain redirects cannot supply owner evidence', async () => {
  assert.deepEqual(extractWebsiteOwnerCandidates([{ url: 'https://directory.example/about', text: card() }], domain), []);
  assert.deepEqual(extractWebsiteOwnerCandidates([{ url: `https://${domain}.evil.example/about`, text: card() }], domain), []);
  assert.deepEqual(extractWebsiteOwnerCandidates([{ url: 'https://linkedin.com/about', text: card('Ada Lovelace', 'Founder', 'ada@gmail.com') }], 'linkedin.com'), []);
  const fixture = withRoleHomepage(card(), { redirects: { '/about': 'https://directory.example/about' } });
  assert.deepEqual(JSON.parse(JSON.stringify(await fixture.scrape())), roleFallback);
  assert.equal(fixture.calls.length, 9, 'unchanged crawl replaces rejected off-domain success');
});

test('role-only, no-email, malformed and oversized pages retain the existing result and bounded crawl', async () => {
  for (const html of ['', 'not HTML', '<html>' + 'x'.repeat(LIMITS.pageChars) + card() + '</html>']) {
    assert.deepEqual(extractWebsiteOwnerCandidates([page(html)], domain), []);
    const fixture = withRoleHomepage(html);
    assert.deepEqual(JSON.parse(JSON.stringify(await fixture.scrape())), roleFallback);
  }
  assert.equal(await scrapeFixture().scrape(), null);
});

test('depth, node and record limits fail closed without truncating into an attributable record', () => {
  assert.deepEqual(extract('<div>'.repeat(LIMITS.depth + 1) + card() + '</div>'.repeat(LIMITS.depth + 1)), []);
  assert.deepEqual(extract('<div></div>'.repeat(LIMITS.nodes + 1) + card()), []);
  assert.deepEqual(extract(card('Grace Hopper', 'Office Manager', `grace@${domain}`).repeat(LIMITS.records) + card()), []);
  assert.deepEqual(extract(json([person(), ...Array(LIMITS.jsonNodes).fill(0)])), []);
  assert.deepEqual(extract(card() + card('Grace Hopper', 'Office Manager', `grace@${domain}`).repeat(LIMITS.records - 1)
    + card('Ada Lovelace', 'Receptionist')), []);
  assert.deepEqual(extractWebsiteOwnerCandidates([page(card()), { url: `https://${domain}/team`, text: 'x'.repeat(LIMITS.pageChars + 1) }], domain), []);
  assert.deepEqual(extractWebsiteOwnerCandidates([{ url: sourceUrl + '?q=' + 'x'.repeat(2048), text: card() }], domain), []);
  assert.deepEqual(extract(json({ ['x'.repeat(501)]: person() })), []);
});

test('owner lookup adds no fetches for discovered links or failed guessed paths', async () => {
  const fixture = scrapeFixture({ pagesByPath: { '/': `<html>${roleFallback.email}<a href="about.html">Our team</a></html>`, '/about.html': `<html>${card()}</html>` } });
  const result = await fixture.scrape();
  assert.equal(result.email, ownerEmail);
  assert.equal(new URL(fixture.calls[1].url).pathname, '/about.html');
  assert.equal(fixture.calls.length, 8);
  assert.equal(fixture.calls.filter(c => c.url.endsWith('/about.html')).length, 1);
});

test('actual standard waterfall/projection/save keeps exact evidence without upgrading verification or policy', async () => {
  const scraped = await withRoleHomepage(card()).scrape();
  const policy = { tenantId: '13', allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'] };
  for (const status of ['valid', 'unknown', 'risky', 'catchall', 'invalid']) {
    const { lead, calls } = await runScoutWaterfall(leadFixture(), { hunter: null, scraped });
    assert.deepEqual(calls, ['prospeo', 'hunter', 'scraped']);
    const db = recordingDb(); let verifications = 0;
    const intake = loadScoutSave({ db, verify: async email => { verifications++; assert.equal(email, ownerEmail); return { ...verificationReceipt, emailStatus: status }; } });
    assert.equal((await intake.save([lead])).saved, 1);
    assert.equal(verifications, 1);
    const row = db.rows[0];
    assert.deepEqual(row.acquisition_metadata.contactResolution.websiteEvidence, scraped.websiteOwnerEvidence);
    assert.equal(row.enrichment_provenance.email.source_url, sourceUrl);
    assert.equal(row.enrichment_provenance.email.source, 'scraped');
    if (status === 'valid') {
      assert.equal(governedContactReason(row, policy), null);
      row.do_not_contact = true;
      assert.equal(governedContactReason(row, policy), 'do_not_contact');
    } else {
      assert.notEqual(row.acquisition_metadata.contactResolution.finalState, 'VERIFIED_FOUNDER_EMAIL');
      assert.notEqual(governedContactReason(row, policy), null);
    }
  }
});

test('evidence passthrough copies only selected bounded fields and cannot inherit stale evidence', () => {
  const [scraped] = extract(card());
  for (const mutate of [
    e => { e.email = `grace@${domain}`; }, e => { e.contactName = 'Grace Hopper'; },
    e => { e.contactTitle = 'Founder'; }, e => { e.sourceUrl = 'https://other.example/about'; },
    e => { e.record = 'x'.repeat(9000); }, e => { e.end += 1; },
  ]) {
    const selected = { ...scraped, websiteOwnerEvidence: { ...scraped.websiteOwnerEvidence } };
    mutate(selected.websiteOwnerEvidence);
    const lead = applyScoutEnrichment({}, selected, 'scraped', 13, domain);
    assert.equal(Object.hasOwn(lead.scoutContactEvidence, 'websiteEvidence'), false);
  }
  const stale = applyScoutEnrichment({ ...scraped }, { email: ownerEmail }, 'scraped', 13, domain);
  assert.equal(Object.hasOwn(stale.scoutContactEvidence, 'websiteEvidence'), false);
  const lead = applyScoutEnrichment({}, { ...scraped, websiteOwnerEvidence: { ...scraped.websiteOwnerEvidence, verified: true, classification: 'VERIFIED_FOUNDER_EMAIL' } }, 'scraped', 13, domain);
  assert.equal(Object.hasOwn(lead.scoutContactEvidence.websiteEvidence, 'verified'), false);
  assert.equal(Object.hasOwn(lead.scoutContactEvidence.websiteEvidence, 'classification'), false);
});
