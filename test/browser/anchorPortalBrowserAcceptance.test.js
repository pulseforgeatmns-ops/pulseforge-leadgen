'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const puppeteer = require('puppeteer');
const { createExeterPortalHarness, illustrativePhoto, DEMO_NAME } = require('../helpers/exeterPortalHarness');

test('desktop and mobile real UI persists report, client photo problem, progress and resolution proof', {timeout:120000}, async () => {
  const h = await createExeterPortalHarness(); let browser;
  const files = fs.mkdtempSync(path.join(os.tmpdir(),'exeter-browser-'));
  const photoPath = path.join(files,'synthetic-zone.png'); fs.writeFileSync(photoPath,Buffer.from(illustrativePhoto(0).data_url.split(',')[1],'base64'));
  try {
    browser = await puppeteer.launch({executablePath:process.env.CHROME_BIN || '/usr/bin/chromium',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
    const page = await browser.newPage(); const errors=[]; page.on('pageerror',e=>errors.push(e.message));
    await page.setRequestInterception(true);
    page.on('request',r => r.url().startsWith(h.base) || r.url().startsWith('data:') || r.url().startsWith('blob:') ? r.continue() : r.abort());
    async function login(identity) {const cookie=await h.login(identity); const split=cookie.indexOf('=');await page.setCookie({name:cookie.slice(0,split),value:cookie.slice(split+1),url:h.base});await page.goto(`${h.base}/portal/anchor`);await page.waitForFunction(()=>document.querySelector('#userLabel').textContent.startsWith('Synthetic')); }
    async function tab(name) {await page.click(`[data-tab="${name}"]`);}
    async function photo(selector) {await (await page.$(selector)).uploadFile(photoPath);await page.waitForFunction(s=>document.querySelector(s+'Preview img')?.complete,{},selector);}
    async function text(selector,value) {await page.type(selector,value);}
    await page.setViewport({width:1365,height:900}); await login('operator');
    await page.select('#building',String(h.location.id));
    await tab('author'); await page.waitForSelector('#reportNotes');
    await page.click('[name="area"]'); await text('#reportNotes','Synthetic browser authored report'); await photo('#reportPhotos');
    await page.click('#saveForm'); await page.waitForFunction(()=>document.querySelector('#flash').textContent.includes('Service Report saved'));
    let result=await h.pool.query("SELECT id FROM anchor_portal_visits WHERE visit_notes='Synthetic browser authored report'");assert.equal(result.rowCount,1);const reportId=result.rows[0].id;
    await page.waitForSelector(`[data-visit="${reportId}"]`);await page.click(`[data-visit="${reportId}"]`);await page.waitForSelector('#backReports');assert.equal(await page.$$eval('.gallery img',images=>images.filter(i=>i.complete&&i.naturalWidth>0).length),1);
    await page.click('[data-photo]');await page.waitForSelector('dialog[open]');await page.click('dialog button');await page.waitForFunction(()=>!document.querySelector('dialog'));
    await page.click('#backReports');await page.waitForSelector('[data-visit]');
    await page.setViewport({width:390,height:844,isMobile:true,hasTouch:true});await login('client');
    assert.equal(await page.$eval('#pageTitle',e=>e.textContent),'Service Assurance Portal');
    assert.equal(await page.$eval('#building option:checked',e=>e.textContent),DEMO_NAME);
    assert.match(await page.$eval('#demoBanner',e=>e.textContent),/Synthetic/);
    assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth),'mobile should not overflow horizontally');
    await tab('history');await page.waitForSelector(`[data-visit="${reportId}"]`);await page.click(`[data-visit="${reportId}"]`);await page.waitForSelector('#backReports');assert.match(await page.$eval('#view',e=>e.textContent),/Synthetic browser authored report/);
    await tab('issues');await page.waitForSelector('#issueDesc');await text('#issueRoom','Synthetic renovation classroom');
    const malicious='Synthetic browser problem <img src=x onerror="window.portalXss=true">';await text('#issueDesc',malicious);await page.select('#issueSeverity','high');await photo('#issuePhoto');
    await page.click('#saveForm');await page.waitForFunction(()=>document.querySelector('#flash').textContent.includes('Problem submitted'));await page.waitForFunction(()=>document.querySelector('#view').textContent.includes('Synthetic browser problem'));
    assert.equal(await page.evaluate(()=>window.portalXss),undefined);assert.match(await page.$eval('#view',e=>e.textContent),/<img src=x/);
    result=await h.pool.query('SELECT id FROM anchor_portal_issues WHERE description=$1',[malicious]);assert.equal(result.rowCount,1);const issueId=result.rows[0].id;
    await login('operator');await page.select('#building',String(h.location.id));await tab('issues');await page.waitForSelector(`[data-resolve="${issueId}"]`);
    await page.click(`[data-resolve="${issueId}"]`);await page.waitForSelector('#resolution');await text('#resolution','Cancelled draft must not save');await page.click('#cancelForm');
    assert.equal((await h.pool.query('SELECT status FROM anchor_portal_issues WHERE id=$1',[issueId])).rows[0].status,'open');
    await tab('issues');await page.waitForSelector(`[data-progress="${issueId}"]`);await page.click(`[data-progress="${issueId}"]`);await page.waitForFunction(()=>document.querySelector('#flash').textContent.includes('In Progress'));
    await page.waitForSelector(`[data-resolve="${issueId}"]`);await page.click(`[data-resolve="${issueId}"]`);await page.waitForSelector('#resolution');await text('#resolution','Synthetic browser resolution verified');await photo('#resolutionPhoto');await page.click('#saveForm');await page.waitForFunction(()=>document.querySelector('#flash').textContent.includes('Resolution saved'));
    await login('client');await tab('issues');await page.waitForSelector('[data-go="resolved"]');await page.click('[data-go="resolved"]');await page.waitForFunction(()=>document.querySelector('#view').textContent.includes('Synthetic browser resolution verified'));
    await page.reload();await page.waitForSelector('[data-tab="issues"]');await tab('issues');await page.waitForSelector('[data-go="resolved"]');await page.click('[data-go="resolved"]');await page.waitForFunction(()=>document.querySelector('#view').textContent.includes('Synthetic browser resolution verified'));
    assert.ok((await page.$$eval('.gallery img',images=>images.filter(i=>i.complete&&i.naturalWidth>0).length))>=2);
    assert.equal(await page.evaluate(()=>window.portalXss),undefined);assert.deepEqual(errors,[]);
    await page.screenshot({path:path.join(files,'mobile-resolved-proof.png'),fullPage:true});
    await login('unassigned');await page.waitForFunction(()=>document.querySelector('#view').textContent.includes('No building assigned'));
  } finally {if(browser)await browser.close();await h.close();fs.rmSync(files,{recursive:true,force:true});}
});
