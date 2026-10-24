'use strict';

// Phase B visual regression capture.
// Spins the Phase A2 preview harness, screenshots before/after Calls +
// Dashboard at desktop and mobile viewports, then exits.
//
//   node scripts/phaseBCaptureScreenshots.js
//
// Writes to artifacts/phase-b/screenshots/.

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const root = path.join(__dirname, '..');
const outDir = path.join(root, 'artifacts', 'phase-b', 'screenshots');
const PREVIEW_URL = 'http://127.0.0.1:4620';

async function waitForReady(timeoutMs = 90000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`${PREVIEW_URL}/setter/api/leads`);
      if (res.ok) return;
    } catch {
      // not up yet
    }
    await new Promise(r => setTimeout(r, 500));
  }
  throw new Error('preview server did not become ready');
}

async function shot(page, name, url, { width, height }) {
  await page.setViewport({ width, height, deviceScaleFactor: 1 });
  await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
  await new Promise(r => setTimeout(r, 1200));
  const file = path.join(outDir, `${name}.png`);
  await page.screenshot({ path: file, fullPage: false });
  console.log('[phase-b-capture]', file);
}

async function main() {
  fs.mkdirSync(outDir, { recursive: true });

  const preview = spawn(process.execPath, [path.join(root, 'scripts', 'phaseA2Preview.js')], {
    cwd: root,
    env: { ...process.env, MAX_SMOKE_DISPOSABLE_PG: 'true' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let previewLog = '';
  preview.stdout.on('data', chunk => { previewLog += chunk; process.stdout.write(chunk); });
  preview.stderr.on('data', chunk => { previewLog += chunk; process.stderr.write(chunk); });

  const cleanup = async () => {
    try { preview.kill('SIGINT'); } catch { /* noop */ }
    await new Promise(r => setTimeout(r, 800));
    try { preview.kill('SIGKILL'); } catch { /* noop */ }
  };
  process.on('exit', () => { try { preview.kill('SIGKILL'); } catch { /* noop */ } });

  try {
    await waitForReady();
    // Give seed a moment after first leads response.
    await new Promise(r => setTimeout(r, 2000));

    const puppeteer = require('puppeteer');
    const executablePath = process.env.PUPPETEER_EXECUTABLE_PATH
      || (process.platform === 'darwin'
        ? '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
        : undefined);
    const browser = await puppeteer.launch({
      headless: true,
      executablePath,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });
    const page = await browser.newPage();

    // Before (git HEAD pages served by the preview harness)
    await shot(page, 'before-calls-desktop', `${PREVIEW_URL}/before/setter`, { width: 1440, height: 900 });
    await shot(page, 'before-calls-mobile', `${PREVIEW_URL}/before/setter`, { width: 390, height: 844 });
    await shot(page, 'before-dashboard-desktop', `${PREVIEW_URL}/before/dashboard`, { width: 1440, height: 900 });

    // After (current working tree)
    await shot(page, 'after-calls-desktop', `${PREVIEW_URL}/setter`, { width: 1440, height: 900 });
    await shot(page, 'after-calls-mobile', `${PREVIEW_URL}/setter`, { width: 390, height: 844 });
    await shot(page, 'after-dashboard-desktop', `${PREVIEW_URL}/dashboard`, { width: 1440, height: 900 });

    // Open workspace if a Call button exists (desktop)
    await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 1 });
    await page.goto(`${PREVIEW_URL}/setter`, { waitUntil: 'networkidle2', timeout: 60000 });
    await new Promise(r => setTimeout(r, 1500));
    const opened = await page.evaluate(() => {
      const btn = document.querySelector('[data-workspace], .start-call-btn, button.primary');
      if (!btn) return false;
      btn.click();
      return true;
    });
    if (opened) {
      await new Promise(r => setTimeout(r, 1500));
      // Select an outcome so the desktop completion footer shows its
      // contextual Save (e.g. "Save — schedule callback for Aug 4").
      await page.evaluate(() => {
        document.querySelector('[data-pf-outcome="callback_requested"]')?.click();
      });
      await new Promise(r => setTimeout(r, 500));
      await page.screenshot({ path: path.join(outDir, 'after-workspace-desktop.png'), fullPage: false });
      console.log('[phase-b-capture]', path.join(outDir, 'after-workspace-desktop.png'));

      // Mobile workspace: activate the Outcome tab so the sticky action bar
      // shows phone · Call · Save · Queue.
      await page.setViewport({ width: 390, height: 844, deviceScaleFactor: 1 });
      await new Promise(r => setTimeout(r, 400));
      await page.evaluate(() => {
        document.querySelector('.pf-workspace-tabs [data-tab="outcome"]')?.click();
      });
      await new Promise(r => setTimeout(r, 600));
      await page.screenshot({ path: path.join(outDir, 'after-workspace-mobile.png'), fullPage: false });
      console.log('[phase-b-capture]', path.join(outDir, 'after-workspace-mobile.png'));
    } else {
      console.warn('[phase-b-capture] no workspace trigger found — skipped workspace shots');
    }

    await browser.close();
  } finally {
    await cleanup();
  }
}

main().catch(err => {
  console.error('[phase-b-capture] failed:', err);
  process.exit(1);
});
