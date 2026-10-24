'use strict';

/**
 * Shared Puppeteer launch options for Node test harnesses (not production agents).
 * GitHub Actions and other CI runners cannot use Chromium's setuid sandbox.
 */

function isCiTestBrowserEnvironment() {
  return process.env.CI === 'true'
    || process.env.GITHUB_ACTIONS === 'true'
    || process.env.PUPPETEER_TEST_NO_SANDBOX === '1';
}

function testBrowserLaunchArgs(extraArgs = []) {
  const args = ['--disable-background-networking'];
  if (isCiTestBrowserEnvironment()) {
    args.unshift('--no-sandbox', '--disable-setuid-sandbox');
  }
  if (extraArgs.length) args.push(...extraArgs);
  return args;
}

async function launchTestBrowser(puppeteer, options = {}) {
  const { extraArgs = [], ...launchOptions } = options;
  return puppeteer.launch({
    headless: true,
    args: testBrowserLaunchArgs(extraArgs),
    ...launchOptions,
  });
}

module.exports = {
  isCiTestBrowserEnvironment,
  testBrowserLaunchArgs,
  launchTestBrowser,
};
