#!/usr/bin/env node
// Optional built-page acceptance. Start a loopback static server separately.
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const crypto = require('node:crypto');
const {chromium} = require(process.env.PLAYWRIGHT_MODULE || 'playwright');

const base = new URL(process.argv[2]);
assert.equal(base.protocol, 'http:');
assert.ok(['127.0.0.1', 'localhost', '[::1]'].includes(base.hostname), 'preview must use loopback');
assert.ok(!base.username && !base.password, 'anonymous preview required');
const output = path.join(process.cwd(), 'target', 'docs-browser');
fs.mkdirSync(output, {recursive: true});
const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'sdk-rust-docs-browser-'));
const api = 'https://status.durable-workflow.com/api/current.json';
const checks = [];
const screenshots = [];
const errors = [];
let context;
let page;
let fixture = 'operational';
function check(value, message) { assert.ok(value, message); checks.push(message); }
function snapshot(kind) {
  const now = Date.now();
  const value = {
    schema: 'durable_workflow.status_page_snapshot.v1', product: 'cloud', environment: 'prod',
    checked_at: new Date(now - 1000).toISOString(), generated_at: new Date(now - 500).toISOString(),
    status: 'ok', alert_level: 'ok', customer_visible: false,
    freshness: {status: 'fresh', age_seconds: 1, stale_after_seconds: 900},
    components: [{name: 'SYNTHETIC_PRIVATE_COMPONENT', status: 'failed'}],
    customer_impact: {status: 'ok', alert_level: 'ok', affected_families: []},
  };
  if (kind === 'degraded') { value.customer_impact.status = 'warning'; value.customer_impact.alert_level = 'warning'; }
  if (kind === 'incident') { value.status = 'failed'; value.alert_level = 'critical'; }
  if (kind === 'maintenance') { value.customer_impact.status = 'warning'; value.customer_impact.alert_level = 'warning'; value.customer_impact.affected_families = ['maintenance']; }
  if (kind === 'stale') value.checked_at = new Date(now - 901000).toISOString();
  if (kind === 'unknown') value.environment = 'synthetic-unsupported';
  if (kind === 'unavailable') value.meta = {public_status_reason: 'cloud_unreachable'};
  if (kind === 'expires') value.freshness.stale_after_seconds = 3;
  return value;
}
async function visit(relative = '') {
  await page.goto(new URL(relative, base).href, {waitUntil: 'networkidle'});
  await page.evaluate(() => document.fonts.ready);
}
async function widthCheck(width) {
  const actual = await page.evaluate(() => ({viewport: innerWidth, width: document.documentElement.scrollWidth}));
  check(actual.width <= width + 1, `no horizontal page overflow at ${width}`);
}
async function capture(name, fullPage = true) {
  await page.screenshot({path: path.join(output, name), fullPage});
  screenshots.push(name);
}
function contrast(foreground, background) {
  const luminance = text => {
    const rgb = text.match(/[\d.]+/g).slice(0, 3).map(Number).map(n => n / 255);
    return rgb.map(n => n <= .04045 ? n / 12.92 : ((n + .055) / 1.055) ** 2.4)
      .reduce((total, n, index) => total + n * [.2126, .7152, .0722][index], 0);
  };
  const a = luminance(foreground), b = luminance(background);
  return (Math.max(a, b) + .05) / (Math.min(a, b) + .05);
}
async function actionContrast() {
  const colors = await page.locator('.dw-actions .dw-button').evaluateAll(elements => elements.map(element => {
    const s = getComputedStyle(element);
    return {foreground:s.color, background:s.backgroundColor};
  }));
  for (const colorset of colors) check(contrast(colorset.foreground, colorset.background) >= 4.5, 'hero action contrast >= 4.5:1');
}
// These cases share one page/profile; navigation and viewport changes are serial.
const sequential = (items, operation) => items.reduce((pending, item) => pending.then(() => operation(item)), Promise.resolve());
async function renderCase({width, theme}) {
  await page.setViewportSize({width, height:1000});
  await visit();
  if (await page.locator('html').getAttribute('data-theme') !== theme) await page.locator('[data-dw-theme-toggle]').click();
  check(await page.locator('html').getAttribute('data-theme') === theme, `explicit ${theme} theme at ${width}`);
  await widthCheck(width);
  await actionContrast();
  const background = await page.locator('.dw-code').first().evaluate(element => getComputedStyle(element).backgroundColor);
  check(background === 'rgb(15, 21, 19)', `Ink code block in ${theme}`);
  const landingLabelSize = await page.locator('.dw-footer-columns h2').first().evaluate(element => getComputedStyle(element).fontSize);
  check(landingLabelSize === '12px', 'landing footer labels keep shared label size');
  await capture(`landing-${width}-${theme}.png`);
  await visit('durable_workflow/');
  check(await page.locator('html').getAttribute('data-theme') === theme, 'explicit theme persists into generated API');
  check(await page.locator('body').getAttribute('class').then(value => value.includes('dw-reference')), 'generated API has shared chrome');
  await widthCheck(width);
  await capture(`api-${width}-${theme}.png`, false);
  const footerSize = await page.locator('.dw-footer-columns h2').first().evaluate(element => getComputedStyle(element).fontSize);
  check(footerSize === '12px', 'footer labels keep shared label size');
}
async function statusCase(state) {
  fixture = state;
  await visit();
  await page.waitForFunction(expected => document.querySelector('.dw-status-pill').dataset.status === expected, state === 'http-error' ? 'unknown' : state);
  check(true, `synthetic status ${state} presentation`);
  check(!(await page.locator('body').innerText()).includes('SYNTHETIC_PRIVATE_COMPONENT'), 'private component is not rendered');
}
async function run() {
  context = await chromium.launchPersistentContext(profile, {
    executablePath: process.env.CHROMIUM_PATH,
    headless: true, viewport: {width:1440, height:1000}, colorScheme:'dark',
    permissions:['clipboard-read','clipboard-write'],
    args:['--renderer-process-limit=2','--no-first-run','--no-default-browser-check'],
  });
  context.setDefaultTimeout(10000);
  await context.route('**/*', route => {
    const url = route.request().url();
    if (url === api) {
      if (fixture === 'http-error') return route.fulfill({status:503, headers:{'access-control-allow-origin':'*'}, body:'unavailable'});
      return route.fulfill({status:200, contentType:'application/json', headers:{'access-control-allow-origin':'*'}, body:JSON.stringify(snapshot(fixture))});
    }
    if (url.startsWith(base.origin + '/')) return route.continue();
    return route.abort();
  });
  page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  page.on('response', response => {
    if (response.url().startsWith(base.origin + '/') && response.status() >= 400) errors.push(`local HTTP ${response.status()}: ${new URL(response.url()).pathname}`);
  });
  await visit();
  check(await page.locator('html').getAttribute('data-theme') === 'light', 'light is default even with dark OS');
  check(await page.locator('.dw-status-pill').getAttribute('data-status') === 'operational', 'fresh synthetic public aggregate hydrates status');
  const fonts = await page.evaluate(() => [document.fonts.check('400 16px Inter'), document.fonts.check('600 64px "Space Grotesk"'), document.fonts.check('500 12px "JetBrains Mono"')]);
  check(fonts.every(Boolean), 'all three self-hosted fonts loaded');
  fixture = 'unknown';
  const cases = [1440,1024,390,320].flatMap(width => ['light','dark'].map(theme => ({width, theme})));
  await sequential(cases, renderCase);
  await visit();
  await page.locator('[data-dw-copy="dw-command-install"]').click();
  check(await page.evaluate(() => navigator.clipboard.readText()) === 'cargo add durable-workflow', 'copy excludes terminal prompt');
  const summary = page.locator('.dw-sdk-switcher summary');
  await summary.focus(); await page.keyboard.press('Enter');
  check(await page.locator('.dw-sdk-switcher').getAttribute('open') !== null, 'SDK switcher opens with keyboard');
  await page.keyboard.press('Tab');
  check(await page.evaluate(() => document.activeElement.href) === 'https://python.durable-workflow.com/', 'SDK switcher reaches Python link');
  await page.keyboard.press('Escape');
  check(await page.locator('.dw-sdk-switcher').getAttribute('open') === null, 'Escape closes SDK switcher');
  check(await summary.evaluate(element => element === document.activeElement), 'Escape restores switcher focus');
  await page.locator('[data-docs-priority="primary"]').click();
  check(new URL(page.url()).pathname === '/durable_workflow/', 'API primary action works');
  await page.locator('.sidebar-menu-toggle').click();
  check(await page.locator('main').evaluate(element => element.inert), 'compact API sidebar isolates main');
  await page.keyboard.press('Escape');
  check(!(await page.locator('main').evaluate(element => element.inert)), 'Escape restores API main');
  await page.setViewportSize({width:1440,height:1000});
  const search = page.locator('input.search-input').first();
  await page.keyboard.press('s');
  await search.fill('WorkflowContext');
  await page.waitForFunction(() => document.querySelector('#search')?.textContent.includes('WorkflowContext'));
  check(true, 'generated API search returns WorkflowContext');
  await sequential(['operational','degraded','incident','maintenance','stale','unknown','unavailable','http-error'], statusCase);
  fixture = 'expires'; await visit();
  await page.waitForFunction(() => document.querySelector('.dw-status-pill').dataset.status === 'stale', null, {timeout:5000});
  check(true, 'status invalidates at evidence cutoff');
  await page.setViewportSize({width:1200,height:630});
  await visit('assets/social-card.svg');
  await capture('social-card.png', false);
  check(errors.length === 0, `no page/local resource errors: ${errors.join('; ')}`);
  return {outcome:'pass', checks:checks.length, screenshots, browser:context.browser().version(), statusEvidence:'screenshots use synthetic unknown; functional states use synthetic fixtures, no live-health claim'};
}
let timer;
async function main() {
  let report;
  try {
    report = await Promise.race([run(), new Promise((_,reject) => { timer = setTimeout(() => reject(new Error('210-second browser bound exceeded')),210000); })]);
  } catch (error) {
    report = {outcome:'fail', error:error.message, checks:checks.length, screenshots, pageErrors:errors};
    if (page) await page.screenshot({path:path.join(output,'failure.png'),fullPage:true}).catch(() => {});
    process.exitCode = 1;
  } finally {
    clearTimeout(timer);
    if (context) await context.close();
    fs.rmSync(profile,{recursive:true,force:true});
  }
  report.screenshotHashes = Object.fromEntries(screenshots.map(name => [name,crypto.createHash('sha256').update(fs.readFileSync(path.join(output,name))).digest('hex')]));
  fs.writeFileSync(path.join(output,'summary.json'),JSON.stringify(report,null,2)+'\n');
  console.log(JSON.stringify({outcome:report.outcome, checks:report.checks, screenshots:screenshots.length, error:report.error}));
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
