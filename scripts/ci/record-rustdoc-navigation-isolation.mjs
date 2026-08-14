#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { createRequire } from 'node:module';

import { bindResponsiveNavigationCapture } from './rustdoc-navigation-evidence.mjs';

const ALLOWED_PUBLIC_HOST = 'rust.durable-workflow.com';
const TOGGLE_SELECTOR = '.sidebar-menu-toggle';
const NAVIGATION_ID = 'dw-rustdoc-navigation';

function fail(message) {
  process.stderr.write(`${message}\n`);
  process.exit(1);
}

function parseArguments(argv) {
  const options = {};
  for (let index = 0; index < argv.length; index += 2) {
    const argument = argv[index];
    const value = argv[index + 1];
    if (!argument?.startsWith('--') || !value) fail(`invalid argument: ${argument || ''}`);
    options[argument.slice(2).replace(/-([a-z])/g, (_, letter) => letter.toUpperCase())] = value;
  }
  for (const name of ['url', 'width', 'height', 'report', 'manifest', 'controllerRoot']) {
    if (!String(options[name] || '').trim()) fail(`--${name.replace(/[A-Z]/g, (letter) => `-${letter.toLowerCase()}`)} is required`);
  }
  options.width = Number.parseInt(options.width, 10);
  options.height = Number.parseInt(options.height, 10);
  if (!Number.isInteger(options.width) || !Number.isInteger(options.height)) {
    fail('--width and --height must be integers');
  }
  return options;
}

function allowedUrl(rawUrl) {
  let parsed;
  try {
    parsed = new URL(rawUrl);
  } catch {
    fail('--url must be an absolute URL');
  }
  const local = parsed.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(parsed.hostname);
  const deployed = parsed.protocol === 'https:' && parsed.hostname === ALLOWED_PUBLIC_HOST;
  if ((!local && !deployed) || parsed.username || parsed.password) {
    fail('--url must be the loopback preview or deployed Rust reference without credentials');
  }
  return parsed;
}

function executable(candidate) {
  try {
    fs.accessSync(candidate, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

function round(value) {
  return Math.round(value * 100) / 100;
}

const options = parseArguments(process.argv.slice(2));
const targetUrl = allowedUrl(options.url);
const controllerRoot = path.resolve(options.controllerRoot);
const requireFromController = createRequire(path.join(controllerRoot, 'package.json'));
const { chromium } = requireFromController('playwright-core');
const chromiumRuntime = requireFromController('@sparticuz/chromium');
const browserCandidates = [
  await chromiumRuntime.executablePath(),
  chromium.executablePath(),
  '/usr/bin/chromium',
  '/usr/bin/chromium-browser',
  '/usr/bin/google-chrome',
];
const browserExecutable = browserCandidates.find(executable);
if (!browserExecutable) fail('the pinned visual evidence Chromium runtime is unavailable');

const reportPath = path.resolve(options.report);
const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
const expectedInteraction = [{ type: 'click', selector: TOGGLE_SELECTOR }];
if (
  report.state !== 'navigation-open'
  || report.viewport?.width !== options.width
  || report.viewport?.height !== options.height
  || JSON.stringify(report.interactions) !== JSON.stringify(expectedInteraction)
) {
  fail('the navigation isolation probe does not match its visual capture report');
}

const browser = await chromium.launch({
  executablePath: browserExecutable,
  headless: true,
  chromiumSandbox: false,
  args: [...chromiumRuntime.args, '--disable-dev-shm-usage'],
});

try {
  const context = await browser.newContext({
    viewport: { width: options.width, height: options.height },
  });
  const page = await context.newPage();
  await page.route('**/*', async (route) => {
    let requestUrl;
    try {
      requestUrl = new URL(route.request().url());
    } catch {
      await route.abort('blockedbyclient');
      return;
    }
    if (requestUrl.origin === targetUrl.origin) {
      await route.continue();
    } else {
      await route.abort('blockedbyclient');
    }
  });
  const response = await page.goto(targetUrl.href, { waitUntil: 'networkidle', timeout: 30_000 });
  if (!response || response.status() < 200 || response.status() >= 300) {
    fail('the navigation isolation probe did not load the Rust reference over HTTP 2xx');
  }
  await page.locator(TOGGLE_SELECTOR).click({ timeout: 10_000 });
  await page.waitForFunction((navigationId) => {
    const navigation = document.getElementById(navigationId);
    const main = document.querySelector('main');
    const toggle = document.querySelector('.sidebar-menu-toggle');
    return Boolean(
      navigation?.classList.contains('shown')
      && document.body.classList.contains('dw-rustdoc-navigation-open')
      && main?.inert
      && toggle?.getAttribute('aria-expanded') === 'true'
    );
  }, NAVIGATION_ID, { timeout: 10_000 });

  const state = await page.evaluate(({ navigationId, toggleSelector }) => {
    const navigation = document.getElementById(navigationId);
    const main = document.querySelector('main');
    const toggle = document.querySelector(toggleSelector);
    if (!navigation || !main || !toggle) return null;

    const controlSelector = [
      'a[href]',
      'button:not([disabled])',
      'input:not([disabled])',
      'select:not([disabled])',
      'textarea:not([disabled])',
      'summary',
      '[tabindex]:not([tabindex="-1"])',
    ].join(',');
    const activeControls = (root) => [...root.querySelectorAll(controlSelector)].filter((element) => (
      element.getClientRects().length > 0
      && !element.closest('[inert]')
      && element.getAttribute('aria-hidden') !== 'true'
    ));
    const navigationBox = navigation.getBoundingClientRect();
    const mainBox = main.getBoundingClientRect();
    const overlapWidth = Math.max(0, Math.min(navigationBox.right, mainBox.right) - Math.max(navigationBox.left, mainBox.left));
    const overlapHeight = Math.max(0, Math.min(navigationBox.bottom, mainBox.bottom) - Math.max(navigationBox.top, mainBox.top));
    const backdrop = getComputedStyle(document.body, '::before');
    const transparentColors = new Set(['rgba(0, 0, 0, 0)', 'transparent']);

    return {
      navigation_id: navigation.id,
      navigation_tag: navigation.tagName.toLowerCase(),
      navigation_position: getComputedStyle(navigation).position,
      navigation_shown: navigation.classList.contains('shown'),
      navigation_inert: navigation.inert,
      navigation_aria_hidden: navigation.getAttribute('aria-hidden'),
      navigation_control_count: activeControls(navigation).length,
      body_open_class: document.body.classList.contains('dw-rustdoc-navigation-open'),
      background_inert: main.inert,
      background_aria_hidden: main.getAttribute('aria-hidden'),
      background_control_count: activeControls(main).length,
      toggle_expanded: toggle.getAttribute('aria-expanded'),
      toggle_controls: toggle.getAttribute('aria-controls'),
      focus_inside_navigation: navigation.contains(document.activeElement),
      backdrop_present: (
        backdrop.content !== 'none'
        && backdrop.position === 'fixed'
        && !transparentColors.has(backdrop.backgroundColor)
      ),
      overlap_width: overlapWidth,
      overlap_height: overlapHeight,
      rect: {
        x: navigationBox.x,
        y: navigationBox.y,
        width: navigationBox.width,
        height: navigationBox.height,
      },
    };
  }, { navigationId: NAVIGATION_ID, toggleSelector: TOGGLE_SELECTOR });

  if (
    !state
    || state.navigation_id !== NAVIGATION_ID
    || state.navigation_tag !== 'nav'
    || state.navigation_position !== 'fixed'
    || !state.navigation_shown
    || state.navigation_inert
    || state.navigation_aria_hidden !== null
    || state.navigation_control_count < 1
    || !state.body_open_class
    || !state.background_inert
    || state.background_aria_hidden !== 'true'
    || state.background_control_count !== 0
    || state.toggle_expanded !== 'true'
    || state.toggle_controls !== NAVIGATION_ID
    || !state.focus_inside_navigation
    || !state.backdrop_present
    || state.overlap_width <= 0
    || state.overlap_height <= 0
  ) {
    fail('the open Rust reference did not prove isolated dw-rustdoc-navigation state');
  }

  const overlay = {
    tag: state.navigation_tag,
    id: state.navigation_id,
    role: null,
    position: state.navigation_position,
    intentional_overlay: true,
    isolated_background_count: 1,
    rect: Object.fromEntries(Object.entries(state.rect).map(([key, value]) => [key, round(value)])),
    overlaps: [{
      tag: 'main',
      text: '',
      overlap_width: round(state.overlap_width),
      overlap_height: round(state.overlap_height),
    }],
  };
  report.geometry.intentional_overlays = [overlay];
  report.navigation_isolation = state;
  fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  try {
    bindResponsiveNavigationCapture({
      manifestPath: path.resolve(options.manifest),
      reportPath,
      width: options.width,
      height: options.height,
    });
  } catch (error) {
    fail(error.message);
  }
  await context.close();
} finally {
  await browser.close();
}

process.stdout.write(`Recorded isolated ${NAVIGATION_ID} state in ${reportPath}\n`);
