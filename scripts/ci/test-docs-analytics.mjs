import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const runtime = readFileSync('docs/analytics/analytics.js', 'utf8');
const token = '0123456789abcdef0123456789abcdef';

function executeRuntime(webdriver) {
  const appendedScripts = [];
  const fetches = [];
  const listeners = [];
  const action = {
    addEventListener(event, listener) {
      listeners.push({event, listener});
    },
  };
  const promotion = {
    querySelector(selector) {
      assert.equal(selector, '[data-promotion-action="early-access"]');
      return action;
    },
  };
  const document = {
    createElement(tag) {
      assert.equal(tag, 'script');
      return {dataset: {}};
    },
    getElementById() {
      return null;
    },
    head: {
      appendChild(script) {
        appendedScripts.push(script);
      },
    },
    querySelector(selector) {
      if (selector === '[data-cloudflare-web-analytics-token]') {
        return {dataset: {cloudflareWebAnalyticsToken: token}};
      }
      if (selector === '[data-promotion-source="sdk-rust-reference"]') {
        return promotion;
      }
      if (selector.startsWith('script[src^=')) return null;
      assert.fail(`unexpected selector: ${selector}`);
    },
  };
  const window = {
    fetch(...args) {
      fetches.push(args);
      return Promise.resolve();
    },
    location: {
      hostname: 'rust.durable-workflow.com',
      pathname: '/',
    },
  };

  vm.runInNewContext(runtime, {
    document,
    navigator: {webdriver},
    window,
  });

  return {appendedScripts, fetches, listeners};
}

const automated = executeRuntime(true);
assert.deepEqual(automated.appendedScripts, []);
assert.deepEqual(automated.fetches, []);
assert.deepEqual(automated.listeners, []);

const ordinary = executeRuntime(false);
assert.equal(ordinary.appendedScripts.length, 1);
assert.equal(
  ordinary.appendedScripts[0].src,
  'https://static.cloudflareinsights.com/beacon.min.js',
);
assert.equal(ordinary.appendedScripts[0].type, 'module');
assert.deepEqual(
  JSON.parse(ordinary.appendedScripts[0].dataset.cfBeacon),
  {token},
);
assert.equal(ordinary.fetches.length, 1);
assert.equal(ordinary.listeners.length, 1);
assert.equal(ordinary.listeners[0].event, 'click');

console.log('Validated analytics suppression for WebDriver and preservation for ordinary browsers.');
