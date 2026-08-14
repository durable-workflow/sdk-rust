import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const runtime = readFileSync('docs/navigation.js', 'utf8');
const frames = [];
const observers = [];

class ClassList {
  constructor() {
    this.values = new Set();
  }

  add(value) {
    this.values.add(value);
  }

  remove(value) {
    this.values.delete(value);
  }

  contains(value) {
    return this.values.has(value);
  }

  toggle(value, force) {
    if (force) this.values.add(value);
    else this.values.delete(value);
  }
}

function element() {
  const attributes = new Map();
  const listeners = new Map();
  return {
    id: '',
    inert: false,
    classList: new ClassList(),
    addEventListener(event, listener) {
      listeners.set(event, listener);
    },
    dispatch(event, value = {}) {
      listeners.get(event)?.(value);
    },
    focus() {
      document.activeElement = this;
    },
    getAttribute(name) {
      return attributes.get(name) ?? null;
    },
    getClientRects() {
      return [{}];
    },
    hasAttribute(name) {
      return attributes.has(name);
    },
    removeAttribute(name) {
      attributes.delete(name);
    },
    setAttribute(name, value) {
      attributes.set(name, String(value));
    },
  };
}

const toggle = element();
const sidebar = element();
const firstLink = element();
const lastLink = element();
const main = element();
const body = element();
const compactListeners = new Map();
const compact = {
  matches: true,
  addEventListener(event, listener) {
    compactListeners.set(event, listener);
  },
  dispatch(event) {
    compactListeners.get(event)?.();
  },
};
const documentListeners = new Map();
const document = {
  activeElement: null,
  body,
  addEventListener(event, listener) {
    documentListeners.set(event, listener);
  },
  dispatch(event, value) {
    documentListeners.get(event)?.(value);
  },
  querySelector(selector) {
    return new Map([
      ['.sidebar', sidebar],
      ['.sidebar-menu-toggle', toggle],
      ['main', main],
    ]).get(selector) ?? null;
  },
};
sidebar.querySelectorAll = () => [firstLink, lastLink];

class MutationObserver {
  constructor(listener) {
    this.listener = listener;
    observers.push(this);
  }

  observe() {}
}

vm.runInNewContext(runtime, {
  Array,
  MutationObserver,
  document,
  requestAnimationFrame(callback) {
    frames.push(callback);
  },
  window: {
    matchMedia() {
      return compact;
    },
  },
});

assert.equal(sidebar.id, 'dw-rustdoc-navigation');
assert.equal(sidebar.getAttribute('aria-label'), 'API navigation');
assert.equal(sidebar.getAttribute('aria-hidden'), 'true');
assert.equal(sidebar.inert, true);
assert.equal(main.inert, false);
assert.equal(toggle.getAttribute('aria-expanded'), 'false');
assert.equal(toggle.getAttribute('aria-controls'), sidebar.id);

sidebar.classList.add('shown');
toggle.dispatch('click');
frames.shift()();
assert.equal(main.inert, true);
assert.equal(main.getAttribute('aria-hidden'), 'true');
assert.equal(sidebar.inert, false);
assert.equal(sidebar.hasAttribute('aria-hidden'), false);
assert.equal(toggle.getAttribute('aria-expanded'), 'true');
assert.equal(body.classList.contains('dw-rustdoc-navigation-open'), true);
assert.equal(document.activeElement, firstLink);

let prevented = false;
document.dispatch('keydown', {
  key: 'Tab',
  shiftKey: true,
  preventDefault() {
    prevented = true;
  },
});
assert.equal(prevented, true);
assert.equal(document.activeElement, toggle);

prevented = false;
toggle.focus();
document.dispatch('keydown', {
  key: 'Tab',
  shiftKey: true,
  preventDefault() {
    prevented = true;
  },
});
assert.equal(prevented, true);
assert.equal(document.activeElement, lastLink);

sidebar.classList.remove('shown');
toggle.dispatch('click');
frames.shift()();
assert.equal(main.inert, false);
assert.equal(main.hasAttribute('aria-hidden'), false);
assert.equal(sidebar.inert, true);
assert.equal(toggle.getAttribute('aria-expanded'), 'false');
assert.equal(body.classList.contains('dw-rustdoc-navigation-open'), false);
assert.equal(document.activeElement, toggle);

sidebar.classList.add('shown');
observers[0].listener();
firstLink.focus();
prevented = false;
document.dispatch('keydown', {
  key: 'Escape',
  preventDefault() {
    prevented = true;
  },
});
assert.equal(prevented, true);
assert.equal(sidebar.classList.contains('shown'), false);
assert.equal(main.inert, false);
assert.equal(document.activeElement, toggle);

compact.matches = false;
compact.dispatch('change');
assert.equal(sidebar.inert, false);
assert.equal(sidebar.hasAttribute('aria-hidden'), false);

console.log('Validated compact rustdoc navigation isolation and focus behavior.');
