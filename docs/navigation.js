(function () {
  'use strict';

  const back = document.querySelector('[data-rustdoc-history-back]');
  if (back) {
    back.addEventListener('click', function (event) {
      if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
      if (window.history.length > 1) {
        event.preventDefault();
        window.history.back();
      }
    });
  }

  const sidebar = document.querySelector('.sidebar');
  const toggle = document.querySelector('.sidebar-menu-toggle');
  const main = document.querySelector('main');
  if (!sidebar || !toggle || !main) return;

  const compact = window.matchMedia('(max-width: 700px)');
  const focusableSelector = [
    'a[href]',
    'button:not([disabled])',
    'input:not([disabled])',
    'select:not([disabled])',
    'textarea:not([disabled])',
    'summary',
    '[tabindex]:not([tabindex="-1"])',
  ].join(',');

  if (!sidebar.id) sidebar.id = 'dw-rustdoc-navigation';
  if (!sidebar.hasAttribute('aria-label')) {
    sidebar.setAttribute('aria-label', 'API navigation');
  }
  toggle.setAttribute('aria-controls', sidebar.id);

  function isOpen() {
    return compact.matches && sidebar.classList.contains('shown');
  }

  function sidebarControls() {
    return Array.from(sidebar.querySelectorAll(focusableSelector)).filter((element) => (
      element.getClientRects().length > 0
      && !element.hasAttribute('disabled')
      && element.getAttribute('aria-hidden') !== 'true'
    ));
  }

  function synchronize() {
    const open = isOpen();
    main.inert = open;
    if (open) {
      main.setAttribute('aria-hidden', 'true');
    } else {
      main.removeAttribute('aria-hidden');
    }

    const compactAndClosed = compact.matches && !open;
    sidebar.inert = compactAndClosed;
    if (compactAndClosed) {
      sidebar.setAttribute('aria-hidden', 'true');
    } else {
      sidebar.removeAttribute('aria-hidden');
    }

    toggle.setAttribute('aria-expanded', String(open));
    toggle.setAttribute('title', open ? 'hide sidebar' : 'show sidebar');
    document.body.classList.toggle('dw-rustdoc-navigation-open', open);
    return open;
  }

  const observer = new MutationObserver(synchronize);
  observer.observe(sidebar, {attributes: true, attributeFilter: ['class']});
  compact.addEventListener('change', synchronize);

  toggle.addEventListener('click', function () {
    requestAnimationFrame(function () {
      if (synchronize()) {
        sidebarControls()[0]?.focus();
      } else {
        toggle.focus();
      }
    });
  });

  document.addEventListener('keydown', function (event) {
    if (!isOpen()) return;

    if (event.key === 'Escape') {
      event.preventDefault();
      sidebar.classList.remove('shown');
      synchronize();
      toggle.focus();
      return;
    }

    if (event.key !== 'Tab') return;
    const navigationControls = sidebarControls();
    const first = navigationControls[0] || toggle;
    const last = navigationControls[navigationControls.length - 1] || toggle;
    if (document.activeElement === toggle) {
      event.preventDefault();
      (event.shiftKey ? last : first).focus();
    } else if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      toggle.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      toggle.focus();
    } else if (!navigationControls.includes(document.activeElement)) {
      event.preventDefault();
      toggle.focus();
    }
  }, true);

  synchronize();
}());

(function () {
  'use strict';
  const root = document.documentElement;
  if (!root || !document.querySelectorAll || !document.body) return;

  const themeButtons = document.querySelectorAll('[data-dw-theme-toggle]');
  function applyTheme(theme) {
    root.dataset.theme = theme;
    for (const button of themeButtons) {
      const dark = theme === 'dark';
      button.setAttribute('aria-pressed', String(dark));
      button.setAttribute('aria-label', `Switch to ${dark ? 'light' : 'dark'} theme`);
      button.querySelector('[data-dw-theme-label]').textContent = dark ? 'Light' : 'Dark';
    }
  }
  let theme = 'light';
  try {
    if (window.localStorage.getItem('dw-theme') === 'dark') theme = 'dark';
  } catch { /* Theme selection also works when browser storage is unavailable. */ }
  applyTheme(theme);
  for (const button of themeButtons) {
    button.addEventListener('click', function () {
      const next = root.dataset.theme === 'dark' ? 'light' : 'dark';
      applyTheme(next);
      try { window.localStorage.setItem('dw-theme', next); } catch { /* Keep this visit's selection. */ }
    });
  }

  const header = document.querySelector('.dw-site-header');
  if (header) {
    const resize = () => root.style.setProperty('--dw-header-height', `${header.getBoundingClientRect().height}px`);
    resize();
    if (typeof ResizeObserver === 'function') new ResizeObserver(resize).observe(header);
    else window.addEventListener('resize', resize);
  }
  for (const menu of document.querySelectorAll('.dw-sdk-switcher')) {
    menu.addEventListener('keydown', function (event) {
      if (event.key === 'Escape' && menu.open) {
        event.preventDefault();
        menu.open = false;
        menu.querySelector('summary').focus();
      }
    });
    document.addEventListener('pointerdown', function (event) {
      if (menu.open && !menu.contains(event.target)) menu.open = false;
    });
  }

  async function copyCommand(text) {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return;
    }
    throw new Error('Clipboard unavailable');
  }
  for (const button of document.querySelectorAll('[data-dw-copy]')) {
    button.addEventListener('click', async function () {
      const command = document.getElementById(button.dataset.dwCopy);
      const feedback = document.querySelector('[data-dw-copy-feedback]');
      if (!command) return;
      try {
        await copyCommand(command.textContent.trim());
        button.textContent = 'Copied';
        if (feedback) feedback.textContent = 'Command copied to clipboard.';
      } catch {
        if (feedback) feedback.textContent = 'Clipboard unavailable. Select and copy the command.';
      } finally {
        button.focus();
        window.setTimeout(() => { button.textContent = 'Copy'; }, 1800);
      }
    });
  }
}());
