(function () {
  'use strict';

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
