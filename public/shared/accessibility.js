'use strict';

// Pulseforge shared accessibility helpers (Phase A2).
// Focus trapping, Escape handling, focus restoration, keyboard tab
// navigation, and polite status announcements. No dependencies.

(function () {
  const FOCUSABLE = [
    'a[href]', 'button:not([disabled])', 'input:not([disabled])',
    'select:not([disabled])', 'textarea:not([disabled])', '[tabindex]:not([tabindex="-1"])',
  ].join(', ');

  function focusables(container) {
    return Array.from(container.querySelectorAll(FOCUSABLE))
      .filter(el => el.offsetParent !== null || el === document.activeElement);
  }

  /**
   * Trap focus inside a dialog element. Returns a release() function that
   * removes listeners and restores focus to the previously focused element.
   * onEscape is invoked when Escape is pressed.
   */
  function trapFocus(container, { onEscape } = {}) {
    const previouslyFocused = document.activeElement;
    function handleKeydown(event) {
      if (event.key === 'Escape' && typeof onEscape === 'function') {
        event.stopPropagation();
        onEscape();
        return;
      }
      if (event.key !== 'Tab') return;
      const items = focusables(container);
      if (!items.length) return;
      const first = items[0];
      const last = items[items.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    }
    container.addEventListener('keydown', handleKeydown);
    const initial = focusables(container)[0];
    if (initial) initial.focus();
    return function release() {
      container.removeEventListener('keydown', handleKeydown);
      if (previouslyFocused && typeof previouslyFocused.focus === 'function') {
        previouslyFocused.focus();
      }
    };
  }

  /** Arrow-key navigation for a role=tablist container. */
  function enableTablistKeyboard(tablist, onActivate) {
    tablist.addEventListener('keydown', event => {
      if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
      const tabs = Array.from(tablist.querySelectorAll('[role="tab"]'));
      const currentIndex = tabs.indexOf(document.activeElement);
      if (currentIndex === -1) return;
      event.preventDefault();
      let nextIndex = currentIndex;
      if (event.key === 'ArrowRight') nextIndex = (currentIndex + 1) % tabs.length;
      if (event.key === 'ArrowLeft') nextIndex = (currentIndex - 1 + tabs.length) % tabs.length;
      if (event.key === 'Home') nextIndex = 0;
      if (event.key === 'End') nextIndex = tabs.length - 1;
      tabs[nextIndex].focus();
      if (typeof onActivate === 'function') onActivate(tabs[nextIndex]);
    });
  }

  function liveRegion() {
    let region = document.getElementById('pf-live-region');
    if (!region) {
      region = document.createElement('div');
      region.id = 'pf-live-region';
      region.setAttribute('role', 'status');
      region.setAttribute('aria-live', 'polite');
      document.body.appendChild(region);
    }
    return region;
  }

  /** Announce a status message (saves, errors) to screen readers. */
  function announce(message, { assertive = false } = {}) {
    const region = liveRegion();
    region.setAttribute('aria-live', assertive ? 'assertive' : 'polite');
    region.textContent = '';
    // Force a mutation so repeated identical messages are re-announced.
    window.setTimeout(() => { region.textContent = String(message || ''); }, 30);
  }

  window.PulseforgeA11y = { announce, enableTablistKeyboard, trapFocus };
})();
