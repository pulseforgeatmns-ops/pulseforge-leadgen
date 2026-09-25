/* ==========================================================================
   Studio Substral — narrative orchestration.

   Rules this file obeys:
   - Native scroll only. Nothing is intercepted, pinned by script, or paced
     for the user (doctrine §10, "no scroll hijacking").
   - Every value it writes is a CSS custom property or a data attribute, so
     the same states are reachable from the stylesheet alone.
   - Reduced motion is checked live, not once at load.
   - The dimensional object is requested last, and only when it is wanted.
   ========================================================================== */

import { initAssessment } from './assessment.js';

const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)');
const clamp = (n, min = 0, max = 1) => (n < min ? min : n > max ? max : n);

/* Weighted interpolation. Low factor = high apparent mass (doctrine §13). */
const approach = (current, target, factor) => current + (target - current) * factor;

const LAYER_NAMES = [
  'Performance',
  'Accessibility',
  'Conversion',
  'Search',
  'Trust',
  'Design',
];

/* --------------------------------------------------------------------------
   Progress of a scrolling region through the viewport, 0 → 1.
   -------------------------------------------------------------------------- */

function regionProgress(el) {
  const rect = el.getBoundingClientRect();
  const travel = rect.height - window.innerHeight;
  if (travel <= 0) {
    // Region is shorter than the viewport: use its position instead so the
    // narrative still advances on small screens.
    const span = window.innerHeight + rect.height;
    return clamp((window.innerHeight - rect.top) / span);
  }
  return clamp(-rect.top / travel);
}

/* --------------------------------------------------------------------------
   Stage: the dimensional object plus the state it derives from scroll.
   -------------------------------------------------------------------------- */

class Stage {
  constructor(root, { region, mode }) {
    this.root = root;
    this.region = region;
    this.mode = mode; // 'decompose' | 'reconstruct'
    this.canvas = root.querySelector('[data-stage-canvas]');
    this.plates = [...root.querySelectorAll('.plate')];
    this.readoutIndex = root.querySelector('[data-readout-index]');
    this.readoutName = root.querySelector('[data-readout-name]');

    this.progress = 0;
    this.eased = this.progress;
    this.pointer = { x: 0, y: 0 };
    this.visible = false;
    this.activeIndex = -1;
    /* Set by the layer observer when one is authoritative. Which layer the
       reader is on is information, not motion, so it is never damped. */
    this.reportedIndex = null;
    this.object = null;
    this.frame = null;
  }

  /* Separation in px for the CSS baseline composition. Act I holds the object
     assembled and only hints at the seams; Act II pulls it apart; Act VI
     brings it back together. */
  separation() {
    const t = this.eased;
    if (this.mode === 'surface') return 4 + t * 7;
    if (this.mode === 'reconstruct') return 4 + (1 - t) * 40;
    return 5 + t * 38;
  }

  /* Which of the six layers is currently under examination. Act I examines
     nothing: the object is still whole.

     This reads from the undamped progress — or, better, from whichever layer
     the reader is actually looking at. A label that lags a second behind the
     heading beside it is wrong, not weighted. */
  index() {
    if (this.mode === 'surface') return -1;
    if (this.reportedIndex != null) return this.reportedIndex;
    return clamp(
      Math.floor(this.progress * LAYER_NAMES.length),
      0,
      LAYER_NAMES.length - 1
    );
  }

  setReportedIndex(index) {
    if (this.reportedIndex === index) return;
    this.reportedIndex = index;
    this.write();
    this.request();
  }

  write() {
    const sep = this.separation();
    this.root.style.setProperty('--sep', `${sep.toFixed(2)}px`);
    this.root.style.setProperty('--lift', (this.pointer.y * 8).toFixed(2));

    const idx = this.index();
    if (idx !== this.activeIndex) {
      this.activeIndex = idx;
      this.plates.forEach((plate, i) => {
        plate.dataset.active = String(i === idx);
      });
      if (this.readoutIndex) {
        this.readoutIndex.textContent = String(idx + 1).padStart(2, '0');
      }
      if (this.readoutName) {
        this.readoutName.textContent = LAYER_NAMES[idx];
      }
    }

    if (this.object) {
      this.object.update({
        progress: this.eased,
        pointer: this.pointer,
        activeIndex: idx,
      });
    }
  }

  settle() {
    /* Static states only: no depth-based scroll animation (doctrine §19).
       Each act settles on the state its narrative needs — Act I whole, Act II
       already decomposed, Act VI whole again. */
    this.eased = this.mode === 'surface' ? 0 : 1;
    this.pointer = { x: 0, y: 0 };
    this.write();
  }

  tick = () => {
    this.frame = null;
    if (reduceMotion.matches) {
      this.settle();
      return;
    }

    this.progress = regionProgress(this.region);
    const before = this.eased;
    this.eased = approach(this.eased, this.progress, 0.1);

    this.write();

    // Keep running while the value is still travelling or the object needs
    // frames. Stop as soon as everything has come to rest — no ambient motion.
    const moving = Math.abs(this.eased - this.progress) > 0.0005
      || Math.abs(this.eased - before) > 0.0005
      || (this.object && this.object.isMoving());
    if (this.visible && moving) this.request();
  };

  request() {
    if (this.frame == null) this.frame = requestAnimationFrame(this.tick);
  }

  setVisible(visible) {
    this.visible = visible;
    if (visible) {
      if (reduceMotion.matches) this.settle();
      else this.request();
    }
    if (this.object) this.object.setActive(visible && !reduceMotion.matches);
  }

  attachObject(object) {
    this.object = object;
    this.root.dataset.webgl = 'on';
    this.object.setActive(this.visible && !reduceMotion.matches);
    this.request();
  }
}

/* --------------------------------------------------------------------------
   Pointer parallax. Deliberately below the threshold of obvious
   cause-and-effect (doctrine §13).
   -------------------------------------------------------------------------- */

function observePointer(stages) {
  if (!window.matchMedia('(hover: hover) and (pointer: fine)').matches) return;

  let queued = false;
  window.addEventListener(
    'pointermove',
    (event) => {
      if (reduceMotion.matches || queued) return;
      queued = true;
      requestAnimationFrame(() => {
        queued = false;
        const x = (event.clientX / window.innerWidth - 0.5) * 2;
        const y = (event.clientY / window.innerHeight - 0.5) * 2;
        for (const stage of stages) {
          if (!stage.visible) continue;
          stage.pointer = { x, y };
          stage.request();
        }
      });
    },
    { passive: true }
  );
}

/* --------------------------------------------------------------------------
   Act II — the layer currently being read gets the accent.
   -------------------------------------------------------------------------- */

function observeLayers(stage) {
  const layers = [...document.querySelectorAll('[data-layer]')];
  if (!layers.length) return;

  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        entry.target.dataset.active = String(entry.isIntersecting);
        if (entry.isIntersecting) {
          stage?.setReportedIndex(Number(entry.target.dataset.layer));
        }
      }
    },
    // A narrow band across the middle of the viewport, so exactly one layer is
    // under examination at a time.
    { rootMargin: '-42% 0px -42% 0px' }
  );

  layers.forEach((layer) => observer.observe(layer));
}

/* --------------------------------------------------------------------------
   Act VI — the six names drift, then align. One final assembly.
   -------------------------------------------------------------------------- */

function observeConvergence() {
  const list = document.querySelector('[data-converge]');
  if (!list) return;
  const items = [...list.querySelectorAll('.converge__item')];
  const base = items.map((item) =>
    Number.parseFloat(item.style.getPropertyValue('--drift')) || 0
  );

  const align = (aligned) => {
    items.forEach((item, i) => {
      item.style.setProperty('--drift', aligned ? '0' : String(base[i]));
      item.dataset.aligned = String(aligned);
    });
  };

  if (reduceMotion.matches) {
    align(true);
    return;
  }

  let frame = null;
  let eased = 0;

  const run = () => {
    frame = null;
    const target = regionProgress(list.closest('.converge') || list);
    const previous = eased;
    eased = approach(eased, target, 0.07);

    items.forEach((item, i) => {
      // Each name resolves at a slightly different point, so the assembly
      // reads as six parts settling rather than one group sliding.
      const local = clamp((eased - i * 0.055) / 0.5);
      item.style.setProperty('--drift', (base[i] * (1 - local)).toFixed(2));
      item.dataset.aligned = String(local > 0.92);
    });

    if (Math.abs(eased - target) > 0.0008 || Math.abs(eased - previous) > 0.0008) {
      frame = requestAnimationFrame(run);
    }
  };

  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (entry.isIntersecting && frame == null) frame = requestAnimationFrame(run);
      }
    },
    { rootMargin: '10% 0px 10% 0px' }
  );
  observer.observe(list);
}

/* --------------------------------------------------------------------------
   One weighted settle per element on first appearance. Never repeated.
   -------------------------------------------------------------------------- */

function observeReveals() {
  const targets = [...document.querySelectorAll('[data-reveal]')];
  if (!targets.length) return;

  if (reduceMotion.matches) {
    targets.forEach((el) => (el.dataset.revealed = 'true'));
    return;
  }

  const observer = new IntersectionObserver(
    (entries, obs) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) continue;
        entry.target.dataset.revealed = 'true';
        obs.unobserve(entry.target);
      }
    },
    { rootMargin: '0px 0px -12% 0px' }
  );

  targets.forEach((el) => observer.observe(el));

  // Anything already on screen settles immediately rather than waiting for a
  // scroll event that may never come.
  requestAnimationFrame(() => {
    targets.forEach((el) => {
      const rect = el.getBoundingClientRect();
      if (rect.top < window.innerHeight * 0.92) el.dataset.revealed = 'true';
    });
  });
}

/* --------------------------------------------------------------------------
   Navigation: lifted state and current section.
   -------------------------------------------------------------------------- */

function observeNav() {
  const nav = document.querySelector('[data-nav]');
  if (!nav) return;

  const sentinel = document.getElementById('surface');
  if (sentinel) {
    new IntersectionObserver(
      ([entry]) => {
        nav.dataset.lifted = String(!entry.isIntersecting);
      },
      { rootMargin: '-72px 0px 0px 0px', threshold: 0 }
    ).observe(sentinel);
  }

  const links = [...nav.querySelectorAll('.nav__link')];
  const sections = links
    .map((link) => ({ link, section: document.querySelector(link.hash) }))
    .filter((pair) => pair.section);

  if (!sections.length) return;

  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        const pair = sections.find((p) => p.section === entry.target);
        if (!pair) continue;
        if (entry.isIntersecting) {
          sections.forEach((p) => p.link.removeAttribute('aria-current'));
          pair.link.setAttribute('aria-current', 'true');
        }
      }
    },
    { rootMargin: '-45% 0px -45% 0px' }
  );

  sections.forEach((pair) => observer.observe(pair.section));
}

/* --------------------------------------------------------------------------
   The dimensional object is an enhancement. It is requested only when it is
   both wanted and affordable, and never before the document is interactive.
   -------------------------------------------------------------------------- */

function webglAvailable() {
  try {
    const canvas = document.createElement('canvas');
    return Boolean(
      canvas.getContext('webgl2') ||
        canvas.getContext('webgl') ||
        canvas.getContext('experimental-webgl')
    );
  } catch {
    return false;
  }
}

function shouldRenderObject() {
  if (reduceMotion.matches) return false;
  if (!webglAvailable()) return false;
  // Below this width the CSS composition is the intended treatment, not a
  // fallback: fewer simultaneous objects, no lighting cost (doctrine §17).
  if (window.innerWidth < 600) return false;
  if (navigator.connection?.saveData) return false;
  if (typeof navigator.deviceMemory === 'number' && navigator.deviceMemory < 2) return false;
  return true;
}

function loadObject(stages) {
  if (!shouldRenderObject()) return;

  let requested = false;
  const start = () => {
    if (requested) return;
    requested = true;
    import('./dimensional.js')
      .then(({ createDimensionalObject }) => {
        for (const stage of stages) {
          if (!stage.canvas) continue;
          const object = createDimensionalObject(stage.canvas, { mode: stage.mode });
          if (object) stage.attachObject(object);
        }
      })
      .catch(() => {
        /* The baseline composition is already on screen. Nothing to recover. */
      });
  };

  // Request when the first stage is within a screen of the viewport.
  const observer = new IntersectionObserver(
    (entries) => {
      if (entries.some((entry) => entry.isIntersecting)) {
        observer.disconnect();
        if ('requestIdleCallback' in window) {
          requestIdleCallback(start, { timeout: 1200 });
        } else {
          setTimeout(start, 200);
        }
      }
    },
    { rootMargin: '100% 0px 100% 0px' }
  );
  stages.forEach((stage) => observer.observe(stage.root));
}

/* --------------------------------------------------------------------------
   Boot
   -------------------------------------------------------------------------- */

function boot() {
  const stages = [];

  const surface = document.querySelector('[data-stage="surface"]');
  if (surface) {
    stages.push(
      new Stage(surface, { region: document.getElementById('surface'), mode: 'surface' })
    );
  }

  const decomposition = document.querySelector('[data-stage="decomposition"]');
  if (decomposition) {
    const region =
      decomposition.closest('.decomposition__layout') ||
      document.getElementById('decomposition');
    stages.push(new Stage(decomposition, { region, mode: 'decompose' }));
  }

  const reconstruction = document.querySelector('[data-stage="reconstruction"]');
  if (reconstruction) {
    const region =
      reconstruction.closest('.reconstruction__layout') ||
      document.getElementById('reconstruction');
    stages.push(new Stage(reconstruction, { region, mode: 'reconstruct' }));
  }

  const stageObserver = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        const stage = stages.find((s) => s.root === entry.target);
        stage?.setVisible(entry.isIntersecting);
      }
    },
    { rootMargin: '15% 0px 15% 0px' }
  );
  stages.forEach((stage) => {
    stageObserver.observe(stage.root);
    stage.write();
  });

  observePointer(stages);
  observeLayers(stages.find((stage) => stage.mode === 'decompose'));
  observeConvergence();
  observeReveals();
  observeNav();
  initAssessment();
  loadObject(stages);

  // A user turning reduced motion on mid-session gets the static reading
  // immediately, without a reload.
  reduceMotion.addEventListener('change', () => {
    for (const stage of stages) {
      if (reduceMotion.matches) {
        stage.object?.setActive(false);
        stage.settle();
      } else {
        stage.object?.setActive(stage.visible);
        stage.request();
      }
    }
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', boot, { once: true });
} else {
  boot();
}
