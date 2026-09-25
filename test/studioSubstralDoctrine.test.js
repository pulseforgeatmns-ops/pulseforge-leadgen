'use strict';

/**
 * Studio Substral — Design & Experience Doctrine v1 conformance.
 *
 * The doctrine is the creative source of truth, and several of its rules are
 * mechanically checkable: the layer ordering, the evidence taxonomy, the
 * forbidden copy and visual patterns, the accessibility floor, and the
 * performance budget the site publishes about itself in its own colophon.
 *
 * These are the checks that should fail loudly if a later edit quietly
 * converts the site into "a premium dark website" (doctrine §26).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { gzipSync } = require('node:zlib');

const {
  PROHIBITED_CLAIM_PATTERNS,
  EVIDENCE_CLASS,
  DIAGNOSIS_CLASS,
} = require('../packages/capabilities/websiteOpportunityIntelligence/types');

const SITE = path.join(__dirname, '..', 'sites', 'studio-substral');
const read = (...parts) => fs.readFileSync(path.join(SITE, ...parts), 'utf8');

const html = read('index.html');
const css = read('assets', 'css', 'substral.css');
const orchestration = read('assets', 'js', 'substral.js');
const assessmentJs = read('assets', 'js', 'assessment.js');
const dimensionalSrc = read('src', 'dimensional.js');

/** Text content of the page with tags and entities stripped. */
const copy = html
  .replace(/<script[\s\S]*?<\/script>/gi, ' ')
  .replace(/<style[\s\S]*?<\/style>/gi, ' ')
  .replace(/<[^>]+>/g, ' ')
  .replace(/&[a-z]+;/gi, ' ')
  .replace(/\s+/g, ' ');

const head = html.slice(html.indexOf('<head'), html.indexOf('</head>'));

/* -------------------------------------------------------------------------- */

describe('Studio Substral — brand and narrative (doctrine §2, §3, §14)', () => {
  it('leads with the core statement, not a marketing summary', () => {
    assert.match(copy, /Look beneath\s*the surface\./i);
    assert.match(copy, /Your website is working\./i);
    assert.match(copy, /But is it working for you\?/i);
  });

  it('carries the core philosophy and the closing principle verbatim', () => {
    assert.match(copy, /We measure what exists before deciding what should change\./i);
    assert.match(
      copy,
      /What.{0,3}s underneath\s*determines what\s*happens above it\./i
    );
  });

  it('uses SUBSTRAL as the wordmark with STUDIO as secondary metadata', () => {
    assert.match(html, /class="wordmark__name">Substral</);
    assert.match(html, /class="wordmark__meta">Studio \/ Manchester, NH</);
  });

  it('presents the primary CTA as an assessment', () => {
    assert.match(copy, /Start with an assessment/);
    assert.match(copy, /See what we see\./i);
  });

  it('runs all six acts in order', () => {
    const acts = ['Act II', 'Act III', 'Act IV', 'Act V', 'Act VI'];
    let cursor = html.indexOf('id="surface"');
    assert.ok(cursor > -1, 'Act I (surface) is missing');
    for (const act of acts) {
      const next = html.indexOf(act, cursor);
      assert.ok(next > cursor, `${act} is missing or out of sequence`);
      cursor = next;
    }
  });

  it('moves dark, to mineral, and back to dark', () => {
    const scopes = [...html.matchAll(/class="act ([a-z]+) (env-dark|env-mineral)/g)].map(
      (m) => m[2]
    );
    assert.deepEqual(scopes, [
      'env-dark', // I surface
      'env-dark', // II decomposition
      'env-mineral', // III diagnosis
      'env-mineral', // IV assessment
      'env-mineral', // V work
      'env-dark', // VI reconstruction
    ]);
  });
});

describe('The six layers (doctrine §12)', () => {
  const EXPECTED = [
    'Performance',
    'Accessibility',
    'Conversion',
    'Search',
    'Design',
  ];

  it('decomposes into exactly six layers', () => {
    const layers = [...html.matchAll(/data-layer="(\d)"/g)].map((m) => Number(m[1]));
    assert.deepEqual(layers, [0, 1, 2, 3, 4, 5]);
  });

  it('orders them Performance, Accessibility, Conversion, Search, Trust, Design', () => {
    const names = [...html.matchAll(/class="layer__name">([^<]+)</g)].map((m) => m[1]);
    assert.deepEqual(names, [
      'Performance',
      'Accessibility',
      'Conversion',
      'Search',
      'Trust',
      'Design',
    ]);
  });

  it('keeps DESIGN last and says why the order is not aesthetic', () => {
    assert.match(html, /id="layer-design"[\s\S]*?class="layer__name">Design</);
    const designIndex = html.indexOf('id="layer-design"');
    for (const earlier of EXPECTED.slice(0, 4)) {
      assert.ok(
        html.indexOf(`>${earlier}<`) < designIndex,
        `${earlier} must precede Design`
      );
    }
    assert.match(copy, /Design is last\./);
    assert.match(copy, /The order is not stylistic/);
  });

  it('reuses the same six names in the reconstruction act', () => {
    const converge = html.slice(html.indexOf('data-converge'));
    for (const name of ['Performance', 'Accessibility', 'Conversion', 'Search', 'Trust', 'Design']) {
      assert.match(converge, new RegExp(`<span>${name}</span>`));
    }
  });
});

describe('Assessment integrity (doctrine §16)', () => {
  it('states all four evidence classes', () => {
    for (const cls of Object.keys(EVIDENCE_CLASS)) {
      const label = cls[0] + cls.slice(1).toLowerCase();
      assert.match(
        html,
        new RegExp(`class="taxonomy__class">${label}<`),
        `evidence class ${cls} is not declared on the page`
      );
    }
  });

  it('separates the four report sections', () => {
    const labels = [...html.matchAll(/class="report__label">([^<]+)</g)].map((m) =>
      m[1].replace(/&rsquo;/g, "'")
    );
    assert.deepEqual(labels, [
      'What we measured',
      'What we observed',
      'What it may mean',
      "What we'd investigate next",
    ]);
  });

  it('never presents a score', () => {
    assert.doesNotMatch(copy, /\b\d{1,3}\s*\/\s*100\b/);
    assert.doesNotMatch(copy, /\byour (website|site) score\b/i);
    assert.doesNotMatch(copy, /\bgrade\b\s*[:=]/i);
    // And it says out loud that it will not produce one.
    assert.match(copy, /A score out of one hundred/i);
  });

  it('contains none of the claims the assessment engine itself prohibits', () => {
    for (const pattern of PROHIBITED_CLAIM_PATTERNS) {
      assert.doesNotMatch(
        copy,
        pattern,
        `page copy trips the engine's prohibited-claim guard: ${pattern}`
      );
    }
  });

  it('refuses the specific manufactured-urgency claims by name', () => {
    assert.match(copy, /revenue you are losing, when we do not have your revenue data/i);
    assert.match(copy, /legal compliance verdict derived from automated accessibility checks/i);
    assert.match(copy, /guarantee of search ranking or conversion improvement/i);
    assert.match(copy, /Urgency the evidence does not support/i);
  });

  it('uses no fabricated metrics or counters anywhere', () => {
    assert.doesNotMatch(copy, /\b\d+% (increase|more|faster|lift|growth|improvement)\b/i);
    assert.doesNotMatch(copy, /\b\d+x (more|faster|better)\b/i);
    assert.doesNotMatch(copy, /\b(happy clients|projects delivered|years of experience)\b/i);
  });
});

describe('Discover → Diagnose → Advise (doctrine §15)', () => {
  it('names the three movements in order', () => {
    const steps = [...html.matchAll(/class="mono">(Discover|Diagnose|Advise)</g)].map(
      (m) => m[1]
    );
    assert.deepEqual(steps, ['Discover', 'Diagnose', 'Advise']);
  });

  it('exposes the same four diagnosis classes the engine emits', () => {
    const shown = [...html.matchAll(/class="conclusion__class">([^<]+)</g)].map((m) =>
      m[1].toUpperCase().replace(/\s+/g, '_')
    );
    assert.deepEqual(shown.sort(), Object.keys(DIAGNOSIS_CLASS).sort());
  });

  it('is willing to conclude that no redesign is required', () => {
    assert.match(copy, /No redesign required.{0,2} is a conclusion we are willing to reach/i);
  });
});

describe('Copy doctrine (doctrine §8, §23)', () => {
  const BANNED = [
    /passionate about/i,
    /cutting[- ]edge/i,
    /innovative solutions?/i,
    /elevate your brand/i,
    /digital transformation/i,
    /unlock your (potential|growth)/i,
    /best[- ]in[- ]class/i,
    /game[- ]chang(er|ing)/i,
    /seamless(ly)?/i,
    /synerg/i,
    /world[- ]class/i,
    /take your .* to the next level/i,
    /we craft/i,
    /bespoke digital/i,
    /holistic approach/i,
  ];

  it('avoids agency cliché and hype', () => {
    for (const pattern of BANNED) {
      assert.doesNotMatch(copy, pattern, `banned marketing phrase present: ${pattern}`);
    }
  });

  it('keeps the two hero statements short', () => {
    const hero = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/)[1];
    const words = hero.replace(/<[^>]+>/g, ' ').trim().split(/\s+/);
    assert.ok(words.length <= 6, `hero headline is ${words.length} words`);
  });

  it('does not lean on rhetorical questions outside the layer questions', () => {
    // The six layer questions are the intended use. Anything beyond a couple
    // more is the "excessive rhetorical questions" the doctrine warns about.
    const questions = copy.match(/\?/g) || [];
    assert.ok(questions.length <= 8, `${questions.length} question marks in page copy`);
  });
});

describe('Forbidden visual patterns (doctrine §10)', () => {
  it('ships no neon, mesh or blob gradient decoration', () => {
    assert.doesNotMatch(css, /conic-gradient/);
    assert.doesNotMatch(css, /filter:\s*blur\(\s*[6-9]\d|filter:\s*blur\(\s*\d{3}/);
    assert.doesNotMatch(css, /#0ff|#f0f|#00ffff|#ff00ff/i);
    // Exactly one restrained radial wash, anchored to the object.
    const radials = css.match(/radial-gradient/g) || [];
    assert.ok(radials.length <= 2, `${radials.length} radial gradients`);
  });

  it('does not use glassmorphism as a general language', () => {
    const blurs = css.match(/backdrop-filter/g) || [];
    assert.ok(blurs.length <= 1, `${blurs.length} backdrop-filter declarations`);
  });

  it('does not use rounded rectangles as the default object language', () => {
    const radii = css.match(/border-radius/g) || [];
    assert.equal(radii.length, 0, 'border-radius is not part of this design language');
  });

  it('avoids drop shadow as decoration', () => {
    const shadows = css.match(/box-shadow:/g) || [];
    assert.ok(shadows.length <= 3, `${shadows.length} box-shadow declarations`);
    assert.doesNotMatch(css, /text-shadow/);
  });

  it('has no carousel, counter, marquee or parallax-background machinery', () => {
    assert.doesNotMatch(html, /carousel|testimonial|slider/i);
    assert.doesNotMatch(orchestration, /carousel|marquee|odometer|countUp/i);
    assert.doesNotMatch(css, /background-attachment:\s*fixed/);
  });

  it('does not hijack scrolling or replace the cursor', () => {
    assert.doesNotMatch(orchestration, /preventDefault\s*\(\s*\)[\s\S]{0,80}(wheel|scroll)/);
    assert.doesNotMatch(orchestration, /addEventListener\(\s*['"]wheel/);
    assert.doesNotMatch(orchestration, /scrollTo|scrollIntoView|scroll-snap/);
    assert.doesNotMatch(css, /cursor:\s*(none|url\()/);
  });

  it('presents work as editorial stories rather than a portfolio grid', () => {
    assert.match(html, /class="study__movements"/);
    const movements = [...html.matchAll(/class="study__movement">\s*<h3>([^<]+)</g)].map(
      (m) => m[1]
    );
    assert.deepEqual(movements, [
      'Context',
      'Diagnosis',
      'Decision',
      'Experience',
      'Outcome',
    ]);
  });

  it('shows real work rather than a device mockup', () => {
    assert.doesNotMatch(html, /laptop|macbook|iphone|mockup|device-frame/i);
    assert.match(html, /assets\/work\/anchor-cleaning-home\.webp/);
  });
});

describe('Palette (doctrine §6)', () => {
  const tokens = {};
  for (const [, name, value] of css.matchAll(/--([a-z-]+):\s*(#[0-9a-f]{6})/gi)) {
    tokens[name] = value;
  }

  const luminance = (hex) => {
    const channels = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16) / 255);
    const [r, g, b] = channels.map((c) =>
      c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4
    );
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
  };
  const contrast = (a, b) => {
    const [x, y] = [luminance(a), luminance(b)].sort((m, n) => n - m);
    return (x + 0.05) / (y + 0.05);
  };

  it('uses a warm charcoal and a warm mineral, never pure black or white', () => {
    assert.equal(tokens['substral-black'], '#11110f');
    assert.equal(tokens.mineral, '#f0ede5');
    assert.notEqual(tokens['substral-black'], '#000000');
    assert.notEqual(tokens.mineral, '#ffffff');
  });

  it('declares one accent family, in oxidized-copper territory', () => {
    assert.equal(tokens.patina, '#7fa890');
    assert.equal(tokens['patina-deep'], '#3d6b57');
    // Both tints share the same green hue: one family, not two brand colours.
    const hue = (hex) => {
      const [r, g, b] = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));
      return Math.round(
        (Math.atan2(Math.sqrt(3) * (g - b), 2 * r - g - b) * 180) / Math.PI
      );
    };
    assert.ok(
      Math.abs(hue(tokens.patina) - hue(tokens['patina-deep'])) < 18,
      'accent tints must belong to one hue family'
    );
  });

  it('clears WCAG AA for small text on every declared text pairing', () => {
    const pairings = [
      ['mineral', 'substral-black'],
      ['patina', 'substral-black'],
      ['patina', 'graphite'],
      ['substral-black', 'mineral'],
      ['patina-deep', 'mineral'],
    ];
    for (const [fg, bg] of pairings) {
      const ratio = contrast(tokens[fg], tokens[bg]);
      assert.ok(ratio >= 4.5, `${fg} on ${bg} is ${ratio.toFixed(2)}:1`);
    }
  });

  it('keeps the structural greys legible in both environments', () => {
    const dark = css.match(/\.env-dark\s*\{[\s\S]*?\}/)[0];
    const mineral = css.match(/\.env-mineral\s*\{[\s\S]*?\}/)[0];
    const structuralDark = dark.match(/--ink-structural:\s*(#[0-9a-f]{6})/i)[1];
    const structuralMineral = mineral.match(/--ink-structural:\s*(#[0-9a-f]{6})/i)[1];
    assert.ok(contrast(structuralDark, tokens['substral-black']) >= 4.5);
    assert.ok(contrast(structuralMineral, tokens.mineral) >= 4.5);
  });

  it('does not paint large areas in the accent', () => {
    // The accent marks measurement and state. Anywhere it is used as a fill,
    // the rule must also constrain the element to a small, deliberate size.
    for (const [, selector, body] of css.matchAll(/([^{}]+)\{([^}]*)\}/g)) {
      if (!/background(-color)?:\s*var\(--(accent|patina)/.test(body)) continue;
      const sizes = [...body.matchAll(/(?:width|height):\s*([\d.]+)(rem|px)/g)];
      assert.ok(
        sizes.length > 0,
        `accent fill with no size constraint: ${selector.trim()}`
      );
      for (const [, value, unit] of sizes) {
        const px = unit === 'rem' ? Number(value) * 16 : Number(value);
        assert.ok(
          px <= 16,
          `accent fill on ${selector.trim()} is ${px}px — too large for an accent`
        );
      }
    }
  });
});

describe('Typography (doctrine §7)', () => {
  it('uses exactly two voices: an editorial grotesk and a technical mono', () => {
    const families = [...css.matchAll(/@font-face[\s\S]*?font-family:\s*'([^']+)'/g)].map(
      (m) => m[1]
    );
    assert.deepEqual([...new Set(families)].sort(), [
      'Archivo Substral',
      'Plex Mono Substral',
    ]);
  });

  it('sets hero type at architectural scale', () => {
    const display = css.match(/--t-display:\s*([^;]+);/)[1];
    assert.match(display, /clamp\(/);
    assert.match(display, /1[01](\.\d+)?rem/, 'hero should reach a very large size');
  });

  it('reserves the mono voice for evidence and metadata, not prose', () => {
    // Prose blocks must not be set in mono.
    assert.doesNotMatch(css, /\.prose\s*\{[^}]*var\(--mono\)/);
    assert.match(css, /\.manifest dd\s*\{[^}]*var\(--mono\)/);
  });
});

describe('Progressive enhancement (doctrine §18)', () => {
  it('renders the dimensional composition without WebGL', () => {
    const stages = html.match(/class="strata"/g) || [];
    assert.equal(stages.length, 2, 'both stages need a no-WebGL composition');
    const plates = html.match(/class="plate"/g) || [];
    assert.equal(plates.length, 12, 'six plates per stage');
    assert.match(css, /\.strata__stack\s*\{[\s\S]*?transform-style:\s*preserve-3d/);
  });

  it('keeps every narrative label in the document, not in the canvas', () => {
    for (const name of ['Performance', 'Accessibility', 'Conversion', 'Search', 'Trust', 'Design']) {
      assert.ok(html.includes(`>${name}<`), `${name} must exist as document text`);
    }
  });

  it('loads three.js only on demand', () => {
    assert.doesNotMatch(html, /three(\.min)?\.js/);
    assert.doesNotMatch(html, /dimensional\.js/);
    assert.match(orchestration, /import\(\s*['"]\.\/dimensional\.js['"]\s*\)/);
  });

  it('gates the object on capability, motion preference and viewport', () => {
    assert.match(orchestration, /reduceMotion\.matches/);
    assert.match(orchestration, /getContext\('webgl2'\)/);
    assert.match(orchestration, /innerWidth\s*<\s*600/);
    assert.match(orchestration, /saveData/);
  });

  it('excludes the decorative canvas from the accessibility tree', () => {
    const canvases = [...html.matchAll(/<canvas[^>]*>/g)].map((m) => m[0]);
    assert.equal(canvases.length, 2);
    for (const canvas of canvases) {
      assert.match(canvas, /aria-hidden="true"/);
    }
    assert.match(html, /<div class="strata" aria-hidden="true">/);
  });
});

describe('Reduced motion (doctrine §19)', () => {
  it('declares a reduced-motion treatment in the stylesheet', () => {
    assert.match(css, /@media \(prefers-reduced-motion: reduce\)/);
  });

  it('presents the object already decomposed rather than animating depth', () => {
    const blocks = css
      .split('@media (prefers-reduced-motion: reduce)')
      .slice(1)
      .join('\n');
    assert.match(blocks, /\.plate\s*\{[^}]*translate3d/);
    assert.match(blocks, /\.stage__canvas\s*\{[^}]*display:\s*none/);
    assert.match(blocks, /scroll-behavior:\s*auto/);
  });

  it('checks the preference at runtime and reacts to changes', () => {
    assert.match(orchestration, /matchMedia\('\(prefers-reduced-motion: reduce\)'\)/);
    assert.match(orchestration, /reduceMotion\.addEventListener\('change'/);
  });

  it('still reveals content when motion is reduced', () => {
    assert.match(orchestration, /if \(reduceMotion\.matches\)[\s\S]{0,140}revealed = 'true'/);
  });
});

describe('Accessibility doctrine (doctrine §21)', () => {
  it('declares a language and a skip link', () => {
    assert.match(html, /<html lang="en"/);
    assert.match(html, /class="skip" href="#surface"/);
  });

  it('has exactly one h1', () => {
    assert.equal((html.match(/<h1[\s>]/g) || []).length, 1);
  });

  it('labels every form control', () => {
    const ids = [...html.matchAll(/<input[^>]*\sid="([^"]+)"/g)].map((m) => m[1]);
    assert.ok(ids.length >= 4);
    for (const id of ids) {
      assert.match(html, new RegExp(`<label[^>]*for="${id}"`), `no label for #${id}`);
    }
  });

  it('gives every image alternative text', () => {
    for (const tag of html.match(/<img[^>]*>/g) || []) {
      assert.match(tag, /\salt="[^"]+"/, `image without alt text: ${tag}`);
    }
  });

  it('keeps focus visible and only suppresses the outline for pointer focus', () => {
    assert.match(css, /:focus-visible\s*\{[^}]*outline:\s*2px solid/);
    for (const [, selector, body] of css.matchAll(/([^{}]+)\{([^}]*)\}/g)) {
      if (!/outline:\s*(none|0)\b/.test(body)) continue;
      assert.match(
        selector,
        /:not\(:focus-visible\)/,
        `${selector.trim()} removes the focus outline for keyboard users`
      );
    }
  });

  it('announces assessment results to assistive technology', () => {
    assert.match(html, /data-assessment-status[^>]*role="status"/);
    assert.match(html, /aria-live="polite"/);
  });

  it('marks up the narrative with landmarks and labelled sections', () => {
    assert.match(html, /<main id="main">/);
    assert.match(html, /<footer/);
    const sections = html.match(/<section class="act[^"]*"[^>]*>/g) || [];
    assert.equal(sections.length, 6);
    for (const section of sections) {
      assert.match(section, /aria-labelledby="/, `unlabelled section: ${section}`);
    }
  });
});

describe('Performance doctrine (doctrine §20)', () => {
  it('has no render-blocking script in the head', () => {
    assert.doesNotMatch(head, /<script[^>]+src=/);
  });

  it('self-hosts the fonts and preloads the critical path', () => {
    assert.doesNotMatch(html, /fonts\.googleapis\.com|fonts\.gstatic\.com/);
    assert.match(head, /rel="preload"[^>]*archivo-var-latin\.woff2[^>]*as="font"/);
    assert.match(head, /rel="preload"[^>]*substral\.css[^>]*as="style"/);
    assert.match(css, /font-display:\s*swap/);
  });

  it('loads no third-party origin on the critical path', () => {
    const origins = [...head.matchAll(/https?:\/\/([a-z0-9.-]+)/gi)]
      .map((m) => m[1].toLowerCase())
      .filter(
        (host) =>
          !host.endsWith('studiosubstral.com') &&
          host !== 'schema.org' &&
          host !== 'www.w3.org'
      );
    assert.deepEqual(origins, [], `third-party origins in head: ${origins}`);
  });

  it('keeps the eager script inside the budget the colophon publishes', () => {
    const budget = 10 * 1024;
    const gz = gzipSync(orchestration + assessmentJs, { level: 9 }).length;
    assert.ok(gz <= budget, `eager JS is ${(gz / 1024).toFixed(1)} KB gzip`);
    assert.match(copy, /Under 10 KB compressed/);
  });

  it('keeps the deferred dimensional bundle inside its budget', () => {
    const bundle = fs.readFileSync(
      path.join(SITE, 'assets', 'js', 'dimensional.js')
    );
    const gz = gzipSync(bundle, { level: 9 }).length;
    assert.ok(gz <= 170 * 1024, `dimensional bundle is ${(gz / 1024).toFixed(1)} KB gzip`);
  });

  it('reserves space for the only raster image, so it cannot shift layout', () => {
    const img = html.match(/<img[^>]*anchor-cleaning-home[^>]*>/)[0];
    assert.match(img, /width="\d+"/);
    assert.match(img, /height="\d+"/);
    assert.match(img, /loading="lazy"/);
  });

  it('keeps every image the page actually loads under 150 KB', () => {
    for (const [, src] of html.matchAll(/<img[^>]*\ssrc="([^"]+)"/g)) {
      const { size } = fs.statSync(path.join(SITE, src));
      assert.ok(
        size <= 150 * 1024,
        `${src} is ${(size / 1024).toFixed(0)} KB — re-encode it`
      );
    }
  });

  it('keeps the whole publish set small enough to justify the critique', () => {
    const weigh = (dir) =>
      fs.readdirSync(dir, { withFileTypes: true }).reduce((total, entry) => {
        const full = path.join(dir, entry.name);
        return total + (entry.isDirectory() ? weigh(full) : fs.statSync(full).size);
      }, 0);

    const assets = weigh(path.join(SITE, 'assets'));
    const page = fs.statSync(path.join(SITE, 'index.html')).size;
    const total = (assets + page) / 1024;
    assert.ok(total <= 1200, `publish set is ${total.toFixed(0)} KB`);
  });

  it('stops rendering when nothing is moving', () => {
    assert.match(orchestration, /if \(this\.visible && moving\) this\.request\(\)/);
    assert.match(dimensionalSrc, /function isMoving\(\)/);
  });
});

describe('Motion physics (doctrine §13)', () => {
  it('interpolates toward targets instead of playing keyframes', () => {
    assert.match(orchestration, /const approach = \(current, target, factor\)/);
    assert.doesNotMatch(css, /@keyframes/);
  });

  it('uses no elastic, bouncing or overshooting easing', () => {
    assert.doesNotMatch(css, /cubic-bezier\(\s*[^)]*,\s*-\d/);
    assert.doesNotMatch(css, /elastic|bounce|back(In|Out)/i);
  });

  it('keeps pointer response below obvious cause and effect', () => {
    const yaw = dimensionalSrc.match(/target\.yaw = pointer\.x \* ([\d.]+)/)[1];
    const pitch = dimensionalSrc.match(/target\.pitch = pointer\.y \* ([\d.]+)/)[1];
    assert.ok(Number(yaw) <= 0.08, `pointer yaw amplitude ${yaw} is too large`);
    assert.ok(Number(pitch) <= 0.05, `pointer pitch amplitude ${pitch} is too large`);
  });
});

describe('Responsive intent (doctrine §17)', () => {
  it('designs the mobile treatment rather than scaling the desktop one', () => {
    assert.match(css, /\.decomposition__stage\s*\{[\s\S]*?height:\s*44svh/);
    assert.match(css, /@media \(min-width: 62em\)/);
  });

  it('treats the CSS composition as the intended small-screen object', () => {
    assert.match(orchestration, /the CSS composition is the intended treatment/);
  });
});

describe('Relationship to PulseForge (doctrine §24)', () => {
  it('does not advertise the infrastructure behind the assessment', () => {
    assert.doesNotMatch(copy, /PulseForge/i);
    assert.doesNotMatch(html, /Pulseforge(?![-\w]*\.up\.railway\.app)/i);
  });

  it('keeps the intake endpoint out of the visible copy', () => {
    assert.doesNotMatch(copy, /railway\.app/i);
    assert.match(assessmentJs, /api\/public\/website-assessment/);
  });
});

describe('Discoverability the studio would demand of a client', () => {
  it('ships robots.txt and a sitemap that agree with the canonical URL', () => {
    const canonical = html.match(/<link rel="canonical" href="([^"]+)"/)[1];
    assert.match(read('robots.txt'), /Sitemap: https:\/\/studiosubstral\.com\/sitemap\.xml/);
    assert.match(read('sitemap.xml'), new RegExp(`<loc>${canonical}</loc>`));
  });

  it('describes itself for sharing and for structured data', () => {
    assert.match(head, /property="og:image"/);
    assert.match(head, /property="og:image:alt"/);
    assert.match(head, /"@type": "ProfessionalService"/);
    assert.ok(fs.existsSync(path.join(SITE, 'assets', 'brand', 'social-preview.png')));
  });
});
