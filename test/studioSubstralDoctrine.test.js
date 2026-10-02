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

const { PROHIBITED_CLAIM_PATTERNS } = require('../packages/capabilities/websiteOpportunityIntelligence/types');

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
    assert.match(copy, /Before you rebuild, find out what s broken\./i);
  });

  it('leads with proof before the service explanation', () => {
    const work = html.indexOf('id="work"');
    const fix = html.indexOf('id="fix"');
    assert.ok(work > -1 && fix > work, 'selected work must precede what we fix');
    assert.match(copy, /Selected work/i);
    assert.match(copy, /Anchor Cleaning/i);
    assert.match(copy, /Meridian \/ MCFO Services/i);
  });

  it('builds the commercial sequence from proof to scope, process and assessment', () => {
    const sequence = ['id="work"', 'id="fix"', 'id="deliverables"', 'id="process"', 'id="assessment"'];
    let cursor = -1;
    for (const marker of sequence) {
      const next = html.indexOf(marker, cursor + 1);
      assert.ok(next > cursor, `${marker} is missing or out of commercial sequence`);
      cursor = next;
    }

    assert.match(copy, /What we fix/i);
    assert.match(copy, /What you get/i);
    assert.match(copy, /How it works/i);
    assert.match(copy, /Website assessment/i);
    assert.match(copy, /Request a Website Assessment/i);
    assert.ok(
      html.indexOf('data-assessment-form') > html.indexOf('id="process"'),
      'the assessment form must remain the final commercial decision point'
    );
  });

  it('moves dark, to mineral, and back to dark', () => {
    const scopes = [...html.matchAll(/class="act ([a-z-]+) (env-dark|env-mineral)/g)].map(
      (m) => m[2]
    );
    assert.deepEqual(scopes, [
      'env-dark', // hero
      'env-mineral', // selected work
      'env-mineral', // what we fix
      'env-mineral', // what you get
      'env-mineral', // how it works
      'env-dark', // assessment
    ]);
  });
});

describe('The dimensional specimen (doctrine §12)', () => {
  it('keeps the six-layer stack in the hero object only', () => {
    const surface = html.slice(html.indexOf('id="surface"'), html.indexOf('id="work"'));
    const arts = [...surface.matchAll(/data-art="([a-z]+)"/g)].map((m) => m[1]).slice(0, 6);
    assert.deepEqual(arts, [
      'design',
      'trust',
      'search',
      'conversion',
      'accessibility',
      'performance',
    ]);
    assert.equal(surface.match(/class="plate"/g)?.length, 6);
    assert.equal(surface.match(/class="plinth"/g)?.length, 1);
  });
});

describe('Assessment integrity (doctrine §16)', () => {
  it('never presents a score', () => {
    assert.doesNotMatch(copy, /\b\d{1,3}\s*\/\s*100\b/);
    assert.doesNotMatch(copy, /\byour (website|site) score\b/i);
    assert.doesNotMatch(copy, /\bgrade\b\s*[:=]/i);
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

  it('uses no fabricated metrics or counters anywhere', () => {
    assert.doesNotMatch(copy, /\b\d+% (increase|more|faster|lift|growth|improvement)\b/i);
    assert.doesNotMatch(copy, /\b\d+x (more|faster|better)\b/i);
    assert.doesNotMatch(copy, /\b(happy clients|projects delivered|years of experience)\b/i);
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

  it('does not lean on rhetorical questions', () => {
    const questions = copy.match(/\?/g) || [];
    assert.ok(questions.length <= 4, `${questions.length} question marks in page copy`);
  });
});

describe('Forbidden visual patterns (doctrine §10)', () => {
  it('ships no neon, mesh or blob gradient decoration', () => {
    assert.doesNotMatch(css, /conic-gradient/);
    assert.doesNotMatch(css, /filter:\s*blur\(\s*[6-9]\d|filter:\s*blur\(\s*\d{3}/);
    assert.doesNotMatch(css, /#0ff|#f0f|#00ffff|#ff00ff/i);

    /* The layer drawings use radial gradients as hard-edged rings and nodes —
       a seal, a set of conversion nodes — which is the opposite of a glowing
       blob. What the doctrine forbids is the soft coloured wash, so only those
       are counted: everything outside the layer art and the substrate. */
    const decoration = css
      .replace(/\.plate\[data-art='[a-z]+'\][^{]*\{[^}]*\}/g, '')
      .replace(/\.plinth__face\s*\{[^}]*\}/g, '')
      // The turn is a cut section of the substrate: the same stone mottling.
      .replace(/\.turn(--back)?\s*\{[^}]*\}/g, '');
    const washes = decoration.match(/radial-gradient/g) || [];
    assert.ok(washes.length <= 2, `${washes.length} decorative radial washes`);

    // And no radial anywhere may be a saturated glow.
    for (const [, body] of css.matchAll(/radial-gradient\(([^;]*?)\)\s*[,;]/g)) {
      assert.doesNotMatch(body, /rgba?\(\s*(?:\d+\s*,\s*)?(?:2[0-5]\d|1[89]\d)\s*,\s*[0-4]\d?\s*,/);
    }
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
    /* What the doctrine forbids is the drop shadow used to lift things off the
       page. Inset shadows are the opposite: they are the material highlight
       along a machined edge, and the specimen is built out of them. */
    const drops = [...css.matchAll(/box-shadow:\s*([^;]+);/g)]
      .map((m) => m[1])
      // Colour functions contain commas, so flatten them before splitting the
      // shadow list on its own separators.
      .map((value) => value.replace(/rgba?\([^)]*\)/g, 'C'))
      .filter((value) => value.split(',').some((part) => !part.includes('inset')));
    assert.ok(drops.length <= 2, `${drops.length} outer drop shadows: ${drops}`);
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

  it('presents work as proof cards rather than a portfolio grid', () => {
    assert.match(html, /class="proof"/);
    assert.match(html, /class="proof__card"/);
    assert.doesNotMatch(html, /portfolio|case-study-grid/i);
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

  it('keeps the rejection tone legible in both environments', () => {
    // The instrument appears in the mineral act, so an error colour tuned only
    // for the dark environment is unreadable exactly where it is used.
    const scope = (name) => css.match(new RegExp(`\\.env-${name}\\s*\\{[\\s\\S]*?\\}`))[0];
    const pairs = [
      [scope('dark'), tokens['substral-black']],
      [scope('mineral'), tokens.mineral],
    ];
    for (const [block, background] of pairs) {
      const tone = block.match(/--tone-error:\s*(#[0-9a-f]{6})/i)?.[1];
      assert.ok(tone, 'each environment must define --tone-error');
      const ratio = contrast(tone, background);
      assert.ok(ratio >= 4.5, `${tone} on ${background} is ${ratio.toFixed(2)}:1`);
    }
    assert.match(css, /\[data-tone='error'\]\s*\{[^}]*var\(--tone-error\)/);
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
    const modes = [...html.matchAll(/data-stage="([a-z]+)"/g)].map((m) => m[1]);
    assert.deepEqual(modes, ['surface']);
    const stages = html.match(/class="strata"/g) || [];
    assert.equal(stages.length, 1, 'the hero carries the no-WebGL composition');
    const plates = html.match(/class="plate"/g) || [];
    assert.equal(plates.length, 6, 'six plates in the hero specimen');
    const plinths = html.match(/class="plinth"/g) || [];
    assert.equal(plinths.length, 1, 'the hero stands on the mineral substrate');
    assert.match(css, /\.strata__stack\s*\{[\s\S]*?transform-style:\s*preserve-3d/);
  });

  it('stacks the six systems over the substrate, design nearest the surface', () => {
    // The order is the doctrine's claim stated physically: performance is the
    // deepest layer and design is the visible surface, so what is underneath
    // really is underneath.
    const stack = [...html.matchAll(/data-art="([a-z]+)"/g)].map((m) => m[1]).slice(0, 6);
    assert.deepEqual(stack, [
      'design',
      'trust',
      'search',
      'conversion',
      'accessibility',
      'performance',
    ]);
    assert.deepEqual(
      [...dimensionalSrc.matchAll(/key: '([a-z]+)'/g)].map((m) => m[1]),
      stack,
      'the WebGL stack and the CSS stack must agree'
    );
  });

  it('gives every layer its own material and its own drawing', () => {
    /* Six near-identical glass panes at different spacings communicate the idea
       and none of the material. Each layer must differ across the non-colour
       axes: thickness, roughness, opacity, reflectivity and edge treatment. */
    const blocks = dimensionalSrc.split(/\n  \{\n/).slice(1, 7);
    const axes = {
      thickness: new Set(),
      roughness: new Set(),
      opacity: new Set(),
      envMapIntensity: new Set(),
      arris: new Set(),
      art: new Set(),
    };
    for (const block of blocks) {
      const body = block.slice(0, block.indexOf('\n  },'));
      for (const axis of Object.keys(axes)) {
        if (axis === 'art') {
          axes.art.add(body.match(/art: '([a-z]+)'/)?.[1]);
          continue;
        }
        const value = body.match(new RegExp(`\\b${axis}: ([\\d.]+)`))?.[1];
        assert.ok(value, `a layer is missing ${axis}`);
        axes[axis].add(value);
      }
    }
    assert.equal(axes.art.size, 6, 'each layer needs its own drawing');
    for (const axis of ['thickness', 'roughness', 'opacity', 'envMapIntensity', 'arris']) {
      assert.equal(axes[axis].size, 6, `all six layers must differ in ${axis}`);
    }

    // The thickness range has to be wide enough to see, not a rounding.
    const thicknesses = [...axes.thickness].map(Number).sort((a, b) => a - b);
    assert.ok(
      thicknesses.at(-1) / thicknesses[0] >= 2,
      `thickest layer is only ${(thicknesses.at(-1) / thicknesses[0]).toFixed(2)}x the thinnest`
    );

    // And each layer draws something different in the CSS baseline too.
    for (const key of axes.art) {
      assert.match(
        css,
        new RegExp(`\\.plate\\[data-art='${key}'\\] \\.plate__face::before`),
        `${key} has no drawing in the CSS composition`
      );
    }
  });

  it('separates the layers optically, through transmission and refraction', () => {
    /* Blended opacity can only make a layer darker or fainter. Transmission with
       volumetric absorption is what lets Search read genuinely clear, Conversion
       genuinely deep, and Accessibility scatter rather than merely dim. */
    const transmission = [...dimensionalSrc.matchAll(/\n    transmission: ([\d.]+),/g)].map(
      (m) => Number(m[1])
    );
    const absorption = [...dimensionalSrc.matchAll(/\n    attenuation: ([\d.]+),/g)].map(
      (m) => Number(m[1])
    );
    assert.equal(transmission.length, 6);
    assert.equal(absorption.length, 6);
    assert.ok(Math.max(...transmission) >= 0.85, 'no layer is genuinely clear');
    assert.ok(Math.min(...transmission) === 0, 'no layer is genuinely opaque');
    assert.ok(new Set(transmission).size >= 5, 'transmission barely varies');
    assert.match(dimensionalSrc, /capMaterial\.attenuationDistance = layer\.attenuation/);
    assert.match(dimensionalSrc, /capMaterial\.transmission = layer\.transmission/);

    // Refractive index varies too, so the layers bend light differently.
    const iors = [...dimensionalSrc.matchAll(/\n    ior: ([\d.]+),/g)].map((m) => Number(m[1]));
    assert.equal(iors.length, 6);
    assert.ok(new Set(iors).size >= 3, 'every layer refracts identically');
  });

  it('separates the layers by material rather than by hue', () => {
    /* The tint values are a value ladder, not a colour wheel: graphite darkest,
       frosted polymer lightest. They must survive grayscale, so the only hue
       departure allowed is the single warmth nudge on Trust. */
    const tints = [...dimensionalSrc.matchAll(/\n    tint: ([\d.]+),/g)].map((m) =>
      Number(m[1])
    );
    assert.equal(tints.length, 6);
    assert.equal(new Set(tints).size, 6, 'every layer needs its own value');
    assert.ok(
      Math.max(...tints) / Math.min(...tints) >= 8,
      'the value ladder is too compressed to read in grayscale'
    );
    const warmths = dimensionalSrc.match(/warmth:/g) || [];
    assert.ok(warmths.length <= 1, `${warmths.length} layers depart from the palette`);
  });

  it('models each named material, not six variants of one', () => {
    // Frosted polymer needs sheen and high roughness; graphite needs brushing;
    // etched glass needs its markings to drive roughness rather than colour.
    assert.match(dimensionalSrc, /key: 'accessibility'[\s\S]*?sheen: 0\.\d/);
    assert.match(dimensionalSrc, /key: 'performance'[\s\S]*?anisotropy: 0\.\d/);
    assert.match(dimensionalSrc, /key: 'search'[\s\S]*?etched: true/);
    assert.match(dimensionalSrc, /capMaterial\.roughnessMap = artTexture/);
    assert.match(dimensionalSrc, /capMaterial\.anisotropy =/);
    assert.match(dimensionalSrc, /capMaterial\.sheen =/);

    /* Graphite is not glass. Performance has to be the thickest layer and the
       only fully opaque one, and the frosted polymer has to be the pale one:
       those two ends are what give the value ladder somewhere to run between. */
    const layers = [...dimensionalSrc.matchAll(/key: '([a-z]+)',\n {4}narrative[\s\S]*?\n {2}\},/g)]
      .map((m) => ({
        key: m[1],
        thickness: Number(m[0].match(/\bthickness: ([\d.]+)/)[1]),
        tint: Number(m[0].match(/\btint: ([\d.]+)/)[1]),
        transmission: Number(m[0].match(/\btransmission: ([\d.]+)/)[1]),
        roughness: Number(m[0].match(/\broughness: ([\d.]+)/)[1]),
        edge: Number(m[0].match(/\bedge: ([\d.]+)/)[1]),
      }));
    assert.equal(layers.length, 6);
    const thickest = layers.reduce((a, b) => (b.thickness > a.thickness ? b : a));
    assert.equal(thickest.key, 'performance', 'graphite composite must be the thickest layer');
    const opaque = layers.filter((l) => l.transmission === 0).map((l) => l.key);
    assert.deepEqual(opaque, ['performance'], 'graphite composite must be the only opaque layer');
    const palest = layers.reduce((a, b) => (b.tint > a.tint ? b : a));
    assert.equal(palest.key, 'accessibility', 'frosted polymer must be the pale material');

    /* Frosted polymer has to be matte. Asserting a literal figure here read as a
       guard and was not one: the pattern searched forward from the layer's key, so
       once its roughness moved it matched the next layer's instead and passed
       without checking anything. */
    const accessibility = layers.find((l) => l.key === 'accessibility');
    assert.ok(
      accessibility.roughness >= 0.6,
      `frosted polymer at roughness ${accessibility.roughness} is not matte`
    );
    const polished = layers.filter((l) => l.roughness <= 0.05).map((l) => l.key);
    assert.deepEqual(
      polished.sort(),
      ['conversion', 'design'],
      'the precision surface and the smoked acrylic are the polished pair'
    );

    /* Edge treatment is specified per layer rather than emerging from whatever
       the rasteriser does with a degenerate sliver, so it has to be a stated width
       and the widths have to differ. */
    assert.match(dimensionalSrc, /function arrisGeometry\(shape, thickness, width\)/);
    assert.match(dimensionalSrc, /arrisGeometry\(shape, layer\.thickness, layer\.edge\)/);
    assert.equal(new Set(layers.map((l) => l.edge)).size, 6, 'all six edges are the same weight');
    const edges = layers.map((l) => l.edge).sort((a, b) => a - b);
    assert.ok(
      edges.at(-1) / edges[0] >= 2,
      `the widest machined edge is only ${(edges.at(-1) / edges[0]).toFixed(1)}x the finest`
    );
  });

  it('separates the layers far enough apart to see', () => {
    /* Two passes were told the layers still read as six of the same pane. The
       reason the second pass did not fix it is that five of the six use
       transmission, and a transmissive material ignores blended opacity — so the
       axis the values varied along was inert. What actually decides how a
       transmissive layer reads is the absorption it applies over the path through
       it, and that has to vary by a lot, not by a rounding. */
    const absorption = [...dimensionalSrc.matchAll(/\n {4}volume: ([\d.]+),[\s\S]{0,60}?\n {4}attenuation: ([\d.]+),/g)]
      .map(([, volume, attenuation]) => Number(volume) / Number(attenuation));
    assert.equal(absorption.length, 6, 'every layer needs a stated optical path');
    const transmissive = absorption.filter((a) => a > 0).sort((a, b) => a - b);
    assert.ok(
      transmissive.at(-1) / transmissive[0] >= 50,
      `the darkest optical path is only ${(transmissive.at(-1) / transmissive[0]).toFixed(1)}x ` +
        'the clearest — the glass layers will look alike'
    );

    // And they must bend light differently, not merely absorb it differently.
    const iors = [...dimensionalSrc.matchAll(/\n {4}ior: ([\d.]+),/g)].map((m) => Number(m[1]));
    assert.ok(
      Math.max(...iors) - Math.min(...iors) >= 0.12,
      'the refractive indices are too close to distinguish the materials'
    );

    /* Markings sit in two different physical relationships to their material:
       lit through the dark layers, drawn into the pale one. */
    assert.match(dimensionalSrc, /blending: layer\.artInk \? NormalBlending : AdditiveBlending/);
    assert.match(dimensionalSrc, /key: 'accessibility'[\s\S]*?artInk: true/);
  });

  it('builds the substrate as stone, not as an extruded plate', () => {
    /* This is the distinction the whole object turns on. An extruded polygon has
       a constant thickness and vertical walls however it is textured, and the eye
       reads that as a manufactured panel. So the block is displaced geometry cut
       by fracture planes, flat-shaded so each facet answers light on its own. */
    assert.doesNotMatch(dimensionalSrc, /hewnShape/);
    assert.match(dimensionalSrc, /new IcosahedronGeometry\(/);
    assert.match(dimensionalSrc, /const CLEAVAGE = /);
    assert.match(dimensionalSrc, /flatShading: true/);
    assert.match(dimensionalSrc, /function fbm3\(/);
    assert.match(dimensionalSrc, /function stoneNormalTexture\(/);
    assert.match(dimensionalSrc, /vertexColors: true/);

    // The substrate is the only thing in the scene that is not extruded.
    const extrusions = dimensionalSrc.match(/new ExtrudeGeometry\(/g) || [];
    assert.equal(extrusions.length, 1, 'only the plates are extruded');

    /* Its thickness must vary across its extent — a constant-thickness solid is
       a slab, and a slab is a plate. */
    assert.match(dimensionalSrc, /Thickness varies independently/);
    assert.match(dimensionalSrc, /y \*= 0\.\d+ \+ \(fbm3\(/);

    // Enough fracture planes to read as quarried rather than as a lump.
    const sides = Number(dimensionalSrc.match(/const sides = (\d+);/)[1]);
    assert.ok(sides >= 10, `${sides} side fracture planes is too few`);
  });

  it('breaks the stone rather than only displacing it', () => {
    /* A displaced sphere has curvature everywhere, and curvature everywhere is
       what the eye calls a lump — or, once it has been cut flat top and bottom,
       a polygonal plate. That was the note on the pass before this one. Stone is
       flat in patches and sharp between them, so the surface is passed through a
       fracture step that snaps neighbouring points onto shared planes, and the
       displacement field itself is built from crack distances rather than from
       smooth noise. */
    assert.match(dimensionalSrc, /function shatter\(/);
    assert.match(dimensionalSrc, /function crack3\(/);
    assert.match(dimensionalSrc, /const clipped = shatter\(points, o,/);
    assert.match(dimensionalSrc, /const seam = \(1 - crack3\(/);

    /* Facets have to be small enough relative to the block to read as broken
       stone. three's polyhedron subdivides each of twenty faces into
       (detail + 1)² triangles, so the detail figure is the whole story: at 4 the
       block was five hundred triangles and read as low-poly. */
    const detail = Number(dimensionalSrc.match(/new IcosahedronGeometry\(1, (\d+)\)/)[1]);
    assert.ok(detail >= 18, `icosahedron detail ${detail} gives facets too large for stone`);

    /* Occlusion in the fracture network is most of why the reference reads as
       stone: its cracks are nearly black while its broken high points take the
       light. A 1024px shadow map cannot resolve that, so it is measured while the
       surface is displaced and baked into the vertices. */
    assert.match(dimensionalSrc, /const recess = new Float32Array\(count\)/);
    assert.match(dimensionalSrc, /const shade = lerp\(1, 0\.\d+, recess\[/);

    /* Geometry stops carrying structure at about twice its facet size. Below
       that, the grain map has to take over — at a scale that reads as a broken
       surface rather than as a sheen, which is what a map repeating every quarter
       of a facet gave. */
    const grainScale = Number(dimensionalSrc.match(/\n {6}const scale = ([\d.]+);/)[1]);
    assert.ok(grainScale <= 1.6, `grain repeats every ${(1 / grainScale).toFixed(2)} units: too fine`);
    const normalScale = Number(
      dimensionalSrc.match(/stoneMaterial\.normalScale\.set\(([\d.]+)/)[1]
    );
    assert.ok(normalScale >= 1.8, `normal relief of ${normalScale} is too shallow to read`);

    /* Normals, albedo and roughness all come off the same height field, so what
       the surface says is broken, what it says is dark and what it says is matte
       agree. A normal map on its own is only convincing under moving light. */
    for (const map of ['stoneAlbedoTexture', 'stoneNormalTexture', 'stoneRoughnessTexture']) {
      assert.match(dimensionalSrc, new RegExp(`function ${map}\\(`));
      assert.match(dimensionalSrc, new RegExp(`${map}\\(\\),`));
    }
    assert.match(dimensionalSrc, /function stoneGrain\(\)/);
  });

  it('gives the block a silhouette rather than an outline', () => {
    /* Cleavage and fracture work on the surface, and neither can stop the outline
       converging on an ellipsoid, because both trim every direction to roughly the
       same radius. That is what "credible but a little too polite" was about. Three
       things at a larger scale fix it, and each does something the others cannot:
       spurs run the block further in a few directions, a keel takes the underside
       down to where it parted, and spherical bites are the only primitive here that
       produces a genuinely concave face — displacement and clipping can only give a
       surface that curves outward or is flat. */
    assert.match(dimensionalSrc, /const SPURS = /);
    assert.match(dimensionalSrc, /const GOUGES = /);
    assert.match(dimensionalSrc, /for \(const s of SPURS\)/);
    assert.match(dimensionalSrc, /for \(const g of GOUGES\)/);
    assert.match(dimensionalSrc, /The keel\./);

    /* A limb goes on after the quarrying. A cleavage plane caps the radius in its
       direction, so a spur folded into the displacement is clipped straight back
       off — which is what being trimmed looks like. */
    const spurAt = dimensionalSrc.indexOf('if (spur > 0)');
    const cleavageAt = dimensionalSrc.indexOf('for (const plane of CLEAVAGE)');
    assert.ok(spurAt > cleavageAt, 'the cleavage planes will clip the spurs back off');

    /* Few, and unevenly weighted. A ring of equal protrusions is a cog, and an
       even scatter of equal bites is a golf ball. */
    const table = (name) =>
      dimensionalSrc.slice(
        dimensionalSrc.indexOf(`const ${name} = `),
        dimensionalSrc.indexOf('];', dimensionalSrc.indexOf(`const ${name} = `))
      );
    const reaches = [...table('SPURS').matchAll(/reach: ([\d.]+)/g)].map((m) => Number(m[1]));
    assert.ok(reaches.length >= 3 && reaches.length <= 6, `${reaches.length} spurs`);
    assert.equal(new Set(reaches).size, reaches.length, 'the spurs are all the same reach');
    assert.ok(
      Math.max(...reaches) / Math.min(...reaches) >= 2,
      'the spurs are too evenly weighted to read as a break'
    );
    const gouges = table('GOUGES');
    const radii = [...gouges.matchAll(/radius: ([\d.]+)/g)].map((m) => Number(m[1]));
    const depths = [...gouges.matchAll(/depth: ([\d.]+)/g)].map((m) => Number(m[1]));
    assert.ok(radii.length >= 3 && radii.length <= 8, `${radii.length} gouges`);
    assert.equal(new Set(radii).size, radii.length, 'the gouges are all the same size');
    assert.equal(depths.length, radii.length, 'every gouge needs a stated depth');
    assert.ok(Math.max(...depths) <= 0.45, 'a gouge that deep would cut the block in half');

    /* Carved along the ray from the block's centre. Pushing points away from the
       sphere's centre instead moves everything on its far side outward, so the
       spheres inflate the block rather than subtracting from it. */
    assert.match(dimensionalSrc, /const near = toward - Math\.sqrt\(discriminant\)/);
    assert.match(dimensionalSrc, /if \(near > 0 && near < limit\) limit = near;/);

    // Only the top is bedded: a plane under it flattens the break into a cut.
    const bedding = dimensionalSrc.match(/planes\.push\(\{ n: \[-?[\d.]/g) || [];
    assert.equal(bedding.length, 1, 'the underside must not be planed off');
  });

  it('stains a few fractures without lighting any of them', () => {
    /* The reference carries warmth inside its cracks. Reproduced literally that is
       glowing lava, which §10 rules out, so it is oxidised mineral instead: albedo,
       not emission — a dark warm ochre the key light happens to find. Gated on a
       low-frequency field as well as on crevice depth, so it appears in some
       fractures rather than along all of them. */
    assert.match(dimensionalSrc, /const oxide =/);
    assert.match(dimensionalSrc, /smoothstep\([\d.]+, [\d.]+, recess\[base \+ v\]\)/);

    // Albedo only. Nothing about the stone may emit, and nothing may bloom.
    const stone = dimensionalSrc.slice(dimensionalSrc.indexOf('const stoneMaterial'));
    assert.doesNotMatch(stone.slice(0, 400), /emissive/);
    assert.doesNotMatch(dimensionalSrc, /UnrealBloom|BloomPass|toneMappingExposure = [2-9]/);

    /* And the stain has to stay a stain. It lifts a crevice, and a crevice is
       already the darkest thing on the block, so the lift must not be large enough
       to carry it past ordinary lit stone. */
    const lift = Number(dimensionalSrc.match(/value \* \(1 \+ oxide \* ([\d.]+)\)/)[1]);
    const floor = Number(dimensionalSrc.match(/const shade = lerp\(1, ([\d.]+), recess/)[1]);
    assert.ok(
      floor * (1 + lift) < 0.5,
      `a stained crevice reaches ${(floor * (1 + lift)).toFixed(2)} of lit stone: that is a glow`
    );
  });

  it('works only the part of the stone the stack sits on', () => {
    /* The brief allows a planed region where the engineered system interfaces
       with the block, and requires that natural stone stay dominant. The previous
       pass planed a plateau out to seven tenths of the radius, which is most of
       the top and is how it turned back into a plate. The worked region is now
       bounded by the stack's own footprint — it is a patch cut off a high point,
       not a terrace across the middle. */
    assert.doesNotMatch(dimensionalSrc, /const plateau = halfT \*/);
    const seatX = Number(dimensionalSrc.match(/const seatX = PLATE_W \* ([\d.]+);/)[1]);
    const seatZ = Number(dimensionalSrc.match(/const seatZ = PLATE_H \* ([\d.]+);/)[1]);
    const blockW = Number(dimensionalSrc.match(/const SUBSTRATE_W = PLATE_W \* ([\d.]+);/)[1]);
    const blockD = Number(dimensionalSrc.match(/const SUBSTRATE_D = PLATE_H \* ([\d.]+);/)[1]);
    const worked = ((2 * seatX) / blockW) * ((2 * seatZ) / blockD);
    assert.ok(worked <= 0.35, `the planed region covers ${Math.round(worked * 100)}% of the top`);

    // And it is offset, because a machined patch centred on the block reads as a
    // feature of the design rather than as a cut made for a reason.
    assert.match(dimensionalSrc, /const seatOffsetX = PLATE_W \* 0\.\d+;/);
    assert.match(dimensionalSrc, /const seatOffsetZ = -PLATE_H \* 0\.\d+;/);
  });

  it('gives the substrate mass, and the layers none by comparison', () => {
    const width = Number(dimensionalSrc.match(/const SUBSTRATE_W = PLATE_W \* ([\d.]+);/)[1]);
    const thickness = Number(dimensionalSrc.match(/const SUBSTRATE_T = ([\d.]+);/)[1]);
    assert.ok(width >= 1.6, `substrate is only ${width}x the plate width`);

    // It must dwarf the thickest engineered layer, or it is just another plate.
    const plate = Math.max(
      ...[...dimensionalSrc.matchAll(/\n    thickness: ([\d.]+),/g)].map((m) => Number(m[1]))
    );
    assert.ok(
      thickness / plate >= 8,
      `substrate is only ${(thickness / plate).toFixed(1)}x the thickest layer`
    );
  });

  it('self-shadows the stone, and only the stone', () => {
    // Deep shadow in the crevices is most of what makes it read as rock. The
    // translucent plates stay out of it: opaque shadows from glass look wrong.
    assert.match(dimensionalSrc, /renderer\.shadowMap\.enabled = true/);
    assert.match(dimensionalSrc, /key\.castShadow = true/);
    assert.match(dimensionalSrc, /substrate\.castShadow = true/);
    assert.match(dimensionalSrc, /substrate\.receiveShadow = true/);
    assert.doesNotMatch(dimensionalSrc, /face\.castShadow|plate\.castShadow/);
  });

  it('models the material the doctrine asked for', () => {
    // Smoked acrylic caps, machined metal walls, environmental shadow and
    // depth falloff — not a translucent rectangle (doctrine §11).
    assert.match(dimensionalSrc, /ior: layer\.ior/);
    assert.match(dimensionalSrc, /clearcoat:/);
    assert.match(dimensionalSrc, /wall: \{ colour:/);
    assert.match(dimensionalSrc, /\[capMaterial, wallMaterial\]/);
    assert.match(dimensionalSrc, /contactShadow/);
    assert.match(dimensionalSrc, /new Fog\(/);
    assert.match(dimensionalSrc, /function arrisGeometry/);
  });

  it('makes the layer under discussion the subject through light, not paint', () => {
    /* The brief is explicit: do not do this by increasing opacity or changing
       colour. So a raking light crosses the subject, its reflectivity rises,
       the camera reframes — and the emissive glow that used to tint the whole
       plate is gone. */
    assert.match(dimensionalSrc, /const graze = new DirectionalLight/);
    assert.match(dimensionalSrc, /graze\.intensity = state\.focus/);
    assert.match(dimensionalSrc, /grazeTarget\.position\.set/);
    assert.match(dimensionalSrc, /examine\.intensity = state\.focus/);
    assert.match(dimensionalSrc, /capMaterial\.envMapIntensity = lerp\(/);
    assert.match(dimensionalSrc, /target\.lookAt = examining/);
    assert.match(dimensionalSrc, /examinePitch/);

    // The active layer must not be made more opaque than it already is.
    const opacityLine = dimensionalSrc.match(/plate\.capMaterial\.opacity = [^;]+;/)[0];
    assert.doesNotMatch(opacityLine, /\+ 0\.\d/, 'emphasis must not add opacity');

    /* And the stated coverage has to be the coverage. Setting it to a constant
       anywhere means the figure in the layer table is decorative: it was, for the
       five transmissive layers, and the animation loop wrote the real value back
       every frame from a different place. */
    assert.doesNotMatch(dimensionalSrc, /capMaterial\.opacity = 1;/);

    // And it must not be recoloured: the accent survives only as an edge trace.
    assert.match(dimensionalSrc, /emissiveIntensity: 0,/);
    assert.doesNotMatch(dimensionalSrc, /emissiveIntensity = emphasis/);
    const patinaUse = dimensionalSrc.match(/lerp\(patina, emphasis \* ([\d.]+)\)/);
    assert.ok(patinaUse && Number(patinaUse[1]) <= 0.35, 'accent tint is doing too much work');
  });

  it('names the six systems on the hero specimen, not inside the canvas', () => {
    const surface = html.slice(html.indexOf('id="surface"'), html.indexOf('id="work"'));
    for (const key of [
      'performance',
      'accessibility',
      'conversion',
      'search',
      'trust',
      'design',
    ]) {
      assert.match(surface, new RegExp(`data-art="${key}"`));
    }
  });

  it('loads three.js only on demand', () => {
    assert.doesNotMatch(html, /three(\.min)?\.js/);
    assert.doesNotMatch(html, /dimensional\.js/);
    assert.match(orchestration, /import\(\s*['"]\.\/dimensional\.js['"]\s*\)/);
  });

  it('keeps the approved renderer available at every launch viewport and honours resource preferences', () => {
    const gate = orchestration.slice(
      orchestration.indexOf('function shouldRenderObject()'),
      orchestration.indexOf('function loadObject(')
    );
    const eligible = new Function('window', 'navigator', 'reduceMotion', 'webglAvailable',
      `${gate}; return shouldRenderObject();`);
    const check = (width, options = {}) => eligible(
      { innerWidth: width },
      { connection: { saveData: options.saveData ?? false }, deviceMemory: options.memory ?? 4 },
      { matches: options.reduced ?? false },
      () => options.webgl ?? true
    );
    for (const width of [390, 768, 1280, 1600]) {
      assert.equal(check(width), true, `capable ${width}px devices receive the dimensional object`);
      assert.equal(check(width, { reduced: true }), false);
      assert.equal(check(width, { saveData: true }), false);
      assert.equal(check(width, { memory: 1 }), false);
      assert.equal(check(width, { webgl: false }), false);
    }
  });

  it('constructs dimensional stages individually instead of compiling all three together', () => {
    const loader = orchestration.slice(
      orchestration.indexOf('function loadObject('),
      orchestration.indexOf('/* --------------------------------------------------------------------------\n   Boot')
    );
    assert.match(loader, /start = \(stage\)/);
    assert.match(loader, /createDimensionalObject\(stage\.canvas/);
    assert.match(loader, /observer\.unobserve/);
    assert.doesNotMatch(loader, /for \(const stage of stages\)[\s\S]*createDimensionalObject/);
  });

  it('excludes the decorative canvas from the accessibility tree', () => {
    const canvases = [...html.matchAll(/<canvas[^>]*>/g)].map((m) => m[0]);
    assert.equal(canvases.length, 1);
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
    assert.match(css, /\.surface__object\s*\{[\s\S]*?min-height:/);
    assert.match(css, /@media \(min-width: 62em\)/);
  });

  it('keeps the fixed nav opaque without waiting for an observer', () => {
    /* The bar was transparent until an IntersectionObserver marked it lifted,
       and under render load that callback arrived late enough for display type
       to scroll straight through it. Opacity is now unconditional; only the
       hairline rule depends on the observer. */
    const base = css.match(/^\.nav \{[^}]*\}/m)[0];
    assert.match(base, /background:\s*var\(--bg\)/);
    const lifted = css.match(/\.nav\[data-lifted='true'\]\s*\{[^}]*\}/)[0];
    assert.doesNotMatch(lifted, /background/);
    assert.match(lifted, /border-bottom-color/);
  });

  it('reserves the fixed nav height on the hero stage', () => {
    assert.match(css, /--nav-h:/);
    assert.match(css, /\.surface\s*\{[\s\S]*?padding-top:\s*var\(--nav-h\)/);
  });

  it('keeps the CSS composition as the small-screen fallback without replacing the approved renderer', () => {
    const gate = orchestration.slice(
      orchestration.indexOf('function shouldRenderObject()'),
      orchestration.indexOf('function loadObject(')
    );
    assert.doesNotMatch(gate, /innerWidth/);
  });
});

describe('Relationship to PulseForge (doctrine §24)', () => {
  it('does not advertise the infrastructure behind the assessment', () => {
    assert.doesNotMatch(copy, /PulseForge/i);
    assert.doesNotMatch(html, /Pulseforge(?![-\w]*\.up\.railway\.app)/i);
  });

  it('keeps the intake endpoint out of the visible copy', () => {
    assert.doesNotMatch(copy, /railway\.app/i);
    assert.match(html, /action="https:\/\/pulseforge-leadgen-production.up.railway.app\/api\/public\/website-assessment"/);
    assert.match(assessmentJs, /fetch\(form.action/);
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
