/* ==========================================================================
   Studio Substral — the dimensional object.

   A website rendered as an engineered specimen: a surface plate that reads as
   a designed page, the six systems beneath it that decide whether it works,
   and a mineral substrate the whole thing is cut from. Not a laptop, not a
   screenshot in space, not a stack of UI cards (doctrine §11).

   Read top to bottom, the specimen is:

   10|     SURFACE         the visible website — nav, headline, media, CTA
       06 DESIGN          composition system: column and baseline grid
       05 TRUST           embedded marks, seals and credentials
       04 SEARCH          index and hierarchy — information architecture
       03 CONVERSION      pathways and nodes, one of which goes nowhere
       02 ACCESSIBILITY   semantic structure and focus order, frosted polymer
       01 PERFORMANCE     measurement traces in graphite composite, thickest
       ---------------    mineral foundation

   Performance is deepest and design is nearest the surface, which is the
   20|   doctrine's claim stated physically: what is underneath determines what
   happens above it.

   This module is the enhancement layer. It is imported dynamically and only
   when WebGL is present, motion is permitted and the viewport can justify it.
   If it never loads, the CSS composition in the document is the object.

   Built with esbuild into assets/js/dimensional.js — see build/build.mjs.
   ========================================================================== */

import {
  ACESFilmicToneMapping,
  AdditiveBlending,
  BufferAttribute,
  BufferGeometry,
  CanvasTexture,
  Color,
  DirectionalLight,
  DoubleSide,
  EquirectangularReflectionMapping,
  ExtrudeGeometry,
  Fog,
  Group,
  LinearFilter,
  LinearMipmapLinearFilter,
  Mesh,
  MeshBasicMaterial,
  MeshPhysicalMaterial,
  Object3D,
  PMREMGenerator,
  PerspectiveCamera,
  PlaneGeometry,
  PointLight,
  RepeatWrapping,
  Scene,
  Shape,
  SRGBColorSpace,
  Vector3,
  WebGLRenderer,
} from 'three';

/* Palette, matching the stylesheet exactly. */
const SUBSTRAL_BLACK = 0x11110f;
const MINERAL = 0xf0ede5;
const PATINA = 0x7fa890;
const STONE = 0x5c5649;

const PLATE_W = 3.05;
const PLATE_H = 2.25;
const CORNER = 0.05; // A machined relief, not a rounded-rectangle style choice.

/** Air between plates: none when assembled, 0.52 when fully apart. */
const AIR_ASSEMBLED = 0.004;
const AIR_SEPARATED = 0.52;
/** Act I only opens the seams far enough to suggest the object comes apart. */
const AIR_SURFACE_HINT = 0.1;

/* The substrate is a block, not a plate: a quarter of the object's width thick,
   wider than the layers it carries, and irregular in plan. */
const FOUNDATION_T = 0.86;
const FOUNDATION_SCALE = 1.32;
/** Air between the substrate and the deepest layer. */
const FOUNDATION_CLEARANCE = 0.1;
/** The planed pad where the engineered system seats into the stone. */
const SEAT_T = 0.026;
const SEAT_SCALE = 1.08;

const lerp = (a, b, t) => a + (b - a) * t;
const clamp = (n, min = 0, max = 1) => (n < min ? min : n > max ? max : n);

/* --------------------------------------------------------------------------
   The stack, top to bottom.

   Every layer differs in thickness, tint, roughness, clearcoat and rim alloy,
   because six identical slabs at different spacings communicate the idea and
   none of the material. `narrative` is the index the document uses
   (0 Performance … 5 Design); the surface plate has none, since it is the
   website the six explain rather than a seventh system.
   -------------------------------------------------------------------------- */

/* --------------------------------------------------------------------------
   The stack, top to bottom.

   Six layers, six materials. The differentiation is deliberately NOT six
   colours: it comes from roughness, opacity, thickness, edge treatment,
   internal markings, reflectivity and how each one answers light. In grayscale
   they still separate, because tint here is a value ladder — graphite darkest,
   frosted polymer lightest — not a hue wheel.

   `narrative` is the index the document uses (0 Performance … 5 Design).
   -------------------------------------------------------------------------- */

const STACK = [
  {
    /* 06 DESIGN — precision surface. The most finished layer: laminated, near
       mirror-polished, carrying the recognisable page composition on its face
       with the grid it sits on traced faintly beneath. */
    key: 'design',
    narrative: 5,
    thickness: 0.05,
    tint: 0.21,
    opacity: 0.56,
    roughness: 0.045,
    clearcoat: 1,
    clearcoatRoughness: 0.03,
    metalness: 0.02,
    envMapIntensity: 2.6,
    wall: { colour: 0xe6e2d6, roughness: 0.13, metalness: 0.97 },
    arris: 0.66,
    art: 'design',
    artOnTop: true,
    artOpacity: 0.68,
    artResolution: 1024,
  },
  {
    /* 05 TRUST — warm smoked glass. Substantial and refined rather than
       technical: the thickest of the glass layers, a touch warmer in its
       response, carrying discrete credibility marks instead of fine data. */
    key: 'trust',
    narrative: 4,
    thickness: 0.104,
    tint: 0.145,
    warmth: 0.55,
    opacity: 0.44,
    roughness: 0.1,
    clearcoat: 0.92,
    clearcoatRoughness: 0.075,
    metalness: 0.09,
    envMapIntensity: 1.95,
    wall: { colour: 0xd2c8b2, roughness: 0.27, metalness: 0.9 },
    arris: 0.6,
    art: 'trust',
    artOpacity: 0.58,
  },
  {
    /* 04 SEARCH — etched architectural glass. The body is the clearest and
       smoothest in the stack; the index markings are cut into it, so the art
       drives roughness and the marks only appear when light grazes them. */
    key: 'search',
    narrative: 3,
    thickness: 0.044,
    tint: 0.08,
    opacity: 0.22,
    roughness: 0.92,
    etched: true,
    clearcoat: 0.8,
    clearcoatRoughness: 0.05,
    metalness: 0.04,
    envMapIntensity: 2.35,
    wall: { colour: 0xbdb8a6, roughness: 0.17, metalness: 0.93 },
    arris: 0.34,
    art: 'search',
    artOpacity: 0.44,
  },
  {
    /* 03 CONVERSION — smoked acrylic. Dark with real optical depth, polished
       edges, controlled internal reflection. Sparse bright nodes. */
    key: 'conversion',
    narrative: 2,
    thickness: 0.078,
    tint: 0.048,
    opacity: 0.62,
    roughness: 0.11,
    clearcoat: 1,
    clearcoatRoughness: 0.09,
    metalness: 0.06,
    envMapIntensity: 1.45,
    wall: { colour: 0xa09a8b, roughness: 0.15, metalness: 0.89 },
    arris: 0.44,
    art: 'conversion',
    artOpacity: 0.52,
  },
  {
    /* 02 ACCESSIBILITY — frosted polymer. Milky rather than dark: the lightest
       material in the stack, high roughness, sheen for the soft diffuse halo,
       almost no clearcoat and a soft edge. Reads as frosted, not as glass at
       low opacity. */
    key: 'accessibility',
    narrative: 1,
    thickness: 0.09,
    tint: 0.38,
    opacity: 0.6,
    roughness: 0.64,
    clearcoat: 0.1,
    clearcoatRoughness: 0.62,
    metalness: 0,
    envMapIntensity: 0.8,
    sheen: 0.75,
    sheenRoughness: 0.85,
    wall: { colour: 0x9d978a, roughness: 0.74, metalness: 0.22 },
    arris: 0.2,
    art: 'accessibility',
    artOpacity: 0.3,
  },
  {
    /* 01 PERFORMANCE — graphite composite. The deepest, darkest, densest and
       least transparent layer: brushed along one axis so it reads as an
       engineered conductive material, with fine measurement traces cut in. */
    key: 'performance',
    narrative: 0,
    thickness: 0.108,
    tint: 0.014,
    opacity: 0.7,
    roughness: 0.52,
    etched: true,
    anisotropy: 0.85,
    clearcoat: 0.3,
    clearcoatRoughness: 0.4,
    metalness: 0.44,
    envMapIntensity: 0.7,
    wall: { colour: 0x726c61, roughness: 0.56, metalness: 0.72 },
    arris: 0.3,
    art: 'performance',
    artOpacity: 0.46,
  },
];

const PLATES = STACK.length;
/** Stack position, counted from the substrate up. */
const stackPosition = (index) => PLATES - 1 - index;
/** Document layer index (0 Performance … 5 Design) to array index. */
const NARRATIVE_TO_INDEX = new Map(
  STACK.filter((l) => l.narrative != null).map((l) => [l.narrative, STACK.indexOf(l)])
);

/** Height of the plates below stack position s, ignoring air. */
function solidBelow(position) {
  let total = 0;
  for (let p = 0; p < position; p += 1) {
    total += STACK[PLATES - 1 - p].thickness;
  }
  return total;
}

const TOTAL_SOLID = solidBelow(PLATES);

/** Underside of a plate, measured from the top of the substrate. */
const plateBase = (index, air) => solidBelow(stackPosition(index)) + stackPosition(index) * air;

/* --------------------------------------------------------------------------
   Plate silhouette and its machined rim.
   -------------------------------------------------------------------------- */

function plateShape(w, h, r) {
  const x = w / 2;
  const y = h / 2;
  const shape = new Shape();
  shape.moveTo(-x + r, -y);
  shape.lineTo(x - r, -y);
  shape.quadraticCurveTo(x, -y, x, -y + r);
  shape.lineTo(x, y - r);
  shape.quadraticCurveTo(x, y, x - r, y);
  shape.lineTo(-x + r, y);
  shape.quadraticCurveTo(-x, y, -x, y - r);
  shape.lineTo(-x, -y + r);
  shape.quadraticCurveTo(-x, -y, -x + r, -y);
  return shape;
}

/* --------------------------------------------------------------------------
   The substrate's silhouette. A hewn block: straight facets of uneven length
   rather than a rounded rectangle, so it cannot be mistaken for another pane
   even in outline. Deterministic, so the object is the same on every load.
   -------------------------------------------------------------------------- */

function hewnShape(w, h, facets = 38, amount = 0.075) {
  const x = w / 2;
  const y = h / 2;
  const perimeter = [];
  for (let i = 0; i < facets; i += 1) {
    const t = i / facets;
    // Walk the rectangle perimeter.
    const side = t * 4;
    let px;
    let py;
    if (side < 1) { px = -x + 2 * x * side; py = -y; }
    else if (side < 2) { px = x; py = -y + 2 * y * (side - 1); }
    else if (side < 3) { px = x - 2 * x * (side - 2); py = y; }
    else { px = -x; py = y - 2 * y * (side - 3); }

    /* Layered irrational frequencies give organic variation without noise
       tables, and a coarse quantisation breaks it into facets rather than a
       smooth lump. */
    const wobble =
      0.46 * Math.sin(t * 19.1 + 1.13) +
      0.31 * Math.sin(t * 34.7 + 0.41) +
      0.23 * Math.sin(t * 61.3 + 2.67);
    const faceted = Math.round(wobble * 4) / 4;
    const scale = 1 + faceted * amount;
    perimeter.push([px * scale, py * scale]);
  }

  const shape = new Shape();
  shape.moveTo(perimeter[0][0], perimeter[0][1]);
  for (let i = 1; i < perimeter.length; i += 1) {
    shape.lineTo(perimeter[i][0], perimeter[i][1]);
  }
  shape.closePath();
  return shape;
}

/* A hairline highlight following the top and bottom arrises. The extruded side
   wall carries the metal; this is the glint along its edge. */
function arrisGeometry(shape, thickness) {
  const points = shape.getPoints(12);
  if (points.length && points[0].equals(points[points.length - 1])) points.pop();

  const positions = [];
  const pushLoop = (z) => {
    for (let i = 0; i < points.length; i += 1) {
      const a = points[i];
      const b = points[(i + 1) % points.length];
      positions.push(a.x, a.y, z, b.x, b.y, z);
    }
  };
  pushLoop(0.0006);
  pushLoop(thickness - 0.0006);

  const geometry = new BufferGeometry();
  geometry.setAttribute('position', new BufferAttribute(new Float32Array(positions), 3));
  return geometry;
}

/* ==========================================================================
   LAYER ARTWORK

   Each layer carries a different drawing, embedded at mid-thickness so it is
   read through the material rather than sitting on it. The surface plate is
   the exception: its composition sits on the top face, because it is the part
   you are meant to see.

   Drawn once and shared between stages.
   ========================================================================== */

const INK = (a) => `rgba(240,237,229,${a})`;
const ACCENT = (a) => `rgba(127,168,144,${a})`;

function artCanvas(resolution) {
  const canvas = document.createElement('canvas');
  canvas.width = resolution;
  canvas.height = Math.round(resolution * (PLATE_H / PLATE_W));
  const ctx = canvas.getContext('2d');
  ctx.fillStyle = '#000';
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  return { canvas, ctx, w: canvas.width, h: canvas.height };
}

/** Axis-aligned hairline, snapped so it stays crisp. */
function rule(ctx, x1, y1, x2, y2, alpha, width = 1) {
  ctx.strokeStyle = INK(alpha);
  ctx.lineWidth = width;
  ctx.beginPath();
  ctx.moveTo(Math.round(x1) + 0.5, Math.round(y1) + 0.5);
  ctx.lineTo(Math.round(x2) + 0.5, Math.round(y2) + 0.5);
  ctx.stroke();
}

function bar(ctx, x, y, w, h, alpha, colour = INK) {
  ctx.fillStyle = colour(alpha);
  ctx.fillRect(Math.round(x), Math.round(y), Math.round(w), Math.round(h));
}

function frame(ctx, x, y, w, h, alpha, width = 1) {
  ctx.strokeStyle = INK(alpha);
  ctx.lineWidth = width;
  ctx.strokeRect(Math.round(x) + 0.5, Math.round(y) + 0.5, Math.round(w), Math.round(h));
}

function dot(ctx, x, y, r, alpha, colour = INK) {
  ctx.fillStyle = colour(alpha);
  ctx.beginPath();
  ctx.arc(x, y, r, 0, Math.PI * 2);
  ctx.fill();
}

function ring(ctx, x, y, r, alpha, width = 1) {
  ctx.strokeStyle = INK(alpha);
  ctx.lineWidth = width;
  ctx.beginPath();
  ctx.arc(x, y, r, 0, Math.PI * 2);
  ctx.stroke();
}

/** Lines of body copy: varied lengths so it reads as text, not as stripes. */
function textBlock(ctx, x, y, width, lines, leading, alpha, seed = 1) {
  let n = seed * 9301;
  const next = () => ((n = (n * 9301 + 49297) % 233280) / 233280);
  for (let i = 0; i < lines; i += 1) {
    const last = i === lines - 1;
    const len = width * (last ? 0.42 + next() * 0.22 : 0.82 + next() * 0.18);
    bar(ctx, x, y + i * leading, len, Math.max(2, leading * 0.22), alpha);
  }
}

/* --- 06 DESIGN — the precision surface: a designed page on its own grid ---------------------------------------- */

function drawDesign(resolution) {
  const { canvas, ctx, w, h } = artCanvas(resolution);
  const m = w * 0.072; // page margin
  const col = (w - m * 2) / 12;

  // The system the page is set on, traced faintly beneath it.
  for (let c = 1; c < 12; c += 1) {
    rule(ctx, m + col * c, h * 0.05, m + col * c, h * 0.95, 0.055);
  }
  for (let r = 1; r < 16; r += 1) {
    rule(ctx, m, (h / 16) * r, w - m, (h / 16) * r, 0.03);
  }

  // Masthead: wordmark, navigation, one emphasised action.
  bar(ctx, m, h * 0.072, col * 1.35, h * 0.026, 0.86);
  for (let i = 0; i < 4; i += 1) {
    bar(ctx, m + col * (5.4 + i * 1.25), h * 0.079, col * 0.82, h * 0.014, 0.4);
  }
  frame(ctx, m + col * 10.1, h * 0.062, col * 1.9, h * 0.05, 0.5);
  bar(ctx, m + col * 10.38, h * 0.079, col * 1.34, h * 0.015, 0.62);
  rule(ctx, m, h * 0.15, w - m, h * 0.15, 0.22);

  // Hero: a short headline set very large, a line of supporting copy, one CTA.
  const heroY = h * 0.225;
  bar(ctx, m, heroY, col * 6.2, h * 0.062, 0.94);
  bar(ctx, m, heroY + h * 0.085, col * 4.5, h * 0.062, 0.94);
  textBlock(ctx, m, heroY + h * 0.2, col * 4.4, 2, h * 0.032, 0.4, 3);

  const ctaY = heroY + h * 0.29;
  frame(ctx, m, ctaY, col * 2.9, h * 0.062, 0.62);
  bar(ctx, m + col * 0.3, ctaY + h * 0.026, col * 1.9, h * 0.016, 0.28, ACCENT);

  // Media: a framed plate with a horizon and an aperture mark, not a photo.
  const mx = m + col * 7.1;
  const my = heroY - h * 0.03;
  const mw = col * 4.9;
  const mh = h * 0.4;
  frame(ctx, mx, my, mw, mh, 0.42);
  ctx.save();
  ctx.beginPath();
  ctx.rect(mx, my, mw, mh);
  ctx.clip();
  const wash = ctx.createLinearGradient(mx, my, mx + mw * 0.6, my + mh);
  wash.addColorStop(0, INK(0.16));
  wash.addColorStop(1, INK(0.02));
  ctx.fillStyle = wash;
  ctx.fillRect(mx, my, mw, mh);
  rule(ctx, mx, my + mh * 0.66, mx + mw, my + mh * 0.66, 0.3);
  ring(ctx, mx + mw * 0.72, my + mh * 0.34, mh * 0.13, 0.36);
  ctx.restore();

  // Page structure below the fold: three ruled measures of running copy.
  const bodyY = h * 0.68;
  rule(ctx, m, bodyY - h * 0.045, w - m, bodyY - h * 0.045, 0.2);
  for (let c = 0; c < 3; c += 1) {
    const cx = m + c * (col * 4);
    bar(ctx, cx, bodyY, col * 1.5, h * 0.018, 0.56);
    textBlock(ctx, cx, bodyY + h * 0.05, col * 3.3, 4, h * 0.036, 0.3, c + 5);
  }

  // Footer.
  rule(ctx, m, h * 0.935, w - m, h * 0.935, 0.24);
  bar(ctx, m, h * 0.955, col * 1.1, h * 0.014, 0.34);
  for (let i = 0; i < 3; i += 1) {
    bar(ctx, w - m - col * (1 + i * 1.3), h * 0.955, col * 0.9, h * 0.012, 0.22);
  }
  return canvas;
}

/* --- 05 TRUST — embedded marks and credentials --------------------------- */

function drawTrust(resolution) {
  const { canvas, ctx, w, h } = artCanvas(resolution);
  const m = w * 0.09;

  // A seal: concentric rings with a check struck through the centre.
  const sx = m + w * 0.1;
  const sy = h * 0.32;
  const sr = h * 0.15;
  ring(ctx, sx, sy, sr, 0.5, 2);
  ring(ctx, sx, sy, sr * 0.72, 0.26);
  for (let i = 0; i < 24; i += 1) {
    const a = (i / 24) * Math.PI * 2;
    rule(
      ctx,
      sx + Math.cos(a) * sr * 0.82,
      sy + Math.sin(a) * sr * 0.82,
      sx + Math.cos(a) * sr * 0.93,
      sy + Math.sin(a) * sr * 0.93,
      0.3
    );
  }
  ctx.strokeStyle = ACCENT(0.66);
  ctx.lineWidth = 3;
  ctx.beginPath();
  ctx.moveTo(sx - sr * 0.3, sy);
  ctx.lineTo(sx - sr * 0.06, sy + sr * 0.26);
  ctx.lineTo(sx + sr * 0.34, sy - sr * 0.26);
  ctx.stroke();

  // A closure mark: shackle over a body. Abstract, but unmistakably a lock.
  const lx = m + w * 0.33;
  const ly = h * 0.3;
  ctx.strokeStyle = INK(0.46);
  ctx.lineWidth = 3;
  ctx.beginPath();
  ctx.arc(lx, ly, h * 0.055, Math.PI, 0);
  ctx.stroke();
  frame(ctx, lx - h * 0.082, ly, h * 0.164, h * 0.12, 0.46, 2);
  dot(ctx, lx, ly + h * 0.06, 3.5, 0.42);

  // A credential plate: identifier over two data rows.
  const px = m + w * 0.52;
  const py = h * 0.22;
  frame(ctx, px, py, w * 0.3, h * 0.2, 0.36);
  bar(ctx, px + w * 0.02, py + h * 0.035, w * 0.13, h * 0.022, 0.6);
  bar(ctx, px + w * 0.02, py + h * 0.09, w * 0.24, h * 0.013, 0.28);
  bar(ctx, px + w * 0.02, py + h * 0.128, w * 0.18, h * 0.013, 0.28);
  bar(ctx, px + w * 0.24, py + h * 0.155, w * 0.04, h * 0.013, 0.5, ACCENT);

  // A verification ledger: timestamped rows, one still open.
  const ry = h * 0.62;
  rule(ctx, m, ry, w - m, ry, 0.3);
  for (let i = 0; i < 5; i += 1) {
    const y = ry + h * 0.055 + i * h * 0.062;
    bar(ctx, m, y, w * 0.07, h * 0.012, 0.42);
    bar(ctx, m + w * 0.1, y, w * (0.2 + (i % 3) * 0.08), h * 0.012, 0.22);
    if (i === 3) {
      ring(ctx, w - m - h * 0.02, y + h * 0.006, h * 0.016, 0.34);
    } else {
      bar(ctx, w - m - h * 0.03, y, h * 0.03, h * 0.012, 0.3);
    }
  }
  return canvas;
}

/* --- 04 SEARCH — index and hierarchy ------------------------------------- */

function drawSearch(resolution) {
  const { canvas, ctx, w, h } = artCanvas(resolution);
  const m = w * 0.08;

  // A site hierarchy: root, sections, leaves, drawn with elbow connectors.
  const rootX = m + w * 0.06;
  const rootY = h * 0.5;
  bar(ctx, rootX - w * 0.022, rootY - h * 0.014, w * 0.044, h * 0.028, 0.66);

  const branchX = m + w * 0.18;
  const leafX = m + w * 0.3;
  const sections = 3;
  for (let s = 0; s < sections; s += 1) {
    const by = h * (0.24 + s * 0.26);
    rule(ctx, rootX + w * 0.022, rootY, branchX - w * 0.03, rootY, 0.3);
    rule(ctx, branchX - w * 0.03, rootY, branchX - w * 0.03, by, 0.3);
    rule(ctx, branchX - w * 0.03, by, branchX - w * 0.018, by, 0.3);
    frame(ctx, branchX - w * 0.018, by - h * 0.018, w * 0.05, h * 0.036, 0.44);

    for (let l = 0; l < 2; l += 1) {
      const ly = by - h * 0.05 + l * h * 0.1;
      rule(ctx, branchX + w * 0.032, by, leafX - w * 0.022, by, 0.2);
      rule(ctx, leafX - w * 0.022, by, leafX - w * 0.022, ly, 0.2);
      rule(ctx, leafX - w * 0.022, ly, leafX - w * 0.01, ly, 0.2);
      bar(ctx, leafX - w * 0.01, ly - h * 0.008, w * 0.036, h * 0.016, 0.3);
    }
  }

  // An index: entries with leader dots and a locator, one entry unresolved.
  const ix = m + w * 0.46;
  rule(ctx, ix, h * 0.14, w - m, h * 0.14, 0.34);
  for (let i = 0; i < 8; i += 1) {
    const y = h * 0.21 + i * h * 0.09;
    const depth = i % 3 === 0 ? 0 : w * 0.024;
    bar(ctx, ix + depth, y, w * (0.11 - (i % 3) * 0.018), h * 0.014, i % 3 === 0 ? 0.5 : 0.3);
    const dotsFrom = ix + depth + w * (0.12 - (i % 3) * 0.018);
    const dotsTo = w - m - w * 0.05;
    for (let d = dotsFrom; d < dotsTo; d += w * 0.016) {
      dot(ctx, d, y + h * 0.007, 1.4, 0.18);
    }
    if (i === 5) {
      frame(ctx, w - m - w * 0.042, y - h * 0.004, w * 0.042, h * 0.022, 0.4);
    } else {
      bar(ctx, w - m - w * 0.03, y, w * 0.03, h * 0.014, 0.3);
    }
  }
  // The accent marks the term currently being resolved.
  bar(ctx, ix, h * 0.21 + 5 * h * 0.09, w * 0.11, 3, 0.55, ACCENT);
  return canvas;
}

/* --- 03 CONVERSION — pathways and nodes ---------------------------------- */

function drawConversion(resolution) {
  const { canvas, ctx, w, h } = artCanvas(resolution);
  const m = w * 0.09;

  const nodes = [
    { x: m, y: h * 0.2 },
    { x: m, y: h * 0.5 },
    { x: m, y: h * 0.8 },
    { x: m + w * 0.28, y: h * 0.32 },
    { x: m + w * 0.28, y: h * 0.66 },
    { x: m + w * 0.55, y: h * 0.5 },
  ];
  const target = { x: w - m - w * 0.04, y: h * 0.5 };

  const path = (a, b, alpha) => {
    ctx.strokeStyle = INK(alpha);
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.moveTo(a.x, a.y);
    const midX = (a.x + b.x) / 2;
    ctx.bezierCurveTo(midX, a.y, midX, b.y, b.x, b.y);
    ctx.stroke();
    // A direction mark two thirds along, so flow is legible.
    const t = 0.66;
    const px = (1 - t) ** 3 * a.x + 3 * (1 - t) ** 2 * t * midX + 3 * (1 - t) * t * t * midX + t ** 3 * b.x;
    const py = (1 - t) ** 3 * a.y + 3 * (1 - t) ** 2 * t * a.y + 3 * (1 - t) * t * t * b.y + t ** 3 * b.y;
    ctx.beginPath();
    ctx.moveTo(px - 7, py - 5);
    ctx.lineTo(px + 4, py);
    ctx.lineTo(px - 7, py + 5);
    ctx.stroke();
  };

  path(nodes[0], nodes[3], 0.34);
  path(nodes[1], nodes[3], 0.28);
  path(nodes[1], nodes[4], 0.28);
  path(nodes[2], nodes[4], 0.34);
  path(nodes[3], nodes[5], 0.4);
  path(nodes[4], nodes[5], 0.4);

  // The converging path into the single action.
  ctx.strokeStyle = ACCENT(0.6);
  ctx.lineWidth = 2.5;
  ctx.beginPath();
  ctx.moveTo(nodes[5].x, nodes[5].y);
  ctx.lineTo(target.x - w * 0.05, target.y);
  ctx.stroke();

  // And one route that simply stops. Attention arriving with nowhere to go.
  ctx.setLineDash([7, 9]);
  ctx.strokeStyle = INK(0.24);
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  ctx.moveTo(nodes[4].x, nodes[4].y);
  ctx.lineTo(m + w * 0.46, h * 0.88);
  ctx.stroke();
  ctx.setLineDash([]);
  rule(ctx, m + w * 0.43, h * 0.845, m + w * 0.49, h * 0.915, 0.34);
  rule(ctx, m + w * 0.49, h * 0.845, m + w * 0.43, h * 0.915, 0.34);

  for (const node of nodes) ring(ctx, node.x, node.y, h * 0.022, 0.46, 2);
  for (const node of nodes.slice(0, 3)) dot(ctx, node.x, node.y, h * 0.008, 0.4);
  dot(ctx, nodes[5].x, nodes[5].y, h * 0.01, 0.5);

  // The action itself.
  frame(ctx, target.x - w * 0.05, target.y - h * 0.045, w * 0.1, h * 0.09, 0.56, 2);
  bar(ctx, target.x - w * 0.032, target.y - h * 0.008, w * 0.064, h * 0.016, 0.62, ACCENT);
  return canvas;
}

/* --- 02 ACCESSIBILITY — semantic structure and focus order --------------- */

function drawAccessibility(resolution) {
  const { canvas, ctx, w, h } = artCanvas(resolution);
  const m = w * 0.075;
  const iw = w - m * 2;

  // Landmark regions, nested as a document outline would be.
  const regions = [
    { x: m, y: h * 0.09, w: iw, h: h * 0.1 }, // banner
    { x: m, y: h * 0.21, w: iw * 0.62, h: h * 0.44 }, // main
    { x: m + iw * 0.66, y: h * 0.21, w: iw * 0.34, h: h * 0.44 }, // complementary
    { x: m, y: h * 0.67, w: iw, h: h * 0.24 }, // contentinfo
  ];
  for (const [i, r] of regions.entries()) {
    frame(ctx, r.x, r.y, r.w, r.h, i === 1 ? 0.42 : 0.28, i === 1 ? 2 : 1);
    bar(ctx, r.x + w * 0.014, r.y + h * 0.026, w * (0.07 - i * 0.008), h * 0.014, 0.42);
  }
  // Two articles inside main, each with a heading and its copy.
  for (let a = 0; a < 2; a += 1) {
    const ax = m + w * 0.03;
    const ay = h * 0.3 + a * h * 0.17;
    bar(ctx, ax, ay, iw * 0.3, h * 0.022, 0.36);
    textBlock(ctx, ax, ay + h * 0.045, iw * 0.46, 2, h * 0.032, 0.2, a + 2);
  }
  // A heading-level ladder: h1 to h4, each step indented.
  for (let l = 0; l < 4; l += 1) {
    const lx = m + iw * 0.69 + l * w * 0.018;
    const ly = h * 0.27 + l * h * 0.075;
    rule(ctx, lx, ly, lx, ly + h * 0.05, 0.3);
    bar(ctx, lx + w * 0.008, ly + h * 0.02, iw * (0.2 - l * 0.035), h * 0.013, 0.3 - l * 0.04);
  }

  // Focus order: a single path through numbered stops, in sequence.
  const stops = [
    { x: m + w * 0.05, y: h * 0.14 },
    { x: m + iw * 0.5, y: h * 0.14 },
    { x: m + iw * 0.9, y: h * 0.14 },
    { x: m + w * 0.06, y: h * 0.36 },
    { x: m + w * 0.06, y: h * 0.53 },
    { x: m + iw * 0.78, y: h * 0.42 },
    { x: m + w * 0.08, y: h * 0.78 },
  ];
  ctx.strokeStyle = ACCENT(0.4);
  ctx.lineWidth = 1.5;
  ctx.setLineDash([5, 6]);
  ctx.beginPath();
  stops.forEach((s, i) => (i ? ctx.lineTo(s.x, s.y) : ctx.moveTo(s.x, s.y)));
  ctx.stroke();
  ctx.setLineDash([]);
  for (const [i, s] of stops.entries()) {
    const r = h * 0.017;
    ctx.strokeStyle = i === 0 ? ACCENT(0.7) : INK(0.42);
    ctx.lineWidth = i === 0 ? 2.5 : 1.5;
    ctx.strokeRect(s.x - r, s.y - r, r * 2, r * 2);
  }
  return canvas;
}

/* --- 01 PERFORMANCE — measurement traces --------------------------------- */

function drawPerformance(resolution) {
  const { canvas, ctx, w, h } = artCanvas(resolution);
  const m = w * 0.08;
  const iw = w - m * 2;

  // A request waterfall. Offsets and lengths are fixed, not random, so the
  // trace reads as one measurement rather than noise.
  const requests = [
    [0.0, 0.14], [0.05, 0.1], [0.08, 0.22], [0.12, 0.09], [0.16, 0.31],
    [0.2, 0.07], [0.24, 0.18], [0.3, 0.12], [0.34, 0.26], [0.42, 0.1],
    [0.48, 0.2], [0.56, 0.08],
  ];
  const top = h * 0.16;
  const rowH = h * 0.045;
  requests.forEach(([start, length], i) => {
    const y = top + i * rowH;
    rule(ctx, m, y + rowH * 0.5, w - m, y + rowH * 0.5, 0.05);
    bar(ctx, m + iw * start, y + rowH * 0.2, iw * length, rowH * 0.42, i === 4 ? 0.0 : 0.34);
    if (i === 4) bar(ctx, m + iw * start, y + rowH * 0.2, iw * length, rowH * 0.42, 0.52, ACCENT);
  });

  // Time axis with major and minor ticks.
  const axisY = top + requests.length * rowH + h * 0.03;
  rule(ctx, m, axisY, w - m, axisY, 0.44);
  for (let t = 0; t <= 20; t += 1) {
    const x = m + (iw / 20) * t;
    const major = t % 5 === 0;
    rule(ctx, x, axisY, x, axisY + (major ? h * 0.028 : h * 0.014), major ? 0.44 : 0.22);
    if (major) bar(ctx, x + 4, axisY + h * 0.04, w * 0.03, h * 0.011, 0.24);
  }
  // Two thresholds crossing the whole trace.
  for (const [at, alpha] of [[0.45, 0.22], [0.72, 0.16]]) {
    const x = m + iw * at;
    ctx.setLineDash([4, 6]);
    rule(ctx, x, h * 0.13, x, axisY, alpha);
    ctx.setLineDash([]);
  }

  // A sampled trace across the head of the plate.
  ctx.strokeStyle = INK(0.3);
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  const samples = [0.42, 0.3, 0.55, 0.38, 0.72, 0.5, 0.62, 0.34, 0.46, 0.28];
  samples.forEach((v, i) => {
    const x = m + (iw / (samples.length - 1)) * i;
    const y = h * 0.11 - v * h * 0.05;
    return i ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
  });
  ctx.stroke();
  for (let i = 0; i < samples.length; i += 1) {
    const x = m + (iw / (samples.length - 1)) * i;
    dot(ctx, x, h * 0.11 - samples[i] * h * 0.05, 1.8, 0.34);
  }
  return canvas;
}

const ARTISTS = {
  design: drawDesign,
  trust: drawTrust,
  search: drawSearch,
  conversion: drawConversion,
  accessibility: drawAccessibility,
  performance: drawPerformance,
};

const artCache = new Map();
function layerArt(key, resolution) {
  if (!artCache.has(key)) artCache.set(key, ARTISTS[key](resolution));
  return artCache.get(key);
}

function artTexture(key, resolution) {
  const texture = new CanvasTexture(layerArt(key, resolution));
  texture.colorSpace = SRGBColorSpace;
  texture.anisotropy = 8;
  texture.magFilter = LinearFilter;
  texture.minFilter = LinearMipmapLinearFilter;
  return texture;
}

/* --------------------------------------------------------------------------
   Mineral substrate. Warm stone with fine grain and a few veins — the
   physical material the digital layers are lifted out of.
   -------------------------------------------------------------------------- */

let stoneCanvas = null;
function stoneTexture() {
  if (!stoneCanvas) {
    const size = 512;
    const canvas = document.createElement('canvas');
    canvas.width = size;
    canvas.height = size;
    const ctx = canvas.getContext('2d');
    ctx.fillStyle = '#5b5649';
    ctx.fillRect(0, 0, size, size);

    let n = 12345;
    const next = () => ((n = (n * 1103515245 + 12345) % 2147483648) / 2147483648);

    // Mottling, coarse to fine.
    for (let pass = 0; pass < 3; pass += 1) {
      const r = size * (0.16 / (pass + 1));
      for (let i = 0; i < 90 * (pass + 1); i += 1) {
        const x = next() * size;
        const y = next() * size;
        const shade = next() > 0.5 ? 255 : 0;
        const g = ctx.createRadialGradient(x, y, 0, x, y, r);
        g.addColorStop(0, `rgba(${shade},${shade},${shade},${0.035 + next() * 0.03})`);
        g.addColorStop(1, 'rgba(0,0,0,0)');
        ctx.fillStyle = g;
        ctx.fillRect(x - r, y - r, r * 2, r * 2);
      }
    }
    // Veins.
    for (let v = 0; v < 5; v += 1) {
      ctx.strokeStyle = `rgba(232,226,210,${0.05 + next() * 0.05})`;
      ctx.lineWidth = 1 + next() * 2.5;
      ctx.beginPath();
      let x = next() * size;
      let y = -10;
      ctx.moveTo(x, y);
      while (y < size + 10) {
        x += (next() - 0.5) * size * 0.16;
        y += size * 0.1;
        ctx.lineTo(x, y);
      }
      ctx.stroke();
    }
    // Grain.
    for (let i = 0; i < 9000; i += 1) {
      const a = next() * 0.06;
      ctx.fillStyle = next() > 0.5 ? `rgba(255,252,244,${a})` : `rgba(20,18,14,${a})`;
      ctx.fillRect(next() * size, next() * size, 1, 1);
    }
    stoneCanvas = canvas;
  }
  const texture = new CanvasTexture(stoneCanvas);
  texture.colorSpace = SRGBColorSpace;
  texture.anisotropy = 4;
  return texture;
}

/**
 * A normal map for the substrate's hewn faces. Multi-octave value noise turned
 * into surface normals, so the sides answer light as broken stone rather than
 * as a flat extrusion. The planed top uses the same map at a fraction of the
 * strength, which is what makes the contrast between worked and unworked stone.
 */
let stoneNormalCanvas = null;
function stoneNormalTexture() {
  if (!stoneNormalCanvas) {
    const size = 256;
    const height = new Float32Array(size * size);

    const hash = (x, y) => {
      const n = Math.sin(x * 127.1 + y * 311.7) * 43758.5453123;
      return n - Math.floor(n);
    };
    const smooth = (t) => t * t * (3 - 2 * t);

    for (let octave = 0; octave < 5; octave += 1) {
      const frequency = 4 * 2 ** octave;
      const amplitude = 1 / 2 ** octave;
      const cell = size / frequency;
      for (let y = 0; y < size; y += 1) {
        for (let x = 0; x < size; x += 1) {
          const fx = x / cell;
          const fy = y / cell;
          const x0 = Math.floor(fx);
          const y0 = Math.floor(fy);
          const tx = smooth(fx - x0);
          const ty = smooth(fy - y0);
          const wrap = (v) => ((v % frequency) + frequency) % frequency;
          const a = hash(wrap(x0), wrap(y0));
          const b = hash(wrap(x0 + 1), wrap(y0));
          const c = hash(wrap(x0), wrap(y0 + 1));
          const d = hash(wrap(x0 + 1), wrap(y0 + 1));
          const top = a + (b - a) * tx;
          const bottom = c + (d - c) * tx;
          height[y * size + x] += (top + (bottom - top) * ty) * amplitude;
        }
      }
    }

    const canvas = document.createElement('canvas');
    canvas.width = size;
    canvas.height = size;
    const ctx = canvas.getContext('2d');
    const image = ctx.createImageData(size, size);
    const strength = 5.5;
    const at = (x, y) => height[((y + size) % size) * size + ((x + size) % size)];

    for (let y = 0; y < size; y += 1) {
      for (let x = 0; x < size; x += 1) {
        const dx = (at(x + 1, y) - at(x - 1, y)) * strength;
        const dy = (at(x, y + 1) - at(x, y - 1)) * strength;
        const length = Math.hypot(-dx, -dy, 1);
        const index = (y * size + x) * 4;
        image.data[index] = ((-dx / length) * 0.5 + 0.5) * 255;
        image.data[index + 1] = ((-dy / length) * 0.5 + 0.5) * 255;
        image.data[index + 2] = (1 / length) * 0.5 * 255 + 127.5;
        image.data[index + 3] = 255;
      }
    }
    ctx.putImageData(image, 0, 0);
    stoneNormalCanvas = canvas;
  }

  const texture = new CanvasTexture(stoneNormalCanvas);
  texture.wrapS = RepeatWrapping;
  texture.wrapT = RepeatWrapping;
  texture.anisotropy = 4;
  return texture;
}

/** Contact shadow the stack casts on the substrate. */
let shadowCanvas = null;
function shadowTexture() {
  if (!shadowCanvas) {
    const size = 256;
    const canvas = document.createElement('canvas');
    canvas.width = size;
    canvas.height = size;
    const ctx = canvas.getContext('2d');
    const g = ctx.createRadialGradient(size / 2, size / 2, 0, size / 2, size / 2, size / 2);
    g.addColorStop(0, 'rgba(0,0,0,0.78)');
    g.addColorStop(0.55, 'rgba(0,0,0,0.42)');
    g.addColorStop(1, 'rgba(0,0,0,0)');
    ctx.fillStyle = g;
    ctx.fillRect(0, 0, size, size);
    shadowCanvas = canvas;
  }
  const texture = new CanvasTexture(shadowCanvas);
  texture.colorSpace = SRGBColorSpace;
  return texture;
}

/* --------------------------------------------------------------------------
   Framing. The three stages occupy very differently shaped boxes and the
   specimen is a broad flat slab, so any hand-tuned camera distance clips it
   in at least one of them. Solve for the distance instead.
   -------------------------------------------------------------------------- */

function assemblyCorners(air, yaw, centre) {
  const cos = Math.cos(yaw);
  const sin = Math.sin(yaw);
  const top = plateBase(0, air) + STACK[0].thickness;
  const bottom = -(FOUNDATION_CLEARANCE + FOUNDATION_T);
  const halfW = (PLATE_W * FOUNDATION_SCALE) / 2;
  const halfD = (PLATE_H * FOUNDATION_SCALE) / 2;

  const corners = [];
  for (const sx of [-1, 1]) {
    for (const sz of [-1, 1]) {
      for (const y of [bottom, top]) {
        const x = sx * halfW;
        const z = sz * halfD;
        corners.push(new Vector3(x * cos + z * sin, y - centre, -x * sin + z * cos));
      }
    }
  }
  return corners;
}

/** Vertical centre of the whole specimen, substrate included. */
function assemblyCentre(air) {
  const top = plateBase(0, air) + STACK[0].thickness;
  const bottom = -(FOUNDATION_CLEARANCE + FOUNDATION_T);
  return (top + bottom) / 2;
}

function fitsAt(probe, corners, distance, pitch) {
  probe.position.set(0, distance * pitch, distance);
  probe.lookAt(0, 0, 0);
  probe.updateMatrixWorld(true);
  probe.updateProjectionMatrix();
  for (const corner of corners) {
    const ndc = corner.clone().project(probe);
    if (Math.abs(ndc.x) > 0.94 || Math.abs(ndc.y) > 0.94) return false;
    if (ndc.z > 1) return false;
  }
  return true;
}

function frameDistance(probe, air, yaw, pitch) {
  const corners = assemblyCorners(air, yaw, assemblyCentre(air));
  let low = 2;
  let high = 70;
  if (!fitsAt(probe, corners, high, pitch)) return high;
  for (let i = 0; i < 22; i += 1) {
    const mid = (low + high) / 2;
    if (fitsAt(probe, corners, mid, pitch)) high = mid;
    else low = mid;
  }
  return high;
}

const FIT_SAMPLES = 7;

function buildFitTable(probe, widestAir, yaw, pitch) {
  const table = [];
  for (let i = 0; i < FIT_SAMPLES; i += 1) {
    const t = i / (FIT_SAMPLES - 1);
    table.push(frameDistance(probe, lerp(AIR_ASSEMBLED, widestAir, t), yaw, pitch));
  }
  return table;
}

function fitAt(table, t) {
  const position = clamp(t) * (table.length - 1);
  const index = Math.min(Math.floor(position), table.length - 2);
  return lerp(table[index], table[index + 1], position - index);
}

/* --------------------------------------------------------------------------
   Environment: a procedural equirectangular studio. Two softboxes, a bright
   horizon for the machined edges to catch, and warm bounce from below. Enough
   structure for the acrylic and the metal to have something to reflect, with
   no HDR asset to download.
   -------------------------------------------------------------------------- */

function studioEnvironment(renderer) {
  const canvas = document.createElement('canvas');
  canvas.width = 512;
  canvas.height = 256;
  const ctx = canvas.getContext('2d');
  const w = canvas.width;
  const h = canvas.height;

  const sky = ctx.createLinearGradient(0, 0, 0, h);
  sky.addColorStop(0, '#46423a');
  sky.addColorStop(0.38, '#221f1b');
  sky.addColorStop(0.52, '#0d0d0b');
  sky.addColorStop(0.8, '#1a1815');
  sky.addColorStop(1, '#2b2822');
  ctx.fillStyle = sky;
  ctx.fillRect(0, 0, w, h);

  const softbox = (x, y, rx, ry, alpha, tint = '255,250,240') => {
    const g = ctx.createRadialGradient(x, y, 0, x, y, Math.max(rx, ry));
    g.addColorStop(0, `rgba(${tint},${alpha})`);
    g.addColorStop(0.5, `rgba(${tint},${alpha * 0.35})`);
    g.addColorStop(1, `rgba(${tint},0)`);
    ctx.save();
    ctx.translate(x, y);
    ctx.scale(1, ry / rx);
    ctx.translate(-x, -y);
    ctx.fillStyle = g;
    ctx.fillRect(x - rx * 1.4, y - rx * 1.4, rx * 2.8, rx * 2.8);
    ctx.restore();
  };

  softbox(w * 0.24, h * 0.2, w * 0.17, h * 0.2, 1);
  softbox(w * 0.72, h * 0.3, w * 0.1, h * 0.12, 0.4);
  softbox(w * 0.52, h * 0.86, w * 0.22, h * 0.1, 0.16, '236,228,208');

  // Horizon strip: the specular line that reads along a machined arris.
  const horizon = ctx.createLinearGradient(0, h * 0.47, 0, h * 0.53);
  horizon.addColorStop(0, 'rgba(255,248,236,0)');
  horizon.addColorStop(0.5, 'rgba(255,248,236,0.5)');
  horizon.addColorStop(1, 'rgba(255,248,236,0)');
  ctx.fillStyle = horizon;
  ctx.fillRect(0, h * 0.47, w, h * 0.06);

  const texture = new CanvasTexture(canvas);
  texture.mapping = EquirectangularReflectionMapping;
  texture.colorSpace = SRGBColorSpace;

  const pmrem = new PMREMGenerator(renderer);
  const target = pmrem.fromEquirectangular(texture);
  pmrem.dispose();
  texture.dispose();
  return target.texture;
}

/* --------------------------------------------------------------------------
   Shared geometry. three.js keeps GPU state per renderer, so the geometry
   itself is built once for all three stages.
   -------------------------------------------------------------------------- */

let shared = null;

function sharedGeometry() {
  if (shared) return shared;
  const shape = plateShape(PLATE_W, PLATE_H, CORNER);
  const plates = STACK.map((layer) => ({
    /* Two material groups: group 0 is the acrylic cap, group 1 the extruded
       side wall, which is where the machined metal lives. */
    body: new ExtrudeGeometry(shape, {
      depth: layer.thickness,
      bevelEnabled: false,
      curveSegments: 5,
    }),
    arris: arrisGeometry(shape, layer.thickness),
    art: new PlaneGeometry(PLATE_W * 0.9, PLATE_H * 0.9),
  }));

  shared = {
    plates,
    /* The block: hewn in plan, a quarter of the object's width thick. Group 0
       is its lids, group 1 its broken sides. */
    foundation: new ExtrudeGeometry(
      hewnShape(PLATE_W * FOUNDATION_SCALE, PLATE_H * FOUNDATION_SCALE),
      { depth: FOUNDATION_T, bevelEnabled: false }
    ),
    /* The planed pad cut into the top of the block, where the engineered system
       seats into it. Regular, because this part has been worked. */
    seat: new ExtrudeGeometry(
      plateShape(PLATE_W * SEAT_SCALE, PLATE_H * SEAT_SCALE, CORNER),
      { depth: SEAT_T, bevelEnabled: false, curveSegments: 5 }
    ),
    seatArris: arrisGeometry(
      plateShape(PLATE_W * SEAT_SCALE, PLATE_H * SEAT_SCALE, CORNER),
      SEAT_T
    ),
    shadow: new PlaneGeometry(PLATE_W * 1.5, PLATE_H * 1.5),
  };
  return shared;
}

/* --------------------------------------------------------------------------
   The object
   -------------------------------------------------------------------------- */

export function createDimensionalObject(canvas, { mode = 'decompose' } = {}) {
  let renderer;
  try {
    renderer = new WebGLRenderer({
      canvas,
      antialias: true,
      alpha: true,
      powerPreference: 'low-power',
    });
  } catch {
    return null;
  }

  renderer.setClearAlpha(0);
  renderer.toneMapping = ACESFilmicToneMapping;
  renderer.toneMappingExposure = 1.12;
  renderer.outputColorSpace = SRGBColorSpace;

  /* Act I looks down onto the specimen so the surface reads as a page. Act II
     and VI sit lower and more architectural, but still high enough that each
     layer's own drawing is legible rather than foreshortened into a line.
     Examining a layer raises the angle further: the subject turns its face up. */
  const basePitch = mode === 'surface' ? 0.44 : mode === 'reconstruct' ? 0.44 : 0.5;
  const examinePitch = basePitch + 0.1;
  const baseYaw = mode === 'surface' ? -0.36 : -0.46;

  const scene = new Scene();
  const camera = new PerspectiveCamera(32, 1, 0.1, 120);

  /* Depth falloff: the far side of the specimen recedes into the environment
     instead of staying uniformly lit to the edge of the frame. */
  scene.fog = new Fog(0x0d0d0b, 6, 20);

  const environment = studioEnvironment(renderer);
  scene.environment = environment;

  /* Directional and restrained. One key, one cool fill, one low back light to
     catch the machined arrises. No coloured practicals, no rim theatrics. */
  const key = new DirectionalLight(0xfff4e2, 2.5);
  key.position.set(-3.6, 7.4, 3.2);
  scene.add(key);

  const fill = new DirectionalLight(0xa8c0b4, 0.62);
  fill.position.set(4.8, 1.1, -3.4);
  scene.add(fill);

  const back = new DirectionalLight(0xf0ede5, 0.8);
  back.position.set(1.4, -1.2, -4.6);
  scene.add(back);

  /* The examination lights. They travel to whichever layer is under discussion
     and are dark the rest of the time. */
  const examine = new PointLight(0xfff6e8, 0, 3.4, 2);
  scene.add(examine);

  const assembly = new Group();
  assembly.rotation.y = baseYaw;
  scene.add(assembly);

  /* The raking light lives inside the assembly so its direction stays fixed
     relative to the plates rather than swinging with the pointer parallax. */
  const grazeTarget = new Object3D();
  assembly.add(grazeTarget);
  const graze = new DirectionalLight(0xfff8ec, 0);
  graze.target = grazeTarget;
  assembly.add(graze);

  const geometry = sharedGeometry();
  const tintBase = new Color(SUBSTRAL_BLACK);
  const mineral = new Color(MINERAL);
  const patina = new Color(PATINA);

  /* --- Substrate -------------------------------------------------------- */

  /* The substrate. Two materials on one block: the lids are planed stone, the
     sides are left broken. The contrast between worked and unworked is the
     whole point — a geological foundation under a precision-engineered system. */
  const stone = stoneTexture();
  const stoneNormal = stoneNormalTexture();
  const hewnNormal = stoneNormalTexture();
  hewnNormal.repeat.set(2.4, 1.4);

  const planedMaterial = new MeshPhysicalMaterial({
    color: new Color(STONE),
    map: stone,
    roughnessMap: stone,
    normalMap: stoneNormal,
    roughness: 0.74,
    metalness: 0.04,
    clearcoat: 0.14,
    clearcoatRoughness: 0.6,
    envMapIntensity: 0.72,
  });
  planedMaterial.normalScale.set(0.3, 0.3);

  const hewnMaterial = new MeshPhysicalMaterial({
    color: new Color(STONE).multiplyScalar(0.94),
    map: stone,
    roughnessMap: stone,
    normalMap: hewnNormal,
    roughness: 1,
    metalness: 0.02,
    envMapIntensity: 0.6,
  });
  hewnMaterial.normalScale.set(1.9, 1.9);

  const foundation = new Mesh(geometry.foundation, [planedMaterial, hewnMaterial]);
  foundation.rotation.x = -Math.PI / 2;
  /* Extrusion runs along world +Y once the block is laid flat, so it is dropped
     by its full thickness to put its planed top on the clearance line. */
  foundation.position.y = -FOUNDATION_CLEARANCE - FOUNDATION_T;
  assembly.add(foundation);

  /* The seat: a shallow machined pad on top of the block, with its own arris.
     This is where the stone has been worked to receive the layers. */
  const seatMaterial = new MeshPhysicalMaterial({
    color: new Color(STONE).multiplyScalar(1.22),
    map: stone,
    roughness: 0.42,
    metalness: 0.14,
    clearcoat: 0.3,
    clearcoatRoughness: 0.4,
    envMapIntensity: 0.7,
  });
  const seat = new Mesh(geometry.seat, seatMaterial);
  seat.rotation.x = -Math.PI / 2;
  seat.position.y = -FOUNDATION_CLEARANCE;
  assembly.add(seat);

  const seatArrisMaterial = new MeshBasicMaterial({
    color: mineral.clone(),
    transparent: true,
    opacity: 0.16,
    depthWrite: false,
    fog: false,
  });
  const seatArris = new Mesh(geometry.seatArris, seatArrisMaterial);
  seatArris.rotation.x = -Math.PI / 2;
  seatArris.position.y = seat.position.y;
  assembly.add(seatArris);

  const shadowMaterial = new MeshBasicMaterial({
    map: shadowTexture(),
    transparent: true,
    depthWrite: false,
    fog: false,
  });
  const contactShadow = new Mesh(geometry.shadow, shadowMaterial);
  contactShadow.rotation.x = -Math.PI / 2;
  contactShadow.position.y = -FOUNDATION_CLEARANCE + SEAT_T + 0.004;
  assembly.add(contactShadow);

  /* --- Plates ----------------------------------------------------------- */

  const plates = STACK.map((layer, index) => {
    const group = new Group();
    // The plates lie flat: the extrusion axis becomes vertical thickness.
    group.rotation.x = -Math.PI / 2;

    /* The value ladder, warmed very slightly for Trust. Warmth is a material
       response, not a brand colour: it stays inside the mineral palette. */
    const body = tintBase.clone().lerp(mineral, layer.tint);
    if (layer.warmth) body.lerp(new Color(0xbfa98a), layer.warmth * 0.09);

    const capMaterial = new MeshPhysicalMaterial({
      color: body,
      metalness: layer.metalness,
      roughness: layer.roughness,
      clearcoat: layer.clearcoat,
      clearcoatRoughness: layer.clearcoatRoughness,
      ior: 1.49, // Acrylic.
      transparent: true,
      opacity: layer.opacity,
      depthWrite: false,
      side: DoubleSide,
      envMapIntensity: layer.envMapIntensity,
      emissive: patina.clone(),
      emissiveIntensity: 0,
    });

    /* Etched layers drive roughness from their own markings: the body stays
       optically smooth and the cut marks are matte, so they only declare
       themselves when light grazes across the plate. This is what makes Search
       read as etched glass rather than as a printed decal. */
    if (layer.etched) {
      capMaterial.roughnessMap = artTexture(layer.art, layer.artResolution || 512);
    }
    if (layer.sheen) {
      capMaterial.sheen = layer.sheen;
      capMaterial.sheenRoughness = layer.sheenRoughness;
      capMaterial.sheenColor = mineral.clone();
    }
    /* Brushed along one axis: the graphite layer answers light directionally. */
    if (layer.anisotropy) {
      capMaterial.anisotropy = layer.anisotropy;
      capMaterial.anisotropyRotation = Math.PI / 2;
    }

    const wallMaterial = new MeshPhysicalMaterial({
      color: new Color(layer.wall.colour),
      metalness: layer.wall.metalness,
      roughness: layer.wall.roughness,
      envMapIntensity: 1.35,
    });

    group.add(new Mesh(geometry.plates[index].body, [capMaterial, wallMaterial]));

    /* Edge treatment is per layer too: a bright machined arris on the precision
       surface, a soft one on the frosted polymer. */
    const arrisMaterial = new MeshBasicMaterial({
      color: mineral.clone(),
      transparent: true,
      opacity: layer.arris,
      depthWrite: false,
      fog: false,
    });
    group.add(new Mesh(geometry.plates[index].arris, arrisMaterial));

    const artMaterial = new MeshBasicMaterial({
      map: artTexture(layer.art, layer.artResolution || 512),
      transparent: true,
      opacity: layer.artOpacity,
      blending: AdditiveBlending,
      depthWrite: false,
      fog: false,
    });
    const art = new Mesh(geometry.plates[index].art, artMaterial);
    /* The surface composition sits on the top face because it is what you are
       meant to see. Every other drawing is embedded at mid-thickness, read
       through the material. */
    art.position.z = layer.artOnTop
      ? layer.thickness + 0.0012
      : layer.thickness * 0.45;
    group.add(art);

    assembly.add(group);
    return { group, capMaterial, wallMaterial, arrisMaterial, artMaterial, layer };
  });

  /* --- State ------------------------------------------------------------ */

  const widestAir = mode === 'surface' ? AIR_SURFACE_HINT : AIR_SEPARATED;
  const probe = new PerspectiveCamera(camera.fov, 1, camera.near, camera.far);
  /* One table per viewing angle, interpolated by focus, so raising the angle to
     examine a layer cannot push the specimen out of frame. */
  let fitTables = { base: [10, 10], examine: [10, 10] };
  let fitted = 10;

  const state = {
    air: mode === 'reconstruct' ? AIR_SEPARATED : AIR_ASSEMBLED,
    yaw: 0,
    pitch: 0,
    dolly: fitted,
    lookAt: 0,
    focus: 0, // How much a single layer is the subject, 0 to 1.
    emphasis: new Array(PLATES).fill(0),
  };
  const target = { ...state, emphasis: [...state.emphasis] };

  let running = false;
  let sized = false;

  function resize() {
    const parent = canvas.parentElement;
    const width = parent?.clientWidth || canvas.clientWidth;
    const height = parent?.clientHeight || canvas.clientHeight;
    if (!width || !height) return;
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.75));
    renderer.setSize(width, height, false);
    camera.aspect = width / height;
    camera.updateProjectionMatrix();

    probe.aspect = camera.aspect;
    fitTables = {
      base: buildFitTable(probe, widestAir, baseYaw, basePitch),
      examine: buildFitTable(probe, widestAir, baseYaw, examinePitch),
    };
    fitted = fitAt(fitTables.base, mode === 'reconstruct' ? 1 : 0);
    if (!sized) {
      state.dolly = fitted;
      target.dolly = fitted;
    }
    sized = true;
  }

  resize();
  const resizeObserver = new ResizeObserver(() => {
    resize();
    render();
  });
  if (canvas.parentElement) resizeObserver.observe(canvas.parentElement);

  function render() {
    if (!sized) resize();
    if (!sized) return;

    const centre = assemblyCentre(state.air);
    assembly.position.y = -centre;

    /* Layout, and the local relief that gives the layer under examination
       physical room. */
    let subjectY = 0;
    for (let i = 0; i < PLATES; i += 1) {
      const plate = plates[i];
      const base = plateBase(i, state.air);
      const emphasis = state.emphasis[i];
      let relief = 0;
      for (let j = 0; j < PLATES; j += 1) {
        if (j === i) continue;
        // Everything above the subject lifts, everything below settles.
        relief += state.emphasis[j] * (i < j ? 0.075 : -0.075);
      }
      plate.group.position.y = base + relief;
      if (emphasis > 0.5) subjectY = plate.group.position.y;

      /* Examining a layer works through light, not through paint. The subject
         is not made more opaque and it is not recoloured: a raking light
         crosses it to reveal its own finish, its reflectivity rises, its
         machined arris catches the environment, and its internal markings come
         up out of the material. Adjacent layers lose reflectivity and recede. */
      const { layer } = plate;
      plate.capMaterial.envMapIntensity = lerp(
        layer.envMapIntensity * lerp(1, 0.45, state.focus),
        layer.envMapIntensity * 2.1,
        emphasis
      );
      plate.capMaterial.opacity = layer.opacity * lerp(1, 0.84, state.focus - emphasis);
      // The accent still marks the selected layer, but only as a trace on the
      // edge — it is not how the layer is identified.
      plate.arrisMaterial.color.copy(mineral).lerp(patina, emphasis * 0.3);
      plate.arrisMaterial.opacity = lerp(
        layer.arris * lerp(1, 0.4, state.focus),
        Math.min(0.98, layer.arris + 0.42),
        emphasis
      );
      plate.artMaterial.opacity = lerp(
        layer.artOpacity * lerp(1, 0.5, state.focus),
        Math.min(0.96, layer.artOpacity + 0.42),
        emphasis
      );
      plate.wallMaterial.envMapIntensity = lerp(
        lerp(1.35, 0.7, state.focus),
        2.9,
        emphasis
      );
    }

    /* Two lights do the examining. A soft fill rides above the subject, and a
       raking light crosses it at a few degrees — which is how you read an
       etched, brushed or frosted surface at all. */
    examine.position.set(-0.9, subjectY + 0.5, 1.1);
    examine.intensity = state.focus * 1.7;

    graze.position.set(2.85, subjectY + 0.3, 1.5);
    grazeTarget.position.set(-0.4, subjectY, -0.2);
    graze.intensity = state.focus * 4.4;

    // The shadow softens and shrinks as the stack lifts off the substrate.
    const lift = clamp((state.air - AIR_ASSEMBLED) / (AIR_SEPARATED - AIR_ASSEMBLED));
    contactShadow.scale.setScalar(lerp(1, 0.82, lift));
    shadowMaterial.opacity = lerp(0.92, 0.3, lift);

    assembly.rotation.y = baseYaw + state.yaw;
    assembly.rotation.z = state.pitch;

    const angle = lerp(basePitch, examinePitch, state.focus);
    camera.position.set(0, state.dolly * angle, state.dolly);
    camera.lookAt(0, state.lookAt, 0);

    scene.fog.near = state.dolly * 0.45;
    scene.fog.far = state.dolly * 2.4;

    renderer.render(scene, camera);
  }

  function step() {
    // Heavy damping: the object has mass and never overshoots (doctrine §13).
    state.air = lerp(state.air, target.air, 0.06);
    state.yaw = lerp(state.yaw, target.yaw, 0.035);
    state.pitch = lerp(state.pitch, target.pitch, 0.035);
    state.dolly = lerp(state.dolly, target.dolly, 0.05);
    state.lookAt = lerp(state.lookAt, target.lookAt, 0.05);
    state.focus = lerp(state.focus, target.focus, 0.07);
    for (let i = 0; i < PLATES; i += 1) {
      state.emphasis[i] = lerp(state.emphasis[i], target.emphasis[i], 0.075);
    }
    render();
  }

  function isMoving() {
    if (!running) return false;
    if (Math.abs(state.air - target.air) > 0.0004) return true;
    if (Math.abs(state.yaw - target.yaw) > 0.0004) return true;
    if (Math.abs(state.pitch - target.pitch) > 0.0004) return true;
    if (Math.abs(state.dolly - target.dolly) > 0.002) return true;
    if (Math.abs(state.lookAt - target.lookAt) > 0.002) return true;
    if (Math.abs(state.focus - target.focus) > 0.004) return true;
    for (let i = 0; i < PLATES; i += 1) {
      if (Math.abs(state.emphasis[i] - target.emphasis[i]) > 0.004) return true;
    }
    return false;
  }

  return {
    /**
     * @param {{progress:number, pointer:{x:number,y:number}, activeIndex:number}} input
     *   activeIndex is the document's layer index: 0 Performance … 5 Design.
     */
    update({ progress = 0, pointer = { x: 0, y: 0 }, activeIndex = -1 } = {}) {
      const p = clamp(progress);
      const separation = mode === 'reconstruct' ? 1 - p : p;

      const subject = NARRATIVE_TO_INDEX.get(activeIndex);
      const examining = subject != null;
      target.focus = examining ? 1 : 0;

      /* Eased rather than linear, and floored while a layer is under
         examination. The first chapter is reached at the very top of the act,
         where a linear mapping leaves the object still shut — and a layer
         nobody can see is no use to the person reading about it. */
      const widest = mode === 'surface' ? AIR_SURFACE_HINT : AIR_SEPARATED;
      let air = lerp(AIR_ASSEMBLED, widest, (mode === 'surface' ? p : separation) ** 0.62);
      if (examining) air = Math.max(air, AIR_ASSEMBLED + (widest - AIR_ASSEMBLED) * 0.46);
      target.air = air;

      /* Framing follows the air the object actually has, not the scroll. */
      const opening = clamp((air - AIR_ASSEMBLED) / (widest - AIR_ASSEMBLED));

      for (let i = 0; i < PLATES; i += 1) {
        target.emphasis[i] = examining && i === subject ? 1 : 0;
      }

      /* The camera withdraws only as far as the opening object requires, then
         closes in and raises its aim to the layer under examination. */
      fitted = lerp(
        fitAt(fitTables.base, opening),
        fitAt(fitTables.examine, opening),
        examining ? 1 : 0
      );
      target.dolly = fitted * (examining ? 0.94 : 1);
      target.lookAt = examining
        ? plateBase(subject, target.air) - assemblyCentre(target.air)
        : 0;

      /* Pointer parallax below the threshold of obvious cause and effect. */
      target.yaw = pointer.x * 0.05;
      target.pitch = pointer.y * 0.022;

      if (running) step();
      else render();
    },

    isMoving,

    setActive(active) {
      running = Boolean(active);
      if (running) step();
    },

    dispose() {
      // Geometry and source canvases are shared and intentionally kept.
      resizeObserver.disconnect();
      environment.dispose();
      for (const material of [planedMaterial, hewnMaterial, seatMaterial]) {
        material.map?.dispose();
        material.normalMap?.dispose();
        material.dispose();
      }
      seatArrisMaterial.dispose();
      shadowMaterial.map?.dispose();
      shadowMaterial.dispose();
      for (const plate of plates) {
        plate.capMaterial.dispose();
        plate.wallMaterial.dispose();
        plate.arrisMaterial.dispose();
        plate.artMaterial.map?.dispose();
        plate.artMaterial.dispose();
      }
      renderer.dispose();
    },
  };
}

export const LAYER_STACK = STACK.map((layer) => layer.key);
export const SUBSTRATE_THICKNESS = FOUNDATION_T;
export const TOTAL_PLATE_SOLID = TOTAL_SOLID;
