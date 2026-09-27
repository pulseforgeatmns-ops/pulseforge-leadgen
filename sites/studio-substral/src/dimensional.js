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
  IcosahedronGeometry,
  LinearFilter,
  LinearMipmapLinearFilter,
  Mesh,
  MeshBasicMaterial,
  MeshPhysicalMaterial,
  Object3D,
  PMREMGenerator,
  PCFShadowMap,
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
const STONE = 0x2a2721;

const PLATE_W = 3.05;
const PLATE_H = 2.25;
const CORNER = 0.05; // A machined relief, not a rounded-rectangle style choice.

/** Air between plates: none when assembled, 0.52 when fully apart. */
const AIR_ASSEMBLED = 0.004;
const AIR_SEPARATED = 0.52;
/** Act I only opens the seams far enough to suggest the object comes apart. */
const AIR_SURFACE_HINT = 0.1;

/* The substrate is a chunk of stone, not a block and certainly not a plate. It
   is displaced geometry with fracture planes cut through it, so its thickness
   varies across its extent and none of its walls are vertical. Dimensions here
   are the pre-displacement slab it starts from; the real extent is measured off
   the built geometry. */
const SUBSTRATE_W = PLATE_W * 2.02;
const SUBSTRATE_D = PLATE_H * 2.02;
const SUBSTRATE_T = 1.5;
/** Air between the planed plateau and the deepest layer. */
const FOUNDATION_CLEARANCE = 0.17;
/* How far the camera's aim may travel toward the layer under examination, as a
   fraction of the distance from the object's centre to its top. The fit table
   reserves room for it, otherwise raising the aim pushes the substrate out of
   the bottom of the frame — which is exactly what it did. */
const AIM_TRAVEL = 0.22;

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
    transmission: 0.14,
    volume: 0.4,
    attenuation: 1.5,
    ior: 1.52,
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
    thickness: 0.112,
    tint: 0.145,
    warmth: 0.55,
    opacity: 0.44,
    roughness: 0.17,
    clearcoat: 0.92,
    clearcoatRoughness: 0.14,
    metalness: 0.09,
    envMapIntensity: 1.95,
    wall: { colour: 0xd2c8b2, roughness: 0.34, metalness: 0.88 },
    arris: 0.56,
    transmission: 0.52,
    volume: 1.7,
    attenuation: 1.15,
    ior: 1.5,
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
    transmission: 0.93,
    volume: 0.28,
    attenuation: 4.5,
    ior: 1.52,
    art: 'search',
    artOpacity: 0.44,
  },
  {
    /* 03 CONVERSION — smoked acrylic. Dark with real optical depth, polished
       edges, controlled internal reflection. Sparse bright nodes. */
    key: 'conversion',
    narrative: 2,
    thickness: 0.078,
    tint: 0.03,
    opacity: 0.68,
    roughness: 0.05,
    clearcoat: 1,
    clearcoatRoughness: 0.035,
    metalness: 0.06,
    envMapIntensity: 1.75,
    wall: { colour: 0xa09a8b, roughness: 0.09, metalness: 0.94 },
    arris: 0.44,
    transmission: 0.46,
    volume: 2.7,
    attenuation: 0.52,
    ior: 1.49,
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
    transmission: 0.66,
    volume: 1.1,
    attenuation: 2.2,
    ior: 1.46,
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
    transmission: 0,
    volume: 0,
    attenuation: 1,
    ior: 1.49,
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
  bar(ctx, m, heroY, col * 6.2, h * 0.062, 0.78);
  bar(ctx, m, heroY + h * 0.085, col * 4.5, h * 0.062, 0.78);
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

/* ==========================================================================
   THE SUBSTRATE

   An extruded polygon can never read as stone: however it is textured, its
   silhouette is a constant-thickness prism with vertical walls, and the eye
   reads that as a manufactured plate. So this is displaced geometry instead.

   A subdivided icosahedron is squashed into a slab, pushed around by several
   octaves of value noise so its thickness varies across its extent and no wall
   is vertical, then cut by a handful of arbitrary planes which leave flat
   fracture facets. Flat-shaded, so every facet answers light on its own — the
   macro structure is cleavage, and the normal map supplies the grain on top.

   A shallow region of the top is planed flat where the engineered stack seats
   into it. Natural stone stays dominant everywhere else.
   ========================================================================== */

/** Deterministic 3D value noise. No tables, same rock on every load. */
function hash3(x, y, z) {
  const n = Math.sin(x * 127.1 + y * 311.7 + z * 74.7) * 43758.5453123;
  return n - Math.floor(n);
}

function valueNoise3(x, y, z) {
  const xi = Math.floor(x);
  const yi = Math.floor(y);
  const zi = Math.floor(z);
  const xf = x - xi;
  const yf = y - yi;
  const zf = z - zi;
  const u = xf * xf * (3 - 2 * xf);
  const v = yf * yf * (3 - 2 * yf);
  const w = zf * zf * (3 - 2 * zf);
  const corner = (dx, dy, dz) => hash3(xi + dx, yi + dy, zi + dz);
  const x00 = corner(0, 0, 0) + (corner(1, 0, 0) - corner(0, 0, 0)) * u;
  const x10 = corner(0, 1, 0) + (corner(1, 1, 0) - corner(0, 1, 0)) * u;
  const x01 = corner(0, 0, 1) + (corner(1, 0, 1) - corner(0, 0, 1)) * u;
  const x11 = corner(0, 1, 1) + (corner(1, 1, 1) - corner(0, 1, 1)) * u;
  const y0 = x00 + (x10 - x00) * v;
  const y1 = x01 + (x11 - x01) * v;
  return y0 + (y1 - y0) * w;
}

function fbm3(x, y, z, octaves = 4) {
  let amplitude = 1;
  let frequency = 1;
  let sum = 0;
  let norm = 0;
  for (let i = 0; i < octaves; i += 1) {
    sum += amplitude * valueNoise3(x * frequency, y * frequency, z * frequency);
    norm += amplitude;
    amplitude *= 0.5;
    frequency *= 2.07;
  }
  return sum / norm;
}

const smoothstep = (edge0, edge1, x) => {
  const t = clamp((x - edge0) / (edge1 - edge0));
  return t * t * (3 - 2 * t);
};

/* Cleavage planes. Anything beyond one is projected onto it, leaving a flat
   fracture face. Two near-horizontal planes cut the block flat top and bottom so
   it is a slab rather than the lens the displaced sphere would otherwise be; the
   rest come in around the sides at shallow angles, which is what gives the
   angular, quarried silhouette. Deterministic, so it is the same rock every
   load. */
const CLEAVAGE = (() => {
  const planes = [
    { n: [0.06, 0.99, -0.11], d: 0.6 },
    { n: [-0.1, -0.98, 0.17], d: 0.64 },
  ];
  const sides = 13;
  for (let i = 0; i < sides; i += 1) {
    const theta = i * 2.399963229728653;
    const tilt = (hash3(i * 5.3, 2.1, 8.7) - 0.5) * 0.5;
    let nx = Math.cos(theta);
    let ny = tilt;
    let nz = Math.sin(theta);
    const length = Math.hypot(nx, ny, nz) || 1;
    nx /= length;
    ny /= length;
    nz /= length;
    planes.push({ n: [nx, ny, nz], d: 0.7 + hash3(i * 3.7, 11.3, 5.1) * 0.2 });
  }
  return planes;
})();

function buildSubstrate() {
  const halfW = SUBSTRATE_W / 2;
  const halfD = SUBSTRATE_D / 2;
  const halfT = SUBSTRATE_T / 2;

  const source = new IcosahedronGeometry(1, 3);
  const position = source.attributes.position;
  const count = position.count;
  const points = [];

  for (let i = 0; i < count; i += 1) {
    const ux = position.getX(i);
    const uy = position.getY(i);
    const uz = position.getZ(i);

    /* Two octave sets: the first breaks the overall mass, the second roughens
       it. Sampled on the unit sphere so the field is continuous across seams. */
    const broad = fbm3(ux * 1.25 + 4.1, uy * 1.25 + 1.7, uz * 1.25 + 9.3, 4) - 0.5;
    const rough = fbm3(ux * 3.4 + 21.3, uy * 3.4 + 5.9, uz * 3.4 + 13.1, 3) - 0.5;
    const swell = 1 + broad * 0.38 + rough * 0.34;

    let x = ux * halfW * swell;
    let y = uy * halfT * swell;
    let z = uz * halfD * swell;

    // Thickness varies independently, so the profile is never a constant slab.
    y *= 0.74 + (fbm3(ux * 1.9 + 31, 0.5, uz * 1.9 + 17, 3) - 0.5) * 1.15;

    for (const plane of CLEAVAGE) {
      const [nx, ny, nz] = plane.n;
      // Planes are defined against the normalised slab so they cut evenly.
      const px = x / halfW;
      const py = y / halfT;
      const pz = z / halfD;
      const distance = px * nx + py * ny + pz * nz;
      if (distance > plane.d) {
        const over = distance - plane.d;
        x -= nx * over * halfW;
        y -= ny * over * halfT;
        z -= nz * over * halfD;
      }
    }

    points.push([x, y, z]);
  }

  /* The planed interface: a shallow plateau across the middle of the top, which
     fades out into natural stone well before the perimeter. */
  const plateau = halfT * 0.74;
  for (const p of points) {
    if (p[1] <= plateau * 0.35) continue;
    const radial = Math.hypot(p[0] / halfW, p[2] / halfD);
    const worked = 1 - smoothstep(0.34, 0.72, radial);
    p[1] = lerp(p[1], Math.min(p[1], plateau), worked);
  }

  const flat = new Float32Array(count * 3);
  for (let i = 0; i < count; i += 1) {
    flat[i * 3] = points[i][0];
    flat[i * 3 + 1] = points[i][1];
    flat[i * 3 + 2] = points[i][2];
  }
  source.setAttribute('position', new BufferAttribute(flat, 3));

  /* IcosahedronGeometry is already non-indexed, so every triangle owns its
     vertices and computeVertexNormals yields per-face normals — which is what
     gives the fracture facets rather than a smooth lump. */
  const geometry = source;
  geometry.computeVertexNormals();

  const finalPosition = geometry.attributes.position;
  const normal = geometry.attributes.normal;
  const triangles = finalPosition.count / 3;
  const uv = new Float32Array(finalPosition.count * 2);
  const colour = new Float32Array(finalPosition.count * 3);
  const bounds = {
    minX: Infinity, maxX: -Infinity,
    minY: Infinity, maxY: -Infinity,
    minZ: Infinity, maxZ: -Infinity,
  };

  const stone = new Color(STONE);
  const vein = new Color(0xa9a293);

  for (let t = 0; t < triangles; t += 1) {
    const base = t * 3;
    /* Box projection from the face's dominant axis: the icosahedron's own UVs
       are useless after displacement, and this gives the grain an even scale on
       every facet with no stretching. */
    const nx = Math.abs(normal.getX(base));
    const ny = Math.abs(normal.getY(base));
    const nz = Math.abs(normal.getZ(base));
    const axis = ny > nx && ny > nz ? 1 : nx > nz ? 0 : 2;

    for (let v = 0; v < 3; v += 1) {
      const i = base + v;
      const x = finalPosition.getX(i);
      const y = finalPosition.getY(i);
      const z = finalPosition.getZ(i);

      const scale = 0.55;
      if (axis === 1) {
        uv[i * 2] = x * scale;
        uv[i * 2 + 1] = z * scale;
      } else if (axis === 0) {
        uv[i * 2] = z * scale;
        uv[i * 2 + 1] = y * scale;
      } else {
        uv[i * 2] = x * scale;
        uv[i * 2 + 1] = y * scale;
      }

      /* Mineral variation and crevice darkening baked per vertex, so the grain
         does not depend on a UV-mapped albedo surviving the displacement. */
      const grain = fbm3(x * 2.6, y * 2.6, z * 2.6, 4);
      const band = fbm3(x * 0.9 + 60, y * 0.9, z * 0.9 + 12, 2);
      let value = 1.0 + (grain - 0.5) * 1.7 + (band - 0.5) * 0.8;
      value = clamp(value, 0.28, 2.4);
      const quartz = smoothstep(0.78, 0.9, fbm3(x * 5.1 + 3, y * 5.1, z * 5.1, 2));
      const c = stone.clone().multiplyScalar(value).lerp(vein, quartz * 0.35);
      colour[i * 3] = c.r;
      colour[i * 3 + 1] = c.g;
      colour[i * 3 + 2] = c.b;

      if (x < bounds.minX) bounds.minX = x;
      if (x > bounds.maxX) bounds.maxX = x;
      if (y < bounds.minY) bounds.minY = y;
      if (y > bounds.maxY) bounds.maxY = y;
      if (z < bounds.minZ) bounds.minZ = z;
      if (z > bounds.maxZ) bounds.maxZ = z;
    }
  }

  /* A decimated copy of the surface, for framing. A bounding box is hopeless
     here: the block is five units wide and one thick, and its extreme corners in
     plan sit at mid-height, so a box reserves a great deal of vertical space
     nothing occupies and the specimen ends up floating small in its frame. */
  const hull = [];
  for (let i = 0; i < finalPosition.count; i += 2) {
    hull.push([finalPosition.getX(i), finalPosition.getY(i), finalPosition.getZ(i)]);
  }

  geometry.setAttribute('uv', new BufferAttribute(uv, 2));
  geometry.setAttribute('color', new BufferAttribute(colour, 3));
  geometry.computeBoundingSphere();

  return { geometry, bounds, hull, plateau, relief: bounds.maxY - plateau };
}

/**
 * A normal map for the substrate's broken faces. Multi-octave value noise
 * turned into surface normals: the geometry supplies the cleavage, this
 * supplies the grain.
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

/** Where the block sits: its highest point lands on the clearance line. */
function substrateDrop() {
  return -FOUNDATION_CLEARANCE - sharedGeometry().substrate.bounds.maxY;
}

function assemblyCorners(air, yaw, centre) {
  const cos = Math.cos(yaw);
  const sin = Math.sin(yaw);
  const rock = sharedGeometry().substrate;
  const drop = substrateDrop();
  const top = plateBase(0, air) + STACK[0].thickness;

  const points = [];
  const add = (x, y, z) => {
    points.push(new Vector3(x * cos + z * sin, y - centre, -x * sin + z * cos));
  };

  // The plate stack, which really is a box.
  for (const sx of [-1, 1]) {
    for (const sz of [-1, 1]) {
      for (const y of [plateBase(PLATES - 1, air), top]) {
        add((sx * PLATE_W) / 2, y, (sz * PLATE_H) / 2);
      }
    }
  }
  // And the block's actual surface.
  for (const p of rock.hull) add(p[0], p[1] + drop, p[2]);

  return points;
}

/** How far the camera's aim may travel, in world units, for this air gap. */
function aimReach(air) {
  const top = plateBase(0, air) + STACK[0].thickness;
  return (top - assemblyCentre(air)) * AIM_TRAVEL;
}

/** Vertical centre of the whole specimen, substrate included. */
function assemblyCentre(air) {
  const rock = sharedGeometry().substrate;
  const top = plateBase(0, air) + STACK[0].thickness;
  const bottom = substrateDrop() + rock.bounds.minY;
  return (top + bottom) / 2;
}

/* xLimit above 1 lets the object run past the left and right edges of the
   frame. The hero wants that: the specimen is a wide flat slab, so fitting it
   on width leaves the frame half empty and pushes the substrate down behind the
   statement. Cropping the far tips instead makes it read as larger than the
   composition can hold — which is the relationship the reference has. */
function fitsAt(probe, corners, distance, pitch, xLimit = 0.94, aim = 0) {
  probe.position.set(0, distance * pitch, distance);
  probe.lookAt(0, 0, 0);
  probe.updateMatrixWorld(true);
  probe.updateProjectionMatrix();
  /* Reserve the aim's travel where it applies — on screen — rather than by
     inflating the object, which over-reserves badly for a wide flat block. */
  const tanHalf = Math.tan((probe.fov * Math.PI) / 360);
  const yLimit = 0.94 - Math.min(0.45, aim / (distance * tanHalf));
  for (const corner of corners) {
    const ndc = corner.clone().project(probe);
    if (Math.abs(ndc.x) > xLimit || Math.abs(ndc.y) > yLimit) return false;
    if (ndc.z > 1) return false;
  }
  return true;
}

function frameDistance(probe, air, yaw, pitch, xLimit) {
  const corners = assemblyCorners(air, yaw, assemblyCentre(air));
  const aim = aimReach(air);
  let low = 2;
  let high = 70;
  if (!fitsAt(probe, corners, high, pitch, xLimit, aim)) return high;
  for (let i = 0; i < 22; i += 1) {
    const mid = (low + high) / 2;
    if (fitsAt(probe, corners, mid, pitch, xLimit, aim)) high = mid;
    else low = mid;
  }
  return high;
}

const FIT_SAMPLES = 7;

function buildFitTable(probe, widestAir, yaw, pitch, xLimit) {
  const table = [];
  for (let i = 0; i < FIT_SAMPLES; i += 1) {
    const t = i / (FIT_SAMPLES - 1);
    table.push(
      frameDistance(probe, lerp(AIR_ASSEMBLED, widestAir, t), yaw, pitch, xLimit)
    );
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

  const substrate = buildSubstrate();

  shared = {
    plates,
    substrate,
    shadow: new PlaneGeometry(PLATE_W * 1.35, PLATE_H * 1.35),
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
  renderer.shadowMap.enabled = true;
  renderer.shadowMap.type = PCFShadowMap;
  renderer.toneMapping = ACESFilmicToneMapping;
  renderer.toneMappingExposure = 1.12;
  renderer.outputColorSpace = SRGBColorSpace;

  /* Act I looks down onto the specimen so the surface reads as a page. Act II
     and VI sit lower and more architectural, but still high enough that each
     layer's own drawing is legible rather than foreshortened into a line.
     Examining a layer raises the angle further: the subject turns its face up. */
  const basePitch = mode === 'surface' ? 0.35 : mode === 'reconstruct' ? 0.42 : 0.46;
  const examinePitch = basePitch + 0.1;
  const baseYaw = mode === 'surface' ? -0.36 : -0.46;
  /* The hero crops; the narrow sticky columns do not, where a cut edge would
     read as broken rather than as framing. */
  const frameCrop = mode === 'surface' ? 1.3 : 1.16;

  const scene = new Scene();
  const camera = new PerspectiveCamera(24, 1, 0.1, 140);

  /* Depth falloff: the far side of the specimen recedes into the environment
     instead of staying uniformly lit to the edge of the frame. */
  scene.fog = new Fog(0x0d0d0b, 6, 20);

  const environment = studioEnvironment(renderer);
  scene.environment = environment;

  /* Directional and restrained. One key, one cool fill, one low back light to
     catch the machined arrises. No coloured practicals, no rim theatrics. */
  const key = new DirectionalLight(0xfff4e2, 2.5);
  key.position.set(-3.6, 7.4, 3.2);
  key.castShadow = true;
  key.shadow.mapSize.set(1024, 1024);
  key.shadow.camera.left = -3.8;
  key.shadow.camera.right = 3.8;
  key.shadow.camera.top = 3.8;
  key.shadow.camera.bottom = -3.8;
  key.shadow.camera.near = 0.5;
  key.shadow.camera.far = 22;
  key.shadow.bias = -0.0016;
  key.shadow.normalBias = 0.012;
  scene.add(key);

  const fill = new DirectionalLight(0xa8c0b4, 0.62);
  fill.position.set(4.8, 1.1, -3.4);
  scene.add(fill);

  const back = new DirectionalLight(0xf0ede5, 0.8);
  back.position.set(1.4, -1.2, -4.6);
  scene.add(back);

  /* A low bounce from the front, so the substrate's near faces carry some
     detail instead of falling to black. */
  const bounce = new DirectionalLight(0xe8dcc6, 0.5);
  bounce.position.set(-1.2, -2.6, 4.2);
  scene.add(bounce);

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

  /* The substrate: one chunk of stone. Flat-shaded displaced geometry carries
     the cleavage, the normal map carries the grain, vertex colours carry the
     mineral variation, and a real shadow map carries the self-shadowing in its
     crevices. No second material and no seat plate — the planed plateau is cut
     into the stone itself, so there is nothing left here that could be mistaken
     for another pane. */
  const rock = geometry.substrate;
  const grain = stoneNormalTexture();

  const stoneMaterial = new MeshPhysicalMaterial({
    color: 0xffffff,
    vertexColors: true,
    normalMap: grain,
    roughness: 0.96,
    metalness: 0.03,
    envMapIntensity: 0.5,
    flatShading: true,
  });
  stoneMaterial.normalScale.set(1.15, 1.15);

  const substrate = new Mesh(rock.geometry, stoneMaterial);
  substrate.position.y = substrateDrop();
  substrate.castShadow = true;
  substrate.receiveShadow = true;
  assembly.add(substrate);

  const shadowMaterial = new MeshBasicMaterial({
    map: shadowTexture(),
    transparent: true,
    depthWrite: false,
    fog: false,
  });
  const contactShadow = new Mesh(geometry.shadow, shadowMaterial);
  contactShadow.rotation.x = -Math.PI / 2;
  contactShadow.position.y = substrateDrop() + rock.plateau + 0.008;
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
      ior: layer.ior,
      transparent: true,
      opacity: layer.opacity,
      depthWrite: false,
      side: DoubleSide,
      envMapIntensity: layer.envMapIntensity,
      emissive: patina.clone(),
      emissiveIntensity: 0,
    });

    /* Transmission gives these layers real refraction and real volumetric
       absorption, so Search reads clear, Conversion reads deep, Trust reads
       warm and thick, and Accessibility scatters instead of merely being
       semi-opaque. Attenuation distance is what makes thickness mean something:
       the same tint over a longer path arrives darker. */
    if (layer.transmission > 0) {
      capMaterial.transmission = layer.transmission;
      capMaterial.thickness = layer.volume;
      capMaterial.attenuationDistance = layer.attenuation;
      capMaterial.attenuationColor = body.clone();
      capMaterial.opacity = 1;
    }

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
      base: buildFitTable(probe, widestAir, baseYaw, basePitch, frameCrop),
      examine: buildFitTable(probe, widestAir, baseYaw, examinePitch, frameCrop),
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
        Math.min(0.88, layer.artOpacity + 0.36),
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
      target.dolly = fitted * (examining ? 0.97 : 1);
      target.lookAt = examining
        ? (plateBase(subject, target.air) - assemblyCentre(target.air)) * AIM_TRAVEL
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
      stoneMaterial.normalMap?.dispose();
      stoneMaterial.dispose();
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
export const SUBSTRATE_THICKNESS = SUBSTRATE_T;
export const TOTAL_PLATE_SOLID = TOTAL_SOLID;
