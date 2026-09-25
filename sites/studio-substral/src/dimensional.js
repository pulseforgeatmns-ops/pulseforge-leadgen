/* ==========================================================================
   Studio Substral — the dimensional object.

   An abstract website rendered as an engineered specimen: six machined plates
   in smoked acrylic, stacked along depth, with thin aluminium rims. Not a
   laptop, not a screenshot in space, not a stack of UI cards (doctrine §11).

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
  Group,
  LineBasicMaterial,
  LineSegments,
  Mesh,
  MeshBasicMaterial,
  MeshPhysicalMaterial,
  PMREMGenerator,
  PerspectiveCamera,
  PlaneGeometry,
  Scene,
  Shape,
  SRGBColorSpace,
  Vector3,
  WebGLRenderer,
} from 'three';

/* Materials, tuned to the palette. Values are in linear-friendly hex; the
   renderer's tone mapping and colour space do the rest. */
const SUBSTRAL_BLACK = 0x11110f;
const MINERAL = 0xf0ede5;
const PATINA = 0x7fa890;

const LAYERS = 6;
const PLATE_W = 3.05;
const PLATE_H = 2.25;
const PLATE_T = 0.055;
const CORNER = 0.045; // Machined chamfer, not a rounded-rectangle style choice.

const GAP_ASSEMBLED = 0.052;
const GAP_SEPARATED = 0.6;

const ASSEMBLY_YAW = -0.44;
/** Camera height as a fraction of its distance. Fixes the viewing angle. */
const VIEW_PITCH = 0.29;
/** Fraction of the frame the object may occupy before it is considered clipped. */
const SAFE_FRAME = 0.93;

const lerp = (a, b, t) => a + (b - a) * t;
const clamp = (n, min = 0, max = 1) => (n < min ? min : n > max ? max : n);

/* --------------------------------------------------------------------------
   Plate silhouette: a rectangle with a small machined corner relief.
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

/* The rim, built from the silhouette rather than from EdgesGeometry, so the
   plate carries exactly two clean outlines and no tessellation noise. */
function rimGeometry(shape, thickness) {
  const points = shape.getPoints(10);
  if (points.length && points[0].equals(points[points.length - 1])) points.pop();

  const positions = [];
  const pushLoop = (z) => {
    for (let i = 0; i < points.length; i += 1) {
      const a = points[i];
      const b = points[(i + 1) % points.length];
      positions.push(a.x, a.y, z, b.x, b.y, z);
    }
  };
  pushLoop(0);
  pushLoop(thickness);

  const geometry = new BufferGeometry();
  geometry.setAttribute('position', new BufferAttribute(new Float32Array(positions), 3));
  return geometry;
}

/* --------------------------------------------------------------------------
   Etched graticule. Each plate carries a different measurement pattern, so
   the layers are distinguishable as objects rather than as six copies.

   The source canvases are built once and shared: three stages draw the same
   six patterns, and there is no reason to rasterise them three times.
   -------------------------------------------------------------------------- */

const graticuleCanvases = new Map();

function graticuleCanvas(index) {
  if (!graticuleCanvases.has(index)) {
    graticuleCanvases.set(index, drawGraticule(index));
  }
  return graticuleCanvases.get(index);
}

function graticuleTexture(index) {
  const texture = new CanvasTexture(graticuleCanvas(index));
  texture.colorSpace = SRGBColorSpace;
  texture.anisotropy = 4;
  return texture;
}

function drawGraticule(index) {
  const size = 512;
  const canvas = document.createElement('canvas');
  canvas.width = size;
  canvas.height = Math.round(size * (PLATE_H / PLATE_W));
  const ctx = canvas.getContext('2d');
  const w = canvas.width;
  const h = canvas.height;

  ctx.fillStyle = '#000';
  ctx.fillRect(0, 0, w, h);

  const cols = 5 + index * 2;
  const rows = 3 + index;
  ctx.strokeStyle = 'rgba(240,237,229,0.30)';
  ctx.lineWidth = 1;

  for (let c = 1; c < cols; c += 1) {
    const x = Math.round((w / cols) * c) + 0.5;
    ctx.beginPath();
    ctx.moveTo(x, h * 0.08);
    ctx.lineTo(x, h * 0.92);
    ctx.stroke();
  }
  for (let r = 1; r < rows; r += 1) {
    const y = Math.round((h / rows) * r) + 0.5;
    ctx.beginPath();
    ctx.moveTo(w * 0.06, y);
    ctx.lineTo(w * 0.94, y);
    ctx.stroke();
  }

  // Baseline ruling with ticks along the lower edge.
  ctx.strokeStyle = 'rgba(240,237,229,0.55)';
  ctx.beginPath();
  ctx.moveTo(w * 0.06, h * 0.95);
  ctx.lineTo(w * 0.94, h * 0.95);
  ctx.stroke();
  for (let t = 0; t <= 24; t += 1) {
    const x = Math.round(w * 0.06 + ((w * 0.88) / 24) * t) + 0.5;
    const len = t % 6 === 0 ? h * 0.05 : h * 0.025;
    ctx.beginPath();
    ctx.moveTo(x, h * 0.95);
    ctx.lineTo(x, h * 0.95 - len);
    ctx.stroke();
  }

  // Index marker: position encodes which layer this is.
  ctx.fillStyle = 'rgba(127,168,144,0.85)';
  ctx.fillRect(Math.round(w * (0.08 + index * 0.145)), Math.round(h * 0.055), 26, 5);

  return canvas;
}

/* --------------------------------------------------------------------------
   Framing.

   The three stages occupy very differently shaped boxes — a wide hero band, a
   tall sticky column, another tall column — and the specimen is a broad flat
   slab, so a single hand-tuned camera distance clips it in at least one of
   them. Instead, solve for the distance at which the fully separated assembly
   fits inside a safe frame, and let each stage dolly within that.
   -------------------------------------------------------------------------- */

function assemblyCorners(gap) {
  const halfStack = ((LAYERS - 1) / 2) * gap + PLATE_T;
  const cos = Math.cos(ASSEMBLY_YAW);
  const sin = Math.sin(ASSEMBLY_YAW);
  const corners = [];
  for (const sx of [-1, 1]) {
    for (const sy of [-1, 1]) {
      for (const sz of [-1, 1]) {
        const x = (sx * PLATE_W) / 2;
        const z = (sz * PLATE_H) / 2;
        corners.push(
          new Vector3(x * cos + z * sin, sy * halfStack, -x * sin + z * cos)
        );
      }
    }
  }
  return corners;
}

function fitsAt(probe, corners, distance) {
  probe.position.set(0, distance * VIEW_PITCH, distance);
  probe.lookAt(0, -0.1, 0);
  probe.updateMatrixWorld(true);
  probe.updateProjectionMatrix();
  for (const corner of corners) {
    const ndc = corner.clone().project(probe);
    if (Math.abs(ndc.x) > SAFE_FRAME || Math.abs(ndc.y) > SAFE_FRAME) return false;
    if (ndc.z > 1) return false;
  }
  return true;
}

/** Smallest distance at which the assembly at this gap is fully framed. */
function frameDistance(probe, gap) {
  const corners = assemblyCorners(gap);
  let low = 2;
  let high = 60;
  if (!fitsAt(probe, corners, high)) return high;
  for (let i = 0; i < 22; i += 1) {
    const mid = (low + high) / 2;
    if (fitsAt(probe, corners, mid)) high = mid;
    else low = mid;
  }
  return high;
}

const FIT_SAMPLES = 7;

/**
 * Fit distance sampled across the separation range, so the specimen fills its
 * frame whether whole or apart and the camera simply withdraws as it opens.
 * Solving per frame would cost far more than interpolating seven samples.
 */
function buildFitTable(probe, widestGap) {
  const table = [];
  for (let i = 0; i < FIT_SAMPLES; i += 1) {
    const t = i / (FIT_SAMPLES - 1);
    table.push(frameDistance(probe, lerp(GAP_ASSEMBLED, widestGap, t)));
  }
  return table;
}

function fitAt(table, t) {
  const position = clamp(t) * (table.length - 1);
  const index = Math.min(Math.floor(position), table.length - 2);
  return lerp(table[index], table[index + 1], position - index);
}

/* --------------------------------------------------------------------------
   Environment. A small procedural equirectangular gradient with two soft
   bands standing in for studio softboxes. Enough to give the acrylic and the
   aluminium something to reflect; no HDR asset to download.
   -------------------------------------------------------------------------- */

function studioEnvironment(renderer) {
  const canvas = document.createElement('canvas');
  canvas.width = 256;
  canvas.height = 128;
  const ctx = canvas.getContext('2d');

  const sky = ctx.createLinearGradient(0, 0, 0, canvas.height);
  sky.addColorStop(0, '#39362f');
  sky.addColorStop(0.45, '#1d1c19');
  sky.addColorStop(1, '#0b0b0a');
  ctx.fillStyle = sky;
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  const softbox = (x, y, w, h, alpha) => {
    const g = ctx.createRadialGradient(x, y, 0, x, y, Math.max(w, h));
    g.addColorStop(0, `rgba(255,250,240,${alpha})`);
    g.addColorStop(1, 'rgba(255,250,240,0)');
    ctx.fillStyle = g;
    ctx.fillRect(x - w, y - h, w * 2, h * 2);
  };
  softbox(64, 26, 54, 30, 0.95);
  softbox(190, 40, 40, 22, 0.42);

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
   Shared geometry. Three stages render the same specimen, and three.js keeps
   its GPU state per renderer, so the geometry itself is built once.
   -------------------------------------------------------------------------- */

let shared = null;

function sharedGeometry() {
  if (shared) return shared;
  const shape = plateShape(PLATE_W, PLATE_H, CORNER);
  shared = {
    plate: new ExtrudeGeometry(shape, {
      depth: PLATE_T,
      bevelEnabled: false,
      curveSegments: 4,
    }),
    rim: rimGeometry(shape, PLATE_T),
    etch: new PlaneGeometry(PLATE_W * 0.94, PLATE_H * 0.94),
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
  renderer.toneMappingExposure = 1.05;
  renderer.outputColorSpace = SRGBColorSpace;

  const scene = new Scene();
  const camera = new PerspectiveCamera(33, 1, 0.1, 100);

  const environment = studioEnvironment(renderer);
  scene.environment = environment;

  /* Restrained cinematic lighting: one key, one low fill. No rim theatrics,
     no coloured practicals (doctrine §11). */
  const key = new DirectionalLight(0xfff6e8, 2.1);
  key.position.set(-3.4, 6.2, 3.1);
  scene.add(key);

  const fill = new DirectionalLight(0xbcd2c6, 0.5);
  fill.position.set(4.2, 1.4, -3.6);
  scene.add(fill);

  /* Assembly */
  const assembly = new Group();
  assembly.rotation.y = -0.44;
  scene.add(assembly);

  const geometry = sharedGeometry();

  const plates = [];
  for (let i = 0; i < LAYERS; i += 1) {
    const group = new Group();
    // Plates lie horizontal: the extrusion axis becomes vertical thickness.
    group.rotation.x = -Math.PI / 2;

    const faceMaterial = new MeshPhysicalMaterial({
      color: new Color(SUBSTRAL_BLACK).lerp(new Color(MINERAL), 0.1),
      metalness: 0.08,
      roughness: 0.22,
      clearcoat: 1,
      clearcoatRoughness: 0.12,
      transparent: true,
      opacity: 0.46,
      depthWrite: false,
      side: DoubleSide,
      envMapIntensity: 1.6,
      emissive: new Color(PATINA),
      emissiveIntensity: 0,
    });

    const face = new Mesh(geometry.plate, faceMaterial);
    group.add(face);

    const rimMaterial = new LineBasicMaterial({
      color: new Color(MINERAL),
      transparent: true,
      opacity: 0.46,
      depthWrite: false,
    });
    group.add(new LineSegments(geometry.rim, rimMaterial));

    const etchMaterial = new MeshBasicMaterial({
      map: graticuleTexture(i),
      transparent: true,
      opacity: 0.2,
      blending: AdditiveBlending,
      depthWrite: false,
    });
    const etch = new Mesh(geometry.etch, etchMaterial);
    etch.position.z = PLATE_T + 0.0015;
    group.add(etch);

    assembly.add(group);
    plates.push({ group, faceMaterial, rimMaterial, etchMaterial });
  }

  /* --- State ------------------------------------------------------------ */

  /* The widest state this stage ever reaches is what has to stay framed. */
  const widestGap = mode === 'surface' ? 0.115 : GAP_SEPARATED;
  const probe = new PerspectiveCamera(camera.fov, 1, camera.near, camera.far);
  let fitTable = [8, 8];
  let fitted = 8;

  /* Act I opens on the whole object; Act VI opens on it separated. */
  const state = {
    gap: mode === 'reconstruct' ? GAP_SEPARATED : GAP_ASSEMBLED,
    yaw: 0,
    pitch: 0,
    dolly: fitted,
    height: fitted * VIEW_PITCH,
    active: -1,
    highlight: new Array(LAYERS).fill(0),
  };
  const target = { ...state, highlight: [...state.highlight] };

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
    fitTable = buildFitTable(probe, widestGap);
    fitted = fitAt(fitTable, mode === 'reconstruct' ? 1 : 0);
    if (!sized) {
      state.dolly = fitted;
      state.height = fitted * VIEW_PITCH;
      target.dolly = state.dolly;
      target.height = state.height;
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

    for (let i = 0; i < LAYERS; i += 1) {
      const { group, faceMaterial, rimMaterial, etchMaterial } = plates[i];
      group.position.y = ((LAYERS - 1) / 2 - i) * state.gap;

      const h = state.highlight[i];
      faceMaterial.emissiveIntensity = h * 0.16;
      faceMaterial.opacity = lerp(0.44, 0.56, h);
      rimMaterial.color.set(h > 0.02 ? new Color(MINERAL).lerp(new Color(PATINA), h) : MINERAL);
      rimMaterial.opacity = lerp(0.46, 0.95, h);
      etchMaterial.opacity = lerp(0.2, 0.46, h);
    }

    assembly.rotation.y = -0.44 + state.yaw;
    assembly.rotation.z = state.pitch;

    camera.position.set(0, state.height, state.dolly);
    camera.lookAt(0, -0.1, 0);

    renderer.render(scene, camera);
  }

  function step() {
    // Heavy damping: the object has mass and never overshoots (doctrine §13).
    state.gap = lerp(state.gap, target.gap, 0.06);
    state.yaw = lerp(state.yaw, target.yaw, 0.035);
    state.pitch = lerp(state.pitch, target.pitch, 0.035);
    state.dolly = lerp(state.dolly, target.dolly, 0.05);
    state.height = lerp(state.height, target.height, 0.05);
    for (let i = 0; i < LAYERS; i += 1) {
      state.highlight[i] = lerp(state.highlight[i], target.highlight[i], 0.08);
    }
    render();
  }

  function isMoving() {
    if (!running) return false;
    if (Math.abs(state.gap - target.gap) > 0.0004) return true;
    if (Math.abs(state.yaw - target.yaw) > 0.0004) return true;
    if (Math.abs(state.pitch - target.pitch) > 0.0004) return true;
    if (Math.abs(state.dolly - target.dolly) > 0.002) return true;
    if (Math.abs(state.height - target.height) > 0.002) return true;
    for (let i = 0; i < LAYERS; i += 1) {
      if (Math.abs(state.highlight[i] - target.highlight[i]) > 0.004) return true;
    }
    return false;
  }

  return {
    /**
     * @param {{progress:number, pointer:{x:number,y:number}, activeIndex:number}} input
     */
    update({ progress = 0, pointer = { x: 0, y: 0 }, activeIndex = -1 } = {}) {
      const p = clamp(progress);
      const separation = mode === 'reconstruct' ? 1 - p : p;

      /* Act I holds the specimen assembled and only lets the seams open far
         enough to suggest that it comes apart. */
      target.gap =
        mode === 'surface'
          ? lerp(GAP_ASSEMBLED, 0.115, p)
          : lerp(GAP_ASSEMBLED, GAP_SEPARATED, separation);

      /* The camera withdraws only as far as the opening object requires, so the
         specimen stays framed at every separation and the movement reads as a
         consequence of the object rather than a travelling shot (§14, Act II).
         A little elevation comes with it, and nothing else moves. */
      fitted = fitAt(fitTable, separation);
      target.dolly = fitted;
      target.height = fitted * VIEW_PITCH * lerp(0.96, 1.1, separation);

      /* Pointer parallax below the threshold of obvious cause and effect. */
      target.yaw = pointer.x * 0.05;
      target.pitch = pointer.y * 0.022;

      for (let i = 0; i < LAYERS; i += 1) {
        target.highlight[i] = i === activeIndex && separation > 0.08 ? 1 : 0;
      }

      if (running) step();
      else render();
    },

    isMoving,

    setActive(active) {
      running = Boolean(active);
      if (running) step();
    },

    dispose() {
      // Geometry is shared across stages and intentionally not disposed here.
      resizeObserver.disconnect();
      environment.dispose();
      for (const plate of plates) {
        plate.faceMaterial.dispose();
        plate.rimMaterial.dispose();
        plate.etchMaterial.map?.dispose();
        plate.etchMaterial.dispose();
      }
      renderer.dispose();
    },
  };
}
