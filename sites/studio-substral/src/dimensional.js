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
   -------------------------------------------------------------------------- */

function graticuleTexture(index) {
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

  const texture = new CanvasTexture(canvas);
  texture.colorSpace = SRGBColorSpace;
  texture.anisotropy = 4;
  return texture;
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

  const shape = plateShape(PLATE_W, PLATE_H, CORNER);
  const plateGeometry = new ExtrudeGeometry(shape, {
    depth: PLATE_T,
    bevelEnabled: false,
    curveSegments: 4,
  });
  const rim = rimGeometry(shape, PLATE_T);
  const etchGeometry = new PlaneGeometry(PLATE_W * 0.94, PLATE_H * 0.94);

  const plates = [];
  for (let i = 0; i < LAYERS; i += 1) {
    const group = new Group();
    // Plates lie horizontal: the extrusion axis becomes vertical thickness.
    group.rotation.x = -Math.PI / 2;

    const faceMaterial = new MeshPhysicalMaterial({
      color: new Color(SUBSTRAL_BLACK).lerp(new Color(MINERAL), 0.1),
      metalness: 0.06,
      roughness: 0.3,
      clearcoat: 1,
      clearcoatRoughness: 0.16,
      transparent: true,
      opacity: 0.44,
      depthWrite: false,
      side: DoubleSide,
      envMapIntensity: 1.15,
      emissive: new Color(PATINA),
      emissiveIntensity: 0,
    });

    const face = new Mesh(plateGeometry, faceMaterial);
    group.add(face);

    const rimMaterial = new LineBasicMaterial({
      color: new Color(MINERAL),
      transparent: true,
      opacity: 0.34,
      depthWrite: false,
    });
    group.add(new LineSegments(rim, rimMaterial));

    const etchMaterial = new MeshBasicMaterial({
      map: graticuleTexture(i),
      transparent: true,
      opacity: 0.2,
      blending: AdditiveBlending,
      depthWrite: false,
    });
    const etch = new Mesh(etchGeometry, etchMaterial);
    etch.position.z = PLATE_T + 0.0015;
    group.add(etch);

    assembly.add(group);
    plates.push({ group, faceMaterial, rimMaterial, etchMaterial });
  }

  /* --- State ------------------------------------------------------------ */

  const state = {
    gap: mode === 'reconstruct' ? GAP_SEPARATED : GAP_ASSEMBLED,
    yaw: 0,
    pitch: 0,
    dolly: 7.4,
    height: 1.95,
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
      rimMaterial.opacity = lerp(0.34, 0.92, h);
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

      target.gap = lerp(GAP_ASSEMBLED, GAP_SEPARATED, separation);

      /* Camera movement stays restrained: a small elevation and dolly, never
         a travelling shot (doctrine §14, Act II). */
      target.dolly = lerp(7.4, 6.7, separation);
      target.height = lerp(1.8, 2.55, separation);

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
      resizeObserver.disconnect();
      plateGeometry.dispose();
      rim.dispose();
      etchGeometry.dispose();
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
