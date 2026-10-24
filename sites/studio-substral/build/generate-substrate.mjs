#!/usr/bin/env node
/** Bake the approved procedural rock for the CSS/no-WebGL composition.
 * Build-time only: no mesh, light, material or runtime WebGL code is changed.
 * Run from this directory: node generate-substrate.mjs
 */
import { build } from 'esbuild';
import puppeteer from 'puppeteer';
import { createServer } from 'node:http';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));
const site = path.resolve(here, '..');
let source = await readFile(path.join(site, 'src/dimensional.js'), 'utf8');
const returnMarker = '  return {\n    /**\n';
if (source.split(returnMarker).length !== 2) throw new Error('Object export changed; inspect the bake hook.');
// Expose the scene only in this in-memory build. The shipped source and bundle
// remain byte-for-byte unchanged, including the approved material and lighting.
source = source.replace(returnMarker,
  '  return {\n    bake: { renderer, scene, assembly, plates, substrate, contactShadow },\n    /**\n');
source += "\nexport { OrthographicCamera } from 'three';\n";
const { outputFiles } = await build({
  stdin: { contents: source, resolveDir: here, loader: 'js' },
  nodePaths: [path.join(here, 'node_modules')],
  bundle: true, format: 'esm', write: false,
});
const server = createServer((req, res) => {
  if (req.url === '/scene.js') {
    res.writeHead(200, { 'Content-Type': 'text/javascript' }).end(outputFiles[0].text);
  } else {
    res.writeHead(200, { 'Content-Type': 'text/html' }).end(
      '<style>html,body{margin:0;background:transparent}div{width:1000px;height:600px}</style><div><canvas></canvas></div>');
  }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
let browser;
try {
  browser = await puppeteer.launch({ args: ['--no-sandbox', '--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader'] });
  const page = await browser.newPage();
  await page.setViewport({ width: 1000, height: 600, deviceScaleFactor: 1 });
  await page.goto(`http://127.0.0.1:${server.address().port}/`);
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await mkdir(path.join(site, 'assets/object'), { recursive: true });
  for (const pitch of [62, 64]) {
    const data = await page.evaluate(async pitch => {
      const { createDimensionalObject, OrthographicCamera } = await import('/scene.js');
      const object = createDimensionalObject(document.querySelector('canvas'), { mode: 'surface' });
      const { renderer, scene, assembly, plates, contactShadow } = object.bake;
      plates.forEach(({ group }) => { group.visible = false; });
      contactShadow.visible = false; // CSS plates continue to own their shadow.
      assembly.position.set(0, 0, 0);
      assembly.rotation.set(0, 38 * Math.PI / 180, 0);
      scene.fog = null; // The transparent export composites on the existing stage.
      renderer.setPixelRatio(1);
      renderer.setSize(1000, 600, false);
      const camera = new OrthographicCamera(-5, 5, 3, -3, 0.1, 140);
      const elevation = (90 - pitch) * Math.PI / 180;
      camera.position.set(0, 20 * Math.sin(elevation), 20 * Math.cos(elevation));
      camera.lookAt(0, 0, 0);
      renderer.render(scene, camera);
      // Tight bounds shared by both poses, with transparent breathing room.
      // Removing the empty canvas border also prevents CSS scroll overflow.
      const crop = document.createElement('canvas');
      crop.width = 640;
      crop.height = 308;
      crop.getContext('2d').drawImage(renderer.domElement, 230, 236, 640, 308, 0, 0, 640, 308);
      const data = crop.toDataURL('image/webp', 0.86);
      object.dispose();
      return data;
    }, pitch);
    const bytes = Buffer.from(data.split(',')[1], 'base64');
    if (bytes.length > 100 * 1024) throw new Error('Substrate exceeds the 100 KB asset budget.');
    const file = `assets/object/substrate-${pitch}.webp`;
    await writeFile(path.join(site, file), bytes);
    console.log(`${file}: ${bytes.length} bytes, 640 × 308, transparent`);
  }
  if (errors.length) throw new Error(errors.join('\n'));
} finally {
  await browser?.close();
  server.close();
}
