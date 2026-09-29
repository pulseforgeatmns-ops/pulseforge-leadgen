# Studio Substral — implementation record

How `DOCTRINE.md` was translated into the built site, what was measured, and
where implementation had to adapt. The doctrine is the creative source of
truth; this file is the engineering account of it.

`test/studioSubstralDoctrine.test.js` enforces the mechanically checkable parts
of what follows. If you change the site and a doctrine test fails, the test is
probably right.

---

## Palette (§6)

Values are fixed, not eyeballed. Every pairing used for text clears WCAG AA for
small text (4.5:1), which the doctrine requires under §21 and the test suite
recomputes from the stylesheet on every run.

| Token | Value | Role |
|---|---|---|
| `--substral-black` | `#11110F` | Warm charcoal environment |
| `--graphite` | `#1C1B18` | Dimensional surface |
| `--graphite-raised` | `#26241F` | Raised material |
| `--mineral` | `#F0EDE5` | Warm architectural off-white |
| `--mineral-sunk` | `#E4E0D5` | Secondary paper |
| `--patina` | `#7FA890` | Accent on dark |
| `--patina-deep` | `#3D6B57` | Accent on mineral |

Measured contrast:

| Foreground | Background | Ratio |
|---|---|---|
| `#F0EDE5` mineral | `#11110F` black | 16.16:1 |
| `#C9C4B6` body | `#11110F` black | 10.85:1 |
| `#8A8578` structural | `#11110F` black | 5.14:1 |
| `#7FA890` patina | `#11110F` black | 7.12:1 |
| `#7FA890` patina | `#1C1B18` graphite | 6.48:1 |
| `#11110F` black | `#F0EDE5` mineral | 16.16:1 |
| `#3A382F` body | `#F0EDE5` mineral | 10.05:1 |
| `#5E5A4F` structural | `#F0EDE5` mineral | 5.88:1 |
| `#3D6B57` patina-deep | `#F0EDE5` mineral | 5.22:1 |

**One accent family, two tints.** The doctrine asks for one accent *family* and
forbids large accent fills. A single value cannot clear AA on both `#11110F`
and `#F0EDE5`, so the accent resolves per environment through `--accent` inside
`.env-dark` / `.env-mineral`. Both tints sit in the same green hue, which the
test verifies, so this is one family rather than two brand colours. The accent
is never used as a fill larger than 16px; the test walks every rule that paints
it and requires an explicit small size.

---

## Typography (§7)

| Voice | Family | Why |
|---|---|---|
| Editorial grotesk | **Archivo** (variable, weight 100–900, width 62–125%) | Precise rather than friendly, excellent uppercase, holds together at extreme display sizes, and the width axis lets the hero be set slightly expanded so it reads architectural rather than merely large. Not geometric, not startup-soft. |
| Technical mono | **IBM Plex Mono** | An engineering voice rather than a coding-nostalgia one. Carries the evidence classes, measurements and section numbering. |

Both are SIL OFL and **self-hosted** as Latin `woff2` subsets in
`assets/fonts/` (see `assets/fonts/LICENSE.txt`). Self-hosting removes two
third-party connections from the critical path, which matters more here than
the convenience of the Google Fonts CDN.

The mono voice is restricted to evidence, metadata and labels. The test asserts
that `.prose` is never set in mono.

---

## The dimensional object (§11, §18)

It is **one specimen across three acts** — whole in Act I, separated in Act II,
whole again in Act VI — and there are **two implementations of it**, which is
deliberate.

### What the specimen is

Six layers, six materials, on a block of stone. Read top to bottom:

| | Layer | Material | How it is told apart | What it draws |
|---|---|---|---|---|
| 06 | **Design** | Precision surface | Near mirror-polished, the highest reflectivity in the stack, the brightest machined arris, almost no transmission | The page itself: masthead, oversized headline, one emphasised action, a framed media plate, three measures of copy, a footer — traced over the grid it sits on |
| 05 | **Trust** | Warm smoked glass | The **thickest** layer, with a broad soft highlight and a substantial edge. Mass is the signal | A struck seal, a closure mark, a credential plate, a ledger with one row still open |
| 04 | **Search** | Etched architectural glass | The **clearest** body and **thinnest** plate — transmission 0.98 over a long absorption path, at the lowest coverage in the stack — with its markings driving roughness, so they only appear when light rakes them | A site hierarchy with elbow connectors, and an index where one entry is unresolved |
| 03 | **Conversion** | Smoked acrylic | The **most optically dense**: the shortest absorption distance, so light crossing it arrives about a fifth as bright. Polished to a mirror at the surface, which is the contradiction that makes smoked acrylic recognisable | Nodes and paths converging on a single action — and one dashed route that simply stops |
| 02 | **Accessibility** | Frosted polymer | The **one pale material**, and the only one that scatters rather than transmits: the lowest transmission over the highest roughness, with sheen and a soft edge. Its markings are **inked into** it rather than lit through it | Landmark regions nested as a document outline, a heading ladder, a focus-order path |
| 01 | **Performance** | Graphite composite | The **darkest** and **thickest**, and the only **opaque** one — nothing passes through it, which is what separates a composite from the glass above. Brushed along one axis so it answers light directionally | A request waterfall over a measured axis with thresholds, and a sampled trace |
| 00 | **Substrate** | Mineral | A block, not a plate — see below | — |

Performance is deepest and design is the visible surface, which is the
doctrine's closing principle stated physically: what is underneath determines
what happens above it.

### Why they are not six colours

The brief was explicit that six tinted panes would not do. Three separate
properties decide how a layer reads, and they are kept separate:

- **Coverage** (`opacity`, 0.18 to 1) — how much of the layer's own surface you
  see rather than whatever is behind it. One for graphite because it is opaque,
  high for frosted polymer because it scatters, modest for the glass, because
  glass you cannot see through is not glass.
- **Transmission** (0 to 0.98) — how much of what is behind arrives refracted
  rather than merely blended through.
- **Absorption** — `volume`, the optical path in world units, over
  `attenuation`. This is what makes thickness mean something: the same tint over
  a longer path arrives darker. The path follows each plate's own thickness, and
  the ratio spans about seventy-fold from the etched glass to the smoked acrylic.

On top of those: **refractive index** (1.42 to 1.62), so they bend light
differently; thickness (an 8× range from the etched glass to the graphite);
roughness (0.022 to 0.94); reflectivity (0.55 to 3.4 `envMapIntensity`); sheen;
whether the internal markings are lit through the material or inked into it; and
**edge treatment**, which is now a stated hairline width per layer across a range
of more than two to one, plus a deliberate ladder of machined walls from bright
aluminium on the precision surface to dark anodised graphite at the bottom.

The arris used to be two vertices per edge drawn as triangles from consecutive
triples — a chain of degenerate slivers whose apparent width is whatever the
rasteriser lands on. It pinched at the relieved corners and came out a different
weight on every plate for no reason anyone had chosen. It is a ribbon now, four
vertices per segment with inward normals averaged across the corner, so the
highlight holds its width and the width is a number in the layer table. Tint is used only as a **value ladder** — graphite darkest,
frosted polymer palest, a sixtyfold spread — and exactly one layer departs from
the palette at all: Trust takes a 9% warmth nudge toward mineral.

The test suite asserts all six differ on every one of those axes, that the
thickness range is at least 2×, that the absorption range is at least 50×, that
the value ladder spans at least 8×, and that no more than one layer carries a
warmth shift. The browser harness then renders the exploded stack and counts how
many separated grey bands it actually occupies, because the source assertions
were all satisfied by a stack that rendered as far fewer materials than it had.

**Two things were wrong for two passes, and neither was the material.** Every
plate was drawn double-sided, so it painted its own unlit underside over its own
lit top face; the two lids are offset in projection by the plate's thickness and
neither writes depth, so what survived was a rim of the top face around a dark
middle. The pale polymer arrived as a white picture frame with nothing in it.
And the absorption path was set to forty times each plate's thickness, which put
the smoked acrylic's transmittance at eight ten-thousandths — no longer a dark
material but an occluder, blacking out the pale layer beneath it. The layers were
not similar. Two of them were not visible.

### The substrate

**It is not an extrusion, and that is the whole point.** An extruded polygon has
a constant thickness and vertical sidewalls however it is textured, and the eye
reads that as a manufactured panel. A first attempt did exactly that — a hewn
polygon pushed through `ExtrudeGeometry` with a normal map — and it still read as
another plate. The category was wrong, not the finish.

A second attempt displaced a subdivided icosahedron instead, and that read as a
**low-poly block** — which is on the list of things it must not be. Two reasons,
both about scale. three's polyhedron subdivides each of its twenty faces into
(detail + 1)² triangles, so at detail 4 the whole block was five hundred
triangles: facets a third of a unit across on a block eight across. And the
finest octave of its displacement had a wavelength of about a fifth of its width,
so it had curvature everywhere and nowhere to be sharp. The grain map that was
meant to rescue it repeated every 0.28 units, which made all five of its octaves
finer than a millimetre of real stone — it read as a sheen, not as a surface.

So the block is **broken**, not merely displaced:

- **Fracture.** Space is divided into jittered cells, each owning one
  outward-facing plane; any point past its cell's plane is pushed back onto it.
  Points inside a cell land on the same plane, so the result is flat shards
  meeting along sharp arrises. Two scales: a coarse pass cuts the large cleavage
  faces that give the silhouette its angles, a fine pass chips them. The planes
  face outward so the operation only ever removes material — projecting onto an
  arbitrarily oriented plane can move a point outward as easily as inward, and
  outward means a vertex left standing off the surface as a spike.
- **Creases, not dunes.** The displacement field is built from the distance to the
  zero set of a noise function, which is a connected network of thin valleys
  rather than a field of rounded bumps. Stone breaks along lines.
- **Fifteen cleavage planes** at the scale of the whole block, applied twice so the
  solid actually satisfies all of them: two near-horizontal ones flatten it into
  something quarried out of a bed, thirteen more come in around the sides at
  shallow angles and widely varying distances, because evenly spaced planes give a
  regular prism and a regular prism reads as manufactured.
- **Occlusion, measured.** A point displaced inward relative to the local surface
  is by definition in a hollow, and that is measured while the surface is
  displaced and baked into the vertices. A 1024px shadow map cannot resolve a
  fracture network, and the network being nearly black while the broken high
  points take the light is most of why stone reads as stone.
- **One height field, three maps.** Normals, albedo and roughness all come off the
  same crease field, so what the surface says is broken, what it says is dark and
  what it says is matte agree. A normal map on its own is only convincing under
  moving light. The field is scaled so its coarsest feature is about a quarter of
  a unit — the point where the geometry stops carrying structure.
- **Flat shaded**, so every facet answers light on its own, and **vertex colours**
  carry the mineral variation, the occlusion and the fact that downward-facing
  faces are in their own shadow whatever the light does.
- **A real shadow map**, with the stone the only caster and receiver. The
  translucent plates stay out of it: opaque shadows cast by glass look wrong.
- The top is **planed** only where the stack seats into it — a patch cut off the
  crest, bounded by the stack's own footprint and offset from centre. The previous
  pass planed a plateau out to seven tenths of the radius, which is most of the
  top, and is how it turned back into a plate.

And three things at a larger scale, because none of the above decides a
silhouette. Cleavage and fracture both trim every direction to roughly the same
radius, so whatever they do to the surface the outline converges on an ellipsoid:

- **Spurs.** Four unevenly weighted directions in which the block simply runs
  further, roughened at their tips by the same fine field so a limb is broken
  rather than moulded. Applied **after** the cleavage planes: a plane caps the
  radius in its direction, so a spur folded into the displacement is clipped
  straight back off. A spur is where the rock did not break along the bedding, so
  it belongs outside the planes that describe the bedding.
- **A keel.** The underside runs deeper along an off-centre, off-axis line, because
  that is where the block parted and a break does not radiate from the middle of
  one. Only the top is bedded; a plane under it flattened the break into a cut. A
  radial version of this read as a cone, which is a different and much less
  geological object.
- **Bites.** Five spherical subtractions, stated as a direction, a radius and how
  far into the block they reach. This is the only primitive here that produces a
  genuinely *concave* face — displacement and clipping can only give a surface that
  curves outward or is flat — so without them the deepest feature on the block is a
  groove. They are carved along the ray from the block's centre, the way the rest of
  the surface is defined; pushing points away from a sphere's centre, which is the
  obvious reading of subtracting one, moves everything on its far side outward, so
  the spheres inflated the block instead of carving it.

Measured across twenty-four directions, the plan radius runs from 1.6 to 3.8
units, against a near-constant radius before.

**Warmth without neon.** The reference carries light inside its cracks.
Reproduced literally that is glowing lava, which §10 rules out, so it is oxidised
mineral instead: albedo, never emission — a dark warm ochre the key light happens
to find. It is gated on a low-frequency field as well as on crevice depth, so it
stains a few fractures rather than lining all of them, which is the difference
between a mineral and a decoration, and it lifts a stained crevice only far enough
to read as warm rather than as black. A test holds the lifted value below half
that of lit stone, so it cannot drift into a glow.

The block is about 6.5 × 4.1 units in plan against a 3.05 × 2.25 plate, with a
body 2.2 deep and a keel running half a unit below that: twice the plate width,
twelve times the thickest engineered layer, and a third as thick as it is wide,
which is the reference's proportion. It overhangs the stack and anchors the object
— the layers read as thin and precise *because* of the mass underneath them. A
later pass had it at two and a half plate widths and it started to dwarf the
stack; in the reference the rock is barely wider than the layers. It is visible in
the hero, the layers rise off it as the object opens, and it is still there in
Act VI when they reassemble.

**The keel changed the framing.** A taller object is a smaller object once the
camera has to fit it, so reserving frame for the keel meant a deeper keel produced
a less imposing block — and centring on the keel tip aimed the camera low enough
to ride the plates up until they clipped the top of the frame. So the composition
is centred on the block's *body*, taken as the fourth percentile of its surface,
and the keel is allowed past the bottom edge: hardest in the hero, where the lower
two thirds of the frame is display type and the keel is behind it, and only
slightly in the acts, where the block has to stay visible under the layers because
that is the whole reason it is there. A plan-radius rule was tried for the body
first and does not work — the block's rim dips nearly as deep as its keel, so it
selected the same point.

Building it costs about 130ms, once, shared across all three stages, inside the
idle callback that already defers the object. The tessellation is chosen for
facet size rather than smoothness, so that figure is the ceiling on how fine the
geometry can go before it has to hand over to the maps.

There is no giant SUBSTRATE word on the stone. A `00 / SUBSTRATE` annotation
appears in the stage readout beneath the active layer, in the same restrained
engineering-label language as the six chapters, and secondary to it.

### Framing a wide flat object

Worth recording because it cost several passes. The specimen is five units wide
and roughly one thick, which breaks two reasonable-looking shortcuts:

1. **A bounding box is useless.** The block's extreme corners in plan sit at
   mid-height, so a box reserves a great deal of vertical space nothing occupies
   and the specimen floats at 40% of its frame. The fit now runs against a
   decimated copy of the block's actual surface plus the plate stack's corners.
2. **A wide lens diverges.** At 32° the block's near corner blew up and pushed the
   camera back. It is now 21° — a long lens, which also reads as engineering
   render rather than wide-angle drama, and matches the reference.

The camera's aim travels toward the layer under examination, and that travel is
reserved **in screen space inside the fit test**, where it actually applies.
Inflating the geometry to reserve it over-reserves badly for a flat object, and
not reserving it at all pushed the substrate out of the bottom of the frame.

The hero is allowed to crop horizontally (`frameCrop`), because fitting a wide
flat slab on width leaves the frame half empty and pushes the substrate down
behind the statement. The narrow sticky columns crop only slightly.

### Making a layer the subject

The brief ruled out doing this with opacity or colour, so it is done with light.
Four things respond:

1. **A raking light** crosses the subject at a few degrees. This is the whole
   mechanism: an etched, brushed or frosted surface cannot be read at all
   without grazing light, so the same light that reveals the layer is what
   proves its material.
2. **Reflectivity** rises on the subject (2.1× its own `envMapIntensity`) and
   falls to 0.45× on its neighbours.
3. **Camera** — the aim rises to the subject's plane, the dolly closes, and the
   viewing angle steepens against a second solved fit table so the specimen
   cannot leave frame.
4. **Relief** — plates above the subject lift and those below settle.

The emissive tint that used to wash the whole active plate is gone. The accent
survives only as a trace on the arris, and the tests assert that emphasis adds
no opacity and no colour.

The opening is eased and floored: the first chapter is reached at the very top of
the act, where a linear mapping left the object still shut and the layer being
described invisible inside the stack.

### The two implementations

**Baseline — CSS 3D.** Six plates and the substrate in a `preserve-3d` stack.
Each plate paints its own drawing from flat gradient rectangles, so the six
layers are distinguishable with **no requests and no images** — a page, a grid,
marks, an index, a flow, a structure, a trace. Per-layer `--face-lift`,
`--face-body` and `--face-edge` vary the material the same way the WebGL
version does. It needs no WebGL and no JavaScript to be composed, and on
viewports under 600px it is not a fallback — it *is* the intended treatment
(§17), drawn at reduced scale and depth for the shallow pinned band.

**Enhancement — Three.js.** `src/dimensional.js`. Each plate is one
`ExtrudeGeometry` carrying **two materials**: group 0 is the cap (per-layer `ior`,
clearcoat, `depthWrite: false` so the layers read through each other, front faces
only so a plate does not paint its own underside over its own face) and group 1 is
the extruded side wall, which is opaque machined metal.
That is where visible thickness and the metal arris come from. A hairline
highlight follows the top and bottom arrises; the environment gives the metal
and the clearcoat something to reflect; a contact shadow on the stone softens
and shrinks as the stack lifts; and linear fog keyed to camera distance gives
depth falloff so the far side of the specimen recedes.

Adaptations, and why:

- **Transmission, after all.** An earlier pass left it out on cost grounds —
  refractive transmission needs a render target per frame — and relied on cap
  opacity and clearcoat instead. That was the wrong economy: it is the axis that
  makes etched glass read clear, smoked acrylic read deep and frosted polymer
  scatter, and without it the six were opacity variants of one material, which is
  what the brief kept coming back about. It is in, and the object stays inside its
  frame budget and its bundle budget.
- **Procedural environment, not an HDR asset.** A 512×256 canvas with two
  softboxes, warm bounce from below and a bright horizon strip for the machined
  arrises to catch, run through `PMREMGenerator`. Nothing to download.
- **One shadow map, for the stone only.** Six translucent plates casting opaque
  shadow-map shadows looks wrong, so they do not cast: the plates' shadow on the
  substrate is a contact decal, scaled to the lift, which is both cheaper and more
  accurate. The stone does cast and receive, because self-shadowing across a
  broken surface is not something a decal can fake.
- **Arrises built by hand, not `EdgesGeometry`.** `EdgesGeometry` on a plate
  with relieved corners produces either tessellation noise or gaps depending on
  the threshold. Two closed loops from the silhouette give exactly the machined
  outline intended.
- **Artwork drawn to canvas, not loaded.** Six procedural drawings, cached
  and shared between the three stages, at 1024px for Design and 512px for the
  rest. No network cost, and the page composition gets the resolution it needs
  to read as a page.
- **Camera distance is solved, not authored.** The three stages occupy very
  differently shaped boxes and the specimen is a broad flat slab, so any
  hand-tuned distance clips it in at least one of them. `frameDistance()`
  binary-searches the distance at which the assembly's projected corners all
  sit inside a 93% safe frame, sampled across the separation range at resize
  and interpolated per frame. The camera then withdraws exactly as far as the
  opening object requires — which is both correct at any aspect ratio and a
  better reading of §14's "restrained camera movement" than a scripted move.
- **Geometry and graticule canvases are shared** across the three stages.
  three.js keeps GPU state per renderer, so there is no reason to build the
  same six plates or rasterise the same six patterns three times.
- **Loaded last, and conditionally.** Dynamically imported, gated on WebGL
  support, `prefers-reduced-motion`, viewport width, `saveData` and
  `deviceMemory`, and then deferred again to `requestIdleCallback`. It cannot
  affect LCP.

Both stages keep the canvas `aria-hidden="true"`. Every layer name, question
and evidence class lives in the document, so the narrative survives with the
object switched off entirely (§18, §21).

---

## Hero composition (§14 Act I, §9)

The statement and the specimen are **two competing masses that overlap**, not a
headline with a graphic above it. On desktop the object is cropped off the right
edge of the frame — so it reads as larger than the composition can contain —
and `LOOK BENEATH THE SURFACE.` is set across it, the two interlocking rather
than taking turns.

The vertical composition was also rebuilt, because negative space has to create
tension and an earlier pass left stretches of black that simply delayed the next
section. The eyebrow is now pinned directly under the nav, the statement absorbs
the slack and sits on the floor of the viewport, and the scroll cue follows
immediately beneath it. The leftover height therefore collects **between** them,
where the object is, instead of accumulating below the call to action as a band
of nothing. In Act II the layer chapters were tightened from 82svh to 72svh for
the same reason.

## Motion (§13)

No `@keyframes` anywhere in the stylesheet. All narrative motion is weighted
interpolation toward a target, in a `requestAnimationFrame` loop that **stops
as soon as nothing is moving** — there is no ambient motion competing for
attention.

- Separation damping factor `0.075`; camera `0.05`; pointer `0.035`.
- Pointer parallax amplitude is 0.05 rad of yaw and 0.022 rad of pitch. The
  test caps these, because the doctrine wants the visitor to half-wonder
  whether they caused the shift rather than to see a mouse-follower.
- No easing curve has a negative control point, so nothing can overshoot.
- The CSS transition on `.plate` transform was deliberately **removed**:
  transitioning a property the script already interpolates reads as lag, not
  mass.

Scroll is never intercepted. There is no wheel listener, no `scrollTo`, no
scroll-snap. Sticky positioning does all the pinning, so the scrollbar always
means what it says.

Generic reveal motion is limited to the approved opening settle. In the two
acts where the object physically separates or reconstructs, supporting copy is
exposed horizontally on the section datum rather than floated upward.
Diagnosis, protocol, work, engagement and the closing assessment render in
place; those sections do not animate simply because they entered the viewport.
Their composition, not an animation preset, carries the transition.

**Damping applies to mass, not to meaning.** Which layer the reader is on is
information: the readout and the accented plate come from the layer observer,
undamped, so the label always matches the heading beside it. Only the physical
separation is interpolated. An earlier build derived both from the damped value
and the readout named a layer up to two behind what was on screen.

**Act VI aligns by indent, not by translation.** Each of the six names starts
inset by a different amount and resolves to flush left, with the measurement
rule between name and status absorbing the change. Translating the rows instead
carried their ends outside the column — which both widened the document
sideways on narrow viewports and clipped "ALIGNED" mid-word.

---

## Reduced motion (§19)

Not a degraded version. Reduced-motion visitors get:

- the object presented **already decomposed** at a fixed separation, which is
  the state the narrative actually needs to make its point;
- **a single column**, because with nothing pinned the two-column layout left an
  empty gutter beside all six chapters once the specimen had scrolled past. The
  specimen is shown once, whole and static, and the chapters read beneath it.
  The harness asserts the column count, since this was a real defect and an easy
  one to reintroduce;
- the canvas suppressed entirely (`display: none`) and the Three.js module
  never requested;
- the six converged names aligned rather than drifting;
- the sticky stages released to normal flow at a fixed height;
- all revealed content visible.

The preference is read through `matchMedia` and **re-checked on change**, so
turning it on mid-session takes effect without a reload.

---

## Performance (§20)

The site publishes its own budget in the footer colophon, and the test suite
holds it to it.

| Budget | Enforced by |
|---|---|
| Eager JavaScript under 10 KB gzip | `studioSubstralDoctrine.test.js` gzips `substral.js` + `assessment.js` |
| Deferred dimensional bundle under 170 KB gzip | same test, and `build/build.mjs` fails the build |
| No render-blocking script in `<head>` | test asserts no `<script src>` in head |
| No third-party origin on the critical path | test enumerates hosts in `<head>` |
| Targets: LCP < 2.0s, CLS < 0.05, INP < 100ms | stated as commitments, not measurements |

Current measured sizes: eager JS ~4 KB gzip, dimensional bundle 143 KB gzip /
118 KB brotli (561 KB raw). three.js is tree-shaken and minified by
`build/build.mjs` rather than shipped whole.

Layout stability: the one raster image carries explicit `width`/`height`, fonts
use `font-display: swap` with preload so the swap happens early, and an inline
`<style>` in `<head>` paints the dark environment before the stylesheet
resolves so the hero never flashes as a light page.

The budget numbers in the colophon are **commitments**, not claimed
measurements. Published measured figures would go stale in static HTML, and
§16's integrity rules apply to our own claims as much as to a client's report.

---

## The turn into the mineral act (§14 Act III)

The dark-to-mineral transition is **a cut section of the substrate**, not a
fade: the dark environment stops at a machined arris, the reader passes through
a short band of stone, and the paper begins at another arris. The first attempt
was a 22vh soft gradient, and reviewing the recorded scroll-through it read as a
rendering artifact — everything else on this site is a crisp physical edge, so a
soft wipe broke the illusion the object works so hard to build. It is the same
stone as the foundation, which makes the transition mean something: you go down
through the mineral to reach the paper.

The acts either side no longer add their full breathing room against it. The
turn is itself a pause, and stacking three pauses together is what had produced
a stretch of nothing before the next content arrived.

## The fixed nav is unconditionally opaque

Worth recording because it is a class of bug, not a detail. The bar used to be
transparent until an `IntersectionObserver` marked it lifted. Under the render
load of three WebGL stages that callback arrived **seconds** late, and the
narrative's display type scrolled straight through the bar in the meantime.

Legibility must never wait on a callback. The background is now unconditional —
over Act I it is the same colour as the environment behind it, so it still reads
as full bleed — and only the hairline separating rule depends on the observer,
which is decoration and safe to arrive late.

## Responsive intent (§17)

Single column is a designed treatment, not a narrowed desktop.

The stage becomes a shallow band pinned under the nav — 28svh in Act II, 30svh
in Act VI — sitting **above** the copy that scrolls beneath it and drawn with
reduced layer depth (`--strata-scale`, `--sep-scale`). Two things were wrong
before this was settled and both are worth recording, because both are easy to
reintroduce:

1. The band sat *below* the scrolling column in stacking order, so the specimen
   and the prose rendered on top of one another. `studioSubstralDoctrine.test.js`
   now asserts the band outranks its scrolling sibling and is opaque.
2. The band was 44svh, which together with the nav left barely half the viewport
   to read in.

Capable phones now receive the approved material object inside a dedicated,
smaller stage. The CSS composition remains the complete intended treatment
under reduced motion, data saving, low memory or missing WebGL; it is not an
error state or a hidden object.

## Verification

`build/verify-layout.mjs` renders the built page and checks what source-level
tests cannot see. It found every defect listed in this section, so it is part of
the deliverable rather than scaffolding:

- every authored line break, at thirteen widths from 360px to 1920px — a
  doctrine headline that silently rewraps is a doctrine violation;
- horizontal document overflow;
- elements overflowing their container;
- running text crushed into a sliver, which does not overflow and so is
  invisible to the check above (a stray grid child once reduced the refusals
  list to one word per line);
- the three progressive-enhancement states;
- the assessment instrument's rejections, normalisation and offline route;
- and, under `verify-layout.mjs object`, what the signature object actually
  renders: the substrate's local contrast and luminance range, and how many
  separated grey bands the exploded stack occupies. Both of the earlier substrates
  — the extruded plate and the low-poly block — would have failed the first of
  those, and a stack that satisfied every source rule about material
  differentiation failed the second. Neither is visible to a source assertion.

It checks the committed object bundle against its source before measuring
anything, because a build failure inside a shell pipeline reports the exit status
of the pipeline: a broken build looks like a quiet one, and every render after it
silently measures the previous version. That happened, for several passes.

It exits non-zero on failure. Run it after any change to the stylesheet, the
markup or the object.

One defect it caught is worth naming because it is a general CSS trap: a `22ch`
measure on a `<blockquote>` resolved against the *inherited body* font size
rather than the large statement inside it, crushing an 89px pull quote into a
423px column. The measure belongs on the element whose font size it refers to.

## Assessment integrity (§16)

This is where the site is load-bearing rather than decorative.

The four evidence classes on the page are the same four the assessment engine
emits (`packages/capabilities/websiteOpportunityIntelligence/types.js`:
`MEASURED`, `OBSERVED`, `INFERRED`, `UNKNOWN`). The four conclusions in Act III
are that engine's actual `DIAGNOSIS_CLASS` values — `REDESIGN_CANDIDATE`,
`TARGETED_REMEDIATION`, `HEALTHY_SITE`, `INSUFFICIENT_EVIDENCE`. The test reads
both enums from the engine and asserts the page matches, so the marketing
surface cannot drift away from what the product can actually produce.

The engine already refuses a list of claims via
`PROHIBITED_CLAIM_PATTERNS`. **The test runs those same regexes against the
page copy and against the intake's response messages.** The studio's website is
held to the standard the studio's product enforces.

Act IV states the assessment protocol: how measured, observed, inferred and
unknown evidence remain separate, what the written report contains, and which
claims it refuses to manufacture. The live intake now closes Act VI, after the
proof and engagement scope, so the form is the conclusion of the commercial
narrative rather than an interruption before the work.

The four evidence classes are rendered as progressively exposed cuts rather
than equal feature columns. Measured evidence begins on the datum; observed,
inferred and unknown evidence reveal successively more of the underlying field.
The geometry therefore carries the degree of remove from direct measurement
without inventing a chart or a confidence score.

The closing instrument is a real intake, not a decorative form:

- `POST /api/public/website-assessment` → `routes/substralAssessment.js`
- validation and capture → `lib/substralAssessmentIntake.js`
- writes one `pending` `agent_actions` row so a request reaches the operator
  queue rather than an inbox
- the payload is stamped `stage: 'requested'`, and a test asserts no
  score/diagnosis/evidence key can ride along on an intake row

Domain admission reuses the engine's own `discoveryAdmission` rules, so a
search engine, directory or social profile is rejected with the same logic the
discovery pipeline uses. If the endpoint is unreachable the browser hands the
visitor a `mailto:` carrying their input, so a request is never silently lost.

**No score is produced anywhere.** The page says so out loud, and the test
asserts no `nn/100` pattern exists.

---

## Work (§14 Act V)

The published case study is **Anchor Cleaning**, a real site in this repository
(`sites/anchor-cleaning/`), written up in the doctrine's five movements:
Context, Diagnosis, Decision, Experience, Outcome.

Every claim in it is checkable against that source: the two-page audience
split, the inlined stylesheet, the six-field form that confirms in place, the
`ProfessionalService` structured data and explicit service-area list, and the
submissions that write into `agent_actions` through
`lib/walkthroughCapture.js`.

The Outcome section reports that the site is instrumented and **explicitly
declines to publish conversion figures** until there is a full quarter of data
and client approval. Under §10 and §16 a plausible-sounding number would be a
fabricated metric, and inventing one on our own case study would be the exact
failure the assessment promises not to commit.

The screenshot is generated by `build/generate-assets.mjs` from the canonical
source in this repository, served over a temporary local HTTP server so its
root-relative asset paths resolve. It is set inside an evidence cut with three
plain observations — buyer, action and route — so the image reads as captured
proof rather than as an unexplained embedded webpage. It is **not** captured from
`goanchorcleaning.com`, because the live deployment currently lags behind
`main` and still shows retired "walkthrough" copy that the Anchor README
explicitly bars from customer-facing use.

The doctrine names MCFO Services as a possible first case study *if approved
for public use*. It is not approved, so it is not published and it is not
named. The "Publication standard" note states that a second engagement is
waiting on approval, which is true and is also the point.

---

## Engagement scope and close (Act VI)

The commercial scope is presented as three evidence-dependent responses, not
as packages: assessment, targeted remediation, and redesign plus build. Each
response operates on the same six-plane specimen: assessment exposes the
layers, remediation marks one bounded repair, and redesign resolves separated
fragments onto one datum. The distinction is therefore an intervention on a
shared system, not three equal service cards or three oversized numerals.

A fit statement qualifies the work around established businesses with a live
site and a material decision to make. The reconstruction sequence then
resolves the six full-scale layers before the assessment instrument appears as
the final action. The scope introduces no pricing tier, fabricated outcome or
additional ambient motion language.

---

## Where the doctrine was adapted

Per §26, the concept is preserved and only the implementation adapts. The
complete list:

1. **Accent split into two tints** — required to clear AA in both
   environments. One hue family preserved.
2. **No refractive transmission on the plates** — performance. Material
   character preserved through clearcoat, opacity and environment reflection.
3. **Approved materials on capable phones** — the dedicated mobile stage owns
   scale and safe area, while reduced motion, data saving, low memory and missing
   WebGL still resolve to the complete CSS composition.
4. **Colophon publishes budgets, not measurements** — a measured figure baked
   into static HTML would become a false claim the first time it drifted.
5. **Case study outcome withheld** — §16 integrity applied to our own work.
6. **Display sizes capped to the measure they sit in** — the doctrine's line
   breaks (`LOOK BENEATH / THE SURFACE.`, `WE DIAGNOSE / BEFORE WE DESIGN.`)
   are the specification, so type is sized to preserve them rather than set as
   large as possible and left to rewrap. The diagnosis statement was given the
   full page width, with its supporting prose dropped into the right column
   beneath it, so that break holds without shrinking the type.
7. **The lifted nav is opaque rather than blurred** — translucency let display
   type ghost through the bar. This also leaves the stylesheet with no
   glassmorphism at all, which §10 prefers anyway.

Nothing in the narrative, the layer ordering, the evidence taxonomy or the
refusals was simplified.

---

## Launch blockers

These are placeholders in the committed source and must be settled before the
site is pointed at a real domain.

| Item | Current value | Needed |
|---|---|---|
| Domain | `studiosubstral.com` | Register, or replace throughout `index.html`, `robots.txt`, `sitemap.xml`, `assets/brand/site.webmanifest` |
| Mailbox | `hello@studiosubstral.com` | Create, or replace in `index.html` and `assets/js/assessment.js` (`FALLBACK_MAILBOX`) |
| Intake origin | `pulseforge-leadgen-production.up.railway.app` | Confirm this is the production host; it is the `ENDPOINT` constant in `assets/js/assessment.js` |
| Operator queue | `client_id = 1` | Set `STUDIO_SUBSTRAL_CLIENT_ID` if Studio Substral should have its own tenant |
| Second case study | withheld | Publish only with client approval and verifiable claims |
