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

Read top to bottom:

| | Layer | Physical treatment | What it draws |
|---|---|---|---|
| — | **Surface** | Polished acrylic, thinnest, brightest rim | A designed page: masthead, navigation, an oversized headline, one emphasised action, a framed media plate, three measures of copy, a footer |
| 06 | **Design** | Clear acrylic, high clearcoat | Twelve-column and baseline grid, margin markers, a dimension callout, two type traces sitting on the system |
| 05 | **Trust** | Slightly clouded, more metal in the cap | A struck seal, a closure mark, a credential plate, a verification ledger with one row still open |
| 04 | **Search** | Lower clearcoat, more diffuse | A site hierarchy with elbow connectors, and an index with leader dots where one entry is unresolved |
| 03 | **Conversion** | Diffuse acrylic | Nodes and paths converging on a single action — and one dashed route that simply stops |
| 02 | **Accessibility** | **Frosted polymer**: roughness 0.42, clearcoat 0.32 — milky, not glassy | Landmark regions nested as a document outline, a heading-level ladder, and a focus-order path through numbered stops |
| 01 | **Performance** | **Graphite composite**: thickest at 0.092, roughest, most metallic, least transparent | A request waterfall over a measured time axis with thresholds, and a sampled trace |
| — | **Foundation** | **Mineral**: warm stone, procedurally grained and veined, wider than the plates, and it never moves | — |

Performance is deepest and design sits directly beneath the surface, which is
the doctrine's closing principle stated physically: what is underneath
determines what happens above it. The digital layers rise off the stone as the
object opens.

The surface plate is **not a seventh system**. It is the visible website the six
explain, which is why its composition sits on the top face while every other
drawing is embedded at mid-thickness and read *through* the material.

### Why seven materials instead of one

Six identical slabs at different spacings communicate the idea and none of the
material — that was the defect this stack replaced. Thickness, tint, opacity,
roughness, clearcoat, metalness, rim alloy and rim finish all vary per layer,
and the test suite asserts that at least six distinct values exist for
thickness and finish and that all seven drawings differ.

### Making a layer the subject

When the reader reaches a layer, that layer physically becomes the subject.
Four things respond together, none of them spacing:

1. **Lighting** — an examination `PointLight` travels to the subject's height
   and lifts from zero. It is dark whenever no layer is under discussion.
2. **Camera** — the aim rises to the subject's plane, the dolly closes about
   six percent, and the viewing angle steepens so the subject turns its face
   up. A second solved fit table for the steeper angle guarantees this cannot
   push the specimen out of frame.
3. **Material** — the subject gains opacity and emissive patina at its arris;
   its machined wall catches more of the environment; everything else recedes.
4. **Relief** — plates above the subject lift and those below settle, giving it
   physical room. This is a consequence of the emphasis, not the mechanism.

### The two implementations

**Baseline — CSS 3D.** Seven plates and the substrate in a `preserve-3d` stack.
Each plate paints its own drawing from flat gradient rectangles, so the seven
layers are distinguishable with **no requests and no images** — a page, a grid,
marks, an index, a flow, a structure, a trace. Per-layer `--face-lift`,
`--face-body` and `--face-edge` vary the material the same way the WebGL
version does. It needs no WebGL and no JavaScript to be composed, and on
viewports under 600px it is not a fallback — it *is* the intended treatment
(§17), drawn at reduced scale and depth for the shallow pinned band.

**Enhancement — Three.js.** `src/dimensional.js`. Each plate is one
`ExtrudeGeometry` carrying **two materials**: group 0 is the smoked acrylic cap
(`ior: 1.49`, clearcoat, `depthWrite: false` so the layers read through each
other) and group 1 is the extruded side wall, which is opaque machined metal.
That is where visible thickness and the metal arris come from. A hairline
highlight follows the top and bottom arrises; the environment gives the metal
and the clearcoat something to reflect; a contact shadow on the stone softens
and shrinks as the stack lifts; and linear fog keyed to camera distance gives
depth falloff so the far side of the specimen recedes.

Adaptations, and why:

- **No `transmission`.** Real refractive transmission on seven overlapping
  plates needs a render target per frame and is the single most expensive thing
  in the scene. Cap opacity plus clearcoat plus a structured environment reads
  as smoked acrylic at this scale for a fraction of the cost. §26 permits
  adapting implementation to preserve performance; the material character is
  preserved.
- **Procedural environment, not an HDR asset.** A 512×256 canvas with two
  softboxes, warm bounce from below and a bright horizon strip for the machined
  arrises to catch, run through `PMREMGenerator`. Nothing to download.
- **No shadow maps.** Seven translucent plates casting opaque shadow-map
  shadows looks wrong and costs a second pass. A contact shadow on the
  substrate, scaled to the lift, is both cheaper and more accurate.
- **Arrises built by hand, not `EdgesGeometry`.** `EdgesGeometry` on a plate
  with relieved corners produces either tessellation noise or gaps depending on
  the threshold. Two closed loops from the silhouette give exactly the machined
  outline intended.
- **Artwork drawn to canvas, not loaded.** Seven procedural drawings, cached
  and shared between the three stages, at 1024px for the surface and 512px for
  the rest. No network cost, and the surface composition gets the resolution it
  needs to read as a page.
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

WebGL is off below 600px. On a phone the CSS composition is the intended
object — fewer simultaneous objects, no lighting cost — rather than a fallback.

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
- the assessment instrument's rejections, normalisation and offline route.

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

Act IV is a real intake, not a decorative form:

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
root-relative asset paths resolve. It is **not** captured from
`goanchorcleaning.com`, because the live deployment currently lags behind
`main` and still shows retired "walkthrough" copy that the Anchor README
explicitly bars from customer-facing use.

The doctrine names MCFO Services as a possible first case study *if approved
for public use*. It is not approved, so it is not published and it is not
named. The "Publication standard" note states that a second engagement is
waiting on approval, which is true and is also the point.

---

## Where the doctrine was adapted

Per §26, the concept is preserved and only the implementation adapts. The
complete list:

1. **Accent split into two tints** — required to clear AA in both
   environments. One hue family preserved.
2. **No refractive transmission on the plates** — performance. Material
   character preserved through clearcoat, opacity and environment reflection.
3. **WebGL off below 600px** — the CSS composition becomes the intended mobile
   object rather than a degraded desktop one, which is what §17 asks for.
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
