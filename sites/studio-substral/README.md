# Studio Substral website

Source for the Studio Substral site. Static, self-contained, no server-side
rendering and no build step at deploy time.

- **`DOCTRINE.md`** — the Design & Experience Doctrine v1. Creative source of
  truth. Read it before changing anything visual.
- **`DOCTRINE-IMPLEMENTATION.md`** — how the doctrine was built, what was
  measured, where implementation adapted, and the launch blockers.

## Layout

```
index.html                     the whole narrative, six acts
robots.txt  sitemap.xml
assets/css/substral.css        design system + all six acts
assets/js/substral.js          narrative orchestration (eager, ~4 KB gzip)
assets/js/assessment.js        the Act IV instrument
assets/js/dimensional.js       GENERATED — three.js bundle, loaded on demand
assets/fonts/                  self-hosted Archivo + IBM Plex Mono (Latin)
assets/brand/                  favicon.svg is the source; rasters are generated
assets/work/                   GENERATED — case-study capture
src/dimensional.js             source for the three.js object
build/                         build tooling, not published
```

## Publish set

Deploy exactly these, and nothing else:

```
index.html  robots.txt  sitemap.xml  assets/
```

`build/`, `src/`, and the three markdown files are repository-only. Follow the
Anchor Cleaning pattern: GitHub Pages from a dedicated repo, `main` → `/`.

## Building

Two generated artefacts are committed so the publish set needs no toolchain.
Rebuild them only when their source changes.

```bash
cd build
npm install

# assets/js/dimensional.js  <- src/dimensional.js (tree-shakes three.js)
npm run build

# verify the committed bundle matches the source
npm run check

# brand rasters and the case-study capture
node generate-assets.mjs            # all
node generate-assets.mjs icons      # from assets/brand/favicon.svg
node generate-assets.mjs social     # open graph preview
node generate-assets.mjs work       # Anchor Cleaning capture
```

`generate-assets.mjs` uses the repository's puppeteer, so run `npm install` at
the repo root first if `node_modules` is absent.

`npm run build` fails if the bundle exceeds its gzip budget. That is intended:
see the performance doctrine (§20).

## Tests

```bash
node --test test/studioSubstralDoctrine.test.js     # doctrine conformance
node --test test/substralAssessmentIntake.test.js   # Act IV intake
```

The doctrine suite checks the mechanically verifiable rules — layer ordering,
the evidence taxonomy, banned copy and visual patterns, contrast ratios
recomputed from the stylesheet, the accessibility floor, and the performance
budget the footer publishes. It also runs the assessment engine's own
`PROHIBITED_CLAIM_PATTERNS` against the page copy, so the site is held to the
standard the product enforces.

## The assessment form

`POST /api/public/website-assessment` on the Pulseforge app
(`routes/substralAssessment.js`, `lib/substralAssessmentIntake.js`). It captures
a request as a `pending` `agent_actions` row; it does **not** run an assessment
and must never return a finding about the submitted domain.

Rejected input: non-domains, unroutable hosts, search engines, directories,
social profiles, URL shorteners, and our own properties. Admission reuses
`packages/capabilities/websiteOpportunityIntelligence/discoveryAdmission.js`.

Set `STUDIO_SUBSTRAL_CLIENT_ID` to route requests to a tenant other than
`client_id = 1`.

## Before launch

`DOCTRINE-IMPLEMENTATION.md` has the full table. In short: the domain, the
mailbox, and the intake origin are placeholders.
