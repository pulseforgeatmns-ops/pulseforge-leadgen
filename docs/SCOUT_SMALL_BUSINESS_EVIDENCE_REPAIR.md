# Mission-aware small-business website evidence

Base: `8cbeba9d1c95d74d6a658108ca9840a7044744ed`.

## Concrete repair

The business-evidence resolver previously crawled an otherwise unresolved local business but recognized only property-management declarations. The same fetched pages can now identify explicit operator declarations for the existing founder-led small-business taxonomy. It does not infer a vertical from the requested search category.

The new parser is pure and bounded. It examines the complete fetched batch, preserves earlier classification observations and descriptive evidence, and rejects contrary supplier/vendor or other-mission statements/headings. It excludes hidden, quoted, testimonial, portfolio, client, article, and review contexts, including semantic section headings. It never creates contact details, owner attribution, verification, or sendability.

## Activation and integration

- New interpretation is disabled unless `SCOUT_DURABLE_CANDIDATES_ENABLED=true` and the supplied tenant identity is exactly 10 or 13.
- It additionally requires an explicit small-business-owner mission and passes every category through the existing mission-compatible admission checks. Explicit narrower subsegments remain narrower.
- The caller must pass its resolved tenant ID into `persistDiscoveredCompanies`' admission context. This one-line integration is supplied by the coordinated durable-candidate patch; without it the new branch remains disabled.
- `BUSINESS_EVIDENCE_VERSION` is exported as `scout-website-business-evidence-v2` for the durable dependency signature. Changed approved extractor dependencies can be distinguished from an unchanged blocked candidate.
- No environment setting was changed. Publication and activation remain separate decisions.

## Unchanged limits and safeguards

The caller's CLEAR-only business cursor, local geography check, three-successful-page/six-logical-fetch-attempt crawl, timeouts, and redirect settings are unchanged. Six logical attempts are not a wire-level HTTP cap because the existing Axios adapter permits redirects. No added fetch, provider, verifier, database, or send operation exists in the parser.

The Anchor-only same-company recovery tenant guard, Babrun verified-founder requirement, canonical ownership/suppression rules, queue attempts, and cooldowns are untouched. The existing nationwide-without-allowed-cities fetch restriction also remains unchanged; this patch does not expand acquisition geography.

## Verification

The same new 46-test file was run offline on both the exact base and patched tree:

- Base: 30 passed, 16 failed. All ten supported operator-category cases failed to obtain business evidence.
- Patch: 46 passed, 0 failed.

An expanded network-denied local run passed **302 tests, 0 failed, 0 skipped**, covering the new parser, existing strict owner extraction, Babrun intake/inventory, Anchor tenant guard, CLEAR-only cursor behavior, and CI selection contracts. Local runtime was Node 24.19; no Node 22 executable was available. CI remains configured for Node 22 and includes the new suite under the existing network-denying preload. New syntax uses features supported by Node 22, but actual execution there remains unverified until CI runs.

Example focused command:

`node --require ./test/helpers/denyWebsiteEvidenceNetwork.cjs --test test/scoutSmallBusinessWebsiteEvidence.test.js`

## What this does not establish

This repair can admit a genuinely fitting new company to the normal permitted enrichment flow. It does not itself produce a verified owner or prove sustained production inventory growth. Durable progression and multi-cycle integration are a separate coordinated change.

No trusted retained production evidence snapshot or exact-address verifier receipt was supplied for existing-contact metadata repair. Held queue rows and existing unqualified contacts therefore remain held. Reconstructing discarded owner evidence or acquiring/verifying a different owner address still needs an approved bounded acquisition path; the parser never launches one.
