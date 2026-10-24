# SPEC-SCOUT-OWNER-001 — Observed Website Owner Evidence

Status: Local implementation and synthetic validation complete; integration
review pending. Authorized local/test work only (2026-10-17).

## Objective and scope

Preserve an explicit owner or founder's name, title, observed email, and exact
source record from pages that Scout's existing website fallback already fetched.
Prefer that bound record over an earlier unassociated/role email. This is a
bounded extraction repair under [SPEC-099A](SPEC-099A_Scout_Investigation_Provenance.md),
not a new source or contact-eligibility policy. It follows the evidence and
approval principles in [Product Thesis](../vision/Product_Thesis.md).

## Contract

- Explicit `ownerEvidence: true` opt-in only; standard intake and promotion set
  it only for Babrun/tenant 13. Default shared scrape and provider-chain callers
  preserve prior behavior independently of mutable Scout configuration.
- Accept only complete single-person records: supported semantic staff cards,
  schema.org Person microdata, or JSON-LD Person records with explicit fields.
- Generic HTML Person records require an explicit matching-company affiliation;
  JSON-LD requires the same or a direct founder/employee/member relationship
  from an Organization with a matching URL. Foreign organization scopes veto.
  HTML name/title/email fields must be direct children of the same record.
  Title fields must be explicitly labeled; free-text biographies are not titles.
- Conflicting names/emails or a non-owner/former-role record veto selection.
  Caps fail closed: eight fetched pages, one million characters per page,
  20,000 DOM nodes, 200 records per page, 8,192 characters per source record,
  40 nesting levels, and 2,000 JSON nodes. URLs/pointers remain within the
  existing receipt's 2,048/500-character limits.
- Never infer a person from an email local part or page-wide proximity. An
  observed email must additionally agree with the existing founder-local-part
  check before it may outrank the fallback.
- Exclude hidden content, unrelated scripts, templates, reviews/testimonials,
  nested/multiple-person ambiguity, conflicting identity/email bindings, and
  off-company sources/domains. Prefer false negatives.
- Retain a bounded exact source slice and location in the selected evidence;
  propagate it through the existing Babrun selected-contact receipt. It grants
  no verification or sending authority.
- If nothing qualifies, preserve the existing first allowed email result exactly.

## Out of scope and invariants

No added HTTP/provider calls, changed eight-successful-page or request cap,
changed provider waterfall, inferred addresses, changed classification/verifier/
DNC/tenant gates, live database writes, production changes, push, or deployment.
No new schema. Existing shared parser version is made a direct dependency;
no dependency version or installation change is needed for the local test.

## Integration dependency

Integrate the promotion evidence safety consolidation
`bf03ca091aa7eb23341b9b1f5b859cd5fa03c3bc` before this repair. That change makes
`babrunPromotionContactResolution` delegate to the same selected-contact helper
as standard intake. Exact source-record persistence and the stricter shared
classification/verification gates on the promotion path depend on that change;
this ranking commit alone must not be treated as a complete promotion rollout.
Both commits remain local until separate publication/production authorization.

## Testing and acceptance

Synthetic regression must fail against main's first-email implementation, pass
with this patch, and prove unchanged request URLs/counts. Cover multiple staff,
nested cards, founder mentions in employee bios, hidden/script/template content,
untrusted email/source domains, role-only/no-email sites, malformed/oversize
records, duplicate/conflicting bindings, and verification/policy rejection.
Use the actual Scout waterfall/projection/save test harness with fake I/O to
show the exact selected evidence reaches existing metadata. No live calls.

The `test:scout-owner-evidence` package command uses a preload denying HTTP, HTTPS, socket,
fetch and DNS calls, failing the test process even if a caller swallows the
exception. The prior crawl fixture's robots request is now explicitly stubbed.
For a strictly offline run, invoke its Node command directly (npm itself may
check its registry): `node --require ./test/helpers/denyWebsiteEvidenceNetwork.cjs
--test test/websiteOwnerEvidence.test.js test/websiteEnrichmentCrawl.test.js
test/babrunScoutIntake.test.js test/babrunContactResolution.test.js
test/canonicalEmailEligibility.test.js test/anchorPreparationBinding.test.js
test/scoutInventoryCapacityRecovery.test.js test/babrunGovernedEnrichmentEvidence.test.js`.

## Deliberate limitations

No rendering/JavaScript execution or arbitrary CSS evaluation; unmarked prose,
bare profile cards, cross-record JSON-LD references, missing company affiliation,
obfuscated addresses, multi-recipient links, and unsupported/malformed/oversized
records keep the original fallback. Extracted text is observed evidence, not
proof of current employment or email deliverability. The existing verifier and
policy remain authoritative. Exact evidence is persisted by the existing
Babrun receipt path only; no cross-tenant persistence/schema expansion is made.
