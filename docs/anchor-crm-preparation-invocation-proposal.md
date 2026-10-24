# One-shot CRM preparation invocation proposal

SPEC264, local-only. Not approved for publication, installation, SSH login/key registration, deployment or production execution. This is a separate overlay on the frozen SPEC263 bridge; it must not be silently included in an already approved source publication.

## Why a tiny wrapper
The existing `prepareExistingCrm(pool, {programId, sourceMissionHash, admissions}, {env, now})` always attempts writes. It has no read-only invocation mode. `scripts/anchorCrmPreparationOnce.js` adds only a fixed one-shot operator boundary, no public route or scheduler entry. No generic admin framework or new schema is introduced.

Default execution opens a bounded private database pool using the container's existing DATABASE_URL and existing app TLS-selection semantics. No credential value is accepted as a command flag or returned. The script validates exact tenant10, expected deployed revision, canonical full manifest hash and source payload hash; it uses a real READ ONLY snapshot, existing installed-grant/operator validation, current program/policy/source and quarantine binding. It releases that transaction before any apply operation. This preview validates manifest/source authority only: it does not assert that full contact/ownership/telemetry eligibility will pass or that drafts exist.

`--apply` is the sole write selector. It requires the already-present activation ID and pins that ID while retaining every live kill/removal environment field. The immutable same-ID grant row and the unchanged bridge's own revalidation reject replacement/revocation after preview. The script never installs a grant, enables an environment flag, initializes review, changes source/program policy or creates send authority.

The approved batch is exactly the complete 1–5 identities in that immutable preparation-only manifest. Selecting a different subset requires a separately approved exact manifest; this wrapper does not accept arbitrary recipient data or callback injection. Email/research scope must be disabled. The original bridge enforces eligibility, nonexpiring quarantine, permanent consumption, telemetry, deterministic no-provider drafting and review-only READY. Prior held input cannot become fresh.

Nested raw library/driver diagnostics are suppressed in this isolated process. Output is fixed status/count/hashes and, after success, an opaque child mission ID. It contains no sender, contact/email, copy, credential, raw SQL or raw error text. A failure after apply begins reports writesMayHaveOccurred=true; inspect the permanent consumption/child before further action. Never blindly retry or refund.

## Supported transport, not presently connected
Official Railway documentation: [SSH one-shot commands](https://docs.railway.com/cli/ssh). Syntax is `railway ssh [OPTIONS] [COMMAND...]`, with `--` before the remote command. The docs require a registered SSH key and first invocation can prompt registration. Existing SSH login/key availability is unverified; connector authorization does not supply SSH access. Do not install/login/register a key or accept a new access prompt without the required separate permission.

Dockerfile source sets WORKDIR /app and copies the repository there; the actual deployed file/path/revision must still be verified. The wrapper must first be separately reviewed, published and deployed in the exact approved gate-aware build. RAILWAY_GIT_COMMIT_SHA must match the explicitly approved 40-hex deployment revision or the command closes before connecting to the DB. No production invocation is performed by this document.

## Exact default read-only command template
The following quoted placeholders intentionally fail validation and must be replaced only with separately approved exact values. No secret, contact data or manifest JSON belongs on the command line.

```sh
railway ssh --project 5f6c50eb-6a61-4649-885e-dc3f3b80a2e5 \
  --environment 7046b237-d3a6-4884-8a1e-bd75d809fa3b \
  --service 5c662f94-ef90-42fc-8497-75b9c6d3cca1 -- \
  node /app/scripts/anchorCrmPreparationOnce.js \
  --tenant 10 --revision 'APPROVED_REVISION_40_HEX' \
  --manifest-sha256 'APPROVED_MANIFEST_64_HEX' \
  --source-sha256 'APPROVED_SOURCE_PAYLOAD_64_HEX'
```

For later separately approved production canonical preparation, use the same exact command with `--apply` appended. Approval must cover the named batch, exact source/manifest/revision, durable canonical preparation and permanent operation/item consumption. The SSH session and database credentials remain in their existing environments; no local DATABASE_URL export or credential retrieval is part of the plan. Database transport is necessarily used; no provider/network discovery, enrichment, LLM or dispatch is invoked.

## Remaining prerequisites
All 13 inspected current program-scoped contacts are held. No current fresh batch or draft exists. Fresh separately approved inventory, current exact review/admission, historical quarantine/control schema and the bridge operation CHECK extension must precede this invocation. Keep legacy autosend false and the original program expiry/caps. Draft N plus later freeze N consumes 2N preparation item units/two operations; canonical approval is separate. The wrapper grants no send authority.

This source proposal resolves the shape of a safe operator command, not current access or production readiness. SSH availability, exact deployed revision/path and the actual approval values remain unverified/unset. Any authentication registration, publication/deploy, schema/admission/grant write, production apply or send still needs the appropriate separate decision.
