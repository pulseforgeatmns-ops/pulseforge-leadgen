# Signal V1 continuous deployment

Base main/deployed commit at inspection: `1b5ef029aa4438432204d2fe4c0e81ee80687d4d`. Continuous rollout was approved on 2026-10-07 with a $10/month incremental operating budget, private @frontrunz reads, existing secret references and the fixed email relay including one labeled test. Activation remains gated on user-entered feed auth, genuine source readiness and the parent-coordinated webhook. No optional management controller is included.

## Actual state

Railway project `charming-trust` (`5f6c50eb-6a61-4649-885e-dc3f3b80a2e5`), production (`7046b237-d3a6-4884-8a1e-bd75d809fa3b`). PulseForge service `5c662f94-ef90-42fc-8497-75b9c6d3cca1`, inspected deployment `bb88d20c-b32d-44ab-b1db-155e8c623c95`, SUCCESS/RUNNING. PR875 and auth utility PR878 are on main. No dedicated caller service exists in accessible projects. Required Telegram secret names are present but deployed GramJS is missing. Session validity and real source read access remain UNKNOWN.

Feed/shadow settings were unset. Prospective-001 was absent, raw evidence count zero, registry/jobs empty. Only `frontrunz` is user-grounded; its actual account-resolved channel ID remains unknown. No other candidates are configured. Aggregate research performance was not inspected. Secret values were never retrieved, printed or copied.

## Core continuous pipeline

The core-only patch installs the Telegram dependency, removes invented default sources, enforces the actual pinned identity and read-success health, persists the cursor/feed buffer and fails closed on corrupted state. GramJS is archived upstream; no unapproved library migration was substituted.

Prospective storage persists before updating its temporal projection, restores jobs/evidence, preserves cohort start and recovers interrupted processing. It requires a single research writer/no overlapping deployments. Blinding uses an allowlist, UNKNOWN never implies independence, evidence/source/provenance/clocks are checked, market token identity is validated and requested target times never replace actual sample times. FDV does not substitute for market cap. Missing prospective entries are not invented at 24h.

Research ingestion polls every 60s; delay captures independently every 1s; Telegram polling is 15s. Early captures may be late or missing and are labeled accordingly. The immediate alert reader polls a private authenticated endpoint every 1s, independently of research. It atomically persists minimal operational evidence/outbox entries without writing research rows. Quote enrichment has a 2s budget. Mail payloads are immutable across retries; leases, bounded backoff and dedup survive restart. Provider acceptance, receipt and display are distinct. No 24h outcome wait gates the initial alert.

Only one new shared feed-auth token is required for continuous operation. Existing Telegram secrets use Railway references; existing Brevo credentials stay in PulseForge. Continuous mode has no Railway management token, controller permission, pilot readiness file, heartbeat or expiry. The optional management controller is not loaded when continuous mode is selected, even if a stale controller-enable flag exists. It can be omitted from deployment entirely using the core-only patch.

Relay is disabled until separate consent for the exact sender/recipient and Gmail-to-ChatGPT event path. Operational transport tests are labeled, contain no CA and are excluded from research. No real mail, Telegram message or trade has been sent.

## Configuration and activation

`bootstrap.json` remains unapplied with `SIGNAL_OPERATION_MODE=unselected`, empty source configuration and disabled delivery. The approved operating model is continuous with a $10/month incremental budget. For continuous mode, both services receive mode=continuous, an actual activation timestamp and the selected positive monthly budget. These settings record approval; the budget is not an invoice limiter.

After approved publication/deployment, run the metadata-only `frontrunz` probe inside existing PulseForge. Pass successful sanitized probe JSON plus a JSON object `{mode: "continuous", monthlyBudgetUsd: <selected number>, startAt: <actual timestamp>}` to `scripts/buildSignalDeploymentBundle.js`. Without a model the generated bundle remains gated. No actual identity, budget or start time is invented.

Create one private caller service with 0.25 CPU/256 MiB, one replica, 1GB /data and ON_FAILURE restarts. Read back limits and verify storage/private URL access. Configure only real pinned source access; require genuine connected/available health before prospective cohort start. Enable the immediate operator consumer separately from relay consent. Prove the first real CA -> feed -> raw research evidence -> CALL -> FIRST_CALLER -> cohort -> market -> delay captures -> pending24h using IDs, clocks and data-quality status only.

## Limits and costs

The 500-call buffer and 100-message fetch are bounded, lack consumer acknowledgments and do not establish complete older-edit/burst recovery. Docker is unavailable locally, so the dedicated image remains unbuilt here. Real Telegram access, actual market coverage and ChatGPT delivery are unproved. General Signal UI routes use separate stores/fixtures and are not the minimal operator interface.

At the proposed resource ceilings, continuous feed CPU/RAM/1GB volume costs about $8.11 per 31 days before egress/credits and incremental PulseForge/database/mail use. $10/month additional spend is an option to discuss, not a selected or guaranteed ceiling. Resource limits must be verified after deployment. No automatic cost-stop or custom cost alert is claimed for continuous mode; existing Railway usage/billing facilities remain available. No workspace hard cap is changed.

FOMO identity/Android CA deeplink remains unverified; trade destination stays null and CA is copyable. This does not block core engineering.

## Review packages

- `signal-core-continuous.patch`: core implementation, continuous-mode choice and tests. No Railway management controller/API adapter/stop scripts are included.
- `signal-optional-pilot.patch`: separate unselected management-controller proposal, requiring extra permission and a Railway project token. Not a prerequisite for continuous operation. Its NEVER restart policy must be explicitly configured if that alternative is later chosen.
- `signal-readiness.patch`: complete local working diff for audit; do not assume all optional code is approved for publication.

Sources: https://railway.com/pricing ; https://docs.railway.com/pricing/cost-control ; https://fomo.family/answers/can-i-trade-crypto-on-my-phone .

Validation: core-only patch applied to a clean temporary checkout with management controller files absent: 123 tests passed, zero failures/skips. Complete local set: 128 tests passed; startup: 3 passed. No live messages, credentials or production services were used by these tests.
