# SIGNAL REA finding — FOMO token actionability

**Investigation:** `signal-rea-fomo-token-actionability-001`  
**Captured:** 2026-10-12  
**Status:** Complete for the public web contract  
**Scope:** Public, unauthenticated FOMO web assets; no wallet or trade interaction

## Evidence boundary

The capture used the public HTML and JavaScript assets served by `https://fomo.family`. Raw third-party assets remain in the ignored local REA workspace. The REA application analyzer inventoried and parsed the capture but could not launch its child analysis process because the runner's `ps` implementation failed REA's ownership check. The route conclusions below were verified directly against the captured modules after that boundary was reached.

Reviewed artifact fingerprints and source locations are recorded in the local evidence ledger. This committed finding contains only the minimal behavioral contract needed for Signal review.

## Finding ledger

| Question | Observation | Evidence type | Confidence | Remaining gap | Signal design choice |
| --- | --- | --- | --- | --- | --- |
| Q1 | FOMO's canonical token route uses the token address as `:tokenAddress`; it does not use a pool address. For Solana the route chain segment is `solana`. | Public route manifest and captured chain map | High | The destination can still reject a syntactically valid address if it has no supported market. | Treat Signal's validated Solana token address as the sole action-link identity. |
| Q2 | The canonical URL is `https://fomo.family/tokens/:chain/:tokenAddress`. The compatibility URL `/coin?address=…&chainId=…` maps numeric chain ID `1399811149` to `solana`, removes `address` and `chainId`, preserves other query parameters, and redirects to the canonical route. FOMO also supplies the canonical web URL as the iOS app argument for token pages. Missing address or an unmapped chain ID redirects to `/`. | Public route manifest, redirect module, chain module, and root app-link module | High | Android intent resolution was not exercised in this runner. | Emit the canonical HTTPS URL. It works as a web destination and is eligible for platform universal-link handling without Signal inventing an app scheme. |
| Q3 | The route contract confirms only chain and token address before the token page loads. Symbol, name, price, liquidity, market cap, social links, and tradability require separate evidence or destination data. | Route parameters and token page module boundary | High | Authenticated and unsupported-market states were not exercised. | Signal may show fields backed by its own timestamped provenance. It must not imply that FOMO confirmed metadata or tradability merely because the URL was built. |

## Implemented contract

`buildFomoTokenUrl(tokenAddress)` emits the canonical Solana URL only for a syntactically valid Solana address. Missing, malformed, EVM-shaped, or otherwise ambiguous identity returns `null`. The builder performs no network request, wallet action, or trade action.
