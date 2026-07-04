# Client Email Onboarding

This is the required onboarding path for every client before Emmett may send. A client fails closed until every acceptance criterion passes. Do not enable Emmett to work around a failed check.

## Ownership legend

- **Automatable**: code or an operator command can perform and verify the step.
- **Human review**: a person must approve the judgment or content.
- **Human only**: requires an account login or authority that automation must not use.

## 1. Create and isolate the client

**Owner:** Automatable

1. Create the `clients` row with a permanent client ID.
2. Set the business identity, target verticals, service area, and scoring profile.
3. Keep `enabled_agents` free of `emmett`.
4. Confirm all prospects and companies carry the new `client_id`.

**Pass:** The client and its data are partitioned by `client_id`; Emmett is disabled.

**Fail:** Missing client ownership, mixed-client records, or Emmett already enabled.

## 2. Configure the sender identity

**Owner:** Automatable, then human review

Set all three fields without defaults:

- `sender_email`
- `sender_name`
- `sending_domain`

Confirm the display name and reply mailbox with the client owner.

**Pass:** All fields are non-empty, the email domain exactly matches `sending_domain`, and the owner approves the visible From identity.

**Fail:** Any null, placeholder, borrowed identity, or unapproved display name.

## 3. Add the sending domain in Brevo

**Owner:** Human only

1. Open Brevo.
2. Go to **Settings > Senders, Domains, IPs > Domains**.
3. Click **Add a domain**.
4. Enter the exact `sending_domain`.
5. Choose manual authentication. Do not add a domain blindly through the API.
6. Copy the exact records Brevo generates for this account.

Brevo normally supplies:

| Record | Type | Acceptance criterion |
|---|---|---|
| Brevo ownership code | TXT | The exact Brevo-generated host and value are present and Brevo reports it valid. |
| DKIM | One TXT or two CNAME records | Every exact Brevo-generated host and value is present and reports valid. |
| DMARC | TXT | Exactly one `_dmarc` record exists and Brevo accepts it. Never create a duplicate. |
| SPF | TXT | No extra SPF record for normal shared-IP Brevo sending. Preserve the existing single SPF record. Dedicated-IP instructions are a separate process. |

The ownership token and DKIM targets are account-generated. They cannot be safely guessed or precomputed. Record the exact values in the onboarding ticket after the domain is added in Brevo.

**Pass:** Brevo's domain response has both `verified: true` and `authenticated: true`.

**Fail:** Domain merely added, pending, partially valid, or authenticated under a different domain.

Allow up to 48 hours for DNS propagation before treating an unchanged result as an error.

## 4. Add and verify the Brevo sender

**Owner:** Human only

1. In Brevo, open **Settings > Senders, Domains, IPs > Senders**.
2. Add the exact `sender_email` and approved `sender_name`.
3. Complete any mailbox verification Brevo requests.
4. Confirm the sender is Active.

**Pass:** Brevo's sender list contains the exact email with `active: true`.

**Fail:** Missing, inactive, unverified, or a different sender address.

## 5. Create client-owned templates

**Owner:** Automatable draft, then human review

1. Write a separate sequence for each exact prospect vertical.
2. Brand every message for the client, never Pulseforge or another client.
3. Use only `{{first_name}}` and `{{business_name}}`.
4. Apply the Emmett voice rules: default to contractions, vary sentence length, avoid cadence narration and sender-positioning openers, and use no em dashes in body copy.
5. Add drafts to the sequence catalog, but do not add the client-to-sequence mapping yet.
6. Render every step with representative sample data and inspect subject, body, signature, and token replacement.

**Pass:** The client owner approves every rendered step for every target vertical.

**Fail:** Missing vertical, unsupported token, wrong brand, fallback copy, unrendered content, or unresolved review edits.

## 6. Meet prospect data thresholds

**Owner:** Automatable enrichment, then operator review

Every prospect considered for sending must have:

- A non-empty email.
- `email_status` equal to `valid` or `verified`.
- A non-empty `first_name`.
- `do_not_contact` not true.
- No recorded hard or soft bounce.
- No pending send or conflicting active sequence.

The onboarding batch must also have enough approved prospects to review meaningful rendered samples. Record total prospects and counts for missing email, invalid status, missing first name, DNC, and bounced.

**Pass:** Every prospect in the activation batch meets every row-level condition.

**Fail:** Any row depends on a greeting fallback, unverified address, DNC override, or manual assumption.

## 7. Run the readiness gate

**Owner:** Automatable

Run `evaluateSendingReadiness` for every activation-batch prospect. Save the failure breakdown. The gate checks live prospect state and read-only Brevo state, and Emmett repeats it at the final pre-send boundary.

Blocked results are queryable in `agent_log` with:

```text
action = sending_readiness_blocked
status = failed
payload.failures = structured failed conditions
payload.stage = prospect_evaluation or pre_send
```

**Pass:** Every activation-batch prospect returns `sendable: true` and zero failures.

**Fail:** Any failure, Brevo/API uncertainty, database-check error, or fallback requirement.

### Stale or orphaned sequence recovery

The gate intentionally blocks a forever-pending send or a different incomplete sequence. There is no automatic expiry yet, so an orphan can remain blocked indefinitely. It will remain visible through `sending_readiness_blocked`; it must never be silently skipped.

Operator handling today:

1. Inspect the prospect's `agent_log`, touchpoints, and Brevo events.
2. Prove whether Brevo accepted a message. Do not retry while delivery is uncertain.
3. If no message was accepted, mark the orphaned `email_pending` record `failed` with an audit reason. The gate will then allow a retry if every other condition passes.
4. A conflicting incomplete sequence has no safe first-class closure signal today. Do not rewrite historical `email_sent` records merely to bypass the gate. Keep the prospect blocked and escalate it for a reviewed recovery change.
5. After a stale-pending repair, rerun the readiness gate and retain both the original block and recovery record.

**Known limitation:** There is no first-class sequence enrollment table or automatic stale-pending timeout. A conflicting incomplete sequence can therefore remain blocked indefinitely. A future improvement should add explicit enrollment states (`active`, `completed`, `failed`, `cancelled`) and an operator recovery action that the gate reads, with an immutable audit log. Do not infer completion solely from age.

## 8. Review rendered samples

**Owner:** Human review

Render, but do not send, a sample for each vertical and each sequence step using real-looking test names and businesses. Include the exact From name/address, subject, plain text, and token substitutions.

**Pass:** The client owner approves the rendered batch in writing.

**Fail:** Approval is missing, conditional, or based only on raw templates.

## 9. Activate deliberately

**Owner:** Human approval plus automatable change

Activation requires all of the following:

1. Brevo domain is verified and authenticated.
2. Brevo sender is active.
3. Templates and rendered samples are approved.
4. The activation batch passes the readiness gate.
5. The client owner explicitly authorizes activation.

Only then:

1. Add exact vertical mappings for the client to `CLIENT_SEQUENCE_MAP`.
2. Add `emmett` to the client's `enabled_agents`.
3. Start with an approved low cap.
4. Monitor send logs, bounces, replies, and readiness blocks.

**Pass:** Written authorization exists and all five prerequisites are evidenced.

**Fail:** Never activate partially. Keep Emmett disabled.

## Anchor Cleaning: current onboarding record

- Client: `client_10`
- Sender: `Jacob Maynard <jacob@goanchorcleaning.com>`
- Sending domain: `goanchorcleaning.com`
- Phone: `(603) 420-2430`
- Draft verticals: `law_firm`, `accounting`
- Draft template keys: `anchor_law_firm_draft`, `anchor_accounting_draft`
- Activation state: **OFF**. Client sequence mappings are intentionally absent and `enabled_agents` remains `['scout']`.

### Anchor DNS state observed before Brevo setup

- Nameservers: `dns1.registrar-servers.com`, `dns2.registrar-servers.com` (Namecheap).
- Existing SPF: `v=spf1 include:_spf.google.com ~all`. Preserve it; do not add another SPF record.
- Existing DMARC: `v=DMARC1; p=none; rua=mailto:jacob@goanchorcleaning.com`. Preserve a single DMARC record and use the exact value Brevo validates; never add a second `_dmarc` TXT record.
- Missing until Jacob adds the domain in Brevo: the account-specific Brevo ownership TXT value and DKIM TXT/CNAME value(s).

Anchor remains blocked until Jacob adds the domain in Brevo, copies its exact generated records into Namecheap Advanced DNS, confirms Brevo authentication, approves both template sequences, approves rendered samples, and explicitly authorizes activation.
