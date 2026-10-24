# Anchor Cleaning: Brevo and Namecheap DNS Handoff

## Current status

Checked June 28, 2026:

- `goanchorcleaning.com` is not registered in the connected Brevo account. The read-only Brevo domain lookup returns 404.
- `jacob@goanchorcleaning.com` is not present in Brevo's sender list.
- Authoritative nameservers are `dns1.registrar-servers.com` and `dns2.registrar-servers.com`.
- Existing root SPF TXT: `v=spf1 include:_spf.google.com ~all`
- Existing DMARC TXT at `_dmarc`: `v=DMARC1; p=none; rua=mailto:jacob@goanchorcleaning.com`

No API call should create the domain or sender. Jacob must complete the Brevo and registrar steps below.

## Why the final Brevo values are not printed yet

Brevo generates the ownership token and DKIM values for the specific account when the domain is added. There is no universal DKIM target or Brevo-code value for `goanchorcleaning.com`, and inventing one would be unsafe.

After adding the domain in Brevo, copy the exact generated values into this table and into Namecheap:

| Purpose | Type | Namecheap Host | Namecheap Value | Status |
|---|---|---|---|---|
| Brevo ownership code | TXT | `[COPY EXACT BREVO HOST]` | `[COPY EXACT BREVO VALUE]` | Pending |
| DKIM 1 | CNAME or TXT | `[COPY EXACT BREVO HOST]` | `[COPY EXACT BREVO VALUE]` | Pending |
| DKIM 2, if shown | CNAME | `[COPY EXACT BREVO HOST]` | `[COPY EXACT BREVO VALUE]` | Pending |
| Existing DMARC | TXT | `_dmarc` | `v=DMARC1; p=none; rua=mailto:jacob@goanchorcleaning.com` | Present; do not duplicate |
| Existing Google SPF | TXT | `@` | `v=spf1 include:_spf.google.com ~all` | Present; do not duplicate |

Brevo may show one TXT DKIM record or two CNAME DKIM records. Use exactly the format shown for this account.

## Add the domain in Brevo

1. Log in to Brevo.
2. Open **Settings > Senders, Domains, IPs > Domains**.
3. Click **Add a domain**.
4. Enter `goanchorcleaning.com`.
5. Choose manual authentication.
6. Leave this page open. Copy every exact Host/Name, Type, and Value into the table above.

Official Brevo instructions: <https://help.brevo.com/hc/en-us/articles/12163873383186-Authenticate-your-domain-with-Brevo-Brevo-code-DKIM-DMARC>

## Add the generated records in Namecheap

1. Log in to Namecheap.
2. Open **Domain List > goanchorcleaning.com > Manage > Advanced DNS**.
3. In **Host Records**, click **Add New Record** for each Brevo ownership and DKIM record.
4. Select the exact record type Brevo shows.
5. For a root host, enter `@`. For a named host, enter the host label. Namecheap automatically appends the domain, so do not accidentally create `host.goanchorcleaning.com.goanchorcleaning.com`.
6. Paste the exact Brevo value. Use Automatic TTL unless Brevo specifies otherwise.
7. Save each record.

Namecheap CNAME instructions: <https://www.namecheap.com/support/knowledgebase/article.aspx/9646/2237/how-to-create-a-cname-record-for-your-domain/>

### Do not change these records blindly

- Do not add a second SPF TXT record. Brevo states SPF is not required for normal shared-IP domain authentication. Preserve the existing Google Workspace SPF record.
- Do not add a second `_dmarc` TXT record. The domain already has DMARC with a reporting address. If Brevo rejects it, edit the single existing record based on Brevo's displayed requirement rather than adding another.
- Do not delete Google verification or mail records.

## Verify the domain

1. Return to the open Brevo domain page.
2. Click **Authenticate this email domain**.
3. Wait for DNS propagation and retry for up to 48 hours if necessary.
4. Do not treat “Added,” “Pending,” or `verified: true` by itself as ready.

Acceptance requires both:

```json
{
  "verified": true,
  "authenticated": true
}
```

## Add the sender

After the domain authenticates:

1. Open **Settings > Senders, Domains, IPs > Senders**.
2. Add `Jacob Maynard <jacob@goanchorcleaning.com>`.
3. Complete any mailbox confirmation Brevo requests.
4. Confirm the sender appears with `active: true`.

## Hard stop

Do not add Anchor to `CLIENT_SEQUENCE_MAP`, enable Emmett, or send a test email. Activation remains blocked until the domain and sender pass the live gate, Jacob approves the draft templates, and Jacob approves an offline rendered sample batch.
