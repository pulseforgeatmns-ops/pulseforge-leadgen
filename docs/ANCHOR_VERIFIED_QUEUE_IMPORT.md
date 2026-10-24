# Anchor verified-queue importer

The importer adds only operator-verified, phone-ready prospects for Anchor (client 10). It never searches for leads, calls, emails, texts, schedules, creates a disposition, or enables any automation.

Create a JSON file outside the repository with either an array of leads or an object containing a `leads` array. Every lead must have `company`, `phone`, `vertical`, `verification_source`, `verified_at`, and `manual_verified: true`. `vertical` must be one of the six approved Anchor categories.

```json
{
  "leads": [{
    "company": "Example Property Management",
    "phone": "(603) 555-1212",
    "vertical": "property_manager",
    "contact_name": "Alex Example",
    "website": "https://example.com",
    "location": "Manchester, NH",
    "verification_source": "owner review",
    "verified_at": "2026-07-23T12:00:00.000Z",
    "manual_verified": true
  }]
}
```

Preview first; it performs validation and database deduplication without writing anything:

```sh
npm run anchor:verified-queue:dry-run -- --input=/absolute/path/to/anchor-verified-queue.json
```

After reviewing the preview, use the guarded apply command:

```sh
npm run anchor:verified-queue:apply -- --input=/absolute/path/to/anchor-verified-queue.json
```

The importer skips matches by phone, company name, or domain, then makes new prospects visible to the setter as `source: scout`, `preferred_channel: phone`, and `setter_status: new`.
