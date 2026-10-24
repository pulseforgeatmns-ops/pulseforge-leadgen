# Scout skip diagnostics

Scout persists pre-save ingestion skips to `scout_skip_log` and includes the same
counts in each completed `agent_log` payload under `skipped_breakdown`.

Current persisted reasons are `duplicate`, `out_of_area`, `insert_conflict`,
`db_error`, `no_email`, `low_score`, `missing_required_field`,
`invalid_prospect`, and `pre_enrichment_reject`. The existing post-enrichment
filter remains exclusively in `excluded_prospect_log`; completed runs expose its
count as `excluded_filter` without duplicating those rows in `scout_skip_log`.

When adding a skip path, add its constant to `utils/scoutSkipLog.js`, increment
the run breakdown, and call `logScoutSkip` with the run, client, vertical,
location, discovery method, search query, candidate identifier, and useful
structured detail. A candidate must be assigned exactly one reason.

Daily diagnostics are available through `scout_skip_summary_7d`. The migration
also contains the one-hour post-deploy validation query.
