# ADR-106: Fixed CRM-only canonical preparation

Status: Local in progress, SPEC263. This is a separate follow-on to frozen SPEC262.

Use the existing canonical deterministic preparation pipeline with a fixed, typed, exact CRM-only discovery adapter. Never use normal discovery's empty-inventory fallback. Reserve the preparation operation durably and revalidate current source/program/identity/quarantine under existing locks. No arbitrary caller callbacks, provider calls or DDL. Stop at review-only READY and explicitly report provider readiness not checked; sending needs its original live gates and separate approval.

The current live batch is entirely held. This operation is for future genuinely fresh approved inventory; it cannot relabel attempted prospects as fresh. Fixed child/operation identity prevents grant/day/caller-ID retry resets. Legacy initialization and preparation behavior is unchanged unless this explicit operation is selected.

Draft preparation consumes one item unit per selected identity; later freeze is separately charged. The manual follow-on DDL extends only the two existing consumption CHECK constraints, preserving all rows and triggers. Unknown provider/authentication/blacklist evidence stays unknown in artifact explanations; native planning is not verification.
