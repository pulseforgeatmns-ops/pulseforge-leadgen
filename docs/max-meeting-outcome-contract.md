# Canonical meeting outcome contract

No reliable live source currently exists for cancellation, show, or no-show, so no adapter is enabled.

Accepted future sources must provide a stable provider event ID or immutable operator-disposition ID, exact prospect and client mapping, an explicit event timestamp, and either `confirmed_provider` or `confirmed_operator` confidence. Corrections append a new event referencing `correction_of_event_id`; existing events are never updated or deleted.

- `meeting_cancelled`: signed calendar/booking-provider cancellation event. The provider event ID is the source-record ID.
- `meeting_showed`: append-only operator or provider attendance disposition tied to the calendar/booking ID.
- `meeting_no_showed`: append-only operator or provider no-show disposition tied to the calendar/booking ID.

Elapsed time, missing status, `cal_queue` presence, and mutable CRM status are not canonical evidence. Invalid mappings or missing stable IDs fail before Max signal ingestion and do not affect calendar or booking behavior.
