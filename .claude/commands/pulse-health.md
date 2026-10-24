# Pulse Health

Run the Pulseforge health monitor manually.

```bash
node scripts/monitorQueries.js
```

For the production cron path, call:

```bash
curl -X POST "$APP_URL/cron/pulse-health?secret=$CRON_SECRET"
```

Report in PULSE HEALTH format:

1. ALERT LEVEL
2. DIGEST
3. SENDS LAST 24H
4. FAILURES BY VERTICAL
5. FAILURE SPIKE
6. WARM PROSPECTS
7. RECENT ERRORS
8. ACTION ITEM
