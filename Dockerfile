FROM ghcr.io/puppeteer/puppeteer:24

ENV NODE_ENV=production
ENV PORT=3000

# PostgreSQL 18 client tools + python for the one-shot cutover restore receiver
# (scripts/migration/restoreFinalRender.py, PF_MIGRATION_RECEIVER=1 only).
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
  && install -d /usr/share/postgresql-common/pgdg \
  && curl -fsSo /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  && . /etc/os-release \
  && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends postgresql-client-18 python3 python3-psycopg2 python3-requests python3-cryptography \
  && rm -rf /var/lib/apt/lists/*
ENV PATH="/usr/lib/postgresql/18/bin:${PATH}"
USER pptruser

WORKDIR /app

COPY --chown=pptruser:pptruser package.json package-lock.json ./
RUN npm ci --omit=dev

COPY --chown=pptruser:pptruser . .

EXPOSE 3000

CMD ["sh", "-c", "if [ \"$PF_MIGRATION_RECEIVER\" = \"1\" ]; then exec python3 scripts/migration/restoreFinalRender.py; elif [ \"$PF_MIGRATION_STAGING\" = \"1\" ]; then node --test test/migrationSplitManifest.test.js test/signalDatabaseRouting.test.js test/migrationDatabaseTarget.test.js test/maxAnchorOperationalInspectionIntent.test.js test/restoreGatewayNov21Contract.test.js && exec node migrationStagingServer.js; else exec node server.js; fi"]
