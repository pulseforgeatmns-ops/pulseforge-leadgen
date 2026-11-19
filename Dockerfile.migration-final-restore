# Render one-shot restore receiver for the cutover. Creates NEW databases only.
FROM postgres:18-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-psycopg2 python3-requests ca-certificates \
  && rm -rf /var/lib/apt/lists/*
COPY scripts/migration/fingerprintSql.py scripts/migration/restoreFinalRender.py /usr/local/bin/
USER postgres
ENV PYTHONUNBUFFERED=1
CMD ["python3", "/usr/local/bin/restoreFinalRender.py"]
