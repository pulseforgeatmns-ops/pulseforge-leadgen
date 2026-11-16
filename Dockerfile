FROM ghcr.io/puppeteer/puppeteer:24

ENV NODE_ENV=production
ENV PORT=3000

WORKDIR /app

COPY --chown=pptruser:pptruser package.json package-lock.json ./
RUN npm ci --omit=dev

COPY --chown=pptruser:pptruser . .

EXPOSE 3000

CMD ["sh", "-c", "if [ \"$PF_MIGRATION_STAGING\" = \"1\" ]; then exec node migrationStagingServer.js; else exec node server.js; fi"]
