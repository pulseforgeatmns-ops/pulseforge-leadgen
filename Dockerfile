FROM ghcr.io/puppeteer/puppeteer:24

ENV NODE_ENV=production
ENV PORT=3000
ENV PUPPETEER_SKIP_DOWNLOAD=true

WORKDIR /app

COPY --chown=pptruser:pptruser package.json package-lock.json ./
RUN npm ci --omit=dev

COPY --chown=pptruser:pptruser . .

EXPOSE 3000

CMD ["node", "server.js"]
