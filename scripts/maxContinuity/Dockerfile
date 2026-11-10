# Explicitly selected for the staging one-shot only; never the default image.
FROM node:22-bookworm-slim
WORKDIR /runner
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts --omit=dev --no-audit --no-fund
COPY . .
USER node
# No EXPOSE, listener on PORT, browser, cron, healthcheck or application startup.
CMD ["node", "scripts/maxContinuity/run.cjs"]
