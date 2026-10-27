FROM node:22-bookworm-slim
WORKDIR /app
ENV NODE_ENV=production PUPPETEER_SKIP_DOWNLOAD=true PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund
COPY services/signalDashboard ./services/signalDashboard
COPY services/signalOpportunities ./services/signalOpportunities
COPY services/signalDistribution/risk.js services/signalDistribution/marketProvider.js services/signalDistribution/requestBudget.js ./services/signalDistribution/
COPY packages/signal-v1/acquisition/solanaAddress.js ./packages/signal-v1/acquisition/
COPY packages/signal-v1/prospective/caExtraction.js ./packages/signal-v1/prospective/
COPY routes/signalDashboard.js ./routes/
COPY public/signal-positions* ./public/
COPY migrations/2026-10-25-signal-dashboard*.sql ./migrations/
COPY standalone/signal-dashboard ./standalone/signal-dashboard
CMD ["node","standalone/signal-dashboard/server.js"]
