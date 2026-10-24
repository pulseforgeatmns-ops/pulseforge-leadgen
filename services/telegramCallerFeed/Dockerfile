FROM node:22-bookworm-slim
WORKDIR /app
ENV NODE_ENV=production PORT=3099 PUPPETEER_SKIP_DOWNLOAD=true PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund
COPY services/telegramCallerFeed ./services/telegramCallerFeed
COPY packages/signal-v1/prospective/constants.js ./packages/signal-v1/prospective/constants.js
COPY packages/signal-v1/prospective/caExtraction.js ./packages/signal-v1/prospective/caExtraction.js
COPY packages/signal-v1/acquisition/solanaAddress.js ./packages/signal-v1/acquisition/solanaAddress.js
COPY services/signalOperator/operationGate.js ./services/signalOperator/operationGate.js
COPY services/signalOperator/pilotGate.js ./services/signalOperator/pilotGate.js
COPY services/signalOperator/runtimeLimits.js ./services/signalOperator/runtimeLimits.js
RUN mkdir -p /data
EXPOSE 3099
CMD ["node", "services/telegramCallerFeed/server.js"]
