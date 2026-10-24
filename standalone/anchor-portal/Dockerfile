# Build from repository root with -f standalone/anchor-portal/Dockerfile.
# This is a distinct runtime, never the production application Dockerfile.
FROM node:24-bookworm-slim
WORKDIR /app
COPY standalone/anchor-portal/package.json standalone/anchor-portal/package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force
COPY standalone/anchor-portal ./standalone/anchor-portal
COPY services/anchorPortal.js services/anchorPortalServiceRequests.js ./services/
COPY routes/anchorPortal.js ./routes/anchorPortal.js
COPY utils/anchorPortalSchema.js utils/anchorPortalImages.js utils/usersRoleConstraint.js utils/userRoles.js ./utils/
COPY public/anchor-portal.html public/anchor-portal.js ./public/
USER node
CMD ["node", "standalone/anchor-portal/runtime.js"]
