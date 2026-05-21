# ---- Dependencies stage ----
FROM node:22-alpine AS deps

WORKDIR /app

COPY package*.json ./
# --ignore-scripts skips postinstall; we run prisma generate explicitly below
RUN npm ci --ignore-scripts

COPY prisma ./prisma
COPY prisma.config.ts ./
# Generate Prisma client into generated/prisma/ — no DATABASE_URL needed at build time
RUN npx prisma generate

# ---- Production image ----
FROM node:22-alpine AS production

WORKDIR /app

RUN addgroup -S platform && adduser -S platform -G platform

COPY --from=deps --chown=platform:platform /app/node_modules ./node_modules
COPY --from=deps --chown=platform:platform /app/generated ./generated
COPY --chown=platform:platform . .

USER platform

EXPOSE 3075

CMD ["node", "src/index.js"]
