FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force && mkdir -p /logs && chown node:node /logs
COPY --from=build /app/dist ./dist
COPY src/schema.sql ./dist/schema.sql
COPY web ./web
COPY tools ./tools
USER node
EXPOSE 8080
CMD ["node", "dist/server.js"]
