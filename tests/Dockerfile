FROM node:22-bookworm-slim
WORKDIR /tests
COPY package.json package-lock.json ./
RUN npm ci && npx playwright install --with-deps chromium && rm -rf /var/lib/apt/lists/*
COPY . .
CMD ["bash", "run-all.sh"]
