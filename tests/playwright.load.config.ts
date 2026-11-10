import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './load',
  timeout: 900_000,
  workers: 1,
  retries: 0,
  reporter: [['list']],
  globalSetup: './helpers/global-setup.ts',
  outputDir: `${process.env.OUT_DIR ?? './out'}/playwright-artifacts`,
});
