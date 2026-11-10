import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './specs',
  timeout: 240_000,
  // One worker: the tests share one toxiproxy and read server-wide counters.
  workers: 1,
  fullyParallel: false,
  // No retries on purpose. A test that passes on the second try is hiding something.
  retries: 0,
  reporter: [['list']],
  globalSetup: './helpers/global-setup.ts',
  outputDir: `${process.env.OUT_DIR ?? './out'}/playwright-artifacts`,
  use: { trace: 'retain-on-failure', screenshot: 'only-on-failure' },
});
