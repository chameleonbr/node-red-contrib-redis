"use strict";

module.exports = {
  testDir: "test/playwright",
  timeout: 45000,
  fullyParallel: false,
  workers: 1,
  reporter: [["list"]],
  use: {
    browserName: "chromium",
    headless: true,
    viewport: { width: 1440, height: 960 },
    actionTimeout: 10000,
    trace: "retain-on-failure",
  },
};
