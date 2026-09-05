import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "edge-runtime",
    include: ["src/**/*.test.ts", "example/convex/**/*.test.ts"],
  },
});
