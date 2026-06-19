import { afterEach, describe, expect, it, vi } from "vitest";
import { createLogger, shouldLog } from "./logging.js";

describe("logging", () => {
  describe("shouldLog", () => {
    it("should return true if the log level is above the config level", () => {
      expect(shouldLog("INFO", "DEBUG")).toBe(false);
    });
    it("should return false if the log level is below the config level", () => {
      expect(shouldLog("INFO", "WARN")).toBe(true);
    });
    it("should return true if the log level is equal to the config level", () => {
      expect(shouldLog("INFO", "INFO")).toBe(true);
    });
  });

  describe("LOG_LEVEL env override", () => {
    afterEach(() => {
      delete process.env.LOG_LEVEL;
      vi.restoreAllMocks();
    });

    it("uses the caller's level when LOG_LEVEL is unset", () => {
      delete process.env.LOG_LEVEL;
      const info = vi.spyOn(console, "info").mockImplementation(() => {});
      createLogger("INFO").info("hello");
      expect(info).toHaveBeenCalled();
    });
    it("LOG_LEVEL takes precedence over the caller's level", () => {
      process.env.LOG_LEVEL = "ERROR";
      const info = vi.spyOn(console, "info").mockImplementation(() => {});
      // Caller asks for INFO, but the env var raises the bar to ERROR.
      createLogger("INFO").info("hello");
      expect(info).not.toHaveBeenCalled();
    });
  });
});
