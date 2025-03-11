import { describe, expect, it } from "vitest";
import { shouldLog } from "./logging";

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
});
