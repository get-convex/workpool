import { describe, expect, it } from "vitest";
import { ConvexError } from "convex/values";
import {
  getNonRetryableErrorMessage,
  isNonRetryableError,
  NonRetryableError,
} from "./errors.js";

class CustomTerminalError extends NonRetryableError {}

describe("errors", () => {
  it("identifies non-retryable error subclasses", () => {
    // Users should be able to model domain-specific terminal failures.
    const error = new CustomTerminalError("invalid input");

    expect(isNonRetryableError(error)).toBe(true);
    expect(getNonRetryableErrorMessage(error)).toBe("invalid input");
  });

  it("identifies ConvexError data from nested mutations", () => {
    // Nested mutations preserve ConvexError data instead of custom Error fields.
    const error = new ConvexError({
      __convexWorkpoolNonRetryable: true,
      message: "invalid input",
    });

    expect(isNonRetryableError(error)).toBe(true);
    expect(getNonRetryableErrorMessage(error)).toBe("invalid input");
  });

  it("identifies serialized ConvexError data", () => {
    // Some Convex boundaries surface ConvexError data as a serialized string.
    const error = new ConvexError(
      JSON.stringify({
        __convexWorkpoolNonRetryable: true,
        message: "invalid input",
      }),
    );

    expect(isNonRetryableError(error)).toBe(true);
    expect(getNonRetryableErrorMessage(error)).toBe("invalid input");
  });

  it("identifies marker-compatible errors", () => {
    // The marker path keeps detection working across duplicated package modules.
    const error = new Error("invalid input");
    Object.assign(error, { __convexWorkpoolNonRetryable: true });

    expect(isNonRetryableError(error)).toBe(true);
  });

  it("does not identify ordinary errors as non-retryable", () => {
    // Existing retry behavior depends on plain errors remaining retryable.
    expect(isNonRetryableError(new Error("temporary failure"))).toBe(false);
  });
});
