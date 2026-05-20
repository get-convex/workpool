import { ConvexError } from "convex/values";

const NON_RETRYABLE_ERROR_MARKER = "__convexWorkpoolNonRetryable";

type NonRetryableErrorMarker = {
  readonly [NON_RETRYABLE_ERROR_MARKER]: true;
};

type NonRetryableErrorData = NonRetryableErrorMarker & {
  message: string;
};

/**
 * Throw this from workpool jobs when the failure should finish immediately
 * instead of using the configured retry behavior.
 */
export class NonRetryableError extends ConvexError<NonRetryableErrorData> {
  constructor(message: string, options?: ErrorOptions) {
    super({
      [NON_RETRYABLE_ERROR_MARKER]: true,
      message,
    });
    this.name = new.target.name;
    if (options?.cause !== undefined) {
      this.cause = options.cause;
    }
  }
}

export function isNonRetryableError(error: unknown): boolean {
  if (error instanceof NonRetryableError) {
    return true;
  }
  if (error instanceof ConvexError) {
    return getNonRetryableErrorData(error.data) !== undefined;
  }
  return (
    typeof error === "object" &&
    error !== null &&
    (error as Partial<NonRetryableErrorMarker>)[NON_RETRYABLE_ERROR_MARKER] ===
      true
  );
}

export function getNonRetryableErrorMessage(
  error: unknown,
): string | undefined {
  if (error instanceof ConvexError) {
    return getNonRetryableErrorData(error.data)?.message;
  }
  if (
    typeof error === "object" &&
    error !== null &&
    (error as Partial<NonRetryableErrorMarker>)[NON_RETRYABLE_ERROR_MARKER] ===
      true
  ) {
    const message = (error as Partial<NonRetryableErrorData>).message;
    return typeof message === "string" ? message : undefined;
  }
  return undefined;
}

function getNonRetryableErrorData(
  data: unknown,
): NonRetryableErrorData | undefined {
  if (isNonRetryableErrorData(data)) {
    return data;
  }
  return undefined;
}

function isNonRetryableErrorData(data: unknown): data is NonRetryableErrorData {
  if (
    typeof data === "object" &&
    data !== null &&
    (data as Partial<NonRetryableErrorMarker>)[NON_RETRYABLE_ERROR_MARKER] ===
      true &&
    typeof (data as Partial<NonRetryableErrorData>).message === "string"
  ) {
    return true;
  }
  return false;
}
