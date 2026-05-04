import { ConvexError, type Value } from "convex/values";

const NON_RETRYABLE_ERROR_MARKER = "__convexWorkpoolNonRetryable";

type NonRetryableErrorMarker = {
  readonly [NON_RETRYABLE_ERROR_MARKER]: true;
};

type NonRetryableErrorData<TData extends Value = Value> =
  NonRetryableErrorMarker & {
    message: string;
    data?: TData;
  };

type NonRetryableErrorOptions<TData extends Value = Value> = ErrorOptions & {
  data?: TData;
};

/**
 * Throw this from workpool jobs when the failure should finish immediately
 * instead of using the configured retry behavior.
 */
export class NonRetryableError<TData extends Value = never> extends ConvexError<
  NonRetryableErrorData<TData>
> {
  constructor(message: string, options?: NonRetryableErrorOptions<TData>) {
    super({
      [NON_RETRYABLE_ERROR_MARKER]: true,
      message,
      ...(options?.data === undefined ? {} : { data: options.data }),
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
  if (error instanceof NonRetryableError) {
    return getNonRetryableErrorData(error.data)?.message;
  }
  if (error instanceof ConvexError) {
    return getNonRetryableErrorData(error.data)?.message;
  }
  return undefined;
}

function getNonRetryableErrorData(
  data: unknown,
): NonRetryableErrorData | undefined {
  if (isNonRetryableErrorData(data)) {
    return data;
  }
  if (typeof data === "string") {
    try {
      const parsed = JSON.parse(data);
      if (isNonRetryableErrorData(parsed)) {
        return parsed;
      }
    } catch {
      return undefined;
    }
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
