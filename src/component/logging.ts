import { v, Infer } from "convex/values";

export const DEFAULT_LOG_LEVEL: LogLevel = "REPORT";

export const logLevel = v.union(
  v.literal("DEBUG"),
  v.literal("INFO"),
  v.literal("REPORT"),
  v.literal("WARN"),
  v.literal("ERROR")
);
export type LogLevel = Infer<typeof logLevel>;

export type Logger = {
  debug: (...args: unknown[]) => void;
  warn: (...args: unknown[]) => void;
  error: (...args: unknown[]) => void;
  time: (label: string) => void;
  timeEnd: (label: string) => void;
  event: (event: string, payload: Record<string, unknown>) => void;
};

const logLevelOrder = logLevel.members.map((l) => l.value);
const logLevelOrderMap = logLevelOrder.reduce(
  (acc, l, i) => {
    acc[l] = i;
    return acc;
  },
  {} as Record<LogLevel, number>
);
const DEBUG = logLevelOrderMap["DEBUG"];
const INFO = logLevelOrderMap["INFO"];
const REPORT = logLevelOrderMap["REPORT"];
const WARN = logLevelOrderMap["WARN"];
const ERROR = logLevelOrderMap["ERROR"];

export function createLogger(level?: LogLevel): Logger {
  const levelIndex = logLevelOrderMap[level ?? DEFAULT_LOG_LEVEL];
  if (levelIndex === undefined) {
    throw new Error(`Invalid log level: ${level}`);
  }
  return {
    debug: (...args: unknown[]) => {
      if (levelIndex <= DEBUG) {
        console.debug(...args);
      }
    },
    warn: (...args: unknown[]) => {
      if (levelIndex <= WARN) {
        console.warn(...args);
      }
    },
    error: (...args: unknown[]) => {
      if (levelIndex <= ERROR) {
        console.error(...args);
      }
    },
    time: (label: string) => {
      if (levelIndex <= DEBUG) {
        console.time(label);
      }
    },
    timeEnd: (label: string) => {
      if (levelIndex <= DEBUG) {
        console.timeEnd(label);
      }
    },
    event: (event: string, payload: Record<string, unknown>) => {
      const fullPayload = {
        event,
        ...payload,
      };
      if (levelIndex === REPORT && event === "report") {
        console.info(JSON.stringify(fullPayload));
      } else if (levelIndex <= INFO) {
        console.info(JSON.stringify(fullPayload));
      }
    },
  };
}
