import { LogLevel } from "./shared.js";

export const DEFAULT_LOG_LEVEL: LogLevel = "WARN";

export type Logger = {
  debug: (...args: unknown[]) => void;
  info: (...args: unknown[]) => void;
  warn: (...args: unknown[]) => void;
  error: (...args: unknown[]) => void;
  time: (label: string) => void;
  timeEnd: (label: string) => void;
};

export function createLogger(level?: LogLevel): Logger {
  const levelIndex = ["DEBUG", "INFO", "WARN", "ERROR"].indexOf(
    level ?? DEFAULT_LOG_LEVEL
  );
  if (levelIndex === -1) {
    throw new Error(`Invalid log level: ${level}`);
  }
  return {
    debug: (...args: unknown[]) => {
      if (levelIndex <= 0) {
        console.debug(...args);
      }
    },
    info: (...args: unknown[]) => {
      if (levelIndex <= 1) {
        console.info(...args);
      }
    },
    warn: (...args: unknown[]) => {
      if (levelIndex <= 2) {
        console.warn(...args);
      }
    },
    error: (...args: unknown[]) => {
      if (levelIndex <= 3) {
        console.error(...args);
      }
    },
    time: (label: string) => {
      if (levelIndex <= 0) {
        console.time(label);
      }
    },
    timeEnd: (label: string) => {
      if (levelIndex <= 0) {
        console.timeEnd(label);
      }
    },
  };
}
