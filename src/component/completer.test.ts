import { describe, expect, it } from "vitest";
import type { Id } from "./_generated/dataModel.js";
import type { CompleteJob } from "./complete.js";
import { createCompleter } from "./completer.js";

function job(n: number): CompleteJob {
  return {
    workId: `work${n}` as Id<"work">,
    attempt: 0,
    runResult: { kind: "success", returnValue: null },
  };
}

function deferred() {
  let resolve!: () => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("createCompleter", () => {
  it("commits the first job alone, right away", async () => {
    const calls: string[][] = [];
    const completer = createCompleter(
      async (jobs) => {
        calls.push(jobs.map((j) => j.workId));
      },
      async () => {},
    );
    await completer.add(job(1));
    expect(calls).toEqual([["work1"]]);
  });

  it("groups jobs that finish while a commit is in flight", async () => {
    const calls: string[][] = [];
    const commits = [deferred(), deferred()];
    const completer = createCompleter(
      async (jobs) => {
        calls.push(jobs.map((j) => j.workId));
        await commits[calls.length - 1]!.promise;
      },
      async () => {},
    );
    const first = completer.add(job(1));
    const second = completer.add(job(2));
    const third = completer.add(job(3));
    expect(calls).toEqual([["work1"]]);

    commits[0]!.resolve();
    await Promise.resolve();
    await Promise.resolve();
    expect(calls).toEqual([["work1"], ["work2", "work3"]]);

    commits[1]!.resolve();
    await Promise.all([first, second, third]);
    expect(calls).toHaveLength(2);
  });

  it("hands a failed commit's jobs to the fallback and keeps going", async () => {
    const fallback: string[][] = [];
    const commits = [deferred(), deferred()];
    let call = 0;
    const completer = createCompleter(
      async () => {
        await commits[call++]!.promise;
      },
      async (jobs) => {
        fallback.push(jobs.map((j) => j.workId));
      },
    );
    const first = completer.add(job(1));
    const second = completer.add(job(2));
    commits[0]!.reject(new Error("conflict"));
    commits[1]!.resolve();
    await Promise.all([first, second]);
    expect(fallback).toEqual([["work1"]]);
    expect(call).toBe(2);
  });

  it("starts a new commit after the previous one drained", async () => {
    const calls: string[][] = [];
    const completer = createCompleter(
      async (jobs) => {
        calls.push(jobs.map((j) => j.workId));
      },
      async () => {},
    );
    await completer.add(job(1));
    await completer.add(job(2));
    expect(calls).toEqual([["work1"], ["work2"]]);
  });
});
