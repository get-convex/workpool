import "./App.css";
import { useState, useMemo, useEffect } from "react";
import { useQuery, useAction } from "convex/react";
import { api } from "../convex/_generated/api";
import type { Id } from "../convex/_generated/dataModel";
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

type RunId = Id<"runs">;
type Tab = "history" | "detail" | "compare" | "run";

type PoolKind = "0.4.7" | "0.4.6" | "0.4.2";
// Legacy values still appear on historical run docs.
type PoolValue = PoolKind | "new" | "old";

const POOL_CSS_CLASS: Record<PoolValue | "none", string> = {
  "0.4.7": "new",
  new: "new",
  "0.4.6": "old",
  old: "old",
  "0.4.2": "older",
  none: "none",
};

function PoolBadge({ pool }: { pool?: PoolValue }) {
  const cls = POOL_CSS_CLASS[pool ?? "none"];
  return <span className={`pool-badge ${cls}`}>{pool ?? "—"}</span>;
}

function fmt(ms: number | undefined): string {
  if (ms === undefined) return "—";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

function fmtTime(t: number): string {
  return new Date(t).toLocaleString();
}

type CompareIds = [RunId | null, RunId | null, RunId | null];

const SLOT_LABELS = ["A", "B", "C"] as const;
const SLOT_COLORS = ["#4f8cff", "#ff8c4f", "#5cc97a"] as const;

type UrlState = {
  tab: Tab;
  selectedRunId: RunId | null;
  compareIds: CompareIds;
};

function serializeUrlState(s: UrlState): string {
  switch (s.tab) {
    case "detail":
      return s.selectedRunId ? `detail/${s.selectedRunId}` : "history";
    case "compare": {
      const ids = s.compareIds.filter((x): x is RunId => x !== null);
      return ids.length > 0 ? `compare/${ids.join(",")}` : "compare";
    }
    case "run":
      return "new";
    case "history":
    default:
      return "history";
  }
}

function parseUrlHash(hash: string): Partial<UrlState> {
  const h = hash.replace(/^#\/?/, "");
  if (!h || h === "history") return { tab: "history" };
  if (h === "new") return { tab: "run" };
  if (h === "compare") return { tab: "compare" };
  const detailMatch = h.match(/^detail\/(.+)$/);
  if (detailMatch) {
    return { tab: "detail", selectedRunId: detailMatch[1] as RunId };
  }
  const compareMatch = h.match(/^compare\/(.+)$/);
  if (compareMatch) {
    const parts = compareMatch[1].split(",").slice(0, 3);
    const ids: CompareIds = [null, null, null];
    parts.forEach((p, i) => {
      if (p) ids[i] = p as RunId;
    });
    return { tab: "compare", compareIds: ids };
  }
  return { tab: "history" };
}

function readHashState(): UrlState {
  const parsed = parseUrlHash(window.location.hash);
  return {
    tab: parsed.tab ?? "history",
    selectedRunId: parsed.selectedRunId ?? null,
    compareIds: parsed.compareIds ?? [null, null, null],
  };
}

function App() {
  const initial = readHashState();
  const [tab, setTab] = useState<Tab>(initial.tab);
  const [selectedRunId, setSelectedRunId] = useState<RunId | null>(
    initial.selectedRunId,
  );
  const [compareIds, setCompareIds] = useState<CompareIds>(initial.compareIds);

  // Sync state → hash.
  useEffect(() => {
    const next = serializeUrlState({ tab, selectedRunId, compareIds });
    const current = window.location.hash.replace(/^#\/?/, "");
    if (next !== current) {
      const url = `${window.location.pathname}${window.location.search}#${next}`;
      window.history.replaceState(null, "", url);
    }
  }, [tab, selectedRunId, compareIds]);

  // Sync hash → state (back/forward, pasted URLs).
  useEffect(() => {
    const onHashChange = () => {
      const s = readHashState();
      setTab(s.tab);
      setSelectedRunId(s.selectedRunId);
      setCompareIds(s.compareIds);
    };
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  return (
    <>
      <h1>Workpool Dashboard</h1>
      <nav className="tabs">
        <button
          className={tab === "history" ? "active" : ""}
          onClick={() => setTab("history")}
        >
          History
        </button>
        <button
          className={tab === "detail" ? "active" : ""}
          onClick={() => setTab("detail")}
          disabled={!selectedRunId}
        >
          Detail
        </button>
        <button
          className={tab === "compare" ? "active" : ""}
          onClick={() => setTab("compare")}
        >
          Compare
        </button>
        <button
          className={tab === "run" ? "active" : ""}
          onClick={() => setTab("run")}
        >
          Run scenario
        </button>
      </nav>

      {tab === "history" && (
        <History
          onPick={(id) => {
            setSelectedRunId(id);
            setTab("detail");
          }}
          onCompare={(ids) => {
            setCompareIds(ids);
            setTab("compare");
          }}
        />
      )}
      {tab === "detail" && selectedRunId && <RunDetail runId={selectedRunId} />}
      {tab === "compare" && <Compare ids={compareIds} setIds={setCompareIds} />}
      {tab === "run" && <RunScenarioForm onStarted={() => setTab("history")} />}
    </>
  );
}

function History({
  onPick,
  onCompare,
}: {
  onPick: (id: RunId) => void;
  onCompare: (ids: CompareIds) => void;
}) {
  const runs = useQuery(api.test.dashboard.listRuns, { limit: 100 });
  const [slots, setSlots] = useState<CompareIds>([null, null, null]);
  const setSlot = (i: 0 | 1 | 2, id: RunId | null) => {
    const next = [...slots] as CompareIds;
    next[i] = id;
    // Clear duplicates in other slots.
    for (let j = 0; j < 3; j++) {
      if (j !== i && next[j] === id) next[j] = null;
    }
    setSlots(next);
  };

  const picked = slots.filter((s): s is RunId => s !== null);
  const canCompare = picked.length >= 2;

  if (runs === undefined) return <p className="muted">Loading…</p>;
  if (runs.length === 0)
    return <p className="muted">No runs yet. Use “Run scenario”.</p>;

  return (
    <div className="card">
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "0.5rem" }}>
        <button
          className="primary"
          disabled={!canCompare}
          onClick={() => onCompare(slots)}
        >
          Compare selected
        </button>
        <span className="muted" style={{ alignSelf: "center" }}>
          {picked.length === 0
            ? "select up to 3 runs (A, B, C)"
            : `${picked.length} selected${picked.length < 2 ? " — need at least 2" : ""}`}
        </span>
      </div>
      <table>
        <thead>
          <tr>
            {SLOT_LABELS.map((l) => (
              <th key={l}>{l}</th>
            ))}
            <th>Scenario</th>
            <th>Pool</th>
            <th>Status</th>
            <th>Tasks</th>
            <th>Duration</th>
            <th>p50</th>
            <th>p95</th>
            <th>p99</th>
            <th>Started</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((r) => (
            <HistoryRow
              key={r._id}
              row={r}
              slots={slots}
              setSlot={setSlot}
              onPick={onPick}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}

type HistoryRowData = {
  _id: RunId;
  scenario: string;
  pool?: string;
  startTime: number;
  taskCount?: number;
};

function HistoryRow({
  row,
  slots,
  setSlot,
  onPick,
}: {
  row: HistoryRowData;
  slots: CompareIds;
  setSlot: (i: 0 | 1 | 2, id: RunId | null) => void;
  onPick: (id: RunId) => void;
}) {
  const run = useQuery(api.test.dashboard.getRun, { runId: row._id });
  return (
    <tr
      className="clickable"
      onClick={(e) => {
        if ((e.target as HTMLElement).tagName === "INPUT") return;
        onPick(row._id);
      }}
    >
      {SLOT_LABELS.map((label, i) => (
        <td key={label}>
          <input
            type="radio"
            name={`slot-${label}`}
            checked={slots[i] === row._id}
            onChange={() => setSlot(i as 0 | 1 | 2, row._id)}
          />
        </td>
      ))}
      <td>{row.scenario}</td>
      <td>
        <PoolBadge pool={row.pool as PoolValue | undefined} />
      </td>
      <td className={run ? `status-${run.status}` : "muted"}>
        {run ? run.status : "…"}
      </td>
      <td>
        {run ? run.completedCount : "…"}/{row.taskCount ?? "?"}
      </td>
      <td>{fmt(run?.totalDurationMs)}</td>
      <td>{fmt(run?.latency?.p50)}</td>
      <td>{fmt(run?.latency?.p95)}</td>
      <td>{fmt(run?.latency?.p99)}</td>
      <td className="muted">{fmtTime(row.startTime)}</td>
    </tr>
  );
}

function RunDetail({ runId }: { runId: RunId }) {
  const run = useQuery(api.test.dashboard.getRun, { runId });
  const throughput = useQuery(api.test.dashboard.throughputOverTime, {
    runId,
    bucketMs: 500,
  });
  const cdf = useQuery(api.test.dashboard.latencyCdf, { runId });

  if (run === undefined) return <p className="muted">Loading…</p>;
  if (run === null) return <p className="muted">Run not found.</p>;

  return (
    <>
      <div className="card">
        <div style={{ display: "flex", gap: "1rem", alignItems: "center" }}>
          <h2 style={{ margin: 0 }}>{run.scenario}</h2>
          <PoolBadge pool={run.pool as PoolValue | undefined} />
          <span className={`status-${run.status}`}>{run.status}</span>
          <span className="muted">{fmtTime(run.startTime)}</span>
        </div>
        <div className="metric-grid">
          <Metric
            label="Completed"
            value={`${run.completedCount}/${run.taskCount ?? "?"}`}
          />
          <Metric label="Duration" value={fmt(run.totalDurationMs)} />
          <Metric label="p50" value={fmt(run.latency?.p50)} />
          <Metric label="p95" value={fmt(run.latency?.p95)} />
          <Metric label="p99" value={fmt(run.latency?.p99)} />
          <Metric label="max" value={fmt(run.latency?.max)} />
          <Metric
            label="tps"
            value={
              run.totalDurationMs && run.completedCount
                ? `${Math.round((run.completedCount / run.totalDurationMs) * 1000)}`
                : "—"
            }
          />
        </div>
        <details>
          <summary className="muted">parameters</summary>
          <pre className="params">
            {JSON.stringify(run.parameters, null, 2)}
          </pre>
        </details>
      </div>

      <div className="charts">
        <ChartCard title="Throughput over time (per 500ms bucket)">
          <ResponsiveContainer width="100%" height={240}>
            <AreaChart
              data={throughput?.points ?? []}
              margin={{ top: 8, right: 16, bottom: 8, left: 0 }}
            >
              <CartesianGrid strokeOpacity={0.15} />
              <XAxis
                dataKey="tMs"
                tickFormatter={(t) => `${(t / 1000).toFixed(1)}s`}
              />
              <YAxis />
              <Tooltip
                labelFormatter={(t) =>
                  `t=${((t as number) / 1000).toFixed(2)}s`
                }
              />
              <Legend />
              <Area
                type="monotone"
                dataKey="inFlight"
                stroke="#b8a352"
                fill="#b8a35233"
                name="in flight"
              />
              <Area
                type="monotone"
                dataKey="completed"
                stroke="#5cc97a"
                fill="#5cc97a33"
                name="completed/bucket"
              />
              <Area
                type="monotone"
                dataKey="enqueued"
                stroke="#4f8cff"
                fill="#4f8cff33"
                name="enqueued/bucket"
              />
            </AreaChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Latency CDF">
          <ResponsiveContainer width="100%" height={240}>
            <LineChart
              data={cdf ?? []}
              margin={{ top: 8, right: 16, bottom: 8, left: 0 }}
            >
              <CartesianGrid strokeOpacity={0.15} />
              <XAxis
                dataKey="ms"
                tickFormatter={(t) =>
                  t < 1000 ? `${t}ms` : `${(t / 1000).toFixed(1)}s`
                }
              />
              <YAxis domain={[0, 100]} unit="%" />
              <Tooltip
                labelFormatter={(t) =>
                  `${(t as number) < 1000 ? `${t}ms` : `${((t as number) / 1000).toFixed(2)}s`}`
                }
              />
              <Line
                type="monotone"
                dataKey="pct"
                stroke="#4f8cff"
                dot={false}
                name="cumulative %"
              />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </>
  );
}

function Compare({
  ids,
  setIds,
}: {
  ids: CompareIds;
  setIds: (ids: CompareIds) => void;
}) {
  const runs = useQuery(api.test.dashboard.listRuns, { limit: 100 });
  const [a, b, c] = ids;

  const runA = useQuery(api.test.dashboard.getRun, a ? { runId: a } : "skip");
  const runB = useQuery(api.test.dashboard.getRun, b ? { runId: b } : "skip");
  const runC = useQuery(api.test.dashboard.getRun, c ? { runId: c } : "skip");

  const tA = useQuery(
    api.test.dashboard.throughputOverTime,
    a ? { runId: a, bucketMs: 500 } : "skip",
  );
  const tB = useQuery(
    api.test.dashboard.throughputOverTime,
    b ? { runId: b, bucketMs: 500 } : "skip",
  );
  const tC = useQuery(
    api.test.dashboard.throughputOverTime,
    c ? { runId: c, bucketMs: 500 } : "skip",
  );

  const cdfA = useQuery(
    api.test.dashboard.latencyCdf,
    a ? { runId: a } : "skip",
  );
  const cdfB = useQuery(
    api.test.dashboard.latencyCdf,
    b ? { runId: b } : "skip",
  );
  const cdfC = useQuery(
    api.test.dashboard.latencyCdf,
    c ? { runId: c } : "skip",
  );

  const runsBySlot = [runA, runB, runC];
  const throughputsBySlot = [tA, tB, tC];
  const cdfsBySlot = [cdfA, cdfB, cdfC];

  const throughputData = useMemo(() => {
    const ptsBySlot = throughputsBySlot.map((t) => t?.points ?? []);
    const len = Math.max(0, ...ptsBySlot.map((p) => p.length));
    const out: Array<Record<string, number | undefined>> = [];
    for (let i = 0; i < len; i++) {
      const tMs =
        ptsBySlot.find((p) => p[i] !== undefined)?.[i]?.tMs ?? i * 500;
      const row: Record<string, number | undefined> = { tMs };
      ptsBySlot.forEach((pts, slotIdx) => {
        row[`completed_${SLOT_LABELS[slotIdx]}`] = pts[i]?.completed;
      });
      out.push(row);
    }
    return out;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tA, tB, tC]);

  const cdfData = useMemo(() => {
    const points: Array<Record<string, number | undefined>> = [];
    cdfsBySlot.forEach((arr, slotIdx) => {
      (arr ?? []).forEach((p) => {
        points.push({ ms: p.ms, [`pct_${SLOT_LABELS[slotIdx]}`]: p.pct });
      });
    });
    points.sort((x, y) => (x.ms ?? 0) - (y.ms ?? 0));
    return points;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cdfA, cdfB, cdfC]);

  const setSlot = (i: 0 | 1 | 2, value: RunId | null) => {
    const next = [...ids] as CompareIds;
    next[i] = value;
    setIds(next);
  };

  const loadedRuns = runsBySlot.filter(
    (r): r is NonNullable<typeof runA> => !!r,
  );

  return (
    <>
      <div className="card">
        <div className="form-row">
          {SLOT_LABELS.map((label, i) => (
            <label key={label}>
              <span style={{ color: SLOT_COLORS[i] }}>● </span>Run {label}
              <select
                value={ids[i] ?? ""}
                onChange={(e) =>
                  setSlot(
                    i as 0 | 1 | 2,
                    (e.target.value || null) as RunId | null,
                  )
                }
              >
                <option value="">— select —</option>
                {(runs ?? []).map((r) => (
                  <option key={r._id} value={r._id}>
                    {r.scenario} [{r.pool ?? "?"}] · {fmtTime(r.startTime)}
                  </option>
                ))}
              </select>
            </label>
          ))}
        </div>
      </div>

      {loadedRuns.length >= 2 && (
        <div className="card">
          <h2>Summary delta</h2>
          <DeltaTable runs={runsBySlot} />
        </div>
      )}

      <div className="charts">
        <ChartCard title="Throughput — completed per bucket">
          <ResponsiveContainer width="100%" height={260}>
            <LineChart
              data={throughputData}
              margin={{ top: 8, right: 16, bottom: 8, left: 0 }}
            >
              <CartesianGrid strokeOpacity={0.15} />
              <XAxis
                dataKey="tMs"
                tickFormatter={(t) => `${(t / 1000).toFixed(1)}s`}
              />
              <YAxis />
              <Tooltip />
              <Legend />
              {SLOT_LABELS.map((label, i) =>
                ids[i] ? (
                  <Line
                    key={label}
                    type="monotone"
                    dataKey={`completed_${label}`}
                    stroke={SLOT_COLORS[i]}
                    dot={false}
                    name={label}
                  />
                ) : null,
              )}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Latency CDF (lower-and-leftward = better)">
          <ResponsiveContainer width="100%" height={260}>
            <LineChart
              data={cdfData}
              margin={{ top: 8, right: 16, bottom: 8, left: 0 }}
            >
              <CartesianGrid strokeOpacity={0.15} />
              <XAxis
                dataKey="ms"
                tickFormatter={(t) =>
                  t < 1000 ? `${t}ms` : `${(t / 1000).toFixed(1)}s`
                }
              />
              <YAxis domain={[0, 100]} unit="%" />
              <Tooltip />
              <Legend />
              {SLOT_LABELS.map((label, i) =>
                ids[i] ? (
                  <Line
                    key={label}
                    type="monotone"
                    dataKey={`pct_${label}`}
                    stroke={SLOT_COLORS[i]}
                    dot={false}
                    connectNulls
                    name={label}
                  />
                ) : null,
              )}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </>
  );
}

type RunDoc = NonNullable<
  ReturnType<typeof useQuery<typeof api.test.dashboard.getRun>>
>;

function DeltaTable({ runs }: { runs: Array<RunDoc | null | undefined> }) {
  const rows: Array<{
    label: string;
    pick: (r: RunDoc) => number | undefined;
    lower: boolean;
  }> = [
    {
      label: "Total duration (ms)",
      pick: (r) => r.totalDurationMs,
      lower: true,
    },
    { label: "p50 (ms)", pick: (r) => r.latency?.p50, lower: true },
    { label: "p95 (ms)", pick: (r) => r.latency?.p95, lower: true },
    { label: "p99 (ms)", pick: (r) => r.latency?.p99, lower: true },
    { label: "max (ms)", pick: (r) => r.latency?.max, lower: true },
  ];

  // Find the first present run to act as the baseline for deltas.
  const baselineIdx = runs.findIndex((r) => !!r);
  const baseline = baselineIdx >= 0 ? runs[baselineIdx] : null;
  const baselineLabel = baselineIdx >= 0 ? SLOT_LABELS[baselineIdx] : "—";

  return (
    <table>
      <thead>
        <tr>
          <th>Metric</th>
          {runs.map((r, i) =>
            r ? (
              <th key={SLOT_LABELS[i]} style={{ color: SLOT_COLORS[i] }}>
                {SLOT_LABELS[i]} ({r.scenario} · {r.pool ?? "?"})
              </th>
            ) : null,
          )}
          {runs.map((r, i) =>
            r && i !== baselineIdx ? (
              <th key={`d-${SLOT_LABELS[i]}`}>
                Δ {SLOT_LABELS[i]} vs {baselineLabel}
              </th>
            ) : null,
          )}
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => {
          const baseVal = baseline ? row.pick(baseline) : undefined;
          return (
            <tr key={row.label}>
              <td>{row.label}</td>
              {runs.map((r, i) =>
                r ? <td key={SLOT_LABELS[i]}>{row.pick(r) ?? "—"}</td> : null,
              )}
              {runs.map((r, i) => {
                if (!r || i === baselineIdx) return null;
                const v = row.pick(r);
                const delta =
                  v !== undefined && baseVal !== undefined && baseVal !== 0
                    ? ((v - baseVal) / baseVal) * 100
                    : undefined;
                const better =
                  delta !== undefined && (row.lower ? delta < 0 : delta > 0);
                return (
                  <td
                    key={`d-${SLOT_LABELS[i]}`}
                    className={better ? "delta-positive" : "delta-negative"}
                  >
                    {delta === undefined
                      ? "—"
                      : `${delta > 0 ? "+" : ""}${delta.toFixed(1)}%`}
                  </td>
                );
              })}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

const SCENARIO_PRESETS = {
  burstyBatches: {
    waveCount: 10,
    tasksPerWave: 20,
    delayBetweenWavesMs: 500,
    maxParallelism: 50,
    taskDurationMs: 0,
  },
  throughput: {
    taskCount: 1000,
    batchSize: 100,
    interBatchMs: 50,
    maxParallelism: 100,
    taskDurationMs: 20,
  },
  overhead: {
    taskCount: 500,
    batchSize: 50,
    interBatchMs: 0,
    mode: "pool",
    onComplete: false,
    maxParallelism: 50,
  },
  sustained: {
    targetTps: 50,
    durationSec: 20,
    workerMinMs: 50,
    workerMaxMs: 500,
    onComplete: false,
    maxParallelism: 100,
  },
  bigArgs: {
    taskCount: 30,
    argSizeBytes: 800000,
    maxParallelism: 30,
  },
  bigContext: {
    taskCount: 30,
    contextSizeBytes: 800000,
    maxParallelism: 30,
  },
  bigReturnTypes: {
    taskCount: 20,
    returnSizeBytes: 1000000,
    maxParallelism: 20,
  },
} as const satisfies Record<string, Record<string, unknown>>;

type ScenarioName = keyof typeof SCENARIO_PRESETS;

function RunScenarioForm({ onStarted }: { onStarted: () => void }) {
  const runScenarios = useAction(api.test.dashboard.runScenarios);
  const [scenario, setScenario] = useState<ScenarioName>("burstyBatches");
  const [pool, setPool] = useState<PoolKind | "all">("0.4.7");
  const [paramsText, setParamsText] = useState<string>(
    JSON.stringify(SCENARIO_PRESETS[scenario], null, 2),
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const updateScenario = (next: ScenarioName) => {
    setScenario(next);
    setParamsText(JSON.stringify(SCENARIO_PRESETS[next], null, 2));
    setError(null);
  };

  const launch = async () => {
    setError(null);
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(paramsText);
    } catch (e) {
      setError(`Invalid JSON: ${(e as Error).message}`);
      return;
    }
    setBusy(true);
    try {
      const launches: Array<PoolKind> =
        pool === "all" ? ["0.4.2", "0.4.6", "0.4.7"] : [pool];
      const argsList = launches.map((p) => ({
        ...parsed,
        pool: p,
      }));
      await runScenarios({ scenario, argsList });
      onStarted();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="card">
      <div className="form-row">
        <label>
          Scenario
          <select
            value={scenario}
            onChange={(e) => updateScenario(e.target.value as ScenarioName)}
          >
            {Object.keys(SCENARIO_PRESETS).map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
        <label>
          Pool
          <select
            value={pool}
            onChange={(e) => setPool(e.target.value as PoolKind | "all")}
          >
            <option value="0.4.7">0.4.7 (this branch)</option>
            <option value="0.4.6">0.4.6 (published, with cooldown)</option>
            <option value="0.4.2">0.4.2 (pre-cooldown)</option>
            <option value="all">all (sequential)</option>
          </select>
        </label>
      </div>
      <label>
        Parameters (JSON)
        <textarea
          value={paramsText}
          onChange={(e) => setParamsText(e.target.value)}
          spellCheck={false}
        />
      </label>
      {error && (
        <p style={{ color: "#d96363", fontSize: "0.85rem" }}>{error}</p>
      )}
      <button className="primary" onClick={launch} disabled={busy}>
        {busy ? "Starting…" : "Run"}
      </button>
      <p className="muted" style={{ fontSize: "0.8rem", marginTop: "1rem" }}>
        Tip: pick “all” to run the same scenario back-to-back on each version
        (0.4.2 → 0.4.6 → 0.4.7), then compare them under “Compare”. The
        dashboard waits for each run to finish (plus a short buffer for the
        runner's 5s reentry guard) before starting the next, so the button stays
        busy for the full duration.
      </p>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
    </div>
  );
}

function ChartCard({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div className="card">
      <h2 style={{ marginTop: 0 }}>{title}</h2>
      {children}
    </div>
  );
}

export default App;
