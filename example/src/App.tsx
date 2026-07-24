import "./App.css";
import { useEffect, useMemo, useState } from "react";
import { useAction, useQuery } from "convex/react";
import { api } from "../convex/_generated/api";
import type { Id } from "../convex/_generated/dataModel";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type RunId = Id<"runs">;
type CompareIds = { old: RunId | null; current: RunId | null };
type RunData = NonNullable<
  ReturnType<typeof useQuery<typeof api.test.dashboard.getRun>>
>;

const CURRENT_COLOR = "#16a085";
const OLD_COLOR = "#d97706";

function formatDuration(ms: number | undefined): string {
  if (ms === undefined) return "—";
  if (ms < 1_000) return `${Math.round(ms)} ms`;
  return `${(ms / 1_000).toFixed(ms < 10_000 ? 2 : 1)} s`;
}

function formatNumber(value: number | undefined): string {
  return value === undefined ? "—" : new Intl.NumberFormat().format(value);
}

function formatTime(value: number): string {
  return new Date(value).toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function throughput(run: RunData): number | undefined {
  if (!run.totalDurationMs || !run.completedCount) return undefined;
  return (run.completedCount / run.totalDurationMs) * 1_000;
}

function improvement(
  baseline: number | undefined,
  current: number | undefined,
  lowerIsBetter: boolean,
): number | undefined {
  if (baseline === undefined || current === undefined || baseline === 0) {
    return undefined;
  }
  return lowerIsBetter
    ? ((baseline - current) / baseline) * 100
    : ((current - baseline) / baseline) * 100;
}

function readIdsFromHash(): CompareIds {
  const match = window.location.hash.match(/^#compare\/([^,]+),([^,]+)$/);
  return match
    ? { old: match[1] as RunId, current: match[2] as RunId }
    : { old: null, current: null };
}

function App() {
  const [ids, setIds] = useState<CompareIds>(readIdsFromHash);
  const [linked, setLinked] = useState(true);
  const runs = useQuery(api.test.dashboard.listRuns, { limit: 100 });

  const selectRun = (side: "old" | "current", runId: RunId | null) => {
    if (!runId) {
      setIds((current) => ({ ...current, [side]: null }));
      return;
    }
    const selected = runs?.find((run) => run._id === runId);
    if (!linked || !selected || !runs) {
      setIds((current) => ({ ...current, [side]: runId }));
      return;
    }
    const otherSide = side === "old" ? "current" : "old";
    const counterpart = findCounterpart(selected, runs);
    setIds({
      [side]: runId,
      [otherSide]: counterpart?._id ?? null,
    } as CompareIds);
  };

  const changeLinked = (nextLinked: boolean) => {
    setLinked(nextLinked);
    if (!nextLinked || !runs) return;
    const selected = runs.find(
      (run) => run._id === (ids.current ?? ids.old),
    );
    if (!selected) return;
    const counterpart = findCounterpart(selected, runs);
    if (!counterpart) return;
    const selectedIsOld = selected.pool === "old";
    setIds({
      old: selectedIsOld ? selected._id : counterpart._id,
      current: selectedIsOld ? counterpart._id : selected._id,
    });
  };

  useEffect(() => {
    if (ids.old && ids.current) {
      window.history.replaceState(
        null,
        "",
        `${window.location.pathname}${window.location.search}#compare/${ids.old},${ids.current}`,
      );
    }
  }, [ids]);

  return (
    <main>
      <header className="page-header">
        <div>
          <p className="eyebrow">Workpool benchmark lab</p>
          <h1>See what the new worker changes.</h1>
          <p className="page-intro">
            Run the same workload against both implementations, then compare
            speed, tail latency, throughput, and scheduler pressure in one view.
          </p>
        </div>
        <div className="legend-pills" aria-label="Comparison legend">
          <span className="legend-pill current">Current branch</span>
          <span className="legend-pill old">Workpool 0.4.7</span>
        </div>
      </header>

      <ComparisonRunner
        onCompleted={(oldRunId, currentRunId) =>
          setIds({ old: oldRunId, current: currentRunId })
        }
      />

      <RunPicker
        runs={runs ?? []}
        ids={ids}
        linked={linked}
        onLinkedChange={changeLinked}
        onSelect={selectRun}
      />
      <Comparison ids={ids} />
      <RunHistory runs={runs ?? []} ids={ids} onSelect={selectRun} />
    </main>
  );
}

const SCENARIOS = {
  burstyBatches: {
    label: "Bursty batches",
    description: "Shows scheduler churn while work arrives in repeated waves.",
    parameters: {
      waveCount: 10,
      tasksPerWave: 20,
      delayBetweenWavesMs: 500,
      maxParallelism: 50,
      taskDurationMs: 0,
      taskType: "action",
    },
  },
  throughput: {
    label: "Sustained throughput",
    description: "Saturates both pools and compares steady-state completion rate.",
    parameters: {
      taskCount: 1000,
      batchSize: 100,
      interBatchMs: 50,
      maxParallelism: 100,
      taskDurationMs: 20,
      taskType: "action",
    },
  },
  overhead: {
    label: "Pool overhead",
    description: "Measures the wall-clock cost of scheduling small mutations.",
    parameters: {
      taskCount: 500,
      batchSize: 50,
      interBatchMs: 0,
      mode: "pool",
      onComplete: false,
      maxParallelism: 50,
    },
  },
  sustained: {
    label: "Mixed-duration load",
    description: "Maintains a target rate while task duration varies.",
    parameters: {
      targetTps: 50,
      durationSec: 20,
      workerMinMs: 50,
      workerMaxMs: 500,
      onComplete: false,
      maxParallelism: 100,
    },
  },
  bigArgs: {
    label: "Large arguments",
    description: "Compares scheduling with near-limit function arguments.",
    parameters: { taskCount: 30, argSizeBytes: 800000, maxParallelism: 30 },
  },
  bigContext: {
    label: "Large context",
    description: "Compares large onComplete context handling.",
    parameters: { taskCount: 30, contextSizeBytes: 800000, maxParallelism: 30 },
  },
  bigReturnTypes: {
    label: "Large return values",
    description: "Compares completion handling for large results.",
    parameters: { taskCount: 20, returnSizeBytes: 1000000, maxParallelism: 20 },
  },
} as const;

type ScenarioName = keyof typeof SCENARIOS;

function ComparisonRunner({
  onCompleted,
}: {
  onCompleted: (oldRunId: RunId | null, currentRunId: RunId) => void;
}) {
  const runComparison = useAction(api.test.dashboard.runComparison);
  const runCurrent = useAction(api.test.dashboard.runCurrent);
  const [runMode, setRunMode] = useState<"compare" | "current">("compare");
  const [scenario, setScenario] = useState<ScenarioName>("burstyBatches");
  const [parameters, setParameters] = useState(
    JSON.stringify(SCENARIOS.burstyBatches.parameters, null, 2),
  );
  const [showParameters, setShowParameters] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const changeScenario = (next: ScenarioName) => {
    setScenario(next);
    setParameters(JSON.stringify(SCENARIOS[next].parameters, null, 2));
    setError(null);
  };

  const launch = async () => {
    setError(null);
    let args: Record<string, unknown>;
    try {
      args = JSON.parse(parameters) as Record<string, unknown>;
    } catch (caught) {
      setError(`Invalid parameters: ${(caught as Error).message}`);
      return;
    }

    setBusy(true);
    try {
      if (runMode === "compare") {
        const result = await runComparison({ scenario, args });
        onCompleted(result.oldRunId, result.newRunId);
      } else {
        const result = await runCurrent({ scenario, args });
        onCompleted(null, result.newRunId);
      }
      document
        .getElementById("comparison-results")
        ?.scrollIntoView({ behavior: "smooth", block: "start" });
    } catch (caught) {
      setError((caught as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="runner-panel" aria-labelledby="runner-title">
      <div className="runner-copy">
        <p className="step-label">01 · Run a matched pair</p>
        <h2 id="runner-title">One workload. Two implementations.</h2>
        <p>{SCENARIOS[scenario].description}</p>
      </div>
      <div className="runner-controls">
        <label>
          Scenario
          <select
            value={scenario}
            onChange={(event) =>
              changeScenario(event.target.value as ScenarioName)
            }
            disabled={busy}
          >
            {Object.entries(SCENARIOS).map(([name, config]) => (
              <option key={name} value={name}>
                {config.label}
              </option>
            ))}
          </select>
        </label>
        <button
          className="text-button"
          type="button"
          onClick={() => setShowParameters((value) => !value)}
          disabled={busy}
        >
          {showParameters ? "Hide parameters" : "Tune parameters"}
        </button>
        {showParameters && (
          <label className="parameters-field">
            Parameters
            <textarea
              value={parameters}
              onChange={(event) => setParameters(event.target.value)}
              spellCheck={false}
              disabled={busy}
            />
          </label>
        )}
        <div className="run-action-row">
          <select
            className="run-mode-select"
            aria-label="Run mode"
            value={runMode}
            onChange={(event) =>
              setRunMode(event.target.value as "compare" | "current")
            }
            disabled={busy}
          >
            <option value="compare">Baseline vs current</option>
            <option value="current">Current only</option>
          </select>
          <button className="run-button" onClick={launch} disabled={busy}>
            {busy ? (
              <>
                <span className="spinner" />{" "}
                {runMode === "compare"
                  ? "Running baseline, then current…"
                  : "Running current…"}
              </>
            ) : runMode === "compare" ? (
              "Run comparison"
            ) : (
              "Run current"
            )}
          </button>
        </div>
        {busy && (
          <p className="runner-status">
            This stays active until both workloads finish so the result below is
            immediately comparable.
          </p>
        )}
        {error && <p className="error-message">{error}</p>}
      </div>
    </section>
  );
}

type RunSummary = {
  _id: RunId;
  scenario: string;
  pool?: string;
  startTime: number;
  taskCount?: number;
  scheduledFunctions?: number;
  taskType?: string;
  parameters: unknown;
};

function scenarioKey(scenario: string): string {
  return scenario.replace(/-(old|new)-(bare|oc)$/, "");
}

function findCounterpart(
  selected: RunSummary,
  runs: RunSummary[],
): RunSummary | undefined {
  const selectedIsOld = selected.pool === "old";
  const parameters = JSON.stringify(selected.parameters);
  return runs
    .filter(
      (candidate) =>
        (candidate.pool === "old") !== selectedIsOld &&
        scenarioKey(candidate.scenario) === scenarioKey(selected.scenario) &&
        JSON.stringify(candidate.parameters) === parameters,
    )
    .sort(
      (a, b) =>
        Math.abs(a.startTime - selected.startTime) -
        Math.abs(b.startTime - selected.startTime),
    )[0];
}

function RunPicker({
  runs,
  ids,
  linked,
  onLinkedChange,
  onSelect,
}: {
  runs: RunSummary[];
  ids: CompareIds;
  linked: boolean;
  onLinkedChange: (linked: boolean) => void;
  onSelect: (side: "old" | "current", runId: RunId | null) => void;
}) {
  const oldRuns = runs.filter((run) => run.pool === "old");
  const currentRuns = runs.filter((run) => (run.pool ?? "new") === "new");

  return (
    <section className="picker-section">
      <div>
        <p className="step-label">02 · Compare results</p>
        <h2>Matched run</h2>
        <label className="link-toggle">
          <input
            type="checkbox"
            checked={linked}
            onChange={(event) => onLinkedChange(event.target.checked)}
          />
          <span>
            Link matching runs
            <small>Keep scenario and parameters together</small>
          </span>
        </label>
      </div>
      <div className="run-selectors">
        <label>
          Baseline
          <select
            value={ids.old ?? ""}
            onChange={(event) =>
              onSelect("old", (event.target.value || null) as RunId | null)
            }
          >
            <option value="">Select a baseline run</option>
            {oldRuns.map((run) => (
              <option key={run._id} value={run._id}>
                {run.scenario} [{run.taskType ?? "mutation"}] · {formatTime(run.startTime)}
              </option>
            ))}
          </select>
        </label>
        <span className="versus">vs</span>
        <label>
          Current
          <select
            value={ids.current ?? ""}
            onChange={(event) =>
              onSelect("current", (event.target.value || null) as RunId | null)
            }
          >
            <option value="">Select a current run</option>
            {currentRuns.map((run) => (
              <option key={run._id} value={run._id}>
                {run.scenario} [{run.taskType ?? "mutation"}] · {formatTime(run.startTime)}
              </option>
            ))}
          </select>
        </label>
      </div>
    </section>
  );
}

function Comparison({ ids }: { ids: CompareIds }) {
  const oldRun = useQuery(
    api.test.dashboard.getRun,
    ids.old ? { runId: ids.old } : "skip",
  );
  const currentRun = useQuery(
    api.test.dashboard.getRun,
    ids.current ? { runId: ids.current } : "skip",
  );
  const oldThroughput = useQuery(
    api.test.dashboard.throughputOverTime,
    ids.old ? { runId: ids.old, bucketMs: 500 } : "skip",
  );
  const currentThroughput = useQuery(
    api.test.dashboard.throughputOverTime,
    ids.current ? { runId: ids.current, bucketMs: 500 } : "skip",
  );
  const oldCdf = useQuery(
    api.test.dashboard.latencyCdf,
    ids.old ? { runId: ids.old } : "skip",
  );
  const currentCdf = useQuery(
    api.test.dashboard.latencyCdf,
    ids.current ? { runId: ids.current } : "skip",
  );

  const throughputData = useMemo(() => {
    const oldPoints = oldThroughput?.points ?? [];
    const currentPoints = currentThroughput?.points ?? [];
    const length = Math.max(oldPoints.length, currentPoints.length);
    return Array.from({ length }, (_, index) => ({
      tMs: oldPoints[index]?.tMs ?? currentPoints[index]?.tMs ?? index * 500,
      baseline: oldPoints[index]?.completed,
      current: currentPoints[index]?.completed,
      currentInFlight: currentPoints[index]?.inFlight,
    }));
  }, [oldThroughput, currentThroughput]);

  const cdfData = useMemo(() => {
    const oldPoints = oldCdf ?? [];
    const currentPoints = currentCdf ?? [];
    const times = [...new Set([...oldPoints, ...currentPoints].map((p) => p.ms))]
      .sort((a, b) => a - b);
    const cumulativeAt = (points: Array<{ ms: number; pct: number }>, ms: number) =>
      points.reduce(
        (latest, point) => (point.ms <= ms ? point.pct : latest),
        0,
      );
    return times.map((ms) => ({
      ms,
      baseline: cumulativeAt(oldPoints, ms),
      current: cumulativeAt(currentPoints, ms),
    }));
  }, [oldCdf, currentCdf]);

  if (ids.current && !ids.old) {
    if (currentRun === undefined) {
      return <section className="loading-panel">Loading current run…</section>;
    }
    if (!currentRun) {
      return <section className="empty-state">This run no longer exists.</section>;
    }
    return (
      <section id="comparison-results" className="results-section">
        <OutcomeSummary current={currentRun} />
        <MetricGrid current={currentRun} />
        <RunCharts
          throughputData={throughputData}
          cdfData={cdfData}
          showBaseline={false}
        />
        <details className="run-details">
          <summary>Run details and parameters</summary>
          <div className="detail-columns single">
            <div>
              <strong>Current · {formatTime(currentRun.startTime)}</strong>
              <RunIdControl runId={currentRun._id} />
              <pre>{JSON.stringify(currentRun.parameters, null, 2)}</pre>
            </div>
          </div>
        </details>
      </section>
    );
  }

  if (!ids.old || !ids.current) {
    return (
      <section id="comparison-results" className="empty-state">
        <span>↗</span>
        <h2>Run a comparison or select two existing runs.</h2>
        <p>The summary and charts will appear here.</p>
      </section>
    );
  }

  if (oldRun === undefined || currentRun === undefined) {
    return <section className="loading-panel">Loading comparison…</section>;
  }
  if (!oldRun || !currentRun) {
    return <section className="empty-state">One of these runs no longer exists.</section>;
  }

  const sameScenario =
    oldRun.scenario.replace(/-(old|new)-bare$/, "") ===
    currentRun.scenario.replace(/-(old|new)-bare$/, "");

  return (
    <section id="comparison-results" className="results-section">
      {!sameScenario && (
        <p className="comparison-warning">
          These runs use different scenarios. Select a matched pair for a fair
          comparison.
        </p>
      )}
      <OutcomeSummary baseline={oldRun} current={currentRun} />
      <MetricGrid baseline={oldRun} current={currentRun} />
      <RunCharts throughputData={throughputData} cdfData={cdfData} />

      <details className="run-details">
        <summary>Run details and parameters</summary>
        <div className="detail-columns">
          <div>
            <strong>Baseline · {formatTime(oldRun.startTime)}</strong>
            <RunIdControl runId={oldRun._id} baseline />
            <pre>{JSON.stringify(oldRun.parameters, null, 2)}</pre>
          </div>
          <div>
            <strong>Current · {formatTime(currentRun.startTime)}</strong>
            <RunIdControl runId={currentRun._id} />
            <pre>{JSON.stringify(currentRun.parameters, null, 2)}</pre>
          </div>
        </div>
      </details>
    </section>
  );
}

function OutcomeSummary({
  baseline,
  current,
}: {
  baseline?: RunData;
  current: RunData;
}) {
  const durationGain = improvement(
    baseline?.totalDurationMs,
    current.totalDurationMs,
    true,
  );
  const schedulerGain = improvement(
    baseline?.scheduledFunctions,
    current.scheduledFunctions,
    true,
  );
  const gains = [
    durationGain === undefined
      ? null
      : `${Math.abs(durationGain).toFixed(0)}% ${durationGain >= 0 ? "faster" : "slower"}`,
    schedulerGain === undefined
      ? null
      : `${Math.abs(schedulerGain).toFixed(0)}% ${schedulerGain >= 0 ? "fewer" : "more"} scheduled calls`,
  ].filter(Boolean);

  return (
    <div className="outcome-card">
      <div>
        <p className="step-label">
          Result · {current.scenario} · {String(current.parameters?.taskType ?? "mutation")}
        </p>
        <h2>
          {gains.length > 0
            ? `Current is ${gains.join(" with ")}.`
            : baseline
              ? "Current and baseline are ready to compare."
              : "Current run completed."}
        </h2>
        <p>
          {baseline
            ? `${current.completedCount} tasks completed on both implementations under the same scenario parameters.`
            : `${current.completedCount} tasks completed on the current implementation.`}
        </p>
      </div>
      <div className="outcome-duration">
        <span>Current wall time</span>
        <strong>{formatDuration(current.totalDurationMs)}</strong>
        {baseline && (
          <small>Baseline {formatDuration(baseline.totalDurationMs)}</small>
        )}
      </div>
    </div>
  );
}

function MetricGrid({
  baseline,
  current,
}: {
  baseline?: RunData;
  current: RunData;
}) {
  const metrics = [
    {
      label: "Wall time",
      baseline: baseline?.totalDurationMs,
      current: current.totalDurationMs,
      format: formatDuration,
      lower: true,
    },
    {
      label: "Throughput",
      baseline: baseline ? throughput(baseline) : undefined,
      current: throughput(current),
      format: (value: number | undefined) =>
        value === undefined ? "—" : `${Math.round(value)} tasks/s`,
      lower: false,
    },
    {
      label: "p95 latency",
      baseline: baseline?.latency?.p95,
      current: current.latency?.p95,
      format: formatDuration,
      lower: true,
    },
    {
      label: "p99 latency",
      baseline: baseline?.latency?.p99,
      current: current.latency?.p99,
      format: formatDuration,
      lower: true,
    },
    {
      label: "Scheduled calls",
      baseline: baseline?.scheduledFunctions,
      current: current.scheduledFunctions,
      format: formatNumber,
      lower: true,
      note:
        baseline && baseline.scheduledFunctions === undefined
          ? "Baseline capture pending"
          : undefined,
    },
  ];

  return (
    <div className="metric-grid">
      {metrics.map((metric) => {
        const gain = improvement(
          metric.baseline,
          metric.current,
          metric.lower,
        );
        return (
          <article className="metric-card" key={metric.label}>
            <div className="metric-heading">
              <span>{metric.label}</span>
              {gain !== undefined && (
                <span className={`delta ${gain >= 0 ? "good" : "bad"}`}>
                  {gain >= 0 ? "+" : ""}
                  {gain.toFixed(1)}%
                </span>
              )}
            </div>
            <strong>{metric.format(metric.current)}</strong>
            {baseline && <p>Baseline {metric.format(metric.baseline)}</p>}
            {metric.note && <small>{metric.note}</small>}
          </article>
        );
      })}
    </div>
  );
}

function RunCharts({
  throughputData,
  cdfData,
  showBaseline = true,
}: {
  throughputData: Array<{
    tMs: number;
    baseline?: number;
    current?: number;
  }>;
  cdfData: Array<{ ms: number; baseline: number; current: number }>;
  showBaseline?: boolean;
}) {
  return (
    <div className="chart-grid">
      <ChartCard
        title="Throughput over time"
        subtitle="Tasks completed in each 500 ms window; higher is better."
      >
        <ResponsiveContainer width="100%" height={300}>
          <AreaChart
            data={throughputData}
            margin={{ top: 12, right: 8, left: -12 }}
          >
            <defs>
              <linearGradient id="currentFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={CURRENT_COLOR} stopOpacity={0.3} />
                <stop offset="100%" stopColor={CURRENT_COLOR} stopOpacity={0.02} />
              </linearGradient>
            </defs>
            <CartesianGrid stroke="#d8e1df" vertical={false} />
            <XAxis
              dataKey="tMs"
              tickFormatter={(value) => `${(value / 1000).toFixed(1)}s`}
              tickLine={false}
              axisLine={false}
            />
            <YAxis tickLine={false} axisLine={false} />
            <Tooltip labelFormatter={(value) => `${Number(value) / 1000}s`} />
            <Legend />
            <Area
              type="monotone"
              dataKey="current"
              name="Current"
              stroke={CURRENT_COLOR}
              strokeWidth={2.5}
              fill="url(#currentFill)"
              connectNulls
            />
            {showBaseline && (
              <Line
                type="monotone"
                dataKey="baseline"
                name="0.4.7 baseline"
                stroke={OLD_COLOR}
                strokeWidth={2}
                strokeDasharray="5 4"
                dot={false}
                connectNulls
              />
            )}
          </AreaChart>
        </ResponsiveContainer>
      </ChartCard>

      <ChartCard
        title="Latency distribution"
        subtitle="Cumulative completion by latency; further left is better."
      >
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={cdfData} margin={{ top: 12, right: 8, left: -12 }}>
            <CartesianGrid stroke="#d8e1df" vertical={false} />
            <XAxis
              dataKey="ms"
              tickFormatter={(value) => formatDuration(value)}
              tickLine={false}
              axisLine={false}
            />
            <YAxis
              domain={[0, 100]}
              tickFormatter={(value) => `${value}%`}
              tickLine={false}
              axisLine={false}
            />
            <Tooltip
              labelFormatter={(value) =>
                `${formatDuration(Number(value))} latency`
              }
              formatter={(value) => `${Number(value).toFixed(1)}%`}
            />
            <Legend />
            <Line
              type="stepAfter"
              dataKey="current"
              name="Current"
              stroke={CURRENT_COLOR}
              strokeWidth={2.5}
              dot={false}
            />
            {showBaseline && (
              <Line
                type="stepAfter"
                dataKey="baseline"
                name="0.4.7 baseline"
                stroke={OLD_COLOR}
                strokeWidth={2}
                strokeDasharray="5 4"
                dot={false}
              />
            )}
          </LineChart>
        </ResponsiveContainer>
      </ChartCard>
    </div>
  );
}

function ChartCard({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle: string;
  children: React.ReactNode;
}) {
  return (
    <article className="chart-card">
      <h3>{title}</h3>
      <p>{subtitle}</p>
      {children}
    </article>
  );
}

function RunIdControl({
  runId,
  baseline = false,
}: {
  runId: RunId;
  baseline?: boolean;
}) {
  const copy = () => {
    void navigator.clipboard.writeText(
      `npm run capture:scheduled -- ${runId} ${baseline ? "oldWorkpool" : "testWorkpool"}`,
    );
  };

  return (
    <div className="run-id-control">
      <code title={runId}>{runId}</code>
      <button type="button" onClick={copy}>
        Copy capture command
      </button>
    </div>
  );
}

function RunHistory({
  runs,
  ids,
  onSelect,
}: {
  runs: RunSummary[];
  ids: CompareIds;
  onSelect: (side: "old" | "current", runId: RunId | null) => void;
}) {
  if (runs.length === 0) return null;
  return (
    <section className="history-section">
      <div className="history-heading">
        <div>
          <p className="step-label">Recent data</p>
          <h2>Benchmark runs</h2>
        </div>
        <span>{runs.length} most recent</span>
      </div>
      <div className="history-table-wrap">
        <table>
          <thead>
            <tr>
              <th>Implementation</th>
              <th>Scenario</th>
              <th>Work</th>
              <th>Run ID</th>
              <th>Tasks</th>
              <th>Scheduled calls</th>
              <th>Started</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {runs.map((run) => {
              const isOld = run.pool === "old";
              const selected = isOld
                ? ids.old === run._id
                : ids.current === run._id;
              return (
                <tr key={run._id} className={selected ? "selected" : ""}>
                  <td>
                    <span className={`implementation-badge ${isOld ? "old" : "current"}`}>
                      {isOld ? "0.4.7" : "Current"}
                    </span>
                  </td>
                  <td>{run.scenario}</td>
                  <td>{run.taskType ?? "mutation"}</td>
                  <td>
                    <RunIdControl runId={run._id} baseline={isOld} />
                  </td>
                  <td>{formatNumber(run.taskCount)}</td>
                  <td>{formatNumber(run.scheduledFunctions)}</td>
                  <td>{formatTime(run.startTime)}</td>
                  <td>
                    <button
                      className="table-button"
                      onClick={() =>
                        onSelect(isOld ? "old" : "current", run._id)
                      }
                    >
                      {selected ? "Selected" : "Compare"}
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export default App;
