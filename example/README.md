# Example app

Components need an app that uses them in order to run codegen. This example app
also doubles as a benchmark dashboard for the workpool component itself — it
exercises the API and surfaces throughput and latency metrics for the scenarios
in `convex/test/scenarios/`.

## Running the dashboard

From the repo root, in two terminals:

```sh
npm run dev           # backend: convex dev + workpool codegen watch
npm run dev:dashboard # frontend: vite dev server (defaults to http://localhost:5173)
```

The first run of `npm run dev` writes `.env.local` with `VITE_CONVEX_URL`, which
the vite config reads from the repo root (`envDir: "../"`).

## What's in the dashboard

- **Run scenario** — pick a preset (`burstyBatches`, `throughput`, `overhead`,
  `sustained`, `bigArgs`, `bigContext`, `bigReturnTypes`), tweak the JSON
  parameters, and launch it against the "new" pool (this branch), the "old" pool
  (`workpool@0.4.6`, installed as `@convex-dev/workpool-old`), or both
  back-to-back.
- **History** — every run is persisted to the `runs` table. Pick A and B to diff
  them.
- **Detail** — per-run throughput-over-time and latency CDF charts.
- **Compare** — side-by-side throughput and CDF for two runs, plus a summary
  delta table (p50/p95/p99/max/duration).

URL state is encoded in the hash (`#detail/<id>`, `#compare/<id1>,<id2>`), so
links are shareable.

## Deploying it as a static site (optional)

The example is wired to `@convex-dev/static-hosting`, so you can publish the
dashboard to your dev deployment with:

```sh
npm run deploy:dashboard         # uploads to dev
npm run deploy:dashboard:prod    # uploads to prod
```

It will be served at `https://<your-deployment>.convex.site/`. See the component
setup in `convex/convex.config.ts`, `convex/http.ts`, and
`convex/staticHosting.ts`.
