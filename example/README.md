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

The dashboard is a single comparison workspace. Pick a preset
(`burstyBatches`, `throughput`, `overhead`, `sustained`, `bigArgs`,
`bigContext`, or `bigReturnTypes`) and it runs the published 0.4.7 baseline,
then this branch, with identical parameters. The completed pair is selected
automatically and rendered as outcome cards, throughput and latency charts,
and recent-run history on the same page.

Scheduled-function instrumentation stays internal to the components. Backfill
a checked run from its component system table with:

```sh
npm run capture:scheduled -- <baselineRunId> oldWorkpool
npm run capture:scheduled -- <currentRunId> testWorkpool
```

Comparison URLs are encoded as `#compare/<oldRunId>,<newRunId>` so a result is
shareable.

## Deploying it as a static site (optional)

The example mounts `@convex-dev/static-hosting` directly at `/`, so no
`convex/http.ts` catch-all is required. You can publish the dashboard to your
dev deployment with:

```sh
npm run deploy:dashboard         # uploads to dev
npm run deploy:dashboard:prod    # uploads to prod
```

It will be served at `https://<your-deployment>.convex.site/`. See the component
setup in `convex/convex.config.ts` and `convex/staticHosting.ts`.
