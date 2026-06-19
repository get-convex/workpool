# Developing guide

## Running locally

```sh
npm i
npm run dev
```

## Running the example dashboard

The `example/` app includes a dashboard for launching benchmark scenarios,
inspecting individual runs (throughput, latency CDF), and comparing two runs
side-by-side. To use it, run the backend and the frontend together:

```sh
npm run dev          # in one terminal: convex dev + library codegen watch
npm run dev:dashboard # in another:     vite dev server for example/
```

Then open the URL vite prints (typically <http://localhost:5173>). The "Run
scenario" tab launches presets; "History" lists past runs; "Compare" diffs two
of them. See `example/README.md` for more.

## Testing

```sh
npm run clean
npm run build
npm run typecheck
npm run lint
npm run test
```

## Deploying

### Building a one-off package

```sh
npm run clean
npm ci
npm pack
```

### Deploying a new version

```sh
npm run release
```

or for alpha release:

```sh
npm run alpha
```
