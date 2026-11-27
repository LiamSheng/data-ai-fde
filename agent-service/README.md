# agent-service

The Transit chat assistant backend. Since 2026-11-03 it is rebuilt with a
provider SDK and plain Python (no agent framework), so each layer stays visible.

Status [2026-11-27]: the scope router is chosen and evaluated; the service itself
is not started.

## Scope router

Each message is first classified by a decision model (Clef, via Experiential
Labs `POST /v1/systemone`, model `clef:free`) with one `choice` question
(`evals/clef_scope_request.json`). `p_in_scope` (transit + weather_transit +
smalltalk) at or above 0.5 goes to the agent; below it gets a fixed reply.

Evaluate it:

```sh
export EXPLABS_API_KEY=...   # never inline in a command you will paste
uv run --with pyyaml python agent-service/evals/run_scope_eval.py [--limit N]
```

`evals/scope_cases.yaml` holds 62 labelled cases; answers go to `evals/results/`
(git-ignored) so thresholds can be re-scored offline. `clef:free` is undocumented
and the free pool allows 10 requests a minute, so the service must fall back when
the router fails.
