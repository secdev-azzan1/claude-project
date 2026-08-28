# Gateway + Iceberg Continuation

Run date: 2026-08-15

Frontend under test: `http://localhost:3001`
Backend under test: `http://localhost:8011`

Artifact folder: `frontend/e2e/artifacts/gateway-iceberg-continuation/`

## What Was Verified

- The UI sidebar and page title use `Proxies`.
- A fresh APISIX proxy was created and reconciled from the UI:
  - name: `codex15aug26 dummyjson`
  - id: `gw-proxy-ysb749`
  - target host: `dummyjson.com`
  - allowlist: `dummyjson.com`
- The proxy card and allowlist dialog were captured in screenshots.
- The APISIX admin object dump was captured in `A3-apisix-objects.json`.

## Blocker

The run stopped at the proxy verification step because the live APISIX runtime did not return a successful upstream response for the created proxy.

Observed outcomes during diagnosis:

- `POST /api/v2/gateway/proxies/{id}/test` returned a timeout for the reconciled proxy.
- Direct requests against the APISIX runtime route returned `503 Server Unavailable` for multiple upstream candidates.
- I also probed candidate upstreams through the real APISIX route and saw the same failure pattern for public and internal hosts.

Because of that, I could not complete the requested end-to-end path for:

- flow creation through the APISIX proxy
- Iceberg sink service creation and connector deployment
- Trino/Iceberg row verification

## Evidence On Disk

- `A1-sidebar-proxies.png`
- `A1b-proxies-page.png`
- `A2-admin-confirm-dialog.png`
- `A2b-host-allowlisted.png`
- `A3-proxy-reconciled-card.png`
- `A3-apisix-objects.json`
- `codex15aug26-posts-sample.json`
- Playwright failure screenshot in the shared test-output folder

## Notes

- No product source was changed during this verification pass.
- No flow, schema, topic, connector, or table was deleted.
- The failure mode looks environmental: APISIX routing to upstream targets is not completing from this runtime.
