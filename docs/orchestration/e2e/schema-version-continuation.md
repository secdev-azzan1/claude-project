# Schema Version Continuation

Date: 2026-08-15  
Verifier: Playwright E2E run against the live app at `http://localhost:3001` and backend at `http://localhost:8011`

## Outcome

PASS.

The schema continuation journey was verified through the UI without file upload:

1. Created a manual library template.
2. Verified the template in the browser before registration.
3. Registered the template under Apicurio subject `codex15aug26-schema-version-continuation-20260815123811098-value`.
4. Proved the template appears under `Registered` and disappears under `Not registered` after registration.
5. Edited the template through 20 successive registry versions.
6. Browsed version `v1` and then returned to the current editable draft through the UI version selector.
7. Confirmed the registry history through backend API reads.

## Exact Evidence

| Item | Value |
|---|---|
| Template name | `codex15aug26 schema version continuation 20260815123811098` |
| Template id | `tpl-8u58hm` |
| Subject | `codex15aug26-schema-version-continuation-20260815123811098-value` |
| Initial register | global id `29`, version `1` |
| Final register | global id `48`, version `20` |
| Registry version count | `20` |
| First registry payload | `v1`, global id `29`, fields: `id` |
| Latest registry payload | `v20`, global id `48`, fields: `id` + `extra_2`..`extra_20` |
| Current template draft | editable draft remained visible in the UI after returning from history |

## Browser Behavior

The UI version selector was exercised against the full registry history:

- The popover contained `20` version entries.
- `v1` was selected and shown as the read-only historical version.
- The selector was returned to the current editable draft.
- The current draft state is separate from the registry history, which is why the final editable textarea matched the backend template draft rather than the latest registered payload.

## Screenshots

Artifacts are under [frontend/e2e/artifacts/schema-version-continuation/](<C:\Users\kaifm\Desktop\claude-project\frontend\e2e\artifacts\schema-version-continuation\>).

- `01-template-opened.png`
- `02-not-registered-filter.png`
- `03-verifying-manual-template.png`
- `04-registered-filter.png`
- `05-version-menu-open.png`
- `06-version-1-readonly.png`
- `07-version-current-editable.png`

## Machine-Readable Ledger

The full captured response ledger is in [frontend/e2e/artifacts/schema-version-continuation/evidence.json](<C:\Users\kaifm\Desktop\claude-project\frontend\e2e\artifacts\schema-version-continuation\evidence.json>).

## Test Command

```bash
npx playwright test e2e/schema-version-continuation.spec.ts -c e2e/playwright.config.ts
```
