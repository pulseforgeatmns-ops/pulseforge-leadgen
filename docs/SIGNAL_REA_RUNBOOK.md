# Signal REA behavior lab

This workflow uses REA to investigate a bounded third-party software behavior and turn the result into a verified Signal engineering decision. It does not run in production.

## Readiness

REA requires Node.js 22.19+, 24.11+ or 26+. The repository pins REA 6.4.0.

```bash
npm run signal:rea:check
npm run signal:rea:doctor
npm run signal:rea:brief
```

If Codex registration is missing, preview the exact config changes before applying them:

```bash
npx -y rea-agents@6.4.0 setup --client codex --dry-run
npx -y rea-agents@6.4.0 setup --client codex
```

Restart Codex after registration. A native decompiler is unnecessary for browser inspection and static JavaScript analysis. Android code inspection additionally requires a caller-supplied JDK and headless JADX.

## Run the first investigation

1. Read the committed brief at `signal-v1/rea/investigations/fomo-token-actionability.json`.
2. Open the operator-owned target and keep the selected origin explicit.
3. Start with passive tools: list the exact target, inspect the page, and capture the web bundle or authorized Android package.
4. Trace only the token identity and destination behavior required by Q1-Q3.
5. Keep a finding ledger under ignored `signal-v1/rea-workspace/` while investigating.
6. Record negative evidence with its search boundary. Absence from one capture is not proof that a behavior does not exist.
7. Translate supported findings into normal Signal code. Add fixtures and fail closed when identity is ambiguous.

Useful REA surfaces include `list_browser_targets`, `inspect_web_page`, `analyze_web_bundle`, `trace_web_module_imports`, `inspect_android_package`, `search_android_classes`, `inspect_android_method`, and `build_reconstruction_obligation_ledger`. Use only tools advertised by the installed server.

## Finding ledger template

```json
{
  "investigationId": "signal-rea-fomo-token-actionability-001",
  "targetIdentity": {},
  "findings": [
    {
      "questionId": "Q1",
      "status": "ANSWERED_OR_PARTIAL_OR_UNRESOLVED",
      "observation": "",
      "evidenceRefs": [],
      "confidence": "HIGH_OR_MEDIUM_OR_LOW",
      "searchBoundary": "",
      "remainingUnknowns": [],
      "signalDesignChoice": ""
    }
  ]
}
```

Before closing the investigation, revisit Q1-Q3 and label each answered, partial or unresolved. Commit the minimum evidence needed for review; do not commit credentials, sessions, full application bundles or unrelated captured material.
