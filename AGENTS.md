# pi-openai-websocket

## Agent skills

### Issue tracker

Issues live as GitHub issues in `angribot/pi-openai-websocket`, managed with the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-label vocabulary, unchanged. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.

### Dependency lockfiles

Dependency lockfiles are local-only and must not be committed. Keep `package-lock.json` ignored and pin the
Pi development baseline with exact direct dependency versions in `package.json`.
