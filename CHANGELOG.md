# Changelog

## Unreleased

- Preserve Continuation deltas when Pi 1.4.0 declares tools through `additional_tools` or tool search.
  Probe-generated tool declarations no longer enter the Baseline, so a grammar tool-call follow-up sends
  only the new tool result instead of a Full Request.
