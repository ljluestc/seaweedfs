# PR Description — Fix cross-mount metadata visibility delay caused by client/server time skew

## Summary
This PR addresses a mount consistency issue where files written from one mounted host are not immediately visible from another mounted host until remount. The issue is caused by metadata cache invalidation relying on timestamps that are sensitive to client/server clock differences.

Closes #2672.

## Problem
In multi-host mount setups:
- Host A writes a file through `weed mount`
- Host B does not see the new file until remount
- Filer Web UI already shows the file correctly

This indicates the write is committed in filer metadata, but mount-side cache refresh/invalidations are not triggered correctly on another client.

## Root Cause Hypothesis
Cross-mount metadata sync currently relies on comparing timestamps from different machines (mount client and filer server). When host clocks drift or are skewed, one client can consider stale cache entries as still valid and skip refresh.

Result:
- stale directory/file metadata persists in mount cache
- remount clears local cache and makes entries visible

## Proposed Fix
1. Replace wall-clock dependent cache freshness checks with server-authoritative change indicators.
2. Use monotonic or sequence-based metadata versioning (where available) for invalidation decisions.
3. Ensure mount cache refresh logic does not trust local client clock for cross-client visibility.
4. Keep existing cache behavior for performance but make invalidation robust under time skew.

## Scope
- In scope: mount metadata sync and cache invalidation logic related to cross-client visibility.
- Out of scope: unrelated filer storage consistency, volume data path, or mount write-path semantics.

## Risk Assessment
- **Risk**: more frequent metadata refreshes may increase filer metadata requests.
- **Mitigation**:
  - keep invalidation targeted to changed paths,
  - preserve existing cache TTL/perf knobs,
  - add regression and scale tests for request amplification.

## Validation Plan
- Add/extend tests for:
  - two mount clients seeing updates without remount
  - simulated clock skew between mount clients and filer
  - no regression for same-host/same-clock scenarios
- Manual verification:
  1. Mount same filer path on two hosts with intentional clock offset.
  2. Create/rename/delete files on host A.
  3. Verify host B sees changes promptly without remount.
  4. Confirm filer UI and mount views converge consistently.

## Expected Outcome
- Metadata updates become visible across mounts without remount.
- Behavior remains correct even when client and server clocks differ.

## Rollout
- Merge with targeted regression tests.
- Recommend clock-sync best practices (NTP/chrony), but correctness should no longer depend on strict clock alignment.

## Checklist
- [x] Full local PR description drafted.
- [ ] Functional code changes implemented.
- [ ] Regression tests added and passing.
