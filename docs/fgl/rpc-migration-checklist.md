# FGL_IIP RPC Migration Checklist

Every RPC migration to `FGL_IIP` mode must follow this checklist. Reviewers reject PRs that do not satisfy every item.

**Authoritative references:**
- [HDFS-17385-wave4-pilot-design.md](HDFS-17385-wave4-pilot-design.md) — full design rationale, mode taxonomy (§1.4), lock-ordering rules (§1.8), RPC-10 gate scope (§6.3).
- [pre-rpc-10-review.md](pre-rpc-10-review.md) — findings from the 9-migration cohort. Read before starting migration #10+, especially for the dispatch-template work deferred to the RPC-10 gate.

## Pre-work

- [ ] JIRA sub-ticket filed under HDFS-17385 with label `fgl-iip-rpc-migration`.
- [ ] Target RPC identified: name, interface method, `FSNamesystem` method, `FSDirectory` helpers it calls, any `BlockManager` or `LeaseManager` interactions.
- [ ] Pilot design spec re-read (sections 1–3).
- [ ] Previous RPC migration PRs (if any) reviewed for current conventions.

## Mode selection

- [ ] Picked the appropriate `IIPAcquireMode`:
  - Read-only, single file/dir → `PATH_READ`
  - Write to single file/dir, no parent mutation (setPermission, setOwner, setTimes, setReplication) → `PATH_WRITE`
  - Create-child or single-child removal (create, mkdir, delete) → `PARENT_WRITE`
  - Subtree operation (delete -r, chown -R, setQuota on subtree) → `ANCESTOR_WRITE` *(deferred)*
  - Rename → `RENAME_WRITE` *(deferred)*
  - Admin/meta → `ADMIN_META` *(fallback)*
- [ ] If the RPC needs a `DEFERRED` mode, **STOP**. File a separate ticket to implement the mode in `INodeLockManager` first.
- [ ] Did **not** add a new mode without updating the taxonomy table in the pilot design spec and re-running the pilot gate review.

## Code shape

**Post-RPC-10 pattern (mandatory for all new migrations).** The §6.3 RPC-10 gate landed a shared dispatch template in `FSNamesystem`. Every new migration uses it. The shape is:

```java
// Outer method:
final String operationName = "xxx";
... standard setup ...
PilotResult<R> pr = tryPilot(
    () -> canUsePilotXxx(src),     // Phase A (lock-free)
    () -> xxxPilot(src, ..., pc),   // Phase B (under IIP lock)
    operationName, src);
if (pr.handled) {
  auditStat = pr.value;
} else {
  // legacy writeLock/readLock path unchanged
}
getEditLog().logSync();
logAuditEvent(true, operationName, src, null, auditStat);

// Pilot method:
private R xxxPilot(String src, ..., FSPermissionChecker pc)
    throws IOException, InterruptedException {
  IIPBasedFSNamesystemLock iipLock = (IIPBasedFSNamesystemLock) fsLock;
  try (LockedIIP lip = iipLock.lockPath(src, IIPAcquireMode.MODE)) {
    checkOperation(OperationCategory.XXX);
    checkNameNodeSafeMode("Cannot xxx " + src);
    INodesInPath iip = lip.iip();
    // Phase-B checks — throw PilotEnvelopeMissException to fall back:
    if (iip.getLastINode() == null) {
      throw new PilotEnvelopeMissException("xxx: ... " + src);
    }
    if (!ancestorsAllowMutate(iip)) {  // or ancestorsAllowCreate
      throw new PilotEnvelopeMissException("xxx: ... " + src);
    }
    // BMLock nested here if needed (rule 4).
    return FSDirXxxOp.xxx(dir, pc, iip, ...);
  }
}
```

- [ ] Uses `tryPilot(...)` — no bespoke dispatch block.
- [ ] Pilot method `throws IOException, InterruptedException` — template wraps `InterruptedException` into `InterruptedIOException`.
- [ ] Pilot method throws `PilotEnvelopeMissException` on Phase-B miss — never `return null` to signal "fall back".
- [ ] `null` returned from the pilot method means "legitimate null result" (e.g., `getFileInfo` file-not-found) — the template's `PilotResult.handled(null)` preserves this.
- [ ] `auditOnAce=false` overload is used ONLY by `startFileInt` (its outer `startFile` wrapper owns ACE audit); every other RPC uses the default overload.
- [ ] RPC body uses `try (LockedIIP lip = fsLock.lockPath(path, mode))` — no direct `fsLock.readLock/writeLock` calls.
- [ ] RPC body does NOT touch `LockPool`, `LockRef`, or `INodeLockManager` directly — they are package-private by design.
- [ ] No `if (lockMode == FGL_IIP) { ... } else { ... }` dispatch anywhere in the RPC body. Dispatch is virtual through `FSNLockManager.lockPath()`.
- [ ] Envelope-miss delegation goes through the legacy fallback branch of the outer method — no re-entry of `lockPath`.
- [ ] Edit log `logSync()` called OUTSIDE the `try-with-resources` block (in the outer method's shared tail).
- [ ] Audit log `LOG.info` / `LOG.warn` called OUTSIDE the `try-with-resources` block (except trace-level debug inside the block).
- [ ] No nested `lockPath` calls on the same thread. (The pilot forbids nesting; an assertion fires.)
- [ ] BMLock, if acquired, is acquired AND released INSIDE the `try-with-resources` block but AFTER the IIP locks are held.

## Invariants

- [ ] Lock-ordering rules 1–6 (design spec §1.8) verified manually:
  1. Ancestor before descendant.
  2. Ancestor always read in pilot modes.
  3. Sibling locks ordered by ascending INode ID (if the mode touches multiple siblings).
  4. IIPLock always before BMLock.
  5. Compat-write never held under IIP lock.
  6. No read→write upgrade — acquire write upfront.
- [ ] If the RPC uses BlockManager: BMLock acquired UNDER the IIP lock, released BEFORE the IIP lock closes.
- [ ] If the RPC uses LeaseManager: lease manager's lock acquired UNDER IIP and released BEFORE IIP closes. Pre-check LeaseManager's lock discipline for cycles.
- [ ] All code paths that can throw release held locks via `try-finally` or try-with-resources — no lock leaks on error paths.

## Testing

- [ ] Pilot-specific tests live in `TestFSNamesystemFGLIIP` (FGL_IIP-only cluster). Tri-mode correctness coverage comes from the existing module test suite run under `-Dlockmode=FGL_IIP` per design-spec §4.8a — **not** from parameterising `TestFSNamesystemFGLIIP` itself. The `FSNamesystemLockMode` enum and `@EnumSource` harness referenced in earlier drafts of this checklist were never built; a future ticket may introduce them, at which point this item converts to "use the parameterised harness". Until then, pilot test classes remain FGL_IIP-only.
- [ ] Existing test classes exercised by the RPC pass under `-Dlockmode=FGL`, `-Dlockmode=GLOBAL`, and `-Dlockmode=FGL_IIP`.
- [ ] If the RPC **introduces a new envelope clause** (not reused from a prior pilot RPC), that clause has an explicit miss test case. Metric assertion (`CreateEnvelopeMissCount{clause=X}`) is deferred — the `FGLockMetrics` infrastructure is not yet implemented. File a follow-up ticket when the first RPC needs a genuinely new clause.
- [ ] Concurrency test: at least one scenario with N=16 concurrent clients on disjoint INodes, asserting throughput scales near-linearly under `FGL_IIP`.
- [ ] Concurrency test: at least one "hot" scenario (16 clients on shared INode) asserting no regression vs `FGL`.
- [ ] Coverage on new code: ≥80% line, ≥75% branch.
- [ ] Added case to the integration scenarios list in the design spec (§4.5) if this RPC introduces a new class of correctness concern.

## Metrics

- [ ] If the RPC adds a new envelope clause, a new enum value + counter registered in `FGLockMetrics` with the clause name. **Note:** `FGLockMetrics` is not yet implemented; until the first RPC genuinely needs it, this item is deferred. File a prerequisite ticket to introduce `FGLockMetrics` if you are the first caller.
- [ ] If the RPC has a new failure mode, a new counter or histogram registered.
- [ ] If the RPC has a different latency profile, a new histogram dimension added.

## Documentation

- [ ] PR description links to this file and to the pilot design spec.
- [ ] New class javadoc includes `@see docs/fgl/HDFS-17385-wave4-pilot-design.md`.
- [ ] If the RPC touches a cross-cutting concern in §3 of the design spec, spec updated to reflect any new behavior.

## Review

- [ ] code-reviewer agent run against the diff; all CRITICAL and HIGH findings resolved.
- [ ] architect agent "would this duplicate if 40 more RPCs followed?" check passed.
- [ ] At least one HDFS committer has approved the PR.

## Post-merge

- [ ] JIRA ticket resolved with a comment linking the merge commit SHA.
- [ ] JIRA ticket has label `fgl-iip-rpc-migration` (verified by CI trigger count).
- [ ] If RPC count reached a multiple of 5 (excluding 10): audit cycle scheduled (§6.5 of the design spec).
- [ ] If RPC count reached 10: RPC #10 refactor gate triggers (§6.3 of the design spec). **Further RPC migrations are blocked until the gate passes.**

## Anti-patterns to reject in review

- Copy-pasted `try (LockedIIP lip = ...)` boilerplate that could be extracted.
- Envelope checks sprinkled in the RPC body instead of a dedicated helper.
- Direct `LockPool` or `LockRef` access — these are package-private; if you need them, something is wrong with the design.
- New `IIPAcquireMode` values added without updating the taxonomy table.
- Mode-specific branching in `FSNamesystem` — dispatch must be virtual.
- Tests that only cover `FGL_IIP` mode without the parameterization harness.
- Comments like `// TODO: handle X later` in lock-related code — file a ticket or fix it now.
- Edit log or audit calls inside an IIP-held scope.
- Lock acquisition ordering that diverges from rules 1–6 "just this once."

## When something doesn't fit

If your RPC genuinely doesn't fit this checklist, **stop** and open a discussion:

- Post in the `#hdfs-fgl` channel or JIRA epic comment.
- Describe the mismatch and why you think the current pattern can't accommodate it.
- Propose: (a) new mode, (b) new helper, (c) design change.
- Do NOT merge a workaround. The cost of divergence multiplies by every future RPC that copies it.
