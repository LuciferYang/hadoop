# Pre-RPC-10 Gate Review — HDFS-17385 FGL_IIP Pilot

**Status:** informational, produced at counter = 9/10.
**Purpose:** surface findings from the 9-migration cohort BEFORE the 10th
RPC triggers the formal §6.3 refactor gate. Easy wins are fixed in the
same commit batch; larger extractions are explicitly deferred to the
gate itself.

## Context

The pilot design spec (§6.3) defines a mandatory refactor gate at 10
migrated RPCs. Its scope:

1. Duplication sweep (≥3x must be extracted)
2. Abstraction review (is `IIPAcquireMode` still right? Is
   `INodeLockManager.acquire` still the right shape?)
3. Patch reorganization (squash fixups, extract helpers, update
   migration checklist with lessons)
4. Hard freeze (after the gate, the pattern is the pattern)

This review runs the first half of steps 1 and 2 one RPC early so the
gate exercise can proceed from known findings rather than discovery.

## The 9 migrations

| # | RPC | Mode | Notable concern |
|---|---|---|---|
| 1 | `mkdirs` | `PARENT_WRITE` | create-class envelope |
| 2 | `getBlockLocations` | `PATH_READ` | nested BM read lock |
| 3 | `isFileClosed` | `PATH_READ` | simplest read |
| 4 | `delete` (single-file) | `PARENT_WRITE` | first delete-class |
| 5 | `setPermission` | `PATH_WRITE` | PATH_WRITE promotion |
| 6 | `setOwner` | `PATH_WRITE` | non-su constraints |
| 7 | `setTimes` | `PATH_WRITE` | mtime/atime semantics |
| 8 | `setReplication` | `PATH_WRITE` | nested BM write lock |
| 9 | `getListing` | `PATH_READ` | directory iteration |

Plus the pilot itself: `getFileInfo` (PATH_READ, U8a) and `startFile`
(PARENT_WRITE, U8b). Effective in-flight pilot code: 11 RPCs across
three modes.

Mode coverage:

| Mode | Post-pilot | Total (incl. U8) |
|---|---|---|
| `PATH_READ` | 3 | 4 |
| `PARENT_WRITE` | 2 | 3 |
| `PATH_WRITE` | 4 | 4 |
| `ANCESTOR_WRITE` | 0 | 0 (deferred) |
| `RENAME_WRITE` | 0 | 0 (deferred) |

All three implemented pilot modes have ≥3 data points. The two deferred
modes are post-pilot work.

## Findings

Severity mapping: **CRITICAL** = blocks merge; **HIGH** = should fix
before the gate; **MEDIUM** = consider at the gate; **LOW** = note for
future.

### [FIXED in this review commit batch]

**[CRITICAL] `rpc-migration-checklist.md` referenced non-existent
modes.** The checklist listed `SINGLE_INODE_READ` / `SINGLE_INODE_WRITE`
which do not exist in `IIPAcquireMode`. Corrected to `PATH_READ` /
`PATH_WRITE`. Any engineer using the checklist to pick a mode would
have been blocked.
*Fix:* `docs/fgl/rpc-migration-checklist.md` §Mode selection.

**[HIGH] No test exercised `ancestorsAllowMutate` / `ancestorsAllowCreate`
fallback triggers.** The Phase-B ancestor check appears in 4 pilot
methods (now via shared helpers after the mid-pilot refactor). Zero
tests set a namespace quota on an ancestor and verified the pilot
falls back. A future migration copying the helper usage could
silently skip the check without a test catching it.
*Fix:* added 4 tests (`setPermissionUnderAncestorQuotaFallsBackToLegacy`,
`setOwnerUnderAncestorQuotaFallsBackToLegacy`,
`deleteUnderSnapshottableAncestorFallsBackToLegacy`,
`createUnderAncestorQuotaFallsBackToLegacy`). All 4 pilot → legacy
fallback paths now have explicit coverage.

**[MEDIUM] `getFileInfo("/")` edge case untested.** PATH_READ on root
(pathLen=1) is a valid call — `computeMaxLockDepth` returns 0,
locking only the root INode. Not covered before.
*Fix:* added `getFileInfoOnRootPath`.

**[MEDIUM] Lock-ordering rule 5 (compat-write never held under IIP)
was structurally enforced but unasserted.**
*Fix:* added `assert !compatLock.isWriteLockedByCurrentThread()` at
the top of `INodeLockManager.acquire`. Cheap defensive guard; would
catch any future caller that accidentally invokes `lockPath` while
holding compat-write.

### [DEFERRED to the RPC-10 gate]

**[HIGH] Outer dispatch block — 8 near-identical copies, ~15 lines
each.** Every pilot outer method in `FSNamesystem.java` contains:

```java
boolean pilotHandled = false;
if (canUsePilotXxx(src)) {
  try {
    result = xxxPilot(...);
    if (result != null) { pilotHandled = true; }
  } catch (PilotEnvelopeMissException pem) { }
    catch (AccessControlException e) {
      logAuditEvent(false, operationName, src); throw e;
    }
}
if (!pilotHandled) { /* legacy path */ }
```

`startFileInt` diverges intentionally (ACE is caught by the outer
`startFile` wrapper, not by the dispatch block). Two RPCs
(`getFileInfo`, `getListing`) treat null-return as a legitimate
result; the other 6 treat it as "miss, fall back". These semantic
variations are real and must be preserved.

*Recommended extraction at gate:* A generic `PilotOperation<R>`
functional interface plus a shared
`runPilotThenLegacy(canUse, pilotOp, legacyOp, ...)` template,
parameterised over:

1. Null-means-legitimate vs null-means-miss
2. Whether the dispatch block owns ACE audit logging

Two template variants is probably the right shape; a single template
with a boolean flag would be an anti-pattern. Landing this at the
gate means RPC #10's migration can consume the template as its first
user, stress-testing it before 30 more RPCs pile on. Attempting the
extraction now carries higher risk of subtle regression across 9
existing callers; the gate's dedicated review + testing cycle is the
right home.

**[HIGH] `InterruptedException` wrap — 9 identical copies.** Each
pilot method ends with:

```java
} catch (InterruptedException ie) {
    Thread.currentThread().interrupt();
    throw new InterruptedIOException("xxx interrupted on " + src);
}
```

Structurally folds into the dispatch template above. Deferred with
the same rationale.

**[MEDIUM] `canUsePilotStartFile` / `canUsePilotMkdirs` / `canUsePilotDelete`
still each carry the full 5-check Phase-A body.** `canUsePilotPathRead`
and `canUsePilotPathWrite` extracted the common core for read and
PATH_WRITE RPCs; the PARENT_WRITE cluster never got the equivalent.
A `canUsePilotParentWrite(src)` helper with the 5 shared checks,
plus per-RPC specialisations for the create-specific flag/policy
checks, would eliminate one more copy.

*Recommendation at gate:* extract alongside the dispatch template.
Low-risk but touches 3 call sites; consistent treatment with the read
helpers.

**[MEDIUM] `startFileInt` dispatch shape differs from the other 8.**
The asymmetry (ACE handled by the outer wrapper, `stat != null`
return-short-circuit instead of a `pilotHandled` flag) is
load-bearing — it predates the consolidation. Future migrations
might copy-paste `startFileInt` and silently lose ACE audit logging.
*Recommendation at gate:* document this asymmetry in a comment on
`startFileInt` explaining WHY it differs, or normalise the shape if
the `startFile` outer wrapper can be slightly restructured to match.

**[LOW] `IIPBasedFSNamesystemLock iipLock = (IIPBasedFSNamesystemLock)
fsLock;` inline cast — 9 copies.** Noise. Vanishes when the dispatch
template is introduced.

**[LOW] `ancestorsAllowCreate` vs `ancestorsAllowMutate` copy-paste
risk.** The two helpers differ by the snapshot-feature check only. A
future engineer migrating `setQuota` might copy `ancestorsAllowCreate`
instead of `ancestorsAllowMutate` and silently skip the snapshot
check. Javadoc already explains the intentional asymmetry.
*Recommendation at gate:* add a comment on each helper that names the
other and explicitly calls the asymmetry out.

## Abstraction fitness

Rated 1-5, where 5 = production-ready for another 30 migrations.

| Component | Rating | Notes |
|---|---|---|
| `IIPAcquireMode` taxonomy | 4 | Clean. The delete-class split between single-file (PARENT_WRITE) and recursive (ANCESTOR_WRITE, deferred) should be documented; otherwise future engineers may try to shoehorn recursive delete into PARENT_WRITE. |
| `INodeLockManager.acquire` | 4 | Right shape for all 3 pilot modes. Add the rule-5 assert (done). Rule-4 BM-write direction has one data point (`setReplication`) — consider a second before the gate if another PATH_WRITE + BM RPC surfaces. |
| `LockedIIP` | 5 | Carries the right fields. `RENAME_WRITE` will need a new return type (two targets), but that's a deferred-mode concern. |
| `canUsePilotPathRead/Write` | 5 | Shared helpers working cleanly. |
| `ancestorsAllowCreate/Mutate` | 5 | Two named helpers is the right shape; boolean flag would be an anti-pattern. |
| Outer dispatch boilerplate | 2 | Duplication is the single highest-leverage item for the gate. |

**Overall fitness for 30 post-pilot migrations: 4/5.** The inner
abstractions are solid; the outer boilerplate is where drift will
accumulate if the gate doesn't extract a template.

## Bug pattern observation

Bugs caught during review of the 9 migrations:

| # | Bug | Location | Pattern |
|---|---|---|---|
| R6 #1 | `startFilePilot` missed `checkTraverse` | outer pilot method | permission-check gap in new code |
| R6 #2 | stale root reference captured at construction | `IIPBasedFSNamesystemLock` | lifecycle assumption |
| R7 #3 | INodeReference walk broke early | `INodeLockManager` | snapshot edge case |
| R8 #4 | PNDE → ACE conversion for non-superuser | outer pilot method | legacy-compat semantics |
| new | `pilotHandled = true` unconditional | outer pilot method (setPermission) | dispatch boilerplate drift |
| new | missing BM read lock in getBlockLocations | outer pilot method | lock-ordering assertion not enforced at a unified point |

**Pattern:** most bugs (5 of 6) are in the **outer FSNamesystem
plumbing**, not in `INodeLockManager` / `LockedIIP` / `IIPAcquireMode`.
The inner abstractions have held up across 9 migrations without
modification. This confirms the gate's focus should be on the outer
plumbing (dispatch template) rather than revisiting the inner
taxonomy.

## Action items at the RPC-10 gate

1. **Extract the dispatch template** (`PilotOperation<R>` +
   `runPilotThenLegacy`). Two variants to handle null-means-miss vs
   null-means-legitimate. Migrate all 9 existing call sites.
2. **Extract `canUsePilotParentWrite`** for the 3 create-class Phase-A
   copies.
3. **Normalise or comment the `startFileInt` asymmetry** so future
   engineers don't copy-paste-and-lose ACE audit.
4. **Add intra-helper pointers** so `ancestorsAllowCreate` and
   `ancestorsAllowMutate` explicitly reference each other.
5. **Update `rpc-migration-checklist.md`** — add a reference to this
   review doc and to the dispatch template from step 1.

Expected total at-gate work: 3-5 working days per §6.3.

## Verification (pre-gate baseline)

```
mvn test -pl hadoop-hdfs-project/hadoop-hdfs -o \
  -Dtest='TestFSNamesystemFGLIIP,TestIIPBasedFSNamesystemLock,\
         TestLockPool,TestLockRef,TestIIPAcquireMode,TestLockedIIP,\
         TestINodeLockManager,TestINodeMapConcurrent,\
         TestGetFullPathNameConcurrent'
=> Tests run: 227, Failures: 0, Errors: 0, Skipped: 0
```

## Sign-off

This review is conducted at counter = 9/10 and produces no code drift
vs merged pilot behaviour. The fixed items (CRITICAL, HIGH test gaps,
MEDIUM test gap, MEDIUM rule-5 assert) land in the same commit batch
as this document. All deferred items are recorded with rationale for
the RPC-10 gate's consumption.

**Reviewer:** code-reviewer agent + pilot author.
**Date:** 2026-04-15.
**Gate ticket to file at 10/10:** `HDFS-17385: [FGL] RPC-10 refactor gate`.
