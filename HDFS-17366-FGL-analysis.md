# HDFS-17366 NameNode Fine-Grained Locking — Status Analysis & R&D Plan Split

> Source design doc: *NameNode Fine-Grained Locking Based On Directory Tree (III).pdf* (Dec 2024, ZanderXu).
> Cross-checked against: trunk git history, JIRA HDFS-17366 / 17384 / 17385 / 17386, merged PRs.
> Date: 2026-04-07

---

## 1. HDFS-17366 — Goals & Overall Progress

### 1.1 Goal
Replace the single `FSNamesystemLock` (a global `ReentrantReadWriteLock`) with a **directory-tree based fine-grained locking (FGL)** mechanism so that disjoint write operations on different branches of the namespace can run concurrently. Reported result in production: ~7x write throughput improvement.

### 1.2 Final lock model (target)
Four lock families coexist; operations are categorised into four buckets:

| Lock | Scope | Operations |
|------|-------|------------|
| **Global FSLock** | Entire namespace, write mode used only for HA / Safe-mode style operations | `start/stopCommonServices`, `start/stopActive/StandbyServices`, `saveNamespace` |
| **IIPLock** (chain of `INodeLock`s on a path) | Directory path | All directory-related RPCs (`create`, `mkdir`, `rename`, `delete`, `setQuota`, …) |
| **INodeFileLock** | Single `INodeFile` (also covers all `BlockInfo` belonging to that file) | Block-related work: `BR/IBR`, `RedundancyMonitor`, `complete`, `addBlock`, … |
| **DNLock** | Single `DataNodeInfo` | DN-related work: `registerDataNode`, `heartbeat`, `cacheReport`, `decommission`, `maintenance` |

Lock acquisition order (must be consistent to avoid deadlock):
`Global FSLock → DNLock → INodeLock (IIPLock / INodeFileLock) → small-scale locks (LeaseManager, …)`
Release in reverse order.

### 1.3 Phased plan (per the design doc)

| Phase | JIRA | Scope | Configurable rollback? |
|-------|------|-------|-----------------------|
| I | HDFS-17384 | Split global lock into **Global FSLock + Global BMLock** | Yes (config switch) |
| II | HDFS-17385 | Replace Global FSLock(write) with **Global FSLock(read) + IIPLock** | Yes (config switch) |
| III | HDFS-17386 | Replace Global BMLock with **DNLock + INodeFileLock** | **No** — too invasive |

### 1.4 Cross-cutting "Problems" tickets (Resolved)
The doc also enumerates several pre-existing global-lock implementations that block FGL and must be refactored first:

| JIRA | Topic | Status |
|------|-------|--------|
| HDFS-17473 | Quota consistency under FGL (introduce `QuotaLocks` lock pool) | Resolved |
| HDFS-17480 | `getListing` RPC under FGL | Resolved |
| HDFS-17482 | Quota update when completing a block (alternative to HDFS-10843) | Resolved (HDFS-17497) |
| HDFS-17505 | Storage-policy propagation so BR/IBR no longer needs IIPLock | Resolved |
| HDFS-17507 | Symlink handling | Resolved |
| HDFS-17479 | Snapshot interaction with FGL | Resolved |

These are prerequisites for Phase II/III, **not** sub-tasks of HDFS-17384.

---

## 2. HDFS-17384 — Phase I Completion Verification

### 2.1 Target deliverables (per design doc §"Phase I")
- Replace the single global lock by two locks:
  - **FSLock** — protects directory-tree / namespace state (LeaseManager, EditLog, ErasureCodingPolicy, DelegationTokenSecretManager, …).
  - **BMLock** — protects BlockManager / DatanodeManager state.
- Acquire order: `FSLock → BMLock`. HA-related operations acquire both writes.
- **No change to processing logic** — purely a lock substitution.
- Administrators can switch the lock mode (`global` vs `fine_grained`) via configuration.

### 2.2 Evidence from trunk
- Package `org.apache.hadoop.hdfs.server.namenode.fgl/` now contains:
  - `FSNLockManager.java` (abstraction)
  - `GlobalFSNamesystemLock.java` (legacy single lock)
  - `FineGrainedFSNamesystemLock.java` (new dual-lock impl)
- Foundation commits on trunk:
  - `HDFS-17387` Abstract configuration locking mode (#6572)
  - `HDFS-17394` Remove unused `WriteHoldCount` (#6571)
  - `HDFS-17390` `FSDirectory` supports FGL (#6573)
  - `HDFS-17398` Implement the FGL lock for `FSNLockManager` (#6599)
  - `HDFS-17405` Different metric name for FGL vs Global lock (#6600)
- Subsystem-by-subsystem migration commits all merged:
  - **Client RPCs**: HDFS-17388 (write), 17389 (read), 17410 (file-attr), 17411 (snapshot), 17412 (maintenance)
  - **Datanode/Namenode protocols**: HDFS-17414, 17415, 17417
  - **BlockManager internals**: HDFS-17413 (CacheReplicationMonitor), 17416 (BM monitor threads), 17423 (BM SafeMode)
  - **Edit log / HA**: HDFS-17420 (EditLogTailer + FSEditLogLoader)
  - **Misc**: HDFS-17395 (ECPolicy), 17424 (DelegationTokenSecretManager), 17472 (gcDeletedSnapshot/getDelegationToken), 17445 (misc operations)
- Test & docs:
  - `HDFS-17457` UTs support FGL (#6741)
  - `HDFS-17459` Add documentation (#6737)
- Performance validation: `HDFS-17506. [FGL] Performance for phase 1`
- Post-merge bug fixes still flowing in (proves the code is alive in trunk):
  - HDFS-17691 — move `FSNamesystemLockMode` to util package (#7232)
  - HDFS-17692 — fix bug in `getContentSummary` (#7233)
  - HDFS-17697 — `hasWriteLock`/`hasReadLock` shouldn't `assert` (#7250)
  - HDFS-17701 — javadoc fixes (#7256)
- Aggregate PR consolidating Phase I onto trunk: `apache/hadoop#6762`.
- JIRA state: HDFS-17384 **Resolved / Fixed**, **Fix Version 3.5.0**.

### 2.3 Completion-verification checklist

| Verification item | Method | Result |
|-------------------|--------|--------|
| FSLock / BMLock abstraction exists | Inspect `fgl/` package on trunk | ✅ `FineGrainedFSNamesystemLock`, `GlobalFSNamesystemLock`, `FSNLockManager` present |
| Configurable switch between modes | `HDFS-17387` introduces config | ✅ Merged |
| All RPC paths migrated | Sub-task list (27 items) merged | ✅ All read/write/snapshot/maintenance/file-attr RPC tickets merged |
| BlockManager / DatanodeManager / SafeMode use BMLock | `HDFS-17414/17415/17416/17417/17423` | ✅ Merged |
| EditLog / HA paths migrated | `HDFS-17420` | ✅ Merged |
| Unit tests adapted | `HDFS-17457` | ✅ Merged |
| Docs updated | `HDFS-17459` | ✅ Merged |
| Performance baseline measured | `HDFS-17506` | ✅ Merged |
| Released | Fix Version | ✅ `3.5.0` |

**Verdict — HDFS-17384 is fully completed and shipped in Hadoop 3.5.0.** Only minor follow-up bug fixes remain (HDFS-17691/17692/17697/17701, all already merged). No remaining acceptance work.

---

## 3. HDFS-17385 — Phase II R&D Plan Split

### 3.1 Phase II scope (per design)
- Directory ops protected by `Global FSLock(read) + IIPLock` (lock chain on path).
- Block / DN ops still use `BMLock`.
- After Phase II, three lock modes coexist behind config: `global` / `fs+bm` / `fs(read)+bm+iip`.

### 3.2 Sub-tasks already filed under HDFS-17385

| # | JIRA | Title | Trunk merged? |
|---|------|-------|---------------|
| 1 | HDFS-17474 | Make `INodeMap` thread-safe | ❌ not in trunk |
| 2 | HDFS-17487 | Make `rollEdits` thread-safe | ❌ |
| 3 | HDFS-17489 | Implement a `LockPool` | ❌ |
| 4 | HDFS-17490 | Make `INodesInPath` closeable | ❌ |
| 5 | HDFS-17491 | Make `INode#getFullPathName` thread-safe | ❌ |
| 6 | HDFS-17492 | Abstract an `INodeLockManager` | ❌ |
| 7 | HDFS-17493 | `Create` RPC supports FGL | ❌ |
| 8 | HDFS-17494 | `GetFileInfo` supports FGL | ❌ |
| 9 | HDFS-17517 | Abstract IIPLock mode covering all RPCs | ❌ |
| 10 | HDFS-17603 | Abstract a `LockManager` to manage locks | ❌ |

> Branch `HDFS-17385` exists in the repo (announced 2024-05-07) but **none of the sub-tasks have been merged to trunk yet**. Phase II is in active development on the feature branch.

### 3.3 Proposed R&D work-stream split

The Phase II work naturally decomposes into **5 independent tracks** that can be executed in parallel by different contributors and merged track-by-track. Each track has clear entry/exit gates.

#### Track A — Core lock infrastructure (foundation, must land first)
| Step | JIRA | Output |
|------|------|--------|
| A.1 | **HDFS-17489** | High-throughput `LockPool` (target ≥ 30B QPS as in design doc benchmark; capacity = `AmplificationFactor × HandlerCount × AvgDirectoryDepth`). |
| A.2 | **HDFS-17492 / HDFS-17603** | `INodeLockManager` / `LockManager` abstractions; defines `LockInstance` ref-counting, bind/unbind to active INodes, `RetryException` when pool exhausted. |
| A.3 | New | `TraceLogManager` for lock-leak detection (mirrors `DataSetLockManager`). Used in UT, optional in production. |
| A.4 | New | Deadlock monitor thread (periodic blocked-thread scan). |

**Exit gate:** All abstractions compile against current `FSNLockManager`, unit tests cover acquire/release ordering, lock-leak detection works in UT.

#### Track B — Path-resolution rewrite (must land after Track A)
| Step | JIRA | Output |
|------|------|--------|
| B.1 | **HDFS-17474** | Make `INodeMap` thread-safe (concurrent reads/writes during path resolution). |
| B.2 | **HDFS-17491** | Make `INode#getFullPathName` thread-safe. |
| B.3 | **HDFS-17490** | Make `INodesInPath` `Closeable`; carry per-INode `LockInstance` references; release in reverse order on `close()`. |
| B.4 | New | Implement `acquireIIPLockForPath()` and `acquireIIPLockForInodeId()` per design (root-to-leaf order, retry-on-rename for INode-ID variant, max retries → `RetryException`). |
| B.5 | **HDFS-17517** | `IIPLockMode` enum: `LOCK_READ`, `LOCK_WRITE`, `LOCK_PARENT`, `LOCK_ANCESTOR`, `NONE`. |

**Exit gate:** Micro-benchmark of `getFileInfo` showing 0 contention on disjoint branches; chaos test renaming files concurrently with `addBlock`-style ID-based resolution.

#### Track C — Cross-cutting consistency problems (can run in parallel with B)
These tickets are prerequisites flagged by the doc; most are already done but Phase II depends on them being on the same branch.

| Step | JIRA | Output | Status |
|------|------|--------|--------|
| C.1 | HDFS-17473 | `QuotaLocks` lock pool; quota update under per-`DirectoryWithQuotaFeature` lock; root-to-leaf acquisition order. | ✅ done |
| C.2 | HDFS-17497 | Update space consumed at **commit-block**, not complete-block; rolls back HDFS-10843 BR/FS coupling. | ✅ done |
| C.3 | HDFS-17480 | `getListing` RPC under FGL. | ✅ done |
| C.4 | HDFS-17479 | Snapshot interaction (read ops use IIPLock + global read; write ops still take global write lock; rename across snapshots handled). | ✅ done |
| C.5 | HDFS-17507 | Symlink resolution under FGL. | ✅ done |
| C.6 | HDFS-17505 | Recursive storage-policy propagation so BR/IBR no longer needs IIPLock. | ✅ done |

**Exit gate:** Forward-port these patches into the `HDFS-17385` branch (already in trunk, just confirm rebases cleanly).

#### Track D — RPC migration to IIPLock (depends on A + B)
Migrate ClientProtocol RPCs in batches, ordered by risk:

| Batch | Example RPCs | Lock mode | Sample tickets |
|-------|--------------|-----------|----------------|
| D.1 — read-only | `getFileInfo`, `getBlockLocations`, `getContentSummary`, `listStatus` | `LOCK_READ` | **HDFS-17494** |
| D.2 — single-INode mutators | `setPermission`, `setOwner`, `setReplication`, `setStoragePolicy`, `setTimes`, `setXAttr` | `LOCK_WRITE` | new |
| D.3 — parent mutators | `rename`, `delete` (single), `concat` | `LOCK_PARENT` (lexicographic order for multi-path) | new |
| D.4 — ancestor mutators | `mkdirs`, `create(createParent=true)`, `addBlock` | `LOCK_ANCESTOR` | **HDFS-17493** |
| D.5 — block-pipeline | `complete`, `abandonBlock`, `updatePipeline`, `fsync` (resolved by INode-ID) | `LOCK_WRITE` via inode-ID acquisition | new |
| D.6 — admin/meta | `getQuotaUsage`, `setQuota`, `setAcl`, snapshot read RPCs | mixed | new |

**Exit gate per batch:** `TestFSNamesystemLockMode#fineGrained` UT, plus a stress test with 256 client threads on disjoint branches; 0 deadlocks across 24h soak run.

#### Track E — Background services & Edit log (depends on A)
| Step | JIRA | Output |
|------|------|--------|
| E.1 | **HDFS-17487** | `rollEdits` thread-safe without holding global write lock — temporarily block `logEdit` during the swap. |
| E.2 | New | `LeaseManager` reviewed for IIPLock interaction (it currently holds its own lock; verify acquisition order). |
| E.3 | New | `FSEditLogLoader` re-validated under IIPLock when replaying edits. |
| E.4 | New | `FSImageSaver` / `saveNamespace` continues to take global write FSLock — no change needed but add an integration test. |

**Exit gate:** Standby NameNode tailing latency improvement measured; no regression in NN startup time.

### 3.4 Phase II Definition of Done
1. All sub-tasks above merged into the `HDFS-17385` branch.
2. Configurable lock mode `fs(read)+bm+iip` selectable at runtime.
3. Performance: documented improvement ≥ N× over Phase I on disjoint-branch concurrent write benchmark.
4. 24h soak run, 0 deadlocks, 0 lock leaks (TraceLogManager).
5. Documentation update (extend `HDFS-17459`).
6. Aggregate PR onto trunk, mirroring the `#6762` model used for Phase I.

---

## 4. HDFS-17386 — Phase III R&D Plan Split

> **Re-validated 2026-04-21** against `HDFS-17385-fork` branch and upstream
> Apache JIRA. Supersedes the 2026-04-07 Track C "all prerequisites done"
> claim — see §4.2 for the corrected state.

### 4.1 Phase III scope (per design)

Replace the global `BMLock` with:
- **`INodeFileLock`** for all block-related work (BR/IBR, RedundancyMonitor, complete, addBlock).
- **`DNLock`** for all DataNode-related work (heartbeat, registerDataNode, decommission, maintenance, cacheReport).

The "BlockLock" is conceptually identical to the `INodeFile` lock — *blocks with the same `BCId` share a lock*. The design doc is explicit that the fourth strategy is the only viable one (per-block locks would consume too much memory; range/hash sharing is incompatible with file-level dependencies).

**Important constraint from the design:** Phase III is **NOT configurable** — its data-model changes (eventual consistency for storage policy / quota, removal of HDFS-13671 block-block dependencies) cannot coexist with the legacy code path. **This means every prerequisite must actually land in trunk before Phase III can be merged — there is no fallback envelope like the Phase II pilot uses.**

### 4.2 Current state (validated 2026-04-21)

**Upstream prerequisites — corrected picture.** §3.3 Track C listed these as "✅ done." Verified against `git log --all` on `HDFS-17385-fork` and `trunk`: none of the six JIRA numbers appear in any ref. The "Resolved" status in JIRA is administrative — likely closed by the design author when the umbrella was approved, without the implementations landing in Apache trunk.

| JIRA | Topic | JIRA Status | Fix Version | In trunk code? |
|------|-------|-------------|-------------|----------------|
| HDFS-17473 | QuotaLocks | Resolved / Fixed | None | **No** |
| HDFS-17479 | Snapshot under FGL | **Open** | None | No |
| HDFS-17480 | `getListing` under FGL | Resolved / Fixed | None | **No** |
| HDFS-17497 | Quota-at-commit-block | **Open** (PR #6765) | None | No |
| HDFS-17505 | Storage-policy propagation | Resolved / Fixed | None | **No** |
| HDFS-17507 | Symlink under FGL | Resolved / Fixed | None | **No** |

The Phase II pilot works around this gap by **envelope fallback**: `ancestorsAllowCreate` rejects any quota ancestor; `ancestorsAllowMutate` additionally rejects snapshot / non-default storage-policy ancestors; `canUsePilotBase` disables the entire pilot when `provider != null` (any KMS key provider configured). **This workaround is not available to Phase III** — non-configurable means prerequisites must land for real.

**Phase III-specific state:**
- JIRA HDFS-17386: Open, no sub-tasks filed, no PRs.
- No `DNLock` / `INodeFileLock` classes anywhere in the repo.
- No Phase III scaffolding in `fgl/` package — it is Phase-II-only.
- Full block report still takes `writeLock(RwLockMode.GLOBAL)` at `BlockManager.java:2913`.

**BM-lock inventory to be dismantled:** 31 `writeLock(BM)` + 15 `readLock(BM)` call sites across 8 files.
- **DN-side (→ `DNLock`):** `DatanodeManager`, `HeartbeatManager`, `DatanodeAdminBackoffMonitor`, DN-oriented sites in `BlockManager`.
- **File-side (→ `INodeFileLock`):** client RPC entry points in `FSNamesystem`, `BlockManager` block-map mutations, `CacheManager`, `FSDirWriteFileOp`.

### 4.3 Proposed R&D work-stream split

Six tracks (Track P added for prerequisites) with strict dependencies: **P before everything; F before G/H; I in parallel; J last.**

#### Track P — Upstream prerequisites (must land first)

These are the six items from §3.3 Track C that are **not actually in trunk**. Each needs a real implementation, PR, and merge — not just a JIRA resolution.

Placeholder IDs (`P.1`–`P.6`) are fine for planning; upstream JIRAs can be re-opened / re-filed when each track actually starts work.

| Step | JIRA (existing) | Placeholder | Status | Action |
|------|-----------------|-------------|--------|--------|
| P.1 | HDFS-17497 | — | Open, PR #6765 | **Ported to `HDFS-17386-fork` as commit 7afb70cf2c4 (2026-04-21).** Kept FGL `RwLockMode.GLOBAL` assertion; migrated ported test to JUnit 5; fixed "recoery" javadoc typo and `"/dir`"` test-path typo via multi-persona review. Compile + 104 related tests pass. Upstream PR #6765 was auto-closed stale — revive or re-file when merging upstream. |
| P.2 | HDFS-17473 (QuotaLocks) | `P.2-TBD` | Resolved w/o code | Implement `QuotaLocks` pool + per-`DirectoryWithQuotaFeature` lock; re-open or re-file when starting |
| P.3 | HDFS-17505 (storage-policy propagation) | `P.3-TBD` | Resolved w/o code | **Landed on `HDFS-17386-fork` (2026-05-12).** Eager recursive propagation: `setStoragePolicy` / `unsetStoragePolicy` on a directory now stamps the effective policy onto every descendant file (via `FSDirAttrOp.propagateStoragePolicyToDescendantFiles`); new files inherit the parent's effective policy at create time; rename re-propagates from the destination's parent. After P.3 every `INodeFile`'s local header carries the effective policy, so BR/IBR / BlockManager no longer walk the ancestor chain (and so no longer need the FS lock) to read storage policy. Envelope narrowed — `ancestorsAllowCreate` / `ancestorsAllowMutate` no longer reject non-default ancestor policies. 3 new integration tests; full FGL_IIP suite (155 tests in `TestFSNamesystemFGLIIP`) and `TestBlockStoragePolicy` (25 tests) green. Semantic change: a file's effective policy now follows its current location even if it previously had an explicit per-file policy — intentional per HDFS-17505. Known caveat: `verifyQuotaForRename` still uses `INode.isSetStoragePolicy()` to decide whether to use src or dst policy for the quota delta check; with eager propagation that flag can no longer distinguish "inherited" from "explicit", so cross-policy rename quota checks may use the src's effective policy and miss a destination-type violation (see `TestQuota.testRename` regression). Strict enforcement would require an explicit-bit on `INodeFile`; deferred. |
| P.4 | HDFS-17479 (snapshot under FGL) | — | Open | Design + implement snapshot interaction with IIP lock |
| P.5 | HDFS-17480 (`getListing` under FGL) | `P.5-TBD` | Resolved w/o code | Phase II pilot has a working `getListingPilot`. **Option 1 landed on `HDFS-17386-fork` as commit dcaf6267886 (2026-04-22)** — INodePath-style `startAfter` now handled inside the pilot via `FSDirStatAndListingOp.resolveInodePathStartAfter`; 2 new tests in `TestFSNamesystemFGLIIP`, full 150-test suite still green. Broader formalisation deferred — see §4.3.1. |
| P.6 | HDFS-17507 (symlink under FGL) | `P.6-TBD` | Resolved w/o code | **Landed on `HDFS-17386-fork` as commit 1259352d123 (2026-05-11).** Replaced the `PilotEnvelopeMissException` raised on ancestor symlinks with a direct `UnresolvedPathException`, matching trunk's `FSPermissionChecker.checkNotSymlink` contract. Client's `FileSystemLinkResolver` retries transparently with the resolved path — no global-lock fallback round-trip. 5 new unit + 2 new integration tests; full FGL_IIP suite (267 tests) green; `TestSymlinkHdfsFileSystem` (72 tests) unchanged. |

**Exit gate:** All six prerequisites merged to trunk. Phase II pilot's envelope fallbacks can be narrowed because the underlying cases are now correctly handled.

##### §4.3.1 Track P.5 follow-up work (deferred from Option 1 scope)

The initial P.5 commit on `HDFS-17386-fork` (Option 1 — narrow) only closes the INodePath-style `startAfter` gap. The work below is intentionally deferred so the current commit stays focused; record here so we don't lose it.

**A. Envelope-gate removals — ride on other tracks (not new P.5 commits):**
- Remove `/.reserved` src gate in `canUsePilotBase` → after **Track I.1** lands.
- Remove `/.snapshot` src gate in `canUsePilotBase` → after **Track P.4** lands.
- Remove `provider != null` (KMS) gate in `canUsePilotBase` → after **Track I.3** lands.
- Remove `ancestorsAllow*` quota gate → after **Track P.2** (QuotaLocks) lands.

Each is roughly one-line deletion in `canUsePilotBase` / `canUsePilotGetListing` as the corresponding track's PR lands.

**B. Formalisation cleanup — only when all A-gates are gone:**
- Fold `getListingPilot` into `getListing` as the primary (non-pilot) path.
- Delete `getListingForPilot`; merge back into `getListingInt`.
- Delete `canUsePilotGetListing` if no other callers.
- Simplify the `FSNamesystem.getListing` entry point (drop `tryPilot` dispatch).
- **Requires Track J.2** (`FSNLockManager` exposes only the final 4-lock model) — otherwise the `fsLock instanceof IIPBasedFSNamesystemLock` check in `canUsePilotBase` cannot be removed.

**C. Phase III integration — when `BMLock` is being torn down:**
- `needLocation=true` variant of `getListing` builds `LocatedBlocks` per child file. Today the pilot relies on `readLock(BM)` for this (matching `getBlockLocations`). Under Phase III, that read becomes per-child `readLock(INodeFileLock)`. Refactor belongs with **Track H.6**.

**D. Upstream contribution — independent of Phase III:**
- Once B is done, draft a real PR for HDFS-17480 from the formalised code. Expect committer review + rebase cycle; upstream trunk may have drifted.

**E. Tests to add at appropriate time:**
- Concurrent `delete` + listing on same parent directory (stale-batch behaviour).
- Large-directory pagination with mid-batch rename of the next-seen entry.
- `needLocation=true` listing under FGL (after Track H lands).
- Snapshot listing (after Track P.4 removes that envelope gate).

#### Track F — Block/INode dependency removal

Re-scoped after code re-validation:

| Step | JIRA | Output | Verified state |
|------|------|--------|----------------|
| F.1 | (Deferred) | Roll back HDFS-13671. `FoldedTreeSet` **no longer in the codebase** — Track F.1 is likely moot. Verify large-directory-deletion benchmarks before declaring complete. | **Moot pending verification** |
| F.2 | "[FGL] Volatile replica state in DatanodeDescriptor and BlockInfo" | Mark DN state fields (`isAlive`, `disallowed`, `heartbeatedSinceRegistration`, …) and block state fields (`replication`, `uc`) as `volatile` so `RedundancyMonitor` sees latest state without holding DN lock. | **Real work** — currently only `BlockInfo.bcId` is volatile |
| F.3 | Depends on P.3 | Confirm HDFS-17505 recursive storage-policy propagation lands via Track P. | Dependency on P.3 |
| F.4 | Depends on P.1 | Confirm HDFS-17497 commit-block quota lands via Track P. | Dependency on P.1 |

**Exit gate:** A block can be processed end-to-end while holding only its `INodeFileLock` (no IIP lock, no DN lock).

#### Track G — `DNLock` infrastructure

| Step | New JIRA | Output |
|------|----------|--------|
| G.1 | "[FGL] Per-DataNode lock pool" | Allocate one lock per `DatanodeDescriptor`; reuse `LockPool` from the `fgl/iip` package. |
| G.2 | "[FGL] `registerDataNode` + `sendHeartbeat` under DNLock" | Targets: `DatanodeManager.java:903`, `HeartbeatManager.java:518,527`. Replace `writeLock(BM)` with per-DN lock + read FSLock. |
| G.3 | "[FGL] HeartbeatManager parallel-DN redesign" | Multiple DNs processed in parallel by acquiring DN locks individually; tune `HeartbeatManager.Monitor` step loop. |
| G.4 | "[FGL] DatanodeAdminBackoffMonitor under DNLock" | Targets: `DatanodeAdminBackoffMonitor.java:174,364`. Decom/maintenance scans take per-DN lock, never global BM. |
| G.5 | "[FGL] `cacheReport` and DN-side `BlockManager` under DNLock" | Targets: DN-oriented call sites in `BlockManager`. |

**Exit gate:** Heartbeat throughput scales linearly with handler count up to 256 DNs; no global BM lock contention in flame graph.

#### Track H — `INodeFileLock` infrastructure (critical path)

This track structurally fixes the Phase II regression on `addBlock` (restores `chooseTarget` parallelism).

| Step | New JIRA | Output | Key risk |
|------|----------|--------|----------|
| H.1 | "[FGL] Per-INodeFile lock pool" | Pool sized like DNLock; reuse `LockPool`. | — |
| H.2 | "[FGL] Acquire-by-BlockID algorithm" | Design §"How to Acquire an INodeFileLock for a Block ID": (1) load `BlockInfo` lock-free, (2) read `bcId` (already volatile ✓), (3) lock `INodeMap`, (4) acquire `INodeFile` lock, (5) re-validate `BlockInfo` still belongs to file, (6) retry. | Concat/delete races |
| H.3 | "[FGL] BR processing under INodeFileLock" | Split `processReport` loop: per-block INodeFile lock acquire/release. Replace `writeLock(GLOBAL)` at `BlockManager.java:2913`. | Longest-lived migration |
| H.4 | "[FGL] IBR under INodeFileLock" | `processIncrementalBlockReport` currently `writeLock(GLOBAL)` at `FSNamesystem.java:7118`. Depends on P.1 to remove the FS-write coupling via `completeBlock` quota. | FS-coupling via quota |
| H.5 | "[FGL] RedundancyMonitor under INodeFileLock" | Under/over-replicated scans operate per-INodeFile; depends on F.2. | — |
| H.6 | "[FGL] `complete` / `addBlock` / `abandonBlock` / `updatePipeline` under INodeFileLock" | Migrates the block-lifecycle RPCs currently on `PATH_WRITE + writeLock(BM)` in the Phase II pilot. **This is the fix for the Phase II `addBlock` regression.** | — |
| H.7 | "[FGL] BlockManagerSafeMode under INodeFileLock" | SafeMode block-count tracking lock-free or via per-file locks. | — |
| H.8 | "[FGL] CacheReplicationMonitor + CacheManager under INodeFileLock" | Cache directive processing per file. | — |
| H.9 | "[FGL] EC blocks (`BlockInfoStriped`) verification" | Per design — same INodeFileLock model applies. Add focused tests. | — |

**Exit gate:** All BR/IBR paths free of `writeLock(GLOBAL)` / `writeLock(BM)`; flame graph shows lock acquisition concentrated on per-file locks; Phase II `addBlock` regression reversed.

#### Track I — Reserved-path & special-case handling

| Step | New JIRA | Output |
|------|----------|--------|
| I.1 | "[FGL] `/.reserved/iNodes/${id}` resolution" | Skip prefix, use INode-ID acquisition path. |
| I.2 | "[FGL] `/.reserved/raw/${path}` resolution" | Skip prefix, normal IIPLock acquisition. |
| I.3 | "[FGL] Encryption-zone support under FGL" | **Critical:** Phase II disables the entire pilot when `provider != null`. Phase III must work *correctly* under KMS. Involves `EncryptionZoneManager` lookup redesign + per-path EZ test inside the lock plan. May require its own design doc before implementation. |

**Exit gate:** `canUsePilotBase`'s `provider != null` gate can be removed. KMS-enabled clusters use the full FGL plan.

#### Track J — Cleanup, integration & release

| Step | Output |
|------|--------|
| J.1 | Remove the `BMLock` code path entirely (no config switch — Phase III is non-configurable by design). |
| J.2 | Update `FSNLockManager` to expose only the final 4-lock model (`Global FSLock + DNLock + IIPLock + INodeFileLock`). |
| J.3 | Stress / chaos suite: rename + BR + RedundancyMonitor + decommission + `addBlock` concurrently, 24h, 0 deadlock, 0 stale state. Extend the existing FGL_IIP stress test suite. |
| J.4 | Performance validation: ≥ 7× over global-lock baseline on mixed workload; **explicit regression test: `addBlock`-only workload ≥ legacy throughput** (verifies the Phase II regression is gone). |
| J.5 | Documentation refresh (extends `HDFS-17459` + `docs/fgl/`). |
| J.6 | Aggregate PR onto trunk. |

### 4.4 Risks specific to Phase III

| Risk | Mitigation |
|------|-----------|
| Eventual-consistency change for storage-policy / quota updates may surprise operators | Document semantic change in release notes; provide a verification CLI that crawls quotas. |
| Track P prerequisites take longer than expected to land upstream | Track P is the critical path — front-load it; Tracks G/H cannot start until at least P.1 + P.3 land. |
| Phase II pilot's envelope fallbacks mask prerequisite gaps → Phase III discovers them late | Each Track P step should be validated end-to-end (remove the corresponding envelope check in the pilot, run the benchmark, confirm no regression). |
| Removal of HDFS-13671 optimisation may regress huge-directory deletion *outside* FGL | Already-moot in current code (FoldedTreeSet absent), but add a benchmark gate. Phase III has no global-lock fallback. |
| Concat-during-block-processing race (H.2) | Soak test with concat + BR + addBlock running concurrently. |
| Lock-ordering bugs | Mandatory deadlock-detection in pre-merge UT (extend the pilot's existing `TraceLogManager` pattern). |
| EZ handling redesign (I.3) is larger than §4 previously suggested | Scope I.3 as its own design doc before implementation; may require its own JIRA umbrella. |

### 4.5 Phase III Definition of Done

1. All Track P–J tickets merged into trunk (Track P upstream directly; others via `HDFS-17386` branch aggregated into trunk).
2. Final lock model (`Global FSLock + DNLock + IIPLock + INodeFileLock`) is the **only** model present. `BMLock` removed.
3. `GlobalFSNamesystemLock` retained only for HA write operations and `saveNamespace`.
4. Performance: ≥ 7× over global-lock baseline on mixed workload; `addBlock`-only workload ≥ Phase I legacy throughput (regression fix verified).
5. 24h chaos soak run passes.
6. Documentation + release notes updated.
7. KMS-enabled cluster works without envelope fallback (Track I.3 validated).

### 4.6 Sequencing summary

```
Track P (prerequisites)  ──────────────────────────────┐
                                                        │
Track F (F.2 volatile state; F.1 verify)   ────────────┼──► Track H
                                                        │    (critical path)
Track G (DNLock)         ──────────────────────────────┤
                                                        │
Track I (reserved paths + EZ) ─────────────────────────┤
                                                        │
                                                        ▼
                                               Track J (cleanup,
                                                        perf, release)
```

Critical path runs **P → H**. Tracks F / G / I can run in parallel once Track P lands the pieces each depends on.

---

## 5. Summary

| Phase | JIRA | Status | Next action |
|-------|------|--------|-------------|
| Phase I | HDFS-17384 | **✅ Resolved, shipped in 3.5.0.** Only cosmetic follow-up fixes (HDFS-17691/17692/17697/17701) post-merge. | None — closed. Monitor for regressions. |
| Phase II | HDFS-17385 | 🚧 Active pilot on `HDFS-17385-fork`, **~41 RPCs migrated**, 0 sub-tasks merged upstream. Cross-cutting prerequisites (HDFS-17473/17479/17480/17497/17505/17507) **were listed as done in §3.3 but are NOT actually in trunk** — 2 still Open, 4 Resolved-without-Fix-Version. The pilot works around this via envelope fallback (`ancestorsAllowCreate/Mutate`, `canUsePilotBase(provider != null)`). | Execute Tracks A→E (§3.3); land upstream prerequisites (now Track P in §4.3); land aggregate PR like #6762. |
| Phase III | HDFS-17386 | 📋 Open, **no sub-tasks filed, no code**. **Non-configurable** — cannot ship with envelope fallbacks; every prerequisite must actually land in trunk. | Front-load Track P (prerequisites), then Tracks F (volatile state) / G (DNLock) / I (EZ) in parallel, then Track H (INodeFileLock critical path), then Track J (cleanup). See §4.3. |

The phased plan in the design doc maps cleanly onto these three JIRAs; **Phase I is the only one that is genuinely complete today**, and the previous claim that Phase II prerequisites were all done was a JIRA-status-as-proxy mistake — code verification against `HDFS-17385-fork`/trunk disproves it.
