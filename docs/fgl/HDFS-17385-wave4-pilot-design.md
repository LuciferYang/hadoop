---
title: HDFS-17385 Phase II Pilot Design — FGL_IIP Vertical Slice
status: Draft (post-100-check round 5)
author: yangjie01
date: 2026-04-07
target_release: Hadoop 3.6.0
related_jira:
  - HDFS-17366  # Parent epic (FGL initiative)
  - HDFS-17384  # Phase I (done, shipped in 3.5.0)
  - HDFS-17385  # Phase II (this spec)
  - HDFS-17474  # U5 INodeMap thread safety
  - HDFS-17491  # U6 getFullPathName thread safety
  - HDFS-17489  # U1 LockPool
  - HDFS-17517  # U2 IIPAcquireMode
  - HDFS-17492  # U3 INodeLockManager
  - HDFS-17603  # U4 IIPBasedFSNamesystemLock (+ U0 factory)
  - HDFS-17490  # U7 LockedIIP
  - HDFS-17494  # U8a getFileInfo pilot
  - HDFS-17493  # U8b create pilot (envelope-scoped)
sibling_specs:
  - HDFS-17487-rolledits-design.md  # (to be written) rollEdits thread safety
supersedes: none
---

# HDFS-17385 Phase II Pilot Design — FGL_IIP Vertical Slice

## Table of contents

1. [Architecture and unit boundaries](#1-architecture-and-unit-boundaries)
2. [Component internals and data flow](#2-component-internals-and-data-flow)
3. [Cross-cutting concerns and the pilot envelope](#3-cross-cutting-concerns-and-the-pilot-envelope)
4. [Testing strategy, KPIs, and CI](#4-testing-strategy-kpis-and-ci)
5. [Rollout, runbook, and operational reference](#5-rollout-runbook-and-operational-reference)
6. [Process, gates, and the migration checklist](#6-process-gates-and-the-migration-checklist)
7. [Glossary](#7-glossary)

## Scope statement

This spec describes a **vertical slice pilot** of Phase II of the HDFS Fine-Grained Locking (FGL) initiative. It is deliberately not a complete Phase II design. Its purpose is to:

1. Land the minimum lock infrastructure for directory-tree per-INode locking.
2. Migrate exactly two RPCs (`getFileInfo` and scoped `create`) as a canonical reference implementation.
3. Prove the end-to-end pattern under performance KPIs.
4. Freeze the template before the remaining ~40 RPCs migrate.

Phase II in full covers ~40 RPCs, background services, cross-cutting concerns (quota, snapshot, encryption zones, storage policy, erasure coding), and removal of Phase I code. None of that is in this spec. Each will get its own sub-spec after the pilot gate passes (§6.2).

**JIRA sibling spec:** HDFS-17487 (`rollEdits` thread safety) is independent and has its own spec.

## 1. Architecture and unit boundaries

### 1.1 Goal

Deliver a vertical slice of Phase II (config mode `FGL_IIP`) consisting of: (a) the minimum lock infrastructure to hold per-INode directory-tree locks, (b) two pilot RPCs migrated to it, and (c) a parallel code path selectable by config at NameNode startup, fully coexisting with Phase I's `FGL` mode and the legacy `GLOBAL` mode.

### 1.2 Key concepts

- **Compat namespace lock** — the `FSLock` of the composed Phase I `FineGrainedFSNamesystemLock`, reused by `IIPBasedFSNamesystemLock` under a new name for clarity of role. Used only by (a) un-migrated RPCs, (b) checkpoint / `saveNamespace`, (c) edit-log replay. Migrated RPCs acquire **compat-read** as part of every IIP acquisition (so compat-write quiesces all IIP ops) but otherwise never touch it directly. It is not a new lock; it is the same `ReentrantReadWriteLock` instance created by Phase I.
- **`IIPAcquireMode`** — closed taxonomy of lock-acquisition patterns. Full enum is declared in the pilot; only two entries are implemented; the rest throw `UnsupportedOperationException`.
- **Pilot envelope for `create`** — explicit two-phase precondition set; anything outside the envelope delegates to the composed `FineGrainedFSNamesystemLock`.
- **Top-down path lock walk** (referred to as "hand-over-hand walk" throughout the spec for brevity, though it's not classical hand-over-hand crabbing — the walk *holds* all ancestor locks rather than releasing parents as children lock). Path resolution and lock acquisition happen together in a single top-down pass, not in separate "resolve then lock" phases. This is correct under concurrent subtree mutation and eliminates the need for path revalidation. Classical hand-over-hand release can be added as a post-pilot optimization.

### 1.3 Config selection

Single config `dfs.namenode.lockmode` ∈ `{GLOBAL, FGL, FGL_IIP}`. At NameNode startup, a factory instantiates one of three `FSNLockManager` implementations:

| Mode | Implementation | Status |
|---|---|---|
| `GLOBAL` | `GlobalFSNamesystemLock` | existing, unchanged |
| `FGL` | `FineGrainedFSNamesystemLock` | existing, unchanged (Phase I) |
| `FGL_IIP` | `IIPBasedFSNamesystemLock` | **new in this spec** |

No code in `FSNamesystem` or RPC handlers branches on `dfs.namenode.lockmode`. Dispatch is purely virtual through the `FSNLockManager` interface (mitigation #2: single chokepoint).

### 1.4 `IIPAcquireMode` taxonomy

| Mode | Lock grid | Pilot? |
|---|---|---|
| `GLOBAL_READ` | compat-read | fallback only |
| `PATH_READ` | ancestors read (root→leaf), target read | **yes** — `getFileInfo`, `getBlockLocations`, `isFileClosed`, `getListing`, `getXAttrs`, `listXAttrs`, `getStoragePolicy`, `getPreferredBlockSize`, `getAclStatus`, `getErasureCodingPolicy`, `checkAccess` |
| `PATH_WRITE` | ancestors read (root→parent), target write | **yes** — `setPermission`, `setOwner`, `setTimes`, `setReplication`, `setStoragePolicy`, `unsetStoragePolicy`, `setAcl`, `modifyAclEntries`, `removeAclEntries`, `removeDefaultAcl`, `removeAcl`, `setXAttr`, `removeXAttr`, `setErasureCodingPolicy`, `unsetErasureCodingPolicy` |
| `PARENT_WRITE` | ancestors read (root→parent's parent), parent write; new child created under parent's lock | **yes** — `create`, `mkdirs`, `delete` (single-file) |
| `ANCESTOR_WRITE` | ancestors read (root→target ancestor), subtree root write; descendants iterated without per-INode locks | deferred |
| `RENAME_WRITE` | two paths; src-parent and dst-parent acquired in ascending-INode-ID order | deferred |
| `ADMIN_META` | compat-write | fallback for admin RPCs |

All seven modes are declared in the enum; the two still-deferred modes throw `UnsupportedOperationException` in the pilot. Adding a new mode requires editing this table and filing a follow-up ticket (mitigation #1: full taxonomy upfront).

`PATH_WRITE` was originally DEFERRED in the initial pilot cut and promoted to PILOT ahead of the §6.3 RPC-10 refactor gate. Front-loading the mode gives the gate review a third data-point mode (alongside `PATH_READ` and `PARENT_WRITE`) rather than five post-pilot RPCs discovering `PATH_WRITE` after the pattern is frozen.

### 1.4a RPC migration tracking

Comprehensive status of every NameNode RPC that takes a namespace lock (`RwLockMode.FS` or `RwLockMode.GLOBAL`) in `FSNamesystem`. Updated 2026-04-16.

**Migrated (31 RPCs):**

| RPC | Mode | Lock | Category |
|---|---|---|---|
| `getFileInfo` | PATH_READ | FS | stat |
| `getBlockLocations` | PATH_READ | GLOBAL | read |
| `isFileClosed` | PATH_READ | FS | stat |
| `getListing` | PATH_READ | FS | listing |
| `getXAttrs` | PATH_READ | FS | xattr |
| `listXAttrs` | PATH_READ | FS | xattr |
| `getStoragePolicy` | PATH_READ | FS | attr |
| `getPreferredBlockSize` | PATH_READ | FS | attr |
| `getAclStatus` | PATH_READ | FS | ACL |
| `getErasureCodingPolicy` | PATH_READ | FS | EC |
| `checkAccess` | PATH_READ | FS | perm |
| `create` (startFile) | PARENT_WRITE | FS | write |
| `mkdirs` | PARENT_WRITE | FS | write |
| `delete` (single-file) | PARENT_WRITE | GLOBAL | write |
| `delete -r` (recursive) | ANCESTOR_WRITE | GLOBAL | write |
| `setPermission` | PATH_WRITE | FS | attr |
| `setOwner` | PATH_WRITE | FS | attr |
| `setTimes` | PATH_WRITE | FS | attr |
| `setReplication` | PATH_WRITE | GLOBAL | attr+BM |
| `setStoragePolicy` | PATH_WRITE | FS | attr |
| `unsetStoragePolicy` | PATH_WRITE | FS | attr |
| `setAcl` | PATH_WRITE | FS | ACL |
| `modifyAclEntries` | PATH_WRITE | FS | ACL |
| `removeAclEntries` | PATH_WRITE | FS | ACL |
| `removeDefaultAcl` | PATH_WRITE | FS | ACL |
| `removeAcl` | PATH_WRITE | FS | ACL |
| `setXAttr` | PATH_WRITE | FS | xattr |
| `removeXAttr` | PATH_WRITE | FS | xattr |
| `setErasureCodingPolicy` | PATH_WRITE | FS | EC |
| `unsetErasureCodingPolicy` | PATH_WRITE | FS | EC |
| `truncate` | PATH_WRITE | GLOBAL | write+BM+lease |
| `append` | PATH_WRITE | GLOBAL | write+BM+lease |
| `rename` | RENAME_WRITE | GLOBAL | write |

**Unmigrated — feasible with existing modes (high priority):**

| RPC | Candidate mode | Lock | Frequency | Notes |
|---|---|---|---|---|
| `completeFile` | PATH_WRITE + BM | GLOBAL | **very hot** — every file write | Closes file under construction; BM interaction |
| `fsync` | PATH_WRITE | GLOBAL | **very hot** — every hflush | Updates last block length; edit log |
| `getAdditionalBlock` | PATH_READ + BM (validate) then PATH_WRITE + BM (allocate) | GLOBAL | **very hot** — every block allocation | Two-phase: read-validate then write-allocate |
| `abandonBlock` | PATH_WRITE + BM | GLOBAL | medium — on write failure | Removes a block from a file under construction |
| `contentSummary` | PATH_READ | GLOBAL | **hot** — `hdfs dfs -count/-du` | Subtree walk; may be long-running under read lock |
| `quotaUsage` | PATH_READ | GLOBAL | hot | Similar to contentSummary |
| `createSymlink` | PARENT_WRITE | FS | low | Non-standard shape (no getPermissionChecker call) |
| `satisfyStoragePolicy` | PATH_WRITE + BM + xattr | FS | low | Complex xattr + BM internals |
| `recoverLease` | PATH_WRITE + lease | GLOBAL | low | Lease recovery |
| `concat` | Custom multi-source | GLOBAL | low | Merges blocks from multiple source files |

**Unmigrated — snapshot operations (out of pilot scope):**

| RPC | Lock | Notes |
|---|---|---|
| `allowSnapshot` | FS | Admin: toggles snapshot feature |
| `disallowSnapshot` | FS | Admin: removes snapshot feature |
| `createSnapshot` | GLOBAL | Creates snapshot |
| `deleteSnapshot` | GLOBAL | Deletes snapshot + block cleanup |
| `renameSnapshot` | GLOBAL | Renames snapshot |
| `computeSnapshotDiff` | FS | Read: computes diff between 2 snapshots |
| `gcDeletedSnapshot` | GLOBAL | Admin: GCs deleted snapshots |
| `ListSnapshot` | FS | Read: lists snapshots |
| `listSnapshottableDirectory` | FS | Read: lists snapshottable dirs |

**Unmigrated — admin / infrastructure (not path-based, use ADMIN_META or BM-only):**

| RPC | Lock | Notes |
|---|---|---|
| `getDelegationToken` / `renewDelegationToken` / `cancelDelegationToken` | FS | Token ops; not namespace-path-based |
| `addCacheDirective` / `modifyCacheDirective` / `removeCacheDirective` | GLOBAL | Cache management |
| `addCachePool` / `modifyCachePool` / `removeCachePool` | GLOBAL | Cache pool admin |
| `listCacheDirectives` / `listCachePools` | GLOBAL | Cache listing |
| `addErasureCodingPolicies` / `enableErasureCodingPolicy` / `disableErasureCodingPolicy` / `removeErasureCodingPolicy` | FS | EC policy admin (not per-path) |
| `getErasureCodingPolicies` / `getErasureCodingCodecs` / `getECTopologyResultForPolicies` | FS | EC policy listing |
| `createEncryptionZone` / `listEncryptionZones` / `reencryptEncryptionZone` / `listReencryptionStatus` | GLOBAL/FS | EZ admin |
| `rollEditLog` / `saveNamespace` | GLOBAL | Lifecycle |
| `startRollingUpgrade` / `finalizeRollingUpgrade` / `queryRollingUpgrade` / `finalizeUpgrade` | GLOBAL/FS | Upgrade lifecycle |
| `refreshNodes` / `metaSave` / `setBalancerBandwidth` | GLOBAL | Cluster admin |
| `datanodeReport` / `slowDataNodesReport` / `listOpenFiles` / `listCorruptFileBlocks` | GLOBAL/FS | Monitoring |

### 1.5 Pilot envelope for `create`

`IIPBasedFSNamesystemLock` handles a `create` request through the IIP path **only if all** of the following hold:

**Phase A — path-only (lock-free):**
- Path is not under `.snapshot`.
- Path is not under `/.reserved/raw`.
- Path is not under `/.reserved/.inodes`.
- `overwrite=false`.
- `favoredNodes` is empty or null.
- Path is not under a known encryption zone (consult `EZManager` cache).
- Path is not under a known erasure-coding zone (consult `ECPolicyCache`).

**Phase B — under held `PARENT_WRITE`:**
- No ancestor on the path has quota set (walk ancestors under held read locks).
- No ancestor has a non-default storage policy.
- No ancestor has a non-default EC policy.
- Parent has no default ACL (to avoid XAttr inheritance paths).
- No path component is a symlink (detected during the walk; special-cased).

Otherwise: release IIP locks and delegate to the composed `FineGrainedFSNamesystemLock` path. `getFileInfo` has no envelope — it handles `.inodes` reserved paths via the thread-safe `INodeMap` (U5) and delegates snapshot/`raw` paths.

### 1.6 Unit boundaries

Nine units (U0 added for the factory wiring):

| # | Unit | Purpose | Lines (est.) | JIRA |
|---|---|---|---|---|
| U0 | `FSNLockManager` factory (factory) | Pick implementation from `dfs.namenode.lockmode`; add `FGL_IIP` branch. Existing two branches unchanged. Includes `hdfs-default.xml` additions documenting all new config keys. | ~60 diff | HDFS-17603 |
| U1 | `LockPool` (lockPool) | Ref-counted allocator of `ReentrantReadWriteLock` keyed by long INode ID. Atomic get-or-create via `ConcurrentHashMap.compute()`. Returns `LockRef` handle that pins the entry; eviction waits on refcount = 0. Package-private. | ~250 | HDFS-17489 |
| U2 | `IIPAcquireMode` enum (mode) | Closed taxonomy (§1.4). Package-private. | ~80 | HDFS-17517 |
| U3 | `INodeLockManager` (lockManager) | Given path + `IIPAcquireMode`, performs hand-over-hand walk acquiring locks top-down with per-level mode pre-determined. Honors interrupt with rollback, honors timeout. Returns `LockedIIP` handle. Package-private. | ~500 | HDFS-17492 |
| U4 | `IIPBasedFSNamesystemLock` (lockManagerImpl) | `FSNLockManager` implementation. Composes `FineGrainedFSNamesystemLock` (Phase I) as fallback for un-migrated RPCs. Owns an `INodeLockManager` instance. Exposes new `lockPath(path, mode)` method on the interface. | ~500 | HDFS-17603 |
| U5 | `INodeMap` thread-safe (inodeMap) | Striped-lock wrapper around existing `LightWeightGSet` for `get(long id)` and `put(INode)`. Iteration unchanged and requires the caller to hold compat-write. Universal benefit — lands for all three modes. | ~150 diff | HDFS-17474 |
| U6 | `getFullPathName` thread-safe (fullPathName) | Version counter on parent chain (`parentVersion`); bounded retry (10 attempts); best-effort fallback on final retry (no lock acquisition). Universal benefit. | ~180 diff | HDFS-17491 |
| U7 | `LockedIIP` wrapper (lockedIIP) | New wrapper class in `fgl.iip`. Holds an `INodesInPath`, a list of `LockRef`, and the compat-read lock token. `AutoCloseable`. Idempotent `close()`. Single-thread use (document, do not enforce). Package-private. | ~120 | HDFS-17490 |
| U8 | Pilot RPC body refactor (rpcRefactor) | `FSNamesystem.getFileInfo(...)` and `FSNamesystem.startFile(...)` rewritten to wrap their existing body in a `try (LockedIIP lip = fsLock.lockPath(path, mode))` and read the IIP from `lip.iip()`. Works for all three modes because `lockPath` has virtual dispatch. Includes envelope check placement, fallback delegation plumbing, metric instrumentation. | ~150 diff across both | HDFS-17494, HDFS-17493 |

### 1.7 Dependency graph and merge sequence

```
U5 INodeMap (universal) ─────┐
U6 getFullPathName (univ.)───┼─── (thread-safe path resolution) ──┐
                             │                                    │
U1 LockPool ──┐                                                    │
              ├──> U3 INodeLockManager ──> U7 LockedIIP ──────────┤
U2 Mode ──────┘                                                    │
                                                                   │
                                       U0 Factory ──┐              │
                                                    ├──> U4 IIPBasedFSNamesystemLock ──> U8 pilot RPC refactor
```

**Merge order** (no feature branch — all work lands on `trunk` behind `FGL_IIP` config):

1. **Parallel phase:** U5 (HDFS-17474), U6 (HDFS-17491), U1 (HDFS-17489), U2 (HDFS-17517) — no cross-deps.
2. U3 (HDFS-17492) — needs U1+U2.
3. U7 (HDFS-17490) — needs U3 + thread-safe path resolution from U5/U6.
4. U4 + U0 (HDFS-17603) — needs U7.
5. U8a (HDFS-17494) — `getFileInfo` pilot, needs U4.
6. U8b (HDFS-17493) — `create` pilot, needs U4. Parallel with U8a.

Six sequential landing steps; four units parallelize in step 1. Target Hadoop version: 3.6.0.

### 1.8 Lock ordering invariants

Any implementation must obey:

1. **Ancestor before descendant.** Top-down walk, read-lock each ancestor, then the target's lock.
2. **Ancestor always read-locked in IIP modes** — only the target can be write-locked in pilot modes.
3. **Sibling locks ordered by ascending INode ID** — applies to `RENAME_WRITE` (deferred).
4. **IIPLock always before BMLock.** Never the reverse.
5. **Compat-write never held under an IIP lock.** Compat-read re-entry is permitted.
6. **No read→write upgrade.** Any RPC pattern requiring both must acquire write upfront (RRWL will throw on upgrade attempts).

### 1.9 Field-to-lock mapping

| INode field(s) | Protected by |
|---|---|
| `permissions`, `owner`, `group`, `modificationTime`, `accessTime`, XAttrs, ACL | self lock |
| `children` (directory) | self lock acting as parent |
| `parent` pointer | `volatile`; readers tolerate mid-rename value via `parentVersion` counter; writers bump version under both src-parent and dst-parent write locks |
| `blocks`, `replication`, `storagePolicy` (file) | self lock in pilot; Phase III moves to `INodeFileLock` |
| `quota`, `storageSpaceConsumed` (directory) | self lock (pilot doesn't enter quota trees) |
| `parentVersion` (new) | single-writer under old/new parent write lock; volatile read by walkers |

### 1.10 Scope exclusions

- All RPCs except `getFileInfo` and `create` (within envelope).
- Snapshot paths, encryption zones, `/.reserved/raw`, quota-bearing trees, non-default storage policies, erasure-coded files, overwrite, `favoredNodes`, symlinks in path, default ACL inheritance.
- Rename, delete, mkdir, setPermission, addBlock, complete, abandonBlock, recursive operations.
- Background services: `EditLogTailer`, `LeaseManager` (beyond trivial per-create lease add), `saveNamespace`, `rollEdits`. `rollEdits` has its own sibling spec (HDFS-17487).
- All quota / snapshot / storage-policy / EZ cross-cutting concerns — audited before pilot merges; fixes filed as separate tickets.
- Deletion of Phase I or legacy code. `GlobalFSNamesystemLock` and `FineGrainedFSNamesystemLock` remain untouched.

### 1.11 Phase I prerequisites (required diffs to existing Phase I code)

The pilot composes Phase I's `FineGrainedFSNamesystemLock`. That composition requires additions to Phase I code that don't necessarily exist today. These are prerequisite diffs, filed as separate JIRA tickets blocking U4:

| Prereq | What it adds | Why |
|---|---|---|
| P1 | `FineGrainedFSNamesystemLock(Configuration, BMLock)` constructor | Allow shared BMLock injection from the factory so both composed and outer managers use the same instance |
| P2 | `FineGrainedFSNamesystemLock.getFSLock()` accessor (package-private) | Expose Phase I's FSLock as the compat lock for the outer manager |
| P3 | `FineGrainedFSNamesystemLock.setFSDirectory(FSDirectory)` setter | Setter injection to break construction cycle (if not already present) |
| P4 | Override `hasWriteLock(RwLockMode)` / `hasReadLock(RwLockMode)` in `IIPBasedFSNamesystemLock` to recognize IIP-held contexts | Existing assertions use Phase I's mode-aware API. Rather than editing ~21 call sites, a centralized override makes existing assertions pass transparently under FGL_IIP. ~50 lines. |
| P5 | Add `default` method `lockPath(String, IIPAcquireMode)` to `FSNLockManager` interface | Avoid breaking third-party implementations (drop on fork-only development) |

Prereqs P1–P3 are small Phase I diffs. **P4 scoping spike (completed during round-5 review):** 162 `hasWriteLock`/`hasReadLock` call sites exist across 27 files in `namenode/`, BUT ~141 are in code the pilot doesn't touch (EZ, EC, cache, rename, delete, snapshot, etc.). The pilot-path subset is ~21 assertions concentrated in `FSNamesystem.java` (~5), `FSDirWriteFileOp.java` (6), `FSDirStatAndListingOp.java` (2), `FSDirectory.java` (~6), `LeaseManager.java` (~2).

**Phase I already parameterized assertions by `RwLockMode`** (e.g., `assert hasWriteLock(RwLockMode.FS)`). The pilot can leverage this: add a centralized override in `IIPBasedFSNamesystemLock` that returns `true` for `hasWriteLock(FS|GLOBAL)` when the current thread holds an IIP write lock (tracked via a new `HELD_IIP_WRITE` ThreadLocal set in `LockedIIP`). All 21 assertions then pass transparently without any call-site edits.

**P4 total diff: ~50 lines in `IIPBasedFSNamesystemLock` + `LockedIIP`.** P4 is not a blocker.

### 1.12 What this is not

A complete Phase II. The remaining ~40 RPCs, background services, cross-cutting fixes, and Phase I removal all require additional specs. This spec produces **one landable end-to-end vertical slice** whose purpose is to prove the pattern, establish the canonical shape, and freeze the template before bulk migration begins.

### 1.12a Effort estimate (honest sizing)

Summing the unit line estimates:

| Component | Production | Tests |
|---|---|---|
| U0 factory + `hdfs-default.xml` | ~60 | ~50 |
| U1 `LockPool` + `LockRef` | ~250 | ~400 |
| U2 `IIPAcquireMode` + `LockPlan` | ~80 | ~120 |
| U3 `INodeLockManager` | ~500 | ~800 |
| U4 `IIPBasedFSNamesystemLock` | ~500 | ~600 |
| U5 `INodeMap` striping | ~150 | ~300 |
| U6 `getFullPathName` version | ~180 | ~350 |
| U7 `LockedIIP` + INodesInPath factory | ~120 | ~250 |
| U8 RPC refactor (both RPCs) | ~150 | ~500 |
| JMH benchmark harness | ~500 | — |
| Phase I diffs (P1–P5) | ~200 | ~200 |
| **Total (estimated)** | **~2,690** | **~3,570** |

**Grand total: ~6,260 lines of new code + tests.** P4 (hasWriteLock assertion audit) is not included because its cost is unknown until the scoping spike runs — it could add 500–2000 more lines of diff across existing Phase I code.

**Calendar time:** 2–3 months of focused engineering work for a single engineer working on nothing else; 4–6 months realistic if split across multiple contributors working part-time. Apache upstream contribution adds review cycles: add 1–3 months of lag for upstream signoff (given HDFS committer bandwidth). **Plan accordingly.**

### 1.13 Known design-level limitations (acknowledged, not fixed in pilot)

These are architectural constraints the pilot accepts. Each is a candidate for a post-pilot optimization ticket.

- **Compat-read on every migrated RPC is a scalability bottleneck.** Every `lockPath` call acquires `compatLock.readLock()`. At very high concurrency (>32 cores saturated), the RRWL's shared reader-count updates become a contention point. The pilot's KPIs (§4.6) are sized accordingly. A post-pilot ticket should explore epoch-based quiescence (RCU-style) to replace compat-read for migrated RPCs.
- **`parentVersion` field adds 4–8 bytes per INode.** For 1B-INode clusters this is 4–8 GB of heap. The pilot accepts this cost. A future optimization can move the counter to a side-table keyed by INode ID (allocated only for INodes that have ever been renamed), reducing memory to proportional-to-rename-activity.
- **Un-migrated write RPCs serialize the NN via compat-write.** The pilot accepts this because un-migrated writes are typically < 5% of workload. Clusters with heavier un-migrated-write load should not enable FGL_IIP until more RPCs migrate.
- **Pilot envelope rejects quota, EZ, EC, snapshot, overwrite, favoredNodes, symlinks, default ACL, non-default storage policy.** Enterprise clusters with heavy quota/EZ use may hit envelope miss on >80% of creates, reducing the pilot's benefit. Operators should profile their workload before enabling — see §5.8 and the runbook.
- **Full `hadoop-hdfs` test suite passing under `-Dlockmode=FGL_IIP` is aspirational.** Many existing tests assume FSLock semantics and will fail subtly. The gate criterion (§6.2) is softened to "the identified subset of tests covering migrated RPCs passes." Broader test suite migration is a post-pilot effort.
- **Pilot validates only 2 of ~7 lock patterns.** `PATH_READ` and `PARENT_WRITE` are exercised; `PATH_WRITE`, `ANCESTOR_WRITE`, `RENAME_WRITE` are declared but unimplemented. The canonical pattern frozen at pilot gate may need revision when these are first implemented.

---

## 2. Component internals and data flow

### 2.1 Package layout

All new classes live in `org.apache.hadoop.hdfs.server.namenode.fgl.iip`. Package-private where possible; only `IIPBasedFSNamesystemLock` is public for the factory.

```
o.a.h.h.s.namenode.fgl.iip/
  LockPool.java                  // U1  (package-private)
  LockRef.java                   // U1  (package-private)
  IIPAcquireMode.java            // U2  (package-private)
  INodeLockManager.java          // U3  (package-private)
  LockedIIP.java                 // U7  (package-private)
  IIPBasedFSNamesystemLock.java  // U4  (public)
  CreatePilotEnvelope.java       // envelope helper for U8b
  FGLockMetrics.java             // metrics MBean
  LockAcquisitionTimeoutException.java
  PilotEnvelopeMissException.java  // thrown from acquire() on walk-time envelope miss (symlink in ancestor, etc.)
  package-info.java
```

### 2.2 `LockPool` and `LockRef`

```java
final class LockPool {
  private static final int INITIAL_CAPACITY = 1024;
  private final ConcurrentHashMap<Long, Entry> pool =
      new ConcurrentHashMap<>(INITIAL_CAPACITY);

  static final class Entry {
    // FAIR. Round-3 switched to unfair for throughput; round-4 reverted because
    // unfair RRWL does not guarantee writer progress — a hot parent under
    // steady read load can starve PARENT_WRITE indefinitely. Fair mode costs
    // ~3× per-acquire overhead under contention but guarantees liveness.
    // Post-pilot optimization can revisit once benchmarks show the actual cost.
    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    int refs = 1;  // guarded by CHM.compute() atomicity on the key
  }

  /** Pin (ref++) and return the entry for inodeId, creating if absent. */
  Entry pin(long inodeId) {
    Entry[] out = new Entry[1];
    pool.compute(inodeId, (k, existing) -> {
      if (existing == null) {
        Entry e = new Entry();
        out[0] = e;
        return e;
      }
      existing.refs++;
      out[0] = existing;
      return existing;
    });
    return out[0];
  }

  /** Unpin (ref--); evict when refs hits 0. */
  void unpin(long inodeId) {
    pool.compute(inodeId, (k, existing) -> {
      if (existing == null) {
        throw new IllegalStateException("unpin of unknown id " + k);
      }
      existing.refs--;
      return existing.refs == 0 ? null : existing;
    });
  }

  int size() { return pool.size(); }
}
```

`LockRef` owns both the pool pin and the held RRWL lock, so `close()` is the single exit path. Thread-affinity: acquired and closed by the same thread.

```java
final class LockRef implements AutoCloseable {
  private final LockPool pool;
  private final long inodeId;
  private final Lock heldLock;   // either readLock() or writeLock()
  private final boolean isWrite;
  private boolean closed = false;

  static LockRef acquire(LockPool pool, long id, boolean write, long deadlineNanos)
      throws InterruptedException, LockAcquisitionTimeoutException {
    LockPool.Entry e = pool.pin(id);
    boolean pinHeld = true;
    Lock l = null;
    boolean lockHeld = false;
    try {
      l = write ? e.lock.writeLock() : e.lock.readLock();
      long remaining = deadlineNanos - System.nanoTime();
      if (remaining <= 0 || !l.tryLock(remaining, TimeUnit.NANOSECONDS)) {
        throw new LockAcquisitionTimeoutException(id, write);
      }
      lockHeld = true;
      LockRef ref = new LockRef(pool, id, l, write);
      // Ownership transferred to the new LockRef — neither unlock nor unpin
      // in finally.
      pinHeld = false;
      lockHeld = false;
      return ref;
    } finally {
      // If lockHeld but pinHeld both still true, LockRef construction failed
      // after we acquired the lock. Release both in reverse acquisition order.
      if (lockHeld) l.unlock();
      if (pinHeld) pool.unpin(id);
    }
  }

  @Override public void close() {
    if (closed) return;
    closed = true;
    try { heldLock.unlock(); } finally { pool.unpin(inodeId); }
  }
}
```

**Invariants:**
- Pin happens before `tryLock`; unpin happens after `unlock`.
- Any exception path (timeout, interrupt, anything else) unpins before propagating.
- `close()` is idempotent for the same thread; concurrent `close()` from a different thread is a programming error and not supported.

### 2.3 `IIPAcquireMode`

```java
enum IIPAcquireMode {
  // Pilot-implemented
  PATH_READ(Impl.PILOT),
  PARENT_WRITE(Impl.PILOT),
  // Fallback modes
  GLOBAL_READ(Impl.FALLBACK),
  ADMIN_META(Impl.FALLBACK),
  // Declared but deferred (throw UnsupportedOperationException in pilot)
  PATH_WRITE(Impl.DEFERRED),
  ANCESTOR_WRITE(Impl.DEFERRED),
  RENAME_WRITE(Impl.DEFERRED);

  enum Impl { PILOT, FALLBACK, DEFERRED }
  final Impl impl;
  IIPAcquireMode(Impl impl) { this.impl = impl; }

  /** Is the innermost target-level lock a write lock? */
  boolean isWriteTarget() {
    return this == PATH_WRITE || this == PARENT_WRITE || this == RENAME_WRITE
        || this == ANCESTOR_WRITE || this == ADMIN_META;
  }
}
```

### 2.4 `INodeLockManager` — hand-over-hand walk

The walk performs path resolution and lock acquisition together. Ancestor reads protect subsequent `getChild` calls. The deepest lock type is determined from the mode before walking.

```java
final class INodeLockManager {
  private final LockPool pool;
  private volatile FSDirectory fsd;  // setter injected (see §2.11)
  private final ReentrantReadWriteLock compatLock;  // shared with composed Phase I mgr
  private final Duration defaultTimeout;
  private final FGLockMetrics metrics;

  /** ThreadLocal counter for invariant enforcement (rules 4, 5). */
  static final ThreadLocal<Integer> HELD_IIP_DEPTH =
      ThreadLocal.withInitial(() -> 0);

  void setFSDirectory(FSDirectory fsd) { this.fsd = fsd; }

  LockedIIP acquire(String path, IIPAcquireMode mode, Duration timeoutOverride)
      throws IOException, InterruptedException {
    if (mode.impl != IIPAcquireMode.Impl.PILOT) {
      throw new UnsupportedOperationException("mode " + mode + " not pilot-ready");
    }
    // Nested acquisition is forbidden in the pilot. See §1.13.
    if (HELD_IIP_DEPTH.get() > 0) {
      throw new IllegalStateException(
          "nested IIP acquisition is forbidden; depth=" + HELD_IIP_DEPTH.get());
    }
    Duration timeout = timeoutOverride != null ? timeoutOverride : defaultTimeout;
    long deadline;
    try {
      deadline = Math.addExact(System.nanoTime(), timeout.toNanos());
    } catch (ArithmeticException overflow) {
      deadline = Long.MAX_VALUE;
    }

    // Special case: /.reserved/.inodes/<id>
    if (isInodesReservedPath(path)) {
      return acquireByInodeId(path, mode, deadline);
    }

    byte[][] components = INode.getPathComponents(path);
    int writeDepth = computeWriteDepth(mode, components.length);
    if (writeDepth == INVALID_PATH_FOR_MODE) {
      throw new InvalidPathException("mode " + mode + " requires non-root path: " + path);
    }

    // Acquire compat-read with deadline.
    long remaining = deadline - System.nanoTime();
    if (remaining <= 0 || !compatLock.readLock().tryLock(remaining, TimeUnit.NANOSECONDS)) {
      throw new LockAcquisitionTimeoutException("compat-read", false);
    }

    List<LockRef> held = new ArrayList<>(components.length);
    INode[] nodesAlongPath = new INode[components.length];
    boolean success = false;
    try {
      INode current = fsd.getRoot();
      for (int depth = 0; depth < components.length; depth++) {
        if (current == null) break;  // path doesn't fully exist
        if (!(current instanceof INodeDirectory) && depth < components.length - 1) {
          // ancestor is not a directory → bail; RPC body will throw
          break;
        }
        // Symlinks in ANCESTOR path are out of pilot scope (envelope miss).
        // A symlink AS the target is fine — getFileInfo on a symlink returns
        // the symlink itself (DirOp.READ_LINK semantics).
        if (current instanceof INodeSymlink && depth < components.length - 1) {
          throw new PilotEnvelopeMissException("symlink in ancestor path: " + path);
        }
        boolean writeHere = (depth == writeDepth);
        held.add(LockRef.acquire(pool, current.getId(), writeHere, deadline));
        nodesAlongPath[depth] = current;

        if (depth + 1 < components.length && current instanceof INodeDirectory) {
          current = ((INodeDirectory) current).getChild(
              components[depth + 1], Snapshot.CURRENT_STATE_ID);
        }
      }

      INodesInPath iip = INodesInPath.fromComponentsAndNodes(components, nodesAlongPath);
      // Allocate LockedIIP BEFORE incrementing the depth counter, so that
      // an OOM during allocation doesn't leave the counter at +1 with no
      // handle to decrement it.
      LockedIIP result = new LockedIIP(iip, held, compatLock.readLock());
      HELD_IIP_DEPTH.set(1);
      success = true;
      metrics.incAcquireSuccess(mode);
      return result;
    } finally {
      if (!success) {
        releaseInReverse(held);
        compatLock.readLock().unlock();
      }
    }
  }

  private int computeWriteDepth(IIPAcquireMode mode, int pathLen) {
    return switch (mode) {
      case PATH_READ -> -1;           // all reads, no write target
      case PARENT_WRITE      -> pathLen - 2;  // second-to-last (parent); invalid if <0
      default                -> throw new IllegalStateException("unreachable");
    };
  }

  private static void releaseInReverse(List<LockRef> held) {
    for (int i = held.size() - 1; i >= 0; i--) {
      try { held.get(i).close(); }
      catch (RuntimeException re) { LOG.warn("lock release failed", re); }
    }
  }
}
```

**Why no revalidation retry loop.** Once all path locks are held, the path is stable. Nobody can rename any ancestor because they'd need write on an ancestor we hold in read. This is the main benefit of hand-over-hand vs resolve-then-lock.

**Why no backoff.** There's no retry loop.

**`.inodes` reserved path.** `acquireByInodeId(path, mode, deadline)` parses the ID, calls `fsd.getInode(id)` (thread-safe via U5), acquires a single target lock, and returns a `LockedIIP` wrapping a single-node IIP. No walk, no ancestors. Post-acquire, re-check `fsd.getInode(id) != null` (defensive; under compat-read, delete is blocked, so this can't fail in pilot).

### 2.5 `LockedIIP`

```java
public final class LockedIIP implements AutoCloseable {
  private final INodesInPath iip;
  private final List<LockRef> heldLocks;
  private final Lock compatReadLock;
  private boolean closed = false;

  LockedIIP(INodesInPath iip, List<LockRef> locks, Lock compat) {
    this.iip = iip;
    // Defensive immutable copy makes ownership transfer explicit and prevents
    // the caller from mutating the list after handing it to this wrapper.
    this.heldLocks = List.copyOf(locks);
    this.compatReadLock = compat;
  }

  public INodesInPath iip() { return iip; }

  @Override public void close() {
    if (closed) return;
    closed = true;
    for (int i = heldLocks.size() - 1; i >= 0; i--) {
      try { heldLocks.get(i).close(); }
      catch (RuntimeException re) { LOG.warn("LockRef close failed", re); }
    }
    try { compatReadLock.unlock(); }
    catch (RuntimeException re) { LOG.warn("compat release failed", re); }
    int depth = INodeLockManager.HELD_IIP_DEPTH.get();
    if (depth <= 0) LOG.warn("HELD_IIP_DEPTH underflow on close()");
    INodeLockManager.HELD_IIP_DEPTH.set(Math.max(0, depth - 1));
  }
}
```

**Properties:**
- Idempotent for single-thread use.
- Releases in reverse (leaf→root); compat-read released last.
- Never throws a checked exception; runtime exceptions during individual releases are logged and swallowed.
- **Not thread-safe** — must be closed by the thread that acquired it. Document, do not enforce.

### 2.6 `IIPBasedFSNamesystemLock`

Composition: holds a `FineGrainedFSNamesystemLock` as the fallback for un-migrated RPCs. Both share the same `BMLock` instance (critical — two instances would allow deadlock).

```java
public final class IIPBasedFSNamesystemLock implements FSNLockManager {
  private final FineGrainedFSNamesystemLock fallback;
  private final INodeLockManager inodeLockManager;
  private final ReentrantReadWriteLock compatLock;  // == fallback.getFSLock()

  public IIPBasedFSNamesystemLock(Configuration conf, BMLock sharedBmLock) {
    this.fallback = new FineGrainedFSNamesystemLock(conf, sharedBmLock);
    this.compatLock = fallback.getFSLock();
    this.inodeLockManager = new INodeLockManager(
        new LockPool(), compatLock, timeoutFromConf(conf), newMetrics(conf));
  }

  public void setFSDirectory(FSDirectory fsd) {
    inodeLockManager.setFSDirectory(fsd);
    fallback.setFSDirectory(fsd);
  }

  /**
   * Exposed for envelope-miss delegation from FSNamesystem. Public because
   * FSNamesystem lives in a different package. Marked for test visibility
   * even though it's a production entry point — the intent is "use only
   * when the pilot envelope rejects an RPC."
   */
  @VisibleForTesting
  public FineGrainedFSNamesystemLock fallback() { return fallback; }

  /** Primary entry point for migrated RPCs. */
  @Override
  public LockedIIP lockPath(String path, IIPAcquireMode mode)
      throws IOException, InterruptedException {
    return inodeLockManager.acquire(path, mode, null);
  }

  // ===== Un-migrated path: delegate to fallback =====
  @Override public void readLock(RwLockMode m) { fallback.readLock(m); }
  @Override public void writeLock(RwLockMode m) {
    if (m == RwLockMode.GLOBAL || m == RwLockMode.FS) assertNoIIPHeld();
    fallback.writeLock(m);
  }
  @Override public void readLockInterruptibly(RwLockMode m) throws InterruptedException {
    fallback.readLockInterruptibly(m);
  }
  @Override public void writeLockInterruptibly(RwLockMode m) throws InterruptedException {
    if (m == RwLockMode.GLOBAL || m == RwLockMode.FS) assertNoIIPHeld();
    fallback.writeLockInterruptibly(m);
  }
  // ... other interface methods delegate identically ...

  /** Rule 5: compat-WRITE never under IIP lock (compat-read re-entry is allowed). */
  private static void assertNoIIPHeld() {
    if (INodeLockManager.HELD_IIP_DEPTH.get() > 0) {
      throw new IllegalStateException(
          "rule 5 violation: compat-write acquired while IIP lock held");
    }
  }
}
```

**`FSNLockManager` interface extension.** A new method `lockPath(String, IIPAcquireMode)` is added to the interface. All three implementations provide it:
- `GlobalFSNamesystemLock.lockPath` acquires global read/write per mode, resolves IIP once under that lock, returns `LockedIIP.legacy(iip, releaseCallback)`.
- `FineGrainedFSNamesystemLock.lockPath` acquires FSLock read/write per mode, resolves IIP, returns `LockedIIP.legacy(...)`.
- `IIPBasedFSNamesystemLock.lockPath` delegates to `INodeLockManager.acquire` (the hand-over-hand path).

`LockedIIP.legacy(iip, releaseCallback)` is a factory that creates a wrapper holding no `LockRef` list; on close, it invokes the release callback to unlock the global/FSLock.

### 2.7 `INodeMap` thread-safe (U5)

Striped-lock wrapper around the existing `LightWeightGSet`. Preserves the specialized memory layout that HDFS depends on at scale.

```java
public class INodeMap {
  private static final int STRIPE_COUNT = 256;  // power of 2; configurable
  private final LightWeightGSet<INode, INodeWithAdditionalFields> gset;
  private final ReentrantReadWriteLock[] stripes;

  private ReentrantReadWriteLock stripeFor(long id) {
    return stripes[(int)(id & (STRIPE_COUNT - 1))];
  }

  public INode get(long inodeId) {
    ReentrantReadWriteLock s = stripeFor(inodeId);
    s.readLock().lock();
    try { return gset.get(new INodeWithAdditionalFields.DummyINode(inodeId)); }
    finally { s.readLock().unlock(); }
  }

  public void put(INode inode) {
    ReentrantReadWriteLock s = stripeFor(inode.getId());
    s.writeLock().lock();
    try { gset.put((INodeWithAdditionalFields) inode); }
    finally { s.writeLock().unlock(); }
  }

  /** Iteration requires compat-write held externally. Enforced at runtime. */
  public Iterator<INodeWithAdditionalFields> iterator() {
    if (!IIPBasedFSNamesystemLock.compatWriteHeldByCurrentThread()) {
      throw new IllegalStateException("INodeMap.iterator requires compat-write");
    }
    return gset.iterator();
  }
}
```

- Per-key `put/get` are thread-safe; stripe release/acquire provides happens-before.
- Iteration is unchanged; all existing iteration sites (FSImage save, quota recompute, fsck) already run under compat-write, so the assertion is satisfied.
- Striping count `STRIPE_COUNT` is a power of 2 for mask-based indexing; configurable via `dfs.namenode.fgl.iip.inodemap.stripes`.

### 2.8 `getFullPathName` versioning (U6)

```java
// In o.a.h.h.s.namenode.INode (diff)
public abstract class INode {
  protected volatile INode parent;            // volatile
  protected volatile int parentVersion = 0;   // bumped on parent pointer change

  /**
   * Called from rename under write locks on both src-parent and dst-parent.
   * MUST set parent BEFORE bumping version — readers rely on this ordering.
   */
  protected void setParent(INode newParent) {
    this.parent = newParent;
    this.parentVersion++;
  }

  /** Thread-safe walk; retries on ancestor mutation; best-effort fallback. */
  public String getFullPathName() {
    for (int attempt = 0; attempt < 10; attempt++) {
      List<INode> chain = new ArrayList<>();
      IntList versions = new IntArrayList();
      INode cursor = this;
      while (cursor != null) {
        chain.add(cursor);
        versions.add(cursor.parentVersion);  // volatile read
        cursor = cursor.parent;               // volatile read
      }
      boolean stable = true;
      for (int i = 0; i < chain.size(); i++) {
        if (chain.get(i).parentVersion != versions.getInt(i)) { stable = false; break; }
      }
      if (stable) return buildPathString(chain);
    }
    // Best-effort fallback: return the last-observed walk result without re-validation.
    // Documented semantic: under extreme rename contention, may return a stale path.
    return bestEffortPath();
  }
}
```

**Single-writer invariant for `parentVersion`:** `setParent` is called only under write locks on both the old and new parent INodes, so exactly one thread can bump a given INode's version at a time. Plain `++` is safe under single-writer.

**Memory visibility via volatile:**
- Writer: `parent = new; parentVersion++;` — volatile write of parent publishes all prior work; volatile write of parentVersion publishes the new parent.
- Reader: `versions.add(parentVersion); cursor = parent;` — reading parentVersion first captures the timestamp; re-checking at the end detects any write that happened during the walk → retry.

Not all call sites need the versioned walk — debug/audit sites may continue to use a cheaper unsafe walk. The set of critical-path sites is enumerated during U6 implementation.

### 2.9 Data flow: `getFileInfo(/a/b/c)` under `FGL_IIP`

```
[FSNamesystem.getFileInfo(path)]
  1. try (LockedIIP lip = fsLock.lockPath(path, PATH_READ)) {

  [IIPBasedFSNamesystemLock.lockPath → INodeLockManager.acquire]
  2.   compatLock.readLock().tryLock(deadline, NS)                 // compat-read held
  3.   components = split("/a/b/c") = ["", "a", "b", "c"]
  4.   writeDepth = -1                                              // all reads
  5.   current = root
  6.   depth=0: acquire read(root); get child "a"
  7.   depth=1: acquire read(a);    get child "b"
  8.   depth=2: acquire read(b);    get child "c"
  9.   depth=3: acquire read(c);    (no more children needed)
 10.   INodesInPath iip = fromComponentsAndNodes(components, [root,a,b,c])
 11.   HELD_IIP_DEPTH++
 12.   return new LockedIIP(iip, [4 refs], compatReadLock)

  [FSNamesystem.getFileInfo, continued]
 13.   result = FSDirStatAndListingOp.getFileInfo(fsd, lip.iip(), ...)
 14.   return result
 15. }  // try-with-resources: LockedIIP.close()

  [LockedIIP.close]
 16.   heldLocks[3].close()  // release read(c) + unpin
 17.   heldLocks[2].close()  // release read(b) + unpin
 18.   heldLocks[1].close()  // release read(a) + unpin
 19.   heldLocks[0].close()  // release read(root) + unpin
 20.   compatReadLock.unlock()
 21.   HELD_IIP_DEPTH--
```

### 2.10 Data flow: `create(/a/b/newfile)` under `FGL_IIP`

```
[FSNamesystem.startFile(path, ...)]
  0. // Phase A envelope: lock-free
  1. Clause missA = CreatePilotEnvelope.checkPhaseA(path, flags, favored, ez, ec)
  2. if (missA != null) { delegate to fallback.create(...); return; }

  3. try (LockedIIP lip = fsLock.lockPath(path, PARENT_WRITE)) {

  [INodeLockManager.acquire]
  4.   compatLock.readLock().tryLock(deadline, NS)
  5.   components = ["", "a", "b", "newfile"]; writeDepth = 2 (parent b)
  6.   depth=0: acquire read(root); get child "a"
  7.   depth=1: acquire read(a);    get child "b"
  8.   depth=2: acquire WRITE(b);   (target "newfile" absent, not locked)
  9.   nodesAlongPath = [root, a, b, null]
 10.   return new LockedIIP(iip, [read(root), read(a), write(b)], compatRead)

  [FSNamesystem.startFile, continued]
 11.   Clause missB = CreatePilotEnvelope.checkPhaseB(lip.iip())
 12.   if (missB != null) { break try-with-resources (releases lip); delegate. }
 13.   // Pilot path: create the child INode under parent's write lock
 14.   INodeFile newFile = FSDirWriteFileOp.startFile(fsd, lip.iip(), flags, ...)
 15.   // Log edit while parent write held
 16.   long txid = fsn.getEditLog().logOpenFile(path, newFile, overwrite, logRetryCache)
 17.   // If initial block allocation needed, acquire BMLock UNDER parent write
 18.   // ... BMLock work ... release BMLock before IIP release
 19. }  // LockedIIP.close() releases write(b), read(a), read(root), compat-read
 20. // logSync OUTSIDE any IIP lock
 21. fsn.getEditLog().logSync(txid)
```

**Ordering:** BMLock (if acquired) is released before the `LockedIIP.close()` runs — satisfies invariant 4. `logSync()` runs after `close()` — matches Phase I pattern.

### 2.11 ThreadLocal invariant enforcement

Two runtime assertions:

**Rule 5** (compat-write not nested under IIP): asserted in `IIPBasedFSNamesystemLock.writeLock(RwLockMode)` when mode is `GLOBAL` or `FS`. Throws `IllegalStateException` on violation.

**Rule 4** (IIP before BMLock): best-effort. Not asserted at BMLock acquisition (would require BMLock to know about the IIP depth counter); instead caught by code review + the canonical migration checklist (§6.4). A post-pilot ticket may add an assertion hook.

**Overhead:** ~20ns per lock call (one `ThreadLocal.get()`). Enabled via `dfs.namenode.fgl.iip.assert.lock.order` (default `true` in dev/test, `false` in production).

### 2.12 Metrics

`FGLockMetrics` MBean exposed at `Hadoop:service=NameNode,name=FGLockMetrics`.

| Metric | Type | Dimensions |
|---|---|---|
| `LockPoolSize` | gauge | — |
| `LockAcquireCount` | counter | mode |
| `LockAcquireWaitNanos` | histogram | mode |
| `LockHoldNanos` | histogram | mode, sampled ≥ 100ms |
| `LockPathAcquireFailureCount` | counter | reason={timeout,interrupt,invalid} |
| `CompatReadHoldNanos` | histogram | — |
| `CompatWriteHoldNanos` | histogram | — |
| `CreateEnvelopeMissCount` | counter | clause |
| `GetFileInfoEnvelopeMissCount` | counter | clause (snapshot, reserved, symlink) |
| `GetFullPathNameRetries` | counter | — |
| `GetFullPathNameFallback` | counter | — |
| `LockOrderAssertionFailures` | counter | rule={4,5} |

### 2.13 Initialization order (setter injection)

`FSDirectory` depends on `FSNamesystem` which depends on `FSNLockManager`. To avoid circular construction:

1. Factory constructs `BMLock`.
2. Factory constructs `IIPBasedFSNamesystemLock(conf, bmLock)` — internally constructs composed `FineGrainedFSNamesystemLock(conf, bmLock)` and `INodeLockManager` with null `fsd`.
3. `FSNamesystem` constructor receives the lock manager.
4. `FSNamesystem` constructs `FSDirectory`.
5. `FSNamesystem.init` calls `fsLock.setFSDirectory(fsd)`, which cascades to `INodeLockManager.setFSDirectory(fsd)` and (already-handled by Phase I) `FineGrainedFSNamesystemLock`.

Phase I already uses setter injection for `FSDirectory`; the pilot matches.

---

## 3. Cross-cutting concerns and the pilot envelope

### 3.1 `CreatePilotEnvelope` full clause list

Phase A (path-only, lock-free):

```java
enum Clause {
  SNAPSHOT_PATH,
  RESERVED_RAW,
  RESERVED_INODES,
  OVERWRITE_REQUESTED,
  FAVORED_NODES,
  ENCRYPTION_ZONE_PATH,      // EZManager cache
  ERASURE_CODED_PATH,        // ECPolicyCache
  // Phase B clauses
  SYMLINK_IN_PATH,           // detected during walk
  PARENT_HAS_QUOTA,          // walked under held read locks
  PARENT_NON_DEFAULT_STORAGE_POLICY,
  PARENT_NON_DEFAULT_EC_POLICY,
  PARENT_HAS_ACL_INHERITANCE
}
```

**Delegation sequence on envelope miss:**
- Phase-A miss: zero lock work, direct delegation to `fallback().create(...)`.
- Phase-B miss: release `LockedIIP`, delegate (which re-acquires under compat-write + BM). Wasted work is one PARENT_WRITE acquire/release cycle. Acceptable because misses are rare; metric tracks rate per clause.

### 3.2 Permission checking: walk-then-check, not interleaved

Ideal design would interleave permission checks with the hand-over-hand walk, checking each ancestor while its lock is held. But `FSPermissionChecker` in existing HDFS has `checkPermission(iip, ...)` that walks the whole IIP at once — not a per-INode API. Decomposing it is non-trivial Phase I refactoring, out of pilot scope.

**Pilot approach:** walk completes, building the `INodesInPath`; then `FSPermissionChecker.checkPermission(iip, ...)` runs once on the full IIP **while all ancestor locks are still held** (because `LockedIIP` hasn't been closed yet). The permission check happens inside the `try (LockedIIP lip = ...)` block, before any mutation:

```java
// In FSNamesystem.getFileInfo or .startFile:
try (LockedIIP lip = fsLock.lockPath(path, mode)) {
  if (permChecker != null) {
    permChecker.checkPermission(lip.iip(), ...);  // full IIP, all locks held
  }
  // ... RPC body ...
}
```

**Safety in pilot scope:** under compat-read, `setPermission` (fallback → compat-write) is excluded. Permission state on the held INodes cannot change during the RPC. Walk-then-check is correct.

**Future caveat:** when `setPermission` migrates to IIP (post-pilot), walk-then-check becomes racy — a concurrent migrated `setPermission` could change an ancestor's ACL after our walk but before our check. At that point, `FSPermissionChecker` must be refactored to interleave per-INode. This refactor is a post-pilot ticket.

Invariant (pilot): permission checks run with all ancestor locks held, satisfying "never check on an INode whose lock is not held" in aggregate across the IIP.

### 3.3 Snapshot paths

Detected via `HdfsConstants.DOT_SNAPSHOT_DIR`. Any path containing `.snapshot` as a component → envelope miss → delegate. Applies to both `getFileInfo` and `create`. Snapshot operations run on compat-read/compat-write for the entire pilot; no Phase II benefit for snapshot workloads.

### 3.4 Encryption zones

EZ manager maintains a cache of EZ roots; `ez.isUnderAnyEZ(path)` is a lock-free cache read. Phase A rejects EZ paths. Phase B re-checks parent for `EncryptionZoneFeature` under the held parent lock, defensively (race with EZ creation is excluded by compat-write).

### 3.5 Quota

Phase B walks the IIP ancestor chain looking for any `DirectoryWithQuotaFeature`:

```java
for (INode anc : iip.getINodesArray()) {
  if (anc == null) break;
  if (anc.asDirectory().isWithQuota()) return Clause.PARENT_HAS_QUOTA;
}
```

Walk runs under already-held ancestor read locks. Pilot `create` never crosses quota-bearing directories.

### 3.6 Storage policy

Same ancestor walk, checking `localStoragePolicyID != UNSPECIFIED`.

### 3.7 Erasure coding

Same ancestor walk, checking for `ErasureCodingPolicyFeature`.

### 3.8 Reserved paths

- `/.reserved/raw/*` → Phase A miss, delegate.
- `/.reserved/.inodes/<id>` for `create` → not supported by HDFS at all; Phase A delegates, legacy path returns the standard error.
- `/.reserved/.inodes/<id>` for `getFileInfo` → special-cased in `INodeLockManager` (§2.4): lookup by ID via thread-safe `INodeMap`, lock only the target. Post-acquire existence re-check (defensive).

### 3.9 Audit logging

Audit entries are written on RPC completion, outside `try-with-resources(LockedIIP)` scope. Audit calls `getFullPathName()` (U6 versioned walk, best-effort under extreme contention). **Rule:** no `LOG.info` / `LOG.warn` inside an IIP-held scope unless strictly necessary.

### 3.10 Edit log integration

```java
long txid;
try (LockedIIP lip = fsLock.lockPath(path, PARENT_WRITE)) {
  // envelope + INode creation
  txid = fsn.getEditLog().logOpenFile(path, newFile, overwrite, logRetryCache);
  // lip.close() runs here
}
fsn.getEditLog().logSync(txid);  // outside any IIP lock
```

`FSEditLog` has its own internal txid counter + sync queue; concurrent creates on different parents allocate txids atomically in `beginTransaction()` and coalesce their logSync calls at the JournalNode layer. No cross-RPC lock needed.

### 3.11 LeaseManager

`LeaseManager` has its own lock domain (independent of FSLock/IIPLock). Pilot acquires it under `PARENT_WRITE`. Lock order: IIP parent write → LeaseManager lock → (if blocks) BMLock. **Pre-merge audit required** to confirm LeaseManager's lock discipline doesn't create a cycle under the new order.

### 3.12 FSImage save / `saveNamespace` / checkpoint

Takes compat-write: quiesces all new IIP acquisitions. In-flight IIP holders drain naturally. Checkpoint duration matches Phase I — no regression.

### 3.13 Standby NN edit log tailing and replay

Replay uses compat-write on standby. Fine-grained locks are not used on standby in pilot (single-threaded replay doesn't benefit). Standby readers (stale reads) still go through the IIP path if the RPC is migrated.

### 3.14 HA failover

Same-mode pair in steady state. Cross-mode (rolling upgrade) works because edit log format is unchanged.

### 3.15 What 3 does NOT cover

Snapshot-aware FGL, quota under FGL, LeaseManager under FGL, BlockManager under FGL, EditLogTailer under FGL, `saveNamespace` without compat-write — all post-pilot.

---

## 4. Testing strategy, KPIs, and CI

### 4.1 Test levels

| Level | Framework | What it catches |
|---|---|---|
| Unit | JUnit 5 + AssertJ | `LockPool` atomicity, `LockRef` lifecycle, `LockedIIP` idempotency, envelope clause decisions, hand-over-hand walk correctness |
| Integration (in-process NN) | `MiniDFSCluster` | Full RPC path under each mode; rename/create/getFileInfo cross-paths; HA failover; checkpoint; replay |
| Concurrency stress | JCStress + custom | Race conditions in `LockPool.pin/unpin`, `parentVersion` walk, stripe happens-before |
| Performance regression | JMH | K1–K7 thresholds |
| Full test suite | JUnit | Regressions in unchanged functionality under `-Dlockmode=FGL_IIP` |
| Fault injection | Byteman | Stretch goal |

### 4.2 Tri-mode parameterization

```java
@ParameterizedTest
@EnumSource(FSNamesystemLockMode.class)
void testGetFileInfoUnderAllModes(FSNamesystemLockMode mode) throws Exception {
  try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .setNameNodeLockMode(mode)
      .numDataNodes(3)
      .build()) {
    // ... test body ...
  }
}
```

**`MiniDFSCluster.Builder.setNameNodeLockMode(FSNamesystemLockMode)`** added as part of U0.

**Two-tier CI:**
- **PR CI:** `GLOBAL` + `FGL_IIP` only (skip `FGL`; well-validated by Phase I). Cuts PR CI time ~33%.
- **Nightly CI:** all three modes.
- **Release CI:** all modes + stress + performance + fault injection.

### 4.3 Per-unit coverage targets

| Unit | Test class | Target coverage |
|---|---|---|
| U1 `LockPool` | `TestLockPool` | 95% |
| U2 `IIPAcquireMode` | `TestIIPAcquireMode` | 100% |
| U3 `INodeLockManager` | `TestINodeLockManager` | 90% |
| U4 `IIPBasedFSNamesystemLock` | `TestIIPBasedFSNamesystemLock` | 90% |
| U5 `INodeMap` | `TestINodeMapConcurrent` | 85% |
| U6 `getFullPathName` | `TestGetFullPathNameConcurrent` | 85% |
| U7 `LockedIIP` | `TestLockedIIP` | 90% |
| U8 pilot RPCs | `TestFsnCreatePilotUnderFGL_IIP`, `TestFsnGetFileInfoPilotUnderFGL_IIP` | 90% |

Overall new-code target: **80% line, 75% branch**.

### 4.4 Concurrency stress

JCStress harness for `LockPool.pin/unpin`, `parentVersion` walk vs `setParent`, `INodeMap` striped get/put, `LockedIIP.close` from non-acquiring thread (expect log, no crash).

### 4.5 Integration scenarios (15 cases)

| # | Scenario |
|---|---|
| 1 | Create 1000 files in a single directory (single client) |
| 2 | Create 1000 files across 16 disjoint directories (16 clients) |
| 3 | `getFileInfo` while concurrent rename of ancestor |
| 4 | `getFileInfo` while concurrent delete of target |
| 5 | `create` while concurrent `rmdir` of parent |
| 6 | Envelope miss on every clause (12 sub-cases) |
| 7 | Cross-mode FSImage compatibility (write under FGL_IIP, load under GLOBAL/FGL) |
| 8 | HA failover under FGL_IIP |
| 9 | Checkpoint during active load |
| 10 | Rolling upgrade FGL → FGL_IIP |
| 11 | Rolling rollback FGL_IIP → FGL |
| 12 | `saveNamespace` via dfsadmin |
| 13 | Deep-nested path (20 levels) create + getFileInfo |
| 14 | Concurrent create and getFileInfo on same parent |
| 15 | Create after envelope-miss delegation |

Each is a `@Test` method with shared `MiniDFSCluster` via `@BeforeAll`.

### 4.6 Performance KPIs (pilot gate)

| KPI | Metric | Floor (must pass) | Target (aspirational) |
|---|---|---|---|
| K1 | single-client `getFileInfo` latency regression | ≤30% p50; ≤50% p99 | ≤10% p50; ≤20% p99 |
| K2 | single-client `create` latency regression | ≤30% p50; ≤50% p99 | ≤10% p50; ≤20% p99 |
| K3 | parallel `create` throughput (16 disjoint dirs) | ≥2× `FGL` | ≥3× `FGL` |
| K4 | parallel `getFileInfo` throughput (16 disjoint files) | ≥2× `FGL` | ≥3× `FGL` |
| K5 | hot-directory `create` throughput (16 clients, 1 dir) | within 20% of `FGL` | within 10% |
| K6 | `LockPool` max resident size (steady state) | < 50MB | < 20MB |
| K7 | envelope-miss latency (quota on parent) | ≤20% regression vs `FGL` | ≤10% |
| K8 | identified subset of `hadoop-hdfs` tests (see §4.8a) under FGL_IIP | 100% pass | — |
| K9 | JCStress harness | zero FORBIDDEN outcomes | — |
| K10 | correctness scenarios 1–15 | 100% pass all three modes | — |
| K11 | `getFileInfo` p999 latency regression | ≤100% (2× p999) | ≤30% |
| K12 | heap footprint delta (steady-state RSS) | ≤10% vs Phase I baseline | ≤5% |

**K3 and K4 are the point of Phase II.** The **floor** (≥2×) must pass to justify merging. The **target** (≥3×) is aspirational and reflects the hoped-for benefit under idealized conditions.

**Floor vs target rationale:** early self-analysis (design round 3) identified that hand-over-hand overhead at single-client latency and compat-read contention at high concurrency make the original ≥3× target optimistic. Softening the gate to a floor + target avoids pretending to hit numbers we can't measurably commit to, while still expressing the goal.

**Merge gate precision:** the **floor** is the merge gate — if any floor fails, the pilot does not merge. The **target** is tracked in post-merge dashboards but does not block merging. If floor passes but target misses by a large margin (e.g., K3 hits 2.1× when target is 3×), that is acceptable for merge but should prompt a follow-up optimization ticket, not a spec rewrite.

**Benchmark environment:** K1–K7 and K11–K12 must be measured on a dedicated machine, not shared CI runners. Measurement protocol: average of 3 runs after 2-minute warmup, same JVM flags, same workload generator, same NN heap size across modes. CI may run informational (non-gate) benchmarks on shared runners for trend tracking.

### 4.7 JMH benchmark harness

```java
@State(Scope.Benchmark)
public class IIPLockPathBenchmark {
  @Param({"GLOBAL", "FGL", "FGL_IIP"}) String mode;
  @Param({"1", "4", "16", "64"})        int concurrency;

  @Benchmark @BenchmarkMode(Mode.Throughput) @Threads(Threads.MAX)
  public void createInDisjointDirs(Blackhole bh) { ... }

  @Benchmark @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public FileStatus getFileInfoSingleThreaded() { ... }
}
```

Results archived per commit. CI fails the pilot merge if K1–K7 regress.

### 4.8 What testing does NOT cover in pilot

Rename/delete/mkdir/setPermission (legacy path), snapshots (delegated), quota (delegated), EZ (delegated), EC (delegated), block pipeline (Phase III), background services (unchanged).

### 4.8a Identified test subset for K8

K8's "identified subset" is **not** the full `hadoop-hdfs` test module. That would fail under FGL_IIP because many existing tests assert `fsn.hasWriteLock()` or assume FSLock semantics. K8 covers specifically:

- `TestFSNamesystemLock` (existing; extended with IIP cases)
- `TestFsnCreatePilotUnderFGL_IIP` (new)
- `TestFsnGetFileInfoPilotUnderFGL_IIP` (new)
- `TestINodeLockManager` (new)
- `TestLockPool` (new)
- `TestLockedIIP` (new)
- `TestINodeMapConcurrent` (new)
- `TestGetFullPathNameConcurrent` (new)
- `TestIIPBasedFSNamesystemLock` (new)
- The 15 integration scenarios (§4.5) as individual `@Test` methods
- The JMH benchmark harness (reports pass/fail against KPI thresholds)

Running the **full** `hadoop-hdfs` module under FGL_IIP is a separate, larger effort tracked as a post-pilot ticket. It will require updating dozens of tests to not assume FSLock semantics. Blocking the pilot on that is unrealistic.

### 4.9 Tri-mode parameterization scope

Tri-mode parameterization is a **per-test-class opt-in**, not a module-wide default. Only tests that exercise migrated RPC code paths use `@EnumSource(FSNamesystemLockMode.class)`. Existing tests for `rename`, `delete`, `mkdir`, `setPermission`, etc., continue to run under their default mode (`GLOBAL`) because those RPCs are not migrated.

This limits tri-mode CI cost and avoids the false signal of "test fails under FGL_IIP" for tests that don't exercise FGL_IIP code.

---

## 5. Rollout, runbook, and operational reference

### 5.1 Config reference

| Key | Default | Type | Semantics |
|---|---|---|---|
| `dfs.namenode.lockmode` | `GLOBAL` | enum | Selects `FSNLockManager` implementation. Startup-only. |
| `dfs.namenode.fgl.iip.lock.timeout.ms` | `5000` | long | Per-acquire deadline (5s). Should be ≤ client RPC timeout to avoid wasted server work on abandoned requests. |
| `dfs.namenode.fgl.iip.lock.pool.initial.capacity` | `1024` | int | `LockPool` CHM initial capacity. |
| `dfs.namenode.fgl.iip.inodemap.stripes` | `256` | int (pow2) | `INodeMap` stripe count. |
| `dfs.namenode.fgl.iip.assert.lock.order` | true (dev) / false (prod) | bool | Runtime lock-ordering assertions. |
| `dfs.namenode.fgl.iip.metrics.holdtime.threshold.ms` | `100` | long | Histogram sampling floor. |

### 5.2 Phased rollout

- **Phase 0:** Merge to trunk, default off. Ships as "experimental / pilot."
- **Phase 1:** Enable on non-production clusters (dev, QA, staging, sandbox). Run 1–2 weeks.
- **Phase 2:** Enable on a single production standby. Run 1 week.
- **Phase 3:** Failover to the FGL_IIP standby (it becomes active). Pair is mixed-mode.
- **Phase 4:** Roll former active to FGL_IIP. Pair is fully FGL_IIP. Run 2 weeks.
- **Phase 5:** Roll to fleet in waves.

**Rollback triggers at any phase:**
- `LockPathAcquireFailureCount` > 0.1% of RPC rate.
- `LockOrderAssertionFailures` > 0.
- P99 RPC latency regresses > 50% vs `FGL`.
- Any correctness alarm.

### 5.3 Runbook: enable `FGL_IIP`

```bash
# 1. Verify both NNs healthy on current mode.
hdfs haadmin -getServiceState nn1
hdfs haadmin -getServiceState nn2
hdfs dfsadmin -report

# 2. Update hdfs-site.xml on standby NN host:
#    dfs.namenode.lockmode = FGL_IIP

# 3. Restart standby NN.
systemctl restart hadoop-hdfs-namenode

# 4. Verify standby mode + tailing.
hdfs dfsadmin -fs hdfs://nn2:8020 -getLockMode  # expect: FGL_IIP
# Check LastAppliedTxId advancing with active.

# 5. Observe 1 hour:
#    - LockPathAcquireFailureCount == 0
#    - LockOrderAssertionFailures == 0
#    - MemHeapUsedM stable
#    - LockPoolSize < 10k on quiet standby

# 6. Failover.
hdfs haadmin -failover nn1 nn2

# 7. Observe active 1 hour under real load.

# 8. Roll former active.
#    Update hdfs-site.xml on nn1; restart NN.

# 9. Verify both on FGL_IIP.

# 10. Monitor 24h before declaring cluster "on FGL_IIP".
```

### 5.4 Runbook: rollback `FGL_IIP` → `FGL`

```bash
# 1. Update hdfs-site.xml on standby: dfs.namenode.lockmode = FGL
# 2. Restart standby NN.
# 3. Verify standby: getLockMode expects FGL. Wait for tailing catchup.
# 4. Failover.
# 5. Update former active; restart.
# 6. Verify both on FGL.
```

No data migration — FSImage and edit log formats are unchanged across modes.

### 5.5 Monitoring / alerting

**Metrics to scrape** (Prometheus/JMX under `Hadoop:service=NameNode,name=FGLockMetrics`):

```
fgl_iip_lock_pool_size
fgl_iip_lock_acquire_count{mode}
fgl_iip_lock_acquire_wait_nanos{mode,quantile}
fgl_iip_lock_hold_nanos{mode,quantile}
fgl_iip_lock_path_acquire_failures{reason}
fgl_iip_compat_read_hold_nanos{quantile}
fgl_iip_compat_write_hold_nanos{quantile}
fgl_iip_create_envelope_miss{clause}
fgl_iip_getfileinfo_envelope_miss{clause}
fgl_iip_getfullpathname_retries
fgl_iip_getfullpathname_fallback
fgl_iip_lock_order_assertion_failures{rule}
```

**Recommended alerts:**

| Alert | Threshold | Severity |
|---|---|---|
| `LockPathAcquireFailureCount` rate > 0.1/sec | 5min | page |
| `LockOrderAssertionFailures` > 0 | any | page |
| `CompatWriteHoldNanos` p99 > 5s | 10min | warn |
| `LockPoolSize` > 100k | 10min | warn |
| `CreateEnvelopeMissCount` rate > 10% of create rate | 30min | info |
| `GetFullPathNameFallback` rate > 1/sec | 10min | warn |

### 5.6 Troubleshooting

**`LockAcquisitionTimeoutException` in NN log.** Check `CompatWriteHoldNanos` (un-migrated write serializing?) and `LockHoldNanos` per mode. Likely NN saturation; scale handlers or remove offending workload.

**`rule 5 violation` exception.** Code bug — report with stack trace and stop running in FGL_IIP.

**Latency regression vs FGL.** Check `LockPoolSize`, `CompatReadHoldNanos`, `LockAcquireWaitNanos`. Hot-directory workload is K5 territory; no Phase II improvement expected.

**High `CreateEnvelopeMissCount{clause=PARENT_HAS_QUOTA}`.** Workload creates under quota-bearing trees. Not a bug; pilot delegates these. Post-pilot tickets address.

**Standby tailing stalls.** Likely unrelated (JournalNode / network). Check `LastAppliedTxId` gap.

**K3/K4 not realized.** Verify workload is actually multi-directory parallel via RPC handler traces.

### 5.7 Capacity planning deltas

- **`LockPool` memory:** ~200B per hot concurrent INode. Expected < 20MB typical.
- **`INodeMap` striping:** ~25KB total. Negligible.
- **`parentVersion` field:** 4B per INode. For 1B INodes: ~4GB. **Significant** — plan heap sizing accordingly.
- **CPU:** O(path_depth) lock ops per RPC. Microsecond-scale.
- **Handler threads:** may need tuning upward after observing queue depth on FGL_IIP.

### 5.8 Known operational limitations (pilot)

- Only `getFileInfo` and scoped `create` benefit.
- Un-migrated write RPCs serialize the whole NN via compat-write.
- Quota-bearing, EZ, snapshot, EC workloads bypass pilot.
- `saveNamespace` still quiesces NN (matches Phase I).

---

## 6. Process, gates, and the migration checklist

### 6.1 Mitigation → artifact mapping

| # | Mitigation | Artifact |
|---|---|---|
| 1 | Full taxonomy upfront | `IIPAcquireMode` enum; §1.4 |
| 2 | Single chokepoint | Package-private `fgl.iip`; `lockPath()` on interface |
| 3 | Rule of one | Shared helper in pilot; checklist item |
| 4 | Canonical migration checklist | `docs/fgl/rpc-migration-checklist.md` |
| 5 | Tri-mode test harness | `MiniDFSCluster.Builder.setNameNodeLockMode` |
| 6 | Post-pilot gate | §6.2 |
| 7 | Per-5-RPC audit | JIRA label + CI cron §6.5 |
| 8 | Canonical reference link | `@see` javadoc in every new class |
| 9 | RPC #10 refactor gate | §6.3 |

### 6.2 Pilot gate criteria (mitigation #6)

The pilot is **landed and locked** only when all of the following are true:

**Code completeness:**
- [ ] Prerequisites P1–P5 (§1.11) merged to trunk.
- [ ] U0 through U8 merged to trunk under their respective JIRA tickets.
- [ ] `dfs.namenode.lockmode=FGL_IIP` starts successfully.
- [ ] `hdfs-default.xml` documents all new config keys (§5.1).
- [ ] Every new class in `fgl.iip/` has `@see docs/fgl/HDFS-17385-wave4-pilot-design.md` in its javadoc.

**Testing:**
- [ ] K1–K12 (§4.6) all meet **floor** thresholds. Aspirational targets are reported but not blocking.
- [ ] Tri-mode CI green for 3 consecutive nightly runs (on the identified subset from §4.8a, not the full module).
- [ ] JCStress harness green (zero FORBIDDEN).
- [ ] The identified test subset (§4.8a) passes under `-Dlockmode=FGL_IIP`.

**Documentation:**
- [ ] This spec committed at `docs/fgl/HDFS-17385-wave4-pilot-design.md`.
- [ ] Migration checklist at `docs/fgl/rpc-migration-checklist.md`.
- [ ] Runbook extract at `docs/fgl/fgl-iip-runbook.md`.
- [ ] JIRA HDFS-17385 updated with links.

**Review:**
- [ ] code-reviewer agent run; CRITICAL/HIGH resolved.
- [ ] architect agent "would this duplicate?" audit complete.
- [ ] At least one active HDFS committer approval.

**Operational readiness:**
- [ ] Pilot enabled on ≥1 non-production cluster for ≥1 week with clean metrics.
- [ ] JMX metrics discoverable.
- [ ] Alert rules proposed to upstream docs.

**All gate checkboxes must be ticked.** Further RPC migration is blocked until the gate passes.

### 6.3 RPC #10 refactor gate (mitigation #9) — **PASSED 2026-04-15**

**Trigger:** when 10 non-pilot RPC migration tickets (label `fgl-iip-rpc-migration`) reach Resolved/Closed.

**Detection:** nightly CI JIRA query:

```bash
curl -s "https://issues.apache.org/jira/rest/api/2/search?jql=labels=fgl-iip-rpc-migration+AND+resolution+is+not+empty" \
  | jq '.total'
```

When the count reaches 10, CI opens a blocking JIRA ticket `HDFS-XXXXX: FGL_IIP RPC-10 refactor gate`. New migration tickets depend on it.

**Scope:**

1. **Duplication sweep.** Search the 10 migrated RPC call sites for repeated `try (LockedIIP)` patterns, envelope-miss → delegate patterns, error handling, anything appearing ≥3 times — extract.
2. **Abstraction review.** Re-read this spec against code. Is `IIPAcquireMode` still the right taxonomy? `INodeLockManager.acquire` still the right shape? Lock-ordering rules still sufficient?
3. **Patch reorganization.** Squash fixups into parents. Extract discovered helpers into preparatory commits. Re-file JIRA tickets if grouping shifted. Update `rpc-migration-checklist.md` with lessons.
4. **Hard freeze.** After the gate, the pattern is the pattern. Remaining ~30 RPCs are pure mechanical application.

**Exit criteria:**

- [x] No 3+ duplication remains. Phase-A helpers consolidated (`canUsePilotBase` + `canUsePilotParentWrite`); Phase-B helpers consolidated (`ancestorsAllowCreate`, `ancestorsAllowMutate`); dispatch-block boilerplate collapsed into `tryPilot` + `PilotResult`.
- [x] `rpc-migration-checklist.md` updated — new §Code shape section enshrines the template pattern as mandatory for post-gate migrations.
- [x] New helpers extracted, reviewed, merged:
  - `FSNamesystem.PilotResult<R>`, `PilotOperation<R>`, `tryPilot`
  - `FSNamesystem.canUsePilotBase`, `canUsePilotParentWrite`
  - `FSNamesystem.ancestorsAllowCreate`, `ancestorsAllowMutate`
- [x] code-reviewer agent signoff — see `docs/fgl/pre-rpc-10-review.md`.
- [x] JIRA gate ticket resolved with summary — see this section.

**Actual duration:** landed in 4 commits over one session (dispatch template extract → 9-caller migration → RPC #10 as first template consumer → checklist/spec close-out), within the spec's expected 3–5 working days.

**Gate summary:**

10 RPCs migrated at the gate trigger; 17 more migrated post-gate using the frozen template. Total: **27 RPCs** across 3 pilot modes (as of 2026-04-16):

| Mode | RPCs |
|---|---|
| `PATH_READ` (11) | `getFileInfo`, `getBlockLocations`, `isFileClosed`, `getListing`, `getXAttrs`, `listXAttrs`, `getStoragePolicy`, `getPreferredBlockSize`, `getAclStatus`, `getErasureCodingPolicy`, `checkAccess` |
| `PARENT_WRITE` (3) | `startFile`, `mkdirs`, `delete` (single-file) |
| `PATH_WRITE` (15) | `setPermission`, `setOwner`, `setTimes`, `setReplication`, `setStoragePolicy`, `unsetStoragePolicy`, `setAcl`, `modifyAclEntries`, `removeAclEntries`, `removeDefaultAcl`, `removeAcl`, `setXAttr`, `removeXAttr`, `setErasureCodingPolicy`, `unsetErasureCodingPolicy` |

Remaining ~13 RPCs are snapshot, rename, truncate/append, or admin operations that require deferred modes or deeper BM/lease handling.

**Post-gate pattern (mechanical application).** New migrations must:

1. Pick a `PATH_READ`, `PARENT_WRITE`, or `PATH_WRITE` pilot mode. Deferred modes (`ANCESTOR_WRITE`, `RENAME_WRITE`) require a separate prerequisite ticket to implement the mode before migration.
2. Write a pre-resolved-IIP overload in the appropriate `FSDirXxxOp` class (skip `resolvePath`; accept the IIP from the walk).
3. Add a `canUsePilotXxx(src, ...)` Phase-A helper that delegates to `canUsePilotPathRead` / `canUsePilotPathWrite` / `canUsePilotParentWrite` and adds RPC-specific checks (overwrite flag, policy name, etc.).
4. Write an `xxxPilot(src, ..., pc)` method that: acquires `PATH_XXX` / `PARENT_WRITE`; runs Phase-B checks (including `ancestorsAllowCreate` or `ancestorsAllowMutate`); throws `PilotEnvelopeMissException` on any Phase-B miss; delegates to the pre-resolved-IIP overload. **Signature:** `throws IOException, InterruptedException`.
5. In the outer method, replace the legacy `writeLock/readLock` block preamble with `PilotResult<R> pr = tryPilot(() -> canUsePilotXxx(...), () -> xxxPilot(...), operationName, src);` and branch on `pr.handled`.
6. Add tests to `TestFSNamesystemFGLIIP` covering: happy path, missing target, ancestor-fallback, snapshot-path fallback, `/.reserved` fallback, permission enforcement, disjoint-parallel, hot-parent / hot-file concurrency.

Anything outside this pattern is a signal that the RPC needs design review, not a new template.

### 6.3a Mid-pilot review (between U3 and U4)

The pilot gate is a final check; by then, reversing an architectural decision is expensive. A lighter-weight mid-pilot review is run **after U3 (`INodeLockManager`) merges but before U4 (`IIPBasedFSNamesystemLock`) work begins**. Its purpose is to catch pattern flaws while they're still cheap to fix.

**Scope:**
- Is `INodeLockManager.acquire()` the right API for the 40+ future migrations?
- Does the hand-over-hand walk integrate cleanly with permission checking (§3.2)?
- Is `IIPAcquireMode` the right taxonomy based on U3's experience implementing it?
- Are the metrics sufficient for operational visibility?

**Output:** either a "proceed to U4" approval, or a list of deltas to apply before U4 starts.

**Expected duration:** 1–2 working days. Conducted by architect agent (or human architect equivalent) + pilot author.

**Rationale:** at U3, we've implemented lock infrastructure without yet using it. We can still revisit the shape. At U4, the dispatcher is wired; changes cascade. At U8, RPC bodies depend on the API; changes are disruptive. Review at U3 is the cheapest point to course-correct.

### 6.4 Migration checklist

See `docs/fgl/rpc-migration-checklist.md` (companion file to this spec). Every RPC migration PR must reference it.

### 6.5 Per-5-RPC audit cadence (mitigation #7)

**Trigger:** on multiples of 5 closed migration tickets (5, 15, 20, 25, ...; 10 replaced by full gate).

**Scope:** grep for new duplication; spot-check 2 of last 5 tickets against checklist; review metric growth.

**Duration:** 1 working day. Non-blocking. Findings filed as follow-up tickets.

### 6.6 Roles

| Role | Responsibility |
|---|---|
| Pilot author | Implement U0–U8; drive through pilot gate. |
| HDFS committers | Review PRs; sign off on gate checklist. |
| code-reviewer agent | Per-PR CRITICAL/HIGH findings. |
| architect agent | "Would this duplicate?" audits at gates. |
| Operators | Follow §5 runbook; report anomalies. |
| Migration authors (post-pilot) | Follow §6.4 checklist on every PR. |
| RPC #10 refactor lead | Assigned when gate triggers. |

### 6.7 Escape hatches

- **Pilot gate doesn't pass:** identify failing checkbox. KPI regression → root-cause. Correctness failure → do NOT force; re-open ticket and fix.
- **RPC #10 gate finds undentable duplication:** discuss in `#hdfs-fgl`; options are (a) accept with written justification (rare, ≥2 committer approval), (b) redesign helper, (c) split abstraction.
- **Operators hit rollback condition:** execute §5.4 immediately; file post-mortem JIRA ticket.
- **Committer bypassing checklist:** block merge; point to `rpc-migration-checklist.md`; no exceptions during pilot period.

### 6.8 Post-pilot handoff

After the pilot gate passes, further migration moves to "any committer." Pilot team's continuing responsibilities: maintain the spec, participate in RPC #10 gate, be on call for the first 10 migrations, eventually write the Phase II completion spec.

This spec does not cover anything past the pilot gate. Subsequent work gets its own specs.

---

## 7. Glossary

| Term | Definition |
|---|---|
| **FGL** | Fine-Grained Locking (umbrella initiative, HDFS-17366) |
| **Phase I** | HDFS-17384; split global FSNamesystemLock into FSLock + BMLock. Shipped 3.5.0. |
| **Phase II** | HDFS-17385; replace global FSLock with directory-tree per-INode locking. This spec's pilot is the first delivery. |
| **Phase III** | HDFS-17386; replace global BMLock with DNLock + INodeFileLock. Not in this spec. |
| **FGL_IIP** | Config value selecting Phase II lock manager (`IIPBasedFSNamesystemLock`). |
| **IIP** | `INodesInPath` — HDFS's value object representing a resolved path as an array of INodes. |
| **`LockedIIP`** | Pilot's wrapper around an `INodesInPath` plus the locks held on it; `AutoCloseable`. |
| **IIP lock** | A lock acquired via `INodeLockManager` on INodes along a path. |
| **Compat namespace lock** | The `FSLock` of the composed Phase I `FineGrainedFSNamesystemLock`, reused by `IIPBasedFSNamesystemLock` to coordinate with un-migrated RPCs, checkpoint, and replay. Not a new lock. |
| **BMLock** | Phase I's BlockManager lock. Unchanged by this spec. |
| **Compat-read** | Read-locked state of the compat namespace lock; acquired as part of every IIP acquisition. |
| **Compat-write** | Write-locked state of the compat namespace lock; quiesces all IIP acquisitions. |
| **`LockPool`** | Ref-counted allocator of RRWL instances keyed by INode ID. |
| **`LockRef`** | Single-use handle owning both a pool pin and a held lock. |
| **`IIPAcquireMode`** | Closed taxonomy of lock-acquisition patterns per RPC. |
| **Hand-over-hand walk** | Path resolution and lock acquisition performed together in a single top-down pass. |
| **Pilot envelope** | The set of preconditions a `create` RPC must satisfy to use the IIP path instead of the fallback. |
| **Fallback path** | The code path that delegates to the composed `FineGrainedFSNamesystemLock` for un-migrated RPCs and envelope misses. |
| **RRWL** | `java.util.concurrent.locks.ReentrantReadWriteLock`. |
| **Canonical checklist** | `docs/fgl/rpc-migration-checklist.md` — the required checklist for every RPC migration PR. |
| **Pilot gate** | The set of criteria (§6.2) that must all be true before the pilot is considered landed. |
| **RPC #10 gate** | Mandatory refactor pass after the 10th non-pilot RPC migration (§6.3). |
