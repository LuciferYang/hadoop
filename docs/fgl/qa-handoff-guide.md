# HDFS-17385 FGL_IIP Pilot — QA Handoff Guide

This document tells QA **what is in this branch**, **how to enable it**, **how to run the tests**, and **what metrics to collect and report**. It is the single starting point for validation work on the pilot.

For deeper context see:
- `docs/fgl/HDFS-17385-wave4-pilot-design.md` — full design spec (engineer-facing)
- `docs/fgl/fgl-iip-runbook.md` — operator enable / rollback / monitoring
- `docs/fgl/real-cluster-benchmark-guide.md` — benchmark methodology and KPI definitions

---

## 1. What is in this branch

**Branch:** `HDFS-17385-fork` on remote `fork` (github.com:LuciferYang/hadoop)
**Base:** Apache trunk commit `f1567a1b558`

### 1.1 Scope — what the pilot does

The pilot introduces a third NameNode lock model — **`FGL_IIP`** (Fine-Grained, INode-In-Path) — alongside the existing two:

| Mode | Class | Status |
|---|---|---|
| `GLOBAL` | `GlobalFSNamesystemLock` | Apache trunk default |
| `FGL` | `FineGrainedFSNamesystemLock` | Phase I (HDFS-17384), shipped |
| `FGL_IIP` | `IIPBasedFSNamesystemLock` | **This pilot** (HDFS-17385) |

`FGL_IIP` composes Phase I's `FineGrainedFSNamesystemLock` and adds **per-INode read/write locks** acquired via a hand-over-hand walk down the directory tree. The intent is parallelism on disjoint subtrees.

### 1.2 RPCs migrated to the pilot path

**Only 2 of ~40+ NameNode RPCs are wired to the pilot in this branch:**

| RPC | Lock mode | Gating conditions (falls back to legacy if any fail) |
|---|---|---|
| `getFileInfo` | `PATH_READ` | Path is not a snapshot, not under `.reserved/raw`, no symlink, no `INodeReference` |
| `create` (scoped) | `PARENT_WRITE` | No `overwrite=true`, no EC policy, no storage policy specified, no erasure coding, parent directory must already exist, not a snapshot path |

All other RPCs (`mkdirs`, `delete`, `rename`, `setPermission`, `addBlock`, `complete`, `getListing`, `getBlockLocations`, …) still run on Phase I's `FineGrainedFSNamesystemLock` even when `FGL_IIP` is configured.

### 1.3 Infrastructure landed

These are the building blocks the pilot uses; QA does not need to test them directly but should know they exist:

- `LockPool` — ref-counted, fair `ReentrantReadWriteLock` allocator (`CHM.compute()`-based)
- `INodeLockManager` — hand-over-hand walk with envelope-miss detection
- `IIPAcquireMode` — 7 modes (`PATH_READ`, `PARENT_WRITE`, `PATH_WRITE`, `ANCESTOR_WRITE`, `RENAME_WRITE` (deferred), `GLOBAL_READ`, `ADMIN_META`)
- `IIPBasedFSNamesystemLock` — top-level lock manager exposing `lockPath(path, mode)`
- `LockedIIP` — `AutoCloseable` bundle of acquired locks; idempotent close in reverse order
- `INodeMap` striping — per-stripe `LightWeightGSet`
- `INode.parentVersion` — versioned `getFullPathName()` walk for thread-safe path reconstruction
- `PilotEnvelopeMissException` — signals "path is structurally outside the pilot envelope; fall back"

### 1.4 Out of scope for this branch

QA should **not** expect the following to work or be measured:

- The other ~40 RPCs migrated to FGL_IIP
- `RENAME_WRITE` mode (declared but throws if requested)
- HDFS-17487 or any sibling spec
- Real-cluster KPI numbers (the benchmark Tool exists; the runs do not)
- Trunk integration (this is fork-only work)

---

## 2. How to enable the pilot

### 2.1 Server-side configuration

Add to `hdfs-site.xml` on the **NameNode** (and Standby NN if HA):

```xml
<property>
  <name>dfs.namenode.lock.model.provider.class</name>
  <value>org.apache.hadoop.hdfs.server.namenode.fgl.iip.IIPBasedFSNamesystemLock</value>
</property>
```

Restart the NameNode. The lock manager is selected at startup; you cannot switch modes at runtime.

### 2.2 Verifying the mode is active

After restart, check the NN startup log for:
```
Using IIPBasedFSNamesystemLock as the lock model provider
```
(or equivalent — see `FSNamesystem.createLock()` log line.)

### 2.3 Rolling back

Replace the value with one of:
- `org.apache.hadoop.hdfs.server.namenode.fgl.GlobalFSNamesystemLock` (trunk default)
- `org.apache.hadoop.hdfs.server.namenode.fgl.FineGrainedFSNamesystemLock` (Phase I)

Restart the NN. No on-disk format change — rollback is safe.

See `docs/fgl/fgl-iip-runbook.md` for the full operator runbook including HA failover procedure.

---

## 3. How to test correctness

### 3.1 Unit tests (in-tree, MiniDFSCluster-based)

Run the FGL_IIP unit and integration suites against this branch:

```bash
# Unit tests for individual building blocks
mvn test -pl hadoop-hdfs-project/hadoop-hdfs \
    -Dtest='TestLockPool,TestLockRef,TestIIPAcquireMode,TestINodeMapConcurrent,TestGetFullPathNameConcurrent,TestINodeLockManager,TestLockedIIP,TestIIPBasedFSNamesystemLock'

# Integration tests (MiniDFSCluster, runs the pilot end-to-end)
mvn test -pl hadoop-hdfs-project/hadoop-hdfs \
    -Dtest='TestFSNamesystemFGLIIP,TestFSNamesystemFGLIIPStress'
```

Expected: all tests pass on this branch.

### 3.2 Existing HDFS tests under FGL_IIP

To validate that the pilot does not regress existing HDFS behavior, re-run any existing test class with the pilot lock manager forced via system property:

```bash
mvn test -pl hadoop-hdfs-project/hadoop-hdfs \
    -Dtest=TestDFSPermission \
    -Dtest.dfs.namenode.lock.model.provider.class=org.apache.hadoop.hdfs.server.namenode.fgl.iip.IIPBasedFSNamesystemLock
```

Replace `TestDFSPermission` with the test class you want to validate. The system property `test.dfs.namenode.lock.model.provider.class` is honored by `FSNamesystem.createLock()` and overrides whatever `hdfs-site.xml` says, so MiniDFSCluster-based tests will use the pilot lock manager.

**Known pre-existing failures unrelated to this pilot** (do not report as pilot regressions):
- `TestDNFencing` — pre-existing Phase I incompatibility (`writeLock(BM)` + `hasReadLock(FS)` assertion mismatch). Reproduces under plain `FGL` too.
- `TestSetTimes` (`setLockForTests`) — pre-existing Phase I limitation.
- `TestStandbyCheckpoints` (`setLockForTests`) — pre-existing Phase I limitation.

### 3.3 Test classes worth re-running under FGL_IIP

These exercise the migrated RPC paths (`getFileInfo`, `create`) most heavily:

- `TestDFSPermission` — permission checks during traversal
- `TestFileCreation` — `create` semantics
- `TestFileStatus` — `getFileInfo` semantics
- `TestINodeFile` — INode lifecycle
- `TestQuota` — envelope-miss path (quota on parent)
- `TestSnapshot*` — snapshot envelope misses must fall back
- `TestSymlinkHdfs` — symlink envelope misses must fall back

---

## 4. How to collect performance metrics

### 4.1 The benchmark tool

A standalone command-line tool runs the pilot KPIs (K1–K7) against any real HDFS cluster:

**Class:** `org.apache.hadoop.hdfs.server.namenode.fgl.iip.FGLIIPBenchmark`
**JAR:** `hadoop-hdfs-tests.jar`
**Source:** `hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/fgl/iip/FGLIIPBenchmark.java`

Build the test jar:
```bash
mvn install -DskipTests -pl hadoop-hdfs-project/hadoop-hdfs
```
The test jar will be at:
```
hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.6.0-SNAPSHOT-tests.jar
```

### 4.2 Running the benchmark

The tool obtains its `FileSystem` from the standard Hadoop client config (`core-site.xml` / `hdfs-site.xml` on the classpath of the machine running the command). Make sure `HADOOP_CONF_DIR` points at a client config for the cluster you want to benchmark.

```bash
hadoop jar hadoop-hdfs-3.6.0-SNAPSHOT-tests.jar \
    org.apache.hadoop.hdfs.server.namenode.fgl.iip.FGLIIPBenchmark \
    -basePath /user/qa/fgliip-bench \
    -numClients 16 \
    -filesPerClient 200
```

#### CLI options

| Flag | Default | Meaning |
|---|---|---|
| `-basePath` | `/tmp/fgliip-bench` | HDFS directory to use for the benchmark; will be deleted on exit |
| `-numClients` | `16` | Concurrency for parallel benchmarks (K3, K4, K5) |
| `-filesPerClient` | `200` | Files per client thread for create benchmarks |
| `-readsPerClient` | `1000` | Reads per client thread for getFileInfo benchmarks |
| `-warmupIterations` | `1000` | Single-client warmup iterations before latency measurement |
| `-latencyIterations` | `1000` | Single-client measurement iterations for K1, K2 |
| `-only` | (all) | Comma-separated KPIs to run, e.g. `K3,K4` |
| `-quota-for-k7` | `1000000` | Quota value applied to K7 parent directory |
| `-help` | | Print usage and exit |

#### Exit codes

- `0` — success
- `1` — invalid arguments
- `2` — benchmark execution failure

### 4.3 KPIs the tool measures

| KPI | Description | Floor | Target |
|---|---|---|---|
| K1 | single-client `getFileInfo` p50 / p99 latency | ≤30% / ≤50% regression vs FGL | ≤10% / ≤20% |
| K2 | single-client `create` p50 / p99 latency | ≤30% / ≤50% regression | ≤10% / ≤20% |
| K3 | parallel `create` throughput, 16 disjoint dirs | ≥2× FGL | ≥3× FGL |
| K4 | parallel `getFileInfo` throughput, 16 disjoint files | ≥2× FGL | ≥3× FGL |
| K5 | hot-directory `create` throughput, 16 clients into 1 dir | within 20% of FGL | within 10% |
| K7 | envelope-miss latency (quota on parent forces fallback) | ≤20% regression | ≤10% |

K6 (LockPool resident size) and K11–K12 (heap/RSS) are observed via JMX / process metrics, not the tool. See §4.5.

### 4.4 Per-mode comparison procedure

**Critical:** the benchmark only measures the lock model the cluster is currently running. To compare modes you must run the benchmark **three times against the same cluster, restarting the NN with a different `dfs.namenode.lock.model.provider.class` between each run.**

Recommended sequence:

```bash
# 1. Configure NN for GLOBAL, restart, run benchmark
# (edit hdfs-site.xml on NN, restart NN, wait for safemode exit)
hadoop jar hadoop-hdfs-tests.jar org.apache.hadoop.hdfs.server.namenode.fgl.iip.FGLIIPBenchmark \
    -basePath /user/qa/fgliip-bench-global -numClients 16 -filesPerClient 200 \
    2>&1 | tee bench-global.log

# 2. Restart NN with FGL, re-run
hadoop jar hadoop-hdfs-tests.jar org.apache.hadoop.hdfs.server.namenode.fgl.iip.FGLIIPBenchmark \
    -basePath /user/qa/fgliip-bench-fgl -numClients 16 -filesPerClient 200 \
    2>&1 | tee bench-fgl.log

# 3. Restart NN with FGL_IIP, re-run
hadoop jar hadoop-hdfs-tests.jar org.apache.hadoop.hdfs.server.namenode.fgl.iip.FGLIIPBenchmark \
    -basePath /user/qa/fgliip-bench-iip -numClients 16 -filesPerClient 200 \
    2>&1 | tee bench-iip.log
```

For repeatable numbers, run each mode **3–5 times** and report the **median** of each KPI.

See `docs/fgl/real-cluster-benchmark-guide.md` for the full runbook script and the cluster setup requirements (3 JournalNodes, 3 DataNodes, HA, separate client machine).

### 4.5 Other metrics to collect

Alongside the benchmark output, collect these per mode/run:

- **NN heap usage** — JMX `Memory.HeapMemoryUsage.used` (steady-state, after warmup) → K12
- **NN RSS** — `ps -o rss= -p $NN_PID` → K12 sanity check
- **GC behavior** — JVM GC log during the benchmark window
- **Edit log sync time** — JMX `NameNodeInfo.JournalTransactionInfo` and per-JN sync metrics
- **iostat / sar** on the NN and JN hosts during the run
- **`LockPool` size** — JMX bean exposed by `IIPBasedFSNamesystemLock` (if instrumented; otherwise heap dump) → K6

---

## 5. What to report

For each benchmark session, please file a report with:

1. **Cluster description** — NN HW, JN count and disk type, DN count, network, client machine spec
2. **Build identifier** — `git rev-parse HEAD` of the branch under test
3. **Configuration** — relevant `hdfs-site.xml` overrides; lock provider class per run
4. **Per-KPI table** — for each of K1, K2, K3, K4, K5, K7:
   - Median of 3–5 runs per mode (`GLOBAL`, `FGL`, `FGL_IIP`)
   - `FGL_IIP` ratio vs `FGL` (the headline number)
   - Pass/fail against the floor threshold in §4.3
5. **K6 / K12 numbers** — `LockPool` resident size and heap delta
6. **Anomalies** — any run with >2× variance from the median, GC spikes, JN sync slowdowns
7. **Test failures** — output of §3.1, §3.2 if any new failures
8. **Logs** — NN log, GC log, benchmark stdout per mode

File the report as a comment on HDFS-17385 or the equivalent internal tracking ticket.

---

## 6. Escalation

If you hit any of the following, stop and escalate to the pilot dev team before proceeding:

- **NN crash or hang** under FGL_IIP that does not reproduce under FGL
- **Data integrity issue** — files reported missing, wrong size, wrong permissions, or wrong owner
- **Pilot RPC behaving differently from legacy** — e.g. `getFileInfo` returning a different `FileStatus` for the same path under FGL_IIP vs FGL
- **K1 or K2 regression > 50% at p50** — breaks the floor and changes the rollout decision
- **K3 or K4 < 1× FGL** — pilot is slower than what it replaces, design issue

For non-blocking observations (K3 misses target but passes floor; K6 above target; etc.), document them in the report but do not block the run.

---

## 7. Quick reference

| I want to… | Do this |
|---|---|
| Enable the pilot | Set `dfs.namenode.lock.model.provider.class` to `IIPBasedFSNamesystemLock`, restart NN |
| Roll back | Set the property back to `GlobalFSNamesystemLock` or `FineGrainedFSNamesystemLock`, restart NN |
| Run the FGL_IIP unit suite | `mvn test -pl hadoop-hdfs-project/hadoop-hdfs -Dtest='TestFSNamesystemFGLIIP*'` |
| Run an existing HDFS test under FGL_IIP | Add `-Dtest.dfs.namenode.lock.model.provider.class=...IIPBasedFSNamesystemLock` |
| Run the benchmark | `hadoop jar hadoop-hdfs-tests.jar org.apache.hadoop.hdfs.server.namenode.fgl.iip.FGLIIPBenchmark -basePath /user/qa/fgliip-bench` |
| Compare modes | Run the benchmark 3–5 times per mode, report medians, restart NN between modes |
| Find KPI definitions | §4.3 of this doc, or `docs/fgl/HDFS-17385-wave4-pilot-design.md` §4.6 |
| Find the runbook | `docs/fgl/fgl-iip-runbook.md` |
