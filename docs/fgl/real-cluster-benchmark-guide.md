# Running the FGL_IIP KPI Benchmark on a Real Cluster

The `TestFGLIIPBenchmark` class bundled with the HDFS-17385 pilot is a JUnit test that builds its own `MiniDFSCluster` internally. MiniDFSCluster benchmarks are too noisy for KPI validation (variance from JIT warmup, GC, single-JournalNode edit log fsync, and cluster startup overhead overwhelms the signal). This document describes how to run the same KPIs against a **real cluster** so the pilot can be properly evaluated.

---

## Why MiniDFSCluster is not enough

Across 5 runs of the K3 parallel-create benchmark in MiniDFSCluster, the FGL_IIP / FGL ratio swung from **0.18× to 2.22×** — a 12× spread. The variance was larger than the signal we were trying to measure. Root causes:

- **Single JournalNode**: every `create` waits for edit log `fsync`. This is the actual bottleneck, downstream of namespace locking. Improving the namespace lock model doesn't help when the next stage serializes everything anyway.
- **Single DataNode**: block placement serializes.
- **JIT warmup ordering bias**: modes run sequentially in the same JVM; the mode that runs last gets the hottest JIT and looks fastest.
- **Short measurement windows** (2-5 seconds per mode) are dominated by GC and JIT noise rather than steady-state throughput.
- **Everything in one JVM**: client RPC serialization is skipped, which is unrealistic.

For definitive KPI numbers (K1 through K12 from `docs/fgl/HDFS-17385-wave4-pilot-design.md` §4.6), the pilot must be benchmarked against a real cluster.

---

## Option A: Extract a standalone Tool (~1 hour of work)

Follow the pattern of Phase I's existing `org.apache.hadoop.hdfs.server.namenode.fgl.FSNLockBenchmarkThroughput` class.

### Steps

1. **Extract the workload logic** from `TestFGLIIPBenchmark` into a new class `FGLIIPBenchmark extends Configured implements Tool` in the same package:
   ```
   hadoop-hdfs-project/hadoop-hdfs/src/test/java/
     org/apache/hadoop/hdfs/server/namenode/fgl/iip/FGLIIPBenchmark.java
   ```
   The new class takes a pre-built `FileSystem` connected to a real cluster and runs the same K3/K4/K5/K7 workloads against it. No `MiniDFSCluster` involvement.

2. **Package it into the hadoop-hdfs test jar** — the hadoop-hdfs test jar already ships with a `ToolRunner` entrypoint.

3. **Invoke against the cluster**:
   ```bash
   hadoop jar hadoop-hdfs-tests.jar \
       org.apache.hadoop.hdfs.server.namenode.fgl.iip.FGLIIPBenchmark \
       -Dfs.defaultFS=hdfs://your-cluster-nn:8020 \
       -basePath /user/you/fgliip-bench \
       -numClients 16 \
       -filesPerClient 200
   ```

4. **Run it against each lock mode separately** — because the lock mode is a server-side config, you cannot switch modes mid-run. Instead:
   - Start your cluster with `dfs.namenode.lock.model.provider.class = GlobalFSNamesystemLock`, run the benchmark, record results
   - Restart the cluster with `dfs.namenode.lock.model.provider.class = FineGrainedFSNamesystemLock`, run again
   - Restart the cluster with `dfs.namenode.lock.model.provider.class = IIPBasedFSNamesystemLock`, run again
   - Compare the three result sets

---

## Option B: Adapt existing `FSNLockBenchmarkThroughput` (fastest)

Phase I already has a working tool for cluster-side benchmarking: `org.apache.hadoop.hdfs.server.namenode.fgl.FSNLockBenchmarkThroughput`. It runs mixed `create` / `append` / `rename` / `delete` / `getFileInfo` / `getListing` / `getBlockLocations` workloads under a specified read-write ratio and reports timings.

Since it reads the cluster's `dfs.namenode.lock.model.provider.class` from the server-side `hdfs-site.xml`, you can run it against a cluster in any mode without modification:

```bash
# Run against a GLOBAL-configured cluster
hadoop jar hadoop-hdfs-tests.jar \
    org.apache.hadoop.hdfs.server.namenode.fgl.FSNLockBenchmarkThroughput \
    /benchmark 80 10000 16

# Args: basePath, readWriteRatio (80 = 80% reads), testingCount, numClients

# Restart the cluster with FineGrainedFSNamesystemLock, re-run, compare
# Restart the cluster with IIPBasedFSNamesystemLock, re-run, compare
```

The output is the wall-clock time to complete N mixed operations. Lower = faster. Compare across runs.

**Caveat**: `FSNLockBenchmarkThroughput` does a *mixed* workload (read + write), not the pilot-specific K3 / K4 / K5 / K7 breakdowns. It is less precise for isolating parallel-create vs parallel-read behavior, but it is a quick first signal.

---

## Option C: Recommended — do both

1. **Quick first pass** with `FSNLockBenchmarkThroughput` to see whether the three modes differ noticeably on your real cluster workload shape. ~30 minutes of work.
2. **If differences are promising or confusing**, extract `FGLIIPBenchmark` as a proper `Tool` (~1 hour) for precise per-KPI measurement. This gives you the K1–K12 breakdown the pilot design spec defines.

---

## Real cluster setup requirements

For meaningful KPI numbers, the real cluster needs the following characteristics. These match the realistic production environment the pilot was designed for.

### Infrastructure

- **At least 3 JournalNodes** with reasonably fast disks (SSD preferred).
  - A single-JN edit log is the dominant bottleneck for create-heavy benchmarks and masks any lock improvement.
  - A 3-JN quorum lets `logSync` proceed after 2 acks, which is what production looks like.
- **At least 3 DataNodes** so that block placement does not serialize.
- **HA enabled** (active + standby NameNode) so you can failover between lock modes without downtime for cluster users.
- **Clients running on separate machines** from the NN — so RPC + protobuf serialization overhead is included in the measurement. In-process `MiniDFSCluster` skips this entirely.

### Methodology

- **Warmup**: run the benchmark once and discard results, then run 3-5 times and take medians. JIT warmup takes minutes on a real NN under load.
- **Repeatable state**: clear the test directory between runs, ideally restart the NN between modes to flush any per-mode state (lock pool entries, etc).
- **Same client machine, same time-of-day**: rules out background noise (backup jobs, other workloads sharing the cluster).
- **Per-mode, not interleaved**: run all 3-5 iterations of one mode before moving to the next. Interleaving adds config-reload noise.

### The hardest part: comparability across restarts

Switching lock modes requires an NN restart — the lock manager is chosen at startup from `dfs.namenode.lock.model.provider.class`. Between restarts, disk cache state, network warmth, and OS-level state change. To get comparable numbers across restarts:

1. **Run each mode 3 times, report the median.** Reduces the impact of first-run cold state.
2. **Restart order matters.** Start each benchmark session with a "throwaway" warmup run in the same mode, then measure.
3. **Same client machine, same time-of-day.** Rules out background noise.
4. **Record system metrics during the run** (iostat, sar, JVM GC logs) so outliers can be traced back to external causes rather than being attributed to lock-model differences.

---

## KPIs to measure

These come from `docs/fgl/HDFS-17385-wave4-pilot-design.md` §4.6. The pilot is considered performance-validated when all **floor** thresholds pass. **Target** thresholds are aspirational.

| KPI | Metric | Floor (must pass) | Target |
|---|---|---|---|
| K1 | single-client `getFileInfo` latency regression | ≤30% p50; ≤50% p99 | ≤10% p50; ≤20% p99 |
| K2 | single-client `create` latency regression | ≤30% p50; ≤50% p99 | ≤10% p50; ≤20% p99 |
| K3 | parallel `create` throughput (16 disjoint dirs) | ≥2× `FGL` | ≥3× `FGL` |
| K4 | parallel `getFileInfo` throughput (16 disjoint files) | ≥2× `FGL` | ≥3× `FGL` |
| K5 | hot-directory `create` throughput (16 clients, 1 dir) | within 20% of `FGL` | within 10% |
| K6 | `LockPool` max resident size (steady state) | < 50 MB | < 20 MB |
| K7 | envelope-miss latency (quota on parent) | ≤20% regression | ≤10% |
| K11 | `getFileInfo` p999 latency regression | ≤100% | ≤30% |
| K12 | heap footprint delta (steady-state RSS) | ≤10% vs Phase I baseline | ≤5% |

**K3 and K4 are the headline KPIs.** If the pilot does not meet the ≥2× floor on these, the design needs revisiting — most likely the `compat-read` scalability ceiling flagged in the spec §1.13.

---

## What to do with the results

### If KPIs pass the floor

The pilot is ready for the §6.2 gate review. Check the remaining gate criteria (documentation, code review sign-offs, etc.) and proceed with the phased rollout in the runbook §5.2.

### If K3 / K4 miss the floor but K1 / K2 / K5 pass

The pilot does not regress but does not deliver the target throughput improvement either. Interpretations:

- **The compat-read scalability ceiling is real.** At ≥32 cores saturated, the `ReentrantReadWriteLock`'s shared reader-count updates become a contention point. This was flagged in the design spec §1.13 as a known limitation.
- **Edit log sync is still the bottleneck even with a 3-JN quorum.** Further namespace lock improvements cannot help; the bottleneck is downstream.
- **Fix**: post-pilot optimization ticket to replace `compat-read` with an epoch-based quiescence mechanism (RCU-like). Out of pilot scope.

Decision: ship the pilot anyway as a pattern-validation exercise, and track the compat-read optimization as follow-up work. The pilot has value as the canonical template for the remaining ~40 RPC migrations, even if it doesn't deliver headline throughput in isolation.

### If K1 / K2 fail the floor

Single-client latency is regressing more than 30% at p50 or 50% at p99. This means the pilot's per-op overhead exceeds tolerance. Most likely causes:

- **`LockPool` CHM.compute overhead** per acquire is larger than expected.
- **Hand-over-hand walk does O(path-depth) lock acquisitions** per RPC, each adding latency.
- **Fair `ReentrantReadWriteLock` overhead** is higher than unfair (the round-4 design decision chose fair for writer-progress guarantees).

Fix: profile the single-client path under production-representative load and identify the dominant overhead. Options include striping the compat lock, caching the RRWL per hot INode, or relaxing to unfair locks with a separate writer-progress mechanism.

### If the benchmark results are wildly noisy

Same story as MiniDFSCluster. Likely causes:

- JournalNode disks are too slow (spinning rust); `logSync` dominates wall-clock time and drowns the signal.
- DataNode block placement is oversubscribed; try with more DNs or larger block sizes.
- The cluster is shared with other workloads; schedule a dedicated benchmark window.

---

## Example runbook for a benchmark session

```bash
#!/bin/bash
# Benchmark session: 3 lock modes x 5 iterations each, median reported.
# Run on a client machine separate from the NN.

CLUSTER_CONF_DIR=/etc/hadoop/conf
BENCHMARK_BASE=/user/$USER/fgliip-bench
CLIENT_COUNT=16
FILES_PER_CLIENT=200

run_mode() {
  local mode=$1
  local mode_class=$2

  echo "=== Mode: $mode ($mode_class) ==="

  # 1. SSH to NN, update hdfs-site.xml, restart NN
  ssh namenode "sudo sed -i \
      's|<value>.*FSNamesystemLock</value>|<value>$mode_class</value>|' \
      $CLUSTER_CONF_DIR/hdfs-site.xml"
  ssh namenode "sudo systemctl restart hadoop-hdfs-namenode"
  sleep 30  # let NN finish safemode

  # 2. Warmup run (discarded)
  hdfs dfs -rm -r -skipTrash $BENCHMARK_BASE 2>/dev/null || true
  hadoop jar hadoop-hdfs-tests.jar \
      org.apache.hadoop.hdfs.server.namenode.fgl.iip.FGLIIPBenchmark \
      -basePath $BENCHMARK_BASE \
      -numClients $CLIENT_COUNT \
      -filesPerClient $FILES_PER_CLIENT > /dev/null 2>&1

  # 3. Measurement runs
  for i in 1 2 3 4 5; do
    hdfs dfs -rm -r -skipTrash $BENCHMARK_BASE 2>/dev/null || true
    echo "--- $mode iteration $i ---"
    hadoop jar hadoop-hdfs-tests.jar \
        org.apache.hadoop.hdfs.server.namenode.fgl.iip.FGLIIPBenchmark \
        -basePath $BENCHMARK_BASE \
        -numClients $CLIENT_COUNT \
        -filesPerClient $FILES_PER_CLIENT \
        2>&1 | tee bench-$mode-iter$i.log
  done
}

run_mode GLOBAL  org.apache.hadoop.hdfs.server.namenode.fgl.GlobalFSNamesystemLock
run_mode FGL     org.apache.hadoop.hdfs.server.namenode.fgl.FineGrainedFSNamesystemLock
run_mode FGL_IIP org.apache.hadoop.hdfs.server.namenode.fgl.iip.IIPBasedFSNamesystemLock

# Parse the logs, compute medians, compare
grep "parallel create throughput" bench-*.log | \
    awk '{print $NF}' | \
    sort -n
```

Adjust for your cluster's specific service management (`systemctl`, `supervisorctl`, Ambari, Cloudera Manager, etc.) and path layout.

---

## Relationship to the §6.2 pilot gate review

The design spec §6.2 lists KPI validation (K1–K12) as part of the pilot gate checklist. Until real-cluster benchmarks confirm the KPIs, the pilot cannot pass the gate. This document describes the steps needed to close that gap.

**Order of operations:**

1. Build the cluster-side benchmark tool (Option A or B from above).
2. Run on a real HA cluster with proper JournalNode/DataNode setup.
3. Record raw numbers per KPI per mode.
4. Compute medians and compare against the floor thresholds in §4.6.
5. File the results as a comment on HDFS-17385 (or internal equivalent).
6. If floors pass: check remaining gate criteria and proceed to Phase 1 of the phased rollout (§5.2).
7. If floors fail: root-cause the shortfall (compat-read, logSync, JVM GC, etc.) and either fix or re-scope the pilot.

---

## See also

- `docs/fgl/HDFS-17385-wave4-pilot-design.md` — the full pilot design spec, including §4.6 KPI definitions, §5.2 phased rollout, and §6.2 gate criteria
- `docs/fgl/fgl-iip-runbook.md` — operator-facing enable / rollback / monitoring guide
- `org.apache.hadoop.hdfs.server.namenode.fgl.FSNLockBenchmarkThroughput` — Phase I's existing cluster-side benchmark tool, suitable for quick smoke tests
- `org.apache.hadoop.hdfs.server.namenode.fgl.iip.TestFGLIIPBenchmark` — MiniDFSCluster-based benchmark, NOT suitable for KPI validation but useful as a reference for the workload shapes
