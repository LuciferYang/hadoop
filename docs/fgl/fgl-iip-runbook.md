# FGL_IIP Operator Runbook

This runbook is the operator-facing extract of the rollout and troubleshooting sections of [HDFS-17385-wave4-pilot-design.md](HDFS-17385-wave4-pilot-design.md). Read the full spec for design rationale; use this file for day-to-day operation.

**Scope:** enabling, rolling back, monitoring, and troubleshooting the Phase II directory-tree locking pilot (`dfs.namenode.lockmode=FGL_IIP`) on an HDFS 3.6.0+ cluster.

**Pilot limitations:** only `getFileInfo` and a scoped subset of `create` benefit from FGL_IIP. All other RPCs fall through to the legacy path. Workloads dominated by rename/delete/mkdir/setPermission see no improvement — and may see small regressions from the compat lock layer. Quota-bearing trees, encryption zones, snapshots, and EC-policy trees bypass the pilot entirely.

## 0. Should you enable FGL_IIP?

**Enable FGL_IIP if all of the following are true:**
- Your workload has significant multi-directory parallel `create` or `getFileInfo` load (≥16 concurrent clients writing to disjoint directories).
- Most of your `create` traffic is outside the pilot envelope exclusions (no heavy quota use, no encryption zones, no snapshots, no erasure coding, no `overwrite=true`).
- You have staging/canary clusters where you can validate for ≥2 weeks.
- Your heap has headroom for the `parentVersion` field (~4–8 bytes × INode count).

**Do NOT enable FGL_IIP if any of the following are true:**
- Your workload is dominated by single-client sequential operations (regression up to 30% at p50, 50% at p99 is possible under the softened KPI floor).
- Your workload is dominated by rename, delete, mkdir, setPermission, or block operations — none of these benefit from the pilot.
- Your namespace is heavily partitioned by quotas or encryption zones — most creates will hit the envelope and delegate to the legacy path, giving no benefit.
- Your cluster has more than ~500M INodes and you're heap-constrained (the `parentVersion` field cost may push you over).
- You cannot do phased rollout with at least 1 week of observation per phase.

**Value proposition (honest framing):** the pilot is primarily a **pattern-validation exercise** for the full Phase II migration. It delivers a real ≥2× parallel throughput improvement on narrow workload slices, but its main value is proving the pattern works end-to-end so the remaining 40+ RPCs can follow confidently. Operators evaluating FGL_IIP purely on immediate production benefit should expect modest gains on most workloads. Operators interested in the long-term 2-3× (Phase II) or 7× (Phase II + III) benefits should treat the pilot as the first step of a multi-release journey.

---

## 1. Config keys

| Key | Default | Effect |
|---|---|---|
| `dfs.namenode.lockmode` | `GLOBAL` | Select lock implementation. Values: `GLOBAL`, `FGL`, `FGL_IIP`. Startup-only — NN restart required to change. |
| `dfs.namenode.fgl.iip.lock.timeout.ms` | `5000` | Per-acquire deadline (5s). Should be ≤ your client RPC timeout; exceeding causes `LockAcquisitionTimeoutException`. |
| `dfs.namenode.fgl.iip.lock.pool.initial.capacity` | `1024` | `LockPool` `ConcurrentHashMap` initial capacity. Tune upward if `LockPoolSize` consistently exceeds 6 figures. |
| `dfs.namenode.fgl.iip.inodemap.stripes` | `256` | `INodeMap` stripe count (power of 2). Tune upward only if stripe-lock contention visible in profiling. |
| `dfs.namenode.fgl.iip.assert.lock.order` | `false` in prod | Enables runtime lock-ordering assertions. ~20ns per acquire. Leave off in production unless investigating a bug. |
| `dfs.namenode.fgl.iip.metrics.holdtime.threshold.ms` | `100` | Floor for `LockHoldNanos` histogram. Holds below this are not recorded. |

Invalid `dfs.namenode.lockmode` value → NN fails to start with a clear error message listing valid values.

---

## 2. Phased rollout

Never enable FGL_IIP everywhere at once. Follow these phases with observation windows between:

| Phase | Target | Minimum dwell time |
|---|---|---|
| **0** | Trunk merge; default off | — |
| **1** | Non-production clusters (dev, QA, staging, sandbox) | 1–2 weeks |
| **2** | Single production standby NN | 1 week |
| **3** | Failover promotes the FGL_IIP standby to active | 1 hour active observation |
| **4** | Restart former active on FGL_IIP; pair is fully FGL_IIP | 2 weeks |
| **5** | Roll remaining production clusters in waves | 1–2 days per wave |

**Abort rollout and roll back (§4) if any of the following trip:**

- `LockPathAcquireFailureCount` > 0.1% of total RPC rate.
- `LockOrderAssertionFailures` > 0.
- P99 RPC latency regresses > 50% vs `FGL`.
- Any correctness alarm from audit log scanning or data integrity checks.

---

## 3. Enable FGL_IIP on an HA cluster

**Precondition:** cluster is running `FGL` (Phase I) or `GLOBAL`. All clients are HDFS 3.6.0+.

```bash
# 1. Announce maintenance window. No HDFS outage — rolling NN restarts only.

# 2. Verify both NNs are healthy.
hdfs haadmin -getServiceState nn1   # expect: active
hdfs haadmin -getServiceState nn2   # expect: standby
hdfs dfsadmin -report                # expect: 0 dead DNs, safe mode OFF

# 3. Update hdfs-site.xml on the STANDBY NN host.
#    Add or change:
#      <property>
#        <name>dfs.namenode.lockmode</name>
#        <value>FGL_IIP</value>
#      </property>

# 4. Restart the standby NN.
sudo systemctl restart hadoop-hdfs-namenode       # on nn2 host

# 5. Verify the standby came up on FGL_IIP.
hdfs dfsadmin -fs hdfs://nn2:8020 -getLockMode
#    Expected: "FGL_IIP (phase2, directory-tree)"
#    Alternatively: grep "FSNLockManager implementation: IIPBasedFSNamesystemLock"
#                   in the NN startup log.

# 6. Verify the standby is tailing edits from the active.
hdfs dfsadmin -fs hdfs://nn2:8020 -metasave /tmp/metasave-nn2.txt
#    Confirm last applied txid is advancing with active NN.

# 7. Observe standby metrics for at least 1 hour (JMX or your metrics stack):
#    - FGLockMetrics.LockPathAcquireFailureCount → expect 0
#    - FGLockMetrics.LockOrderAssertionFailures → expect 0
#    - JvmMetrics.MemHeapUsedM → no abnormal growth
#    - FGLockMetrics.LockPoolSize → expect steady state < 10k on quiet standby
#    If any counter is non-zero or heap grows: STOP, investigate before proceeding.

# 8. Failover to the FGL_IIP standby.
hdfs haadmin -failover nn1 nn2
#    nn2 (FGL_IIP) becomes active; nn1 (still on previous mode) becomes standby.
#    The pair is now "mixed mode" — supported, but a transient state.

# 9. Observe the new active for at least 1 hour under real load:
#    - RPC p99 latency — compare to pre-failover baseline.
#    - FGLockMetrics.CompatWriteHoldNanos — spikes mean un-migrated RPCs
#      are serializing the NN; expected for most workloads.
#    - FGLockMetrics.CreateEnvelopeMissCount per clause — tells you which
#      parts of your workload don't fit the pilot. Not a blocker; informs tuning.
#    - FGLockMetrics.LockPoolSize — should grow with concurrent load and
#      settle at ~10× concurrent client count.

# 10. If active metrics are clean: roll the former active to FGL_IIP.
#     Update hdfs-site.xml on nn1 host:
#       dfs.namenode.lockmode = FGL_IIP
sudo systemctl restart hadoop-hdfs-namenode       # on nn1 host

# 11. Verify both NNs on FGL_IIP.
hdfs dfsadmin -fs hdfs://nn1:8020 -getLockMode
hdfs dfsadmin -fs hdfs://nn2:8020 -getLockMode

# 12. Monitor for 24h before declaring the cluster "on FGL_IIP".
```

**Expected hands-on time:** ~30 minutes, plus observation windows.

**What to expect:**
- Parallel-directory workloads should see ≥3× throughput improvement at 16 concurrent clients.
- Single-client latency should regress no more than 10% at p50 / 20% at p99.
- Hot-directory workloads (all writes to the same directory) should match `FGL` — no improvement.
- `CompatWriteHoldNanos` will show occasional spikes during checkpointing; sustained high values mean un-migrated write RPCs dominate.

---

## 4. Rollback FGL_IIP → FGL (or → GLOBAL)

Rollback involves no on-disk state changes. FSImage and edit log formats are identical across all three modes.

```bash
# 1. Update hdfs-site.xml on the STANDBY NN host:
#      dfs.namenode.lockmode = FGL       (or GLOBAL for full rollback)

# 2. Restart the standby NN.
sudo systemctl restart hadoop-hdfs-namenode

# 3. Verify the standby came up on the target mode.
hdfs dfsadmin -fs hdfs://nn2:8020 -getLockMode

# 4. Wait for the standby to catch up on edits.
#    Verify LastAppliedTxId gap vs active is shrinking to zero.

# 5. Failover to the standby.
hdfs haadmin -failover nn1 nn2

# 6. Update hdfs-site.xml on former active (nn1); restart.
sudo systemctl restart hadoop-hdfs-namenode

# 7. Verify both NNs on the target mode.
```

**Rollback to GLOBAL** follows the same procedure but loses both Phase I and Phase II gains. Use only as a last resort.

---

## 5. Monitoring

### Metrics to scrape (Prometheus / JMX)

All exposed under `Hadoop:service=NameNode,name=FGLockMetrics`:

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

### Recommended alerts

| Alert | Threshold | Severity |
|---|---|---|
| `LockPathAcquireFailureCount` rate > 0.1/sec | sustained 5min | **page** |
| `LockOrderAssertionFailures` > 0 | any | **page** |
| `CompatWriteHoldNanos` p99 > 5s | sustained 10min | warn |
| `LockPoolSize` > 100k | sustained 10min | warn |
| `CreateEnvelopeMissCount` rate > 10% of create rate | sustained 30min | info |
| `GetFullPathNameFallback` rate > 1/sec | sustained 10min | warn |

### Healthy steady state

- `LockPathAcquireFailureCount` / `LockOrderAssertionFailures` / `GetFullPathNameFallback` → 0 or near 0.
- `LockPoolSize` → proportional to concurrent client load; stable.
- `LockAcquireWaitNanos{mode=PARENT_WRITE}` → p50 < 1ms on a healthy NN.
- `CompatWriteHoldNanos` → brief spikes at checkpoint; zero otherwise.

---

## 6. Troubleshooting

### `LockAcquisitionTimeoutException` in NN log

A lock waiter exceeded `dfs.namenode.fgl.iip.lock.timeout.ms`.

**Investigate:**
- `CompatWriteHoldNanos` — is it high? Some un-migrated write RPC is serializing the NN. Identify via RPC handler stack traces.
- `LockHoldNanos` histogram per mode — find the mode with outliers.
- NN saturation — handler queue depth, CPU, heap.

**Fix:** usually NN capacity, not FGL_IIP. Scale handlers (`dfs.namenode.handler.count`) or remove offending workload. Increasing the timeout is a last resort.

### `IllegalStateException: rule 5 violation` in NN log

Some code path acquired compat-write while holding an IIP lock. This is a **code bug**, not a config issue.

**Action:** capture the full stack trace, file a JIRA ticket against HDFS-17385, and stop running in FGL_IIP on that cluster until the bug is fixed. Roll back per §4.

### `getFileInfo` or `create` p99 latency regression vs FGL

**Check:**
- `LockPoolSize` — abnormally high? Memory pressure affecting lock allocation cost.
- `CompatReadHoldNanos` — are IIP acquisitions waiting on compat-read?
- `LockAcquireWaitNanos{mode}` — which mode has the tail?
- Is the workload **hot-directory**? (All creates in one directory.) That's expected to match `FGL`, not improve; the pilot offers no benefit for this pattern.

**Mitigation:** if sustained regression > 20% at p99, roll back per §4 and file a JIRA.

### `CreateEnvelopeMissCount{clause=PARENT_HAS_QUOTA}` very high

Workload is creating files inside quota-bearing directories. The pilot delegates these to the legacy path — **not a bug**, just means FGL_IIP offers no benefit for this workload.

**Action:** none. Post-pilot tickets will address quota-bearing paths. Not a rollback condition.

### Standby NN edit log tailing stalls after enabling FGL_IIP

Likely unrelated to FGL_IIP. Check:
- JournalNode reachability from the standby.
- Standby NN logs for JournalNode errors.
- `LastAppliedTxId` gap vs active; is it growing or oscillating?

If the gap is growing, it's usually JournalNode / network, not FGL_IIP. Roll back only if you can reproduce the stall by flipping the config back and forth.

### Pilot throughput (K3/K4) not realized — no improvement over FGL

- Verify the workload is actually multi-directory parallel. Use RPC handler traces to identify top paths.
- If concurrent creates target the same parent, you're in hot-directory territory — no improvement expected.
- Check `CreateEnvelopeMissCount` — if high, your workload is hitting the pilot envelope boundaries and falling back to legacy.
- Verify `LockAcquireWaitNanos{mode=PARENT_WRITE}` is low — if high, something else is causing contention.

---

## 7. Capacity planning deltas

When sizing heap and CPU for FGL_IIP-enabled NNs:

- **`LockPool` memory:** ~200 bytes per hot concurrent INode. Budget: (expected pool size) × 200B. For most clusters, < 20 MB additional heap.
- **`INodeMap` striping:** 256 RRWLs × ~100B = ~25 KB total. Negligible.
- **`parentVersion` field:** 4 bytes per INode. For 1 billion INodes, this is ~4 GB of additional heap. **Significant** — factor into heap sizing when deciding whether to enable FGL_IIP on very large namespaces.
- **CPU:** hand-over-hand lock acquisition adds O(path depth) lock ops per RPC. For typical depth 10: ~10 `ConcurrentHashMap.compute` calls + 10 RRWL acquires per RPC. Microsecond-scale; not a bottleneck.
- **Handler threads:** with finer locks, handlers spend less time blocked. Existing tuning may leave handlers under-utilized. Consider increasing `dfs.namenode.handler.count` after observing handler queue depth under FGL_IIP.
- **Lock pool initial capacity:** if `LockPoolSize` consistently hits 6-figure values, increase `dfs.namenode.fgl.iip.lock.pool.initial.capacity` to avoid rehash cost.

---

## 8. Known limitations of the pilot

- **Only `getFileInfo` and scoped `create` benefit.** All other RPCs (rename, delete, mkdir, setPermission, addBlock, etc.) fall through to the legacy path, which under FGL_IIP acquires the compat namespace lock (equivalent to the full NN lock). Workloads dominated by these RPCs see no improvement.
- **Un-migrated write RPCs serialize the whole NN.** Because they take compat-write, they exclude all IIP acquisitions. Acceptable during the pilot because they're typically < 5% of most workloads — but operators should profile their workload before enabling FGL_IIP.
- **Quota-bearing directories bypass the pilot.** Any `create` under a quota-bearing ancestor delegates to the legacy path.
- **Encryption zones bypass the pilot.** EZ clusters get no benefit from FGL_IIP until EZ support lands post-pilot.
- **Snapshot paths bypass the pilot.** Snapshot-heavy workloads get no benefit.
- **Erasure-coded paths bypass the pilot.** EC workloads get no benefit.
- **`saveNamespace` still quiesces the NN.** Checkpoint duration matches Phase I — no regression but no improvement.
- **Create `overwrite=true` bypasses the pilot.** Use regular create (without overwrite) to hit the pilot path.
- **Create with `favoredNodes` bypasses the pilot.** Standard RPCs without this option hit the pilot path.
- **Only `FGL_IIP` lock mode benefits.** `GLOBAL` and `FGL` modes are unchanged.

---

## 9. When to ask for help

File a JIRA against HDFS-17385 or post in `#hdfs-fgl` if you hit any of:

- `LockOrderAssertionFailures` > 0 (code bug).
- Sustained p99 latency regression > 20% vs `FGL` on a workload you expected to improve.
- Correctness discrepancy between `FGL` and `FGL_IIP` for the same workload.
- Any exception stack trace referencing `fgl.iip` classes that isn't in this runbook.

Include: cluster size, workload profile, metric snapshots from `FGLockMetrics`, and the output of `hdfs dfsadmin -getLockMode` from both NNs.
