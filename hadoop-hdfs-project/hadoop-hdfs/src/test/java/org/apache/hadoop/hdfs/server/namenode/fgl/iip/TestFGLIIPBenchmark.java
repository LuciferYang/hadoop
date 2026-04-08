/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.fgl.iip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.fgl.FSNLockManager;
import org.apache.hadoop.hdfs.server.namenode.fgl.FineGrainedFSNamesystemLock;
import org.apache.hadoop.hdfs.server.namenode.fgl.GlobalFSNamesystemLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Benchmarks for the HDFS-17385 Phase II pilot. Targets KPIs K1–K7
 * from the design spec §4.6, comparing three lock modes:
 * <ul>
 *   <li>{@code GLOBAL} ({@link GlobalFSNamesystemLock}) — Hadoop default</li>
 *   <li>{@code FGL} ({@link FineGrainedFSNamesystemLock}) — Phase I</li>
 *   <li>{@code FGL_IIP} ({@link IIPBasedFSNamesystemLock}) — Phase II pilot</li>
 * </ul>
 *
 * <p>This is an ad-hoc benchmark, not a regression test. It does NOT
 * fail on threshold misses — it prints comparative numbers to stdout
 * and lets the operator judge whether the pilot is delivering on its
 * design promise. Modeled after {@code FSNLockBenchmarkThroughput}
 * which Phase I uses for similar comparisons.
 *
 * <p>Each {@code @Test} method runs the same workload against all
 * three lock modes in sequence, prints per-mode timings, and (where
 * meaningful) prints derived ratios for the design-spec KPI gates.
 * Results are written to the test's stdout and surefire-reports
 * output file.
 *
 * <p>To run all benchmarks:
 * <pre>
 *   mvn test -pl hadoop-hdfs-project/hadoop-hdfs -o \
 *       -Dtest=TestFGLIIPBenchmark -DforkCount=1 \
 *       -DargLine='-XX:-UseSerialGC -Xmx2g'
 * </pre>
 *
 * <p>To run a single benchmark:
 * <pre>
 *   mvn test -pl hadoop-hdfs-project/hadoop-hdfs -o \
 *       -Dtest=TestFGLIIPBenchmark#benchmarkParallelCreateThroughput
 * </pre>
 *
 * <p><b>Caveat:</b> these benchmarks run on the developer's machine
 * inside a single MiniDFSCluster. They are useful for relative
 * comparison between lock modes but NOT for absolute throughput
 * measurement (which depends on disk speed, JVM warmup, GC, etc.).
 * Production validation requires running against a real cluster
 * with real workload.
 */
public class TestFGLIIPBenchmark {

  /**
   * Lock modes to benchmark.
   *
   * <p>To level the JIT-warmth field across modes, the helper
   * {@link #shuffledModes} returns a shuffled copy of this list per
   * call. Run order across modes is randomized so that no single mode
   * consistently gets the hottest JIT.
   */
  private static final List<LockMode> MODES = Arrays.asList(
      new LockMode("GLOBAL",  GlobalFSNamesystemLock.class.getName()),
      new LockMode("FGL",     FineGrainedFSNamesystemLock.class.getName()),
      new LockMode("FGL_IIP", IIPBasedFSNamesystemLock.class.getName()));

  /**
   * @return a freshly shuffled copy of {@link #MODES}, so the calling
   *         benchmark can iterate modes in randomized order. The seed
   *         comes from {@link System#nanoTime()} so consecutive
   *         benchmarks see different orderings.
   */
  private static List<LockMode> shuffledModes() {
    List<LockMode> copy = new ArrayList<>(MODES);
    java.util.Collections.shuffle(copy,
        new java.util.Random(System.nanoTime()));
    return copy;
  }

  /** Holds the mode name and the lock provider class. */
  private static final class LockMode {
    final String name;
    final String klass;
    LockMode(String name, String klass) {
      this.name = name;
      this.klass = klass;
    }
  }

  /**
   * One benchmark result row: per-mode latency in nanoseconds plus
   * the derived ratio vs the FGL baseline.
   */
  private static final class Result {
    final String label;
    long globalNs;
    long fglNs;
    long fglIipNs;
    Result(String label) { this.label = label; }
  }

  // -----------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------

  /**
   * Spin up a MiniDFSCluster with the given lock model and pass it
   * to the workload callback. Always shuts down cleanly.
   */
  private void runWithCluster(LockMode mode, ClusterWorkload work)
      throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY, mode.klass);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    try {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      work.run(cluster, fs);
    } finally {
      cluster.shutdown();
    }
  }

  @FunctionalInterface
  private interface ClusterWorkload {
    void run(MiniDFSCluster cluster, DistributedFileSystem fs) throws Exception;
  }

  /**
   * Print a result table comparing three modes for a single benchmark.
   */
  private void printTable(String benchmarkName, Result result) {
    System.out.println();
    System.out.println("============================================================");
    System.out.println("BENCHMARK: " + benchmarkName);
    System.out.println("============================================================");
    System.out.printf("  GLOBAL  : %,15d ns%n", result.globalNs);
    System.out.printf("  FGL     : %,15d ns%n", result.fglNs);
    System.out.printf("  FGL_IIP : %,15d ns%n", result.fglIipNs);
    if (result.fglNs > 0) {
      double ratio = (double) result.fglIipNs / result.fglNs;
      System.out.printf("  FGL_IIP / FGL ratio: %.3f%n", ratio);
    }
    System.out.println("============================================================");
  }

  /**
   * Print a throughput-result table (ops/sec instead of latency).
   */
  private void printThroughputTable(String benchmarkName, double globalOps,
      double fglOps, double fglIipOps) {
    System.out.println();
    System.out.println("============================================================");
    System.out.println("BENCHMARK: " + benchmarkName);
    System.out.println("============================================================");
    System.out.printf("  GLOBAL  : %,15.1f ops/sec%n", globalOps);
    System.out.printf("  FGL     : %,15.1f ops/sec%n", fglOps);
    System.out.printf("  FGL_IIP : %,15.1f ops/sec%n", fglIipOps);
    if (fglOps > 0) {
      double speedup = fglIipOps / fglOps;
      System.out.printf("  FGL_IIP speedup vs FGL: %.3fx%n", speedup);
    }
    System.out.println("============================================================");
  }

  // -----------------------------------------------------------------
  // K1: Single-client getFileInfo latency
  // -----------------------------------------------------------------

  /**
   * KPI K1 — single-client getFileInfo latency. Floor: ≤30% p50;
   * ≤50% p99 regression vs FGL. Target: ≤10% p50; ≤20% p99.
   *
   * <p>Pre-creates one file, then calls getFileStatus 10000 times
   * single-threaded and reports the average latency. (No percentiles
   * — that would require sorting; we report mean + min + max.)
   */
  @Test
  @Timeout(value = 600)
  public void benchmarkSingleClientGetFileInfoLatency() throws Exception {
    final int warmup = 1000;
    final int iterations = 10000;
    final Path target = new Path("/k1-target");

    Result result = new Result("K1: single-client getFileInfo latency");
    for (LockMode mode : shuffledModes()) {
      final long[] timing = new long[1];
      runWithCluster(mode, (cluster, fs) -> {
        fs.create(target).close();

        // Warmup.
        for (int i = 0; i < warmup; i++) {
          fs.getFileStatus(target);
        }

        // Measurement.
        long t0 = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
          fs.getFileStatus(target);
        }
        long elapsed = System.nanoTime() - t0;
        timing[0] = elapsed / iterations;
      });
      switch (mode.name) {
      case "GLOBAL":  result.globalNs = timing[0]; break;
      case "FGL":     result.fglNs = timing[0]; break;
      case "FGL_IIP": result.fglIipNs = timing[0]; break;
      default: break;
      }
    }
    printTable("K1: single-client getFileInfo latency (avg ns/op)", result);
  }

  // -----------------------------------------------------------------
  // K2: Single-client create latency
  // -----------------------------------------------------------------

  /**
   * KPI K2 — single-client create latency. Same thresholds as K1.
   *
   * <p>Creates 5000 files in a single directory single-threaded and
   * reports average latency.
   */
  @Test
  @Timeout(value = 600)
  public void benchmarkSingleClientCreateLatency() throws Exception {
    final int iterations = 5000;
    final Path dir = new Path("/k2-dir");

    Result result = new Result("K2: single-client create latency");
    for (LockMode mode : shuffledModes()) {
      final long[] timing = new long[1];
      runWithCluster(mode, (cluster, fs) -> {
        fs.mkdirs(dir);

        long t0 = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
          fs.create(new Path(dir, "f" + i)).close();
        }
        long elapsed = System.nanoTime() - t0;
        timing[0] = elapsed / iterations;
      });
      switch (mode.name) {
      case "GLOBAL":  result.globalNs = timing[0]; break;
      case "FGL":     result.fglNs = timing[0]; break;
      case "FGL_IIP": result.fglIipNs = timing[0]; break;
      default: break;
      }
    }
    printTable("K2: single-client create latency (avg ns/op)", result);
  }

  // -----------------------------------------------------------------
  // K3: Parallel create throughput on disjoint directories
  // -----------------------------------------------------------------

  /**
   * KPI K3 — parallel create throughput. Floor: ≥2× FGL.
   * Target: ≥3× FGL. This is the main throughput claim of the pilot.
   *
   * <p>16 client threads, each creating 200 files in its own
   * dedicated subdirectory. Reports total ops/sec.
   */
  @Test
  @Timeout(value = 1200)
  public void benchmarkParallelCreateThroughput() throws Exception {
    final int numClients = 16;
    final int filesPerClient = 200;
    final int totalFiles = numClients * filesPerClient;

    java.util.Map<String, Double> throughput = new java.util.HashMap<>();
    for (LockMode mode : shuffledModes()) {
      final double[] result = new double[1];
      runWithCluster(mode, (cluster, fs) -> {
        // Pre-create dedicated subdirectories.
        for (int i = 0; i < numClients; i++) {
          fs.mkdirs(new Path("/k3/c" + i));
        }
        final CountDownLatch start = new CountDownLatch(1);
        ExecutorService exec = Executors.newFixedThreadPool(numClients);
        try {
          List<Future<?>> futures = new ArrayList<>();
          for (int c = 0; c < numClients; c++) {
            final int cid = c;
            futures.add(exec.submit(() -> {
              start.await();
              for (int i = 0; i < filesPerClient; i++) {
                fs.create(new Path("/k3/c" + cid + "/f" + i)).close();
              }
              return null;
            }));
          }
          long t0 = System.nanoTime();
          start.countDown();
          for (Future<?> f : futures) {
            f.get(10, TimeUnit.MINUTES);
          }
          long elapsed = System.nanoTime() - t0;
          result[0] = totalFiles / (elapsed / 1e9);
        } finally {
          exec.shutdown();
          exec.awaitTermination(60, TimeUnit.SECONDS);
        }
      });
      throughput.put(mode.name, result[0]);
    }
    printThroughputTable("K3: parallel create throughput, "
        + numClients + " clients x " + filesPerClient + " files",
        throughput.get("GLOBAL"), throughput.get("FGL"),
        throughput.get("FGL_IIP"));
  }

  // -----------------------------------------------------------------
  // K4: Parallel getFileInfo throughput on disjoint files
  // -----------------------------------------------------------------

  /**
   * KPI K4 — parallel getFileInfo throughput. Floor: ≥2× FGL.
   * Target: ≥3× FGL. Mirror of K3 for the read side.
   *
   * <p>16 client threads, each calling getFileStatus 1000 times on
   * its own pre-created file in its own dedicated directory.
   */
  @Test
  @Timeout(value = 1200)
  public void benchmarkParallelGetFileInfoThroughput() throws Exception {
    final int numClients = 16;
    final int requestsPerClient = 1000;
    final int totalRequests = numClients * requestsPerClient;

    java.util.Map<String, Double> throughput = new java.util.HashMap<>();
    for (LockMode mode : shuffledModes()) {
      final double[] result = new double[1];
      runWithCluster(mode, (cluster, fs) -> {
        // Pre-create disjoint files.
        for (int c = 0; c < numClients; c++) {
          fs.mkdirs(new Path("/k4/c" + c));
          fs.create(new Path("/k4/c" + c + "/file")).close();
        }
        final CountDownLatch start = new CountDownLatch(1);
        ExecutorService exec = Executors.newFixedThreadPool(numClients);
        try {
          List<Future<?>> futures = new ArrayList<>();
          for (int c = 0; c < numClients; c++) {
            final Path target = new Path("/k4/c" + c + "/file");
            futures.add(exec.submit(() -> {
              start.await();
              for (int i = 0; i < requestsPerClient; i++) {
                fs.getFileStatus(target);
              }
              return null;
            }));
          }
          long t0 = System.nanoTime();
          start.countDown();
          for (Future<?> f : futures) {
            f.get(10, TimeUnit.MINUTES);
          }
          long elapsed = System.nanoTime() - t0;
          result[0] = totalRequests / (elapsed / 1e9);
        } finally {
          exec.shutdown();
          exec.awaitTermination(60, TimeUnit.SECONDS);
        }
      });
      throughput.put(mode.name, result[0]);
    }
    printThroughputTable("K4: parallel getFileInfo throughput, "
        + numClients + " clients x " + requestsPerClient + " reads",
        throughput.get("GLOBAL"), throughput.get("FGL"),
        throughput.get("FGL_IIP"));
  }

  // -----------------------------------------------------------------
  // K5: Hot-directory create throughput (many clients, ONE directory)
  // -----------------------------------------------------------------

  /**
   * KPI K5 — hot-directory create throughput. Floor: within 20% of FGL.
   * Target: within 10%. The pilot must NOT regress on the
   * single-hot-directory case (where it offers no benefit because
   * all creates serialize on the same parent write lock).
   *
   * <p>16 client threads all creating files in a SINGLE directory.
   */
  @Test
  @Timeout(value = 1200)
  public void benchmarkHotDirCreateThroughput() throws Exception {
    final int numClients = 16;
    final int filesPerClient = 100;
    final int totalFiles = numClients * filesPerClient;

    java.util.Map<String, Double> throughput = new java.util.HashMap<>();
    for (LockMode mode : shuffledModes()) {
      final double[] result = new double[1];
      runWithCluster(mode, (cluster, fs) -> {
        fs.mkdirs(new Path("/k5"));
        final CountDownLatch start = new CountDownLatch(1);
        ExecutorService exec = Executors.newFixedThreadPool(numClients);
        try {
          List<Future<?>> futures = new ArrayList<>();
          for (int c = 0; c < numClients; c++) {
            final int cid = c;
            futures.add(exec.submit(() -> {
              start.await();
              for (int i = 0; i < filesPerClient; i++) {
                fs.create(new Path("/k5/c" + cid + "_f" + i)).close();
              }
              return null;
            }));
          }
          long t0 = System.nanoTime();
          start.countDown();
          for (Future<?> f : futures) {
            f.get(10, TimeUnit.MINUTES);
          }
          long elapsed = System.nanoTime() - t0;
          result[0] = totalFiles / (elapsed / 1e9);
        } finally {
          exec.shutdown();
          exec.awaitTermination(60, TimeUnit.SECONDS);
        }
      });
      throughput.put(mode.name, result[0]);
    }
    printThroughputTable("K5: hot-directory create throughput, "
        + numClients + " clients x " + filesPerClient
        + " files (all in one dir)",
        throughput.get("GLOBAL"), throughput.get("FGL"),
        throughput.get("FGL_IIP"));
  }

  // -----------------------------------------------------------------
  // K7: Envelope-miss latency (quota-bearing parent)
  // -----------------------------------------------------------------

  /**
   * KPI K7 — envelope-miss latency under quota. Floor: ≤20% regression
   * vs FGL. Target: ≤10%. Validates that pilot's envelope-miss
   * fallback path doesn't add excessive overhead.
   *
   * <p>Sets a quota on a directory, then creates 1000 files inside it
   * single-threaded. Under FGL_IIP, every create hits the envelope
   * Phase B check, releases the parent write lock, and falls through
   * to the legacy code path. Compare against legacy modes that don't
   * have the extra dance.
   */
  @Test
  @Timeout(value = 600)
  public void benchmarkEnvelopeMissLatencyOnQuotaDir() throws Exception {
    final int iterations = 1000;
    final Path dir = new Path("/k7-quota");

    Result result = new Result("K7: envelope-miss latency on quota-bearing dir");
    for (LockMode mode : shuffledModes()) {
      final long[] timing = new long[1];
      runWithCluster(mode, (cluster, fs) -> {
        fs.mkdirs(dir);
        // Generous quota — large enough that creates don't actually
        // hit the quota limit, only the envelope-miss check.
        fs.setQuota(dir, 1_000_000L, HdfsConstants.QUOTA_DONT_SET);

        long t0 = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
          fs.create(new Path(dir, "f" + i)).close();
        }
        long elapsed = System.nanoTime() - t0;
        timing[0] = elapsed / iterations;
      });
      switch (mode.name) {
      case "GLOBAL":  result.globalNs = timing[0]; break;
      case "FGL":     result.fglNs = timing[0]; break;
      case "FGL_IIP": result.fglIipNs = timing[0]; break;
      default: break;
      }
    }
    printTable("K7: envelope-miss create latency on quota dir (avg ns/op)",
        result);
  }
}
