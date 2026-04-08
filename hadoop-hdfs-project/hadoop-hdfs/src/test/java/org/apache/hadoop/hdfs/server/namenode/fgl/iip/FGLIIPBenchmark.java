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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Standalone command-line benchmark for the HDFS-17385 Phase II pilot.
 * Runs against a real cluster (not MiniDFSCluster) and reports the
 * pilot's KPIs (K1-K7 from {@code
 * docs/fgl/HDFS-17385-wave4-pilot-design.md} §4.6).
 *
 * <p>The benchmark does NOT change the cluster's lock model. It runs
 * against whatever {@code dfs.namenode.lock.model.provider.class} is
 * currently configured. To compare modes, restart the cluster between
 * runs with different lock model classes and run the tool again.
 *
 * <h2>Usage</h2>
 *
 * <pre>
 *   hadoop jar hadoop-hdfs-tests.jar \
 *       org.apache.hadoop.hdfs.server.namenode.fgl.iip.FGLIIPBenchmark \
 *       [options]
 * </pre>
 *
 * <p><b>Options:</b>
 * <table>
 *   <tr><td>{@code -basePath <path>}</td>
 *       <td>HDFS path under which the benchmark will create its test
 *           directories. Default: {@code /tmp/fgliip-bench}. The path
 *           is cleaned up at the end.</td></tr>
 *   <tr><td>{@code -numClients <n>}</td>
 *       <td>Number of concurrent client threads for parallel
 *           benchmarks (K3, K4, K5). Default: {@code 16}.</td></tr>
 *   <tr><td>{@code -filesPerClient <n>}</td>
 *       <td>Files created per client for create benchmarks. Default:
 *           {@code 200}.</td></tr>
 *   <tr><td>{@code -readsPerClient <n>}</td>
 *       <td>getFileStatus calls per client for read benchmark K4.
 *           Default: {@code 1000}.</td></tr>
 *   <tr><td>{@code -warmupIterations <n>}</td>
 *       <td>Warmup iterations before each latency measurement (K1/K2).
 *           Default: {@code 1000}.</td></tr>
 *   <tr><td>{@code -latencyIterations <n>}</td>
 *       <td>Iterations for single-client latency benchmarks (K1, K2,
 *           K7). Default: {@code 10000} for K1, {@code 5000} for K2/K7.</td></tr>
 *   <tr><td>{@code -only <kpi>}</td>
 *       <td>Run only the specified KPI ({@code K1}, {@code K2},
 *           {@code K3}, {@code K4}, {@code K5}, {@code K7}, or
 *           {@code all}). Default: {@code all}.</td></tr>
 *   <tr><td>{@code -quota-for-k7 <n>}</td>
 *       <td>Namespace quota value for the K7 envelope-miss test.
 *           Default: {@code 1000000}.</td></tr>
 * </table>
 *
 * <h2>Output</h2>
 *
 * <p>Results are printed to stdout in a table per KPI. Example:
 *
 * <pre>
 *   ==============================================================
 *   K3: parallel create throughput (16 clients x 200 files)
 *   ==============================================================
 *     total files : 3200
 *     elapsed ms  : 1234
 *     throughput  :           2594.8 ops/sec
 *   ==============================================================
 * </pre>
 *
 * <p>Run against each lock mode separately and compare throughput
 * values across runs.
 *
 * <h2>Exit codes</h2>
 *
 * <ul>
 *   <li>{@code 0} — all benchmarks ran successfully</li>
 *   <li>{@code 1} — argument parsing failure</li>
 *   <li>{@code 2} — cluster access / benchmark execution failure</li>
 * </ul>
 *
 * @see TestFGLIIPBenchmark
 * @see <a href="file:../../../../../../../../../../../../../docs/fgl/real-cluster-benchmark-guide.md">
 *      Real-cluster benchmark guide</a>
 */
public class FGLIIPBenchmark extends Configured implements Tool {

  private static final PrintStream OUT = System.out;

  /** All supported KPIs (run with {@code -only} to select a subset). */
  private enum Kpi { K1, K2, K3, K4, K5, K7 }

  // ---- CLI-configurable parameters ----
  private String basePathStr = "/tmp/fgliip-bench";
  private int numClients = 16;
  private int filesPerClient = 200;
  private int readsPerClient = 1000;
  private int warmupIterations = 1000;
  private int latencyIterationsK1 = 10_000;
  private int latencyIterationsK2 = 5_000;
  private int latencyIterationsK7 = 1_000;
  private long quotaForK7 = 1_000_000L;
  private List<Kpi> selectedKpis = Arrays.asList(Kpi.values());

  // ---- Runtime state ----
  private FileSystem fs;
  private Path basePath;

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new FGLIIPBenchmark(), args);
    System.exit(rc);
  }

  @Override
  public int run(String[] args) {
    try {
      parseArgs(args);
    } catch (IllegalArgumentException iae) {
      OUT.println("ERROR: " + iae.getMessage());
      printUsage();
      return 1;
    }

    try {
      fs = FileSystem.get(getConf());
      basePath = new Path(basePathStr);
      // Fresh start: clean any leftover state from previous runs.
      if (fs.exists(basePath)) {
        fs.delete(basePath, true);
      }
      fs.mkdirs(basePath);

      OUT.println();
      OUT.println("================================================================");
      OUT.println("FGL_IIP BENCHMARK (HDFS-17385 Phase II pilot)");
      OUT.println("================================================================");
      OUT.println("  cluster URI   : " + fs.getUri());
      OUT.println("  base path     : " + basePath);
      OUT.println("  clients       : " + numClients);
      OUT.println("  files/client  : " + filesPerClient);
      OUT.println("  reads/client  : " + readsPerClient);
      OUT.println("  selected KPIs : " + selectedKpis);
      OUT.println("================================================================");
      OUT.println();
      OUT.println("NOTE: this tool runs against whatever lock model the cluster is");
      OUT.println("currently configured with (dfs.namenode.lock.model.provider.class).");
      OUT.println("To compare modes, restart the NN with a different lock class and");
      OUT.println("re-run this tool.");
      OUT.println();

      for (Kpi kpi : selectedKpis) {
        switch (kpi) {
        case K1: runK1(); break;
        case K2: runK2(); break;
        case K3: runK3(); break;
        case K4: runK4(); break;
        case K5: runK5(); break;
        case K7: runK7(); break;
        default: break;
        }
      }

      return 0;
    } catch (Exception e) {
      OUT.println("ERROR during benchmark: " + e.getMessage());
      e.printStackTrace(OUT);
      return 2;
    } finally {
      // Best-effort cleanup.
      try {
        if (fs != null && basePath != null && fs.exists(basePath)) {
          fs.delete(basePath, true);
        }
      } catch (IOException ignored) {
        // Leave it for the next run to clean up.
      }
      try {
        if (fs != null) {
          fs.close();
        }
      } catch (IOException ignored) {
      }
    }
  }

  // ==================================================================
  // Argument parsing
  // ==================================================================

  private void parseArgs(String[] args) {
    int i = 0;
    while (i < args.length) {
      String a = args[i];
      switch (a) {
      case "-basePath":
        basePathStr = requireValue(args, ++i, a);
        break;
      case "-numClients":
        numClients = parseInt(requireValue(args, ++i, a), a);
        break;
      case "-filesPerClient":
        filesPerClient = parseInt(requireValue(args, ++i, a), a);
        break;
      case "-readsPerClient":
        readsPerClient = parseInt(requireValue(args, ++i, a), a);
        break;
      case "-warmupIterations":
        warmupIterations = parseInt(requireValue(args, ++i, a), a);
        break;
      case "-latencyIterations":
        int lat = parseInt(requireValue(args, ++i, a), a);
        latencyIterationsK1 = lat;
        latencyIterationsK2 = lat;
        latencyIterationsK7 = lat;
        break;
      case "-only":
        selectedKpis = parseKpiList(requireValue(args, ++i, a));
        break;
      case "-quota-for-k7":
        quotaForK7 = parseLong(requireValue(args, ++i, a), a);
        break;
      case "-help":
      case "-h":
      case "--help":
        printUsage();
        throw new IllegalArgumentException("help requested");
      default:
        throw new IllegalArgumentException("unknown argument: " + a);
      }
      i++;
    }
    if (numClients <= 0) {
      throw new IllegalArgumentException("numClients must be positive");
    }
    if (filesPerClient <= 0) {
      throw new IllegalArgumentException("filesPerClient must be positive");
    }
  }

  private static String requireValue(String[] args, int idx, String flag) {
    if (idx >= args.length) {
      throw new IllegalArgumentException("missing value for " + flag);
    }
    return args[idx];
  }

  private static int parseInt(String s, String flag) {
    try {
      return Integer.parseInt(s);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException(
          "invalid integer for " + flag + ": " + s);
    }
  }

  private static long parseLong(String s, String flag) {
    try {
      return Long.parseLong(s);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException(
          "invalid long for " + flag + ": " + s);
    }
  }

  private static List<Kpi> parseKpiList(String s) {
    if ("all".equalsIgnoreCase(s)) {
      return Arrays.asList(Kpi.values());
    }
    List<Kpi> list = new ArrayList<>();
    for (String part : s.split(",")) {
      String p = part.trim().toUpperCase();
      try {
        list.add(Kpi.valueOf(p));
      } catch (IllegalArgumentException iae) {
        throw new IllegalArgumentException("unknown KPI: " + part);
      }
    }
    return list;
  }

  private void printUsage() {
    OUT.println();
    OUT.println("Usage:");
    OUT.println("  hadoop jar hadoop-hdfs-tests.jar \\");
    OUT.println("      org.apache.hadoop.hdfs.server.namenode.fgl.iip.FGLIIPBenchmark \\");
    OUT.println("      [-basePath <hdfs-path>] \\");
    OUT.println("      [-numClients <n>] \\");
    OUT.println("      [-filesPerClient <n>] \\");
    OUT.println("      [-readsPerClient <n>] \\");
    OUT.println("      [-warmupIterations <n>] \\");
    OUT.println("      [-latencyIterations <n>] \\");
    OUT.println("      [-only K1,K2,K3,K4,K5,K7|all] \\");
    OUT.println("      [-quota-for-k7 <n>]");
    OUT.println();
    OUT.println("Defaults:");
    OUT.println("  -basePath           /tmp/fgliip-bench");
    OUT.println("  -numClients         16");
    OUT.println("  -filesPerClient     200");
    OUT.println("  -readsPerClient     1000");
    OUT.println("  -warmupIterations   1000");
    OUT.println("  -latencyIterations  10000 (K1), 5000 (K2), 1000 (K7)");
    OUT.println("  -only               all");
    OUT.println("  -quota-for-k7       1000000");
    OUT.println();
    OUT.println("This tool runs against the currently configured lock model on");
    OUT.println("the cluster. To compare modes, restart the NN with a different");
    OUT.println("dfs.namenode.lock.model.provider.class between runs.");
    OUT.println();
  }

  // ==================================================================
  // KPI runners
  // ==================================================================

  /**
   * K1 — single-client getFileInfo latency.
   * Pre-creates one file, then calls getFileStatus N times
   * single-threaded. Reports average ns/op.
   */
  private void runK1() throws IOException {
    Path dir = new Path(basePath, "k1");
    fs.mkdirs(dir);
    Path target = new Path(dir, "target");
    fs.create(target).close();

    // Warmup.
    for (int i = 0; i < warmupIterations; i++) {
      fs.getFileStatus(target);
    }

    long t0 = System.nanoTime();
    for (int i = 0; i < latencyIterationsK1; i++) {
      fs.getFileStatus(target);
    }
    long elapsed = System.nanoTime() - t0;
    long avgNs = elapsed / latencyIterationsK1;

    printLatencyTable("K1: single-client getFileInfo latency",
        latencyIterationsK1, elapsed, avgNs);
  }

  /**
   * K2 — single-client create latency.
   * Creates N files in a fresh directory single-threaded.
   */
  private void runK2() throws IOException {
    Path dir = new Path(basePath, "k2");
    fs.mkdirs(dir);

    long t0 = System.nanoTime();
    for (int i = 0; i < latencyIterationsK2; i++) {
      fs.create(new Path(dir, "f" + i)).close();
    }
    long elapsed = System.nanoTime() - t0;
    long avgNs = elapsed / latencyIterationsK2;

    printLatencyTable("K2: single-client create latency",
        latencyIterationsK2, elapsed, avgNs);
  }

  /**
   * K3 — parallel create throughput on disjoint directories.
   * numClients concurrent threads each creating filesPerClient files
   * in their own dedicated subdirectory.
   */
  private void runK3() throws Exception {
    Path dir = new Path(basePath, "k3");
    fs.mkdirs(dir);
    for (int c = 0; c < numClients; c++) {
      fs.mkdirs(new Path(dir, "c" + c));
    }

    final CountDownLatch start = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(numClients);
    try {
      List<Future<Long>> futures = new ArrayList<>();
      for (int c = 0; c < numClients; c++) {
        final int cid = c;
        futures.add(exec.submit((Callable<Long>) () -> {
          start.await();
          long tStart = System.nanoTime();
          for (int i = 0; i < filesPerClient; i++) {
            fs.create(new Path(dir, "c" + cid + "/f" + i)).close();
          }
          return System.nanoTime() - tStart;
        }));
      }
      long t0 = System.nanoTime();
      start.countDown();
      for (Future<Long> f : futures) {
        f.get(10, TimeUnit.MINUTES);
      }
      long elapsed = System.nanoTime() - t0;
      int totalFiles = numClients * filesPerClient;
      double ops = totalFiles / (elapsed / 1e9);

      printThroughputTable("K3: parallel create throughput ("
          + numClients + " clients x " + filesPerClient + " files)",
          totalFiles, elapsed, ops);
    } finally {
      exec.shutdown();
      exec.awaitTermination(60, TimeUnit.SECONDS);
    }
  }

  /**
   * K4 — parallel getFileInfo throughput on disjoint files.
   * numClients concurrent threads each calling getFileStatus
   * readsPerClient times on its own pre-created target file.
   */
  private void runK4() throws Exception {
    Path dir = new Path(basePath, "k4");
    fs.mkdirs(dir);
    for (int c = 0; c < numClients; c++) {
      fs.mkdirs(new Path(dir, "c" + c));
      fs.create(new Path(dir, "c" + c + "/file")).close();
    }

    final CountDownLatch start = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(numClients);
    try {
      List<Future<Long>> futures = new ArrayList<>();
      for (int c = 0; c < numClients; c++) {
        final Path target = new Path(dir, "c" + c + "/file");
        futures.add(exec.submit((Callable<Long>) () -> {
          start.await();
          long tStart = System.nanoTime();
          for (int i = 0; i < readsPerClient; i++) {
            fs.getFileStatus(target);
          }
          return System.nanoTime() - tStart;
        }));
      }
      long t0 = System.nanoTime();
      start.countDown();
      for (Future<Long> f : futures) {
        f.get(10, TimeUnit.MINUTES);
      }
      long elapsed = System.nanoTime() - t0;
      int totalReads = numClients * readsPerClient;
      double ops = totalReads / (elapsed / 1e9);

      printThroughputTable("K4: parallel getFileInfo throughput ("
          + numClients + " clients x " + readsPerClient + " reads)",
          totalReads, elapsed, ops);
    } finally {
      exec.shutdown();
      exec.awaitTermination(60, TimeUnit.SECONDS);
    }
  }

  /**
   * K5 — hot-directory create throughput.
   * numClients concurrent threads all creating files in a SINGLE
   * directory. Pilot MUST NOT regress here (all creates serialize
   * on the same parent write lock; no parallelism benefit is
   * expected, but also no regression beyond the ~20% floor).
   */
  private void runK5() throws Exception {
    Path dir = new Path(basePath, "k5");
    fs.mkdirs(dir);

    final CountDownLatch start = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(numClients);
    try {
      List<Future<Long>> futures = new ArrayList<>();
      for (int c = 0; c < numClients; c++) {
        final int cid = c;
        futures.add(exec.submit((Callable<Long>) () -> {
          start.await();
          long tStart = System.nanoTime();
          for (int i = 0; i < filesPerClient; i++) {
            fs.create(new Path(dir, "c" + cid + "_f" + i)).close();
          }
          return System.nanoTime() - tStart;
        }));
      }
      long t0 = System.nanoTime();
      start.countDown();
      for (Future<Long> f : futures) {
        f.get(10, TimeUnit.MINUTES);
      }
      long elapsed = System.nanoTime() - t0;
      int totalFiles = numClients * filesPerClient;
      double ops = totalFiles / (elapsed / 1e9);

      printThroughputTable("K5: hot-directory create throughput ("
          + numClients + " clients x " + filesPerClient
          + " files, one directory)",
          totalFiles, elapsed, ops);
    } finally {
      exec.shutdown();
      exec.awaitTermination(60, TimeUnit.SECONDS);
    }
  }

  /**
   * K7 — envelope-miss latency on a quota-bearing directory.
   * Sets a generous namespace quota on a directory, then creates
   * files inside it single-threaded. Under FGL_IIP, every create
   * hits the pilot envelope Phase B check (quota rejected), falls
   * through to the legacy write-lock path, and completes. Measures
   * the combined envelope-miss + fallback cost.
   */
  private void runK7() throws IOException {
    Path dir = new Path(basePath, "k7");
    fs.mkdirs(dir);
    // Generous quota so we don't actually hit the limit — only the
    // envelope-miss path.
    ((org.apache.hadoop.hdfs.DistributedFileSystem) fs).setQuota(
        dir, quotaForK7, HdfsConstants.QUOTA_DONT_SET);

    long t0 = System.nanoTime();
    for (int i = 0; i < latencyIterationsK7; i++) {
      fs.create(new Path(dir, "f" + i)).close();
    }
    long elapsed = System.nanoTime() - t0;
    long avgNs = elapsed / latencyIterationsK7;

    printLatencyTable("K7: envelope-miss create latency on quota dir",
        latencyIterationsK7, elapsed, avgNs);
  }

  // ==================================================================
  // Output helpers
  // ==================================================================

  private void printLatencyTable(String label, int iterations,
      long elapsedNs, long avgNs) {
    OUT.println();
    OUT.println("================================================================");
    OUT.println(label);
    OUT.println("================================================================");
    OUT.printf("  iterations  : %,15d%n", iterations);
    OUT.printf("  elapsed ms  : %,15d%n", elapsedNs / 1_000_000L);
    OUT.printf("  avg ns/op   : %,15d%n", avgNs);
    OUT.printf("  avg us/op   : %,15.2f%n", avgNs / 1_000.0);
    OUT.println("================================================================");
    OUT.println();
  }

  private void printThroughputTable(String label, int totalOps,
      long elapsedNs, double opsPerSec) {
    OUT.println();
    OUT.println("================================================================");
    OUT.println(label);
    OUT.println("================================================================");
    OUT.printf("  total ops   : %,15d%n", totalOps);
    OUT.printf("  elapsed ms  : %,15d%n", elapsedNs / 1_000_000L);
    OUT.printf("  throughput  : %,15.1f ops/sec%n", opsPerSec);
    OUT.println("================================================================");
    OUT.println();
  }
}
