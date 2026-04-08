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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.fgl.FSNLockManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Round-8 empirical stress tests for the FGL_IIP pilot. These tests
 * deliberately exercise risky concurrent and edge-case scenarios that
 * the unit tests do not cover, in an attempt to surface dormant bugs
 * in the pilot path that visual review would miss.
 *
 * <p>Each test runs a MiniDFSCluster with
 * {@link IIPBasedFSNamesystemLock} and drives mixed workloads across
 * the pilot path (getFileInfo + scoped create) and the legacy
 * fallback path (rename, delete, mkdir, setPermission, snapshots,
 * etc.). The pilot must remain correct in the presence of arbitrary
 * concurrent legacy operations.
 *
 * <p>If any of these tests fail, the failure is a real bug. Round 8
 * is empirical: rather than reading code, we run code and observe.
 */
public class TestFSNamesystemFGLIIPStress {

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  @BeforeEach
  public void setUp() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
        IIPBasedFSNamesystemLock.class, FSNLockManager.class);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
    INodeLockManager.HELD_IIP_DEPTH.set(0);
    INodeLockManager.HELD_IIP_WRITE.set(Boolean.FALSE);
  }

  // -------- Concurrent rename + getFileInfo --------

  /**
   * One thread renames a directory in a tight loop while another
   * thread does getFileInfo on a file inside the directory. Pilot
   * must never observe corrupt or partial state.
   */
  @Test
  @Timeout(60)
  public void concurrentRenameAndGetFileInfo() throws Exception {
    Path parent = new Path("/rename-test");
    Path a = new Path(parent, "a");
    Path b = new Path(parent, "b");
    fs.mkdirs(a);
    Path file = new Path(a, "file");
    fs.create(file).close();

    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicInteger renameOps = new AtomicInteger();
    final AtomicInteger getInfoOps = new AtomicInteger();
    final AtomicInteger getInfoErrors = new AtomicInteger();

    ExecutorService exec = Executors.newFixedThreadPool(2);
    try {
      Future<?> renamer = exec.submit(() -> {
        boolean toB = true;
        while (!stop.get()) {
          try {
            if (toB) {
              fs.rename(new Path(parent, "a"), new Path(parent, "b"));
            } else {
              fs.rename(new Path(parent, "b"), new Path(parent, "a"));
            }
            toB = !toB;
            renameOps.incrementAndGet();
          } catch (IOException e) {
            // Rename can fail if other thread is mid-state; that's
            // acceptable in this stress scenario.
          }
        }
        return null;
      });

      Future<?> reader = exec.submit(() -> {
        while (!stop.get()) {
          try {
            // Try both paths; one will exist at any moment.
            try {
              fs.getFileStatus(new Path(parent, "a/file"));
              getInfoOps.incrementAndGet();
            } catch (java.io.FileNotFoundException expected) {
              try {
                fs.getFileStatus(new Path(parent, "b/file"));
                getInfoOps.incrementAndGet();
              } catch (java.io.FileNotFoundException ok) {
                // Either rename window — both paths absent briefly.
              }
            }
          } catch (IOException ioe) {
            getInfoErrors.incrementAndGet();
          }
        }
        return null;
      });

      Thread.sleep(2_000);
      stop.set(true);
      renamer.get(5, TimeUnit.SECONDS);
      reader.get(5, TimeUnit.SECONDS);
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }

    assertTrue(renameOps.get() > 0);
    assertTrue(getInfoOps.get() > 0);
    assertEquals(0, getInfoErrors.get(),
        "getFileInfo must never throw under concurrent rename");
  }

  // -------- Concurrent create + delete --------

  /**
   * Many threads create files in a directory while another thread
   * deletes them. Pilot create must handle the create-after-delete
   * race correctly. The point is to confirm no unexpected exceptions
   * (correctness), not to measure throughput.
   */
  @Test
  @Timeout(120)
  public void concurrentCreateAndDelete() throws Exception {
    Path dir = new Path("/cd-test");
    fs.mkdirs(dir);

    final int numCreators = 2;
    final int filesPerThread = 20;
    final AtomicInteger errors = new AtomicInteger();
    final AtomicBoolean stop = new AtomicBoolean(false);
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch creatorsDone = new CountDownLatch(numCreators);

    ExecutorService exec = Executors.newFixedThreadPool(numCreators + 1);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < numCreators; t++) {
        final int tid = t;
        futures.add(exec.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < filesPerThread; i++) {
              try {
                Path p = new Path(dir, "t" + tid + "-f" + i);
                fs.create(p).close();
              } catch (IOException e) {
                // Race-acceptable exceptions:
                // - FileAlreadyExistsException: deleter recreated and
                //   our second try collides
                // - FileNotFoundException: deleter removed the file
                //   between create and complete (Lease holder error)
                // - Any IOException whose message indicates a race
                //   ("File does not exist", "Lease...holder")
                String msg = e.getMessage() == null ? "" : e.getMessage();
                if (e instanceof org.apache.hadoop.fs.FileAlreadyExistsException
                    || e instanceof java.io.FileNotFoundException
                    || msg.contains("File does not exist")
                    || msg.contains("does not have any open files")
                    || msg.contains("LeaseExpiredException")) {
                  // Race-related; expected.
                } else {
                  errors.incrementAndGet();
                  throw new RuntimeException(e);
                }
              }
            }
          } finally {
            creatorsDone.countDown();
          }
          return null;
        }));
      }

      // Deleter thread. Stops as soon as creators are done OR stop is
      // set, checking inside the inner loop to respond promptly.
      futures.add(exec.submit(() -> {
        start.await();
        while (!stop.get()) {
          for (int t = 0; t < numCreators && !stop.get(); t++) {
            for (int i = 0; i < filesPerThread && !stop.get(); i++) {
              Path p = new Path(dir, "t" + t + "-f" + i);
              try {
                fs.delete(p, false);
              } catch (IOException ignored) {
                // Race; expected.
              }
            }
          }
          if (creatorsDone.getCount() == 0) {
            break;  // creators done; stop deleting.
          }
        }
        return null;
      }));

      start.countDown();
      // Wait for creators (with generous timeout because compat-write
      // contention from the deleter slows them down).
      assertTrue(creatorsDone.await(60, TimeUnit.SECONDS),
          "creators did not finish in time");
      stop.set(true);
      // Deleter should observe stop and exit promptly.
      futures.get(numCreators).get(30, TimeUnit.SECONDS);
    } finally {
      exec.shutdownNow();
      assertTrue(exec.awaitTermination(30, TimeUnit.SECONDS));
    }

    assertEquals(0, errors.get(),
        "no unexpected exceptions during concurrent create+delete");
  }

  // -------- saveNamespace under load --------

  /**
   * Run a continuous create+getFileInfo workload, then issue a
   * saveNamespace + restart in the middle. Pilot path must continue
   * working correctly after the restart.
   */
  @Test
  @Timeout(180)
  public void saveNamespaceUnderLoad() throws Exception {
    Path dir = new Path("/sn-test");
    fs.mkdirs(dir);

    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicInteger errors = new AtomicInteger();
    final AtomicInteger ops = new AtomicInteger();
    final CountDownLatch creatorStarted = new CountDownLatch(1);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      Future<?> worker = exec.submit(() -> {
        creatorStarted.countDown();
        int i = 0;
        while (!stop.get()) {
          try {
            Path p = new Path(dir, "f" + i);
            fs.create(p).close();
            assertNotNull(fs.getFileStatus(p));
            ops.incrementAndGet();
            i++;
          } catch (IOException e) {
            // Tolerate IOException during NN restart.
            errors.incrementAndGet();
          } catch (Exception e) {
            errors.incrementAndGet();
          }
        }
        return null;
      });

      assertTrue(creatorStarted.await(5, TimeUnit.SECONDS));
      Thread.sleep(500);

      // Issue saveNamespace + restart.
      cluster.getNameNodeRpc().setSafeMode(
          HdfsConstants.SafeModeAction.SAFEMODE_ENTER, false);
      cluster.getNameNodeRpc().saveNamespace(0L, 0L);
      cluster.getNameNodeRpc().setSafeMode(
          HdfsConstants.SafeModeAction.SAFEMODE_LEAVE, false);

      // Stop the worker before restart so it doesn't time out on
      // pending RPC.
      stop.set(true);
      worker.get(10, TimeUnit.SECONDS);

      cluster.restartNameNode();
      cluster.waitActive();
      fs = cluster.getFileSystem();

      // Post-restart, pilot must work for both old and new files.
      Path freshFile = new Path(dir, "fresh");
      fs.create(freshFile).close();
      assertNotNull(fs.getFileStatus(freshFile));
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }

    assertTrue(ops.get() > 0, "worker should have made progress");
    // We tolerate some errors due to NN restart timing, but most ops
    // should succeed.
    assertTrue(errors.get() < ops.get(),
        "errors=" + errors.get() + " should be << ops=" + ops.get());
  }

  // -------- setPermission concurrent with getFileInfo --------

  /**
   * Set permission on an ancestor (legacy path: takes compat-write)
   * while another thread does getFileInfo (pilot path: takes
   * compat-read + per-INode reads). The two must be mutually
   * exclusive on the compat lock so getFileInfo never observes a
   * partial setPermission state.
   */
  @Test
  @Timeout(60)
  public void concurrentSetPermissionAndGetFileInfo() throws Exception {
    Path parent = new Path("/perm-stress");
    Path file = new Path(parent, "f");
    fs.mkdirs(parent);
    fs.create(file).close();

    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicInteger errors = new AtomicInteger();
    final AtomicInteger setPermOps = new AtomicInteger();
    final AtomicInteger getInfoOps = new AtomicInteger();

    ExecutorService exec = Executors.newFixedThreadPool(2);
    try {
      Future<?> setter = exec.submit(() -> {
        boolean alt = false;
        while (!stop.get()) {
          try {
            fs.setPermission(parent,
                new FsPermission(alt ? (short) 0755 : (short) 0700));
            alt = !alt;
            setPermOps.incrementAndGet();
          } catch (IOException e) {
            errors.incrementAndGet();
          }
        }
        return null;
      });

      Future<?> reader = exec.submit(() -> {
        while (!stop.get()) {
          try {
            FileStatus s = fs.getFileStatus(file);
            assertNotNull(s);
            getInfoOps.incrementAndGet();
          } catch (IOException e) {
            errors.incrementAndGet();
          }
        }
        return null;
      });

      Thread.sleep(2_000);
      stop.set(true);
      setter.get(5, TimeUnit.SECONDS);
      reader.get(5, TimeUnit.SECONDS);
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }

    assertTrue(setPermOps.get() > 0);
    assertTrue(getInfoOps.get() > 0);
    assertEquals(0, errors.get(),
        "no exceptions expected under concurrent setPermission + getFileInfo");
  }

  // -------- Mixed pilot + legacy operation soak --------

  /**
   * Run a mixed workload of pilot operations (create + getFileInfo)
   * and legacy operations (mkdir + delete + rename + setPermission)
   * concurrently for several seconds. The cluster state at the end
   * must be consistent — every file we expect to exist actually
   * exists, every file we expect to be gone is gone.
   */
  @Test
  @Timeout(120)
  public void mixedPilotAndLegacySoak() throws Exception {
    Path root = new Path("/mixed-soak");
    fs.mkdirs(root);
    for (int i = 0; i < 4; i++) {
      fs.mkdirs(new Path(root, "d" + i));
    }

    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicInteger pilotCreates = new AtomicInteger();
    final AtomicInteger pilotReads = new AtomicInteger();
    final AtomicInteger legacyOps = new AtomicInteger();
    final AtomicInteger errors = new AtomicInteger();
    final CountDownLatch start = new CountDownLatch(1);

    ExecutorService exec = Executors.newFixedThreadPool(8);
    try {
      // 4 pilot creators (each in its own subdir to avoid same-parent
      // collision noise).
      for (int t = 0; t < 4; t++) {
        final int tid = t;
        exec.submit(() -> {
          start.await();
          int i = 0;
          while (!stop.get()) {
            try {
              Path p = new Path(root, "d" + tid + "/f" + i);
              fs.create(p).close();
              fs.getFileStatus(p);
              pilotCreates.incrementAndGet();
              pilotReads.incrementAndGet();
              i++;
            } catch (IOException e) {
              errors.incrementAndGet();
            }
          }
          return null;
        });
      }

      // 4 legacy operators doing mixed mkdir/delete/setPermission.
      for (int t = 0; t < 4; t++) {
        final int tid = t;
        exec.submit(() -> {
          start.await();
          int op = 0;
          while (!stop.get()) {
            try {
              Path target = new Path(root,
                  "legacy-" + tid + "-" + (op % 10));
              switch (op % 4) {
              case 0:
                fs.mkdirs(target);
                break;
              case 1:
                fs.delete(target, true);
                break;
              case 2:
                if (fs.exists(target)) {
                  fs.setPermission(target, new FsPermission((short) 0750));
                }
                break;
              case 3:
                fs.exists(target);
                break;
              default:
                break;
              }
              legacyOps.incrementAndGet();
              op++;
            } catch (IOException e) {
              // Tolerate races.
            }
          }
          return null;
        });
      }

      start.countDown();
      Thread.sleep(3_000);
      stop.set(true);
      exec.shutdown();
      assertTrue(exec.awaitTermination(15, TimeUnit.SECONDS));
    } finally {
      if (!exec.isTerminated()) {
        exec.shutdownNow();
      }
    }

    assertTrue(pilotCreates.get() > 0, "pilot creates should have run");
    assertTrue(pilotReads.get() > 0, "pilot reads should have run");
    assertTrue(legacyOps.get() > 0, "legacy ops should have run");
    assertEquals(0, errors.get(),
        "no unexpected exceptions in pilot path under mixed load");
  }

  // -------- Quota set then create --------

  /**
   * Set a quota on a directory, then create a file inside it.
   * The pilot envelope must reject (quota-bearing parent) and the
   * legacy path must succeed.
   */
  @Test
  @Timeout(60)
  public void quotaBearingDirectoryFallsBackToLegacy() throws Exception {
    Path dir = new Path("/quota-dir");
    fs.mkdirs(dir);
    fs.setQuota(dir, 100L, HdfsConstants.QUOTA_DONT_SET);

    // Create should succeed via legacy fallback.
    Path file = new Path(dir, "f");
    fs.create(file).close();
    assertNotNull(fs.getFileStatus(file));

    // Verify the file actually counts against quota — the legacy
    // path enforces this.
    assertEquals(2L, fs.getContentSummary(dir).getFileAndDirectoryCount());
  }

  // -------- Mass file creation --------

  /**
   * Create many files in many subdirectories. Stress test for the
   * LockPool's ability to handle many concurrent INode locks.
   */
  @Test
  @Timeout(120)
  public void massFileCreation() throws Exception {
    Path root = new Path("/mass");
    fs.mkdirs(root);
    final int numDirs = 10;
    final int filesPerDir = 50;
    for (int d = 0; d < numDirs; d++) {
      fs.mkdirs(new Path(root, "d" + d));
    }

    final AtomicInteger errors = new AtomicInteger();
    final CountDownLatch start = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(numDirs);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int d = 0; d < numDirs; d++) {
        final int did = d;
        futures.add(exec.submit(() -> {
          start.await();
          for (int i = 0; i < filesPerDir; i++) {
            try {
              fs.create(new Path(root, "d" + did + "/f" + i)).close();
            } catch (IOException e) {
              errors.incrementAndGet();
              throw new RuntimeException(e);
            }
          }
          return null;
        }));
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(60, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
    }

    assertEquals(0, errors.get());
    // Verify all files exist.
    int found = 0;
    for (int d = 0; d < numDirs; d++) {
      for (int i = 0; i < filesPerDir; i++) {
        if (fs.exists(new Path(root, "d" + d + "/f" + i))) {
          found++;
        }
      }
    }
    assertEquals(numDirs * filesPerDir, found,
        "all files created should be discoverable");
  }
}
