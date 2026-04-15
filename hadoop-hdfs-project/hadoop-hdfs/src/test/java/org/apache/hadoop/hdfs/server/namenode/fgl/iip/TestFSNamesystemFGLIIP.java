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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.io.FileNotFoundException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.fgl.FSNLockManager;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Integration tests for U8a and U8b of the HDFS-17385 Phase II pilot.
 *
 * <p>Spins up a {@link MiniDFSCluster} configured with
 * {@link IIPBasedFSNamesystemLock} and exercises the pilot-path
 * {@code getFileInfo} and {@code create} RPCs end-to-end via the
 * {@link DistributedFileSystem} client.
 *
 * <p>Coverage:
 * <ul>
 *   <li>Cluster boots with {@code IIPBasedFSNamesystemLock} set as the
 *       lock model provider.</li>
 *   <li>Simple {@code create} + {@code getFileInfo} round-trip.</li>
 *   <li>Deeply nested paths work.</li>
 *   <li>{@code getFileInfo} on a non-existent path returns null.</li>
 *   <li>{@code create} on an existing target without overwrite fails
 *       (fallback path handles this correctly).</li>
 *   <li>{@code /.snapshot} paths fall through to legacy.</li>
 *   <li>Parallel creates across disjoint parents succeed.</li>
 * </ul>
 *
 * <p>These are the first tests that exercise the pilot through the
 * real NameNode machinery rather than in-memory unit tests.
 */
public class TestFSNamesystemFGLIIP {

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  @BeforeEach
  public void setUp() throws IOException {
    Configuration conf = new HdfsConfiguration();
    // Select the IIPBased lock manager via the class-based factory.
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
    // Defensive ThreadLocal cleanup so state can't leak between tests.
    INodeLockManager.HELD_IIP_DEPTH.set(0);
    INodeLockManager.HELD_IIP_WRITE.set(Boolean.FALSE);
  }

  @Test
  @Timeout(60)
  public void lockProviderIsIIPBased() throws Exception {
    // Smoke test: verify the cluster actually instantiated
    // IIPBasedFSNamesystemLock by reflectively reading the private
    // fsLock field of FSNamesystem.
    Object fsn = cluster.getNameNode().getNamesystem();
    java.lang.reflect.Field fsLockField =
        fsn.getClass().getDeclaredField("fsLock");
    fsLockField.setAccessible(true);
    Object lockMgr = fsLockField.get(fsn);
    assertTrue(lockMgr instanceof IIPBasedFSNamesystemLock,
        "FSNamesystem should be using IIPBasedFSNamesystemLock, "
            + "but got " + lockMgr.getClass().getName());
  }

  @Test
  @Timeout(60)
  public void createAndGetFileInfoShallow() throws Exception {
    Path p = new Path("/hello");
    fs.create(p).close();

    FileStatus status = fs.getFileStatus(p);
    assertNotNull(status);
    assertEquals("hello", status.getPath().getName());
    assertFalse(status.isDirectory());
  }

  @Test
  @Timeout(60)
  public void createAndGetFileInfoDeepPath() throws Exception {
    Path parent = new Path("/a/b/c/d");
    // Multi-level mkdir falls back to legacy because the pilot envelope
    // only handles single-level creation (parent already exists).
    assertTrue(fs.mkdirs(parent));
    Path p = new Path(parent, "deepfile");
    fs.create(p).close();

    FileStatus status = fs.getFileStatus(p);
    assertNotNull(status);
    assertEquals("/a/b/c/d/deepfile", status.getPath().toUri().getPath());
  }

  @Test
  @Timeout(60)
  public void getFileInfoOnMissingPathThrowsFileNotFound() throws Exception {
    // DistributedFileSystem.getFileStatus throws FileNotFoundException
    // if the target doesn't exist.
    Path p = new Path("/does-not-exist");
    assertThrows(FileNotFoundException.class,
        () -> fs.getFileStatus(p));
  }

  @Test
  @Timeout(60)
  public void createOnExistingTargetWithoutOverwriteFails() throws Exception {
    Path p = new Path("/existing");
    fs.create(p).close();

    // Second create without overwrite should fail. Whether this goes
    // through the pilot path (which rejects existing target in
    // envelope B and falls back) or legacy, the result is the same:
    // FileAlreadyExistsException.
    assertThrows(FileAlreadyExistsException.class,
        () -> fs.create(p, /*overwrite*/ false).close());
  }

  @Test
  @Timeout(60)
  public void snapshotPathFallsThroughToLegacy() throws Exception {
    // Create a snapshottable directory + snapshot + a file inside it,
    // then getFileInfo on the snapshot path. The pilot must reject
    // /.snapshot paths in its envelope check and delegate to legacy,
    // which should succeed.
    Path dir = new Path("/snap-parent");
    fs.mkdirs(dir);
    fs.allowSnapshot(dir);
    Path file = new Path(dir, "inside");
    fs.create(file).close();

    fs.createSnapshot(dir, "s1");
    Path snapFile = new Path(dir, ".snapshot/s1/inside");
    FileStatus status = fs.getFileStatus(snapFile);
    assertNotNull(status,
        "snapshot path should resolve via legacy fallback");
  }

  @Test
  @Timeout(60)
  public void overwriteFlagUsesLegacyPath() throws Exception {
    // Overwrite is envelope-rejected, so the pilot path is never
    // entered. This test just confirms the legacy path still works
    // for overwrite creates (regression).
    Path p = new Path("/ovw");
    fs.create(p, /*overwrite*/ true).close();
    assertNotNull(fs.getFileStatus(p));
    fs.create(p, /*overwrite*/ true).close();  // overwrite OK
    assertNotNull(fs.getFileStatus(p));
  }

  @Test
  @Timeout(60)
  public void reservedPathFallsThroughToLegacy() throws Exception {
    // Create a real file, then resolve it via /.reserved/.inodes/<id>.
    // The pilot envelope rejects /.reserved, so this exercises the
    // legacy path — both should return equivalent status.
    Path p = new Path("/reserved-test");
    fs.create(p).close();
    HdfsFileStatus first =
        fs.getClient().getFileInfo("/reserved-test");
    assertNotNull(first);
    long fileId = first.getFileId();
    Path reservedPath = new Path("/.reserved/.inodes/" + fileId);
    FileStatus byInodeId = fs.getFileStatus(reservedPath);
    assertNotNull(byInodeId);
    // The returned path will be the reserved form; the key assertion
    // is that the call succeeded (validating the legacy fallback path).
  }

  @Test
  @Timeout(120)
  public void parallelCreatesAcrossDisjointParents() throws Exception {
    // Multiple threads creating files in disjoint parent directories.
    // Pilot path should handle all of these concurrently without
    // serializing on the compat lock.
    final int numThreads = 8;
    final int filesPerThread = 20;
    final AtomicInteger failures = new AtomicInteger(0);
    final CountDownLatch start = new CountDownLatch(1);

    // Pre-create parent dirs (via legacy mkdir path).
    for (int t = 0; t < numThreads; t++) {
      assertTrue(fs.mkdirs(new Path("/parallel/p" + t)));
    }

    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
    try {
      Future<?>[] futures = new Future<?>[numThreads];
      for (int t = 0; t < numThreads; t++) {
        final int tid = t;
        futures[t] = exec.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < filesPerThread; i++) {
              Path p = new Path("/parallel/p" + tid + "/file" + i);
              fs.create(p).close();
            }
          } catch (Exception e) {
            failures.incrementAndGet();
            throw new RuntimeException(e);
          }
          return null;
        });
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(60, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertEquals(0, failures.get());

    // Verify all files exist.
    for (int t = 0; t < numThreads; t++) {
      for (int i = 0; i < filesPerThread; i++) {
        Path p = new Path("/parallel/p" + t + "/file" + i);
        assertNotNull(fs.getFileStatus(p),
            "missing file: " + p);
      }
    }
  }

  /**
   * Round-6 BUG #2 regression. FSImage save+reload replaces the root
   * INode in FSDirectory. Before the fix, IIPBasedFSNamesystemLock
   * captured the original root reference at FSNamesystem construction
   * time, so post-reload pilot RPCs walked a stale root and returned
   * "not found" for everything.
   */
  @Test
  @Timeout(180)
  public void pilotPathSurvivesFSImageReload() throws Exception {
    Path p = new Path("/before-reload");
    fs.create(p).close();
    assertNotNull(fs.getFileStatus(p),
        "precondition: file visible before reload");

    // Force a save + restart cycle that REPLACES the root INode.
    cluster.getNameNodeRpc().setSafeMode(
        HdfsConstants.SafeModeAction
            .SAFEMODE_ENTER, false);
    cluster.getNameNodeRpc().saveNamespace(0L, 0L);
    cluster.getNameNodeRpc().setSafeMode(
        HdfsConstants.SafeModeAction
            .SAFEMODE_LEAVE, false);
    cluster.restartNameNode();
    cluster.waitActive();
    fs = cluster.getFileSystem();  // re-acquire after restart

    // After reload, FSDirectory.rootDir is a fresh INodeDirectory
    // instance. The pilot path must follow the new root.
    FileStatus status = fs.getFileStatus(p);
    assertNotNull(status,
        "BUG #2: pilot path should follow the post-reload root, "
            + "not the captured-at-construction stale reference");
    assertEquals("before-reload", status.getPath().getName());

    // Also confirm a fresh create works post-reload.
    Path q = new Path("/after-reload");
    fs.create(q).close();
    assertNotNull(fs.getFileStatus(q));
  }

  /**
   * Round-6 BUG #1 regression. U8b startFilePilot was missing
   * dir.checkTraverse(pc, iip, DirOp.CREATE) which checks execute
   * permission on every ancestor. Without it, a non-owner could
   * create files in a directory whose ancestors lacked execute
   * permission for them — a permission bypass.
   */
  @Test
  @Timeout(60)
  public void pilotCreateRespectsTraversalPermission() throws Exception {
    // Setup as superuser: create /perm-test/restricted/inner.
    Path restricted = new Path("/perm-test/restricted");
    Path inner = new Path(restricted, "inner");
    fs.mkdirs(inner);
    // Strip group/other access from "restricted" so non-owners can't
    // traverse through it. Owner (test user) keeps full access.
    fs.setPermission(restricted,
        new FsPermission((short) 0700));

    // As a different user, try to create a file inside the
    // restricted subtree. The pilot path must reject via
    // checkTraverse.
    final UserGroupInformation other =
        UserGroupInformation.createRemoteUser(
            "other-user-without-traverse-perm");
    final URI clusterUri = cluster.getURI();
    final Configuration otherConf = new HdfsConfiguration();
    otherConf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
        IIPBasedFSNamesystemLock.class, FSNLockManager.class);

    other.doAs(
        (PrivilegedExceptionAction<Void>) () -> {
          DistributedFileSystem otherFs = (DistributedFileSystem)
              FileSystem.get(clusterUri, otherConf);
          try {
            Path target = new Path(inner, "newfile");
            try {
              otherFs.create(target).close();
              fail("BUG #1: pilot create should reject when an "
                  + "ancestor lacks execute permission for the "
                  + "current user");
            } catch (AccessControlException
                expected) {
              // Pilot correctly rejected via checkTraverse.
            }
          } finally {
            otherFs.close();
          }
          return null;
        });
  }

  /**
   * Round-7 BUG #3 regression. INodeReference instances appear in
   * the live namespace tree when rename moves an INode that
   * participates in a snapshot. Before the fix, the walk used
   * {@code current instanceof INodeDirectory} which is FALSE for an
   * INodeReference even when the reference wraps a directory. The
   * walk broke early at the reference, the IIP came back partial,
   * and pilot getFileInfo returned "file not found" for paths that
   * actually existed.
   *
   * <p>Fix: detect {@code isReference()} and throw
   * {@code PilotEnvelopeMissException} so the legacy path resolves
   * through the reference. Defense-in-depth: also use
   * {@code isDirectory()} + {@code asDirectory()} so future
   * non-reference INode subclasses don't trip the same trap.
   */
  @Test
  @Timeout(120)
  public void pilotPathHandlesINodeReferences() throws Exception {
    // Setup: snapshottable parent + child dir + file inside;
    // snapshot it; rename the child while the snapshot is alive.
    // After this, the renamed path is reachable through INodeReference
    // chains because the snapshot still references the original
    // location.
    Path snapDir = new Path("/snapref-parent");
    Path origChild = new Path(snapDir, "child");
    Path innerFile = new Path(origChild, "data");
    fs.mkdirs(origChild);
    fs.create(innerFile).close();
    fs.allowSnapshot(snapDir);
    fs.createSnapshot(snapDir, "s1");

    Path renamedChild = new Path(snapDir, "renamed-child");
    Path renamedFile = new Path(renamedChild, "data");
    fs.rename(origChild, renamedChild);

    // Pilot getFileInfo on the renamed path must return a valid
    // FileStatus. Before the fix, the walk broke at the reference
    // and returned null.
    FileStatus status = fs.getFileStatus(renamedFile);
    assertNotNull(status,
        "BUG #3: pilot path should resolve through INodeReference "
            + "(either correctly, or by delegating to legacy)");
    assertEquals("data", status.getPath().getName());
  }

  @Test
  @Timeout(60)
  public void parallelGetFileInfoOnSameFile() throws Exception {
    // All threads hammer the same file with getFileStatus. Pilot
    // PATH_READ should handle this with shared read locks on every
    // ancestor.
    Path p = new Path("/hot-read-target");
    fs.create(p).close();

    final int numThreads = 16;
    final int requestsPerThread = 100;
    final AtomicInteger errors = new AtomicInteger(0);
    final CountDownLatch start = new CountDownLatch(1);

    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
    try {
      Future<?>[] futures = new Future<?>[numThreads];
      for (int t = 0; t < numThreads; t++) {
        futures[t] = exec.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < requestsPerThread; i++) {
              FileStatus s = fs.getFileStatus(p);
              if (s == null) {
                errors.incrementAndGet();
              }
            }
          } catch (Exception e) {
            errors.incrementAndGet();
            throw new RuntimeException(e);
          }
          return null;
        });
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(30, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertEquals(0, errors.get());
  }

  // ======================================================================
  // HDFS-17XXXX: mkdirs pilot path (PARENT_WRITE, single-level creation).
  // ======================================================================

  /** Shallow mkdir: parent = root, target absent. Pilot path handles. */
  @Test
  @Timeout(60)
  public void mkdirsShallowUnderRoot() throws Exception {
    Path p = new Path("/mk-shallow");
    assertTrue(fs.mkdirs(p));
    FileStatus status = fs.getFileStatus(p);
    assertNotNull(status);
    assertTrue(status.isDirectory());
  }

  /**
   * Pre-existing parent, target absent — the single-level case the
   * pilot envelope is designed for.
   */
  @Test
  @Timeout(60)
  public void mkdirsSingleLevelUnderExistingParent() throws Exception {
    fs.mkdirs(new Path("/mk-parent"));  // sets up via fallback path
    Path target = new Path("/mk-parent/child");
    assertTrue(fs.mkdirs(target));
    assertTrue(fs.getFileStatus(target).isDirectory());
  }

  /**
   * mkdir on an existing directory is silent success in HDFS semantics.
   * The pilot path must honour this without mutating state.
   */
  @Test
  @Timeout(60)
  public void mkdirsExistingDirectoryIsSilentSuccess() throws Exception {
    Path p = new Path("/mk-already-exists");
    assertTrue(fs.mkdirs(p));
    // Second call must succeed silently under the pilot path.
    assertTrue(fs.mkdirs(p));
    assertTrue(fs.getFileStatus(p).isDirectory());
  }

  /** mkdir on a path that already exists as a file must fail. */
  @Test
  @Timeout(60)
  public void mkdirsOnExistingFileFails() throws Exception {
    Path p = new Path("/mk-file-collision");
    fs.create(p).close();
    assertThrows(FileAlreadyExistsException.class,
        () -> fs.mkdirs(p));
  }

  /**
   * Multi-level mkdir (missing intermediate ancestors) is an envelope
   * miss — pilot returns null, legacy creates all levels. End-to-end
   * result: success.
   */
  @Test
  @Timeout(60)
  public void mkdirsMultiLevelFallsBackToLegacy() throws Exception {
    Path deep = new Path("/mk-deep/a/b/c/d");
    assertTrue(fs.mkdirs(deep));
    assertTrue(fs.getFileStatus(deep).isDirectory());
    assertTrue(fs.getFileStatus(new Path("/mk-deep/a/b/c")).isDirectory());
    assertTrue(fs.getFileStatus(new Path("/mk-deep/a")).isDirectory());
  }

  /** /.snapshot paths are envelope-rejected; legacy handles. */
  @Test
  @Timeout(60)
  public void mkdirsSnapshotPathFallsBackToLegacy() throws Exception {
    Path parent = new Path("/mk-snap-parent");
    fs.mkdirs(parent);
    fs.allowSnapshot(parent);
    fs.createSnapshot(parent, "s1");
    // Creating a directory under /.snapshot is forbidden by HDFS;
    // legacy throws SnapshotAccessControlException. The pilot must
    // not silently succeed — it must reject the path via envelope and
    // let legacy raise the correct exception.
    Path inSnap = new Path(parent, ".snapshot/s1/newdir");
    assertThrows(IOException.class, () -> fs.mkdirs(inSnap));
  }

  /** /.reserved paths are envelope-rejected; legacy handles. */
  @Test
  @Timeout(60)
  public void mkdirsReservedPathFallsBackToLegacy() throws Exception {
    Path reserved = new Path("/.reserved/fake");
    // Legacy should reject mkdir on /.reserved; we just assert the
    // pilot didn't silently create the path.
    assertThrows(IOException.class, () -> fs.mkdirs(reserved));
  }

  /**
   * Traversal permission must be enforced. Non-owner lacking execute
   * on an ancestor cannot mkdir a child — pilot's checkTraverse
   * enforces this, mirroring U8b.
   */
  @Test
  @Timeout(60)
  public void mkdirsRespectsTraversalPermission() throws Exception {
    Path restricted = new Path("/mk-perm/restricted");
    fs.mkdirs(restricted);
    fs.setPermission(restricted,
        new FsPermission((short) 0700));

    final UserGroupInformation other =
        UserGroupInformation.createRemoteUser(
            "mk-other-user");
    final URI clusterUri = cluster.getURI();
    final Configuration otherConf = new HdfsConfiguration();
    otherConf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
        IIPBasedFSNamesystemLock.class, FSNLockManager.class);

    other.doAs(
        (PrivilegedExceptionAction<Void>) () -> {
          DistributedFileSystem otherFs = (DistributedFileSystem)
              FileSystem.get(clusterUri, otherConf);
          try {
            Path target = new Path(restricted, "child");
            assertThrows(
                AccessControlException.class,
                () -> otherFs.mkdirs(target));
          } finally {
            otherFs.close();
          }
          return null;
        });
  }

  /**
   * Concurrent mkdirs across disjoint parents should not serialise on
   * the compat lock. Under {@code PARENT_WRITE}, each call locks only
   * its own parent, so disjoint-parent mkdirs proceed in parallel.
   */
  @Test
  @Timeout(120)
  public void parallelMkdirsAcrossDisjointParents() throws Exception {
    final int numThreads = 8;
    final int dirsPerThread = 20;
    final AtomicInteger failures = new AtomicInteger(0);
    final CountDownLatch start = new CountDownLatch(1);

    for (int t = 0; t < numThreads; t++) {
      assertTrue(fs.mkdirs(new Path("/pmk/p" + t)));
    }

    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
    try {
      Future<?>[] futures = new Future<?>[numThreads];
      for (int t = 0; t < numThreads; t++) {
        final int tid = t;
        futures[t] = exec.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < dirsPerThread; i++) {
              Path p = new Path("/pmk/p" + tid + "/d" + i);
              if (!fs.mkdirs(p)) {
                failures.incrementAndGet();
              }
            }
          } catch (Exception e) {
            failures.incrementAndGet();
            throw new RuntimeException(e);
          }
          return null;
        });
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(60, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertEquals(0, failures.get());

    for (int t = 0; t < numThreads; t++) {
      for (int i = 0; i < dirsPerThread; i++) {
        Path p = new Path("/pmk/p" + t + "/d" + i);
        assertTrue(fs.getFileStatus(p).isDirectory(),
            "missing dir: " + p);
      }
    }
  }

  /**
   * Hot-parent scenario: 16 threads create distinct children under the
   * same parent. Exercises per-INode write lock contention on a shared
   * parent — PARENT_WRITE serialises at the parent, so the checklist
   * requires asserting this doesn't regress vs the legacy FGL path
   * (which serialises on a global write lock).
   */
  @Test
  @Timeout(120)
  public void parallelMkdirsUnderSameParent() throws Exception {
    final Path hot = new Path("/hot-mk-parent");
    assertTrue(fs.mkdirs(hot));

    final int numThreads = 16;
    final int dirsPerThread = 30;
    final AtomicInteger failures = new AtomicInteger(0);
    final CountDownLatch start = new CountDownLatch(1);

    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
    try {
      Future<?>[] futures = new Future<?>[numThreads];
      for (int t = 0; t < numThreads; t++) {
        final int tid = t;
        futures[t] = exec.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < dirsPerThread; i++) {
              // Distinct child names per thread — no same-target race,
              // but every call contends for the parent's write lock.
              Path p = new Path(hot, "t" + tid + "_" + i);
              if (!fs.mkdirs(p)) {
                failures.incrementAndGet();
              }
            }
          } catch (Exception e) {
            failures.incrementAndGet();
            throw new RuntimeException(e);
          }
          return null;
        });
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(90, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertEquals(0, failures.get());

    for (int t = 0; t < numThreads; t++) {
      for (int i = 0; i < dirsPerThread; i++) {
        Path p = new Path(hot, "t" + t + "_" + i);
        assertTrue(fs.getFileStatus(p).isDirectory(),
            "missing dir: " + p);
      }
    }
  }

  // ======================================================================
  // getBlockLocations pilot path (PATH_READ).
  // ======================================================================

  /** Basic pilot read of a small file. */
  @Test
  @Timeout(60)
  public void getBlockLocationsSingleBlock() throws Exception {
    Path p = new Path("/bl-small");
    byte[] data = new byte[1024];
    Arrays.fill(data, (byte) 0x7f);
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(data);
    }
    LocatedBlocks blocks = fs.getClient().getLocatedBlocks(
        p.toString(), 0L, Long.MAX_VALUE);
    assertNotNull(blocks);
    assertEquals(1, blocks.getLocatedBlocks().size());
    assertEquals(data.length, blocks.getFileLength());
  }

  /**
   * Multi-block file. The pilot must return every block with a valid
   * location, identical to the legacy path.
   */
  @Test
  @Timeout(60)
  public void getBlockLocationsMultiBlock() throws Exception {
    int blockBytes = 512;
    Path p = new Path("/bl-multi");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1,
        blockBytes)) {
      byte[] buf = new byte[blockBytes];
      for (int i = 0; i < 3; i++) {
        Arrays.fill(buf, (byte) i);
        out.write(buf);
      }
    }
    LocatedBlocks blocks = fs.getClient().getLocatedBlocks(
        p.toString(), 0L, Long.MAX_VALUE);
    assertNotNull(blocks);
    assertEquals(3, blocks.getLocatedBlocks().size());
    assertEquals(3L * blockBytes, blocks.getFileLength());
    for (LocatedBlock lb : blocks.getLocatedBlocks()) {
      assertTrue(lb.getLocations().length > 0,
          "block must have at least one location");
    }
  }

  /** getBlockLocations on a missing path throws FileNotFoundException. */
  @Test
  @Timeout(60)
  public void getBlockLocationsOnMissingFileFails() throws Exception {
    assertThrows(FileNotFoundException.class, () ->
        fs.getClient().getLocatedBlocks("/bl-does-not-exist", 0L, 1L));
  }

  /** getBlockLocations on a directory fails (legacy semantics). */
  @Test
  @Timeout(60)
  public void getBlockLocationsOnDirectoryFails() throws Exception {
    Path dir = new Path("/bl-isdir");
    fs.mkdirs(dir);
    assertThrows(IOException.class, () ->
        fs.getClient().getLocatedBlocks(dir.toString(), 0L, 1L));
  }

  /** Range within a file returns a subset of blocks. */
  @Test
  @Timeout(60)
  public void getBlockLocationsWithRange() throws Exception {
    int blockBytes = 512;
    Path p = new Path("/bl-range");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1,
        blockBytes)) {
      byte[] buf = new byte[blockBytes];
      for (int i = 0; i < 3; i++) {
        out.write(buf);
      }
    }
    LocatedBlocks blocks = fs.getClient().getLocatedBlocks(
        p.toString(), (long) blockBytes, (long) blockBytes);
    assertNotNull(blocks);
    assertTrue(blocks.getLocatedBlocks().size() >= 1);
    assertTrue(blocks.getLocatedBlocks().size() <= 2,
        "range within a single block should not return all 3");
  }

  /** /.snapshot paths are envelope-rejected; legacy handles them. */
  @Test
  @Timeout(60)
  public void getBlockLocationsSnapshotPathFallsBackToLegacy()
      throws Exception {
    Path dir = new Path("/bl-snap-parent");
    fs.mkdirs(dir);
    fs.allowSnapshot(dir);
    Path f = new Path(dir, "data");
    try (FSDataOutputStream out = fs.create(f)) {
      out.write(new byte[256]);
    }
    fs.createSnapshot(dir, "s1");

    Path snapFile = new Path(dir, ".snapshot/s1/data");
    LocatedBlocks blocks = fs.getClient().getLocatedBlocks(
        snapFile.toString(), 0L, Long.MAX_VALUE);
    assertNotNull(blocks,
        "snapshot path should resolve via legacy fallback");
    assertEquals(256L, blocks.getFileLength());
  }

  /** /.reserved/.inodes/<id> paths are envelope-rejected. */
  @Test
  @Timeout(60)
  public void getBlockLocationsReservedPathFallsBackToLegacy()
      throws Exception {
    Path p = new Path("/bl-reserved");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[128]);
    }
    long fileId = fs.getClient().getFileInfo(p.toString()).getFileId();
    String reserved = "/.reserved/.inodes/" + fileId;
    LocatedBlocks blocks = fs.getClient().getLocatedBlocks(
        reserved, 0L, Long.MAX_VALUE);
    assertNotNull(blocks);
    assertEquals(128L, blocks.getFileLength());
  }

  /** Non-owner lacking execute on ancestor cannot read block locations. */
  @Test
  @Timeout(60)
  public void getBlockLocationsRespectsTraversalPermission()
      throws Exception {
    Path restricted = new Path("/bl-perm/restricted");
    Path inner = new Path(restricted, "inner");
    fs.mkdirs(inner);
    Path f = new Path(inner, "data");
    try (FSDataOutputStream out = fs.create(f)) {
      out.write(new byte[64]);
    }
    fs.setPermission(restricted, new FsPermission((short) 0700));

    final UserGroupInformation other =
        UserGroupInformation.createRemoteUser("bl-other-user");
    final URI clusterUri = cluster.getURI();
    final Configuration otherConf = new HdfsConfiguration();
    otherConf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
        IIPBasedFSNamesystemLock.class, FSNLockManager.class);

    other.doAs(
        (PrivilegedExceptionAction<Void>) () -> {
          DistributedFileSystem otherFs = (DistributedFileSystem)
              FileSystem.get(clusterUri, otherConf);
          try {
            assertThrows(AccessControlException.class,
                () -> otherFs.getClient().getBlockLocations(
                    f.toString(), 0L, 1L));
          } finally {
            otherFs.close();
          }
          return null;
        });
  }

  /** Disjoint-files scale-out: 16 threads × 50 reads on distinct files. */
  @Test
  @Timeout(120)
  public void parallelGetBlockLocationsDisjointFiles() throws Exception {
    final int numThreads = 16;
    final int requestsPerThread = 50;
    for (int t = 0; t < numThreads; t++) {
      Path p = new Path("/bl-parallel/f" + t);
      try (FSDataOutputStream out = fs.create(p)) {
        out.write(new byte[256]);
      }
    }
    final AtomicInteger errors = new AtomicInteger(0);
    final CountDownLatch start = new CountDownLatch(1);

    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
    try {
      Future<?>[] futures = new Future<?>[numThreads];
      for (int t = 0; t < numThreads; t++) {
        final int tid = t;
        futures[t] = exec.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < requestsPerThread; i++) {
              LocatedBlocks lb = fs.getClient().getLocatedBlocks(
                  "/bl-parallel/f" + tid, 0L, Long.MAX_VALUE);
              if (lb == null || lb.getFileLength() != 256L) {
                errors.incrementAndGet();
              }
            }
          } catch (Exception e) {
            errors.incrementAndGet();
            throw new RuntimeException(e);
          }
          return null;
        });
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
  }

  /**
   * Hot-file scenario: 16 threads hammer the same file. PATH_READ
   * takes shared per-INode read locks; must not serialise or regress.
   */
  @Test
  @Timeout(120)
  public void parallelGetBlockLocationsOnSameFile() throws Exception {
    Path p = new Path("/bl-hot");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[1024]);
    }
    final int numThreads = 16;
    final int requestsPerThread = 100;
    final AtomicInteger errors = new AtomicInteger(0);
    final CountDownLatch start = new CountDownLatch(1);

    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
    try {
      Future<?>[] futures = new Future<?>[numThreads];
      for (int t = 0; t < numThreads; t++) {
        futures[t] = exec.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < requestsPerThread; i++) {
              LocatedBlocks lb = fs.getClient().getLocatedBlocks(
                  p.toString(), 0L, Long.MAX_VALUE);
              if (lb == null || lb.getFileLength() != 1024L) {
                errors.incrementAndGet();
              }
            }
          } catch (Exception e) {
            errors.incrementAndGet();
            throw new RuntimeException(e);
          }
          return null;
        });
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
  }

  // ======================================================================
  // isFileClosed pilot path (PATH_READ).
  // ======================================================================

  /** Closed file: legacy returns true; pilot must do the same. */
  @Test
  @Timeout(60)
  public void isFileClosedOnClosedFile() throws Exception {
    Path p = new Path("/ifc-closed");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[64]);
    }
    assertTrue(fs.isFileClosed(p));
  }

  /**
   * File under construction (writer still holds the lease). The pilot
   * must return false before close, and true after.
   */
  @Test
  @Timeout(60)
  public void isFileClosedWhileUnderConstruction() throws Exception {
    Path p = new Path("/ifc-open");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[32]);
      out.hflush();
      assertFalse(fs.isFileClosed(p),
          "pilot should observe under-construction state");
    }
    assertTrue(fs.isFileClosed(p));
  }

  /** Missing path throws FileNotFoundException (legacy parity). */
  @Test
  @Timeout(60)
  public void isFileClosedOnMissingFileFails() throws Exception {
    assertThrows(FileNotFoundException.class,
        () -> fs.isFileClosed(new Path("/ifc-missing")));
  }

  /** isFileClosed on a directory fails (legacy parity). */
  @Test
  @Timeout(60)
  public void isFileClosedOnDirectoryFails() throws Exception {
    Path dir = new Path("/ifc-dir");
    fs.mkdirs(dir);
    assertThrows(IOException.class, () -> fs.isFileClosed(dir));
  }

  /** /.snapshot paths are envelope-rejected; legacy handles them. */
  @Test
  @Timeout(60)
  public void isFileClosedSnapshotPathFallsBackToLegacy() throws Exception {
    Path dir = new Path("/ifc-snap-parent");
    fs.mkdirs(dir);
    fs.allowSnapshot(dir);
    Path f = new Path(dir, "data");
    try (FSDataOutputStream out = fs.create(f)) {
      out.write(new byte[16]);
    }
    fs.createSnapshot(dir, "s1");
    Path snapFile = new Path(dir, ".snapshot/s1/data");
    assertTrue(fs.isFileClosed(snapFile));
  }

  /** Non-owner lacking execute on ancestor cannot call isFileClosed. */
  @Test
  @Timeout(60)
  public void isFileClosedRespectsTraversalPermission() throws Exception {
    Path restricted = new Path("/ifc-perm/restricted");
    Path inner = new Path(restricted, "inner");
    fs.mkdirs(inner);
    Path f = new Path(inner, "data");
    try (FSDataOutputStream out = fs.create(f)) {
      out.write(new byte[8]);
    }
    fs.setPermission(restricted, new FsPermission((short) 0700));

    final UserGroupInformation other =
        UserGroupInformation.createRemoteUser("ifc-other-user");
    final URI clusterUri = cluster.getURI();
    final Configuration otherConf = new HdfsConfiguration();
    otherConf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
        IIPBasedFSNamesystemLock.class, FSNLockManager.class);

    other.doAs((PrivilegedExceptionAction<Void>) () -> {
      DistributedFileSystem otherFs = (DistributedFileSystem)
          FileSystem.get(clusterUri, otherConf);
      try {
        assertThrows(AccessControlException.class,
            () -> otherFs.isFileClosed(f));
      } finally {
        otherFs.close();
      }
      return null;
    });
  }

  /** 16 threads hammer isFileClosed on the same closed file. */
  @Test
  @Timeout(120)
  public void parallelIsFileClosedOnSameFile() throws Exception {
    Path p = new Path("/ifc-hot");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[256]);
    }
    final int numThreads = 16;
    final int requestsPerThread = 100;
    final AtomicInteger errors = new AtomicInteger(0);
    final CountDownLatch start = new CountDownLatch(1);

    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
    try {
      Future<?>[] futures = new Future<?>[numThreads];
      for (int t = 0; t < numThreads; t++) {
        futures[t] = exec.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < requestsPerThread; i++) {
              if (!fs.isFileClosed(p)) {
                errors.incrementAndGet();
              }
            }
          } catch (Exception e) {
            errors.incrementAndGet();
            throw new RuntimeException(e);
          }
          return null;
        });
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
  }
}
