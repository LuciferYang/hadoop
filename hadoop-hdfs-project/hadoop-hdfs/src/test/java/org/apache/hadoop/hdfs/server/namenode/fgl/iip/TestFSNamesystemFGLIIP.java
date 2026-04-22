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
import org.apache.hadoop.fs.DirectoryListingStartAfterNotFoundException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
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

  // ======================================================================
  // delete pilot path (PARENT_WRITE, single regular-file only).
  // ======================================================================

  /** Happy path: delete a regular file under a simple subtree. */
  @Test
  @Timeout(60)
  public void deleteSingleFile() throws Exception {
    Path p = new Path("/del-single");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[64]);
    }
    assertTrue(fs.exists(p));
    assertTrue(fs.delete(p, /* recursive */ false));
    assertFalse(fs.exists(p));
  }

  /** Deleting under a nested parent also works. */
  @Test
  @Timeout(60)
  public void deleteSingleFileDeepPath() throws Exception {
    Path parent = new Path("/del-deep/a/b/c");
    assertTrue(fs.mkdirs(parent));
    Path p = new Path(parent, "file");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[32]);
    }
    assertTrue(fs.delete(p, false));
    assertFalse(fs.exists(p));
    // Parent tree must be intact.
    assertTrue(fs.exists(parent));
  }

  /** Deleting a missing path returns false (legacy parity). */
  @Test
  @Timeout(60)
  public void deleteMissingPathReturnsFalse() throws Exception {
    assertFalse(fs.delete(new Path("/del-missing"), false));
  }

  /**
   * Deleting an empty directory is out of pilot scope (pilot handles
   * single-file only). Must fall back to legacy which succeeds.
   */
  @Test
  @Timeout(60)
  public void deleteEmptyDirectoryFallsBackToLegacy() throws Exception {
    Path dir = new Path("/del-empty-dir");
    assertTrue(fs.mkdirs(dir));
    assertTrue(fs.delete(dir, false));
    assertFalse(fs.exists(dir));
  }

  /**
   * Deleting a non-empty directory without recursive must throw
   * PathIsNotEmptyDirectoryException (legacy parity). Pilot rejects
   * directories in Phase B and falls back.
   */
  @Test
  @Timeout(60)
  public void deleteNonEmptyDirWithoutRecursiveFails() throws Exception {
    Path dir = new Path("/del-nonempty");
    fs.mkdirs(dir);
    try (FSDataOutputStream out = fs.create(new Path(dir, "inside"))) {
      out.write(new byte[8]);
    }
    assertThrows(org.apache.hadoop.fs.PathIsNotEmptyDirectoryException.class,
        () -> fs.delete(dir, false));
    assertTrue(fs.exists(dir));
  }

  /** Recursive delete of a directory falls back; legacy handles it. */
  @Test
  @Timeout(60)
  public void deleteRecursiveDirectoryFallsBackToLegacy() throws Exception {
    Path dir = new Path("/del-recursive");
    fs.mkdirs(new Path(dir, "sub"));
    try (FSDataOutputStream out = fs.create(new Path(dir, "sub/file"))) {
      out.write(new byte[8]);
    }
    assertTrue(fs.delete(dir, /* recursive */ true));
    assertFalse(fs.exists(dir));
  }

  /** /.snapshot paths are envelope-rejected. */
  @Test
  @Timeout(60)
  public void deleteSnapshotPathFallsBackToLegacy() throws Exception {
    Path dir = new Path("/del-snap-parent");
    fs.mkdirs(dir);
    fs.allowSnapshot(dir);
    Path f = new Path(dir, "data");
    try (FSDataOutputStream out = fs.create(f)) {
      out.write(new byte[16]);
    }
    fs.createSnapshot(dir, "s1");
    // Attempting to delete the live file while a snapshot references
    // the parent exercises the snapshot-feature fallback path.
    assertTrue(fs.delete(f, false),
        "delete of live file under snapshottable parent should succeed");
    assertFalse(fs.exists(f));
  }

  /**
   * /.reserved paths are envelope-rejected and fall through to legacy.
   * Legacy's FSDirDeleteOp.delete throws InvalidPathException for the
   * exact reserved name "/.reserved"; for a non-existent reserved
   * subpath it returns false. Both confirm pilot did not silently
   * handle the path.
   */
  @Test
  @Timeout(60)
  public void deleteReservedPathFallsBackToLegacy() throws Exception {
    // Non-existent reserved subpath — legacy returns false.
    assertFalse(fs.delete(new Path("/.reserved/fake"), false));
    // Exact /.reserved — legacy throws InvalidPathException
    // (wrapped as RemoteException by the RPC layer).
    assertThrows(IOException.class,
        () -> fs.delete(new Path("/.reserved"), false));
  }

  /**
   * Non-owner lacking WRITE on parent cannot delete the file. Pilot
   * must enforce checkPermission(parentAccess=WRITE).
   */
  @Test
  @Timeout(60)
  public void deleteRespectsParentWritePermission() throws Exception {
    Path parent = new Path("/del-perm/restricted");
    fs.mkdirs(parent);
    Path f = new Path(parent, "data");
    try (FSDataOutputStream out = fs.create(f)) {
      out.write(new byte[8]);
    }
    // Strip write on parent for others.
    fs.setPermission(parent, new FsPermission((short) 0755));

    final UserGroupInformation other =
        UserGroupInformation.createRemoteUser("del-other-user");
    final URI clusterUri = cluster.getURI();
    final Configuration otherConf = new HdfsConfiguration();
    otherConf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
        IIPBasedFSNamesystemLock.class, FSNLockManager.class);

    other.doAs((PrivilegedExceptionAction<Void>) () -> {
      DistributedFileSystem otherFs = (DistributedFileSystem)
          FileSystem.get(clusterUri, otherConf);
      try {
        assertThrows(AccessControlException.class,
            () -> otherFs.delete(f, false));
      } finally {
        otherFs.close();
      }
      return null;
    });
    // Owner can still delete.
    assertTrue(fs.delete(f, false));
  }

  /**
   * Disjoint-parents concurrency: 8 threads × 20 file deletes, each
   * thread against its own parent directory.
   */
  @Test
  @Timeout(120)
  public void parallelDeleteDisjointParents() throws Exception {
    final int numThreads = 8;
    final int filesPerThread = 20;
    // Pre-create.
    for (int t = 0; t < numThreads; t++) {
      fs.mkdirs(new Path("/pdel/p" + t));
      for (int i = 0; i < filesPerThread; i++) {
        try (FSDataOutputStream out = fs.create(
            new Path("/pdel/p" + t + "/f" + i))) {
          out.write(new byte[8]);
        }
      }
    }

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
            for (int i = 0; i < filesPerThread; i++) {
              if (!fs.delete(new Path("/pdel/p" + tid + "/f" + i), false)) {
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
      for (int i = 0; i < filesPerThread; i++) {
        assertFalse(fs.exists(new Path("/pdel/p" + t + "/f" + i)));
      }
    }
  }

  /**
   * Hot-parent concurrency: 16 threads delete distinct children of
   * the same parent. Exercises parent-write-lock contention.
   */
  @Test
  @Timeout(120)
  public void parallelDeleteUnderSameParent() throws Exception {
    final Path hot = new Path("/del-hot-parent");
    fs.mkdirs(hot);
    final int numThreads = 16;
    final int filesPerThread = 20;
    for (int t = 0; t < numThreads; t++) {
      for (int i = 0; i < filesPerThread; i++) {
        try (FSDataOutputStream out = fs.create(
            new Path(hot, "t" + t + "_" + i))) {
          out.write(new byte[4]);
        }
      }
    }

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
            for (int i = 0; i < filesPerThread; i++) {
              if (!fs.delete(new Path(hot, "t" + tid + "_" + i), false)) {
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
    // No descendants should remain.
    assertEquals(0, fs.listStatus(hot).length);
  }

  // ======================================================================
  // setPermission pilot path (PATH_WRITE).
  // First migration exercising the PATH_WRITE mode.
  // ======================================================================

  /** Set permission on a regular file under a shallow path. */
  @Test
  @Timeout(60)
  public void setPermissionOnShallowFile() throws Exception {
    Path p = new Path("/sp-shallow");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[8]);
    }
    FsPermission newPerm = new FsPermission((short) 0640);
    fs.setPermission(p, newPerm);
    assertEquals(newPerm, fs.getFileStatus(p).getPermission());
  }

  /** Set permission on a regular file under a nested path. */
  @Test
  @Timeout(60)
  public void setPermissionOnDeepFile() throws Exception {
    Path parent = new Path("/sp-deep/a/b");
    fs.mkdirs(parent);
    Path p = new Path(parent, "file");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[8]);
    }
    FsPermission newPerm = new FsPermission((short) 0600);
    fs.setPermission(p, newPerm);
    assertEquals(newPerm, fs.getFileStatus(p).getPermission());
  }

  /** Set permission on a directory target (not just files). */
  @Test
  @Timeout(60)
  public void setPermissionOnDirectory() throws Exception {
    Path dir = new Path("/sp-dir");
    fs.mkdirs(dir);
    FsPermission newPerm = new FsPermission((short) 0750);
    fs.setPermission(dir, newPerm);
    assertEquals(newPerm, fs.getFileStatus(dir).getPermission());
  }

  /** Missing target: legacy throws FileNotFoundException. */
  @Test
  @Timeout(60)
  public void setPermissionOnMissingTargetFails() throws Exception {
    assertThrows(FileNotFoundException.class, () ->
        fs.setPermission(new Path("/sp-missing"),
            new FsPermission((short) 0755)));
  }

  /** /.snapshot paths fall back to legacy which rejects them. */
  @Test
  @Timeout(60)
  public void setPermissionSnapshotPathFallsBackToLegacy() throws Exception {
    Path dir = new Path("/sp-snap-parent");
    fs.mkdirs(dir);
    fs.allowSnapshot(dir);
    Path f = new Path(dir, "data");
    try (FSDataOutputStream out = fs.create(f)) {
      out.write(new byte[8]);
    }
    fs.createSnapshot(dir, "s1");
    Path snapFile = new Path(dir, ".snapshot/s1/data");
    // Changing permissions through a snapshot path is forbidden in
    // HDFS; legacy throws SnapshotAccessControlException.
    assertThrows(IOException.class,
        () -> fs.setPermission(snapFile, new FsPermission((short) 0700)));
  }

  /** /.reserved paths fall back to legacy. */
  @Test
  @Timeout(60)
  public void setPermissionReservedPathFallsBackToLegacy() throws Exception {
    Path p = new Path("/sp-reserved");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[8]);
    }
    long fileId = fs.getClient().getFileInfo(p.toString()).getFileId();
    String reserved = "/.reserved/.inodes/" + fileId;
    // Legacy accepts /.reserved/.inodes/<id> — the operation still
    // takes effect on the real INode. Pilot envelope rejects
    // /.reserved so the call goes through the legacy path.
    FsPermission newPerm = new FsPermission((short) 0600);
    fs.setPermission(new Path(reserved), newPerm);
    assertEquals(newPerm, fs.getFileStatus(p).getPermission());
  }

  /**
   * Non-owner (and non-superuser) cannot setPermission. Pilot must
   * enforce checkOwner.
   */
  @Test
  @Timeout(60)
  public void setPermissionRespectsOwnerCheck() throws Exception {
    Path p = new Path("/sp-perm/file");
    fs.mkdirs(p.getParent());
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[8]);
    }

    final UserGroupInformation other =
        UserGroupInformation.createRemoteUser("sp-other-user");
    final URI clusterUri = cluster.getURI();
    final Configuration otherConf = new HdfsConfiguration();
    otherConf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
        IIPBasedFSNamesystemLock.class, FSNLockManager.class);

    other.doAs((PrivilegedExceptionAction<Void>) () -> {
      DistributedFileSystem otherFs = (DistributedFileSystem)
          FileSystem.get(clusterUri, otherConf);
      try {
        assertThrows(AccessControlException.class,
            () -> otherFs.setPermission(p, new FsPermission((short) 0777)));
      } finally {
        otherFs.close();
      }
      return null;
    });
    // Owner can still change.
    fs.setPermission(p, new FsPermission((short) 0600));
    assertEquals(new FsPermission((short) 0600),
        fs.getFileStatus(p).getPermission());
  }

  /**
   * Disjoint-targets concurrency: 16 threads setPermission on their
   * own distinct files. Under PATH_WRITE, each call takes write lock
   * only on its own target; concurrent calls do not serialise.
   */
  @Test
  @Timeout(120)
  public void parallelSetPermissionDisjointTargets() throws Exception {
    final int numThreads = 16;
    final int iterationsPerThread = 20;
    for (int t = 0; t < numThreads; t++) {
      Path p = new Path("/sp-parallel/f" + t);
      fs.mkdirs(p.getParent());
      try (FSDataOutputStream out = fs.create(p)) {
        out.write(new byte[4]);
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
            for (int i = 0; i < iterationsPerThread; i++) {
              // Flip between two permission sets — each call must
              // succeed and leave a deterministic final state.
              short mode = (i % 2 == 0) ? (short) 0640 : (short) 0600;
              fs.setPermission(new Path("/sp-parallel/f" + tid),
                  new FsPermission(mode));
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
        f.get(90, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertEquals(0, errors.get());
    // After (iterationsPerThread=20) flips ending on an even iteration
    // index, final permission per file should be 0600 (the odd-index
    // side of the flip). Verify all files ended deterministically.
    FsPermission expected = new FsPermission((short) 0600);
    for (int t = 0; t < numThreads; t++) {
      assertEquals(expected,
          fs.getFileStatus(new Path("/sp-parallel/f" + t)).getPermission(),
          "unexpected final permission for file " + t);
    }
  }

  /**
   * Concurrent readers (getFileInfo via PATH_READ) against a writer
   * (setPermission via PATH_WRITE) on the SAME file. Readers take
   * per-INode READ lock on the target; writer takes WRITE. They
   * conflict on the target INode's lock, so this test verifies no
   * deadlock and eventual consistency.
   */
  @Test
  @Timeout(120)
  public void concurrentReadersVsSetPermissionOnSameFile() throws Exception {
    Path p = new Path("/sp-rw-hot");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[32]);
    }

    final int readers = 12;
    final int writerIterations = 40;
    final AtomicInteger errors = new AtomicInteger(0);
    final CountDownLatch start = new CountDownLatch(1);
    final java.util.concurrent.atomic.AtomicBoolean done =
        new java.util.concurrent.atomic.AtomicBoolean(false);

    ExecutorService exec = Executors.newFixedThreadPool(readers + 1);
    try {
      Future<?>[] futures = new Future<?>[readers + 1];
      for (int t = 0; t < readers; t++) {
        futures[t] = exec.submit(() -> {
          try {
            start.await();
            while (!done.get()) {
              FileStatus s = fs.getFileStatus(p);
              if (s == null || s.getPath() == null) {
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
      futures[readers] = exec.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < writerIterations; i++) {
            short mode = (i % 2 == 0) ? (short) 0640 : (short) 0600;
            fs.setPermission(p, new FsPermission(mode));
          }
        } catch (Exception e) {
          errors.incrementAndGet();
          throw new RuntimeException(e);
        } finally {
          done.set(true);
        }
        return null;
      });
      start.countDown();
      for (Future<?> f : futures) {
        f.get(90, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertEquals(0, errors.get());
  }

  // ======================================================================
  // setOwner pilot path (PATH_WRITE).
  // ======================================================================

  /**
   * Superuser setOwner on a regular file. Non-root user is not
   * allowed (legacy parity), so we run as the cluster superuser.
   */
  @Test
  @Timeout(60)
  public void setOwnerChangesOwnerAsSuperuser() throws Exception {
    Path p = new Path("/so-su");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[8]);
    }
    fs.setOwner(p, "newuser", "newgroup");
    FileStatus status = fs.getFileStatus(p);
    assertEquals("newuser", status.getOwner());
    assertEquals("newgroup", status.getGroup());
  }

  /** Changing only the group (keeping owner null) works. */
  @Test
  @Timeout(60)
  public void setOwnerChangesGroupOnly() throws Exception {
    Path p = new Path("/so-group-only");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[8]);
    }
    String origOwner = fs.getFileStatus(p).getOwner();
    fs.setOwner(p, null, "only-new-group");
    FileStatus status = fs.getFileStatus(p);
    assertEquals(origOwner, status.getOwner(), "owner must not change");
    assertEquals("only-new-group", status.getGroup());
  }

  /** Missing target → FileNotFoundException. */
  @Test
  @Timeout(60)
  public void setOwnerOnMissingTargetFails() throws Exception {
    assertThrows(FileNotFoundException.class,
        () -> fs.setOwner(new Path("/so-missing"), "u", "g"));
  }

  /**
   * Non-superuser cannot change owner to a different user. Must
   * throw AccessControlException.
   */
  @Test
  @Timeout(60)
  public void setOwnerRejectsNonSuperuserChangingOwnerToDifferentUser()
      throws Exception {
    Path parent = new Path("/so-perm");
    fs.mkdirs(parent);
    Path p = new Path(parent, "file");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[8]);
    }
    // Make the file owned by a non-superuser so they can attempt
    // (and be rejected on) setOwner.
    fs.setOwner(p, "so-other-user", "so-other-group");
    fs.setPermission(parent, new FsPermission((short) 0777));

    final UserGroupInformation other =
        UserGroupInformation.createRemoteUser("so-other-user");
    final URI clusterUri = cluster.getURI();
    final Configuration otherConf = new HdfsConfiguration();
    otherConf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
        IIPBasedFSNamesystemLock.class, FSNLockManager.class);

    other.doAs((PrivilegedExceptionAction<Void>) () -> {
      DistributedFileSystem otherFs = (DistributedFileSystem)
          FileSystem.get(clusterUri, otherConf);
      try {
        // Non-su user trying to change owner to a different user.
        assertThrows(AccessControlException.class,
            () -> otherFs.setOwner(p, "yet-another-user", null));
      } finally {
        otherFs.close();
      }
      return null;
    });
  }

  /** /.snapshot paths fall back to legacy which rejects them. */
  @Test
  @Timeout(60)
  public void setOwnerSnapshotPathFallsBackToLegacy() throws Exception {
    Path dir = new Path("/so-snap-parent");
    fs.mkdirs(dir);
    fs.allowSnapshot(dir);
    Path f = new Path(dir, "data");
    try (FSDataOutputStream out = fs.create(f)) {
      out.write(new byte[8]);
    }
    fs.createSnapshot(dir, "s1");
    Path snapFile = new Path(dir, ".snapshot/s1/data");
    assertThrows(IOException.class,
        () -> fs.setOwner(snapFile, "x", "y"));
  }

  /** /.reserved paths fall back and apply through the real INode. */
  @Test
  @Timeout(60)
  public void setOwnerReservedPathFallsBackToLegacy() throws Exception {
    Path p = new Path("/so-reserved");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[8]);
    }
    long fileId = fs.getClient().getFileInfo(p.toString()).getFileId();
    String reserved = "/.reserved/.inodes/" + fileId;
    fs.setOwner(new Path(reserved), "via-reserved", "via-reserved-group");
    FileStatus status = fs.getFileStatus(p);
    assertEquals("via-reserved", status.getOwner());
    assertEquals("via-reserved-group", status.getGroup());
  }

  /** 16 threads setOwner on their own distinct files in parallel. */
  @Test
  @Timeout(120)
  public void parallelSetOwnerDisjointTargets() throws Exception {
    final int numThreads = 16;
    for (int t = 0; t < numThreads; t++) {
      Path p = new Path("/so-parallel/f" + t);
      fs.mkdirs(p.getParent());
      try (FSDataOutputStream out = fs.create(p)) {
        out.write(new byte[4]);
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
            for (int i = 0; i < 20; i++) {
              String u = "user-" + tid + "-" + (i % 2);
              fs.setOwner(new Path("/so-parallel/f" + tid), u, null);
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
        f.get(90, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertEquals(0, errors.get());
    // Final owner for each file must be user-<tid>-1 (last iteration
    // index 19 → i%2 == 1).
    for (int t = 0; t < numThreads; t++) {
      assertEquals("user-" + t + "-1",
          fs.getFileStatus(new Path("/so-parallel/f" + t)).getOwner(),
          "unexpected final owner for file " + t);
    }
  }

  // ======================================================================
  // setTimes pilot path (PATH_WRITE).
  // ======================================================================

  /** Happy path: set both mtime and atime. */
  @Test
  @Timeout(60)
  public void setTimesOnFileUpdatesBothTimes() throws Exception {
    Path p = new Path("/st-both");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[8]);
    }
    long newMtime = 1_700_000_000_000L;
    long newAtime = 1_700_000_001_000L;
    fs.setTimes(p, newMtime, newAtime);
    FileStatus status = fs.getFileStatus(p);
    assertEquals(newMtime, status.getModificationTime());
    assertEquals(newAtime, status.getAccessTime());
  }

  /** mtime=-1 leaves modification time untouched. */
  @Test
  @Timeout(60)
  public void setTimesWithMtimeNegativeOneLeavesMtimeUnchanged()
      throws Exception {
    Path p = new Path("/st-mtime-skip");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[8]);
    }
    long origMtime = fs.getFileStatus(p).getModificationTime();
    long newAtime = 1_700_000_002_000L;
    fs.setTimes(p, -1, newAtime);
    FileStatus status = fs.getFileStatus(p);
    assertEquals(origMtime, status.getModificationTime(),
        "mtime=-1 must leave modification time untouched");
    assertEquals(newAtime, status.getAccessTime());
  }

  /** Missing target → FileNotFoundException. */
  @Test
  @Timeout(60)
  public void setTimesOnMissingTargetFails() throws Exception {
    assertThrows(FileNotFoundException.class,
        () -> fs.setTimes(new Path("/st-missing"), 123L, 456L));
  }

  /** Non-writer user is rejected by the WRITE-permission check. */
  @Test
  @Timeout(60)
  public void setTimesRespectsPathAccessWrite() throws Exception {
    Path parent = new Path("/st-perm");
    fs.mkdirs(parent);
    Path p = new Path(parent, "file");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[8]);
    }
    // 0644 — non-owner has no write access.
    fs.setPermission(p, new FsPermission((short) 0644));

    final UserGroupInformation other =
        UserGroupInformation.createRemoteUser("st-other-user");
    final URI clusterUri = cluster.getURI();
    final Configuration otherConf = new HdfsConfiguration();
    otherConf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
        IIPBasedFSNamesystemLock.class, FSNLockManager.class);

    other.doAs((PrivilegedExceptionAction<Void>) () -> {
      DistributedFileSystem otherFs = (DistributedFileSystem)
          FileSystem.get(clusterUri, otherConf);
      try {
        assertThrows(AccessControlException.class,
            () -> otherFs.setTimes(p, 9_000_000L, 9_000_000L));
      } finally {
        otherFs.close();
      }
      return null;
    });
  }

  /** /.snapshot paths fall back and get rejected by legacy. */
  @Test
  @Timeout(60)
  public void setTimesSnapshotPathFallsBackToLegacy() throws Exception {
    Path dir = new Path("/st-snap-parent");
    fs.mkdirs(dir);
    fs.allowSnapshot(dir);
    Path f = new Path(dir, "data");
    try (FSDataOutputStream out = fs.create(f)) {
      out.write(new byte[8]);
    }
    fs.createSnapshot(dir, "s1");
    Path snapFile = new Path(dir, ".snapshot/s1/data");
    assertThrows(IOException.class,
        () -> fs.setTimes(snapFile, 100L, 200L));
  }

  /** 16 threads setTimes on their own files in parallel. */
  @Test
  @Timeout(120)
  public void parallelSetTimesDisjointTargets() throws Exception {
    final int numThreads = 16;
    for (int t = 0; t < numThreads; t++) {
      Path p = new Path("/st-parallel/f" + t);
      fs.mkdirs(p.getParent());
      try (FSDataOutputStream out = fs.create(p)) {
        out.write(new byte[4]);
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
            for (int i = 0; i < 20; i++) {
              long mt = 1_700_000_000_000L + tid * 1000L + i;
              fs.setTimes(new Path("/st-parallel/f" + tid), mt, -1);
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
        f.get(90, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertEquals(0, errors.get());
    // Final mtime for each file is the last iteration's (i=19).
    for (int t = 0; t < numThreads; t++) {
      long expected = 1_700_000_000_000L + t * 1000L + 19;
      assertEquals(expected,
          fs.getFileStatus(new Path("/st-parallel/f" + t))
              .getModificationTime(),
          "unexpected final mtime for file " + t);
    }
  }

  // ======================================================================
  // setReplication pilot path (PATH_WRITE + nested BM write lock).
  // ======================================================================

  /** Happy path: lower replication from 3 (default) to 1. */
  @Test
  @Timeout(60)
  public void setReplicationDecreases() throws Exception {
    Path p = new Path("/sr-down");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 3, 4096L)) {
      out.write(new byte[64]);
    }
    assertTrue(fs.setReplication(p, (short) 1));
    assertEquals((short) 1, fs.getFileStatus(p).getReplication());
  }

  /** Raise replication. */
  @Test
  @Timeout(60)
  public void setReplicationIncreases() throws Exception {
    Path p = new Path("/sr-up");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1, 4096L)) {
      out.write(new byte[64]);
    }
    assertTrue(fs.setReplication(p, (short) 2));
    assertEquals((short) 2, fs.getFileStatus(p).getReplication());
  }

  /** setReplication on a directory returns false (legacy parity). */
  @Test
  @Timeout(60)
  public void setReplicationOnDirectoryReturnsFalse() throws Exception {
    Path dir = new Path("/sr-dir");
    fs.mkdirs(dir);
    assertFalse(fs.setReplication(dir, (short) 2));
  }

  /** Missing target returns false (legacy parity). */
  @Test
  @Timeout(60)
  public void setReplicationOnMissingTargetReturnsFalse() throws Exception {
    assertFalse(fs.setReplication(new Path("/sr-missing"), (short) 2));
  }

  /**
   * Non-writer user is rejected. Verifies PATH_WRITE path enforces
   * checkPathAccess(WRITE).
   */
  @Test
  @Timeout(60)
  public void setReplicationRespectsPathAccessWrite() throws Exception {
    Path parent = new Path("/sr-perm");
    fs.mkdirs(parent);
    Path p = new Path(parent, "file");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1, 4096L)) {
      out.write(new byte[8]);
    }
    fs.setPermission(p, new FsPermission((short) 0644));

    final UserGroupInformation other =
        UserGroupInformation.createRemoteUser("sr-other-user");
    final URI clusterUri = cluster.getURI();
    final Configuration otherConf = new HdfsConfiguration();
    otherConf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
        IIPBasedFSNamesystemLock.class, FSNLockManager.class);

    other.doAs((PrivilegedExceptionAction<Void>) () -> {
      DistributedFileSystem otherFs = (DistributedFileSystem)
          FileSystem.get(clusterUri, otherConf);
      try {
        assertThrows(AccessControlException.class,
            () -> otherFs.setReplication(p, (short) 2));
      } finally {
        otherFs.close();
      }
      return null;
    });
  }

  /** /.snapshot paths fall back to legacy. */
  @Test
  @Timeout(60)
  public void setReplicationSnapshotPathFallsBackToLegacy() throws Exception {
    Path dir = new Path("/sr-snap-parent");
    fs.mkdirs(dir);
    fs.allowSnapshot(dir);
    Path f = new Path(dir, "data");
    try (FSDataOutputStream out = fs.create(f, true, 4096, (short) 1, 4096L)) {
      out.write(new byte[8]);
    }
    fs.createSnapshot(dir, "s1");
    Path snapFile = new Path(dir, ".snapshot/s1/data");
    assertThrows(IOException.class,
        () -> fs.setReplication(snapFile, (short) 2));
  }

  /** 16 threads setReplication on their own files in parallel. */
  @Test
  @Timeout(120)
  public void parallelSetReplicationDisjointTargets() throws Exception {
    final int numThreads = 16;
    for (int t = 0; t < numThreads; t++) {
      Path p = new Path("/sr-parallel/f" + t);
      fs.mkdirs(p.getParent());
      try (FSDataOutputStream out = fs.create(p, true, 4096,
          (short) 1, 4096L)) {
        out.write(new byte[4]);
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
            for (int i = 0; i < 10; i++) {
              short repl = (short) ((i % 2) + 1);
              if (!fs.setReplication(new Path("/sr-parallel/f" + tid), repl)) {
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
        f.get(90, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertEquals(0, errors.get());
    // Final replication per file: i=9 → (9 % 2)+1 = 2.
    for (int t = 0; t < numThreads; t++) {
      assertEquals((short) 2,
          fs.getFileStatus(new Path("/sr-parallel/f" + t)).getReplication(),
          "unexpected final replication for file " + t);
    }
  }

  // ======================================================================
  // getListing pilot path (PATH_READ with directory iteration).
  // ======================================================================

  /** Happy path: list a directory with a handful of children. */
  @Test
  @Timeout(60)
  public void getListingOnDirectory() throws Exception {
    Path dir = new Path("/gl-dir");
    fs.mkdirs(dir);
    for (int i = 0; i < 5; i++) {
      try (FSDataOutputStream out = fs.create(new Path(dir, "f" + i))) {
        out.write(new byte[4]);
      }
    }
    FileStatus[] entries = fs.listStatus(dir);
    assertEquals(5, entries.length);
  }

  /** Listing a single file returns a one-entry listing of itself. */
  @Test
  @Timeout(60)
  public void getListingOnFile() throws Exception {
    Path p = new Path("/gl-single");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[8]);
    }
    FileStatus[] entries = fs.listStatus(p);
    assertEquals(1, entries.length);
    assertEquals("gl-single", entries[0].getPath().getName());
  }

  /** Listing a missing path throws FileNotFoundException. */
  @Test
  @Timeout(60)
  public void getListingOnMissingPathFails() throws Exception {
    assertThrows(FileNotFoundException.class,
        () -> fs.listStatus(new Path("/gl-missing")));
  }

  /** Empty directory → zero-length listing. */
  @Test
  @Timeout(60)
  public void getListingOnEmptyDirectory() throws Exception {
    Path dir = new Path("/gl-empty");
    fs.mkdirs(dir);
    FileStatus[] entries = fs.listStatus(dir);
    assertEquals(0, entries.length);
  }

  /**
   * Pagination via {@code listStatusIterator} which uses startAfter.
   * Verifies multiple listing batches assemble correctly under
   * PATH_READ.
   */
  @Test
  @Timeout(60)
  public void getListingPaginationAcrossBatches() throws Exception {
    Path dir = new Path("/gl-pagination");
    fs.mkdirs(dir);
    final int total = 50;
    for (int i = 0; i < total; i++) {
      try (FSDataOutputStream out = fs.create(
          new Path(dir, String.format("f%03d", i)))) {
        out.write(new byte[4]);
      }
    }
    org.apache.hadoop.fs.RemoteIterator<FileStatus> it =
        fs.listStatusIterator(dir);
    int count = 0;
    while (it.hasNext()) {
      it.next();
      count++;
    }
    assertEquals(total, count);
  }

  /**
   * {@code startAfter} using the NN-internal INodePath form
   * ({@code /.reserved/.inodes/<id>}) is resolved by the pilot path
   * itself (HDFS-17386 Phase III / Track P.5) — no fallback to
   * legacy. Verifies the pilot now calls
   * {@link org.apache.hadoop.hdfs.server.namenode.FSDirStatAndListingOp#resolveInodePathStartAfter}
   * and returns the correct continuation slice.
   */
  @Test
  @Timeout(60)
  public void getListingWithInodePathStartAfter() throws Exception {
    Path dir = new Path("/gl-inode-start");
    fs.mkdirs(dir);
    final int total = 10;
    for (int i = 0; i < total; i++) {
      try (FSDataOutputStream out = fs.create(
          new Path(dir, String.format("f%02d", i)))) {
        out.write(new byte[4]);
      }
    }
    // Pick the 3rd child (f02) and form an INodePath-style startAfter
    // pointing at it. The listing should return f03..f09.
    HdfsFileStatus third = fs.getClient().getFileInfo("/gl-inode-start/f02");
    assertNotNull(third, "f02 must exist before forming startAfter");
    byte[] startAfter = DFSUtil.string2Bytes(
        "/.reserved/.inodes/" + third.getFileId());

    DirectoryListing listing = fs.getClient().listPaths(
        "/gl-inode-start", startAfter, false);
    HdfsFileStatus[] partial = listing.getPartialListing();
    assertEquals(total - 3, partial.length,
        "INodePath startAfter must yield a 7-entry continuation");
    assertEquals("f03", DFSUtil.bytes2String(partial[0].getLocalNameInBytes()));
    assertEquals("f09", DFSUtil.bytes2String(
        partial[partial.length - 1].getLocalNameInBytes()));
  }

  /**
   * {@code startAfter} pointing at an inode that has been deleted
   * surfaces as {@link DirectoryListingStartAfterNotFoundException}
   * — matches the legacy contract and confirms the pilot's
   * resolver propagates the failure rather than swallowing it.
   */
  @Test
  @Timeout(60)
  public void getListingWithDeletedInodeStartAfterThrows() throws Exception {
    Path dir = new Path("/gl-inode-deleted");
    fs.mkdirs(dir);
    try (FSDataOutputStream out = fs.create(new Path(dir, "child"))) {
      out.write(new byte[4]);
    }
    HdfsFileStatus child = fs.getClient().getFileInfo("/gl-inode-deleted/child");
    assertNotNull(child);
    long childId = child.getFileId();
    assertTrue(fs.delete(new Path(dir, "child"), false));

    byte[] startAfter = DFSUtil.string2Bytes("/.reserved/.inodes/" + childId);
    // RPC layer wraps the exception; unwrap to assert the real cause.
    org.apache.hadoop.ipc.RemoteException re = assertThrows(
        org.apache.hadoop.ipc.RemoteException.class,
        () -> fs.getClient().listPaths(
            "/gl-inode-deleted", startAfter, false));
    IOException unwrapped = re.unwrapRemoteException();
    assertTrue(unwrapped instanceof DirectoryListingStartAfterNotFoundException,
        "expected DirectoryListingStartAfterNotFoundException, got: "
            + unwrapped.getClass().getName());
  }

  /**
   * listStatus with needLocation=true exercises per-child block-
   * location resolution — each createFileStatus call acquires BM
   * read lock nested.
   */
  @Test
  @Timeout(60)
  public void getListingWithBlockLocations() throws Exception {
    Path dir = new Path("/gl-loc");
    fs.mkdirs(dir);
    for (int i = 0; i < 3; i++) {
      try (FSDataOutputStream out = fs.create(new Path(dir, "f" + i))) {
        out.write(new byte[256]);
      }
    }
    // LocatedFileStatus iteration forces needLocation=true server-side.
    org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus>
        it = fs.listLocatedStatus(dir);
    int count = 0;
    while (it.hasNext()) {
      org.apache.hadoop.fs.LocatedFileStatus status = it.next();
      if (!status.isDirectory()) {
        org.apache.hadoop.fs.BlockLocation[] bl = status.getBlockLocations();
        assertNotNull(bl);
        assertTrue(bl.length > 0,
            "file listing must include block locations");
      }
      count++;
    }
    assertEquals(3, count);
  }

  /** /.snapshot paths fall back to legacy which handles them. */
  @Test
  @Timeout(60)
  public void getListingSnapshotPathFallsBackToLegacy() throws Exception {
    Path dir = new Path("/gl-snap-parent");
    fs.mkdirs(dir);
    fs.allowSnapshot(dir);
    try (FSDataOutputStream out = fs.create(new Path(dir, "inside"))) {
      out.write(new byte[16]);
    }
    fs.createSnapshot(dir, "s1");
    FileStatus[] entries = fs.listStatus(
        new Path(dir, ".snapshot/s1"));
    assertEquals(1, entries.length);
    assertEquals("inside", entries[0].getPath().getName());
  }

  /** /.reserved paths fall back to legacy. */
  @Test
  @Timeout(60)
  public void getListingReservedPathFallsBackToLegacy() throws Exception {
    // listStatus on /.reserved returns the well-known set of reserved
    // names (".inodes", "raw"). Legacy handles this via
    // getReservedListing; pilot envelope rejects /.reserved so the
    // call routes through legacy.
    FileStatus[] entries = fs.listStatus(new Path("/.reserved"));
    assertTrue(entries.length >= 1, "reserved listing must return >=1 entry");
  }

  /** Non-owner lacking READ_EXECUTE on the directory is rejected. */
  @Test
  @Timeout(60)
  public void getListingRespectsReadExecutePermission() throws Exception {
    Path dir = new Path("/gl-perm/restricted");
    fs.mkdirs(dir);
    try (FSDataOutputStream out = fs.create(new Path(dir, "inside"))) {
      out.write(new byte[8]);
    }
    fs.setPermission(dir, new FsPermission((short) 0700));

    final UserGroupInformation other =
        UserGroupInformation.createRemoteUser("gl-other-user");
    final URI clusterUri = cluster.getURI();
    final Configuration otherConf = new HdfsConfiguration();
    otherConf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
        IIPBasedFSNamesystemLock.class, FSNLockManager.class);

    other.doAs((PrivilegedExceptionAction<Void>) () -> {
      DistributedFileSystem otherFs = (DistributedFileSystem)
          FileSystem.get(clusterUri, otherConf);
      try {
        assertThrows(AccessControlException.class,
            () -> otherFs.listStatus(dir));
      } finally {
        otherFs.close();
      }
      return null;
    });
  }

  /** Concurrent readers listing disjoint directories in parallel. */
  @Test
  @Timeout(120)
  public void parallelGetListingDisjointDirectories() throws Exception {
    final int numThreads = 16;
    for (int t = 0; t < numThreads; t++) {
      Path d = new Path("/gl-parallel/d" + t);
      fs.mkdirs(d);
      for (int i = 0; i < 5; i++) {
        try (FSDataOutputStream out = fs.create(new Path(d, "f" + i))) {
          out.write(new byte[4]);
        }
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
            for (int i = 0; i < 30; i++) {
              FileStatus[] entries = fs.listStatus(
                  new Path("/gl-parallel/d" + tid));
              if (entries.length != 5) {
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
        f.get(90, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertEquals(0, errors.get());
  }

  // ======================================================================
  // Pre-RPC-10 gate review: ancestor-fallback coverage + root PATH_READ.
  // These tests close gaps surfaced during the 9-migration review.
  // ======================================================================

  /**
   * Pilot-mutate RPC on a file whose ancestor has a directory quota
   * set must fall back to legacy (ancestorsAllowMutate → false). Legacy
   * handles the quota bookkeeping correctly; pilot avoids touching it.
   * Uses namespace-only quota (not disk space) to keep the test tight
   * and avoid per-cluster storage sizing.
   */
  @Test
  @Timeout(60)
  public void setPermissionUnderAncestorQuotaFallsBackToLegacy()
      throws Exception {
    Path quotaDir = new Path("/preview-quota-parent");
    fs.mkdirs(quotaDir);
    // Namespace quota of 100 inodes, no disk space cap.
    fs.setQuota(quotaDir, 100L, HdfsConstants.QUOTA_DONT_SET);
    Path p = new Path(quotaDir, "file");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1, 4096L)) {
      out.write(new byte[8]);
    }
    FsPermission newPerm = new FsPermission((short) 0600);
    fs.setPermission(p, newPerm);
    // Legacy must apply the change correctly.
    assertEquals(newPerm, fs.getFileStatus(p).getPermission());
  }

  /**
   * setOwner under ancestor quota must fall back correctly. Uses
   * namespace-only quota (same rationale as the setPermission case).
   */
  @Test
  @Timeout(60)
  public void setOwnerUnderAncestorQuotaFallsBackToLegacy()
      throws Exception {
    Path quotaDir = new Path("/preview-so-quota-parent");
    fs.mkdirs(quotaDir);
    fs.setQuota(quotaDir, 100L, HdfsConstants.QUOTA_DONT_SET);
    Path p = new Path(quotaDir, "file");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1, 4096L)) {
      out.write(new byte[8]);
    }
    fs.setOwner(p, "quota-fallback-user", null);
    assertEquals("quota-fallback-user",
        fs.getFileStatus(p).getOwner());
  }

  /**
   * Delete on a file whose ancestor is snapshottable must fall back
   * to legacy (ancestorsAllowMutate rejects isSnapshottable).
   */
  @Test
  @Timeout(60)
  public void deleteUnderSnapshottableAncestorFallsBackToLegacy()
      throws Exception {
    Path parent = new Path("/preview-snap-able");
    fs.mkdirs(parent);
    fs.allowSnapshot(parent);
    Path p = new Path(parent, "file");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1, 4096L)) {
      out.write(new byte[8]);
    }
    // Delete must succeed via the legacy fallback, which handles
    // snapshot bookkeeping correctly.
    assertTrue(fs.delete(p, false));
    assertFalse(fs.exists(p));
  }

  /**
   * Create under an ancestor with a namespace quota must fall back
   * to legacy (ancestorsAllowCreate → false).
   */
  @Test
  @Timeout(60)
  public void createUnderAncestorQuotaFallsBackToLegacy()
      throws Exception {
    Path quotaDir = new Path("/preview-create-quota");
    fs.mkdirs(quotaDir);
    fs.setQuota(quotaDir, 100L, HdfsConstants.QUOTA_DONT_SET);
    Path p = new Path(quotaDir, "file");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1, 4096L)) {
      out.write(new byte[8]);
    }
    assertTrue(fs.exists(p));
  }

  /**
   * getFileInfo on the root path "/". PATH_READ on root is allowed
   * (pathLen=1, lock only the root INode). Edge case not previously
   * covered.
   */
  @Test
  @Timeout(60)
  public void getFileInfoOnRootPath() throws Exception {
    FileStatus rootStatus = fs.getFileStatus(new Path("/"));
    assertNotNull(rootStatus);
    assertTrue(rootStatus.isDirectory());
  }

  // ======================================================================
  // setStoragePolicy pilot (RPC #10) — first RPC using the dispatch
  // template introduced by the §6.3 RPC-10 gate refactor.
  // ======================================================================

  /** Happy path: set HOT policy on a file. */
  @Test
  @Timeout(60)
  public void setStoragePolicyOnFile() throws Exception {
    Path p = new Path("/sp10-file");
    try (FSDataOutputStream out = fs.create(p, true, 4096,
        (short) 1, 4096L)) {
      out.write(new byte[8]);
    }
    fs.setStoragePolicy(p,
        HdfsConstants.HOT_STORAGE_POLICY_NAME);
    // The getStoragePolicy client call reflects the change.
    org.apache.hadoop.hdfs.protocol.BlockStoragePolicy policy =
        fs.getClient().getStoragePolicy(p.toString());
    assertEquals(HdfsConstants.HOT_STORAGE_POLICY_ID, policy.getId());
  }

  /** setStoragePolicy on a directory. */
  @Test
  @Timeout(60)
  public void setStoragePolicyOnDirectory() throws Exception {
    Path dir = new Path("/sp10-dir");
    fs.mkdirs(dir);
    fs.setStoragePolicy(dir, HdfsConstants.HOT_STORAGE_POLICY_NAME);
    org.apache.hadoop.hdfs.protocol.BlockStoragePolicy policy =
        fs.getClient().getStoragePolicy(dir.toString());
    assertEquals(HdfsConstants.HOT_STORAGE_POLICY_ID, policy.getId());
  }

  /** Missing target → FileNotFoundException. */
  @Test
  @Timeout(60)
  public void setStoragePolicyOnMissingTargetFails() throws Exception {
    assertThrows(FileNotFoundException.class,
        () -> fs.setStoragePolicy(new Path("/sp10-missing"),
            HdfsConstants.HOT_STORAGE_POLICY_NAME));
  }

  /** Unknown policy → HadoopIllegalArgumentException. */
  @Test
  @Timeout(60)
  public void setStoragePolicyWithUnknownPolicyFails() throws Exception {
    Path p = new Path("/sp10-bad-policy");
    try (FSDataOutputStream out = fs.create(p, true, 4096,
        (short) 1, 4096L)) {
      out.write(new byte[4]);
    }
    assertThrows(IOException.class,
        () -> fs.setStoragePolicy(p, "NOT_A_REAL_POLICY_NAME"));
  }

  /** /.snapshot path falls back to legacy (which rejects it). */
  @Test
  @Timeout(60)
  public void setStoragePolicySnapshotPathFallsBackToLegacy()
      throws Exception {
    Path dir = new Path("/sp10-snap-parent");
    fs.mkdirs(dir);
    fs.allowSnapshot(dir);
    Path f = new Path(dir, "data");
    try (FSDataOutputStream out = fs.create(f, true, 4096,
        (short) 1, 4096L)) {
      out.write(new byte[4]);
    }
    fs.createSnapshot(dir, "s1");
    Path snapFile = new Path(dir, ".snapshot/s1/data");
    assertThrows(IOException.class,
        () -> fs.setStoragePolicy(snapFile,
            HdfsConstants.HOT_STORAGE_POLICY_NAME));
  }

  /**
   * Setting a storage policy on a file under a parent that has a
   * namespace quota triggers the ancestorsAllowMutate fallback — the
   * pilot declines, legacy handles it correctly.
   */
  @Test
  @Timeout(60)
  public void setStoragePolicyUnderAncestorQuotaFallsBackToLegacy()
      throws Exception {
    Path quotaDir = new Path("/sp10-quota-parent");
    fs.mkdirs(quotaDir);
    fs.setQuota(quotaDir, 100L, HdfsConstants.QUOTA_DONT_SET);
    Path p = new Path(quotaDir, "file");
    try (FSDataOutputStream out = fs.create(p, true, 4096,
        (short) 1, 4096L)) {
      out.write(new byte[4]);
    }
    fs.setStoragePolicy(p, HdfsConstants.HOT_STORAGE_POLICY_NAME);
    org.apache.hadoop.hdfs.protocol.BlockStoragePolicy policy =
        fs.getClient().getStoragePolicy(p.toString());
    assertEquals(HdfsConstants.HOT_STORAGE_POLICY_ID, policy.getId());
  }

  /**
   * Non-writer user is rejected by checkPathAccess(WRITE). Verifies
   * the dispatch template's ACE audit wrapping is wired correctly.
   */
  @Test
  @Timeout(60)
  public void setStoragePolicyRespectsPathAccessWrite() throws Exception {
    Path parent = new Path("/sp10-perm");
    fs.mkdirs(parent);
    Path p = new Path(parent, "file");
    try (FSDataOutputStream out = fs.create(p, true, 4096,
        (short) 1, 4096L)) {
      out.write(new byte[4]);
    }
    fs.setPermission(p, new FsPermission((short) 0644));

    final UserGroupInformation other =
        UserGroupInformation.createRemoteUser("sp10-other-user");
    final URI clusterUri = cluster.getURI();
    final Configuration otherConf = new HdfsConfiguration();
    otherConf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
        IIPBasedFSNamesystemLock.class, FSNLockManager.class);

    other.doAs((PrivilegedExceptionAction<Void>) () -> {
      DistributedFileSystem otherFs = (DistributedFileSystem)
          FileSystem.get(clusterUri, otherConf);
      try {
        assertThrows(AccessControlException.class,
            () -> otherFs.setStoragePolicy(p,
                HdfsConstants.HOT_STORAGE_POLICY_NAME));
      } finally {
        otherFs.close();
      }
      return null;
    });
  }

  /** 16 threads × 10 setStoragePolicy on distinct files. */
  @Test
  @Timeout(120)
  public void parallelSetStoragePolicyDisjointTargets() throws Exception {
    final int numThreads = 16;
    for (int t = 0; t < numThreads; t++) {
      Path p = new Path("/sp10-parallel/f" + t);
      fs.mkdirs(p.getParent());
      try (FSDataOutputStream out = fs.create(p, true, 4096,
          (short) 1, 4096L)) {
        out.write(new byte[4]);
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
            for (int i = 0; i < 10; i++) {
              fs.setStoragePolicy(
                  new Path("/sp10-parallel/f" + tid),
                  HdfsConstants.HOT_STORAGE_POLICY_NAME);
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
        f.get(90, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertEquals(0, errors.get());
    for (int t = 0; t < numThreads; t++) {
      org.apache.hadoop.hdfs.protocol.BlockStoragePolicy policy =
          fs.getClient().getStoragePolicy("/sp10-parallel/f" + t);
      assertEquals(HdfsConstants.HOT_STORAGE_POLICY_ID, policy.getId());
    }
  }

  // ======================================================================
  // unsetStoragePolicy pilot (RPC #11).
  // ======================================================================

  /** Happy path: set then unset. */
  @Test
  @Timeout(60)
  public void unsetStoragePolicyOnFile() throws Exception {
    Path p = new Path("/usp-file");
    try (FSDataOutputStream out = fs.create(p, true, 4096,
        (short) 1, 4096L)) {
      out.write(new byte[8]);
    }
    fs.setStoragePolicy(p, HdfsConstants.HOT_STORAGE_POLICY_NAME);
    fs.unsetStoragePolicy(p);
    org.apache.hadoop.hdfs.protocol.BlockStoragePolicy policy =
        fs.getClient().getStoragePolicy(p.toString());
    // Unset → default policy (typically HOT per suite default).
    assertNotNull(policy);
  }

  /** Missing target → FileNotFoundException. */
  @Test
  @Timeout(60)
  public void unsetStoragePolicyOnMissingTargetFails() throws Exception {
    assertThrows(FileNotFoundException.class,
        () -> fs.unsetStoragePolicy(new Path("/usp-missing")));
  }

  // ======================================================================
  // setAcl pilot (RPC #12).
  // ======================================================================

  /** Happy path: set a simple ACL on a file. */
  @Test
  @Timeout(60)
  public void setAclOnFile() throws Exception {
    Path p = new Path("/acl-file");
    try (FSDataOutputStream out = fs.create(p, true, 4096,
        (short) 1, 4096L)) {
      out.write(new byte[8]);
    }
    java.util.List<org.apache.hadoop.fs.permission.AclEntry> acl =
        org.apache.hadoop.fs.permission.AclEntry.parseAclSpec(
            "user::rwx,group::r--,other::r--,user:foo:rwx",
            true);
    fs.setAcl(p, acl);
    // Reading back via getAclStatus confirms the entries were applied.
    org.apache.hadoop.fs.permission.AclStatus status = fs.getAclStatus(p);
    assertTrue(status.getEntries().stream()
        .anyMatch(e -> "foo".equals(e.getName())),
        "named user 'foo' must be present in ACL entries");
  }

  /** setAcl on a directory. */
  @Test
  @Timeout(60)
  public void setAclOnDirectory() throws Exception {
    Path dir = new Path("/acl-dir");
    fs.mkdirs(dir);
    java.util.List<org.apache.hadoop.fs.permission.AclEntry> acl =
        org.apache.hadoop.fs.permission.AclEntry.parseAclSpec(
            "user::rwx,group::r-x,other::---,user:bar:rwx,default:user::rwx,"
                + "default:group::r-x,default:other::---,default:user:bar:rwx",
            true);
    fs.setAcl(dir, acl);
    org.apache.hadoop.fs.permission.AclStatus status =
        fs.getAclStatus(dir);
    assertTrue(status.getEntries().stream()
        .anyMatch(e -> "bar".equals(e.getName())));
  }

  /** Missing target → FileNotFoundException. */
  @Test
  @Timeout(60)
  public void setAclOnMissingTargetFails() throws Exception {
    java.util.List<org.apache.hadoop.fs.permission.AclEntry> acl =
        org.apache.hadoop.fs.permission.AclEntry.parseAclSpec(
            "user::rwx,group::r--,other::r--", true);
    assertThrows(FileNotFoundException.class,
        () -> fs.setAcl(new Path("/acl-missing"), acl));
  }

  /** Non-owner user cannot setAcl. */
  @Test
  @Timeout(60)
  public void setAclRespectsOwnerCheck() throws Exception {
    Path p = new Path("/acl-perm/file");
    fs.mkdirs(p.getParent());
    try (FSDataOutputStream out = fs.create(p, true, 4096,
        (short) 1, 4096L)) {
      out.write(new byte[8]);
    }

    final UserGroupInformation other =
        UserGroupInformation.createRemoteUser("acl-other-user");
    final URI clusterUri = cluster.getURI();
    final Configuration otherConf = new HdfsConfiguration();
    otherConf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
        IIPBasedFSNamesystemLock.class, FSNLockManager.class);

    final java.util.List<org.apache.hadoop.fs.permission.AclEntry> acl =
        org.apache.hadoop.fs.permission.AclEntry.parseAclSpec(
            "user::rwx,group::r--,other::r--,user:evil:rwx", true);

    other.doAs((PrivilegedExceptionAction<Void>) () -> {
      DistributedFileSystem otherFs = (DistributedFileSystem)
          FileSystem.get(clusterUri, otherConf);
      try {
        assertThrows(AccessControlException.class,
            () -> otherFs.setAcl(p, acl));
      } finally {
        otherFs.close();
      }
      return null;
    });
  }

  // ======================================================================
  // modifyAclEntries / removeAclEntries / removeDefaultAcl / removeAcl
  // pilot (RPCs #13–#16).
  // ======================================================================

  /** modifyAclEntries adds a named-user entry. */
  @Test
  @Timeout(60)
  public void modifyAclEntriesAddsEntry() throws Exception {
    Path p = new Path("/macl-add");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[4]);
    }
    java.util.List<org.apache.hadoop.fs.permission.AclEntry> spec =
        org.apache.hadoop.fs.permission.AclEntry.parseAclSpec(
            "user:alice:rwx", true);
    fs.modifyAclEntries(p, spec);
    org.apache.hadoop.fs.permission.AclStatus s = fs.getAclStatus(p);
    assertTrue(s.getEntries().stream()
        .anyMatch(e -> "alice".equals(e.getName())));
  }

  /** removeAclEntries removes the entry added above. */
  @Test
  @Timeout(60)
  public void removeAclEntriesRemovesEntry() throws Exception {
    Path p = new Path("/racl-rm");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[4]);
    }
    // Add then remove.
    java.util.List<org.apache.hadoop.fs.permission.AclEntry> add =
        org.apache.hadoop.fs.permission.AclEntry.parseAclSpec(
            "user:bob:rwx", true);
    fs.modifyAclEntries(p, add);
    assertTrue(fs.getAclStatus(p).getEntries().stream()
        .anyMatch(e -> "bob".equals(e.getName())));
    java.util.List<org.apache.hadoop.fs.permission.AclEntry> rm =
        org.apache.hadoop.fs.permission.AclEntry.parseAclSpec(
            "user:bob:rwx", true);
    fs.removeAclEntries(p, rm);
    assertFalse(fs.getAclStatus(p).getEntries().stream()
        .anyMatch(e -> "bob".equals(e.getName())));
  }

  /** removeDefaultAcl on a directory with default ACL. */
  @Test
  @Timeout(60)
  public void removeDefaultAclOnDirectory() throws Exception {
    Path dir = new Path("/rda-dir");
    fs.mkdirs(dir);
    java.util.List<org.apache.hadoop.fs.permission.AclEntry> acl =
        org.apache.hadoop.fs.permission.AclEntry.parseAclSpec(
            "user::rwx,group::r-x,other::---,default:user::rwx,"
                + "default:group::r-x,default:other::---,default:user:x:rwx",
            true);
    fs.setAcl(dir, acl);
    fs.removeDefaultAcl(dir);
    org.apache.hadoop.fs.permission.AclStatus s = fs.getAclStatus(dir);
    assertTrue(s.getEntries().stream()
        .noneMatch(e -> e.getScope() ==
            org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT));
  }

  /** removeAcl removes all non-base ACL entries. */
  @Test
  @Timeout(60)
  public void removeAclClearsAllExtendedEntries() throws Exception {
    Path p = new Path("/rmacl-all");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[4]);
    }
    java.util.List<org.apache.hadoop.fs.permission.AclEntry> spec =
        org.apache.hadoop.fs.permission.AclEntry.parseAclSpec(
            "user:charlie:rwx", true);
    fs.modifyAclEntries(p, spec);
    assertTrue(fs.getAclStatus(p).getEntries().size() > 0,
        "precondition: non-empty ACL");
    fs.removeAcl(p);
    assertEquals(0, fs.getAclStatus(p).getEntries().size());
  }

  // ======================================================================
  // xattr cluster (RPCs #17–#20).
  // ======================================================================

  /** setXAttr + getXAttrs round-trip. */
  @Test
  @Timeout(60)
  public void setAndGetXAttr() throws Exception {
    Path p = new Path("/xa-set");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[4]);
    }
    byte[] value = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    fs.setXAttr(p, "user.myattr", value);
    byte[] got = fs.getXAttr(p, "user.myattr");
    assertNotNull(got);
    assertEquals("hello", new String(got,
        java.nio.charset.StandardCharsets.UTF_8));
  }

  /** listXAttrs returns set xattrs. */
  @Test
  @Timeout(60)
  public void listXAttrsReturnsSetAttrs() throws Exception {
    Path p = new Path("/xa-list");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[4]);
    }
    fs.setXAttr(p, "user.a", new byte[]{1});
    fs.setXAttr(p, "user.b", new byte[]{2});
    java.util.Map<String, byte[]> map = fs.getXAttrs(p);
    assertTrue(map.containsKey("user.a"));
    assertTrue(map.containsKey("user.b"));
  }

  /** removeXAttr removes the named attribute. */
  @Test
  @Timeout(60)
  public void removeXAttrRemovesEntry() throws Exception {
    Path p = new Path("/xa-rm");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[4]);
    }
    fs.setXAttr(p, "user.todel", new byte[]{99});
    assertNotNull(fs.getXAttr(p, "user.todel"));
    fs.removeXAttr(p, "user.todel");
    assertThrows(IOException.class,
        () -> fs.getXAttr(p, "user.todel"));
  }

  /** xattr operations on missing target → failure. */
  @Test
  @Timeout(60)
  public void xattrOnMissingTargetFails() throws Exception {
    assertThrows(IOException.class,
        () -> fs.setXAttr(new Path("/xa-missing"), "user.x",
            new byte[]{1}));
    assertThrows(IOException.class,
        () -> fs.getXAttr(new Path("/xa-missing"), "user.x"));
    assertThrows(IOException.class,
        () -> fs.removeXAttr(new Path("/xa-missing"), "user.x"));
  }

  // ======================================================================
  // getStoragePolicy + getPreferredBlockSize + getAclStatus
  // pilot (RPCs #21–#23).
  // ======================================================================

  /** getStoragePolicy round-trip with setStoragePolicy. */
  @Test
  @Timeout(60)
  public void getStoragePolicyOnFile() throws Exception {
    Path p = new Path("/gsp-file");
    try (FSDataOutputStream out = fs.create(p, true, 4096,
        (short) 1, 4096L)) {
      out.write(new byte[4]);
    }
    fs.setStoragePolicy(p, HdfsConstants.HOT_STORAGE_POLICY_NAME);
    org.apache.hadoop.hdfs.protocol.BlockStoragePolicy pol =
        fs.getClient().getStoragePolicy(p.toString());
    assertEquals(HdfsConstants.HOT_STORAGE_POLICY_ID, pol.getId());
  }

  /** getPreferredBlockSize reads the block size set at creation. */
  @Test
  @Timeout(60)
  public void getPreferredBlockSizeOnFile() throws Exception {
    long expected = 8192L;
    Path p = new Path("/gpbs-file");
    try (FSDataOutputStream out = fs.create(p, true, 4096,
        (short) 1, expected)) {
      out.write(new byte[4]);
    }
    long actual = fs.getClient().getBlockSize(p.toString());
    assertEquals(expected, actual);
  }

  /** getAclStatus returns ACL entries set via pilot setAcl. */
  @Test
  @Timeout(60)
  public void getAclStatusOnFile() throws Exception {
    Path p = new Path("/gas-file");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[4]);
    }
    java.util.List<org.apache.hadoop.fs.permission.AclEntry> acl =
        org.apache.hadoop.fs.permission.AclEntry.parseAclSpec(
            "user::rwx,group::r--,other::r--,user:zz:rwx", true);
    fs.setAcl(p, acl);
    org.apache.hadoop.fs.permission.AclStatus status = fs.getAclStatus(p);
    assertTrue(status.getEntries().stream()
        .anyMatch(e -> "zz".equals(e.getName())));
  }

  // ======================================================================
  // truncate pilot (PATH_WRITE + nested BM write + lease).
  // ======================================================================

  /** Happy path: truncate on block boundary. */
  @Test
  @Timeout(60)
  public void truncateOnBlockBoundary() throws Exception {
    int blockSize = 512;
    Path p = new Path("/trunc-boundary");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1,
        blockSize)) {
      out.write(new byte[blockSize * 3]);
    }
    assertTrue(fs.truncate(p, blockSize));
    assertEquals(blockSize, fs.getFileStatus(p).getLen());
  }

  /** Truncate not on block boundary — file goes under construction. */
  @Test
  @Timeout(120)
  public void truncateNotOnBlockBoundary() throws Exception {
    int blockSize = 512;
    Path p = new Path("/trunc-mid");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1,
        blockSize)) {
      out.write(new byte[blockSize * 2]);
    }
    boolean onBoundary = fs.truncate(p, blockSize + 100);
    if (!onBoundary) {
      int retries = 60;
      while (retries-- > 0 && !fs.isFileClosed(p)) {
        Thread.sleep(500);
      }
    }
    assertEquals(blockSize + 100, fs.getFileStatus(p).getLen());
  }

  /** Truncate to same length is a no-op. */
  @Test
  @Timeout(60)
  public void truncateToSameLengthIsNoop() throws Exception {
    Path p = new Path("/trunc-noop");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1,
        512)) {
      out.write(new byte[512]);
    }
    assertTrue(fs.truncate(p, 512));
    assertEquals(512, fs.getFileStatus(p).getLen());
  }

  /** Truncate to larger length fails. */
  @Test
  @Timeout(60)
  public void truncateToLargerLengthFails() throws Exception {
    Path p = new Path("/trunc-larger");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1,
        512)) {
      out.write(new byte[100]);
    }
    assertThrows(IOException.class, () -> fs.truncate(p, 200));
  }

  /** Truncate on missing target fails. */
  @Test
  @Timeout(60)
  public void truncateOnMissingTargetFails() throws Exception {
    assertThrows(IOException.class,
        () -> fs.truncate(new Path("/trunc-missing"), 0));
  }

  // ======================================================================
  // append pilot (PATH_WRITE + nested BM write + lease).
  // ======================================================================

  /** Happy path: append to an existing file. */
  @Test
  @Timeout(60)
  public void appendToExistingFile() throws Exception {
    Path p = new Path("/app-happy");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[64]);
    }
    try (FSDataOutputStream out = fs.append(p)) {
      out.write(new byte[32]);
    }
    assertEquals(96, fs.getFileStatus(p).getLen());
  }

  /** Append to missing path fails. */
  @Test
  @Timeout(60)
  public void appendToMissingPathFails() throws Exception {
    assertThrows(IOException.class, () -> fs.append(new Path("/app-missing")));
  }

  /** Append to a directory fails. */
  @Test
  @Timeout(60)
  public void appendToDirectoryFails() throws Exception {
    Path dir = new Path("/app-dir");
    fs.mkdirs(dir);
    assertThrows(IOException.class, () -> fs.append(dir));
  }

  /** Multiple sequential appends accumulate data. */
  @Test
  @Timeout(60)
  public void multipleSequentialAppends() throws Exception {
    Path p = new Path("/app-multi");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[10]);
    }
    for (int i = 0; i < 5; i++) {
      try (FSDataOutputStream out = fs.append(p)) {
        out.write(new byte[10]);
      }
    }
    assertEquals(60, fs.getFileStatus(p).getLen());
  }

  // ======================================================================
  // delete -r pilot (ANCESTOR_WRITE).
  // ======================================================================

  /** Recursive delete of a non-empty directory tree. */
  @Test
  @Timeout(60)
  public void deleteRecursiveNonEmptyDirectory() throws Exception {
    Path dir = new Path("/delr-nonempty");
    fs.mkdirs(new Path(dir, "sub1/sub2"));
    try (FSDataOutputStream out = fs.create(
        new Path(dir, "sub1/sub2/file"))) {
      out.write(new byte[8]);
    }
    try (FSDataOutputStream out = fs.create(
        new Path(dir, "sub1/file2"))) {
      out.write(new byte[4]);
    }
    assertTrue(fs.delete(dir, true));
    assertFalse(fs.exists(dir));
  }

  /** Recursive delete of a deeply nested tree. */
  @Test
  @Timeout(60)
  public void deleteRecursiveDeeplyNested() throws Exception {
    Path base = new Path("/delr-deep");
    Path leaf = new Path(base, "a/b/c/d/e");
    fs.mkdirs(leaf);
    try (FSDataOutputStream out = fs.create(new Path(leaf, "file"))) {
      out.write(new byte[4]);
    }
    assertTrue(fs.delete(base, true));
    assertFalse(fs.exists(base));
  }

  /** Recursive delete on a single file still works. */
  @Test
  @Timeout(60)
  public void deleteRecursiveOnFileFallsToSingleFilePath() throws Exception {
    Path p = new Path("/delr-file");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[8]);
    }
    assertTrue(fs.delete(p, true));
    assertFalse(fs.exists(p));
  }

  /** Concurrent recursive deletes on disjoint trees. */
  @Test
  @Timeout(120)
  public void parallelRecursiveDeleteDisjointTrees() throws Exception {
    final int numThreads = 8;
    for (int t = 0; t < numThreads; t++) {
      Path d = new Path("/pdelr/t" + t);
      fs.mkdirs(d);
      for (int i = 0; i < 5; i++) {
        try (FSDataOutputStream out = fs.create(
            new Path(d, "f" + i))) {
          out.write(new byte[4]);
        }
      }
    }
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
            if (!fs.delete(new Path("/pdelr/t" + tid), true)) {
              failures.incrementAndGet();
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
      assertFalse(fs.exists(new Path("/pdelr/t" + t)));
    }
  }

  // ======================================================================
  // rename pilot (RENAME_WRITE).
  // ======================================================================

  /** Happy path: rename a file within the same directory. */
  @Test
  @Timeout(60)
  public void renameSameDirectory() throws Exception {
    Path src = new Path("/ren-same/old");
    fs.mkdirs(src.getParent());
    try (FSDataOutputStream out = fs.create(src)) {
      out.write(new byte[8]);
    }
    Path dst = new Path("/ren-same/new");
    fs.rename(src, dst);
    assertFalse(fs.exists(src));
    assertTrue(fs.exists(dst));
    assertEquals(8, fs.getFileStatus(dst).getLen());
  }

  /** Rename across different parent directories. */
  @Test
  @Timeout(60)
  public void renameAcrossDirectories() throws Exception {
    fs.mkdirs(new Path("/ren-cross/a"));
    fs.mkdirs(new Path("/ren-cross/b"));
    Path src = new Path("/ren-cross/a/file");
    try (FSDataOutputStream out = fs.create(src)) {
      out.write(new byte[16]);
    }
    Path dst = new Path("/ren-cross/b/file");
    fs.rename(src, dst);
    assertFalse(fs.exists(src));
    assertTrue(fs.exists(dst));
  }

  /** Rename a directory (with contents). */
  @Test
  @Timeout(60)
  public void renameDirectory() throws Exception {
    Path src = new Path("/ren-dir/old");
    fs.mkdirs(new Path(src, "sub"));
    try (FSDataOutputStream out = fs.create(new Path(src, "sub/f"))) {
      out.write(new byte[4]);
    }
    Path dst = new Path("/ren-dir/new");
    fs.rename(src, dst);
    assertFalse(fs.exists(src));
    assertTrue(fs.exists(new Path(dst, "sub/f")));
  }

  /** Rename missing source fails. */
  @Test
  @Timeout(60)
  public void renameMissingSourceFails() throws Exception {
    fs.mkdirs(new Path("/ren-miss"));
    // rename(src, dst) with missing src returns false in HDFS API
    assertFalse(fs.rename(new Path("/ren-miss/no"), new Path("/ren-miss/x")));
  }

  /** Concurrent renames on disjoint source/dest pairs. */
  @Test
  @Timeout(120)
  public void parallelRenameDisjointPairs() throws Exception {
    final int numThreads = 8;
    for (int t = 0; t < numThreads; t++) {
      fs.mkdirs(new Path("/pren/src" + t));
      try (FSDataOutputStream out = fs.create(
          new Path("/pren/src" + t + "/file"))) {
        out.write(new byte[4]);
      }
      fs.mkdirs(new Path("/pren/dst" + t));
    }
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
            fs.rename(new Path("/pren/src" + tid),
                new Path("/pren/dst" + tid + "/moved"));
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
      assertTrue(fs.exists(new Path("/pren/dst" + t + "/moved/file")));
      assertFalse(fs.exists(new Path("/pren/src" + t)));
    }
  }

  // ======================================================================
  // Write-path hot RPCs: completeFile, fsync, abandonBlock.
  // ======================================================================

  /**
   * completeFile: the full create→write→close cycle exercises the
   * pilot for create (startFile), getBlockLocations, and now
   * completeFile. If completeFile pilot fails, close() would hang.
   */
  @Test
  @Timeout(60)
  public void completeFileOnSimpleWrite() throws Exception {
    Path p = new Path("/cf-simple");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[1024]);
    }
    // File is now closed via completeFile.
    assertTrue(fs.isFileClosed(p));
    assertEquals(1024, fs.getFileStatus(p).getLen());
  }

  /** completeFile with multiple blocks. */
  @Test
  @Timeout(60)
  public void completeFileMultiBlock() throws Exception {
    int blockSize = 512;
    Path p = new Path("/cf-multi");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1,
        blockSize)) {
      out.write(new byte[blockSize * 3]);
    }
    assertTrue(fs.isFileClosed(p));
    assertEquals(blockSize * 3, fs.getFileStatus(p).getLen());
  }

  /** fsync (hflush) on an open file. */
  @Test
  @Timeout(60)
  public void fsyncOnOpenFile() throws Exception {
    Path p = new Path("/fsync-test");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[256]);
      out.hflush();  // triggers fsync RPC
      // File should still be open.
      assertFalse(fs.isFileClosed(p));
      // Data should be visible to readers after hflush.
      assertTrue(fs.getFileStatus(p).getLen() >= 256);
    }
    assertTrue(fs.isFileClosed(p));
  }

  /** Parallel creates + writes + closes on disjoint files. */
  @Test
  @Timeout(120)
  public void parallelWritePathDisjointFiles() throws Exception {
    final int numThreads = 8;
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
            for (int i = 0; i < 5; i++) {
              Path p = new Path("/pwp/t" + tid + "/f" + i);
              fs.mkdirs(p.getParent());
              try (FSDataOutputStream out = fs.create(p)) {
                out.write(new byte[128]);
                out.hflush();
                out.write(new byte[128]);
              }
              if (fs.getFileStatus(p).getLen() != 256) {
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
        f.get(90, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertEquals(0, errors.get());
  }

  // ======================================================================
  // getAdditionalBlock, contentSummary, quotaUsage.
  // ======================================================================

  /**
   * getAdditionalBlock is exercised implicitly by every multi-block
   * write. This test creates a file larger than one block to force
   * at least one addBlock call through the pilot.
   */
  @Test
  @Timeout(60)
  public void getAdditionalBlockViaMultiBlockWrite() throws Exception {
    int blockSize = 512;
    Path p = new Path("/gab-multi");
    try (FSDataOutputStream out = fs.create(p, true, 4096, (short) 1,
        blockSize)) {
      // Write 3 blocks worth of data → 2 addBlock calls.
      out.write(new byte[blockSize * 3]);
    }
    assertEquals(blockSize * 3, fs.getFileStatus(p).getLen());
    LocatedBlocks lbs = fs.getClient().getLocatedBlocks(
        p.toString(), 0L, Long.MAX_VALUE);
    assertEquals(3, lbs.getLocatedBlocks().size());
  }

  /** contentSummary on a directory with files. */
  @Test
  @Timeout(60)
  public void contentSummaryOnDirectory() throws Exception {
    Path dir = new Path("/cs-dir");
    fs.mkdirs(dir);
    for (int i = 0; i < 3; i++) {
      try (FSDataOutputStream out = fs.create(
          new Path(dir, "f" + i), true, 4096, (short) 1, 4096)) {
        out.write(new byte[100]);
      }
    }
    org.apache.hadoop.fs.ContentSummary cs = fs.getContentSummary(dir);
    assertEquals(3, cs.getFileCount());
    assertEquals(1, cs.getDirectoryCount());
    assertTrue(cs.getLength() >= 300);
  }

  /** contentSummary on a single file. */
  @Test
  @Timeout(60)
  public void contentSummaryOnFile() throws Exception {
    Path p = new Path("/cs-file");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[256]);
    }
    org.apache.hadoop.fs.ContentSummary cs = fs.getContentSummary(p);
    assertEquals(1, cs.getFileCount());
    assertEquals(0, cs.getDirectoryCount());
    assertEquals(256, cs.getLength());
  }

  /** quotaUsage on a directory with namespace quota. */
  @Test
  @Timeout(60)
  public void quotaUsageOnDirectoryWithQuota() throws Exception {
    Path dir = new Path("/qu-dir");
    fs.mkdirs(dir);
    fs.setQuota(dir, 100L, HdfsConstants.QUOTA_DONT_SET);
    try (FSDataOutputStream out = fs.create(
        new Path(dir, "file"), true, 4096, (short) 1, 4096)) {
      out.write(new byte[50]);
    }
    org.apache.hadoop.fs.QuotaUsage qu =
        fs.getQuotaUsage(dir);
    assertEquals(100L, qu.getQuota());
    assertTrue(qu.getFileAndDirectoryCount() >= 2);
  }

  /** quotaUsage on a path without quota falls back to contentSummary. */
  @Test
  @Timeout(60)
  public void quotaUsageOnPathWithoutQuota() throws Exception {
    Path p = new Path("/qu-noquota");
    try (FSDataOutputStream out = fs.create(p)) {
      out.write(new byte[64]);
    }
    org.apache.hadoop.fs.QuotaUsage qu = fs.getQuotaUsage(p);
    assertEquals(64, qu.getSpaceConsumed());
  }

  // ======================================================================
  // concat pilot (PATH_WRITE on target).
  // ======================================================================

  /** Concat two source files into a target. */
  @Test
  @Timeout(60)
  public void concatTwoFiles() throws Exception {
    int blockSize = 512;
    Path target = new Path("/concat-target");
    try (FSDataOutputStream out = fs.create(target, true, 4096,
        (short) 1, blockSize)) {
      out.write(new byte[blockSize]);
    }
    Path src1 = new Path("/concat-src1");
    try (FSDataOutputStream out = fs.create(src1, true, 4096,
        (short) 1, blockSize)) {
      out.write(new byte[blockSize]);
    }
    Path src2 = new Path("/concat-src2");
    try (FSDataOutputStream out = fs.create(src2, true, 4096,
        (short) 1, blockSize)) {
      out.write(new byte[blockSize]);
    }
    fs.concat(target, new Path[]{src1, src2});
    assertEquals(blockSize * 3, fs.getFileStatus(target).getLen());
    assertFalse(fs.exists(src1));
    assertFalse(fs.exists(src2));
  }
}

