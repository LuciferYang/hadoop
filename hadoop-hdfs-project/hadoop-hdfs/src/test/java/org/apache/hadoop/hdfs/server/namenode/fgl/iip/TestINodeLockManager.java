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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link INodeLockManager}. Build a small INode tree in
 * memory, exercise the hand-over-hand walk across several modes and
 * failure paths, and verify lock state is consistent after every case.
 *
 * <p>Tree used by most tests (INode IDs in parentheses):
 *
 * <pre>
 * /(1)
 * ├─ a(2) [dir]
 * │  ├─ b(3) [dir]
 * │  │  └─ c(4) [file]
 * │  └─ d(5) [file]
 * └─ e(6) [file]
 * </pre>
 */
public class TestINodeLockManager {

  private static final PermissionStatus PERM = new PermissionStatus(
      "test", "test", new FsPermission((short) 0755));

  private LockPool pool;
  private ReentrantReadWriteLock compat;
  private INodeLockManager mgr;

  private INodeDirectory root;
  private INodeDirectory a;
  private INodeDirectory b;
  private INodeFile c;
  private INodeFile d;
  private INodeFile e;

  @BeforeEach
  public void setUp() {
    pool = new LockPool();
    compat = new ReentrantReadWriteLock(true);
    mgr = new INodeLockManager(pool, compat, Duration.ofSeconds(5));

    root = dir(1L, "");
    a = dir(2L, "a");
    b = dir(3L, "b");
    c = file(4L, "c");
    d = file(5L, "d");
    e = file(6L, "e");

    root.addChild(a);
    root.addChild(e);
    a.addChild(b);
    a.addChild(d);
    b.addChild(c);

    mgr.setRootDir(root);
  }

  @AfterEach
  public void tearDown() {
    // Defensive: ensure the depth counter is zero between tests so
    // one failing test does not poison the next.
    INodeLockManager.HELD_IIP_DEPTH.set(0);
  }

  private static INodeDirectory dir(long id, String name) {
    return new INodeDirectory(id, name.getBytes(), PERM, 0L);
  }

  private static INodeFile file(long id, String name) {
    return new INodeFile(id, name.getBytes(), PERM, 0L, 0L, null,
        (short) 3, 1024L);
  }

  // -------- PATH_READ walks --------

  @Test
  @Timeout(10)
  public void pathReadOnRootOnly() throws Exception {
    try (LockedIIP lip = mgr.acquire("/", IIPAcquireMode.PATH_READ)) {
      INodesInPath iip = lip.iip();
      assertNotNull(iip);
      assertEquals(1, iip.length());
      assertEquals(root, iip.getINode(0));
      assertEquals(1, pool.size(), "root should be pinned");
      assertTrue(compat.getReadHoldCount() >= 1);
      assertEquals(Integer.valueOf(1),
          INodeLockManager.HELD_IIP_DEPTH.get());
    }
    assertEquals(0, pool.size());
    assertEquals(0, compat.getReadHoldCount());
    assertEquals(Integer.valueOf(0), INodeLockManager.HELD_IIP_DEPTH.get());
  }

  @Test
  @Timeout(10)
  public void pathReadOnDeepFile() throws Exception {
    try (LockedIIP lip = mgr.acquire("/a/b/c", IIPAcquireMode.PATH_READ)) {
      INodesInPath iip = lip.iip();
      assertEquals(4, iip.length());
      assertEquals(root, iip.getINode(0));
      assertEquals(a, iip.getINode(1));
      assertEquals(b, iip.getINode(2));
      assertEquals(c, iip.getINode(3));
      assertEquals(4, pool.size(), "4 locks pinned: root, a, b, c");
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void pathReadOnShallowFile() throws Exception {
    try (LockedIIP lip = mgr.acquire("/e", IIPAcquireMode.PATH_READ)) {
      INodesInPath iip = lip.iip();
      assertEquals(2, iip.length());
      assertEquals(root, iip.getINode(0));
      assertEquals(e, iip.getINode(1));
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void pathReadOnDirectory() throws Exception {
    try (LockedIIP lip = mgr.acquire("/a/b", IIPAcquireMode.PATH_READ)) {
      INodesInPath iip = lip.iip();
      assertEquals(3, iip.length());
      assertEquals(b, iip.getINode(2));
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void pathReadOnMissingTarget() throws Exception {
    try (LockedIIP lip = mgr.acquire("/a/b/nonexistent",
        IIPAcquireMode.PATH_READ)) {
      INodesInPath iip = lip.iip();
      assertEquals(4, iip.length());
      assertEquals(root, iip.getINode(0));
      assertEquals(a, iip.getINode(1));
      assertEquals(b, iip.getINode(2));
      assertNull(iip.getINode(3), "missing tail should be null");
      assertEquals(3, pool.size(), "only 3 existing ancestors pinned");
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void pathReadOnMissingAncestor() throws Exception {
    try (LockedIIP lip = mgr.acquire("/nope/gone/away",
        IIPAcquireMode.PATH_READ)) {
      INodesInPath iip = lip.iip();
      assertEquals(4, iip.length());
      assertEquals(root, iip.getINode(0));
      assertNull(iip.getINode(1));
      assertNull(iip.getINode(2));
      assertNull(iip.getINode(3));
      assertEquals(1, pool.size(), "only root should be pinned");
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void pathReadThroughNonDirectoryAncestorStops() throws Exception {
    // /e is a file; /e/child is not a valid path — walk stops at e.
    try (LockedIIP lip = mgr.acquire("/e/child", IIPAcquireMode.PATH_READ)) {
      INodesInPath iip = lip.iip();
      assertEquals(3, iip.length());
      assertEquals(root, iip.getINode(0));
      assertEquals(e, iip.getINode(1));
      assertNull(iip.getINode(2),
          "walk must not attempt getChild on a non-directory");
      assertEquals(2, pool.size(), "root and e pinned; nothing beyond");
    }
    assertEquals(0, pool.size());
  }

  // -------- PARENT_WRITE walks --------

  @Test
  @Timeout(10)
  public void parentWriteOnCreateTargetAbsent() throws Exception {
    try (LockedIIP lip = mgr.acquire("/a/b/newchild",
        IIPAcquireMode.PARENT_WRITE)) {
      INodesInPath iip = lip.iip();
      assertEquals(4, iip.length());
      // Ancestors populated; target absent.
      assertEquals(root, iip.getINode(0));
      assertEquals(a, iip.getINode(1));
      assertEquals(b, iip.getINode(2));
      assertNull(iip.getINode(3));
      // 3 locks held: read root, read a, write b.
      assertEquals(3, pool.size());
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void parentWriteTargetAlsoPopulatedWhenExists() throws Exception {
    // If the target already exists, PARENT_WRITE still works but does
    // not lock the target — only the parent. The IIP will have the
    // target populated (it was discovered during the walk) but the
    // lock count is still "depth - 1" (no target lock).
    try (LockedIIP lip = mgr.acquire("/a/b/c",
        IIPAcquireMode.PARENT_WRITE)) {
      INodesInPath iip = lip.iip();
      assertEquals(4, iip.length());
      assertEquals(c, iip.getINode(3), "existing target still populated");
      // 3 locks: read root, read a, write b. c is NOT locked.
      assertEquals(3, pool.size());
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void parentWriteOnRootRejected() {
    assertThrows(InvalidPathException.class,
        () -> mgr.acquire("/", IIPAcquireMode.PARENT_WRITE));
    assertEquals(0, pool.size(), "no locks should leak on rejection");
    assertEquals(0, compat.getReadHoldCount(),
        "compat-read should not leak on rejection");
    assertEquals(Integer.valueOf(0), INodeLockManager.HELD_IIP_DEPTH.get());
  }

  // -------- PATH_WRITE walks --------

  @Test
  @Timeout(10)
  public void pathWriteOnShallowTarget() throws Exception {
    // /e (file directly under root). PATH_WRITE locks root read + e write.
    try (LockedIIP lip = mgr.acquire("/e", IIPAcquireMode.PATH_WRITE)) {
      INodesInPath iip = lip.iip();
      assertEquals(2, iip.length());
      assertEquals(root, iip.getINode(0));
      assertEquals(e, iip.getINode(1));
      // 2 locks: read root, write e.
      assertEquals(2, pool.size());
      // HELD_IIP_WRITE must be true so FSNamesystem.hasWriteLock(FS)
      // assertions pass for PATH_WRITE callers.
      assertTrue(INodeLockManager.HELD_IIP_WRITE.get(),
          "HELD_IIP_WRITE must be set under PATH_WRITE");
    }
    assertEquals(0, pool.size());
    assertFalse(INodeLockManager.HELD_IIP_WRITE.get(),
        "HELD_IIP_WRITE must be cleared after close");
  }

  @Test
  @Timeout(10)
  public void pathWriteOnDeepTarget() throws Exception {
    // /a/b/c (file 3 levels deep). PATH_WRITE locks root read + a read
    // + b read + c write.
    try (LockedIIP lip = mgr.acquire("/a/b/c", IIPAcquireMode.PATH_WRITE)) {
      INodesInPath iip = lip.iip();
      assertEquals(4, iip.length());
      assertEquals(root, iip.getINode(0));
      assertEquals(a, iip.getINode(1));
      assertEquals(b, iip.getINode(2));
      assertEquals(c, iip.getINode(3));
      // 4 locks: read root, read a, read b, write c.
      assertEquals(4, pool.size());
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void pathWriteOnTargetDirectory() throws Exception {
    // PATH_WRITE on a directory target (e.g., /a/b). Locks root read,
    // a read, b write. Useful for setPermission / setOwner on dirs.
    try (LockedIIP lip = mgr.acquire("/a/b", IIPAcquireMode.PATH_WRITE)) {
      INodesInPath iip = lip.iip();
      assertEquals(3, iip.length());
      assertEquals(b, iip.getINode(2));
      assertEquals(3, pool.size());
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void pathWriteOnMissingTargetLeavesNullInIIP() throws Exception {
    // If the target does not exist, the walk stops early and the IIP
    // has null at the last slot. Caller must handle absent-target
    // semantics (e.g., throw FileNotFoundException).
    try (LockedIIP lip = mgr.acquire("/a/missing",
        IIPAcquireMode.PATH_WRITE)) {
      INodesInPath iip = lip.iip();
      assertEquals(3, iip.length());
      assertEquals(root, iip.getINode(0));
      assertEquals(a, iip.getINode(1));
      assertNull(iip.getINode(2));
      // Only root and a acquired; no lock on the missing target.
      assertEquals(2, pool.size());
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void pathWriteOnRootRejected() {
    assertThrows(InvalidPathException.class,
        () -> mgr.acquire("/", IIPAcquireMode.PATH_WRITE));
    assertEquals(0, pool.size(), "no locks should leak on rejection");
    assertEquals(0, compat.getReadHoldCount(),
        "compat-read should not leak on rejection");
    assertEquals(Integer.valueOf(0), INodeLockManager.HELD_IIP_DEPTH.get());
  }

  @Test
  @Timeout(10)
  public void computeMaxLockDepthPathWrite() {
    // "/a/b" → length 3 → target at idx 2 (write).
    assertEquals(2,
        INodeLockManager.computeMaxLockDepth(IIPAcquireMode.PATH_WRITE, 3));
    // "/a" → length 2 → target at idx 1.
    assertEquals(1,
        INodeLockManager.computeMaxLockDepth(IIPAcquireMode.PATH_WRITE, 2));
  }

  @Test
  @Timeout(10)
  public void computeMaxLockDepthPathWriteOnRootIsInvalid() {
    // "/" → length 1 → no valid target for PATH_WRITE.
    assertEquals(INodeLockManager.INVALID_WRITE_DEPTH,
        INodeLockManager.computeMaxLockDepth(IIPAcquireMode.PATH_WRITE, 1));
  }

  // -------- Mode validation --------

  @Test
  @Timeout(10)
  public void deferredModeThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> mgr.acquire("/a", IIPAcquireMode.RENAME_WRITE));
  }

  @Test
  @Timeout(10)
  public void fallbackModeRejectedByPilotCheck() {
    assertThrows(UnsupportedOperationException.class,
        () -> mgr.acquire("/a", IIPAcquireMode.GLOBAL_READ));
    assertThrows(UnsupportedOperationException.class,
        () -> mgr.acquire("/a", IIPAcquireMode.ADMIN_META));
  }

  @Test
  @Timeout(10)
  public void nullModeRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> mgr.acquire("/a", null));
  }

  @Test
  @Timeout(10)
  public void rootDirUnsetThrows() {
    INodeLockManager bare =
        new INodeLockManager(pool, compat, Duration.ofSeconds(1));
    assertThrows(IllegalStateException.class,
        () -> bare.acquire("/a", IIPAcquireMode.PATH_READ));
  }

  // -------- Nested acquisition rejection --------

  @Test
  @Timeout(10)
  public void nestedAcquireOnSameThreadRejected() throws Exception {
    try (LockedIIP ignored = mgr.acquire("/a", IIPAcquireMode.PATH_READ)) {
      assertThrows(IllegalStateException.class,
          () -> mgr.acquire("/a/b", IIPAcquireMode.PATH_READ));
    }
    assertEquals(0, pool.size());
    assertEquals(Integer.valueOf(0), INodeLockManager.HELD_IIP_DEPTH.get());
  }

  // -------- computeMaxLockDepth unit cases --------

  @Test
  @Timeout(10)
  public void computeMaxLockDepthPathRead() {
    // PATH_READ locks every level including the target.
    assertEquals(3,
        INodeLockManager.computeMaxLockDepth(IIPAcquireMode.PATH_READ, 4));
    assertEquals(0,
        INodeLockManager.computeMaxLockDepth(IIPAcquireMode.PATH_READ, 1));
  }

  @Test
  @Timeout(10)
  public void computeMaxLockDepthParentWriteNormalPath() {
    // "/a/b" → components=["", "a", "b"] length 3 → parent at idx 1
    assertEquals(1,
        INodeLockManager.computeMaxLockDepth(IIPAcquireMode.PARENT_WRITE, 3));
    // "/a/b/newfile" → length 4 → parent at idx 2
    assertEquals(2,
        INodeLockManager.computeMaxLockDepth(IIPAcquireMode.PARENT_WRITE, 4));
  }

  @Test
  @Timeout(10)
  public void computeMaxLockDepthParentWriteOnRootIsInvalid() {
    // "/" → length 1 → no parent.
    assertEquals(INodeLockManager.INVALID_WRITE_DEPTH,
        INodeLockManager.computeMaxLockDepth(IIPAcquireMode.PARENT_WRITE, 1));
  }

  // -------- Failure paths leave no state leak --------

  @Test
  @Timeout(10)
  public void unsupportedModeLeavesCompatLockUntouched() {
    assertThrows(UnsupportedOperationException.class,
        () -> mgr.acquire("/a", IIPAcquireMode.RENAME_WRITE));
    assertEquals(0, compat.getReadHoldCount(),
        "compat-read must not be acquired before mode validation");
    assertEquals(0, pool.size());
    assertEquals(Integer.valueOf(0), INodeLockManager.HELD_IIP_DEPTH.get());
  }

  // -------- Concurrent acquisitions on disjoint paths --------

  @Test
  @Timeout(30)
  public void concurrentReadsOnDisjointPaths() throws Exception {
    final int threads = 8;
    final int iterations = 500;
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicBoolean error = new AtomicBoolean(false);

    ExecutorService exec = Executors.newFixedThreadPool(threads);
    try {
      Future<?>[] futures = new Future<?>[threads];
      for (int t = 0; t < threads; t++) {
        final int tid = t;
        futures[t] = exec.submit(() -> {
          start.await();
          String path = (tid % 2 == 0) ? "/a/b/c" : "/e";
          for (int i = 0; i < iterations; i++) {
            try (LockedIIP lip = mgr.acquire(path, IIPAcquireMode.PATH_READ)) {
              // Briefly use the handle.
              assertNotNull(lip.iip().getLastINode());
            } catch (Exception ex) {
              error.set(true);
              throw new RuntimeException(ex);
            }
          }
          return null;
        });
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(25, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }
    assertFalse(error.get());
    assertEquals(0, pool.size());
    assertEquals(0, compat.getReadHoldCount());
  }

  // -------- Write/read exclusion on the same parent --------

  @Test
  @Timeout(15)
  public void writerBlocksReaderOnSameParent() throws Exception {
    // Thread 1 takes PARENT_WRITE on /a (locks root read, a write).
    // Thread 2 attempts PATH_READ on /a/b with a short timeout —
    // should time out because thread 1 has /a in write mode.
    final CountDownLatch writerReady = new CountDownLatch(1);
    final CountDownLatch writerRelease = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(2);
    try {
      Future<?> writer = exec.submit(() -> {
        try (LockedIIP ignored = mgr.acquire("/a/b",
            IIPAcquireMode.PARENT_WRITE)) {
          writerReady.countDown();
          writerRelease.await();
        }
        return null;
      });
      assertTrue(writerReady.await(5, TimeUnit.SECONDS));

      Future<?> reader = exec.submit(() -> {
        // Short timeout: reader expects to fail.
        return assertThrows(LockAcquisitionTimeoutException.class,
            () -> mgr.acquire("/a/b/c", IIPAcquireMode.PATH_READ,
                Duration.ofMillis(200)));
      });
      reader.get(5, TimeUnit.SECONDS);

      writerRelease.countDown();
      writer.get(5, TimeUnit.SECONDS);
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }

    // After both threads finish, state should be clean.
    assertEquals(0, pool.size());
    assertEquals(0, compat.getReadHoldCount());
  }

  // -------- Compat-write blocks all IIP acquires --------

  @Test
  @Timeout(10)
  public void compatWriteBlocksAcquire() throws Exception {
    compat.writeLock().lock();
    try {
      // With compat-write held by this thread, a (different-thread)
      // acquire must time out. Use a brief deadline.
      ExecutorService exec = Executors.newSingleThreadExecutor();
      try {
        Future<?> f = exec.submit(() ->
            assertThrows(LockAcquisitionTimeoutException.class,
                () -> mgr.acquire("/a", IIPAcquireMode.PATH_READ,
                    Duration.ofMillis(100))));
        f.get(5, TimeUnit.SECONDS);
      } finally {
        exec.shutdown();
        assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
      }
    } finally {
      compat.writeLock().unlock();
    }
    assertEquals(0, pool.size());
  }
}
