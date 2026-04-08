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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.fgl.FSNLockManager;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Integration tests for {@link IIPBasedFSNamesystemLock}. Exercises:
 *
 * <ul>
 *   <li>Construction via the same signature FSNamesystem's reflection
 *       factory uses.</li>
 *   <li>{@link FSNLockManager#lockPath} dispatch to
 *       {@link INodeLockManager}.</li>
 *   <li>{@code hasWriteLock} / {@code hasReadLock} override returning
 *       {@code true} from inside a LockedIIP context.</li>
 *   <li>Delegation of all legacy interface methods to the composed
 *       Phase I lock manager (smoke test).</li>
 *   <li>Un-migrated writer holding Phase I's FSLock write blocks a
 *       concurrent IIP acquisition (compat-write → excludes
 *       compat-read).</li>
 * </ul>
 */
public class TestIIPBasedFSNamesystemLock {

  private static final PermissionStatus PERM = new PermissionStatus(
      "test", "test", new FsPermission((short) 0755));

  private IIPBasedFSNamesystemLock lock;
  private INodeDirectory root;

  @BeforeEach
  public void setUp() {
    Configuration conf = new Configuration();
    MutableRatesWithAggregation agg =
        new MetricsRegistry("test").newRatesWithAggregation("test");
    lock = new IIPBasedFSNamesystemLock(conf, agg);

    // Build a small tree: /(1) with child a(2) which has child b(3).
    root = new INodeDirectory(1L, "".getBytes(), PERM, 0L);
    INodeDirectory a = new INodeDirectory(2L, "a".getBytes(), PERM, 0L);
    INodeFile b = new INodeFile(3L, "b".getBytes(), PERM, 0L, 0L, null,
        (short) 3, 1024L);
    root.addChild(a);
    a.addChild(b);

    lock.setRootDir(root);
  }

  @AfterEach
  public void tearDown() {
    // Clean up ThreadLocal state so one test cannot poison the next.
    INodeLockManager.HELD_IIP_DEPTH.set(0);
    INodeLockManager.HELD_IIP_WRITE.set(Boolean.FALSE);
    DefaultMetricsSystem.shutdown();
  }

  // ----- Construction -----

  @Test
  @Timeout(10)
  public void constructorMatchesReflectionFactorySignature() throws Exception {
    // Verify the constructor is discoverable by the same reflection
    // pattern FSNamesystem#createLock uses.
    Class<IIPBasedFSNamesystemLock> klass = IIPBasedFSNamesystemLock.class;
    klass.getDeclaredConstructor(Configuration.class,
        MutableRatesWithAggregation.class);
    // If we reach here without NoSuchMethodException, the signature
    // is correct.
  }

  @Test
  @Timeout(10)
  public void fallbackIsComposedNotInherited() {
    assertNotNull(lock.fallback(),
        "IIPBased must compose (not inherit) FineGrainedFSNamesystemLock");
    assertNotNull(lock.inodeLockManager(),
        "internal INodeLockManager must be present");
  }

  // ----- lockPath dispatch -----

  @Test
  @Timeout(10)
  public void lockPathPathReadReturnsLockedIIP() throws Exception {
    try (LockedIIP lip = lock.lockPath("/a/b", IIPAcquireMode.PATH_READ)) {
      assertNotNull(lip);
      assertNotNull(lip.iip());
      assertEquals(3, lip.iip().length());
      assertEquals(Integer.valueOf(1),
          INodeLockManager.HELD_IIP_DEPTH.get());
    }
    assertEquals(Integer.valueOf(0), INodeLockManager.HELD_IIP_DEPTH.get());
  }

  @Test
  @Timeout(10)
  public void lockPathParentWriteSetsHeldIIPWrite() throws Exception {
    assertFalse(INodeLockManager.HELD_IIP_WRITE.get(),
        "precondition: no write flag set");
    try (LockedIIP lip = lock.lockPath("/a/newfile",
        IIPAcquireMode.PARENT_WRITE)) {
      assertNotNull(lip);
      assertTrue(INodeLockManager.HELD_IIP_WRITE.get(),
          "PARENT_WRITE acquisition should set HELD_IIP_WRITE");
    }
    assertFalse(INodeLockManager.HELD_IIP_WRITE.get(),
        "close() should reset HELD_IIP_WRITE");
  }

  @Test
  @Timeout(10)
  public void lockPathPathReadDoesNotSetHeldIIPWrite() throws Exception {
    assertFalse(INodeLockManager.HELD_IIP_WRITE.get());
    try (LockedIIP lip = lock.lockPath("/a/b", IIPAcquireMode.PATH_READ)) {
      assertFalse(INodeLockManager.HELD_IIP_WRITE.get(),
          "PATH_READ must not set the write flag");
    }
    assertFalse(INodeLockManager.HELD_IIP_WRITE.get());
  }

  // ----- hasWriteLock / hasReadLock override -----

  @Test
  @Timeout(10)
  public void hasReadLockTrueUnderPathRead() throws Exception {
    // Outside any context, both false.
    assertFalse(lock.hasReadLock(RwLockMode.FS));
    assertFalse(lock.hasWriteLock(RwLockMode.FS));

    try (LockedIIP lip = lock.lockPath("/a/b", IIPAcquireMode.PATH_READ)) {
      // Inside PATH_READ, the override should return true for
      // hasReadLock(FS) and hasReadLock(GLOBAL). hasWriteLock must
      // still be false because we don't hold any write.
      assertTrue(lock.hasReadLock(RwLockMode.FS),
          "hasReadLock(FS) must be true inside PATH_READ context");
      assertTrue(lock.hasReadLock(RwLockMode.GLOBAL),
          "hasReadLock(GLOBAL) must be true inside PATH_READ context");
      assertFalse(lock.hasWriteLock(RwLockMode.FS),
          "hasWriteLock(FS) must be false under a read-only mode");
      assertFalse(lock.hasWriteLock(RwLockMode.GLOBAL),
          "hasWriteLock(GLOBAL) must be false under a read-only mode");
    }
    // After close, both false again.
    assertFalse(lock.hasReadLock(RwLockMode.FS));
    assertFalse(lock.hasWriteLock(RwLockMode.FS));
  }

  @Test
  @Timeout(10)
  public void hasWriteLockTrueUnderParentWrite() throws Exception {
    assertFalse(lock.hasWriteLock(RwLockMode.FS));

    try (LockedIIP lip = lock.lockPath("/a/newfile",
        IIPAcquireMode.PARENT_WRITE)) {
      assertTrue(lock.hasWriteLock(RwLockMode.FS),
          "hasWriteLock(FS) must be true inside PARENT_WRITE context");
      assertTrue(lock.hasWriteLock(RwLockMode.GLOBAL),
          "hasWriteLock(GLOBAL) must be true inside PARENT_WRITE context");
      // hasReadLock is also true because write implies read for
      // assertions.
      assertTrue(lock.hasReadLock(RwLockMode.FS));
      assertTrue(lock.hasReadLock(RwLockMode.GLOBAL));
    }

    assertFalse(lock.hasWriteLock(RwLockMode.FS));
    assertFalse(lock.hasReadLock(RwLockMode.FS));
  }

  @Test
  @Timeout(10)
  public void hasBMLockStillDelegates() throws Exception {
    // BM locks are untouched by IIP. hasReadLock(BM) / hasWriteLock(BM)
    // reflect Phase I's BMLock state only.
    assertFalse(lock.hasWriteLock(RwLockMode.BM));
    assertFalse(lock.hasReadLock(RwLockMode.BM));

    // Holding an IIP lock must NOT make hasReadLock(BM) true.
    try (LockedIIP lip = lock.lockPath("/a/b", IIPAcquireMode.PATH_READ)) {
      assertFalse(lock.hasReadLock(RwLockMode.BM),
          "IIP context must not affect BM lock status");
      assertFalse(lock.hasWriteLock(RwLockMode.BM));
    }
  }

  @Test
  @Timeout(10)
  public void hasLockRespectsActualFallbackState() throws Exception {
    // Actually acquiring Phase I's FS write lock via the fallback
    // path should make hasWriteLock(FS) true even without any IIP
    // context.
    lock.writeLock(RwLockMode.FS);
    try {
      assertTrue(lock.hasWriteLock(RwLockMode.FS));
      assertTrue(lock.hasReadLock(RwLockMode.FS));
    } finally {
      lock.writeUnlock(RwLockMode.FS, "test");
    }
    assertFalse(lock.hasWriteLock(RwLockMode.FS));
    assertFalse(lock.hasReadLock(RwLockMode.FS));
  }

  // ----- Compat-write blocks IIP acquires -----

  @Test
  @Timeout(15)
  public void phaseOneWriteBlocksIIPAcquire() throws Exception {
    // A different thread acquires Phase I's FSLock write (un-migrated
    // writer path). A concurrent IIP acquire should time out because
    // compat-read and Phase I FSLock-write are the SAME underlying
    // lock — that's the point of the composition.
    final CountDownLatch writerHolding = new CountDownLatch(1);
    final CountDownLatch writerRelease = new CountDownLatch(1);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      exec.submit(() -> {
        lock.writeLock(RwLockMode.FS);
        try {
          writerHolding.countDown();
          writerRelease.await();
        } finally {
          lock.writeUnlock(RwLockMode.FS, "test");
        }
        return null;
      });

      assertTrue(writerHolding.await(5, TimeUnit.SECONDS));

      // IIP acquire should time out because compat-read can't be
      // taken while the writer holds the same RRWL in write mode.
      assertThrows(LockAcquisitionTimeoutException.class,
          () -> lock.inodeLockManager().acquire("/a/b",
              IIPAcquireMode.PATH_READ, java.time.Duration.ofMillis(200)));

      writerRelease.countDown();
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }

    // After writer releases, IIP acquire should succeed.
    try (LockedIIP lip = lock.lockPath("/a/b", IIPAcquireMode.PATH_READ)) {
      assertNotNull(lip);
    }
  }

  // ----- Interface delegation smoke test -----

  @Test
  @Timeout(10)
  public void legacyMethodsDelegateWithoutException() {
    // Spot-check that the delegating overrides don't throw. Full
    // legacy-method semantics are tested in TestFineGrainedFSNamesystemLock.
    lock.readLock(RwLockMode.FS);
    lock.readUnlock(RwLockMode.FS, "test");
    lock.writeLock(RwLockMode.FS);
    lock.writeUnlock(RwLockMode.FS, "test");

    lock.setMetricsEnabled(true);
    assertTrue(lock.isMetricsEnabled());
    lock.setMetricsEnabled(false);

    lock.setReadLockReportingThresholdMs(1000L);
    assertEquals(1000L, lock.getReadLockReportingThresholdMs());
    lock.setWriteLockReportingThresholdMs(2000L);
    assertEquals(2000L, lock.getWriteLockReportingThresholdMs());

    // Queue length / hold counts are informational; just make sure
    // they return something valid.
    lock.getReadHoldCount(RwLockMode.FS);
    lock.getQueueLength(RwLockMode.FS);
    lock.getNumOfReadLockLongHold(RwLockMode.FS);
    lock.getNumOfWriteLockLongHold(RwLockMode.FS);
  }

  // ----- setLockForTests / getLockForTests delegation -----

  @Test
  @Timeout(10)
  public void lockForTestsDelegatesToFallback() {
    // FineGrainedFSNamesystemLock throws UnsupportedOperationException
    // for these; IIPBased should propagate (it just delegates).
    assertThrows(UnsupportedOperationException.class,
        () -> lock.getLockForTests());
    assertThrows(UnsupportedOperationException.class,
        () -> lock.setLockForTests(null));
  }
}
