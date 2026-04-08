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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link LockPool}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Basic pin/unpin atomicity on a single key.</li>
 *   <li>Eviction when the reference count drops to zero.</li>
 *   <li>Rebuilding an entry after eviction returns a NEW lock instance.</li>
 *   <li>Multiple concurrent pins on the same key share a single entry.</li>
 *   <li>Concurrent pin/unpin stress on disjoint keys.</li>
 *   <li>Concurrent pin/unpin stress on the same key (hot contention).</li>
 *   <li>Unpin of an unknown key throws {@link IllegalStateException}.</li>
 *   <li>The {@code size()} gauge tracks pin/unpin accurately.</li>
 * </ul>
 */
public class TestLockPool {

  @Test
  @Timeout(10)
  public void pinCreatesEntryWithRefCountOne() {
    LockPool pool = new LockPool();
    assertEquals(0, pool.size());

    LockPool.Entry e = pool.pin(42L);
    assertNotNull(e);
    assertNotNull(e.lock);
    assertEquals(1, pool.refCount(42L));
    assertEquals(1, pool.size());
    assertTrue(pool.contains(42L));
  }

  @Test
  @Timeout(10)
  public void unpinDecrementsAndEvictsAtZero() {
    LockPool pool = new LockPool();
    pool.pin(42L);
    assertEquals(1, pool.refCount(42L));

    pool.unpin(42L);
    assertEquals(0, pool.refCount(42L));
    assertEquals(0, pool.size());
    assertFalse(pool.contains(42L));
  }

  @Test
  @Timeout(10)
  public void multiplePinsShareSingleEntry() {
    LockPool pool = new LockPool();
    LockPool.Entry first = pool.pin(42L);
    LockPool.Entry second = pool.pin(42L);
    LockPool.Entry third = pool.pin(42L);

    assertSame(first, second);
    assertSame(second, third);
    assertEquals(3, pool.refCount(42L));
    assertEquals(1, pool.size());
  }

  @Test
  @Timeout(10)
  public void unpinToZeroThenPinCreatesFreshEntry() {
    LockPool pool = new LockPool();
    LockPool.Entry first = pool.pin(42L);
    pool.unpin(42L);
    assertEquals(0, pool.size());

    LockPool.Entry second = pool.pin(42L);
    assertNotNull(second);
    assertNotSame(first, second,
        "after eviction, a new pin should return a fresh Entry instance");
    assertEquals(1, pool.refCount(42L));
  }

  @Test
  @Timeout(10)
  public void unpinOfUnknownKeyThrows() {
    LockPool pool = new LockPool();
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> pool.unpin(999L));
    assertTrue(ex.getMessage().contains("999"),
        "error message should name the offending INode id");
  }

  @Test
  @Timeout(10)
  public void unpinBelowZeroThrowsAfterBalancedRelease() {
    LockPool pool = new LockPool();
    pool.pin(42L);
    pool.unpin(42L);
    assertThrows(IllegalStateException.class, () -> pool.unpin(42L));
  }

  @Test
  @Timeout(10)
  public void disjointKeysDoNotInterfere() {
    LockPool pool = new LockPool();
    pool.pin(1L);
    pool.pin(2L);
    pool.pin(3L);
    assertEquals(3, pool.size());
    assertEquals(1, pool.refCount(1L));
    assertEquals(1, pool.refCount(2L));
    assertEquals(1, pool.refCount(3L));

    pool.unpin(2L);
    assertEquals(2, pool.size());
    assertFalse(pool.contains(2L));
    assertTrue(pool.contains(1L));
    assertTrue(pool.contains(3L));
  }

  @Test
  @Timeout(30)
  public void concurrentPinUnpinOnSameKeyConvergesToZero() throws Exception {
    final LockPool pool = new LockPool();
    final int numThreads = 16;
    final int opsPerThread = 5_000;
    final long inodeId = 42L;
    final CountDownLatch start = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < numThreads; i++) {
        futures.add(exec.submit(() -> {
          start.await();
          for (int j = 0; j < opsPerThread; j++) {
            pool.pin(inodeId);
            pool.unpin(inodeId);
          }
          return null;
        }));
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(20, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }
    assertEquals(0, pool.size(),
        "after balanced pin/unpin on a single key, pool should be empty");
    assertEquals(0, pool.refCount(inodeId));
  }

  @Test
  @Timeout(30)
  public void concurrentPinUnpinOnDisjointKeysConvergesToZero() throws Exception {
    final LockPool pool = new LockPool();
    final int numThreads = 16;
    final int opsPerThread = 5_000;
    final CountDownLatch start = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < numThreads; t++) {
        final long baseId = (long) t * opsPerThread;
        futures.add(exec.submit(() -> {
          start.await();
          for (int j = 0; j < opsPerThread; j++) {
            long id = baseId + j;
            pool.pin(id);
            pool.unpin(id);
          }
          return null;
        }));
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(20, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(30)
  public void concurrentMixedWorkloadSameKeyCountsCorrectly() throws Exception {
    // Half the threads hold a pin for longer, half churn rapidly.
    // Verifies that the entry stays alive under held pins.
    final LockPool pool = new LockPool();
    final int holders = 4;
    final int churners = 4;
    final int churnOps = 1_000;
    final long inodeId = 123L;
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch holdReady = new CountDownLatch(holders);
    final CountDownLatch churnDone = new CountDownLatch(churners);
    ExecutorService exec = Executors.newFixedThreadPool(holders + churners);
    try {
      // Holders: pin, wait, unpin at the end.
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < holders; i++) {
        futures.add(exec.submit(() -> {
          start.await();
          pool.pin(inodeId);
          holdReady.countDown();
          churnDone.await();
          pool.unpin(inodeId);
          return null;
        }));
      }
      // Churners: pin/unpin repeatedly while holders hold.
      for (int i = 0; i < churners; i++) {
        futures.add(exec.submit(() -> {
          start.await();
          holdReady.await();  // wait until all holders have pinned
          for (int j = 0; j < churnOps; j++) {
            pool.pin(inodeId);
            pool.unpin(inodeId);
          }
          churnDone.countDown();
          return null;
        }));
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(20, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }
    assertEquals(0, pool.size(),
        "after all holders and churners release, pool must be empty");
  }

  @Test
  @Timeout(30)
  public void entryIdentityStableAcrossChurnWhilePinned() throws Exception {
    // A long-lived pin holds the entry alive; rapid churn by other threads
    // should all return the SAME entry instance (not a re-created one).
    final LockPool pool = new LockPool();
    final long inodeId = 42L;
    final LockPool.Entry stableEntry = pool.pin(inodeId);
    try {
      final int numThreads = 8;
      final int opsPerThread = 1_000;
      final AtomicInteger mismatches = new AtomicInteger(0);
      ExecutorService exec = Executors.newFixedThreadPool(numThreads);
      try {
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
          futures.add(exec.submit(() -> {
            for (int j = 0; j < opsPerThread; j++) {
              LockPool.Entry e = pool.pin(inodeId);
              if (e != stableEntry) {
                mismatches.incrementAndGet();
              }
              pool.unpin(inodeId);
            }
            return null;
          }));
        }
        for (Future<?> f : futures) {
          f.get(20, TimeUnit.SECONDS);
        }
      } finally {
        exec.shutdown();
        assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
      }
      assertEquals(0, mismatches.get(),
          "entry identity must be stable while any pin is held");
    } finally {
      pool.unpin(inodeId);
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void sizeTracksPinAndUnpinAccurately() {
    LockPool pool = new LockPool();
    for (long i = 0; i < 100; i++) {
      pool.pin(i);
    }
    assertEquals(100, pool.size());

    // Unpin half
    for (long i = 0; i < 50; i++) {
      pool.unpin(i);
    }
    assertEquals(50, pool.size());

    // Pin the same keys twice to get refCount=2
    for (long i = 50; i < 100; i++) {
      pool.pin(i);
    }
    assertEquals(50, pool.size());
    for (long i = 50; i < 100; i++) {
      assertEquals(2, pool.refCount(i));
    }

    // Release down to zero
    for (long i = 50; i < 100; i++) {
      pool.unpin(i);
      pool.unpin(i);
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void customInitialCapacityDoesNotAffectBehavior() {
    LockPool pool = new LockPool(16);
    pool.pin(1L);
    pool.pin(2L);
    assertEquals(2, pool.size());
    pool.unpin(1L);
    pool.unpin(2L);
    assertEquals(0, pool.size());
  }
}
