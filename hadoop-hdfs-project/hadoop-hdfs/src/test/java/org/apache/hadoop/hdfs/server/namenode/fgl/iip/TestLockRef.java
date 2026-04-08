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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link LockRef}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Acquire + close releases both the lock and the pin.</li>
 *   <li>Read locks are shared across threads; pool refcount matches holders.</li>
 *   <li>Write locks are exclusive.</li>
 *   <li>Timeout throws {@link LockAcquisitionTimeoutException} and unwinds
 *       cleanly (no pin leak, no stuck lock).</li>
 *   <li>{@link LockRef#close()} is idempotent on the acquiring thread.</li>
 *   <li>Acquire with already-passed deadline fails fast without leaking
 *       the pin.</li>
 *   <li>Stress: many read acquires on the same key evict cleanly when all
 *       close.</li>
 * </ul>
 */
public class TestLockRef {

  private static long deadline(long timeoutMs) {
    return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
  }

  @Test
  @Timeout(10)
  public void readAcquireAndCloseReleasesPool() throws Exception {
    LockPool pool = new LockPool();
    LockRef ref = LockRef.acquire(pool, 1L, false, deadline(1_000));
    assertEquals(1L, ref.inodeId());
    assertFalse(ref.isWriteLock());
    assertFalse(ref.isClosed());
    assertEquals(1, pool.refCount(1L));

    ref.close();
    assertTrue(ref.isClosed());
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void writeAcquireAndCloseReleasesPool() throws Exception {
    LockPool pool = new LockPool();
    LockRef ref = LockRef.acquire(pool, 1L, true, deadline(1_000));
    assertTrue(ref.isWriteLock());
    assertEquals(1, pool.refCount(1L));

    ref.close();
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void closeIsIdempotent() throws Exception {
    LockPool pool = new LockPool();
    LockRef ref = LockRef.acquire(pool, 1L, false, deadline(1_000));
    ref.close();
    ref.close();
    ref.close();
    assertTrue(ref.isClosed());
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void tryWithResourcesReleases() throws Exception {
    LockPool pool = new LockPool();
    try (LockRef ignored = LockRef.acquire(pool, 1L, false, deadline(1_000))) {
      assertEquals(1, pool.size());
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void alreadyPassedDeadlineThrowsTimeoutAndCleansUp() {
    LockPool pool = new LockPool();
    long expired = System.nanoTime() - 1_000_000L;  // in the past
    assertThrows(LockAcquisitionTimeoutException.class,
        () -> LockRef.acquire(pool, 1L, false, expired));
    // Pin must have been released by the finally block.
    assertEquals(0, pool.size(), "timed-out acquire must not leak the pin");
  }

  @Test
  @Timeout(10)
  public void concurrentReadersShareSameRRWL() throws Exception {
    final LockPool pool = new LockPool();
    final long inodeId = 42L;
    final int numReaders = 8;
    final CountDownLatch allAcquired = new CountDownLatch(numReaders);
    final CountDownLatch release = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(numReaders);
    final List<Future<LockRef>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < numReaders; i++) {
        futures.add(exec.submit(() -> {
          LockRef ref = LockRef.acquire(pool, inodeId, false, deadline(5_000));
          allAcquired.countDown();
          release.await();
          ref.close();
          return ref;
        }));
      }
      assertTrue(allAcquired.await(5, TimeUnit.SECONDS),
          "all readers should acquire concurrently since read lock is shared");
      // Pool refcount should reflect all live read acquires.
      assertEquals(numReaders, pool.refCount(inodeId));
      release.countDown();
      for (Future<LockRef> f : futures) {
        f.get(5, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void writerBlocksReadersAndViceVersa() throws Exception {
    final LockPool pool = new LockPool();
    final long inodeId = 42L;
    final CountDownLatch writerAcquired = new CountDownLatch(1);
    final CountDownLatch writerRelease = new CountDownLatch(1);
    final AtomicBoolean readerGotLock = new AtomicBoolean(false);
    ExecutorService exec = Executors.newFixedThreadPool(2);
    try {
      Future<?> writer = exec.submit(() -> {
        LockRef w = LockRef.acquire(pool, inodeId, true, deadline(5_000));
        writerAcquired.countDown();
        writerRelease.await();
        w.close();
        return null;
      });
      assertTrue(writerAcquired.await(5, TimeUnit.SECONDS));

      Future<?> reader = exec.submit(() -> {
        // Should time out because writer holds exclusive lock.
        try {
          LockRef r = LockRef.acquire(pool, inodeId, false, deadline(200));
          readerGotLock.set(true);
          r.close();
        } catch (LockAcquisitionTimeoutException expected) {
          // Expected — writer is holding.
        }
        return null;
      });
      reader.get(5, TimeUnit.SECONDS);
      assertFalse(readerGotLock.get(),
          "reader must not acquire while writer is holding");

      // Release writer; now a reader should succeed.
      writerRelease.countDown();
      writer.get(5, TimeUnit.SECONDS);

      try (LockRef r = LockRef.acquire(pool, inodeId, false, deadline(5_000))) {
        // ok
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(10)
  public void timeoutDuringContentionLeavesNoLeak() throws Exception {
    final LockPool pool = new LockPool();
    final long inodeId = 42L;
    // Hold a write lock from a parking thread.
    final CountDownLatch holderReady = new CountDownLatch(1);
    final CountDownLatch holderRelease = new CountDownLatch(1);
    Thread holder = new Thread(() -> {
      try {
        LockRef w = LockRef.acquire(pool, inodeId, true, deadline(5_000));
        holderReady.countDown();
        holderRelease.await();
        w.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    holder.start();
    assertTrue(holderReady.await(5, TimeUnit.SECONDS));
    // Refcount should be 1 (holder only).
    assertEquals(1, pool.refCount(inodeId));

    // Attempt a write lock with short deadline; should time out.
    assertThrows(LockAcquisitionTimeoutException.class,
        () -> LockRef.acquire(pool, inodeId, true, deadline(100)));

    // The failed acquire must have released its pin — refcount still 1.
    assertEquals(1, pool.refCount(inodeId));

    holderRelease.countDown();
    holder.join(5_000);
    assertEquals(0, pool.size());
  }

  @Test
  @Timeout(30)
  public void stressManyReadersEvictsCleanly() throws Exception {
    final LockPool pool = new LockPool();
    final long inodeId = 42L;
    final int numThreads = 16;
    final int opsPerThread = 2_000;
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicInteger failures = new AtomicInteger(0);
    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < numThreads; i++) {
        futures.add(exec.submit(() -> {
          start.await();
          for (int j = 0; j < opsPerThread; j++) {
            try {
              try (LockRef r = LockRef.acquire(pool, inodeId, false, deadline(5_000))) {
                // Hold briefly.
              }
            } catch (Exception e) {
              failures.incrementAndGet();
              throw e;
            }
          }
          return null;
        }));
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(25, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }
    assertEquals(0, failures.get());
    assertEquals(0, pool.size(), "after stress run, pool must be empty");
  }

  @Test
  @Timeout(30)
  public void stressWriterReaderInterleave() throws Exception {
    final LockPool pool = new LockPool();
    final int numThreads = 8;
    final int opsPerThread = 500;
    final long[] inodes = {1L, 2L, 3L, 4L};
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicInteger failures = new AtomicInteger(0);
    ExecutorService exec = Executors.newFixedThreadPool(numThreads);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < numThreads; i++) {
        final int tid = i;
        futures.add(exec.submit(() -> {
          start.await();
          for (int j = 0; j < opsPerThread; j++) {
            long id = inodes[(tid + j) % inodes.length];
            boolean write = ((tid + j) % 3) == 0;
            try (LockRef r = LockRef.acquire(pool, id, write, deadline(5_000))) {
              // Hold briefly.
            } catch (Exception e) {
              failures.incrementAndGet();
              throw e;
            }
          }
          return null;
        }));
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(25, TimeUnit.SECONDS);
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }
    assertEquals(0, failures.get());
    assertEquals(0, pool.size());
  }
}
