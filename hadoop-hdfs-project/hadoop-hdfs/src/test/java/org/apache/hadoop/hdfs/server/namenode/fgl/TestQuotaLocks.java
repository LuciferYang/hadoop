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
package org.apache.hadoop.hdfs.server.namenode.fgl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link QuotaLocks}. The pool itself is tested in
 * isolation here; integration with {@code FSDirectory.updateCount} is
 * exercised in {@code TestFSNamesystemFGLIIP}.
 */
public class TestQuotaLocks {

  @Test
  @Timeout(10)
  public void lazyLockCreationReturnsSameInstance() {
    QuotaLocks pool = new QuotaLocks();
    ReentrantLock l1 = pool.getLockForTest(42L);
    ReentrantLock l2 = pool.getLockForTest(42L);
    assertSame(l1, l2,
        "lazy creation must return the same lock for the same INode ID");
  }

  @Test
  @Timeout(10)
  public void distinctIdsGetDistinctLocks() {
    QuotaLocks pool = new QuotaLocks();
    ReentrantLock l1 = pool.getLockForTest(1L);
    ReentrantLock l2 = pool.getLockForTest(2L);
    assertNotSame(l1, l2);
  }

  @Test
  @Timeout(10)
  public void reentrantAcquireFromSameThread() {
    QuotaLocks pool = new QuotaLocks();
    ReentrantLock lock = pool.getLockForTest(7L);
    lock.lock();
    try {
      lock.lock();
      try {
        assertEquals(2, lock.getHoldCount(),
            "ReentrantLock must support nested acquisition by same thread");
      } finally {
        lock.unlock();
      }
    } finally {
      lock.unlock();
    }
    assertEquals(0, lock.getHoldCount());
  }

  @Test
  @Timeout(10)
  public void emptyHandleIsIdempotentOnClose() {
    QuotaLocks.LockedQuota handle = QuotaLocks.LockedQuota.EMPTY;
    handle.close();
    handle.close(); // double-close must not throw
    assertNotNull(handle);
  }

  /**
   * Two threads each acquire and hold the same lock; the second waits
   * until the first releases. Verifies the pool actually serialises
   * concurrent writers on the same INode.
   */
  @Test
  @Timeout(20)
  public void contendedLockSerialisesWriters() throws Exception {
    QuotaLocks pool = new QuotaLocks();
    final ReentrantLock lock = pool.getLockForTest(99L);
    final AtomicInteger ordering = new AtomicInteger(0);
    final CountDownLatch firstHolderAcquired = new CountDownLatch(1);
    final CountDownLatch secondHolderDone = new CountDownLatch(1);

    Future<?> first = Executors.newSingleThreadExecutor().submit(() -> {
      lock.lock();
      firstHolderAcquired.countDown();
      try {
        // Hold the lock long enough for the second thread to attempt
        // acquisition and block on it.
        Thread.sleep(50);
        // First thread releases at ordering=1.
        assertEquals(0, ordering.getAndIncrement());
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      } finally {
        lock.unlock();
      }
      return null;
    });

    Future<?> second = Executors.newSingleThreadExecutor().submit(() -> {
      try {
        firstHolderAcquired.await(5, TimeUnit.SECONDS);
        lock.lock();
        try {
          // Second thread acquires only after first releases →
          // ordering must already be 1.
          assertEquals(1, ordering.getAndIncrement());
        } finally {
          lock.unlock();
          secondHolderDone.countDown();
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      return null;
    });

    first.get(10, TimeUnit.SECONDS);
    second.get(10, TimeUnit.SECONDS);
    assertTrue(secondHolderDone.await(1, TimeUnit.SECONDS));
    assertEquals(2, ordering.get(),
        "exactly two writers should have entered the critical section");
  }
}
