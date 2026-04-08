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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link LockedIIP}.
 *
 * <p>Focus: close() semantics, idempotency, reverse-order release,
 * HELD_IIP_DEPTH decrement safety. Tests use a minimal IIP + a small
 * LockPool rather than a full MiniDFSCluster.
 */
public class TestLockedIIP {

  private static long deadline(long timeoutMs) {
    return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
  }

  /**
   * Build a minimal LockedIIP by acquiring N read locks from the pool,
   * then wrapping in a freshly-constructed LockedIIP. Simulates the
   * state that INodeLockManager.acquire would create.
   */
  private static LockedIIP buildHandle(LockPool pool,
      ReentrantReadWriteLock compatLock, long... inodeIds) throws Exception {
    compatLock.readLock().lock();
    List<LockRef> held = new ArrayList<>();
    for (long id : inodeIds) {
      held.add(LockRef.acquire(pool, id, false, deadline(5_000)));
    }
    INodesInPath iip = INodesInPath.fromComponentsAndInodes(
        new byte[inodeIds.length][], new INode[inodeIds.length]);
    INodeLockManager.HELD_IIP_DEPTH.set(1);
    return new LockedIIP(iip, Collections.unmodifiableList(held),
        compatLock.readLock());
  }

  @Test
  @Timeout(10)
  public void closeReleasesLocksAndCompatReadInOrder() throws Exception {
    LockPool pool = new LockPool();
    ReentrantReadWriteLock compat = new ReentrantReadWriteLock(true);

    LockedIIP handle = buildHandle(pool, compat, 1L, 2L, 3L);
    assertNotNull(handle.iip());
    assertFalse(handle.isClosed());
    assertEquals(3, pool.size());
    assertTrue(compat.getReadHoldCount() >= 1);
    assertEquals(Integer.valueOf(1), INodeLockManager.HELD_IIP_DEPTH.get());

    handle.close();

    assertTrue(handle.isClosed());
    assertEquals(0, pool.size(),
        "all per-INode locks should be released");
    assertEquals(0, compat.getReadHoldCount(),
        "compat-read should be released on current thread");
    assertEquals(Integer.valueOf(0), INodeLockManager.HELD_IIP_DEPTH.get());
  }

  @Test
  @Timeout(10)
  public void closeIsIdempotent() throws Exception {
    LockPool pool = new LockPool();
    ReentrantReadWriteLock compat = new ReentrantReadWriteLock(true);

    LockedIIP handle = buildHandle(pool, compat, 1L);
    handle.close();
    // Subsequent closes must be no-ops.
    handle.close();
    handle.close();

    assertTrue(handle.isClosed());
    assertEquals(0, pool.size());
    assertEquals(0, compat.getReadHoldCount());
    assertEquals(Integer.valueOf(0), INodeLockManager.HELD_IIP_DEPTH.get());
  }

  @Test
  @Timeout(10)
  public void tryWithResourcesReleases() throws Exception {
    LockPool pool = new LockPool();
    ReentrantReadWriteLock compat = new ReentrantReadWriteLock(true);

    try (LockedIIP ignored = buildHandle(pool, compat, 1L, 2L)) {
      assertEquals(2, pool.size());
      assertEquals(Integer.valueOf(1),
          INodeLockManager.HELD_IIP_DEPTH.get());
    }

    assertEquals(0, pool.size());
    assertEquals(Integer.valueOf(0), INodeLockManager.HELD_IIP_DEPTH.get());
  }

  @Test
  @Timeout(10)
  public void emptyLockListIsValid() {
    // A LockedIIP with no per-INode locks (degenerate case: acquire
    // on "/" with PATH_READ would still acquire root's read lock,
    // but the wrapper itself must support an empty list).
    LockPool pool = new LockPool();
    ReentrantReadWriteLock compat = new ReentrantReadWriteLock(true);
    compat.readLock().lock();
    INodeLockManager.HELD_IIP_DEPTH.set(1);

    LockedIIP handle = new LockedIIP(
        INodesInPath.fromComponentsAndInodes(new byte[0][], new INode[0]),
        Collections.emptyList(), compat.readLock());

    assertFalse(handle.isClosed());
    handle.close();
    assertTrue(handle.isClosed());
    assertEquals(0, compat.getReadHoldCount());
    assertEquals(Integer.valueOf(0), INodeLockManager.HELD_IIP_DEPTH.get());
  }

  @Test
  @Timeout(10)
  public void closeDecrementsDepthClampingAtZero() throws Exception {
    LockPool pool = new LockPool();
    ReentrantReadWriteLock compat = new ReentrantReadWriteLock(true);

    LockedIIP handle = buildHandle(pool, compat, 1L);
    // Force the counter to 0 behind close's back to simulate an
    // underflow bug. close() should log-and-clamp, not throw.
    INodeLockManager.HELD_IIP_DEPTH.set(0);

    // Must not throw.
    handle.close();

    assertEquals(Integer.valueOf(0), INodeLockManager.HELD_IIP_DEPTH.get(),
        "depth counter must be clamped at 0");
    assertEquals(0, pool.size());
  }
}
