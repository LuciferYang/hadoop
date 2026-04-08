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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A single-use handle that owns both a {@link LockPool} pin and a held
 * {@link Lock} on a specific INode.
 *
 * <p><b>Ownership model.</b> After {@link #acquire} returns successfully,
 * the returned {@code LockRef} is the sole owner of the pool pin and the
 * held RRWL lock for that INode. Callers must invoke {@link #close()}
 * exactly once — typically via {@code try-with-resources} — to release
 * both.
 *
 * <p><b>Thread affinity.</b> A {@code LockRef} must be closed by the same
 * thread that acquired it. Calling {@link #close()} from a different
 * thread is undefined behaviour and may leave the RRWL in an inconsistent
 * state (RRWL tracks holder thread identity).
 *
 * <p><b>Idempotency.</b> {@link #close()} is idempotent within a single
 * thread — a second call is a no-op. However, it is not safe against
 * concurrent calls from different threads.
 *
 * <p>This class is package-private by design. See mitigation #2 of the
 * pilot design (single lock-acquisition chokepoint).
 *
 * @see LockPool
 * @see LockAcquisitionTimeoutException
 */
@InterfaceAudience.Private
final class LockRef implements AutoCloseable {

  private final LockPool pool;
  private final long inodeId;
  private final Lock heldLock;
  private final boolean writeLock;
  private boolean closed;

  private LockRef(LockPool pool, long inodeId, Lock heldLock, boolean writeLock) {
    this.pool = pool;
    this.inodeId = inodeId;
    this.heldLock = heldLock;
    this.writeLock = writeLock;
    this.closed = false;
  }

  /**
   * Acquire a lock on {@code inodeId} from {@code pool}, honouring the
   * given absolute deadline.
   *
   * <p>The acquisition sequence is:
   * <ol>
   *   <li>Pin the pool entry (creating if absent).</li>
   *   <li>{@link Lock#tryLock(long, TimeUnit) tryLock} with remaining
   *       budget until the deadline.</li>
   *   <li>Construct and return the {@code LockRef}, transferring
   *       ownership.</li>
   * </ol>
   *
   * <p>If any step throws or the {@code tryLock} returns {@code false},
   * resources acquired up to that point are released in reverse order
   * (unlock if locked, unpin if pinned) before the exception propagates.
   * This is the fix for the round-5 leak bug where a throwing {@code
   * LockRef} constructor could leave the RRWL in a held state without a
   * corresponding reference.
   *
   * @param pool          the lock pool to draw from
   * @param inodeId       INode ID identifying the lock
   * @param writeLock     {@code true} to acquire the write lock,
   *                      {@code false} for the read lock
   * @param deadlineNanos absolute deadline in {@link System#nanoTime()}
   *                      units; values &le; {@code System.nanoTime()}
   *                      cause immediate timeout
   * @return a new {@code LockRef} owning both the pin and the held lock
   * @throws InterruptedException               if the calling thread is
   *                                            interrupted while waiting
   * @throws LockAcquisitionTimeoutException    if the deadline elapses
   *                                            before the lock is acquired
   */
  static LockRef acquire(LockPool pool, long inodeId, boolean writeLock,
      long deadlineNanos) throws InterruptedException, LockAcquisitionTimeoutException {
    LockPool.Entry entry = pool.pin(inodeId);
    boolean pinHeld = true;
    Lock lock = null;
    boolean lockHeld = false;
    try {
      lock = writeLock ? entry.lock.writeLock() : entry.lock.readLock();
      long remaining = deadlineNanos - System.nanoTime();
      if (remaining <= 0L
          || !lock.tryLock(remaining, TimeUnit.NANOSECONDS)) {
        throw new LockAcquisitionTimeoutException(inodeId, writeLock);
      }
      lockHeld = true;
      LockRef ref = new LockRef(pool, inodeId, lock, writeLock);
      // Ownership transferred to the returned LockRef — do NOT release
      // in the finally block below.
      pinHeld = false;
      lockHeld = false;
      return ref;
    } finally {
      // Release in reverse acquisition order if ownership was not
      // transferred (exception on any step after pin/lock).
      if (lockHeld) {
        lock.unlock();
      }
      if (pinHeld) {
        pool.unpin(inodeId);
      }
    }
  }

  /**
   * @return the INode ID this ref is bound to.
   */
  long inodeId() {
    return inodeId;
  }

  /**
   * @return {@code true} if this ref holds the write lock; {@code false}
   *         if it holds the read lock.
   */
  boolean isWriteLock() {
    return writeLock;
  }

  /**
   * @return {@code true} if {@link #close()} has been called on this ref.
   */
  boolean isClosed() {
    return closed;
  }

  /**
   * Release the held lock and unpin the pool entry. Idempotent for the
   * acquiring thread. Must not be called from a thread other than the
   * one that called {@link #acquire}.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    try {
      heldLock.unlock();
    } finally {
      pool.unpin(inodeId);
    }
  }
}
