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

import java.util.List;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single-use handle bundling a resolved {@link INodesInPath} with the
 * per-INode lock references acquired during its hand-over-hand walk,
 * plus the compat-namespace read lock held for the duration of the
 * walk.
 *
 * <p>Returned by {@link INodeLockManager#acquire}, wrapped in the
 * {@code FSNLockManager.lockPath} entry point. RPC handlers use it via
 * {@code try-with-resources}:
 *
 * <pre>
 *   try (LockedIIP lip = fsLock.lockPath(path, mode)) {
 *     // RPC body reads lip.iip() while all path locks are held
 *   }  // close() releases locks in reverse order
 * </pre>
 *
 * <p><b>Thread affinity.</b> A {@code LockedIIP} MUST be closed by the
 * thread that acquired it. The underlying {@link LockRef} instances are
 * held by the acquiring thread, and {@link java.util.concurrent.locks.ReentrantReadWriteLock}
 * tracks holder thread identity — releasing from a different thread
 * produces undefined behavior.
 *
 * <p><b>Idempotency.</b> {@link #close()} is idempotent for the
 * acquiring thread. It is NOT safe against concurrent calls from
 * different threads.
 *
 * <p>See {@code docs/fgl/HDFS-17385-wave4-pilot-design.md} §2.5 for the
 * design rationale.
 */
@InterfaceAudience.Private
public final class LockedIIP implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(LockedIIP.class);

  private final INodesInPath iip;
  private final List<LockRef> heldLocks;
  private final Lock compatReadLock;
  /**
   * {@code true} if the acquiring mode took at least one per-INode
   * write lock. {@link #close()} uses this to reset
   * {@link INodeLockManager#HELD_IIP_WRITE} when this handle was the
   * one that set it.
   */
  private final boolean wasWrite;
  private boolean closed;

  /**
   * Package-private convenience constructor for read-only acquisitions.
   * Delegates to the 4-arg constructor with {@code wasWrite=false}.
   */
  LockedIIP(INodesInPath iip, List<LockRef> heldLocks, Lock compatReadLock) {
    this(iip, heldLocks, compatReadLock, false);
  }

  /**
   * Package-private constructor. Only {@link INodeLockManager} (and
   * tests in the same package) may create instances directly.
   *
   * <p>The {@code heldLocks} list is stored as-is; the caller is
   * expected to pass an immutable or defensively-copied list to make
   * ownership transfer unambiguous.
   *
   * @param iip            the resolved INodesInPath — target may have
   *                       {@code null} entries for a non-existent tail
   * @param heldLocks      per-INode locks held for the duration of
   *                       this handle, in acquisition order
   *                       (root → leaf)
   * @param compatReadLock the compat-namespace read lock token to
   *                       release on {@link #close}
   * @param wasWrite       {@code true} if the acquiring mode took any
   *                       per-INode write lock — {@link #close()}
   *                       will reset
   *                       {@link INodeLockManager#HELD_IIP_WRITE}
   */
  LockedIIP(INodesInPath iip, List<LockRef> heldLocks, Lock compatReadLock,
      boolean wasWrite) {
    this.iip = iip;
    this.heldLocks = heldLocks;
    this.compatReadLock = compatReadLock;
    this.wasWrite = wasWrite;
    this.closed = false;
  }

  /**
   * @return the resolved path, with lock guarantees described above.
   */
  public INodesInPath iip() {
    return iip;
  }

  /**
   * @return {@code true} if {@link #close()} has run on this handle.
   */
  public boolean isClosed() {
    return closed;
  }

  /**
   * Release all held resources in the correct order:
   * <ol>
   *   <li>Per-INode locks are closed in REVERSE acquisition order
   *       (leaf → root), mirroring the hand-over-hand walk.</li>
   *   <li>Compat-namespace read lock is released last.</li>
   *   <li>{@link INodeLockManager#HELD_IIP_DEPTH} is decremented.</li>
   * </ol>
   *
   * <p>Runtime exceptions from individual {@link LockRef#close()} calls
   * are logged and swallowed so that subsequent releases still run.
   * Partial release (leaking some locks while others leak too) is
   * strictly worse than log-and-continue.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    // Release per-INode locks in reverse order.
    for (int i = heldLocks.size() - 1; i >= 0; i--) {
      try {
        heldLocks.get(i).close();
      } catch (RuntimeException re) {
        LOG.warn("LockRef close failed during LockedIIP.close()", re);
      }
    }
    // Release compat-read last.
    try {
      compatReadLock.unlock();
    } catch (RuntimeException re) {
      LOG.warn("compat-read unlock failed during LockedIIP.close()", re);
    }
    // Decrement depth counter. Log-and-clamp on underflow rather than
    // throwing — an underflow indicates a bug, but we don't want
    // close() to propagate it and leave the caller in a bad state.
    int depth = INodeLockManager.HELD_IIP_DEPTH.get();
    if (depth <= 0) {
      LOG.warn("HELD_IIP_DEPTH underflow on LockedIIP.close() (was {})", depth);
      INodeLockManager.HELD_IIP_DEPTH.set(0);
    } else {
      INodeLockManager.HELD_IIP_DEPTH.set(depth - 1);
    }
    // Reset the IIP-write flag only if this handle was the one that
    // set it. Nesting is forbidden (asserted in INodeLockManager.acquire),
    // so at most one handle per thread can have wasWrite=true at any
    // time.
    if (wasWrite) {
      INodeLockManager.HELD_IIP_WRITE.set(Boolean.FALSE);
    }
  }
}
