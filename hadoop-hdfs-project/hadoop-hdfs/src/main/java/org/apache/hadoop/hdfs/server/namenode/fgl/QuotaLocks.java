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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;

/**
 * Per-{@code DirectoryWithQuotaFeature} mutex pool for FGL_IIP
 * (HDFS-17386 Phase III / Track P.2, HDFS-17473).
 *
 * <p>Under the legacy global FS lock model, the FS write lock provides
 * the atomicity needed by the {@code verify-quota → update-usage}
 * pattern in {@code FSDirectory.updateCount}. Under the FGL_IIP pilot
 * the FS write lock is no longer held — multiple concurrent operations
 * may share a quota-bearing ancestor under read-only IIP locks, and
 * non-atomic mutations of the ancestor's {@code QuotaCounts} usage
 * would race.
 *
 * <p>This pool hands out one lazy {@link ReentrantLock} per quota-
 * bearing INode (keyed by INode ID). Callers that need to verify and
 * mutate quota usage atomically acquire all the relevant locks via
 * {@link #acquireForIIP} before doing the work, and release them via
 * try-with-resources.
 *
 * <h2>Lock ordering</h2>
 *
 * <p>Locks are acquired in ascending INode-ID order. Two operations
 * that touch the same set of quota-bearing ancestors therefore acquire
 * locks in the same order, preventing deadlock from ABBA patterns.
 *
 * <h2>Reentrance</h2>
 *
 * <p>Locks are {@link ReentrantLock} so a single thread can re-enter
 * a quota-locked section (e.g., a nested {@code updateCount} during
 * rename's quota fixup).
 *
 * <h2>Coexistence with the FS write lock</h2>
 *
 * <p>Under legacy mode this pool is still acquired but is
 * effectively uncontended (FS write lock already serializes writers).
 * The cost is one uncontended {@code lock()} per quota-bearing
 * ancestor on the IIP — cheap.
 *
 * <h2>Memory footprint</h2>
 *
 * <p>Locks are inserted lazily and currently never evicted, so the
 * pool grows over time with the set of distinct quota-bearing INode
 * IDs that have been touched since startup. For a single
 * {@code DirectoryWithQuotaFeature} this is a single
 * {@link ReentrantLock} — negligible. A future enhancement may wire
 * {@link #remove(long)} into {@code FSDirectory.removeQuota} and
 * INode-removal paths to bound the pool size.
 *
 * <h2>Caveats</h2>
 *
 * <p>Two flows currently bypass this pool. Both are safe today but
 * are recorded here so future readers can find them when extending
 * the FGL_IIP pilot to cover wider scenarios.
 *
 * <ol>
 *   <li><b>Verify-only callers</b>: {@code FSDirectory.verifyQuota}
 *       callers that are NOT paired with an immediate
 *       {@code updateCount} ({@code FSDirAppendOp},
 *       {@code FSDirTruncateOp}, {@code FSDirConcatOp},
 *       {@code FSDirRenameOp.verifyQuotaForRename}) do not acquire
 *       this pool. Under FGL_IIP they can return a stale-pass when
 *       another thread is mid-update of the same ancestor. The
 *       eventual update via {@link
 *       org.apache.hadoop.hdfs.server.namenode.FSDirectory#updateCount}
 *       always re-verifies under the lock, so the quota invariant is
 *       still enforced — but a caller may now see a fail-fast
 *       "quota OK" check followed by a real "quota exceeded" at
 *       update time.</li>
 *   <li><b>{@code FSDirRenameOp.updateQuotasInSourceTree}</b> calls
 *       {@code srcParent.addSpaceConsumed} directly (no QuotaLocks
 *       wrap). It guards itself with an
 *       {@code if (isSrcInSnapshot)} check, and snapshot-bearing
 *       ancestors are rejected by {@code ancestorsAllowMutate} —
 *       so the bypass is unreachable from the pilot path today.
 *       Future work extending the pilot to cover snapshot-bearing
 *       renames must add a {@link #acquireForIIP} (or
 *       {@link #acquireForINode}) wrap here.</li>
 * </ol>
 *
 * <p>Tightening these flows is a follow-up.
 */
@InterfaceAudience.Private
public final class QuotaLocks {

  private final ConcurrentHashMap<Long, ReentrantLock> locks =
      new ConcurrentHashMap<>();

  /** @return the lock for {@code inodeId}, creating one lazily. */
  @VisibleForTesting
  ReentrantLock getLockForTest(long inodeId) {
    return locks.computeIfAbsent(inodeId, k -> new ReentrantLock(true));
  }

  /**
   * Acquire locks on every quota-bearing INode at depths
   * {@code [0, numOfINodes)} of {@code iip}, in ascending INode-ID
   * order. The returned {@link LockedQuota} releases all locks in
   * reverse order when closed.
   *
   * <p>If the IIP has no quota-bearing ancestors, returns an empty
   * handle (no locks acquired).
   *
   * @param iip          path whose ancestors to scan
   * @param numOfINodes  exclusive upper bound on depths to scan; the
   *                     value may exceed {@code iip.length()} (in
   *                     which case it is clamped)
   */
  public LockedQuota acquireForIIP(INodesInPath iip, int numOfINodes) {
    if (iip == null) {
      return LockedQuota.EMPTY;
    }
    final int bound = Math.min(numOfINodes, iip.length());
    List<Long> ids = collectQuotaINodeIds(iip, bound);
    if (ids.isEmpty()) {
      return LockedQuota.EMPTY;
    }
    Collections.sort(ids);
    List<ReentrantLock> held = new ArrayList<>(ids.size());
    boolean success = false;
    try {
      for (Long id : ids) {
        ReentrantLock lock = locks.computeIfAbsent(id,
            k -> new ReentrantLock(true));
        lock.lock();
        held.add(lock);
      }
      success = true;
      return new LockedQuota(held);
    } finally {
      if (!success) {
        for (int i = held.size() - 1; i >= 0; i--) {
          try {
            held.get(i).unlock();
          } catch (RuntimeException ignored) {
            // best-effort release
          }
        }
      }
    }
  }

  /**
   * Acquire the lock for a single INode. Useful for direct mutations
   * to a known {@code DirectoryWithQuotaFeature} (e.g., the
   * {@code quotaDirMap} entries handled by
   * {@code FSDirectory.updateCount(INodesInPath, INode.QuotaDelta,
   * boolean)}). Returns an empty handle when {@code inode} is null,
   * has no quota feature, or is not a directory.
   */
  public LockedQuota acquireForINode(INode inode) {
    if (inode == null || !inode.isQuotaSet()) {
      return LockedQuota.EMPTY;
    }
    ReentrantLock lock = locks.computeIfAbsent(inode.getId(),
        k -> new ReentrantLock(true));
    lock.lock();
    return new LockedQuota(Collections.singletonList(lock));
  }

  private static List<Long> collectQuotaINodeIds(INodesInPath iip,
      int bound) {
    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < bound; i++) {
      INode node = iip.getINode(i);
      if (node != null && node.isQuotaSet()) {
        ids.add(node.getId());
      }
    }
    return ids;
  }

  /**
   * @return the number of distinct INode IDs for which a lock has
   *         been lazily created since startup (not the number of
   *         currently-acquired locks)
   */
  @VisibleForTesting
  int size() {
    return locks.size();
  }

  /**
   * Drop the lock entry for {@code inodeId} from the pool. Intended
   * for callers that know the corresponding
   * {@code DirectoryWithQuotaFeature} has been removed (quota
   * cleared, INode deleted), bounding the pool's memory footprint.
   *
   * <p>No-op if no entry exists. <b>Caller must not hold the lock</b>
   * when invoking this — removing an entry that is currently locked
   * by another thread would orphan its waiters. In practice this
   * means callers should remove only after acquiring then releasing,
   * under a higher-level invariant that no concurrent acquirer will
   * re-fetch the entry.
   *
   * <p>Currently unused — wired into FSDirectory's quota-removal /
   * INode-deletion paths is a follow-up.
   */
  public void remove(long inodeId) {
    locks.remove(inodeId);
  }

  /**
   * Handle to a set of held quota locks. Closing the handle releases
   * the locks in reverse acquisition order.
   *
   * <p>{@code close()} is idempotent and safe to call multiple times
   * — typically once via try-with-resources. The {@code closed} flag
   * is {@code volatile} so the early-return read sees writes from
   * other threads with a defined happens-before edge; the shared
   * {@link #EMPTY} singleton is the only handle that can be touched
   * by more than one thread, and its {@code held} list is empty so
   * a missed early-return is harmless (the unlock loop is a no-op).
   */
  public static final class LockedQuota implements AutoCloseable {
    static final LockedQuota EMPTY =
        new LockedQuota(Collections.emptyList());

    private final List<ReentrantLock> held;
    private volatile boolean closed = false;

    LockedQuota(List<ReentrantLock> held) {
      this.held = held;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      for (int i = held.size() - 1; i >= 0; i--) {
        try {
          held.get(i).unlock();
        } catch (RuntimeException ignored) {
          // best-effort release; continue
        }
      }
    }
  }
}
