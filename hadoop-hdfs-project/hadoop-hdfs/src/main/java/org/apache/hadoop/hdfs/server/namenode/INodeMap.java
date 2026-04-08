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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

import org.apache.hadoop.util.Preconditions;

/**
 * Storing all the {@link INode}s and maintaining the mapping between INode ID
 * and INode.
 *
 * <h2>Thread safety (HDFS-17474, part of the HDFS-17385 Phase II pilot)</h2>
 *
 * <p>This class provides per-key thread safety for {@link #get(long)},
 * {@link #put(INode)}, and {@link #remove(INode)} via an internal stripe
 * of {@link LightWeightGSet} instances, each guarded by its own
 * {@link ReentrantReadWriteLock}. Multiple threads may safely call these
 * methods concurrently without holding any external lock; INode IDs
 * mapping to different stripes never contend.
 *
 * <p>Each stripe owns its own {@code LightWeightGSet}, not a shared one.
 * An earlier design attempted to stripe a single underlying GSet, but
 * GSet's internal {@code size++} / bucket linked-list mutations are not
 * safe under concurrent per-key locks — two stripes can race on the
 * same hash bucket. Giving each stripe its own GSet eliminates all
 * cross-stripe sharing while preserving HDFS's specialized memory
 * layout per stripe.
 *
 * <p><b>Iteration requires external exclusion.</b>
 * {@link #getMapIterator()} chains iterators across all stripes without
 * holding stripe locks — a consistent snapshot is not achievable via
 * striping alone. Callers must ensure that no concurrent {@link #put} /
 * {@link #remove} / {@link #clear} runs during iteration, typically by
 * holding the compat namespace lock in write mode (see the pilot design
 * spec §2.7). FSImage save, offline FSImage validation, and quota
 * recomputation already satisfy this contract.
 *
 * <p><b>{@link #size()} is eventually consistent.</b> It sums per-stripe
 * sizes without acquiring stripe locks, so under concurrent mutation
 * without external exclusion the returned count may be briefly stale.
 * Callers that need exact size must hold external exclusion.
 *
 * <p>See {@code docs/fgl/HDFS-17385-wave4-pilot-design.md} §2.7 for the
 * design rationale and §3.1 for interaction with FSImage save.
 */
public class INodeMap {

  /** Default stripe count (must be a power of 2). */
  @VisibleForTesting
  static final int DEFAULT_STRIPE_COUNT = 256;

  /**
   * Create a new {@link INodeMap} with default stripe count.
   *
   * <p>Total capacity is split across stripes so that the aggregate
   * footprint matches the pre-striping baseline (~1% of total memory).
   */
  static INodeMap newInstance(INodeDirectory rootDir) {
    return newInstance(rootDir, DEFAULT_STRIPE_COUNT);
  }

  /**
   * Test / tuning factory with a custom stripe count.
   *
   * @param rootDir the root directory to seed into the appropriate stripe
   * @param stripeCount number of stripes (power of 2, positive)
   */
  @VisibleForTesting
  static INodeMap newInstance(INodeDirectory rootDir, int stripeCount) {
    Preconditions.checkArgument(rootDir != null);
    Preconditions.checkArgument(stripeCount > 0,
        "stripe count must be positive: " + stripeCount);
    Preconditions.checkArgument(
        (stripeCount & (stripeCount - 1)) == 0,
        "stripe count must be a power of 2: " + stripeCount);

    // Compute total capacity at 1% of heap, then split evenly across
    // stripes. Each stripe GSet gets its own hash table.
    int totalCapacity = LightWeightGSet.computeCapacity(1, "INodeMap");
    int perStripeCapacity = Math.max(16, totalCapacity / stripeCount);

    INodeMap result = new INodeMap(stripeCount, perStripeCapacity);
    result.put(rootDir);
    return result;
  }

  /**
   * One stripe: a lock and its own GSet.
   */
  private static final class Stripe {
    final ReentrantReadWriteLock lock;
    final GSet<INode, INodeWithAdditionalFields> gset;

    Stripe(int capacity) {
      // Unfair: stripe locks are never held across unbounded waits,
      // so fairness overhead is unnecessary. This is a different
      // trade-off from LockPool (U1), which uses fair locks on
      // user-visible INode locks to guarantee writer progress.
      this.lock = new ReentrantReadWriteLock(false);
      this.gset = new LightWeightGSet<>(capacity);
    }
  }

  private final Stripe[] stripes;
  private final int stripeMask;

  private INodeMap(int stripeCount, int perStripeCapacity) {
    this.stripes = new Stripe[stripeCount];
    for (int i = 0; i < stripeCount; i++) {
      this.stripes[i] = new Stripe(perStripeCapacity);
    }
    this.stripeMask = stripeCount - 1;
  }

  /**
   * @return the stripe for the given INode ID.
   */
  private Stripe stripeFor(long id) {
    return stripes[(int) (id & stripeMask)];
  }

  /**
   * @return an iterator over all INodes. Callers MUST ensure no
   *         concurrent {@link #put} / {@link #remove} / {@link #clear}
   *         runs during iteration (see class Javadoc).
   */
  public Iterator<INodeWithAdditionalFields> getMapIterator() {
    return new StripeIterator(stripes);
  }

  /**
   * Add an {@link INode} into the map. Replace the old value if
   * necessary. Thread-safe via the stripe lock for the INode's ID.
   *
   * @param inode the INode to be added
   */
  public final void put(INode inode) {
    if (!(inode instanceof INodeWithAdditionalFields)) {
      return;
    }
    Stripe stripe = stripeFor(inode.getId());
    stripe.lock.writeLock().lock();
    try {
      stripe.gset.put((INodeWithAdditionalFields) inode);
    } finally {
      stripe.lock.writeLock().unlock();
    }
  }

  /**
   * Remove an {@link INode} from the map. Thread-safe via the stripe
   * lock for the INode's ID.
   *
   * @param inode the INode to be removed
   */
  public final void remove(INode inode) {
    Stripe stripe = stripeFor(inode.getId());
    stripe.lock.writeLock().lock();
    try {
      stripe.gset.remove(inode);
    } finally {
      stripe.lock.writeLock().unlock();
    }
  }

  /**
   * @return the size of the map. Eventually consistent under concurrent
   *         mutation without external exclusion.
   */
  public int size() {
    int total = 0;
    for (Stripe s : stripes) {
      total += s.gset.size();
    }
    return total;
  }

  /**
   * Get the {@link INode} with the given id from the map. Thread-safe
   * via the stripe lock for {@code id}.
   *
   * @param id the INode ID to look up
   * @return the INode, or {@code null} if no such INode exists
   */
  public INode get(long id) {
    final INode dummy = new INodeWithAdditionalFields(id, null,
        new PermissionStatus("", "", new FsPermission((short) 0)), 0, 0) {

      @Override
      void recordModification(int latestSnapshotId) {
      }

      @Override
      public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
      }

      @Override
      public QuotaCounts computeQuotaUsage(
          BlockStoragePolicySuite bsps, byte blockStoragePolicyId,
          boolean useCache, int lastSnapshotId) {
        return null;
      }

      @Override
      public ContentSummaryComputationContext computeContentSummary(
          int snapshotId, ContentSummaryComputationContext summary) {
        return null;
      }

      @Override
      public void cleanSubtree(
          ReclaimContext reclaimContext, int snapshotId, int priorSnapshotId) {
      }

      @Override
      public byte getStoragePolicyID() {
        return HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      }

      @Override
      public byte getLocalStoragePolicyID() {
        return HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      }
    };

    Stripe stripe = stripeFor(id);
    stripe.lock.readLock().lock();
    try {
      return stripe.gset.get(dummy);
    } finally {
      stripe.lock.readLock().unlock();
    }
  }

  /**
   * Clear the entire map. Acquires every stripe in write mode to
   * guarantee no concurrent mutation is in flight, then clears each
   * stripe's GSet. Intended for shutdown / reset paths.
   */
  public void clear() {
    // Acquire in index order so that any future code path acquiring
    // multiple stripes can follow the same order and avoid deadlock.
    for (Stripe s : stripes) {
      s.lock.writeLock().lock();
    }
    try {
      for (Stripe s : stripes) {
        s.gset.clear();
      }
    } finally {
      for (int i = stripes.length - 1; i >= 0; i--) {
        stripes[i].lock.writeLock().unlock();
      }
    }
  }

  /**
   * @return the number of stripes. Exposed for tests.
   */
  @VisibleForTesting
  int getStripeCount() {
    return stripes.length;
  }

  /**
   * Chained iterator over all stripes. Does NOT acquire stripe locks —
   * the caller must hold external exclusion (see class Javadoc).
   */
  private static final class StripeIterator
      implements Iterator<INodeWithAdditionalFields> {
    private final Stripe[] stripes;
    private int stripeIdx;
    private Iterator<INodeWithAdditionalFields> current;

    StripeIterator(Stripe[] stripes) {
      this.stripes = stripes;
      this.stripeIdx = 0;
      this.current = stripes.length > 0 ? stripes[0].gset.iterator() : null;
      advance();
    }

    private void advance() {
      while (current != null && !current.hasNext()) {
        stripeIdx++;
        if (stripeIdx >= stripes.length) {
          current = null;
          return;
        }
        current = stripes[stripeIdx].gset.iterator();
      }
    }

    @Override
    public boolean hasNext() {
      return current != null && current.hasNext();
    }

    @Override
    public INodeWithAdditionalFields next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      INodeWithAdditionalFields result = current.next();
      advance();
      return result;
    }
  }
}
