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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Ref-counted allocator of {@link ReentrantReadWriteLock} instances keyed by
 * INode ID.
 *
 * <p>The pool creates an entry on first {@link #pin(long)} and evicts it
 * when the last reference is released via {@link #unpin(long)}. Both
 * operations are atomic per-key via {@link ConcurrentHashMap#compute}.
 *
 * <p><b>Locks are FAIR.</b> Fairness ensures writer progress on hot parent
 * directories under steady read load; unfair {@link ReentrantReadWriteLock}
 * does not guarantee writer progress. See the pilot design spec §2.2 for
 * the rationale behind this choice.
 *
 * <p><b>Thread-safety contract:</b> {@link #pin(long)} and {@link #unpin(long)}
 * may be called from any thread; mutation of {@link Entry#refs} happens
 * only inside {@code compute()} blocks, which are atomic on the key.
 *
 * <p>This class is package-private by design — it must not be reached from
 * outside {@code fgl.iip}. See mitigation #2 of the pilot design
 * (single lock-acquisition chokepoint).
 *
 * @see LockRef
 * @see <a href="file:../../../../../../../../../../../../docs/fgl/HDFS-17385-wave4-pilot-design.md">
 *      HDFS-17385 pilot design spec</a>
 */
@InterfaceAudience.Private
final class LockPool {

  @VisibleForTesting
  static final int DEFAULT_INITIAL_CAPACITY = 1024;

  /**
   * A pool entry: one {@link ReentrantReadWriteLock} plus a reference count.
   *
   * <p>The {@code refs} field is guarded by {@link ConcurrentHashMap#compute}
   * atomicity on the key — it MUST only be mutated inside a compute block.
   * Reading {@code refs} outside a compute block is allowed only for
   * diagnostics (e.g., {@link #refCount(long)}) and may return stale values.
   */
  static final class Entry {
    final ReentrantReadWriteLock lock;
    int refs;

    Entry() {
      // Fair: writer progress guarantee under hot read load.
      this.lock = new ReentrantReadWriteLock(true);
      this.refs = 1;
    }
  }

  private final ConcurrentHashMap<Long, Entry> pool;

  LockPool() {
    this(DEFAULT_INITIAL_CAPACITY);
  }

  LockPool(int initialCapacity) {
    this.pool = new ConcurrentHashMap<>(initialCapacity);
  }

  /**
   * Pin the entry for {@code inodeId}, creating it if absent.
   *
   * <p>The returned {@link Entry} is guaranteed to exist in the pool with
   * {@code refs >= 1} on return. The caller must eventually invoke
   * {@link #unpin(long)} to release the pin. Not calling {@code unpin}
   * leaks the pool entry.
   *
   * @param inodeId INode ID to pin
   * @return the entry, never {@code null}
   */
  Entry pin(long inodeId) {
    final Entry[] out = new Entry[1];
    pool.compute(inodeId, (k, existing) -> {
      if (existing == null) {
        Entry e = new Entry();
        out[0] = e;
        return e;
      }
      existing.refs++;
      out[0] = existing;
      return existing;
    });
    return out[0];
  }

  /**
   * Unpin the entry for {@code inodeId}. When the reference count hits
   * zero the entry is evicted from the pool.
   *
   * @param inodeId INode ID to unpin
   * @throws IllegalStateException if no entry exists for the given ID
   */
  void unpin(long inodeId) {
    pool.compute(inodeId, (k, existing) -> {
      if (existing == null) {
        throw new IllegalStateException(
            "unpin of unknown INode id=" + k + " — double-release or missing pin");
      }
      existing.refs--;
      return existing.refs == 0 ? null : existing;
    });
  }

  /**
   * @return the current number of entries in the pool. Exposed for metrics
   *         and testing; the value may lag by a few operations under
   *         concurrent churn.
   */
  int size() {
    return pool.size();
  }

  /**
   * Diagnostic accessor for the reference count of a specific INode ID.
   *
   * <p>The returned value reads {@code Entry.refs} outside of a
   * {@code compute()} block and may be stale. Intended for tests and
   * logging only.
   *
   * @param inodeId INode ID to query
   * @return current reference count, or {@code 0} if no entry exists
   */
  @VisibleForTesting
  int refCount(long inodeId) {
    Entry e = pool.get(inodeId);
    return e == null ? 0 : e.refs;
  }

  /**
   * @return {@code true} if an entry exists for the given INode ID.
   */
  @VisibleForTesting
  boolean contains(long inodeId) {
    return pool.containsKey(inodeId);
  }
}
