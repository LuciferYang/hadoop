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

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Closed taxonomy of lock-acquisition patterns for directory-tree locking
 * (the {@code FGL_IIP} mode introduced by the HDFS-17385 Phase II pilot).
 *
 * <p>Every migrated RPC picks exactly one mode. The mode tells
 * {@code INodeLockManager} (U3) which lock plan to build: which INodes to
 * read-lock, which to write-lock, and in what order.
 *
 * <p><b>Mitigation #1 (full taxonomy upfront).</b> The enum declares ALL
 * modes the pilot design anticipates, even the ones not implemented in
 * the pilot itself. Modes marked {@link Impl#DEFERRED} throw
 * {@link UnsupportedOperationException} when their owning RPC is migrated
 * — but declaring them now prevents post-pilot migrations from inventing
 * ad-hoc modes and fragmenting the taxonomy.
 *
 * <p><b>Mitigation #2 (single chokepoint).</b> This enum is
 * package-private; nothing outside {@code fgl.iip} decides which mode
 * an RPC uses. The choice is made inside the lock manager or in the
 * pilot envelope.
 *
 * <h2>Mode semantics</h2>
 *
 * <table>
 *   <caption>Lock grid per mode</caption>
 *   <tr><th>Mode</th><th>Ancestors</th><th>Target</th><th>Compat lock</th><th>Status</th></tr>
 *   <tr><td>{@link #PATH_READ}</td>
 *       <td>read (root → leaf)</td><td>read</td><td>read</td><td>PILOT</td></tr>
 *   <tr><td>{@link #PARENT_WRITE}</td>
 *       <td>read (root → parent's parent)</td><td>parent: write; target: not locked (absent for create)</td><td>read</td><td>PILOT</td></tr>
 *   <tr><td>{@link #PATH_WRITE}</td>
 *       <td>read (root → leaf)</td><td>write</td><td>read</td><td>PILOT</td></tr>
 *   <tr><td>{@link #ANCESTOR_WRITE}</td>
 *       <td>read (root → subtree root's parent)</td><td>subtree root: write; descendants: not locked</td><td>read</td><td>DEFERRED</td></tr>
 *   <tr><td>{@link #RENAME_WRITE}</td>
 *       <td>read on both paths</td><td>both parents: write in ascending-INode-ID order</td><td>read</td><td>DEFERRED</td></tr>
 *   <tr><td>{@link #GLOBAL_READ}</td>
 *       <td>n/a</td><td>n/a</td><td>read</td><td>FALLBACK</td></tr>
 *   <tr><td>{@link #ADMIN_META}</td>
 *       <td>n/a</td><td>n/a</td><td>write</td><td>FALLBACK</td></tr>
 * </table>
 *
 * <p>See {@code docs/fgl/HDFS-17385-wave4-pilot-design.md} §1.4 for the
 * full design rationale, §1.8 for the lock-ordering invariants, and
 * §6.4 for the migration checklist that every RPC migration PR follows.
 */
@InterfaceAudience.Private
public enum IIPAcquireMode {

  // ========= Pilot-implemented =========

  /**
   * Read-only access to a full path. Acquires read locks on every INode
   * along the resolved path from root to target. Used by the pilot's
   * {@code getFileInfo} RPC.
   */
  PATH_READ(Impl.PILOT),

  /**
   * Write access to the parent of the path target, typically for adding
   * or removing a child. Acquires read locks on every ancestor above the
   * parent, then a write lock on the parent. The target itself is not
   * locked (it doesn't exist yet for {@code create} / {@code mkdir}).
   * Used by the pilot's scoped {@code create} RPC.
   */
  PARENT_WRITE(Impl.PILOT),

  /**
   * Write access to a single INode at the path target (e.g.,
   * {@code setPermission}, {@code setOwner}, {@code setTimes},
   * {@code setReplication}). Acquires read locks on every ancestor
   * from root to parent, then a write lock on the target itself. The
   * target must exist; this mode cannot be used to create a new INode
   * (callers must use {@link #PARENT_WRITE} for that).
   *
   * <p>Required path length is 2 (the shortest path that has both an
   * ancestor chain and a target — e.g., {@code /file}). Acquiring on
   * the root {@code "/"} throws {@link org.apache.hadoop.fs.InvalidPathException}.
   */
  PATH_WRITE(Impl.PILOT),

  // ========= Fallback modes =========

  /**
   * Read-only fallback that uses only the compat namespace lock in read
   * mode. No per-INode locks. Used by un-migrated read RPCs and as the
   * read-side equivalent for legacy implementations
   * ({@code GlobalFSNamesystemLock}, {@code FineGrainedFSNamesystemLock}).
   */
  GLOBAL_READ(Impl.FALLBACK),

  /**
   * Administrative operation that must be globally exclusive (e.g.,
   * {@code saveNamespace}, admin-only meta changes). Acquires the compat
   * namespace lock in write mode, which quiesces all IIP acquisitions.
   * Used as the escape hatch for operations that cannot fit the IIP
   * model.
   */
  ADMIN_META(Impl.FALLBACK),

  // ========= Declared but deferred =========

  /**
   * Write access to a subtree root, for operations that affect all
   * descendants (e.g., {@code delete -r}, {@code chown -R},
   * {@code setQuota} on a subtree). Acquires read locks on ancestors of
   * the subtree root and a write lock on the subtree root itself;
   * descendants are iterated without per-INode locks, under the
   * protection of the subtree root's write lock.
   */
  ANCESTOR_WRITE(Impl.DEFERRED),

  /**
   * Write access to two parents for a rename operation. Acquires the two
   * parent write locks in ascending-INode-ID order to prevent deadlock
   * with a concurrent reverse rename. See §1.8 rule 3 of the pilot spec.
   */
  RENAME_WRITE(Impl.DEFERRED);

  // -----------------------------------------------------------------

  /**
   * Implementation status for each mode. See class Javadoc for the full
   * rationale behind declaring deferred modes alongside pilot modes.
   */
  public enum Impl {
    /** Implemented in the pilot; safe to use from pilot RPC migrations. */
    PILOT,
    /**
     * Routes through the compat namespace lock instead of per-INode
     * locks. Always available regardless of pilot readiness.
     */
    FALLBACK,
    /**
     * Declared in the taxonomy but not implemented. The first RPC
     * migration that needs this mode must implement it and move it to
     * {@link #PILOT}.
     */
    DEFERRED
  }

  private final Impl impl;

  IIPAcquireMode(Impl impl) {
    this.impl = impl;
  }

  /**
   * @return the implementation status of this mode.
   */
  public Impl impl() {
    return impl;
  }

  /**
   * @return {@code true} if this mode is fully implemented by the pilot
   *         and can be used from migrated RPCs.
   */
  public boolean isPilot() {
    return impl == Impl.PILOT;
  }

  /**
   * @return {@code true} if this mode routes through the compat namespace
   *         lock instead of acquiring per-INode locks. Fallback modes
   *         are always available; pilot modes are not.
   */
  public boolean isFallback() {
    return impl == Impl.FALLBACK;
  }

  /**
   * @return {@code true} if this mode is declared but unimplemented.
   *         Acquiring with a deferred mode should throw
   *         {@link UnsupportedOperationException} with a clear message
   *         directing the caller to file a follow-up ticket to implement
   *         the mode.
   */
  public boolean isDeferred() {
    return impl == Impl.DEFERRED;
  }

  /**
   * Does this mode acquire any per-INode write lock?
   *
   * <p>Used by {@code INodeLockManager} (U3) when planning the walk: if
   * {@code true}, the mode specifies a write lock on at least one INode
   * along the path (the parent, the target, a subtree root, or a pair
   * of rename targets).
   *
   * <p>Note: {@link #ADMIN_META} returns {@code false} here because it
   * does not take an IIP-level write lock — it routes entirely through
   * the compat namespace lock. Callers asking "does this mode need the
   * compat lock in write mode?" should use {@link #needsCompatWrite()}.
   *
   * @return {@code true} if the mode specifies any per-INode write lock
   */
  public boolean needsIIPWrite() {
    switch (this) {
    case PARENT_WRITE:
    case PATH_WRITE:
    case ANCESTOR_WRITE:
    case RENAME_WRITE:
      return true;
    case PATH_READ:
    case GLOBAL_READ:
    case ADMIN_META:
      return false;
    default:
      throw new IllegalStateException("unhandled mode: " + this);
    }
  }

  /**
   * Does this mode require the compat namespace lock in write mode?
   *
   * <p>Only {@link #ADMIN_META} returns {@code true}. All other modes
   * either take compat-read (pilot and fallback reads) or take compat-read
   * plus per-INode locks (pilot writes).
   *
   * <p>Used by {@code IIPBasedFSNamesystemLock} to decide whether to
   * acquire compat-read or compat-write when {@code lockPath} is called
   * with a fallback mode.
   *
   * @return {@code true} if the mode takes compat-write
   */
  public boolean needsCompatWrite() {
    return this == ADMIN_META;
  }
}
