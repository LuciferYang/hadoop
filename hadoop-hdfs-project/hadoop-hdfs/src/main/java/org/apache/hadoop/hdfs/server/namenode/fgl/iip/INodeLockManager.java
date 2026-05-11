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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

/**
 * Hand-over-hand lock manager for the FGL_IIP mode (HDFS-17492, part of
 * the HDFS-17385 Phase II pilot).
 *
 * <p>Walks a path from root to target acquiring per-INode locks as it
 * goes, with lock mode (read / write) pre-determined per level based on
 * the requested {@link IIPAcquireMode}. Returns a {@link LockedIIP}
 * handle that the caller releases via {@code try-with-resources}.
 *
 * <h2>Walk algorithm</h2>
 *
 * <ol>
 *   <li>Check the nested-acquisition invariant: the current thread must
 *       NOT already hold an IIP lock ({@link #HELD_IIP_DEPTH} == 0).</li>
 *   <li>Parse the path into components via
 *       {@link INode#getPathComponents(String)}.</li>
 *   <li>Acquire {@code compatReadLock} with the remaining deadline
 *       budget.</li>
 *   <li>Walk from the root INode down each component. At each level:
 *     <ul>
 *       <li>Check for a symlink in an ancestor position — if present,
 *           throw {@link UnresolvedPathException} (the client retries
 *           with the resolved path; matches trunk's contract from
 *           {@code FSPermissionChecker.checkNotSymlink}).</li>
 *       <li>Acquire this level's lock — write if the level equals the
 *           mode's target depth, read otherwise. Locks are acquired
 *           from {@link LockPool} with the remaining budget.</li>
 *       <li>Look up the next component via
 *           {@link INodeDirectory#getChild} — safe because we hold the
 *           current level's read/write lock.</li>
 *     </ul>
 *   </li>
 *   <li>On path-component-not-found (child is null), stop the walk and
 *       leave the remaining {@code inodes[]} entries null. The returned
 *       {@link LockedIIP} represents the existing prefix; the RPC body
 *       is expected to interpret a null last INode as
 *       "target does not exist".</li>
 *   <li>Construct an {@link INodesInPath} from the captured components
 *       and INodes via {@link INodesInPath#fromComponentsAndInodes},
 *       wrap it in a {@link LockedIIP}, increment
 *       {@link #HELD_IIP_DEPTH}, and return.</li>
 * </ol>
 *
 * <h2>Lock ordering and invariants</h2>
 *
 * <ul>
 *   <li><b>Rule 1 (ancestor before descendant).</b> The walk acquires
 *       root first, then each child in order.</li>
 *   <li><b>Rule 2 (ancestor always read).</b> Only the target depth may
 *       receive a write lock (or the parent depth for PARENT_WRITE).
 *       All other levels are read-locked.</li>
 *   <li><b>No read→write upgrade.</b> Lock mode is pre-decided per
 *       level, so there is no call site that would attempt an upgrade.</li>
 *   <li><b>Compat-read held throughout.</b> Acquired before the walk
 *       and released after the last per-INode lock in {@link LockedIIP#close}.</li>
 *   <li><b>No nested acquisition.</b> {@link #HELD_IIP_DEPTH} must be
 *       {@code 0} on entry to {@link #acquire}; a non-zero value
 *       indicates a programming error.</li>
 * </ul>
 *
 * <h2>Failure handling</h2>
 *
 * <p>Any exception during the walk causes all previously-acquired locks
 * to be released in reverse acquisition order, followed by
 * {@code compatReadLock}. The exception then propagates to the caller.
 *
 * <h2>Thread affinity</h2>
 *
 * <p>The returned {@link LockedIIP} must be closed by the acquiring
 * thread. See {@link LockedIIP} Javadoc for details.
 *
 * <p>See {@code docs/fgl/HDFS-17385-wave4-pilot-design.md} §2.4 for the
 * full algorithmic spec and §1.8 for the lock-ordering rules.
 */
@InterfaceAudience.Private
public final class INodeLockManager {

  /**
   * Per-thread depth counter for IIP contexts. Incremented once by
   * {@link #acquire} on success; decremented by
   * {@link LockedIIP#close()}. Non-zero on entry to {@code acquire}
   * indicates forbidden nested acquisition.
   */
  static final ThreadLocal<Integer> HELD_IIP_DEPTH =
      ThreadLocal.withInitial(() -> 0);

  /**
   * Per-thread flag tracking whether the current IIP context holds
   * any per-INode write lock (i.e., {@link IIPAcquireMode#needsIIPWrite}
   * was true at acquisition time).
   *
   * <p>Used by {@code IIPBasedFSNamesystemLock.hasWriteLock} to
   * satisfy existing HDFS assertions like
   * {@code assert hasWriteLock(RwLockMode.FS)} when pilot code holds
   * an IIP write lock but not Phase I's FSLock. See the pilot design
   * spec §1.11 P4.
   *
   * <p>Set to {@code true} by a successful {@link #acquire} when the
   * mode's {@link IIPAcquireMode#needsIIPWrite()} is {@code true};
   * reset to {@code false} by {@link LockedIIP#close()}.
   */
  static final ThreadLocal<Boolean> HELD_IIP_WRITE =
      ThreadLocal.withInitial(() -> Boolean.FALSE);

  private final LockPool pool;
  private final ReentrantReadWriteLock compatLock;
  private final Duration defaultTimeout;
  /**
   * Supplier returning the current root INode directory. Held as a
   * supplier (not a captured reference) so that FSImage reload — which
   * REPLACES the root via {@code FSDirectory#rootDir = createRoot(...)}
   * — does not leave the lock manager pointing at a stale root.
   *
   * <p>Set lazily via {@link #setRootSupplier} (or {@link #setRootDir}
   * for tests) because {@code FSDirectory} construction depends on
   * {@code FSNamesystem} which depends on the lock manager — setter
   * injection breaks the cycle (see pilot design spec §2.13).
   *
   * <p>This is the fix for round-6 BUG #2: production code previously
   * captured a reference to the root INode at FSNamesystem
   * construction time, which became stale after any FSImage clear /
   * reload (e.g., during {@code dfsadmin -saveNamespace} followed by
   * {@code -refreshNamenodes}, or during HA failover-with-image-load).
   */
  private volatile java.util.function.Supplier<INodeDirectory> rootSupplier;

  /**
   * Create a lock manager.
   *
   * @param pool              lock pool for per-INode locks
   * @param compatLock        compat-namespace lock (shared with Phase I
   *                          fallback manager when composed)
   * @param defaultTimeout    default deadline for {@link #acquire}
   *                          calls that do not specify an override
   */
  public INodeLockManager(LockPool pool, ReentrantReadWriteLock compatLock,
      Duration defaultTimeout) {
    this.pool = pool;
    this.compatLock = compatLock;
    this.defaultTimeout = defaultTimeout;
    this.rootSupplier = null;
  }

  /**
   * Production injection: install a supplier that returns the current
   * root INode every time it is invoked. The supplier should typically
   * be {@code fsd::getRoot} so that FSImage reloads (which replace the
   * root) are picked up automatically.
   *
   * <p>Must be called before the first {@link #acquire} call.
   */
  public void setRootSupplier(java.util.function.Supplier<INodeDirectory> supplier) {
    this.rootSupplier = supplier;
  }

  /**
   * Test convenience: install a fixed root reference. The reference is
   * captured into a {@link java.util.function.Supplier}; if the test
   * later replaces the root, this method should be called again. Not
   * suitable for production where FSImage reload can replace the root.
   */
  public void setRootDir(INodeDirectory rootDir) {
    final INodeDirectory captured = rootDir;
    setRootSupplier(() -> captured);
  }

  /**
   * Hand-over-hand lock acquisition for a path and mode.
   *
   * @param path path to resolve
   * @param mode lock mode to apply
   * @return a {@link LockedIIP} handle owning all acquired locks
   * @throws InvalidPathException            if the path is invalid for
   *                                         the requested mode
   * @throws UnresolvedPathException         if a symlink appears in an
   *                                         ancestor position; clients
   *                                         retry with the resolved path
   * @throws PilotEnvelopeMissException      on other structural
   *                                         envelope misses (INode
   *                                         reference, etc.)
   * @throws LockAcquisitionTimeoutException if any lock cannot be
   *                                         acquired within the deadline
   * @throws InterruptedException            if the caller is interrupted
   *                                         before acquisition completes
   * @throws IOException                     wraps invalid state or
   *                                         path parsing errors
   */
  public LockedIIP acquire(String path, IIPAcquireMode mode)
      throws IOException, InterruptedException {
    return acquire(path, mode, defaultTimeout);
  }

  /**
   * Same as {@link #acquire(String, IIPAcquireMode)} but with a custom
   * timeout.
   */
  public LockedIIP acquire(String path, IIPAcquireMode mode,
      Duration timeout) throws IOException, InterruptedException {
    if (mode == null) {
      throw new IllegalArgumentException("mode must not be null");
    }
    if (!mode.isPilot()) {
      throw new UnsupportedOperationException(
          "mode " + mode + " is not implemented in the pilot; file a "
              + "follow-up ticket to implement it");
    }
    if (rootSupplier == null) {
      throw new IllegalStateException(
          "INodeLockManager root supplier has not been set — call "
              + "setRootSupplier or setRootDir before acquire");
    }
    // Rule: no nested IIP acquisition.
    if (HELD_IIP_DEPTH.get() > 0) {
      throw new IllegalStateException(
          "nested IIP acquisition is forbidden (depth="
              + HELD_IIP_DEPTH.get() + ")");
    }
    // Lock-ordering rule 5 (design spec §1.8): compat-write must never
    // be held when acquiring IIP locks. ADMIN_META is the only mode
    // that takes compat-write and it bypasses this method entirely.
    // Assert to catch any future caller that forgets.
    assert !compatLock.isWriteLockedByCurrentThread()
        : "compat-write held at IIP acquire — violates rule 5 "
          + "(compat-write never held under IIP lock)";

    byte[][] components = INode.getPathComponents(path);
    if (components == null || components.length == 0) {
      throw new InvalidPathException("empty path: " + path);
    }
    int maxLockDepth = computeMaxLockDepth(mode, components.length);
    if (maxLockDepth == INVALID_WRITE_DEPTH) {
      throw new InvalidPathException(
          "mode " + mode + " requires a non-root path: " + path);
    }
    // For PATH_READ, every locked level is a read.
    // For PARENT_WRITE, the deepest locked level (the parent) gets
    // the write lock; the target below it is populated unlocked.
    // For PATH_WRITE, the deepest locked level (the target) gets the
    // write lock; all ancestors are read-locked.
    // computeMaxLockDepth() returns parent-depth for PARENT_WRITE and
    // target-depth for PATH_READ/PATH_WRITE, so writeLockDepth =
    // maxLockDepth is the correct deepest-is-write rule for all write
    // modes. Read modes get -1 (no depth matches, all locks are read).
    final int writeLockDepth = mode.needsIIPWrite() ? maxLockDepth : -1;

    long deadlineNanos = computeDeadlineNanos(timeout);

    // Phase 1: acquire compat-read with deadline budget.
    long compatRemaining = deadlineNanos - System.nanoTime();
    if (compatRemaining <= 0L
        || !compatLock.readLock().tryLock(
            compatRemaining, TimeUnit.NANOSECONDS)) {
      throw new LockAcquisitionTimeoutException(
          "timed out acquiring compat-read before IIP walk for: " + path);
    }

    // Phase 2: hand-over-hand walk acquiring per-INode locks up to
    // maxLockDepth inclusive. For PATH_READ, maxLockDepth is the
    // target (components.length - 1). For PARENT_WRITE, maxLockDepth
    // is the parent (components.length - 2); the target is discovered
    // in a final unlocked getChild below but is NOT locked — the
    // parent write lock already protects any children-list mutation
    // involving the target.
    List<LockRef> held = new ArrayList<>(components.length);
    INode[] inodes = new INode[components.length];
    boolean success = false;
    try {
      // Re-resolve the root on every acquire so that FSImage reloads
      // (which REPLACE the root INode in FSDirectory) are picked up
      // automatically. See setRootSupplier Javadoc and round-6 BUG #2.
      INode current = rootSupplier.get();
      if (current == null) {
        throw new IllegalStateException(
            "rootSupplier returned null — FSDirectory not yet initialized?");
      }
      for (int depth = 0; depth <= maxLockDepth; depth++) {
        if (current == null) {
          // Path prefix does not fully exist. Leave the remaining
          // entries null; the RPC body handles the null target case.
          break;
        }

        // Envelope miss: INodeReference in an ANY position. References
        // are created when rename moves an INode that participates in
        // a snapshot; they persist on live paths until all snapshots
        // are deleted. The pilot does not have a coherent story for
        // references — the reference's INode ID is distinct from the
        // wrapped target's ID, so locking one does not protect the
        // other. Round-7 BUG #3 fix: bail out and let the legacy
        // path resolve through the reference.
        if (current.isReference()) {
          throw new PilotEnvelopeMissException(
              "INodeReference in path component at depth " + depth
                  + " (snapshot territory): " + path);
        }

        // Symlink in an ancestor position: throw UnresolvedPathException
        // directly so the client retries with the resolved path. This
        // matches trunk's contract (see FSPermissionChecker.checkNotSymlink)
        // and avoids an unnecessary global-lock fallback for a request
        // that would fail anyway. A symlink AS the target is acceptable
        // (callers may treat it with READ_LINK semantics), so only
        // intercept when depth < last.
        if (current instanceof INodeSymlink
            && depth < components.length - 1) {
          throw newUnresolvedPath(components, depth, (INodeSymlink) current);
        }

        boolean writeHere = (depth == writeLockDepth);
        held.add(LockRef.acquire(pool, current.getId(), writeHere,
            deadlineNanos));
        inodes[depth] = current;

        // Walk one more step if there is another level to lock.
        if (depth < maxLockDepth) {
          // Use isDirectory() rather than 'instanceof INodeDirectory'
          // because INodeReference.isDirectory() returns true for
          // references wrapping a directory. (Round-7 BUG #3 also
          // catches references above; this is defense-in-depth.)
          if (!current.isDirectory()) {
            // Non-directory in an ancestor position: path cannot
            // resolve further. Remaining inodes stay null.
            break;
          }
          current = current.asDirectory().getChild(
              components[depth + 1], Snapshot.CURRENT_STATE_ID);
        }
      }

      // For PARENT_WRITE, populate the target INode (one level beyond
      // the parent) without acquiring a lock on it. If the target does
      // not exist, this leaves inodes[maxLockDepth + 1] null — which
      // is exactly what a create RPC wants to see.
      if (mode == IIPAcquireMode.PARENT_WRITE
          && maxLockDepth + 1 < components.length
          && inodes[maxLockDepth] != null
          && inodes[maxLockDepth].isDirectory()) {
        inodes[maxLockDepth + 1] =
            inodes[maxLockDepth].asDirectory().getChild(
                components[maxLockDepth + 1], Snapshot.CURRENT_STATE_ID);
      }

      INodesInPath iip = INodesInPath.fromComponentsAndInodes(
          components, inodes);
      // Allocate LockedIIP BEFORE incrementing the depth counter so
      // that an OOM or other exception during allocation does not
      // leave the counter at +1 with no handle to decrement it.
      boolean wasWrite = mode.needsIIPWrite();
      LockedIIP result = new LockedIIP(iip, Collections.unmodifiableList(
          new ArrayList<>(held)), compatLock.readLock(), wasWrite);
      HELD_IIP_DEPTH.set(1);
      if (wasWrite) {
        HELD_IIP_WRITE.set(Boolean.TRUE);
      }
      success = true;
      return result;
    } finally {
      if (!success) {
        // Release per-INode locks in reverse order, then compat-read.
        for (int i = held.size() - 1; i >= 0; i--) {
          try {
            held.get(i).close();
          } catch (RuntimeException ignored) {
            // Best-effort release; continue.
          }
        }
        compatLock.readLock().unlock();
      }
    }
  }

  /**
   * Two-path acquisition for rename. Acquires read locks on
   * ancestors of both paths, then write-locks both parents in
   * ascending INode-ID order (§1.8 rule 3) to prevent deadlock
   * with a concurrent reverse rename.
   *
   * <p>Returns {@link LockedRenameIIPs} carrying both pre-resolved
   * IIPs. Targets (one level below each parent) are populated
   * without locking — protected by the parents' write locks.
   *
   * @see docs/fgl/HDFS-17385-wave4-pilot-design.md §1.4, §1.8
   */
  public LockedRenameIIPs acquireRename(String srcPath, String dstPath)
      throws IOException, InterruptedException {
    return acquireRename(srcPath, dstPath, defaultTimeout);
  }

  /**
   * Same as {@link #acquireRename(String, String)} with custom timeout.
   */
  public LockedRenameIIPs acquireRename(String srcPath, String dstPath,
      Duration timeout) throws IOException, InterruptedException {
    if (srcPath == null || dstPath == null) {
      throw new IllegalArgumentException("rename paths must not be null");
    }
    if (HELD_IIP_DEPTH.get() > 0) {
      throw new IllegalStateException("nested IIP acquisition forbidden");
    }
    assert !compatLock.isWriteLockedByCurrentThread()
        : "compat-write held at rename acquire — violates rule 5";

    byte[][] srcComponents = INode.getPathComponents(srcPath);
    byte[][] dstComponents = INode.getPathComponents(dstPath);
    if (srcComponents == null || srcComponents.length < 2) {
      throw new InvalidPathException("rename src requires non-root: " + srcPath);
    }
    if (dstComponents == null || dstComponents.length < 2) {
      throw new InvalidPathException("rename dst requires non-root: " + dstPath);
    }

    long deadlineNanos = computeDeadlineNanos(timeout);

    // Phase 1: compat-read
    long remaining = deadlineNanos - System.nanoTime();
    if (remaining <= 0L || !compatLock.readLock().tryLock(
        remaining, TimeUnit.NANOSECONDS)) {
      throw new LockAcquisitionTimeoutException("timed out for rename");
    }

    List<LockRef> held = new ArrayList<>();
    boolean success = false;
    try {
      INode root = rootSupplier.get();
      if (root == null) {
        throw new IllegalStateException("rootSupplier returned null");
      }

      // Walk src ancestors (read-lock root through src-parent's parent)
      INode[] srcInodes = new INode[srcComponents.length];
      int srcParentDepth = srcComponents.length - 2;
      walkAncestors(root, srcComponents, srcInodes, held, deadlineNanos,
          srcParentDepth - 1);

      // Walk dst ancestors (read-lock root through dst-parent's parent)
      INode[] dstInodes = new INode[dstComponents.length];
      int dstParentDepth = dstComponents.length - 2;
      walkAncestors(root, dstComponents, dstInodes, held, deadlineNanos,
          dstParentDepth - 1);

      // Discover parents (unlocked) via getChild on the last locked
      // ancestor.
      discoverParent(srcComponents, srcInodes, srcParentDepth);
      discoverParent(dstComponents, dstInodes, dstParentDepth);

      INode srcParent = srcInodes[srcParentDepth];
      INode dstParent = dstInodes[dstParentDepth];

      // Write-lock both parents in ascending INode-ID order (rule 3).
      if (srcParent != null && dstParent != null) {
        if (srcParent.getId() == dstParent.getId()) {
          // Same parent (rename within same dir)
          held.add(LockRef.acquire(pool, srcParent.getId(), true,
              deadlineNanos));
        } else if (srcParent.getId() < dstParent.getId()) {
          held.add(LockRef.acquire(pool, srcParent.getId(), true,
              deadlineNanos));
          held.add(LockRef.acquire(pool, dstParent.getId(), true,
              deadlineNanos));
        } else {
          held.add(LockRef.acquire(pool, dstParent.getId(), true,
              deadlineNanos));
          held.add(LockRef.acquire(pool, srcParent.getId(), true,
              deadlineNanos));
        }
      }

      // Populate targets (unlocked, protected by parent write locks)
      discoverTarget(srcComponents, srcInodes, srcParentDepth);
      discoverTarget(dstComponents, dstInodes, dstParentDepth);

      INodesInPath srcIIP = INodesInPath.fromComponentsAndInodes(
          srcComponents, srcInodes);
      INodesInPath dstIIP = INodesInPath.fromComponentsAndInodes(
          dstComponents, dstInodes);

      LockedRenameIIPs result = new LockedRenameIIPs(srcIIP, dstIIP,
          Collections.unmodifiableList(new ArrayList<>(held)),
          compatLock.readLock());
      HELD_IIP_DEPTH.set(1);
      HELD_IIP_WRITE.set(Boolean.TRUE);
      success = true;
      return result;
    } finally {
      if (!success) {
        for (int i = held.size() - 1; i >= 0; i--) {
          try { held.get(i).close(); } catch (RuntimeException ignored) {}
        }
        compatLock.readLock().unlock();
      }
    }
  }

  /**
   * Walk and read-lock ancestors from root to {@code maxDepth}
   * inclusive. Used by {@link #acquireRename}.
   */
  private void walkAncestors(INode root, byte[][] components,
      INode[] inodes, List<LockRef> held, long deadlineNanos,
      int maxDepth) throws IOException, InterruptedException {
    INode current = root;
    for (int depth = 0; depth <= maxDepth && current != null; depth++) {
      if (current.isReference()) {
        throw new PilotEnvelopeMissException(
            "INodeReference in rename ancestor at depth " + depth);
      }
      if (current instanceof INodeSymlink && depth < components.length - 1) {
        throw newUnresolvedPath(components, depth, (INodeSymlink) current);
      }
      held.add(LockRef.acquire(pool, current.getId(), false,
          deadlineNanos));
      inodes[depth] = current;
      if (depth < maxDepth && current.isDirectory()) {
        current = current.asDirectory().getChild(
            components[depth + 1], Snapshot.CURRENT_STATE_ID);
      }
    }
  }

  /** Discover the parent INode without locking it. */
  private void discoverParent(byte[][] components, INode[] inodes,
      int parentDepth) {
    int grandparentDepth = parentDepth - 1;
    if (grandparentDepth >= 0 && inodes[grandparentDepth] != null
        && inodes[grandparentDepth].isDirectory()) {
      inodes[parentDepth] = inodes[grandparentDepth].asDirectory()
          .getChild(components[parentDepth], Snapshot.CURRENT_STATE_ID);
    }
  }

  /** Discover the target INode (one level below parent) without locking. */
  private void discoverTarget(byte[][] components, INode[] inodes,
      int parentDepth) {
    int targetDepth = parentDepth + 1;
    if (targetDepth < components.length
        && inodes[parentDepth] != null
        && inodes[parentDepth].isDirectory()) {
      inodes[targetDepth] = inodes[parentDepth].asDirectory()
          .getChild(components[targetDepth], Snapshot.CURRENT_STATE_ID);
    }
  }

  /**
   * Build the {@link UnresolvedPathException} for a symlink discovered
   * at {@code depth} during the IIP walk. Mirrors the constructor call
   * in {@code FSPermissionChecker.checkNotSymlink} so clients see the
   * same exception they would get from the legacy traversal path.
   *
   * @param components path components being walked
   * @param depth      zero-based index of the symlink within components
   * @param link       the symlink INode
   */
  private static UnresolvedPathException newUnresolvedPath(
      byte[][] components, int depth, INodeSymlink link) {
    final int last = components.length - 1;
    final String path = pathString(components, 0, last);
    final String preceding = pathString(components, 0, depth - 1);
    final String remainder = pathString(components, depth + 1, last);
    final String target = link.getSymlinkString();
    return new UnresolvedPathException(path, preceding, remainder, target);
  }

  /**
   * Mirrors {@code FSPermissionChecker.getPath(components, start, end)}:
   * build the path string for the inclusive range {@code [start, end]}.
   * {@link DFSUtil#byteArray2PathString} returns the empty string when
   * {@code length == 0}, so {@code end == start - 1} produces "".
   */
  private static String pathString(byte[][] components, int start, int end) {
    return DFSUtil.byteArray2PathString(components, start, end - start + 1);
  }

  /**
   * Sentinel returned by {@link #computeMaxLockDepth} when the
   * requested mode is invalid for a given path length (e.g.,
   * PARENT_WRITE or PATH_WRITE on the root "/").
   */
  static final int INVALID_WRITE_DEPTH = Integer.MIN_VALUE;

  /**
   * @return the deepest zero-based depth that the walk should acquire
   *         a lock at (the target for PATH_READ/PATH_WRITE, the parent
   *         for PARENT_WRITE), or {@link #INVALID_WRITE_DEPTH} if the
   *         mode cannot be applied to a path of the given length.
   */
  @VisibleForTesting
  static int computeMaxLockDepth(IIPAcquireMode mode, int pathLen) {
    switch (mode) {
    case PATH_READ:
      // Lock every level from root to target (read on all).
      return pathLen - 1;
    case PATH_WRITE:
    case ANCESTOR_WRITE:
      // Lock every level from root to target; target gets write, all
      // ancestors get read. Requires at least 2 components — the mode
      // is meaningless on the root INode. PATH_WRITE and ANCESTOR_WRITE
      // share the same lock plan; distinct modes express intent
      // (single-node mutation vs subtree operation).
      if (pathLen < 2) {
        return INVALID_WRITE_DEPTH;
      }
      return pathLen - 1;
    case PARENT_WRITE:
      // Parent is the second-to-last component. Only valid for paths
      // with at least 2 components (i.e., not root).
      if (pathLen < 2) {
        return INVALID_WRITE_DEPTH;
      }
      return pathLen - 2;
    default:
      // RENAME_WRITE, GLOBAL_READ, ADMIN_META — not reachable
      // because acquire() pre-checks isPilot().
      throw new IllegalStateException(
          "computeMaxLockDepth called with non-pilot mode: " + mode);
    }
  }

  private long computeDeadlineNanos(Duration timeout) {
    long timeoutNanos = timeout.toNanos();
    if (timeoutNanos < 0L) {
      timeoutNanos = 0L;
    }
    long now = System.nanoTime();
    try {
      return Math.addExact(now, timeoutNanos);
    } catch (ArithmeticException overflow) {
      return Long.MAX_VALUE;
    }
  }

  /**
   * @return the underlying {@link LockPool}. Exposed for tests.
   */
  @VisibleForTesting
  LockPool getLockPool() {
    return pool;
  }
}
