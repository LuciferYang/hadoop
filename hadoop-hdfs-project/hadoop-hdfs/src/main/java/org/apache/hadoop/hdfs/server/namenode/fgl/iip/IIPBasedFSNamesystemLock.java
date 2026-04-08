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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.fgl.FSNLockManager;
import org.apache.hadoop.hdfs.server.namenode.fgl.FineGrainedFSNamesystemLock;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;

/**
 * Phase II (FGL_IIP) lock manager. Composes Phase I's
 * {@link FineGrainedFSNamesystemLock} as a fallback for un-migrated
 * RPCs and exposes {@link #lockPath(String, IIPAcquireMode)} as the
 * entry point for migrated RPCs.
 *
 * <p>Users enable FGL_IIP mode by setting
 * {@code dfs.namenode.lock.model.provider.class} to this class. The
 * constructor signature matches the existing
 * {@link FineGrainedFSNamesystemLock} constructor so that
 * {@code FSNamesystem#createLock} can instantiate it by reflection
 * without modification.
 *
 * <h2>Composition model</h2>
 *
 * <p>All {@link FSNLockManager} interface methods except
 * {@link #lockPath} delegate to the composed
 * {@link FineGrainedFSNamesystemLock}. The composition reuses Phase
 * I's FSLock as the "compat namespace lock" that coordinates between
 * IIP acquisitions and un-migrated RPCs — this is achieved by passing
 * the underlying {@link ReentrantReadWriteLock} (obtained via
 * {@code FSNamesystemLock.getLockForTests()}) to
 * {@link INodeLockManager}. That raw RRWL is the same instance the
 * composed fallback acquires when an un-migrated RPC calls
 * {@code readLock(RwLockMode.FS)}, so pilot readers holding compat-read
 * are mutually exclusive with un-migrated writers holding Phase I's
 * FSLock write.
 *
 * <h2>{@code hasWriteLock} / {@code hasReadLock} override</h2>
 *
 * <p>Existing HDFS code has assertions like
 * {@code assert fsn.hasWriteLock(RwLockMode.FS)} on migrated code
 * paths. Under FGL_IIP, migrated code holds IIP locks (tracked via
 * {@link INodeLockManager#HELD_IIP_DEPTH} and
 * {@link INodeLockManager#HELD_IIP_WRITE}), not Phase I's FSLock.
 * These methods are overridden to return {@code true} when the
 * current thread is inside a LockedIIP context of the appropriate
 * kind — making existing assertions pass transparently without
 * modification. See the pilot design spec §1.11 P4.
 *
 * <h2>{@code lockPath} dispatch</h2>
 *
 * <p>Migrated RPCs call {@code fsLock.lockPath(path, mode)}. This
 * method delegates to {@link INodeLockManager#acquire} for the
 * hand-over-hand walk. Non-pilot modes (DEFERRED / FALLBACK) are
 * rejected inside the walk with a clear error — the pilot does not
 * yet support them.
 *
 * <h2>Initialization order</h2>
 *
 * <p>{@link INodeLockManager} needs a reference to the root INode
 * directory, but {@code FSDirectory} is constructed after
 * {@code FSNamesystem} and depends on it (circular). The
 * {@link #setRootDir} setter is called by {@code FSNamesystem.init}
 * once {@code FSDirectory} is ready. Until {@code setRootDir} is
 * called, {@link #lockPath} throws {@code IllegalStateException} —
 * this matches the existing behavior of {@code FSDirectory}-dependent
 * operations during NN startup.
 *
 * <p>See {@code docs/fgl/HDFS-17385-wave4-pilot-design.md} §2.6 for
 * the full design rationale.
 */
@InterfaceAudience.Private
public class IIPBasedFSNamesystemLock implements FSNLockManager {

  /**
   * Default lock-acquisition timeout for IIP operations. The spec
   * (§5.1) sets this at 5s; operators can tune it via
   * {@code dfs.namenode.fgl.iip.lock.timeout.ms} when the
   * configuration key is wired up in a future U0 commit. For now the
   * constant is hardcoded to avoid introducing a new config key ahead
   * of U8.
   */
  private static final Duration DEFAULT_IIP_TIMEOUT =
      Duration.ofMillis(5000);

  private final FineGrainedFSNamesystemLock fallback;
  private final INodeLockManager inodeLockManager;

  /**
   * Constructor matching {@link FineGrainedFSNamesystemLock} so that
   * {@code FSNamesystem#createLock} can instantiate this class by
   * reflection.
   */
  public IIPBasedFSNamesystemLock(Configuration conf,
      MutableRatesWithAggregation aggregation) {
    this.fallback = new FineGrainedFSNamesystemLock(conf, aggregation);
    // Reuse Phase I's FSLock as the compat namespace lock. The
    // ReentrantReadWriteLock obtained from getLockForTests() is the
    // SAME instance that fallback.readLock(FS) acquires, so IIP
    // readers holding compat-read are mutually exclusive with
    // un-migrated writers holding fallback's FSLock write.
    ReentrantReadWriteLock compatLock = this.fallback.getFsLock()
        .getLockForTests();
    this.inodeLockManager = new INodeLockManager(
        new LockPool(), compatLock, DEFAULT_IIP_TIMEOUT);
  }

  /**
   * Production injection: install the {@link FSDirectory} so that the
   * internal {@link INodeLockManager} can re-resolve the root INode on
   * every {@code acquire} call. Must be called once
   * {@code FSDirectory} has been constructed (typically inside
   * {@code FSNamesystem}'s constructor).
   *
   * <p>This is the production fix for round-6 BUG #2: previously the
   * lock manager captured the root INode at injection time, which
   * became stale after FSImage clear/reload. Holding {@code fsd}
   * itself and calling {@code fsd.getRoot()} per acquire keeps the
   * root reference fresh.
   */
  public void setFSDirectory(FSDirectory fsd) {
    this.inodeLockManager.setRootSupplier(fsd::getRoot);
  }

  /**
   * Test convenience: install a fixed root reference. Delegates to
   * {@link INodeLockManager#setRootDir} which wraps the reference in
   * a fixed supplier. NOT suitable for production where FSImage
   * reload can replace the root.
   */
  @VisibleForTesting
  public void setRootDir(INodeDirectory rootDir) {
    this.inodeLockManager.setRootDir(rootDir);
  }

  /**
   * @return the composed Phase I lock manager. Package-private so
   *         that pilot RPC handlers in FSNamesystem can delegate
   *         envelope-miss cases directly.
   */
  @VisibleForTesting
  public FineGrainedFSNamesystemLock fallback() {
    return fallback;
  }

  /**
   * @return the underlying {@link INodeLockManager}. Exposed for tests.
   */
  @VisibleForTesting
  INodeLockManager inodeLockManager() {
    return inodeLockManager;
  }

  // ================================================================
  // lockPath — primary entry point for migrated RPCs
  // ================================================================

  @Override
  public LockedIIP lockPath(String path, IIPAcquireMode mode)
      throws IOException, InterruptedException {
    return inodeLockManager.acquire(path, mode);
  }

  // ================================================================
  // hasWriteLock / hasReadLock overrides (P4 from the pilot design)
  // ================================================================

  @Override
  public boolean hasWriteLock(RwLockMode lockMode) {
    // Delegate to the fallback first — if Phase I's FSLock / BMLock /
    // GLOBAL is actually held, return true as before. Otherwise,
    // consult the IIP write flag for FS / GLOBAL assertions.
    if (fallback.hasWriteLock(lockMode)) {
      return true;
    }
    if (lockMode == RwLockMode.FS || lockMode == RwLockMode.GLOBAL) {
      return INodeLockManager.HELD_IIP_WRITE.get();
    }
    // BM: delegate only. IIP does not track BM locks.
    return false;
  }

  @Override
  public boolean hasReadLock(RwLockMode lockMode) {
    // Same pattern: delegate first, then check IIP context for
    // FS / GLOBAL.
    if (fallback.hasReadLock(lockMode)) {
      return true;
    }
    if (lockMode == RwLockMode.FS || lockMode == RwLockMode.GLOBAL) {
      // Holding any IIP lock (read or write) implies the thread is
      // inside a scope that would traditionally have held FS read.
      return INodeLockManager.HELD_IIP_DEPTH.get() > 0;
    }
    return false;
  }

  // ================================================================
  // All other FSNLockManager methods: straight delegation
  // ================================================================

  @Override
  public void readLock(RwLockMode lockMode) {
    fallback.readLock(lockMode);
  }

  @Override
  public void readLockInterruptibly(RwLockMode lockMode)
      throws InterruptedException {
    fallback.readLockInterruptibly(lockMode);
  }

  @Override
  public void readUnlock(RwLockMode lockMode, String opName) {
    fallback.readUnlock(lockMode, opName);
  }

  @Override
  public void readUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier) {
    fallback.readUnlock(lockMode, opName, lockReportInfoSupplier);
  }

  @Override
  public void writeLock(RwLockMode lockMode) {
    fallback.writeLock(lockMode);
  }

  @Override
  public void writeLockInterruptibly(RwLockMode lockMode)
      throws InterruptedException {
    fallback.writeLockInterruptibly(lockMode);
  }

  @Override
  public void writeUnlock(RwLockMode lockMode, String opName) {
    fallback.writeUnlock(lockMode, opName);
  }

  @Override
  public void writeUnlock(RwLockMode lockMode, String opName,
      boolean suppressWriteLockReport) {
    fallback.writeUnlock(lockMode, opName, suppressWriteLockReport);
  }

  @Override
  public void writeUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier) {
    fallback.writeUnlock(lockMode, opName, lockReportInfoSupplier);
  }

  @Override
  public int getReadHoldCount(RwLockMode lockMode) {
    return fallback.getReadHoldCount(lockMode);
  }

  @Override
  public int getQueueLength(RwLockMode lockMode) {
    return fallback.getQueueLength(lockMode);
  }

  @Override
  public long getNumOfReadLockLongHold(RwLockMode lockMode) {
    return fallback.getNumOfReadLockLongHold(lockMode);
  }

  @Override
  public long getNumOfWriteLockLongHold(RwLockMode lockMode) {
    return fallback.getNumOfWriteLockLongHold(lockMode);
  }

  @Override
  public boolean isMetricsEnabled() {
    return fallback.isMetricsEnabled();
  }

  @Override
  public void setMetricsEnabled(boolean metricsEnabled) {
    fallback.setMetricsEnabled(metricsEnabled);
  }

  @Override
  public void setReadLockReportingThresholdMs(
      long readLockReportingThresholdMs) {
    fallback.setReadLockReportingThresholdMs(readLockReportingThresholdMs);
  }

  @Override
  public long getReadLockReportingThresholdMs() {
    return fallback.getReadLockReportingThresholdMs();
  }

  @Override
  public void setWriteLockReportingThresholdMs(
      long writeLockReportingThresholdMs) {
    fallback.setWriteLockReportingThresholdMs(writeLockReportingThresholdMs);
  }

  @Override
  public long getWriteLockReportingThresholdMs() {
    return fallback.getWriteLockReportingThresholdMs();
  }

  @Override
  public void setLockForTests(ReentrantReadWriteLock lock) {
    fallback.setLockForTests(lock);
  }

  @Override
  public ReentrantReadWriteLock getLockForTests() {
    return fallback.getLockForTests();
  }
}
