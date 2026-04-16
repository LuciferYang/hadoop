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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Unit tests for {@link IIPAcquireMode}.
 *
 * <p>These tests pin the taxonomy in place — any change to the mode set
 * or its classification requires updating both the enum and this test
 * class, which forces reviewers to think about the change.
 */
public class TestIIPAcquireMode {

  /** Expected pilot-implemented modes. Change with intent. */
  private static final Set<IIPAcquireMode> PILOT_MODES =
      EnumSet.of(IIPAcquireMode.PATH_READ,
          IIPAcquireMode.PARENT_WRITE,
          IIPAcquireMode.PATH_WRITE,
          IIPAcquireMode.ANCESTOR_WRITE,
          IIPAcquireMode.RENAME_WRITE);

  /** Expected fallback modes. Change with intent. */
  private static final Set<IIPAcquireMode> FALLBACK_MODES =
      EnumSet.of(IIPAcquireMode.GLOBAL_READ, IIPAcquireMode.ADMIN_META);

  /** Expected deferred modes. Change with intent. */
  private static final Set<IIPAcquireMode> DEFERRED_MODES =
      EnumSet.noneOf(IIPAcquireMode.class);

  /** Modes that take some per-INode write lock. */
  private static final Set<IIPAcquireMode> IIP_WRITE_MODES =
      EnumSet.of(IIPAcquireMode.PARENT_WRITE,
          IIPAcquireMode.PATH_WRITE,
          IIPAcquireMode.ANCESTOR_WRITE,
          IIPAcquireMode.RENAME_WRITE);

  @Test
  public void taxonomyHasExactlySevenModes() {
    assertEquals(7, IIPAcquireMode.values().length,
        "Changing the taxonomy requires updating the pilot design spec §1.4 "
            + "and the migration checklist. Don't drive-by edit this.");
  }

  @Test
  public void pilotFallbackDeferredPartitionCoversAllModes() {
    int total = PILOT_MODES.size() + FALLBACK_MODES.size() + DEFERRED_MODES.size();
    assertEquals(IIPAcquireMode.values().length, total,
        "Every mode must be exactly one of PILOT / FALLBACK / DEFERRED.");
    // Verify disjoint.
    for (IIPAcquireMode m : PILOT_MODES) {
      assertFalse(FALLBACK_MODES.contains(m));
      assertFalse(DEFERRED_MODES.contains(m));
    }
    for (IIPAcquireMode m : FALLBACK_MODES) {
      assertFalse(DEFERRED_MODES.contains(m));
    }
  }

  @ParameterizedTest
  @EnumSource(IIPAcquireMode.class)
  public void implIsNonNull(IIPAcquireMode mode) {
    assertNotNull(mode.impl());
  }

  @ParameterizedTest
  @EnumSource(IIPAcquireMode.class)
  public void pilotClassificationMatchesExpectation(IIPAcquireMode mode) {
    boolean expected = PILOT_MODES.contains(mode);
    assertEquals(expected, mode.isPilot(),
        mode + " pilot classification mismatch");
    if (expected) {
      assertSame(IIPAcquireMode.Impl.PILOT, mode.impl());
    }
  }

  @ParameterizedTest
  @EnumSource(IIPAcquireMode.class)
  public void fallbackClassificationMatchesExpectation(IIPAcquireMode mode) {
    boolean expected = FALLBACK_MODES.contains(mode);
    assertEquals(expected, mode.isFallback(),
        mode + " fallback classification mismatch");
    if (expected) {
      assertSame(IIPAcquireMode.Impl.FALLBACK, mode.impl());
    }
  }

  @ParameterizedTest
  @EnumSource(IIPAcquireMode.class)
  public void deferredClassificationMatchesExpectation(IIPAcquireMode mode) {
    boolean expected = DEFERRED_MODES.contains(mode);
    assertEquals(expected, mode.isDeferred(),
        mode + " deferred classification mismatch");
    if (expected) {
      assertSame(IIPAcquireMode.Impl.DEFERRED, mode.impl());
    }
  }

  @ParameterizedTest
  @EnumSource(IIPAcquireMode.class)
  public void needsIIPWriteMatchesWriteModeSet(IIPAcquireMode mode) {
    boolean expected = IIP_WRITE_MODES.contains(mode);
    assertEquals(expected, mode.needsIIPWrite(),
        mode + " needsIIPWrite() classification mismatch");
  }

  @Test
  public void onlyAdminMetaNeedsCompatWrite() {
    for (IIPAcquireMode mode : IIPAcquireMode.values()) {
      if (mode == IIPAcquireMode.ADMIN_META) {
        assertTrue(mode.needsCompatWrite(),
            "ADMIN_META is the only mode that takes compat-write");
      } else {
        assertFalse(mode.needsCompatWrite(),
            mode + " must not take compat-write");
      }
    }
  }

  @Test
  public void adminMetaTakesCompatWriteButNotIIPWrite() {
    // ADMIN_META is subtle: it's the only mode that needs compat-write,
    // but it does NOT take any per-INode write lock — it bypasses the
    // IIP path entirely and routes through the compat namespace lock.
    assertTrue(IIPAcquireMode.ADMIN_META.needsCompatWrite());
    assertFalse(IIPAcquireMode.ADMIN_META.needsIIPWrite());
  }

  @Test
  public void pilotExercisesFourWriteModes() {
    // All four write modes are now pilot-implemented.
    long pilotWrites = PILOT_MODES.stream()
        .filter(IIPAcquireMode::needsIIPWrite)
        .count();
    assertEquals(4, pilotWrites,
        "pilot exercises all four IIP write patterns "
            + "(PARENT_WRITE, PATH_WRITE, ANCESTOR_WRITE, RENAME_WRITE)");
  }

  @Test
  public void deferredModeSetMatchesSpec() {
    // No deferred modes remain — all 7 modes are implemented.
    assertEquals(EnumSet.noneOf(IIPAcquireMode.class),
        EnumSet.allOf(IIPAcquireMode.class).stream()
            .filter(IIPAcquireMode::isDeferred)
            .collect(java.util.stream.Collectors.toCollection(
                () -> EnumSet.noneOf(IIPAcquireMode.class))));
  }
}
