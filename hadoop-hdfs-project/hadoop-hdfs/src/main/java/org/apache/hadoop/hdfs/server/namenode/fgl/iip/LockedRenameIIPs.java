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
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;

import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * Two-path IIP handle returned by
 * {@link INodeLockManager#acquireRename}. Holds the source and
 * destination IIPs, all per-INode locks (both ancestor reads and
 * parent writes), and the compat-read lock. Closing releases
 * everything and resets ThreadLocals.
 *
 * @see docs/fgl/HDFS-17385-wave4-pilot-design.md §1.4, §1.8 rule 3
 */
@InterfaceAudience.Private
public final class LockedRenameIIPs implements AutoCloseable {

  private final INodesInPath srcIIP;
  private final INodesInPath dstIIP;
  private final List<LockRef> heldLocks;
  private final Lock compatReadLock;

  LockedRenameIIPs(INodesInPath srcIIP, INodesInPath dstIIP,
      List<LockRef> heldLocks, Lock compatReadLock) {
    this.srcIIP = srcIIP;
    this.dstIIP = dstIIP;
    this.heldLocks = heldLocks;
    this.compatReadLock = compatReadLock;
  }

  public INodesInPath srcIIP() { return srcIIP; }
  public INodesInPath dstIIP() { return dstIIP; }

  @Override
  public void close() {
    try {
      for (int i = heldLocks.size() - 1; i >= 0; i--) {
        try {
          heldLocks.get(i).close();
        } catch (RuntimeException ignored) {
          // best-effort release
        }
      }
      compatReadLock.unlock();
    } finally {
      INodeLockManager.HELD_IIP_DEPTH.set(0);
      INodeLockManager.HELD_IIP_WRITE.set(Boolean.FALSE);
    }
  }
}
