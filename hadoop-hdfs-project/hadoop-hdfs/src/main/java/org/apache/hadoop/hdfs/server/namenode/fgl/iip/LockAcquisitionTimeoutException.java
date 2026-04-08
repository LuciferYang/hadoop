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

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Thrown when a per-INode lock acquisition exceeds its deadline.
 *
 * <p>The RPC handler should fail the current request; clients will
 * retry at their own discretion. See {@code docs/fgl/HDFS-17385-wave4-pilot-design.md}
 * for the rationale behind the default timeout
 * ({@code dfs.namenode.fgl.iip.lock.timeout.ms}, default 5000 ms).
 */
@InterfaceAudience.Private
public class LockAcquisitionTimeoutException extends IOException {

  private static final long serialVersionUID = 1L;

  private final long inodeId;
  private final boolean writeLock;

  public LockAcquisitionTimeoutException(long inodeId, boolean writeLock) {
    super("Timed out acquiring " + (writeLock ? "write" : "read")
        + " lock on INode id=" + inodeId);
    this.inodeId = inodeId;
    this.writeLock = writeLock;
  }

  public LockAcquisitionTimeoutException(String message) {
    super(message);
    this.inodeId = -1L;
    this.writeLock = false;
  }

  public long getInodeId() {
    return inodeId;
  }

  public boolean isWriteLock() {
    return writeLock;
  }
}
