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
 * Thrown from {@link INodeLockManager#acquire} when the path walk
 * encounters a structural condition the FGL_IIP pilot does not handle
 * (for example, a symlink at an ancestor position).
 *
 * <p>Callers should catch this and delegate the RPC to the fallback
 * path (via the composed Phase I lock manager). See the pilot design
 * spec §3 for the full list of envelope-miss conditions.
 *
 * <p>This is distinct from {@link LockAcquisitionTimeoutException}
 * (a timing failure) — an envelope miss is a permanent classification
 * of the request, and retrying under the same mode will produce the
 * same result.
 */
@InterfaceAudience.Private
public class PilotEnvelopeMissException extends IOException {

  private static final long serialVersionUID = 1L;

  public PilotEnvelopeMissException(String message) {
    super(message);
  }

  public PilotEnvelopeMissException(String message, Throwable cause) {
    super(message, cause);
  }
}
