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
/**
 * Directory-tree fine-grained locking (Phase II, HDFS-17385).
 *
 * <p>This package implements the {@code FGL_IIP} lock mode — per-INode
 * directory-tree locks used by the pilot RPCs {@code getFileInfo} and
 * scoped {@code create}. See {@code docs/fgl/HDFS-17385-wave4-pilot-design.md}
 * for the full design rationale, unit boundaries, and migration plan.
 *
 * <p>Classes in this package are package-private by design
 * (mitigation #2 of the pilot spec: single lock-acquisition chokepoint).
 * Only {@link org.apache.hadoop.hdfs.server.namenode.fgl.iip.IIPBasedFSNamesystemLock}
 * is public and is reached exclusively through the
 * {@link org.apache.hadoop.hdfs.server.namenode.fgl.FSNLockManager} interface.
 */
package org.apache.hadoop.hdfs.server.namenode.fgl.iip;
