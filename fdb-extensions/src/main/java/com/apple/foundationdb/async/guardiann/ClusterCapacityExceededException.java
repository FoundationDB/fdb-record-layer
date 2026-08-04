/*
 * ClusterCapacityExceededException.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.util.LoggableException;

import javax.annotation.Nonnull;
import java.util.UUID;

/**
 * Thrown by an insert when a primary cluster reaches its configured hard cap ({@code primaryClusterHardMax}) while the
 * write path is <em>not</em> draining deferred maintenance tasks in the writing transaction. It signals back-pressure:
 * inserts have outrun the deferred split backlog, so the caller should slow down until the background merge catches up
 * rather than letting the cluster grow without bound.
 * <p>
 * This lives in the extensions layer (which cannot reference the record layer's {@code RecordCoreException}); the
 * vector index engine translates it into a record-layer exception for callers.
 */
@SuppressWarnings("serial")
public class ClusterCapacityExceededException extends LoggableException {
    public ClusterCapacityExceededException(@Nonnull final UUID clusterId, final int numPrimaryVectors,
                                            final int primaryClusterHardMax) {
        super("primary cluster reached its hard cap while the deferred split backlog is not being drained");
        addLogInfo("clusterId", clusterId);
        addLogInfo("numPrimaryVectors", numPrimaryVectors);
        addLogInfo("primaryClusterHardMax", primaryClusterHardMax);
    }
}
