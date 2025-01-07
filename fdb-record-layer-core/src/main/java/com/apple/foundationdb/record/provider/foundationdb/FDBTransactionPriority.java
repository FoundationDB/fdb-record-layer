/*
 * FDBTransactionPriority.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;

/**
 * An enum indicating the priority of transactions against FoundationDB. This value can then be used to set options on
 * the transaction to indicate to the database the priority of the transaction's work.
 *
 * <p>
 * This API is {@link API.Status#UNSTABLE UNSTABLE}, but it may add additional values if they become available.
 * </p>
 */
@API(API.Status.UNSTABLE)
public enum FDBTransactionPriority {
    /**
     * The priority level that should be used for batch operations. This should be used for discretionary work
     * such as background jobs that do not have strict time pressure to complete in the near future. For example
     * {@linkplain OnlineIndexer online index builds} and
     * {@linkplain com.apple.foundationdb.record.provider.foundationdb.cursors.SizeStatisticsCollectorCursor statistics collection}
     * should often happen at batch priority. This signals to the database that this work should be rate limited
     * at a higher rate if it would impact work done at a higher priority.
     */
    BATCH,
    /**
     * The default priority level for most transactions. This should be used for most work unless {@link #BATCH}
     * priority is more appropriate. The system will apply normal rate limiting and throttling mechanisms to
     * the transaction.
     */
    DEFAULT,
    /**
     * The priority level for system operations. This is used internally within FoundationDB for operations that must
     * complete immediately for all operations, and it bypasses any rate limiting. For this reason, it should
     * generally <em>not</em> be used by users of the system, and it is included mainly for completeness.
     */
    SYSTEM_IMMEDIATE
}
