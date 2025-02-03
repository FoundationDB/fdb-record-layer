/*
 * FdbConnection.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

/**
 * Represents a connection to a single FDB cluster. This functionally acts as an equivalent to FDBDatabase,
 * but can be swapped out more cleanly on an as-needed basis.
 *
 * Note that FdbConnections isolate {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase},
 * but it is <em>not</em> intended to isolate RecordLayer itself. Instead, it's main usage is to allow flexible
 * implementations of Fdb connections for use <em>by</em> RecordLayer and it's assorted classes.
 */
public interface FdbConnection extends AutoCloseable {

    TransactionManager getTransactionManager();

    @Override
    void close() throws RelationalException;

}
