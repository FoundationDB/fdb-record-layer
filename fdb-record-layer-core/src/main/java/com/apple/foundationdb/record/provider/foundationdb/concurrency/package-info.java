/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

/**
 * Classes for managing concurrency of a store within a single transaction. For managing
 * concurrency across different transactions, we generally rely on the FDB transaction
 * resolver. That will fail any transaction that relies on stale information. However,
 * within a transaction, the FDB client itself offers little help, and so locks need
 * to be managed by the Record Layer framework itself. We also need to avoid taking normal
 * locks over long-running operations, as that can block threads in the asynchronous thread pool.
 * For that reason, we prefer using the {@link com.apple.foundationdb.record.locking.AsyncLock}
 * abstraction, which returns a future that is made available only when a resource is
 * ready. Each async lock is associated with a given resource key (that is, a
 * {@link com.apple.foundationdb.record.locking.LockIdentifier}), and so the classes in
 * this package are primarily present to ensure that we manage those locks the right way.
 *
 * @see com.apple.foundationdb.record.locking.AsyncLock
 * @see com.apple.foundationdb.record.locking.LockRegistry
 */
package com.apple.foundationdb.record.provider.foundationdb.concurrency;
