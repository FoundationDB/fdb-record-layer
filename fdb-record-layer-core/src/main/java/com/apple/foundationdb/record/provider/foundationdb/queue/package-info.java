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
 * Persistent FDB-backed queue primitives shared across the Record Layer.
 *
 * <p>The main class is
 * {@link com.apple.foundationdb.record.provider.foundationdb.queue.PendingWritesQueue
 * PendingWritesQueue}, which holds pending entries while the index is busy (e.g. the indexer is running
 * in the background. The intent is for the queue to be drained when the index becomes available again.</p>
 *
 */
package com.apple.foundationdb.record.provider.foundationdb.queue;
