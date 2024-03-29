/*
 * CursorStreamingMode.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;

/**
 * The streaming mode to use when opening {@link RecordCursor}s.
 *
 * @see ScanProperties#getCursorStreamingMode
 */
@API(API.Status.EXPERIMENTAL)
public enum CursorStreamingMode {
    /** The client will process records one-at-a-time. */
    ITERATOR,
    /** The client will load all records immediately, such as with {@link RecordCursor#asList}. */
    WANT_ALL,
    /** Advanced. Transfer data in batches small enough to not be much more expensive than reading individual rows,
     * to minimize cost if iteration stops early */
    SMALL,
    /** Advanced. Transfer data in batches sized in between small and large */
    MEDIUM,
    /** Advanced. Transfer data in batches large enough to be, in a high-concurrency environment, nearly as efficient as possible */
    LARGE
}
