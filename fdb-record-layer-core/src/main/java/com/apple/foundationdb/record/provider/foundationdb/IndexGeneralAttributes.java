/*
 * indexGeneralAttributes.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;


/**
 * Provide general information and guidelines for the index.
 */
@API(API.Status.EXPERIMENTAL)
public final class IndexGeneralAttributes {
    private final boolean optimizedForMutualIndexing;

    /**
     * Create a new set of index attributes.
     *
     * @param optimizedForMutualIndexing whether the index type is optimized for mutual indexing.
     */
    public IndexGeneralAttributes(boolean optimizedForMutualIndexing) {
        this.optimizedForMutualIndexing = optimizedForMutualIndexing;
    }

    /**
     * Predict if the indexing process goes through a bottleneck (like a semaphore or a single item that will cause a
     * conflict for concurrent transactions). This returns {@code true} if it can be expected that N concurrent workers
     * will take 1/Nth the time to build using mutual indexing. This returns {@code false} if the index maintenance does
     * not play well with concurrent writes, and N workers will take approximately the same time as building without
     * concurrency.
     *
     * @return true unless not optimized to mutual indexing
     */
    public boolean isOptimizedForMutualIndexing() {
        return optimizedForMutualIndexing;
    }
}
