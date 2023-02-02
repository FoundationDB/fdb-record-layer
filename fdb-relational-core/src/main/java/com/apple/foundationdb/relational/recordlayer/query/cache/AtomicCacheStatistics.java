/*
 * AtomicCacheStatistics.java
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import java.util.concurrent.atomic.AtomicLong;

public abstract class AtomicCacheStatistics implements CacheStatistics {
    final AtomicLong cacheHits = new AtomicLong(0L);
    final AtomicLong cacheMisses = new AtomicLong(0L);
    final AtomicLong reads = new AtomicLong(0L);
    final AtomicLong writes = new AtomicLong(0L);

    @Override
    public long numHits() {
        return cacheHits.get();
    }

    @Override
    public long numMisses() {
        return cacheMisses.get();
    }

    @Override
    public long numWrites() {
        return writes.get();
    }

    @Override
    public long numReads() {
        return reads.get();
    }
}
