/*
 * FDBThreadFilter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.codec;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * The randomized testing framework that Lucene uses has checks for leaked threads, but FDB depends on the
 * network thread, a common thread pool, and the {@link com.apple.foundationdb.test.TestExecutors#defaultThreadPool()}, so we filter those out.
 */
public class FDBThreadFilter implements ThreadFilter {
    @Override
    public boolean reject(final Thread t) {
        return t.getName().equals("fdb-network-thread") ||
               t.getName().startsWith("ForkJoinPool.commonPool") ||
               t.getName().startsWith("fdb-record-layer-test-");
    }
}
