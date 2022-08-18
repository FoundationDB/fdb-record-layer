/*
 * LuceneIndexForceMerge.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;

/**
 * Optimize Lucene index by merging segments.
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneIndexForceMerge extends IndexOperation {
    @Nonnull
    private final Tuple groupingKey;
    private final int maxNumSegments;
    private final long commitAfterBytesLimit;
    private final long commitAfterMillisLimit;

    public LuceneIndexForceMerge(@Nonnull Tuple groupingKey, int maxNumSegments, final long commitAfterBytesLimit, final long commitAfterMillisLimit) {
        this.groupingKey = groupingKey;
        this.maxNumSegments = maxNumSegments;
        this.commitAfterBytesLimit = commitAfterBytesLimit;
        this.commitAfterMillisLimit = commitAfterMillisLimit;
    }

    @Nonnull
    public Tuple getGroupingKey() {
        return groupingKey;
    }

    public int getMaxNumSegments() {
        return maxNumSegments;
    }

    public long getCommitAfterBytesLimit() {
        return commitAfterBytesLimit;
    }

    public long getCommitAfterMillisLimit() {
        return commitAfterMillisLimit;
    }
}
