/*
 * LuceneScanQuery.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.search.Query;

import javax.annotation.Nonnull;

/**
 * Scan a {@code LUCENE} index using a Lucene {@link Query}.
 */
@API(API.Status.UNSTABLE)
public class LuceneScanQuery extends LuceneScanBounds {
    @Nonnull
    final Query query;

    public LuceneScanQuery(@Nonnull IndexScanType scanType, @Nonnull Tuple groupKey, @Nonnull Query query) {
        super(scanType, groupKey);
        this.query = query;
    }

    @Nonnull
    public Query getQuery() {
        return query;
    }

    @Override
    public String toString() {
        return super.toString() + " " + query;
    }
}
