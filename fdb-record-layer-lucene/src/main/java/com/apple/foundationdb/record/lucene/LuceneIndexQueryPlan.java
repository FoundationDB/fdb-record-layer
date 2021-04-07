/*
 * LuceneIndexQueryPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Lucene query plan for including sort parameters.
 */
public class LuceneIndexQueryPlan extends RecordQueryIndexPlan {
    private KeyExpression sort;

    public LuceneIndexQueryPlan(@Nonnull final String indexName, @Nonnull final IndexScanType scanType, @Nonnull final ScanComparisons comparisons, final boolean reverse, KeyExpression sort) {
        super(indexName, scanType, comparisons, reverse);
        this.sort = sort;
    }

    public LuceneIndexQueryPlan(@Nonnull final String indexName, @Nonnull final IndexScanType scanType, @Nonnull final ScanComparisons comparisons, final boolean reverse) {
        this(indexName, scanType, comparisons, reverse, null);
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<IndexEntry> executeEntries(@Nonnull final FDBRecordStoreBase<M> store,
                                                                       @Nonnull final EvaluationContext context,
                                                                       @Nullable final byte[] continuation,
                                                                       @Nonnull final ExecuteProperties executeProperties) {
        final TupleRange range = comparisons.toTupleRange(store, context);
        final RecordMetaData metaData = store.getRecordMetaData();
        RecordCursor<IndexEntry> indexEntryRecordCursor = store.scanIndex(metaData.getIndex(indexName), scanType, range, continuation, executeProperties.asScanProperties(reverse));
        if (indexEntryRecordCursor instanceof LuceneRecordCursor && sort != null)  {
            ((LuceneRecordCursor) indexEntryRecordCursor).setSort(sort.toKeyExpression());
        }
        return indexEntryRecordCursor;
    }
}
